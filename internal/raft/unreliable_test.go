package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// C-15: convergence when the network lies
// =============================================================================
//
// Three conditions, and only one of them is a setting.
//
// DROPPED REQUESTS are setDropRate, which already existed: the roll happens in
// route, the handler never runs, and the sender sees a failed RPC.
//
// DROPPED REPLIES are setReplyDropRate, new in this task, and the more useful
// of the two. The follower appended the entries; the leader does not know. Its
// nextIndex stays put and the next tick resends entries the follower already
// holds. Nothing else in this package puts mergeEntries in front of a duplicate
// append, so a receiver that truncated on entries matching its own log would
// have passed every test written before this one.
//
// REORDERING is not a setting and cannot honestly be dialled to 10%. It is
// emergent: replicateAll gives every message its own goroutine and route gives
// every goroutine its own random delay, so a message overtakes an earlier one
// whenever it draws a shorter delay than the gap between their sends. Under
// client load that gap is microseconds and inversions are constant; between
// heartbeats on an idle cluster the gap is 50ms and a 10ms jitter range makes
// them impossible. There is no rate to set, so this file counts what actually
// happened and fails if the answer is none.
//
// WHAT IS ASSERTED, AND WHAT IS NOT. Raft does not promise progress on a lossy
// network -- with enough loss an election never completes and no test can say
// otherwise. It promises that nothing is corrupted meanwhile and that the
// cluster converges once the network behaves. So the shape is: run under
// chaos, quiesce, then assert. Election Safety is the one thing checked DURING
// the chaos, because it must hold at every instant regardless of loss.
//
// Depends on leaderfailure_test.go (C-14) for submitToLiveLeader and
// assertSameAppliedSequence, and on concurrent_test.go (C-13) for concClient,
// inFlightMeter, totalRetries and mustQuietApplyIndex.

const (
	lossyDropRate      = 0.10
	lossyReplyDropRate = 0.10

	// Jitter wide enough that same-peer messages invert constantly under load,
	// and narrow enough that a round trip stays far below an election timeout.
	// A range approaching the election timeout would not be testing loss, it
	// would be manufacturing elections and calling the result convergence.
	lossyMinDelay = 1 * time.Millisecond
	lossyMaxDelay = 10 * time.Millisecond

	lossyBound = 15 * time.Second
)

// =============================================================================
// Setup
// =============================================================================

// lossyCluster configures the network BEFORE start().
//
// The ordering is the point. failoverCluster starts the tickers first, which
// would leave the very first election running on a clean network -- the one
// election most worth making difficult.
func lossyCluster(t *testing.T, size int, seed int64) (*cluster, []*kvMachine) {
	t.Helper()

	c := newCluster(t, size, seed)
	makeNetworkLossy(c)
	c.start()

	machines := make([]*kvMachine, len(c.nodes))
	for i, n := range c.nodes {
		machines[i] = attachMachine(n)
	}
	return c, machines
}

func makeNetworkLossy(c *cluster) {
	c.net.setDelayRange(lossyMinDelay, lossyMaxDelay)
	c.net.setDropRate(lossyDropRate)
	c.net.setReplyDropRate(lossyReplyDropRate)
}

// makeNetworkReliable is the quiesce step. Everything after it is an assertion
// about convergence, which is only a fair question once the network stops
// lying.
func makeNetworkReliable(c *cluster) {
	c.net.setDropRate(0)
	c.net.setReplyDropRate(0)
	c.net.setDelayRange(0, 0)
}

func (c *cluster) networkReport() string {
	return fmt.Sprintf("%d RPCs, %d dropped, %d arrived out of order",
		c.net.rpcs(), c.net.drops(), c.net.reorderedCount())
}

// =============================================================================
// Election Safety, watched rather than sampled
// =============================================================================

// electionSafetyMonitor polls until done closes and reports the first violation
// it saw, plus the number of distinct terms that produced a leader.
//
// NOT checkSingleLeader, which calls t.Fatalf and therefore cannot run anywhere
// but the test goroutine. This returns a string so the caller can join the
// client goroutines first and report afterwards -- the testWriter rule, again.
//
// Election Safety is the one property that must hold at every instant even
// mid-chaos, which is why it is watched for the whole run rather than checked
// at the end. A cluster that briefly had two leaders in one term and then
// settled would pass every end-state assertion in this file.
func electionSafetyMonitor(c *cluster, done <-chan struct{}) (fault string, termsWithALeader int) {
	seen := map[int]bool{}

	for {
		select {
		case <-done:
			return fault, len(seen)
		case <-time.After(5 * time.Millisecond):
			for term, ids := range c.leadersByTerm() {
				seen[term] = true
				if len(ids) > 1 && fault == "" {
					fault = fmt.Sprintf("ELECTION SAFETY VIOLATED: term %d has "+
						"%d leaders %v (seed %d)", term, len(ids), ids, c.seed)
				}
			}
		}
	}
}

// watchFor runs the monitor for a fixed window.
func watchFor(c *cluster, during time.Duration) (string, int) {
	done := make(chan struct{})
	go func() {
		time.Sleep(during)
		close(done)
	}()
	return electionSafetyMonitor(c, done)
}

// =============================================================================
// Elections alone
// =============================================================================

// The cheap half: no writes, just a five-node cluster trying to agree on a
// leader while a fifth of its traffic disappears.
//
// The bound is deliberately loose. A dropped RequestVote costs a whole election
// timeout, and several can be dropped in a row, so the distribution has a long
// tail and a tight bound would be measuring luck. What the test is for is that
// the tail is finite and that nothing goes wrong while it plays out.
func TestElectionConvergesOnALossyNetwork(t *testing.T) {
	c, _ := lossyCluster(t, 5, 41)

	start := time.Now()
	leader := c.waitForSingleLeader(lossyBound)
	if leader == None {
		t.Fatalf("no leader within %v on a %.0f%%-loss network: %s -- %s",
			lossyBound, lossyDropRate*100, c.describe(), c.networkReport())
	}
	elapsed := time.Since(start)

	fault, terms := watchFor(c, 2*time.Second)
	if fault != "" {
		t.Error(fault)
	}

	t.Logf("leader %d in %v; %d term(s) produced a leader; %s",
		leader, elapsed, terms, c.networkReport())

	// ANTI-VACUITY. A run where nothing was dropped is the B-12 test with a
	// longer timeout, and would pass while proving nothing about loss.
	if d := c.net.drops(); d == 0 {
		t.Error("no message was dropped: the loss settings did not take, so " +
			"this run tested a clean network")
	}
}

// =============================================================================
// Replication
// =============================================================================

// THE HEADLINE. Clients write continuously while a fifth of the traffic is lost
// and same-peer messages routinely overtake each other. Then the network is
// repaired and every node must hold the same data in the same order.
//
// Everything a client watched apply must still be there. Writes that were in
// flight when a leader lost its term may or may not have landed -- that is what
// Submit's claim ticket means -- and the clients settle those themselves by
// retrying.
func TestReplicationConvergesAfterAnUnreliableRun(t *testing.T) {
	clientCount, ops := 5, 60
	if testing.Short() {
		ops = 15
	}
	total := clientCount * ops

	c, machines := lossyCluster(t, 5, 43)

	if c.waitForSingleLeader(lossyBound) == None {
		t.Fatalf("no leader to write to within %v: %s -- %s",
			lossyBound, c.describe(), c.networkReport())
	}

	meter := &inFlightMeter{}
	clients := lossyClients(clientCount, c.nodes, machines, meter)

	var wg sync.WaitGroup
	wg.Add(len(clients))
	for _, cl := range clients {
		go cl.run(ops, &wg)
	}

	// Watch Election Safety for exactly as long as the clients run.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	fault, terms := electionSafetyMonitor(c, done)

	// The monitor returns only after done closes, so every client goroutine has
	// finished by here. Nothing below can race a t.Errorf against a live client.
	for _, cl := range clients {
		if cl.fault != "" {
			t.Fatal(cl.fault)
		}
	}
	if fault != "" {
		t.Fatal(fault)
	}

	drops, reordered := c.net.drops(), c.net.reorderedCount()
	t.Logf("%d ops from %d clients; %d term(s) produced a leader; retries = %d; %s",
		total, clientCount, terms, totalRetries(clients), c.networkReport())

	if drops == 0 {
		t.Error("no message was dropped during the run")
	}
	if reordered > 0 {
		t.Logf("%d same-peer inversions, which now takes a leadership change "+
			"mid-flight to produce", reordered)
	}

	// ---- quiesce ----------------------------------------------------------
	makeNetworkReliable(c)

	// §5.4.2, THE SAME FLUSH C-14 NEEDED. If the run changed leaders, the
	// current one is sitting on inherited entries it may not commit by counting
	// replicas. One write of its own term releases the whole prefix by Log
	// Matching. Without this the test would wait forever on entries that are
	// safely replicated everywhere and committed nowhere, and would read a
	// known availability gap as lost data.
	flushIdx, _, flushLeader, ok := submitToLiveLeader(c, encodePut("flush", "quiesced"), lossyBound)
	if !ok {
		t.Fatalf("no leader accepted the flush write within %v after the "+
			"network was repaired: %s", lossyBound, c.describe())
	}
	for i, m := range machines {
		if !m.waitForIndex(flushIdx, 2*lossyBound) {
			t.Fatalf("node %d never applied the flush write at index %d from "+
				"leader %d, on a network that is no longer dropping anything",
				i, flushIdx, flushLeader)
		}
	}

	settled := mustQuietApplyIndex(t, machines, 2*lossyBound)

	for i, m := range machines {
		if _, _, _, f := m.snapshot(); f != "" {
			t.Errorf("node %d state machine: %s", i, f)
		}
	}

	// ---- the applied count ------------------------------------------------
	//
	// SCOPE FENCE (is C-13), and this is the scenario that finally exercises it.
	// C-14 predicted duplicates would need a leader whose entries reached a
	// majority and who then lost its term without committing them -- a
	// partition rather than a kill. Loss produces exactly that: enough dropped
	// heartbeats and a follower times out while the old leader's entries are
	// already durable on a majority. The next leader commits them, the client's
	// retry adds a second copy, and both apply.
	//
	// The expected count includes the flush write.
	expected := total + 1
	if settled < expected {
		t.Errorf("applied %d entries, want at least %d: a write the clients "+
			"watched land is missing", settled, expected)
	}
	if settled > expected {
		t.Logf("applied %d entries for %d ops plus the flush: %d duplicate "+
			"commands survived the run, which is the C-13 fence doing its job "+
			"rather than a fault", settled, total, settled-expected)
	}

	// ---- convergence ------------------------------------------------------
	assertSameAppliedSequence(t, machines)

	want := make(map[string]string, clientCount*concKeysPerClient)
	for _, cl := range clients {
		for k, v := range cl.want {
			want[k] = v
		}
	}
	for i, m := range machines {
		state, _, _, _ := m.snapshot()
		for k, wantV := range want {
			if got := state[k]; got != wantV {
				t.Errorf("node %d: %s = %q, want %q -- a write this client "+
					"watched apply did not survive the unreliable run",
					i, k, got, wantV)
			}
		}
	}
}

// lossyClients is newConcClients with a settable count.
//
// newConcClients is fixed at concClients because C-13 wanted a specific number
// in its name. Here the count is a knob: fewer clients keep the run short
// enough that a lossy network still finishes it, since every operation now
// costs retries at the Raft level.
func lossyClients(count int, nodes []*Node, machines []*kvMachine, meter *inFlightMeter) []*concClient {
	clients := make([]*concClient, count)
	for i := range clients {
		clients[i] = &concClient{
			id: i, nodes: nodes, machines: machines, meter: meter,
		}
	}
	return clients
}
