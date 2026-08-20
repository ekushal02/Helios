package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// C-13: ten clients at once
// =============================================================================
//
// The end-to-end test issued one write at a time from one goroutine. Every
// Submit found the leader idle, appended at a quiet index, and was observed
// applied before the next one existed. That test proves ordering; it cannot
// prove anything about what happens when ten clients hammer the same leader,
// because that never happens in it.
//
// This one runs ten client goroutines against the same three-node cluster.
// The property is unchanged -- every node's state machine holds the same data
// at the end -- but the pressure is different. What is newly under test:
//
//   - The per-follower fan-out from C-3 tolerates being re-entered while an
//     earlier round is still outstanding.
//   - nextIndex and matchIndex survive concurrent mutation from several
//     replication goroutines at once.
//   - The applier keeps delivering a single gapless sequence while commitIndex
//     is being advanced from many reply handlers.
//
// What is NOT newly under test, despite appearances: Submit's index assignment.
// TestConcurrentSubmitsGetDistinctIndices in submit_test.go already proves that
// deterministically against a stub transport, and it is the better test for it.
// The collision check below covers the same ground across leader changes, which
// that one cannot produce -- worth keeping, not worth billing as the headline.
//
// Run this one under -race first and last.
//
// Shared with e2e_test.go, not redeclared here: e2eCluster, kvMachine,
// attachMachine, encodePut, currentLeader, waitForLeader, quietApplyIndex.

const (
	concClients       = 10
	concOpsPerClient  = 100
	concKeysPerClient = 20
)

// =============================================================================
// Why each client owns its own keys
// =============================================================================
//
// The e2e test could name the expected final value of every key because the
// writes were totally ordered: write 100 landed last, so key k holds v099.
//
// With ten clients sharing a key space that calculation is gone. If clients 3
// and 7 both write "k04", the final value is decided by which one the leader
// happened to append second -- a genuine race, not a bug, and not something a
// test can predict. The test would be reduced to asserting the three nodes
// agree with EACH OTHER, which is real but weaker: three nodes that all lost
// the same write agree perfectly.
//
// Giving each client a private key namespace keeps both assertions. Within one
// client the writes are still sequential and still reuse keys five times each,
// so ordering is still load-bearing in the expected map. Across clients the
// namespaces never touch, so interleaving cannot change any final value. The
// test can therefore say what the answer is, not merely that everyone agrees
// on it.
func concKey(client, op int) string {
	return fmt.Sprintf("c%02d-k%02d", client, op%concKeysPerClient)
}

func concVal(op int) string { return fmt.Sprintf("v%04d", op) }

// =============================================================================
// Measuring whether the clients actually overlapped
// =============================================================================

// inFlightMeter records the high-water mark of goroutines inside Submit.
//
// This is the anti-vacuity guard. Ten goroutines that happen to take turns --
// because a mutex somewhere serialises the whole put cycle, or because the
// scheduler never interleaves them -- would pass every assertion below while
// testing exactly what the e2e test already tested. A peak of 1 means this
// file ran a slow sequential test and told you nothing new.
//
// The meter wraps Submit only, not the whole put-and-wait cycle. Wrapping the
// cycle would report a peak of 10 on every run and guard nothing, since the
// clients obviously coexist; the question is whether they ever contend for the
// append path.
type inFlightMeter struct {
	cur atomic.Int64
	max atomic.Int64
}

func (m *inFlightMeter) enter() {
	c := m.cur.Add(1)
	for {
		hi := m.max.Load()
		if c <= hi || m.max.CompareAndSwap(hi, c) {
			return
		}
	}
}

func (m *inFlightMeter) leave() { m.cur.Add(-1) }

func (m *inFlightMeter) peak() int64 { return m.max.Load() }

// =============================================================================
// A client
// =============================================================================

// submitRecord is one accepted Submit: the index the leader promised and the
// term it promised it in.
type submitRecord struct {
	client, op  int
	term, index int
}

type concClient struct {
	id       int
	nodes    []*Node
	machines []*kvMachine
	meter    *inFlightMeter

	// want is this client's contribution to the expected final map.
	want map[string]string

	// submits holds every accepted Submit, including retries. Retries are the
	// interesting entries: two records with the same (client, op) are a
	// duplicate command in the log.
	submits []submitRecord

	retries int

	// fault is set instead of calling t.Fatalf.
	//
	// t.Fatalf from a non-test goroutine is a Go testing bug, not a style
	// preference: it calls runtime.Goexit on the wrong goroutine, so the test
	// body keeps running and reports something unrelated. t.Errorf is legal
	// here but would interleave ten goroutines' output into noise. Recording
	// the first fault and letting the test body report it keeps the failure
	// readable and keeps the testWriter rule satisfied.
	fault string
}

// put issues one write and does not return until it has watched that exact
// command apply, or until it gives up.
//
// THE CLAIM TICKET, AS submit.go DESCRIBES IT. Submit returns (index, term) and
// promises nothing: the entry is on one machine, replicated nowhere. The caller
// learns the outcome by watching for that index to come back out of ApplyCh
// CARRYING THAT TERM. An index that reappears under a different term means a
// later leader overwrote the slot and this submission is gone.
//
// The retry that follows is where the remaining hole lives, and submit.go names
// it: a submission that was overwritten should be reported failed, not retried,
// because the original may have committed after all and a retry double-applies.
// For an idempotent Put that is invisible in the final state and visible only in
// the applied COUNT, which is why the count assertion below is >= rather than
// ==. Client dedup closes it.
func (c *concClient) put(op int) bool {
	key, val := concKey(c.id, op), concVal(op)
	deadline := time.Now().Add(15 * time.Second)

	for attempt := 0; time.Now().Before(deadline); attempt++ {
		if attempt > 0 {
			c.retries++
		}

		li := currentLeader(c.nodes)
		if li < 0 {
			time.Sleep(2 * time.Millisecond)
			continue
		}

		c.meter.enter()
		idx, term, isLeader := c.nodes[li].Submit(encodePut(key, val))
		c.meter.leave()

		if !isLeader {
			// Lost the term between currentLeader and Submit. Normal under
			// load; find whoever won and try again.
			time.Sleep(2 * time.Millisecond)
			continue
		}
		c.submits = append(c.submits, submitRecord{
			client: c.id, op: op, term: term, index: idx,
		})

		if !c.machines[li].waitForIndex(idx, 2*time.Second) {
			// The promise was not kept in time. Either this node lost its term
			// before committing, or the entry is still in flight.
			continue
		}

		// waitForIndex says SOMETHING reached that index, not that it was ours.
		// This is the term half of the ticket.
		if got, ok := c.machines[li].termAt(idx); !ok || got != term {
			continue
		}

		c.want[key] = val
		return true
	}

	c.fault = fmt.Sprintf(
		"client %d: op %d (%s=%s) never landed within 15s -- %d retries so far",
		c.id, op, key, val, c.retries)
	return false
}

func (c *concClient) run(ops int, wg *sync.WaitGroup) {
	defer wg.Done()
	c.want = make(map[string]string, concKeysPerClient)
	for op := 0; op < ops; op++ {
		if !c.put(op) {
			return
		}
	}
}

func newConcClients(nodes []*Node, machines []*kvMachine, meter *inFlightMeter) []*concClient {
	clients := make([]*concClient, concClients)
	for i := range clients {
		clients[i] = &concClient{
			id: i, nodes: nodes, machines: machines, meter: meter,
		}
	}
	return clients
}

func runConcClients(clients []*concClient, ops int) time.Duration {
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(len(clients))
	for _, c := range clients {
		go c.run(ops, &wg)
	}
	wg.Wait()

	return time.Since(start)
}

func totalRetries(clients []*concClient) int {
	n := 0
	for _, c := range clients {
		n += c.retries
	}
	return n
}

// mustQuietApplyIndex is quietApplyIndex with the timeout turned into a stop.
//
// Every assertion after this call assumes a settled cluster, so falling through
// on a timeout would turn one problem into a page of consequential noise. The
// end-to-end test wants the opposite and calls quietApplyIndex directly.
func mustQuietApplyIndex(t *testing.T, machines []*kvMachine, within time.Duration) int {
	t.Helper()

	idxs, ok := quietApplyIndex(machines, within)
	if !ok {
		t.Fatalf("state machines never settled within %v: applied index = %v",
			within, idxs)
	}
	return idxs[0]
}

// =============================================================================
// The test
// =============================================================================

func TestTenConcurrentClientsLeaveIdenticalStateMachines(t *testing.T) {
	ops := concOpsPerClient
	if testing.Short() {
		// Tiered rather than skipped. A consensus implementation whose
		// concurrency test only runs on the long path is a concurrency test
		// that stops running. Forty ops per client is still four hundred
		// commands and still covers all twenty keys, so every assertion below
		// keeps its meaning -- only the sample size shrinks.
		ops = 40
	}
	total := concClients * ops

	nodes := e2eCluster(t, 3, 13)

	machines := make([]*kvMachine, len(nodes))
	for i, n := range nodes {
		machines[i] = attachMachine(n)
	}

	waitForLeader(t, nodes, 3*time.Second)

	meter := &inFlightMeter{}
	clients := newConcClients(nodes, machines, meter)
	elapsed := runConcClients(clients, ops)

	// ---- client-side faults, before anything else -------------------------
	//
	// A client that gave up mid-run leaves a truncated want map, which would
	// turn one timeout into a pile of confusing missing-key errors.
	for _, c := range clients {
		if c.fault != "" {
			t.Fatal(c.fault)
		}
	}

	// ---- did the clients actually contend? --------------------------------
	peak := meter.peak()
	t.Logf("%d ops from %d clients in %v; peak concurrent Submits = %d; retries = %d",
		total, len(clients), elapsed, peak, totalRetries(clients))

	// Checked on both tiers. Four hundred submits across ten goroutines that
	// never once overlap would itself be worth knowing about.
	if peak < 2 {
		t.Error("peak concurrent Submits = 1: the clients never overlapped, so " +
			"this run tested nothing that the single-writer end-to-end test " +
			"does not already cover")
	}

	// ---- no two commands were promised the same slot ----------------------
	//
	// Within a single term there is one leader appending to one log, so the
	// indices it hands out must be distinct. Two Submits in the same term
	// returning the same index means one command overwrote the other between
	// reading lastLogIndex() and appending -- a lost update that leaves the log
	// perfectly well-formed and is therefore invisible to every assertion below.
	//
	// Across terms indices repeat legitimately, which is why the key is the
	// pair. A client's own retry is not a false positive: overwriting an entry
	// requires a new leader, so the retry lands in a different term.
	seen := make(map[[2]int]submitRecord, total)
	for _, c := range clients {
		for _, r := range c.submits {
			k := [2]int{r.term, r.index}
			if prev, dup := seen[k]; dup {
				t.Errorf("term %d index %d promised twice: client %d op %d and "+
					"client %d op %d -- one of these commands was overwritten "+
					"before it was ever replicated",
					r.term, r.index, prev.client, prev.op, r.client, r.op)
				continue
			}
			seen[k] = r
		}
	}

	// ---- let the cluster go quiet -----------------------------------------
	settled := mustQuietApplyIndex(t, machines, 10*time.Second)

	// ---- state machine faults ---------------------------------------------
	for i, m := range machines {
		if _, _, _, fault := m.snapshot(); fault != "" {
			t.Errorf("node %d state machine: %s", i, fault)
		}
	}

	// ---- applied count ----------------------------------------------------
	//
	// SCOPE FENCE (is C-13): >= rather than ==, and the excess is the number of
	// duplicate commands the retry path introduced. When client dedup lands,
	// this tightens to == and the tightening is the visible sign the hole
	// closed -- same marker as the e2e test.
	//
	// On a quiet in-memory network no leader change happens and the excess is
	// zero, so the fence has never actually been exercised. The test that would
	// exercise it is this one with a kill() partway through; the harness
	// already supports it.
	if settled < total {
		t.Errorf("applied %d entries, want at least %d: entries are missing, "+
			"not merely duplicated", settled, total)
	}
	if settled > total {
		t.Logf("applied %d entries for %d ops: %d duplicates from %d retries, "+
			"expected until client dedup lands",
			settled, total, settled-total, totalRetries(clients))
	}

	// ---- the applied sequences are identical ------------------------------
	//
	// Compared before the maps, because a divergence caught here names the
	// position and the two commands, while a map comparison only says the
	// answer came out wrong.
	base, baseOrder, _, _ := machines[0].snapshot()
	for i := 1; i < len(machines); i++ {
		_, order, _, _ := machines[i].snapshot()
		if len(order) != len(baseOrder) {
			t.Errorf("node %d applied %d entries, node 0 applied %d, after both "+
				"reported applied index %d", i, len(order), len(baseOrder), settled)
		}
		n := len(order)
		if len(baseOrder) < n {
			n = len(baseOrder)
		}
		for j := 0; j < n; j++ {
			if order[j] != baseOrder[j] {
				t.Fatalf("nodes 0 and %d diverge at applied position %d: "+
					"%q vs %q -- State Machine Safety is violated",
					i, j, baseOrder[j], order[j])
			}
		}
	}

	// ---- the final maps are identical, and correct ------------------------
	want := make(map[string]string, concClients*concKeysPerClient)
	for _, c := range clients {
		for k, v := range c.want {
			want[k] = v
		}
	}

	for i, m := range machines {
		state, _, _, _ := m.snapshot()
		if len(state) != len(want) {
			t.Errorf("node %d holds %d keys, want %d", i, len(state), len(want))
		}
		for k, wantV := range want {
			if got := state[k]; got != wantV {
				t.Errorf("node %d: %s = %q, want %q -- the last write to this "+
					"key did not land last", i, k, got, wantV)
			}
		}
	}

	// Redundant with the loop above once want is right, and worth having
	// anyway: if want itself is wrong the loop accuses all three nodes
	// identically, which reads like a replication bug and is not one.
	for i := 1; i < len(machines); i++ {
		state, _, _, _ := machines[i].snapshot()
		for k, v := range base {
			if state[k] != v {
				t.Errorf("nodes 0 and %d disagree on %s: %q vs %q",
					i, k, v, state[k])
			}
		}
	}
}

// =============================================================================
// The control
// =============================================================================

// One node, ten clients. Removes replication and leader change entirely and
// leaves only the local append-commit-apply path under concurrent pressure.
//
// If the three-node test above goes red, run this one next. Red here means the
// bug is in Submit or the applier. Green here and red above means the bug is in
// the fan-out, in nextIndex/matchIndex, or in the commit rule.
//
// The count assertion is exact: a single node is its own majority, every
// Submit commits immediately, and no leader change is possible, so there is
// nothing to retry and no duplicate to tolerate.
func TestTenConcurrentClientsOnOneNodeApplyExactlyOnce(t *testing.T) {
	ops := concOpsPerClient
	if testing.Short() {
		ops = 40
	}
	total := concClients * ops

	nodes := e2eCluster(t, 1, 17)
	machines := []*kvMachine{attachMachine(nodes[0])}

	waitForLeader(t, nodes, 3*time.Second)

	meter := &inFlightMeter{}
	clients := newConcClients(nodes, machines, meter)
	runConcClients(clients, ops)

	for _, c := range clients {
		if c.fault != "" {
			t.Fatal(c.fault)
		}
	}

	// The sharpest assertion in the file. Nothing can take the term away from a
	// single-node cluster and nothing crosses the network, so every Submit must
	// commit and apply promptly. A retry here means Submit returned an index it
	// did not honour, or the applier stalled -- either way a bug, not weather.
	if r := totalRetries(clients); r != 0 {
		t.Errorf("%d retries on a single-node cluster: nothing can take the "+
			"term away here, so a retry means Submit or the applier dropped "+
			"something", r)
	}

	settled := mustQuietApplyIndex(t, machines, 10*time.Second)
	if settled != total {
		t.Errorf("applied %d entries, want exactly %d", settled, total)
	}

	state, _, _, fault := machines[0].snapshot()
	if fault != "" {
		t.Error(fault)
	}
	for _, c := range clients {
		for k, wantV := range c.want {
			if got := state[k]; got != wantV {
				t.Errorf("%s = %q, want %q", k, got, wantV)
			}
		}
	}

	t.Logf("peak concurrent Submits = %d", meter.peak())
}
