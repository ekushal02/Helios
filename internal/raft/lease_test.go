package raft

import (
	"errors"
	"testing"
	"time"
)

// =============================================================================
// Lease reads
// =============================================================================
//
// Depends on read_test.go for readThrough, readBound and the error values, and
// on leaderfailure_test.go for failoverCluster.

var (
	errNoLease      = errors.New("no read lease")
	errLeaseExpired = errors.New("lease expired mid-read")
)

// readLeased is the fast path: no append, no round trip, no network at all.
//
// Structurally identical to readThrough except for where the permission comes
// from. readThrough MANUFACTURES evidence of leadership by committing an entry;
// this reuses evidence a heartbeat already produced.
func readLeased(n *Node, m *kvMachine, key string, within time.Duration) (string, bool, error) {
	idx, until, ok := n.ReadLease()
	if !ok {
		return "", false, errNoLease
	}

	// The same wait as the barrier path, and for the same reason: commitIndex
	// moves in a reply handler, lastApplied when the applier hands over. The
	// lease says nothing about that gap.
	//
	// Usually instant. On a healthy cluster the state machine is already past
	// this index, which is what makes a lease read essentially free.
	if !m.waitForIndex(idx, within) {
		return "", false, errTooSlow
	}

	// TIME OF CHECK IS NOT TIME OF USE, and this is the honest half of the
	// bargain. The wait above can take arbitrarily long -- a slow state machine,
	// a descheduled goroutine, a stop-the-world pause -- and the lease may have
	// expired during it. Re-checking closes the window between the wait and the
	// read.
	//
	// It does NOT close the window between this line and the value actually
	// reaching the client. Nothing local can: a process suspended for a second
	// after this check returns data it had no right to serve, and it has no way
	// to find out. That residue is the assumption, not a bug to be fixed. See
	// DESIGN.md §9.
	if !time.Now().Before(until) {
		return "", false, errLeaseExpired
	}

	v, found := m.value(key)
	return v, found, nil
}

// readPreferringLease is the whole client-side read protocol: try the cheap
// path, fall back to the safe one.
//
// The fallback is what makes the lease a pure optimisation. Every reason
// ReadLease can refuse -- not leading, no majority heard from recently, nothing
// committed in this term -- is answered by the barrier, at the cost of a round
// trip. A caller that cannot tolerate the clock assumption calls readThrough
// directly and never comes here.
func readPreferringLease(n *Node, m *kvMachine, key string, within time.Duration) (string, bool, error) {
	if v, found, err := readLeased(n, m, key, within); err == nil {
		return v, found, nil
	}
	return readThrough(n, m, key, within)
}

// =============================================================================
// The gates
// =============================================================================

func TestALeaseIsRefusedByANonLeader(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(3)), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 3
	n.mu.Unlock()

	if _, _, ok := n.ReadLease(); ok {
		t.Error("a follower granted itself a read lease")
	}
}

// A lease needs evidence, and a leader that has just won has none yet: it has
// sent nothing and heard from nobody in this term.
func TestAFreshLeaderHasNoLeaseUntilAMajorityAnswers(t *testing.T) {
	n := NewNode(0, []int{1, 2, 3, 4}, newStubTransport(denyAll(5)), 1)
	t.Cleanup(n.Stop)

	// A stub whose AppendEntries never succeeds: nobody ever answers.
	n.mu.Lock()
	n.currentTerm = 5
	n.log = append(n.log, LogEntry{Term: 5, Command: []byte("x")})
	n.state = Leader
	n.leaderID = n.id
	n.initLeaderState()
	n.commitIndex = 1 // pretend it committed, to isolate the contact gate
	n.mu.Unlock()

	if _, _, ok := n.ReadLease(); ok {
		t.Error("granted a lease having heard from nobody: the lease is not " +
			"derived from any evidence at all")
	}

	// One peer of four is not a majority.
	n.mu.Lock()
	n.noteContact(1, time.Now())
	n.mu.Unlock()

	if _, _, ok := n.ReadLease(); ok {
		t.Error("granted a lease on one peer of five: two are needed besides self")
	}

	// Two peers of four, with self, is three of five.
	n.mu.Lock()
	n.noteContact(2, time.Now())
	n.mu.Unlock()

	if _, _, ok := n.ReadLease(); !ok {
		t.Error("refused a lease with a majority heard from inside the window")
	}
}

// THE §5.4.2 GATE, and the one most likely to be dropped as an over-refinement.
//
// A leader that has not committed in its own term has not committed the entries
// its PREDECESSOR committed either -- the current-term restriction forbids
// counting them -- so its state machine is missing writes that were
// acknowledged to a client. A lease read there is confidently wrong about data
// the cluster considers durable, with no partition and no clock skew involved.
func TestALeaseIsRefusedUntilSomethingCommitsInThisTerm(t *testing.T) {
	c, machines := failoverCluster(t, 5, 91)

	leader := c.waitForStableCluster(readBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}
	_, oldTerm, _ := c.nodes[leader].snapshotState()

	idx, _, isLeader := c.nodes[leader].Submit(encodePut("k", "inherited"))
	if !isLeader {
		t.Fatal("the leader stopped leading before the setup write")
	}
	for i, m := range machines {
		if !m.waitForIndex(idx, readBound) {
			t.Fatalf("node %d never applied the setup write", i)
		}
	}

	c.kill(leader)

	fresh := c.waitForLeaderAmong(c.othersThan(leader), oldTerm+1, 2*readBound)
	if fresh == None {
		t.Fatalf("no new leader within %v: %s", 2*readBound, c.describe())
	}

	// The new leader is heartbeating happily and hearing from a majority, so it
	// has contact evidence. It still must not serve a lease read: everything it
	// holds committed was committed by its predecessor.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, _, ok := c.nodes[fresh].ReadLease(); ok {
			t.Fatal("a fresh leader granted a lease before committing anything " +
				"in its own term: its commitIndex does not yet cover the entries " +
				"its predecessor acknowledged")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// One current-term entry releases the prefix, and with it the lease.
	writeIdx, _, _, ok := submitToLiveLeader(c, encodePut("k2", "own-term"), readBound)
	if !ok {
		t.Fatalf("no leader accepted a write within %v: %s", readBound, c.describe())
	}
	if !machines[fresh].waitForIndex(writeIdx, readBound) {
		t.Fatal("the new leader never applied its own write")
	}

	got, found, err := readLeased(c.nodes[fresh], machines[fresh], "k", readBound)
	if err != nil {
		t.Fatalf("lease read after a current-term commit: %v", err)
	}
	if !found || got != "inherited" {
		t.Errorf("lease read returned %q, want %q: the inherited entry was "+
			"released by the current-term commit but the read did not see it",
			got, "inherited")
	}
}

// =============================================================================
// The property
// =============================================================================

// READ YOUR WRITES, through the cheap path.
func TestALeaseReadSeesAWriteItAcknowledged(t *testing.T) {
	c, machines := failoverCluster(t, 3, 93)

	leader := c.waitForStableCluster(readBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}

	for i, val := range []string{"one", "two", "three"} {
		idx, _, isLeader := c.nodes[leader].Submit(encodePut("k", val))
		if !isLeader {
			t.Fatalf("write %d: node %d stopped leading", i, leader)
		}
		if !machines[leader].waitForIndex(idx, readBound) {
			t.Fatalf("write %d never applied", i)
		}

		got, found, err := readLeased(c.nodes[leader], machines[leader], "k", readBound)
		if err != nil {
			t.Fatalf("lease read after write %d: %v", i, err)
		}
		if !found || got != val {
			t.Errorf("lease read after write %d returned %q, want %q", i, got, val)
		}
	}
}

// Both paths must answer identically on a healthy cluster. An optimisation that
// is faster and disagrees is not an optimisation.
func TestTheLeaseAndBarrierPathsAgree(t *testing.T) {
	c, machines := failoverCluster(t, 5, 95)

	leader := c.waitForStableCluster(readBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}

	for i := 0; i < 20; i++ {
		val := string(rune('a' + i))

		idx, _, isLeader := c.nodes[leader].Submit(encodePut("k", val))
		if !isLeader {
			t.Fatalf("write %d: node %d stopped leading", i, leader)
		}
		if !machines[leader].waitForIndex(idx, readBound) {
			t.Fatalf("write %d never applied", i)
		}

		leased, _, err := readLeased(c.nodes[leader], machines[leader], "k", readBound)
		if err != nil {
			t.Fatalf("round %d: lease read: %v", i, err)
		}
		barrier, _, err := readThrough(c.nodes[leader], machines[leader], "k", readBound)
		if err != nil {
			t.Fatalf("round %d: barrier read: %v", i, err)
		}

		if leased != barrier {
			t.Fatalf("round %d: lease read %q, barrier read %q", i, leased, barrier)
		}
		if leased != val {
			t.Fatalf("round %d: both paths returned %q, want %q", i, leased, val)
		}
	}
}

// =============================================================================
// The assumption
// =============================================================================

// THE HEADLINE TEST. It has two halves and the second is the point.
//
// First: a leader cut off from a majority loses its lease, and loses it before
// any replacement can be elected. That is the safety property, and it holds
// because leaseDuration is strictly below electionTimeoutMin.
//
// Second: the counterfactual. The same node, at the same instant, holds stale
// data and satisfies every gate ReadLease applies EXCEPT the clock. Had its
// clock run slow by more than the drift allowance -- or had the process been
// descheduled across the window -- it would have believed the lease was still
// live and served that stale value.
//
// This is what makes the bound load-bearing rather than decorative. The barrier
// path's guarantee is structural: a deposed leader CANNOT get a majority to
// accept an entry. The lease path's guarantee is an assumption about hardware,
// and the difference is exactly the value below.
func TestTheLeaseBoundIsTheOnlyThingPreventingAStaleRead(t *testing.T) {
	c, machines := failoverCluster(t, 5, 97)

	stale := c.waitForStableCluster(readBound)
	if stale == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}
	_, staleTerm, _ := c.nodes[stale].snapshotState()

	// A current-term write, so the §5.4.2 gate is satisfied and the clock is
	// genuinely the only thing left standing.
	idx, _, isLeader := c.nodes[stale].Submit(encodePut("k", "old"))
	if !isLeader {
		t.Fatal("the leader stopped leading before the setup write")
	}
	for i, m := range machines {
		if !m.waitForIndex(idx, readBound) {
			t.Fatalf("node %d never applied the setup write", i)
		}
	}

	// The lease is live right now, by construction.
	if _, _, ok := c.nodes[stale].ReadLease(); !ok {
		t.Fatal("no lease on a healthy leader that just committed: the scenario " +
			"cannot demonstrate anything about losing one")
	}

	others := c.othersThan(stale)
	minority, majority := others[:1], others[1:]
	c.net.partition(append([]int{stale}, minority...), majority)

	fresh := c.waitForLeaderAmong(majority, staleTerm+1, 2*readBound)
	if fresh == None {
		t.Fatalf("the majority never elected a leader within %v: %s",
			2*readBound, c.describe())
	}

	newIdx, _, isLeader := c.nodes[fresh].Submit(encodePut("k", "new"))
	if !isLeader {
		t.Fatalf("node %d stopped leading immediately after winning", fresh)
	}
	for _, id := range majority {
		if !machines[id].waitForIndex(newIdx, readBound) {
			t.Fatalf("node %d never applied the write from the new leader", id)
		}
	}

	// --- half one: the lease is gone ---------------------------------------

	if st, _, _ := c.nodes[stale].snapshotState(); st != Leader {
		t.Fatalf("node %d already stepped down, so this run never built the "+
			"scenario: %s", stale, c.describe())
	}
	if _, _, ok := c.nodes[stale].ReadLease(); ok {
		t.Errorf("node %d still holds a lease after a replacement was elected: "+
			"leaseDuration (%v) is not below electionTimeoutMin (%v)",
			stale, leaseDuration, electionTimeoutMin)
	}
	if _, _, err := readLeased(c.nodes[stale], machines[stale], "k", time.Second); err != errNoLease {
		t.Errorf("lease read on the deposed leader gave %v, want %v", err, errNoLease)
	}

	// --- half two: what the bound is protecting ----------------------------

	n := c.nodes[stale]
	n.mu.Lock()
	leading := n.state == Leader
	currentTermCommit := n.commitIndex > 0 && n.log[n.commitIndex].Term == n.currentTerm
	realExpiry := n.leaseExpiry()
	n.mu.Unlock()

	if !leading || !currentTermCommit {
		t.Fatal("the counterfactual needs every non-clock gate satisfied: this " +
			"node must still believe it leads and must have committed in its term")
	}

	// What a slow clock, or a descheduled process, would have bought.
	overlong := realExpiry.Add(electionTimeoutMax)
	if !time.Now().Before(overlong) {
		t.Fatal("the counterfactual window closed before it could be measured")
	}

	got, _ := machines[stale].value("k")
	if got != "old" {
		t.Fatalf("the deposed leader holds %q, want the stale %q: without stale "+
			"data on this node there is nothing to demonstrate", got, "old")
	}
	t.Logf("counterfactual: with a lease %v longer -- the drift allowance is "+
		"%d%% of a %v timeout -- this node would have served %q while the "+
		"cluster held %q",
		electionTimeoutMax, maxClockDriftPercent, electionTimeoutMin, got, "new")

	// And the fallback does the right thing regardless.
	if _, _, err := readPreferringLease(c.nodes[stale], machines[stale], "k", time.Second); err == nil {
		t.Error("the full protocol served a read from a deposed leader: the " +
			"barrier fallback did not refuse")
	}
}

// The lease expires on silence alone, with no election and no replacement
// involved. Isolating the timer from the rest of the scenario above.
//
// TWO ASSERTIONS, AND THE SPLIT IS THE POINT.
//
// The bound is arithmetic and is checked as arithmetic. The first draft of this
// test polled ReadLease until it refused and charged the elapsed time against
// leaseDuration -- which fails intermittently by exactly one poll interval,
// because a 2ms poll can only notice an expiry up to 2ms after it happens. It
// duly failed at 123.07ms against a 122.73ms bound, reporting a safety
// violation that was a measurement artefact.
//
// So the exact claim is made against leaseExpiry directly, and the poll is
// demoted to confirming the observable behaviour with a bound loose enough to
// absorb its own granularity.
func TestALeaseExpiresWhenAMajorityGoesSilent(t *testing.T) {
	const poll = 2 * time.Millisecond

	c, machines := failoverCluster(t, 5, 99)

	leader := c.waitForStableCluster(readBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}

	idx, _, isLeader := c.nodes[leader].Submit(encodePut("k", "v"))
	if !isLeader {
		t.Fatal("the leader stopped leading before the setup write")
	}
	if !machines[leader].waitForIndex(idx, readBound) {
		t.Fatal("the leader never applied its own write")
	}
	if _, _, ok := c.nodes[leader].ReadLease(); !ok {
		t.Fatal("no lease on a healthy leader that just committed")
	}

	// Cut the leader off entirely. It keeps sending and hears nothing back, so
	// lastContact stops advancing.
	n := c.nodes[leader]
	c.net.disconnect(leader)
	silentAt := time.Now()

	// --- the exact claim ---------------------------------------------------
	//
	// lastContact holds SEND times, and no message sent after the cut can be
	// answered -- route refuses it outright, and a reply to a message already
	// in flight is discarded on the return path. So every contact time is at or
	// before silentAt, and the expiry must land within one leaseDuration of it.
	n.mu.Lock()
	expiry := n.leaseExpiry()
	n.mu.Unlock()

	if held := expiry.Sub(silentAt); held > leaseDuration {
		t.Errorf("the lease expires %v after the cut, want at most %v: a contact "+
			"time was recorded after the network was severed", held, leaseDuration)
	}

	// --- the observable behaviour -------------------------------------------
	//
	// Bounded at twice the lease rather than at the lease itself, because this
	// one is charged the poll interval and whatever the scheduler adds. It is
	// checking that ReadLease starts refusing at all, not when.
	deadline := time.Now().Add(2 * leaseDuration)
	for {
		if _, _, ok := n.ReadLease(); !ok {
			t.Logf("lease refused %v after the cut; expiry was %v after it "+
				"(bound %v, poll %v)",
				time.Since(silentAt).Round(time.Millisecond),
				expiry.Sub(silentAt).Round(time.Microsecond), leaseDuration, poll)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the lease was still live %v after the leader was cut off, "+
				"want it gone within %v", 2*leaseDuration, leaseDuration)
		}
		time.Sleep(poll)
	}
}
