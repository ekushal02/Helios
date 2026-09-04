package raft

import (
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Phase G-6: clock skew, including a leader whose clock jumps backwards
// =============================================================================
//
// Every other Node timer -- election, heartbeat -- still calls time.Now()
// directly, and stays that way: DESIGN.md §23 already established that
// testing.synctest virtualizes those for free, for any test that runs
// inside a bubble, with zero production code changes. What a bubble
// structurally cannot do is give ONE node a clock that diverges from its
// peers': every goroutine inside one bubble shares the identical virtual
// clock. A clock-SKEW fault is a claim about exactly that divergence, so
// testing it needed a seam synctest cannot provide -- clock.go's new clock
// interface, used ONLY on the lease-critical path (ReadLease, leaseExpiry,
// noteContact), where a wrong answer from Now() is a SAFETY bug rather than
// the liveness-only kind every other timer in this package risks.
//
// fakeClock, below, is that seam's test-only half: a clock a test can Set,
// Advance, or Rewind on demand, injected onto one specific Node by
// assigning its own n.clock field directly (an unexported field, same
// package, no change to NewNode's or OpenNode's own public signatures).
//
// WHAT THIS FOUND, NOT JUST TESTED. Building this surfaced a real,
// PERSISTENT vulnerability in the lease mechanism as it stood, fixed in the
// same commit as this test rather than left as a documented gap:
// noteContact's own monotonicity guard -- "if sentAt.After(n.lastContact[peer])",
// there specifically to stop a late reply to an old message from dragging
// the lease backwards -- means that once a leader's clock jumps backward,
// EVERY sentAt recorded after the jump is, numerically, smaller than
// whatever was recorded before it, so the guard refuses every single one of
// them, forever. lastContact freezes at its pre-jump values, leaseExpiry
// keeps computing an expiry from those frozen values, and clock.Now() --
// also past the jump, so also smaller -- keeps satisfying
// now.Before(until) against that frozen expiry, indefinitely. To the node
// itself, everything looks internally consistent. What it cannot see is
// that real wall-clock time, and every OTHER node's own unrolled-back
// clock, kept moving the entire time -- a new leader can be elected and
// commit writes on the far side of that gap while this node still believes
// its lease is live. Fixed with detectClockRollback (read.go): a clock
// observation earlier than the highest ever seen on the lease-critical path
// clears every recorded lastContact, forcing the lease to be re-earned
// exactly the way a freshly-elected leader already has to.
// TestReadLeaseIsSafeWhenTheLeadersClockJumpsBackward, below, is both the
// proof the fix works and -- with the fix commented out, which the
// change's own commit message records having done as part of verifying
// this -- the demonstration that it was a real bug, not a theoretical one.

// fakeClock lets a test control exactly what one Node's own lease-critical
// Now() calls return, including making them jump BACKWARDS -- the one thing
// a real clock, by definition, never does, and exactly the fault this task
// exists to inject. Safe for concurrent use: noteContact and ReadLease both
// read it from goroutines that do not hold n.mu at the point they call
// clock.Now() (sentAt is stamped before the send, exactly as it is with the
// real clock -- see sendAppendEntries and sendInstallSnapshot).
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Set moves this clock to exactly t, in either direction -- the general
// primitive Rewind and Advance are both built from.
func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// Advance moves this clock forward by d.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Rewind moves this clock BACKWARD by d -- the one operation a real clock
// can never safely be asked to do, and the specific fault this file's own
// tests inject.
func (c *fakeClock) Rewind(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(-d)
}

// TestReadLeaseIsSafeWhenTheLeadersClockJumpsBackward is the headline test
// this task exists to write: the EXACT scenario
// TestTheLeaseBoundIsTheOnlyThingPreventingAStaleRead already builds --
// deposed leader, stale data, cut off from a majority that elected and
// committed through a replacement -- but forcing the clock fault directly
// rather than only reasoning about it as a counterfactual. Where that test
// asks "what would a slow clock or a descheduled process have bought,"
// this one actually rewinds the deposed leader's own clock, past its own
// recorded lastContact entries, and checks that ReadLease still refuses --
// not because the drift bound happened to hold, but because a rollback
// this large is detected and handled directly.
func TestReadLeaseIsSafeWhenTheLeadersClockJumpsBackward(t *testing.T) {
	c, machines := failoverCluster(t, 5, 20260904)

	stale := c.waitForStableCluster(readBound)
	if stale == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}
	_, staleTerm, _ := c.nodes[stale].snapshotState()

	// Inject the fake clock on the leader that is about to be deposed, BEFORE
	// the scenario does anything that would establish a lease under it --
	// so every lastContact entry the real setup produces is timestamped by
	// THIS clock, not the real one, and Rewind later has something genuine
	// to invalidate.
	fc := newFakeClock(time.Now())
	n := c.nodes[stale]
	n.mu.Lock()
	n.clock = fc
	n.mu.Unlock()

	idx, _, isLeader := c.nodes[stale].Submit(encodePut("k", "old"))
	if !isLeader {
		t.Fatal("the leader stopped leading before the setup write")
	}
	for i, m := range machines {
		if !m.waitForIndex(idx, readBound) {
			t.Fatalf("node %d never applied the setup write", i)
		}
	}

	if _, _, ok := c.nodes[stale].ReadLease(); !ok {
		t.Fatal("no lease on a healthy leader that just committed: the scenario " +
			"cannot demonstrate anything about losing one")
	}

	others := c.othersThan(stale)
	minority, majority := others[:1], others[1:]
	c.net.partition(append([]int{stale}, minority...), majority)

	fresh := c.waitForLeaderAmong(majority, staleTerm+1, 2*readBound)
	if fresh == None {
		t.Fatalf("the majority never elected a leader within %v: %s", 2*readBound, c.describe())
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

	if st, _, _ := c.nodes[stale].snapshotState(); st != Leader {
		t.Fatalf("node %d already stepped down, so this run never built the scenario: %s",
			stale, c.describe())
	}

	// THE FAULT: rewind the deposed leader's own clock well past every
	// lastContact entry it holds -- an hour is many orders of magnitude
	// larger than leaseDuration (well under a second), so this is not a
	// borderline case; it is the same shape of discontinuity a live
	// migration or a buggy NTP step could produce.
	fc.Rewind(time.Hour)

	// THE CLAIM: even now, with the deposed leader's own clock insisting far
	// less time has passed than actually has, it must not believe its lease
	// is live. Before the fix, it would have: leaseExpiry keeps computing an
	// expiry from lastContact entries frozen by noteContact's own
	// monotonicity guard, and the rewound Now() keeps satisfying
	// now.Before(until) against that frozen expiry.
	if _, _, ok := c.nodes[stale].ReadLease(); ok {
		t.Fatal("node still holds a lease after its own clock jumped an hour backward: " +
			"a stale read is servable right now")
	}
	if _, _, err := readLeased(c.nodes[stale], machines[stale], "k", time.Second); err != errNoLease {
		t.Errorf("lease read after a backward clock jump gave %v, want %v", err, errNoLease)
	}

	// THE PERSISTENCE CHECK -- not just "refused once," but "recovers
	// correctly, not merely fails forever by accident." A NEW round of
	// contact, timestamped by the now-rewound (but internally consistent
	// again) clock, must be able to re-earn a lease from scratch -- proving
	// detectClockRollback actually cleared the frozen entries rather than
	// leaving the node permanently wedged. The deposed leader is still cut
	// off from the majority, so it cannot actually complete a majority round
	// here; what's checked is the narrower, still load-bearing claim that
	// leaseExpiry no longer reflects the pre-rollback contacts.
	n.mu.Lock()
	_, hasContact := n.lastContact[minority[0]]
	for _, id := range majority {
		if _, ok := n.lastContact[id]; ok {
			hasContact = true
		}
	}
	n.mu.Unlock()
	if hasContact {
		t.Error("lastContact still holds a pre-rollback entry after the rollback was detected: " +
			"detectClockRollback should have cleared all of them")
	}
}

// TestNoteContactAloneNeverClearsLastContact locks in the corrected design:
// detectClockRollback is called ONLY from ReadLease, never from noteContact
// -- see detectClockRollback's own doc for why an earlier version that
// called it from both broke healthy leaders in real operation. A rollback
// presented to noteContact alone, with no intervening ReadLease call, must
// NOT clear anything; only a subsequent ReadLease call, whose own now :=
// n.clock.Now() is genuinely ordered by n.mu, is allowed to.
func TestNoteContactAloneNeverClearsLastContact(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", electionBound, c.describe())
	}
	n := c.nodes[leader]
	fc := newFakeClock(time.Now())

	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		t.Fatalf("node %d already stopped leading before the fake clock could be installed", leader)
	}
	n.clock = fc
	for _, p := range n.peers {
		n.noteContact(p, fc.Now())
	}
	n.mu.Unlock()

	fc.Rewind(time.Hour)

	// noteContact alone, repeatedly, with no ReadLease call in between --
	// the pre-rollback entries must survive untouched. noteContact's own
	// per-peer guard will simply refuse to update them with the smaller,
	// post-rollback sentAt values, which is correct and expected; what this
	// test guards against is noteContact ALSO clearing them as a side
	// effect, which is exactly the regression this design change fixed.
	for i := 0; i < 5; i++ {
		n.mu.Lock()
		for _, p := range n.peers {
			n.noteContact(p, fc.Now())
		}
		_, ok0 := n.lastContact[n.peers[0]]
		_, ok1 := n.lastContact[n.peers[1]]
		n.mu.Unlock()
		if !ok0 || !ok1 {
			t.Fatalf("round %d: a lastContact entry was cleared by noteContact alone, "+
				"with no ReadLease call -- detectClockRollback must only ever be reachable "+
				"from ReadLease", i)
		}
	}
}

// TestOutOfOrderPeerRepliesDoNotClearLastContact is the most direct
// reproduction of the actual regression: two peers' sentAt values, recorded
// out of the order they were captured in -- exactly what replicateAll's own
// one-goroutine-per-peer fan-out produces whenever the peer that left LATER
// happens to answer FIRST. Neither noteContact call may clear anything;
// this is ordinary scheduling variance, not a clock rollback, and
// noteContact's own per-peer guard already handles it correctly on its own.
func TestOutOfOrderPeerRepliesDoNotClearLastContact(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", electionBound, c.describe())
	}
	n := c.nodes[leader]
	fc := newFakeClock(time.Now())

	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		t.Fatalf("node %d already stopped leading before the fake clock could be installed", leader)
	}
	n.clock = fc
	n.mu.Unlock()

	if len(n.peers) < 2 {
		t.Fatalf("need at least 2 peers to reproduce an out-of-order arrival, have %d", len(n.peers))
	}
	peerLate, peerEarly := n.peers[0], n.peers[1]

	// peerLate's own message leaves LATER (a bigger sentAt)...
	laterSentAt := fc.Now()
	fc.Advance(5 * time.Millisecond)
	// ...but peerEarly's own message, sent EARLIER, is the one whose reply
	// noteContact happens to see FIRST -- its own round trip simply took
	// longer. This is the exact interleaving that broke a healthy leader
	// when detectClockRollback was still reachable from noteContact.
	earlierSentAt := laterSentAt.Add(-5 * time.Millisecond)

	n.mu.Lock()
	n.noteContact(peerLate, laterSentAt)
	n.noteContact(peerEarly, earlierSentAt)
	_, ok1 := n.lastContact[peerLate]
	_, ok2 := n.lastContact[peerEarly]
	n.mu.Unlock()

	if !ok1 || !ok2 {
		t.Fatal("an out-of-order (but perfectly ordinary) pair of peer replies cleared lastContact -- " +
			"this is the exact shape of the real regression detectClockRollback's own doc comment records")
	}
}

// TestClockRollbackDoesNotFalseTriggerOnOrdinaryOperation is the negative
// case every fault-detection mechanism in this project gets: normal
// operation, clock advancing forward exactly as a real one does, must never
// trip the rollback detector and clear lastContact for no reason -- that
// would just be a self-inflicted, permanent lease outage with an extra
// step.
func TestClockRollbackDoesNotFalseTriggerOnOrdinaryOperation(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", electionBound, c.describe())
	}
	n := c.nodes[leader]
	fc := newFakeClock(time.Now())

	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		t.Fatalf("node %d already stopped leading before the fake clock could be installed", leader)
	}
	n.clock = fc
	n.mu.Unlock()

	for i := 0; i < 50; i++ {
		fc.Advance(time.Millisecond)
		n.mu.Lock()
		for _, p := range n.peers {
			n.noteContact(p, fc.Now())
		}
		_, ok1 := n.lastContact[n.peers[0]]
		_, ok2 := n.lastContact[n.peers[1]]
		n.mu.Unlock()
		if !ok1 || !ok2 {
			t.Fatalf("round %d: a lastContact entry went missing during ordinary forward-only "+
				"clock advancement -- the rollback detector false-triggered", i)
		}
	}
}