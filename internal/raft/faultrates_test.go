package raft

import (
	"testing"
	"time"
)

// =============================================================================
// Phase G-4: message drop, duplication, reordering, and delay -- each with
// its own configurable rate
// =============================================================================
//
// drop (dropRate, replyDropRate) and delay (minDelay, maxDelay) already
// existed, checked directly by network_test.go's own tests since well before
// this task. Reordering was already MEASURED (arrive's own reordered
// counter, exercised by TestReorderCountIsSendOrderVersusArrivalOrder) but
// only ever produced as delay's own incidental side effect, with no rate of
// its own to dial. Duplication did not exist at all -- a message was never
// delivered more than once. This file is the tests for the two genuinely
// new, independently-configurable fault types harness_test.go's own route()
// now injects: setDuplicateRate and setReorderRate, alongside the
// pre-existing setDropRate/setReplyDropRate/setDelayRange.

// TestDuplicateRateActuallyDeliversTwice is the direct, no-cluster check:
// with duplicateRate=1.0, every successful route() call is recorded as
// duplicated in the decision trace, and the target actually receives the
// RPC twice -- not merely "decided to," which decisionTrace alone would only
// prove was intended. appendStats' own message count is the second,
// independent confirmation: it increments once per REAL delivery to a
// target (harness_test.go's own SendAppendEntries), so with every message
// duplicated it must read double the number of successful routes.
func TestDuplicateRateActuallyDeliversTwice(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no initial leader within %v: %s", electionBound, c.describe())
	}
	// decisionTrace() accumulates for the cluster's whole lifetime, never
	// reset -- unlike resetCounters()'s own counters, which only cover
	// what happens after they're reset. Snapshotted here, before
	// duplicateRate is even set, so the check below can look at exactly
	// what's NEW rather than also re-examining the initial election's own
	// (legitimately non-duplicated) sends.
	before := c.net.decisionTrace()
	c.net.resetCounters()
	c.net.setDuplicateRate(1.0)

	// Drive a handful of real AppendEntries rounds by submitting writes --
	// heartbeats alone would also do it, but a real write exercises the
	// exact RPC kind a duplicate is most consequential for (see the
	// idempotency argument in setDuplicateRate's own doc).
	for i := 0; i < 5; i++ {
		if _, _, ok := c.nodes[leader].Submit([]byte("x")); !ok {
			t.Fatalf("Submit on node %d: not leader", leader)
		}
	}
	time.Sleep(300 * time.Millisecond)

	appendSends := 0
	for k, rec := range c.net.decisionTrace() {
		if _, seenBefore := before[k]; seenBefore {
			continue // predates duplicateRate being set at all
		}
		if k.kind != kindAppendEntries || rec.requestDropped {
			continue
		}
		appendSends++
		if !rec.duplicated {
			t.Errorf("AppendEntries %+v: duplicated = false at duplicateRate=1.0, want true", k)
		}
	}
	if appendSends == 0 {
		t.Fatal("no successful AppendEntries sends recorded after duplicateRate was set -- the scenario did not exercise anything")
	}

	msgs, _ := c.net.appendStats()
	if msgs < 2*appendSends {
		t.Errorf("appendStats() reports %d AppendEntries deliveries for %d successful routes, want at least %d (each duplicated once)",
			msgs, appendSends, 2*appendSends)
	}
}

// TestDuplicateRateZeroNeverDuplicates is setDuplicateRate's own zero-value
// contract, checked directly: no field defaults to "on" by construction (the
// same standard rollDrop/rollReplyDrop already hold themselves to via their
// own `if fn.xRate <= 0 { return false }` guard).
func TestDuplicateRateZeroNeverDuplicates(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no initial leader within %v: %s", electionBound, c.describe())
	}
	// duplicateRate is left at its zero value deliberately -- not set at all.
	for i := 0; i < 10; i++ {
		c.nodes[leader].Submit([]byte("x"))
	}
	time.Sleep(300 * time.Millisecond)

	for k, rec := range c.net.decisionTrace() {
		if rec.duplicated {
			t.Errorf("message %+v: duplicated = true with duplicateRate never set, want false", k)
		}
	}
}

// TestDuplicateVoteDoesNotGrantTwice is the actual correctness property
// duplication exists to exercise, not just the delivery mechanism: a
// candidate's own RequestVote, delivered twice to the same follower,
// must not somehow count for two votes -- RequestVoteGrantingRules' own
// "grant: already voted for THIS candidate" branch (rpc_test.go) is what
// makes the SECOND delivery a no-op rather than a double grant, and this
// test confirms that holds end to end, through the real transport, not just
// unit-tested against the handler directly.
func TestDuplicateVoteDoesNotGrantTwice(t *testing.T) {
	c := newCluster(t, 3, 2)
	c.net.setDuplicateRate(1.0)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no initial leader within %v: %s", electionBound, c.describe())
	}
	// A single-leader outcome, with a real term won under -race-clean
	// concurrent RequestVote duplication throughout the whole election, is
	// itself the property: Majority() counting a duplicated grant as two
	// votes would show up as either a split-brain (two leaders in the same
	// term) or a term that won with fewer than a true majority's worth of
	// distinct grants -- checkSingleLeader's own majority check already
	// catches either.
	if got := c.checkSingleLeader(); got != leader {
		t.Errorf("checkSingleLeader() = %d after the election, want %d (a duplicated vote must not have let a second node also win)", got, leader)
	}
}

// TestReorderRateIncreasesObservedReordering is the distributional check
// rollReorderBoost's own doc promises: a reorder RATE is a bias, not a
// guarantee for any one message, so this is checked the same way
// TestElectionTimeoutsAreRandomised checks randomization -- across many
// draws, not one -- comparing a boosted network's own reorderedCount()
// against an unboosted control built from the identical seed and delay
// range, so the only variable that differs is reorderRate itself.
func TestReorderRateIncreasesObservedReordering(t *testing.T) {
	const trials = 400
	// sendInterval stands in for how far apart, in real time, this pair's
	// own successive sends actually land -- small relative to the delay
	// range below, the same "messages sent close together, in a short
	// burst" shape a real heartbeat round or election fan-out produces,
	// so delay is what actually decides arrival order, exactly as it
	// would for a real endpoint's own time.Sleep(delay) before arrive().
	const sendInterval = 10 * time.Microsecond

	baseline := newCluster(t, 2, 9).net
	boosted := newCluster(t, 2, 9).net
	for _, fn := range []*fakeNetwork{baseline, boosted} {
		fn.setDelayRange(1*time.Millisecond, 5*time.Millisecond)
	}
	boosted.setReorderRate(0.5)

	deliverInSendOrder := func(fn *fakeNetwork) int {
		type delivery struct {
			seq     int
			arrival time.Duration
		}
		order := make([]delivery, 0, trials)
		for i := 0; i < trials; i++ {
			_, delay, seq, _, ok := fn.route(kindAppendEntries, 0, 1)
			if !ok {
				t.Fatalf("route: unexpected drop in a scenario with no dropRate set")
			}
			order = append(order, delivery{seq, time.Duration(seq)*sendInterval + delay})
		}
		// Deliver in ARRIVAL order -- what a real endpoint's own
		// time.Sleep(delay) followed by arrive() would produce -- not
		// send order, which is exactly the distinction reordering is
		// about.
		for i := 1; i < len(order); i++ {
			for j := i; j > 0 && order[j].arrival < order[j-1].arrival; j-- {
				order[j], order[j-1] = order[j-1], order[j]
			}
		}
		for _, d := range order {
			fn.arrive(0, 1, d.seq)
		}
		return fn.reorderedCount()
	}

	baseCount := deliverInSendOrder(baseline)
	boostedCount := deliverInSendOrder(boosted)

	t.Logf("reordered count over %d sends: baseline (reorderRate=0) = %d, boosted (reorderRate=0.5) = %d", trials, baseCount, boostedCount)
	if boostedCount <= baseCount {
		t.Errorf("reorderRate=0.5 produced %d reorders, want more than the unboosted baseline's %d", boostedCount, baseCount)
	}
}

// TestReorderRateZeroMatchesPreExistingBehaviour confirms setReorderRate's
// own zero value changes nothing -- the same backward-compatibility
// standard every other new rate in this file (and duplicateRate before it)
// is held to: a test written before this task, never calling
// setReorderRate at all, must see byte-identical decisions to one that
// calls it with 0 explicitly.
func TestReorderRateZeroMatchesPreExistingBehaviour(t *testing.T) {
	unset := newCluster(t, 2, 77).net
	explicit := newCluster(t, 2, 77).net
	explicit.setReorderRate(0)
	for _, fn := range []*fakeNetwork{unset, explicit} {
		fn.setDelayRange(1*time.Millisecond, 20*time.Millisecond)
	}

	for seq := 0; seq < 200; seq++ {
		if got, want := unset.rollDelay(kindAppendEntries, 0, 1, seq), explicit.rollDelay(kindAppendEntries, 0, 1, seq); got != want {
			t.Fatalf("seq %d: delay = %v with reorderRate unset, %v with it explicitly 0 -- should be identical", seq, got, want)
		}
	}
}
