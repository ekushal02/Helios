package raft

import (
	"maps"
	"testing"
	"testing/synctest"
	"time"
)

// =============================================================================
// Phase G-2: proving the network layer replays exactly
// =============================================================================
//
// harness_test.go's own doc comment explains the bug this task exists to fix:
// fakeNetwork used to draw drop/delay decisions from one shared *rand.Rand,
// consumed in whatever order goroutines happened to reach fn.mu in -- a
// property of the Go scheduler, not of the seed. Pinning a failing seed and
// rerunning it did not reproduce the failure it named.
//
// Two tests below check the fix at two different levels. The first is a
// direct, no-concurrency check of the hash function itself: cheap, fast, and
// exact. The second is the actual claim this task makes, checked under real
// concurrent load -- a full scripted fault scenario against a real 5-node
// cluster, run twice at the same seed, each inside its own synctest bubble so
// real machine jitter cannot be the thing that makes two runs differ.

// TestNetworkDecisionIsPureFunctionOfIdentity checks the property
// route/replyDeliverable actually depend on: the SAME (seed, kind, from, to,
// seq) always yields the SAME decision, regardless of which fakeNetwork
// instance -- and therefore which goroutine, on which call -- is asking.
func TestNetworkDecisionIsPureFunctionOfIdentity(t *testing.T) {
	a := newFakeNetwork(20260902)
	b := newFakeNetwork(20260902)
	for _, fn := range []*fakeNetwork{a, b} {
		fn.setDropRate(0.4)
		fn.setReplyDropRate(0.3)
		fn.setDuplicateRate(0.2)
		fn.setReorderRate(0.25)
		fn.setDelayRange(1*time.Millisecond, 50*time.Millisecond)
	}

	kinds := []messageKind{kindRequestVote, kindPreVote, kindAppendEntries, kindInstallSnapshot}

	for seq := 0; seq < 500; seq++ {
		for _, kind := range kinds {
			if a.rollDrop(kind, 1, 4, seq) != b.rollDrop(kind, 1, 4, seq) {
				t.Fatalf("kind %d seq %d: drop decision disagreed between two independently-built networks sharing a seed", kind, seq)
			}
			if a.rollReplyDrop(kind, 1, 4, seq) != b.rollReplyDrop(kind, 1, 4, seq) {
				t.Fatalf("kind %d seq %d: reply-drop decision disagreed between two independently-built networks sharing a seed", kind, seq)
			}
			if a.rollDelay(kind, 1, 4, seq) != b.rollDelay(kind, 1, 4, seq) {
				t.Fatalf("kind %d seq %d: delay disagreed between two independently-built networks sharing a seed", kind, seq)
			}
			if a.rollDuplicate(kind, 1, 4, seq) != b.rollDuplicate(kind, 1, 4, seq) {
				t.Fatalf("kind %d seq %d: duplicate decision disagreed between two independently-built networks sharing a seed", kind, seq)
			}
			if a.rollReorderBoost(kind, 1, 4, seq) != b.rollReorderBoost(kind, 1, 4, seq) {
				t.Fatalf("kind %d seq %d: reorder-boost decision disagreed between two independently-built networks sharing a seed", kind, seq)
			}
		}
	}
}

// runScriptedFaultScenario drives a fixed sequence of faults against a real
// 5-node cluster built with the given seed: elect a leader, split it 3/2 with
// the leader in the majority, kill one minority node, hold, then heal. It
// returns the network's own decision trace -- what a replay is actually
// compared against -- plus a snapshot of cluster state at the end, the kind of
// observable a person debugging a real chaos failure would look at first.
func runScriptedFaultScenario(t *testing.T, seed int64) (map[traceKey]decisionRecord, string) {
	t.Helper()

	c := newCluster(t, 5, seed)
	c.net.setDropRate(0.25)
	c.net.setReplyDropRate(0.15)
	c.net.setDelayRange(1*time.Millisecond, 30*time.Millisecond)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("seed %d: no initial leader within %v: %s", seed, electionBound, c.describe())
	}

	others := c.othersThan(leader)
	minority := append([]int(nil), others[:2]...)
	majority := append([]int{leader}, others[2:]...)

	c.net.partition(majority, minority)
	time.Sleep(500 * time.Millisecond)

	c.kill(minority[0])
	time.Sleep(300 * time.Millisecond)

	c.net.heal()
	time.Sleep(500 * time.Millisecond)

	desc := c.describe()
	trace := c.net.decisionTrace()

	// Settle and stop the cluster BEFORE this function returns, explicitly --
	// not left to t.Cleanup(c.stop), which harness_test.go's newCluster
	// registers but which runs too late for synctest: the panic this fix
	// responds to happened the instant the bubble's root goroutine (this
	// function, via synctest.Test's callback) returned, before Cleanup ever
	// got a chance to run. Real, outside a bubble: a handful of goroutines
	// (an in-flight AppendEntries, a heartbeat mid-send) always outlive a
	// test's own return by a few real milliseconds, harmlessly -- see
	// testlog_test.go's testWriter doc comment, which names this exact
	// pattern. Inside a bubble it isn't harmless: synctest requires every
	// goroutine to be finished or durably blocked on something that will
	// still eventually unblock once the root returns, and Node.Stop() (via
	// c.stop) ends the election/heartbeat TICKERS immediately but does not
	// cancel a round already past its own ticker dispatch, mid
	// endpoint.SendAppendEntries's own time.Sleep(delay) -- kill's own doc
	// comment says as much ("there is no process to exit and no socket to
	// close"). c.stop() first, so no FURTHER rounds start; then one more
	// sleep, comfortably longer than this scenario's own setDelayRange
	// ceiling and the extra round replicateRound's own pending-flag can still
	// trigger once after stop (replication.go: "one message for everything
	// that arrived while the last one was out"), to let any such stragglers
	// actually finish before the bubble's root goroutine exits.
	c.stop()
	time.Sleep(500 * time.Millisecond)

	return trace, desc
}

// TestChaosScenarioReplaysExactly is the claim this task exists to prove: the
// identical seed, run through the identical scripted scenario twice, produces
// a byte-identical network decision trace both times -- deep-equal, not just
// similarly shaped -- regardless of real goroutine scheduling. Each run
// happens inside its own synctest bubble, which is what removes real machine
// jitter (GC pauses, CI load) from the picture: inside a bubble, time.Sleep
// and every timer Node's own ticker/heartbeat loops already use (election.go,
// heartbeat.go -- unmodified by this task, see DESIGN.md §24) run on a virtual
// clock that only advances when every goroutine in the bubble is durably
// blocked, so two events that are 1ms apart in virtual time stay 1ms apart
// regardless of how fast or slow the real machine running the test is.
//
// What this test does NOT claim: that every possible Raft schedule is
// reproducible, only that this network layer's own fault injection is. Two
// goroutines becoming runnable at the exact same virtual instant can still be
// scheduled in either order by the Go runtime even inside a synctest bubble --
// a real, separate property this task does not attempt, recorded in
// DESIGN.md §24 as its own open question rather than silently assumed solved.
func TestChaosScenarioReplaysExactly(t *testing.T) {
	const seed = 20260902917

	var traces []map[traceKey]decisionRecord
	var descriptions []string

	for run := 0; run < 2; run++ {
		synctest.Test(t, func(t *testing.T) {
			trace, desc := runScriptedFaultScenario(t, seed)
			traces = append(traces, trace)
			descriptions = append(descriptions, desc)
		})
	}

	if len(traces[0]) == 0 {
		t.Fatalf("scenario produced an empty decision trace; the scenario is not exercising the network at all")
	}

	if !maps.Equal(traces[0], traces[1]) {
		t.Fatalf("network decisions differed between two replays of seed %d (%d vs %d decisions recorded)\nrun 1 final state: %s\nrun 2 final state: %s",
			seed, len(traces[0]), len(traces[1]), descriptions[0], descriptions[1])
	}
}