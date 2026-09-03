package raft

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// =============================================================================
// Phase G-3: network partition of arbitrary node subsets, on and off, at
// controlled times
// =============================================================================
//
// Every existing partition test in this package (partition_test.go,
// prevote_test.go) hand-writes its own schedule: call fn.partition(...),
// time.Sleep(some duration), call fn.heal(), inline, once, in the test's own
// body. That's the right amount of machinery for a test that partitions the
// cluster exactly once. It stops being enough the moment a scenario wants
// several INDEPENDENT fault windows, on DIFFERENT arbitrary subsets, whose
// time windows may overlap -- fn.partition/fn.heal both replace the WHOLE
// topology at once, so "isolate {0} for 200ms, and ALSO isolate {2,3} for a
// window that overlaps it" cannot be expressed with them: whichever heal call
// runs first would reopen a link the other fault still needs cut.
//
// faultInjector is the declarative layer that closes this: a schedule of
// (subset, start, duration) entries, each backed by isolateSubset/
// restoreSubset (harness_test.go) rather than partition/heal, so overlapping
// entries compose correctly (cutRefs' own doc explains the reference-counted
// mechanism that makes this safe). It is deliberately built from ordinary
// goroutines and time.Sleep -- no bespoke timer, no polling loop -- which is
// what makes it automatically synctest-compatible (§23): a schedule run
// inside a synctest bubble gets virtual-time, real-machine-jitter-free
// scheduling for free, exactly the way Node's own election/heartbeat timers
// already do, with zero changes to this file needed to get it.

// faultSchedule is one entry in a faultInjector's own plan: isolate Subset
// from the rest of the cluster, starting At (relative to the moment Run is
// called), for Duration, then restore it automatically. Duration must be
// positive -- a schedule always says both when a fault starts AND when it
// ends; a fault meant to outlive the whole scenario is simpler to express by
// calling net.isolateSubset directly and never restoring it, not by asking
// this type to special-case "forever."
type faultSchedule struct {
	Subset   []int
	At       time.Duration
	Duration time.Duration
}

// faultInjector runs a fixed faultSchedule against a fakeNetwork: every
// entry gets its own goroutine, sleeping until its own At, isolating its own
// Subset, sleeping its own Duration, then restoring it -- independently of
// every other entry, which is what lets two entries' own windows overlap in
// time without either one's restore reopening a link the other still needs
// cut (isolateSubset/restoreSubset's own doc explains the mechanism; this
// type only ever calls them, never touches fn.reachable directly).
type faultInjector struct {
	net    *fakeNetwork
	stopCh chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func newFaultInjector(net *fakeNetwork) *faultInjector {
	return &faultInjector{net: net, stopCh: make(chan struct{})}
}

// Run starts every schedule entry on its own goroutine and returns
// immediately -- the caller decides how long to wait (Wait, typically past
// the last entry's own At+Duration) before inspecting cluster state, or cuts
// the whole schedule short early (Stop).
func (fi *faultInjector) Run(schedule []faultSchedule) {
	for _, s := range schedule {
		fi.wg.Add(1)
		go fi.runOne(s)
	}
}

func (fi *faultInjector) runOne(s faultSchedule) {
	defer fi.wg.Done()

	if !fi.sleep(s.At) {
		return // Stop fired before this entry's own fault ever started
	}
	fi.net.isolateSubset(s.Subset)
	// The Duration sleep's own return value is deliberately ignored: the
	// fault is restored whether it ran its full course or Stop cut it
	// short. Either way, isolateSubset already ran above, so restoreSubset
	// MUST run too -- the one invariant Stop's own doc promises: nothing
	// this type ever isolated is left isolated once every goroutine it
	// started has actually returned.
	fi.sleep(s.Duration)
	fi.net.restoreSubset(s.Subset)
}

// sleep waits for d or until Stop is called, whichever comes first,
// reporting whether the full duration elapsed.
func (fi *faultInjector) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-fi.stopCh:
		return false
	}
}

// Wait blocks until every schedule entry has both applied and restored its
// own isolation naturally. Unlike Stop, it never cuts anything short.
func (fi *faultInjector) Wait() {
	fi.wg.Wait()
}

// Stop cuts the schedule short: any entry still waiting for its own At never
// isolates its subset at all, and any entry currently mid-isolation restores
// it immediately rather than waiting out its own remaining Duration -- the
// same "nothing outlives the caller" discipline cluster.stop and
// compaction.Background.Stop already hold themselves to (§13.9). Safe to
// call more than once; blocks until every entry's own goroutine has actually
// returned, so a caller that calls Stop and then inspects fn.reachable is
// guaranteed to see every isolation this injector made already reverted, not
// merely requested to be.
func (fi *faultInjector) Stop() {
	fi.once.Do(func() { close(fi.stopCh) })
	fi.wg.Wait()
}

// =============================================================================
// Tests
// =============================================================================

// TestIsolateSubsetCutsOnlyCrossingLinks is the direct, no-injector check of
// the primitive itself: exactly the links between the subset and everyone
// else are cut, in both directions; links entirely within the subset and
// links entirely within the rest of the cluster are untouched.
func TestIsolateSubsetCutsOnlyCrossingLinks(t *testing.T) {
	c := newCluster(t, 5, 1)
	fn := c.net

	fn.isolateSubset([]int{1, 2})

	crossing := func(a, b int) bool { return (a == 1 || a == 2) != (b == 1 || b == 2) }
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			if i == j {
				continue
			}
			want := !crossing(i, j)
			if got := fn.deliverable(i, j); got != want {
				t.Errorf("deliverable(%d, %d) = %v, want %v (crossing = %v)", i, j, got, want, crossing(i, j))
			}
		}
	}
}

// TestIsolateAndRestoreSubsetComposeAcrossOverlappingCalls is the actual new
// capability this task exists to add, checked directly: two isolateSubset
// calls on DIFFERENT, overlapping subsets share one crossing link (2<->3).
// Restoring the FIRST call's own subset must not reopen that shared link
// while the second call's own isolation is still active -- the exact
// composability partition/heal cannot offer, since either would replace the
// whole topology rather than reference-count one link.
func TestIsolateAndRestoreSubsetComposeAcrossOverlappingCalls(t *testing.T) {
	c := newCluster(t, 4, 1)
	fn := c.net

	// {0,1} isolated: cuts 0<->2, 0<->3, 1<->2, 1<->3.
	fn.isolateSubset([]int{0, 1})
	// {2} isolated: cuts 2<->0, 2<->1, 2<->3 -- 2<->0 and 2<->1 are the
	// SAME links the first call already cut, now cut by both.
	fn.isolateSubset([]int{2})

	if fn.deliverable(2, 3) || fn.deliverable(3, 2) {
		t.Fatal("2<->3 should be cut: isolating {2} alone cuts it")
	}

	// Restore {0,1} first. 0<->2 and 1<->2 must STAY cut -- {2}'s own
	// isolation still needs them cut -- but 2<->3 must ALSO stay cut,
	// since restoring {0,1} never touched it.
	fn.restoreSubset([]int{0, 1})
	if fn.deliverable(0, 2) || fn.deliverable(2, 0) {
		t.Error("0<->2 reopened by restoring {0,1}, but {2}'s own isolation still needs it cut")
	}
	if fn.deliverable(1, 2) || fn.deliverable(2, 1) {
		t.Error("1<->2 reopened by restoring {0,1}, but {2}'s own isolation still needs it cut")
	}
	if fn.deliverable(2, 3) || fn.deliverable(3, 2) {
		t.Error("2<->3 reopened by restoring {0,1}, which never cut it in the first place")
	}
	// 0<->1 was never a crossing link for either call (both are inside
	// {0,1}); 0<->3 and 1<->3 were only ever cut by the first call and
	// must be open again now.
	if !fn.deliverable(0, 3) || !fn.deliverable(3, 0) {
		t.Error("0<->3 should be reopened: only {0,1}'s own isolation ever cut it, and that is now restored")
	}

	// Now restore {2}. Only its own remaining cuts (0<->2, 1<->2) go away.
	fn.restoreSubset([]int{2})
	if !fn.deliverable(0, 2) || !fn.deliverable(2, 0) || !fn.deliverable(1, 2) || !fn.deliverable(2, 1) || !fn.deliverable(2, 3) || !fn.deliverable(3, 2) {
		t.Error("every link should be open again once both isolations are restored")
	}
}

// TestFaultInjectorAppliesAndRestoresOnSchedule is the end-to-end proof
// against a real cluster: a single scheduled entry isolates a subset for a
// fixed window and restores it automatically, with the cluster's own
// observable behavior -- who campaigns, who doesn't -- matching what
// partition_test.go's own hand-scheduled tests already check by hand.
func TestFaultInjectorAppliesAndRestoresOnSchedule(t *testing.T) {
	c := newCluster(t, 5, 4700)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no initial leader within %v: %s", electionBound, c.describe())
	}
	minority := c.othersThan(leader)[:2]

	fi := newFaultInjector(c.net)
	fi.Run([]faultSchedule{
		{Subset: minority, At: 0, Duration: 300 * time.Millisecond},
	})

	time.Sleep(50 * time.Millisecond)
	if c.net.deliverable(minority[0], leader) {
		t.Fatalf("minority node %d should be isolated from leader %d partway through the schedule", minority[0], leader)
	}

	fi.Wait()
	if !c.net.deliverable(minority[0], leader) || !c.net.deliverable(leader, minority[0]) {
		t.Errorf("minority node %d should be reconnected to leader %d once the schedule finishes", minority[0], leader)
	}
	newLeader := c.waitForStableCluster(electionBound)
	if newLeader == None {
		t.Fatalf("cluster failed to reach a stable leader after the schedule finished: %s", c.describe())
	}
}

// TestFaultInjectorStopRestoresEverythingImmediately is Stop's own
// documented guarantee, checked directly: a long-running entry, stopped
// early, leaves nothing isolated once Stop returns -- not "eventually," but
// by the time the call itself is done.
func TestFaultInjectorStopRestoresEverythingImmediately(t *testing.T) {
	c := newCluster(t, 4, 4701)
	c.start()

	fi := newFaultInjector(c.net)
	fi.Run([]faultSchedule{
		{Subset: []int{0, 1}, At: 0, Duration: time.Hour}, // would never finish on its own within this test
	})

	time.Sleep(20 * time.Millisecond) // let the isolation actually apply
	fi.Stop()

	for i := 0; i < 4; i++ {
		for j := 0; j < 4; j++ {
			if i == j {
				continue
			}
			if !c.net.deliverable(i, j) {
				t.Errorf("deliverable(%d, %d) = false after Stop, want true -- nothing should still be isolated", i, j)
			}
		}
	}
}

// TestFaultInjectorHandlesOverlappingArbitrarySubsets is the literal claim
// this task asks for, proved at once rather than piecemeal: two
// independently-scheduled faults on two DIFFERENT, non-adjacent node
// subsets, with overlapping time windows, applied and restored correctly by
// one injector's own schedule -- the scenario partition/heal alone cannot
// express, run here inside a synctest bubble (§23) to show it composes with
// that infrastructure for free, with no changes to this file needed to get
// virtual-time, jitter-free scheduling.
func TestFaultInjectorHandlesOverlappingArbitrarySubsets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 6, 4702)
		c.start()

		fi := newFaultInjector(c.net)
		fi.Run([]faultSchedule{
			{Subset: []int{0}, At: 0, Duration: 200 * time.Millisecond},
			{Subset: []int{3, 4}, At: 100 * time.Millisecond, Duration: 200 * time.Millisecond},
		})

		// Both windows active: {0} isolated since t=0, {3,4} isolated
		// since t=100ms, neither yet restored (both end at or after
		// t=200ms).
		time.Sleep(150 * time.Millisecond)
		if c.net.deliverable(0, 1) {
			t.Error("node 0 should still be isolated at t=150ms (its own window ends at 200ms)")
		}
		if c.net.deliverable(3, 5) {
			t.Error("node 3 should already be isolated at t=150ms (its own window started at 100ms)")
		}
		if !c.net.deliverable(1, 2) {
			t.Error("nodes 1 and 2 were never isolated and should stay reachable throughout")
		}

		// {0}'s own window has ended (t=200ms); {3,4}'s own window
		// (started at 100ms, 200ms long) has not, until t=300ms.
		time.Sleep(100 * time.Millisecond) // now t=250ms
		if !c.net.deliverable(0, 1) {
			t.Error("node 0 should be reconnected by t=250ms (its own window ended at 200ms)")
		}
		if c.net.deliverable(3, 5) {
			t.Error("node 3 should still be isolated at t=250ms (its own window doesn't end until 300ms)")
		}

		fi.Wait()
		if !c.net.deliverable(3, 5) || !c.net.deliverable(4, 5) {
			t.Error("every isolation should be reconnected once the whole schedule finishes")
		}
	})
}
