package raft

import (
	"bytes"
	"testing"
	"time"
)

// applyNode builds a follower holding `entries` real commands (plus the
// sentinel), with nothing else running: peers are unreachable, so there is no
// election, no replication and no other writer of commitIndex. Every commit in
// this file is made by the test, by hand, which is what makes the deliveries
// deterministic.
//
// Stop is idempotent (stopOnce), so a test may stop the node early and still let
// Cleanup run.
func applyNode(t *testing.T, entries int, term int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(term)), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	for i := 1; i <= entries; i++ {
		n.log = append(n.log, LogEntry{Term: term, Command: []byte{byte(i)}})
	}
	n.currentTerm = term
	n.mu.Unlock()

	return n
}

// commit is the test's stand-in for the leader's majority count and the
// follower's LeaderCommit rule. It goes through the same commitTo the production
// paths use, so the wake-up is exercised rather than simulated.
func commit(n *Node, idx int) {
	n.mu.Lock()
	n.commitTo(idx)
	n.mu.Unlock()
}

// mustApply receives one message or fails. The bound is generous because the
// only thing between commit and delivery is a scheduler wake.
func mustApply(t *testing.T, n *Node) ApplyMsg {
	t.Helper()
	select {
	case msg, ok := <-n.ApplyCh():
		if !ok {
			t.Fatal("apply channel closed: the node was stopped mid-test")
		}
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("no message applied within 2s")
		return ApplyMsg{}
	}
}

// mustNotApply fails if anything is delivered inside the window. It is a
// negative liveness check, so the window is a compromise: long enough that a
// real delivery would have happened, short enough not to dominate the suite.
func mustNotApply(t *testing.T, n *Node, why string) {
	t.Helper()
	select {
	case msg := <-n.ApplyCh():
		t.Fatalf("applied index %d, want nothing: %s", msg.CommandIndex, why)
	case <-time.After(100 * time.Millisecond):
	}
}

// The headline property: entries arrive once each, in index order, carrying the
// term they were logged under.
//
// The term travels with the command because a state machine may need to tell a
// client which term its write landed in, and because duplicate detection built
// on index alone is wrong across a leader change.
func TestAppliesEveryCommittedEntryInIndexOrder(t *testing.T) {
	n := applyNode(t, 5, 3)

	commit(n, 5)

	for want := 1; want <= 5; want++ {
		msg := mustApply(t, n)

		if !msg.CommandValid {
			t.Errorf("index %d: CommandValid false, want true", msg.CommandIndex)
		}
		if msg.CommandIndex != want {
			t.Fatalf("applied index %d, want %d: entries must arrive in order",
				msg.CommandIndex, want)
		}
		if msg.CommandTerm != 3 {
			t.Errorf("index %d: term %d, want 3", want, msg.CommandTerm)
		}
		if !bytes.Equal(msg.Command, []byte{byte(want)}) {
			t.Errorf("index %d: command %v, want %v", want, msg.Command, []byte{byte(want)})
		}
	}

	mustNotApply(t, n, "the log holds five entries and five were committed")
}

// The commit gate. Entries past commitIndex are in the log but are not agreed
// on, and a state machine that applies one has applied something a future leader
// may legally overwrite -- the entry vanishes and the reply to the client was a
// lie.
func TestUncommittedEntriesAreNeverApplied(t *testing.T) {
	n := applyNode(t, 5, 3)

	commit(n, 2)

	if got := mustApply(t, n).CommandIndex; got != 1 {
		t.Fatalf("applied %d, want 1", got)
	}
	if got := mustApply(t, n).CommandIndex; got != 2 {
		t.Fatalf("applied %d, want 2", got)
	}
	mustNotApply(t, n, "indices 3 to 5 are in the log but not committed")

	// The applier must be waiting, not finished: a second commit has to reach a
	// goroutine that already emptied its queue once and went back to sleep.
	commit(n, 5)
	for want := 3; want <= 5; want++ {
		if got := mustApply(t, n).CommandIndex; got != want {
			t.Fatalf("applied %d, want %d after the second commit", got, want)
		}
	}
}

// lastApplied means APPLIED, past tense -- handed to the state machine, not
// merely queued for it.
//
// This pins the choice made in applier(). Advancing lastApplied to commitIndex
// as soon as the batch is copied is simpler and passes every other test in this
// file. It would then quietly break the two features that read lastApplied to
// mean "the state machine is caught up": linearizable reads, and deciding which
// entries a snapshot may discard.
func TestLastAppliedFollowsDeliveryNotCommitment(t *testing.T) {
	n := applyNode(t, 3, 3)

	commit(n, 3)

	// Nobody has received anything, so nothing has been applied, however long
	// the applier has been awake.
	time.Sleep(100 * time.Millisecond)
	n.mu.Lock()
	got := n.lastApplied
	n.mu.Unlock()
	if got != 0 {
		t.Fatalf("lastApplied = %d with no consumer, want 0: committing "+
			"releases entries, it does not apply them", got)
	}

	// Take exactly one. lastApplied settles at 1 and stops: the applier is now
	// blocked handing over index 2.
	mustApply(t, n)

	deadline := time.Now().Add(2 * time.Second)
	for {
		n.mu.Lock()
		got = n.lastApplied
		n.mu.Unlock()

		if got == 1 {
			break
		}
		if got > 1 {
			t.Fatalf("lastApplied = %d after one delivery, want 1", got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("lastApplied stuck at %d after one delivery, want 1", got)
		}
		time.Sleep(time.Millisecond)
	}
}

// The applier holds no lock while it sends, so a stalled state machine cannot
// stall consensus.
//
// This is the deadlock the one-goroutine design exists to prevent, and it earns
// an explicit test because getting it wrong does not produce a wrong answer --
// it produces a cluster that stops electing leaders under load, which reads as a
// network problem.
func TestAStalledConsumerDoesNotBlockRaft(t *testing.T) {
	n := applyNode(t, 3, 3)

	commit(n, 3)
	time.Sleep(50 * time.Millisecond) // let the applier park on the send

	acquired := make(chan struct{})
	go func() {
		n.mu.Lock()
		n.mu.Unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("could not acquire mu while the applier was blocked sending: " +
			"the apply path is holding the lock across the channel send")
	}
}

// Recommitting an index applies nothing. Once C-11 lands, rule 5 runs on every
// AppendEntries that carries a LeaderCommit, which for a caught-up follower is
// every heartbeat -- so this is the common case, not an edge case.
func TestRecommittingAnIndexAppliesNothingAgain(t *testing.T) {
	n := applyNode(t, 3, 3)

	commit(n, 2)
	mustApply(t, n)
	mustApply(t, n)

	commit(n, 2) // the same index again
	commit(n, 1) // and a stale, lower one, as a reordered message would carry

	mustNotApply(t, n, "no index above 2 has been committed")

	n.mu.Lock()
	got := n.commitIndex
	n.mu.Unlock()
	if got != 2 {
		t.Errorf("commitIndex = %d, want 2: commitIndex must never move backwards", got)
	}
}

// Committing past the end of the log is refused rather than obeyed.
//
// It should never happen -- it means the majority count or rule 5 is broken --
// but the applier indexes n.log directly, so obeying it would panic the node
// instead of logging a diagnosable error.
func TestCommitBeyondTheLogIsRefused(t *testing.T) {
	n := applyNode(t, 2, 3)

	commit(n, 7)

	n.mu.Lock()
	got := n.commitIndex
	n.mu.Unlock()
	if got != 0 {
		t.Fatalf("commitIndex = %d, want 0: the log holds two entries", got)
	}
	mustNotApply(t, n, "nothing was legitimately committed")
}

// Stop releases an applier parked on a send to a consumer that never came, and
// closes the channel so `for range` consumers terminate.
//
// Without the stopCh arm of the send select this is a permanent goroutine leak,
// and every test that commits without draining hangs at Cleanup.
func TestStopReleasesAParkedApplier(t *testing.T) {
	n := applyNode(t, 3, 3)

	commit(n, 3)
	time.Sleep(50 * time.Millisecond)

	n.Stop()

	select {
	case <-n.applierDone:
	case <-time.After(2 * time.Second):
		t.Fatal("applier still running 2s after Stop")
	}

	// Draining a closed channel yields the zero value immediately rather than
	// blocking, which is what makes `for range n.ApplyCh()` a complete consumer.
	select {
	case _, ok := <-n.ApplyCh():
		if ok {
			t.Fatal("apply channel delivered after the applier exited")
		}
	case <-time.After(time.Second):
		t.Fatal("apply channel was not closed when the applier exited")
	}
}
