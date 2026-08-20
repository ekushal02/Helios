package raft

import (
	"bytes"
	"testing"
	"time"
)

// Every message in this file uses a term far above anything the node can reach
// on its own. The node has unreachable peers, so its ticker campaigns and its
// currentTerm creeps upward while a test runs; at this distance it would take
// twenty seconds of elections to overtake the leader and start rejecting
// messages under rule 1. That keeps the tests about rule 5 rather than about
// timing.
const commitTestTerm = 100

// commitFollower returns a follower with `log` installed verbatim, including the
// sentinel the caller must supply at index 0.
//
// The log is written directly rather than replayed through AppendEntries because
// several of these tests need a follower holding entries the leader never had --
// a state that is reachable in a real cluster only through a leader that has
// since died.
func commitFollower(t *testing.T, log []LogEntry) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(commitTestTerm)), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.log = log
	// Push the first campaign out of the way; every AppendEntries below resets
	// this again on arrival.
	n.electionDeadline = time.Now().Add(time.Hour)
	n.mu.Unlock()

	return n
}

// sentinel returns a fresh log containing only index 0.
func sentinel() []LogEntry {
	return []LogEntry{{Term: 0}}
}

// send delivers one AppendEntries and returns the reply.
func send(n *Node, prevIndex, prevTerm int, entries []LogEntry, leaderCommit int) AppendEntriesReply {
	args := &AppendEntriesArgs{
		Term:         commitTestTerm,
		LeaderID:     1,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	var reply AppendEntriesReply
	n.AppendEntries(args, &reply)
	return reply
}

// The plain case: entries and their commitment arrive together, and the
// follower's state machine sees them in index order.
func TestFollowerAppliesWhatTheLeaderCommitted(t *testing.T) {
	n := commitFollower(t, sentinel())

	entries := []LogEntry{
		{Term: commitTestTerm, Command: []byte("a")},
		{Term: commitTestTerm, Command: []byte("b")},
		{Term: commitTestTerm, Command: []byte("c")},
	}
	if reply := send(n, 0, 0, entries, 3); !reply.Success {
		t.Fatal("message rejected: the consistency check at the sentinel cannot fail")
	}

	if got := commitIndexOf(n); got != 3 {
		t.Fatalf("commitIndex = %d, want 3", got)
	}

	for i, want := range []string{"a", "b", "c"} {
		msg := mustApply(t, n)
		if msg.CommandIndex != i+1 {
			t.Fatalf("applied index %d, want %d: order is the guarantee",
				msg.CommandIndex, i+1)
		}
		if !bytes.Equal(msg.Command, []byte(want)) {
			t.Errorf("index %d: command %q, want %q", i+1, msg.Command, want)
		}
	}
	mustNotApply(t, n, "three entries were committed and three were applied")
}

// THE HEADLINE OF THIS TASK: commitment arrives on a LATER message than the
// entries it refers to.
//
// This is the normal case in a running cluster, not an edge case. The leader
// cannot know an entry is committed until a majority has acknowledged it, and by
// then the message that carried the entry is long gone. Every follower therefore
// learns what is safe from a heartbeat that carries nothing at all.
func TestCommitArrivesOnALaterMessageThanTheEntries(t *testing.T) {
	n := commitFollower(t, sentinel())

	entries := []LogEntry{
		{Term: commitTestTerm, Command: []byte("a")},
		{Term: commitTestTerm, Command: []byte("b")},
	}
	send(n, 0, 0, entries, 0) // stored, but nothing is committed yet

	if got := commitIndexOf(n); got != 0 {
		t.Fatalf("commitIndex = %d, want 0: LeaderCommit was 0", got)
	}
	mustNotApply(t, n, "entries are stored but not committed")

	// The empty heartbeat that follows is what releases them.
	send(n, 2, commitTestTerm, nil, 2)

	if got := commitIndexOf(n); got != 2 {
		t.Fatalf("commitIndex = %d, want 2 after the heartbeat", got)
	}
	if got := mustApply(t, n).CommandIndex; got != 1 {
		t.Fatalf("applied %d, want 1", got)
	}
	if got := mustApply(t, n).CommandIndex; got != 2 {
		t.Fatalf("applied %d, want 2", got)
	}
}

// THE min, AND THE REASON IT EXISTS. Figure 7 case (d): the follower holds a
// tail the leader never had.
//
// The heartbeat passes the consistency check at index 2 and announces
// LeaderCommit 5. Committing to 5 would commit indices 3, 4 and 5 -- entries
// from a dead leader that never reached a majority -- and the state machine
// would apply them. The very next message truncates them.
//
// If this test fails, the second half of it shows the actual damage: an entry
// that was applied and then ceased to exist.
func TestPrivateTailIsNotCommittedByAHeartbeat(t *testing.T) {
	n := commitFollower(t, []LogEntry{
		{Term: 0},
		{Term: 90, Command: []byte("a")},
		{Term: 90, Command: []byte("b")},
		{Term: 95, Command: []byte("ghost-3")},
		{Term: 95, Command: []byte("ghost-4")},
		{Term: 95, Command: []byte("ghost-5")},
	})

	send(n, 2, 90, nil, 5)

	if got := commitIndexOf(n); got != 2 {
		t.Fatalf("commitIndex = %d, want 2: the message covered nothing past "+
			"index 2, so LeaderCommit 5 must be capped at 2", got)
	}

	first := mustApply(t, n)
	second := mustApply(t, n)
	if first.CommandIndex != 1 || second.CommandIndex != 2 {
		t.Fatalf("applied indices %d, %d; want 1, 2", first.CommandIndex, second.CommandIndex)
	}
	mustNotApply(t, n, "indices 3 to 5 were never covered by any message")

	// Now the real leader speaks for index 3 and the ghosts are gone. Nothing
	// that was applied may be among them.
	send(n, 2, 90, []LogEntry{{Term: commitTestTerm, Command: []byte("real-3")}}, 3)

	msg := mustApply(t, n)
	if msg.CommandIndex != 3 {
		t.Fatalf("applied index %d, want 3", msg.CommandIndex)
	}
	if !bytes.Equal(msg.Command, []byte("real-3")) {
		t.Fatalf("applied %q at index 3, want %q: a truncated entry reached the "+
			"state machine", msg.Command, "real-3")
	}
}

// A message that is refused commits nothing, however high its LeaderCommit.
//
// Both refusal paths are covered. The rule-1 case matters most: a deposed leader
// still broadcasting its own commitIndex must not be able to move this node's,
// because its commitIndex describes a log this node has already replaced.
func TestRefusedMessagesCommitNothing(t *testing.T) {
	t.Run("stale term", func(t *testing.T) {
		n := commitFollower(t, []LogEntry{
			{Term: 0},
			{Term: commitTestTerm, Command: []byte("a")},
		})

		n.mu.Lock()
		n.currentTerm = commitTestTerm + 5
		n.mu.Unlock()

		if reply := send(n, 1, commitTestTerm, nil, 1); reply.Success {
			t.Fatal("accepted a message from a stale term")
		}
		if got := commitIndexOf(n); got != 0 {
			t.Errorf("commitIndex = %d, want 0", got)
		}
		mustNotApply(t, n, "the message was refused under rule 1")
	})

	t.Run("failed consistency check", func(t *testing.T) {
		n := commitFollower(t, sentinel())

		// PrevLogIndex 4 is past the end of a log holding only the sentinel.
		if reply := send(n, 4, commitTestTerm, nil, 4); reply.Success {
			t.Fatal("accepted a message whose PrevLogIndex is past the log")
		}
		if got := commitIndexOf(n); got != 0 {
			t.Errorf("commitIndex = %d, want 0", got)
		}
		mustNotApply(t, n, "the message was refused under rule 2")
	})
}

// A reordered message carrying a stale LeaderCommit must not un-commit anything.
// This is routine over an unordered transport, not an exotic failure.
func TestStaleLeaderCommitDoesNotMoveCommitIndexBack(t *testing.T) {
	n := commitFollower(t, sentinel())

	entries := []LogEntry{
		{Term: commitTestTerm, Command: []byte("a")},
		{Term: commitTestTerm, Command: []byte("b")},
		{Term: commitTestTerm, Command: []byte("c")},
	}
	send(n, 0, 0, entries, 3)
	mustApply(t, n)
	mustApply(t, n)
	mustApply(t, n)

	// The same message the leader sent a moment earlier, arriving late.
	send(n, 0, 0, entries[:1], 1)

	if got := commitIndexOf(n); got != 3 {
		t.Errorf("commitIndex = %d, want 3: an announced commit cannot be "+
			"withdrawn", got)
	}
	mustNotApply(t, n, "everything committed was already applied")

	n.mu.Lock()
	last := n.lastLogIndex()
	n.mu.Unlock()
	if last != 3 {
		t.Errorf("lastLogIndex = %d, want 3: a duplicate message truncated the log", last)
	}
}

// A follower that is already caught up keeps taking heartbeats without applying
// anything twice. For a healthy cluster this is the overwhelming majority of all
// messages, so re-applying here would mean re-applying constantly.
func TestCaughtUpFollowerAppliesNothingOnRepeatedHeartbeats(t *testing.T) {
	n := commitFollower(t, sentinel())

	send(n, 0, 0, []LogEntry{{Term: commitTestTerm, Command: []byte("a")}}, 1)
	if got := mustApply(t, n).CommandIndex; got != 1 {
		t.Fatalf("applied %d, want 1", got)
	}

	for i := 0; i < 5; i++ {
		send(n, 1, commitTestTerm, nil, 1)
	}

	mustNotApply(t, n, "five heartbeats carried no new entries and no new commit")
	if got := commitIndexOf(n); got != 1 {
		t.Errorf("commitIndex = %d, want 1", got)
	}
}
