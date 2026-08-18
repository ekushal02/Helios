package raft

import (
	"testing"
	"time"
)

// followerWithLog builds a follower at the given term whose log holds the given entry terms at indices 1..n.
func followerWithLog(t *testing.T, term int, entryTerms ...int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(term)), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = term
	for _, et := range entryTerms {
		n.log = append(n.log, LogEntry{Term: et})
	}
	n.resetElectionTimer()
	n.mu.Unlock()

	return n
}

func (n *Node) deadline() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.electionDeadline
}

func (n *Node) logLen() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.log)
}

// The happy path: the leader's claim about this follower's log is correct.
func TestConsistencyCheckAcceptsMatchingEntry(t *testing.T) {
	n := followerWithLog(t, 5, 3, 3, 5) // indices 1,2,3 with terms 3,3,5

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  3,
	}, &reply)

	if !reply.Success {
		t.Error("rejected a message whose PrevLogIndex/PrevLogTerm match")
	}
	if reply.Term != 5 {
		t.Errorf("reply.Term = %d, want 5", reply.Term)
	}
}

// Failure one: this follower is simply behind. It has nothing at that index.
func TestConsistencyCheckRejectsShortLog(t *testing.T) {
	n := followerWithLog(t, 5, 3) // lastLogIndex = 1

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 4, // three entries past the end
		PrevLogTerm:  5,
	}, &reply)

	if reply.Success {
		t.Error("accepted a message about an index this node does not have")
	}
	if reply.Term != 5 {
		t.Errorf("reply.Term = %d, want 5 so the leader can tell this is not "+
			"a term rejection", reply.Term)
	}
}

// Failure two: the log diverged. Something is at that index, from a different leader's term.
func TestConsistencyCheckRejectsTermMismatch(t *testing.T) {
	n := followerWithLog(t, 6, 4, 4, 4) // index 3 has term 4

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         6,
		LeaderID:     1,
		PrevLogIndex: 3,
		PrevLogTerm:  5, // leader believes index 3 was created in term 5
	}, &reply)

	if reply.Success {
		t.Error("accepted a message whose PrevLogTerm disagrees with the log")
	}
}

// A follower LONGER than the leader believes is not a mismatch. The check asks
// only about one position; extra entries beyond it are C-5's problem.
func TestConsistencyCheckIgnoresEntriesBeyondPrevLogIndex(t *testing.T) {
	n := followerWithLog(t, 5, 5, 5, 5, 5, 5)

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  5,
	}, &reply)

	if !reply.Success {
		t.Error("rejected because the follower had entries past PrevLogIndex; " +
			"the check concerns exactly one position")
	}
}

func TestConsistencyCheckAlwaysMatchesAtTheSentinel(t *testing.T) {
	for _, entries := range [][]int{nil, {1}, {7, 7, 9}} {
		n := followerWithLog(t, 9, entries...)

		var reply AppendEntriesReply
		n.AppendEntries(&AppendEntriesArgs{
			Term:         9,
			LeaderID:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
		}, &reply)

		if !reply.Success {
			t.Errorf("log %v: rejected at the sentinel; repair cannot terminate",
				entries)
		}
	}
}

func TestRejectedMessageStillResetsElectionTimer(t *testing.T) {
	n := followerWithLog(t, 5, 5)

	n.mu.Lock()
	n.electionDeadline = time.Now() // expiring at this instant
	n.mu.Unlock()

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 9, // guaranteed rejection: nothing at that index
		PrevLogTerm:  5,
	}, &reply)

	if reply.Success {
		t.Fatal("test setup wrong: this message should have been rejected")
	}
	assertTimerWasReset(t, n, "rejected AppendEntries")

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID != 1 {
		t.Errorf("leaderID = %d, want 1: rejection is about the log, not about "+
			"who leads", n.leaderID)
	}
}

// The contrast that gives the previous test meaning.
func TestStaleTermRejectionWithholdsTimerReset(t *testing.T) {
	n := followerWithLog(t, 7, 7)
	before := n.deadline()

	time.Sleep(2 * time.Millisecond)

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         3, // a deposed leader
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}, &reply)

	if reply.Success {
		t.Error("accepted a message from a term this node has left")
	}
	if !n.deadline().Equal(before) {
		t.Error("a stale leader reset this node's election timer: it could hold " +
			"a healthy cluster hostage")
	}
	if reply.Term != 7 {
		t.Errorf("reply.Term = %d, want 7 so the stale leader steps down", reply.Term)
	}
}

// A candidate losing the term steps down even when the message that deposed it fails the log check.
func TestCandidateStepsDownEvenOnRejectedMessage(t *testing.T) {
	n := followerWithLog(t, 5)

	n.mu.Lock()
	n.state = Candidate
	n.votedFor = n.id
	n.mu.Unlock()

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 4, // rejected
		PrevLogTerm:  5,
	}, &reply)

	state, term, votedFor := n.snapshotState()
	if state != Follower {
		t.Errorf("state = %v, want follower: a leader exists in this term "+
			"regardless of whether its message applied", state)
	}
	if term != 5 {
		t.Errorf("currentTerm = %d, want 5 (unchanged)", term)
	}
	if votedFor != n.id {
		t.Errorf("votedFor = %d, want %d: the vote in this term is spent",
			votedFor, n.id)
	}
}

// The check runs on heartbeats too. There is no separate heartbeat message, so
// divergence is discovered during idle periods rather than waiting for a client write.
func TestConsistencyCheckRunsOnEmptyMessages(t *testing.T) {
	n := followerWithLog(t, 5, 3, 3)

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  9, // diverged
		Entries:      nil,
	}, &reply)

	if reply.Success {
		t.Error("an empty message skipped the consistency check")
	}
}

func TestStoringEntriesDoesNotCommit(t *testing.T) {
	f := followerWithLog(t, 5)

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 5}, {Term: 5}},
		LeaderCommit: 2,
	}, &reply)

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0: LeaderCommit is handled in C-11",
			f.commitIndex)
	}
	if f.lastApplied != 0 {
		t.Errorf("lastApplied = %d, want 0", f.lastApplied)
	}
}

// A negative PrevLogIndex cannot come from a correct leader, but a malformed message must not panic the receiver.
func TestNegativePrevLogIndexIsRejectedNotFatal(t *testing.T) {
	n := followerWithLog(t, 5, 5)

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
	}, &reply)

	if reply.Success {
		t.Error("accepted a message with a negative PrevLogIndex")
	}
}
