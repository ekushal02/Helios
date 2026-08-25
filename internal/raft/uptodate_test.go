package raft

import "testing"

// withLog builds a node whose log ends at the given terms.
// entryTerms are the terms of the real entries, after the log[0] sentinel.
func withLog(t *testing.T, entryTerms ...int) *Node {
	t.Helper()
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)
	t.Cleanup(n.Stop) // NewNode starts the applier; without this it outlives the test

	n.mu.Lock()
	for _, term := range entryTerms {
		n.log = append(n.log, LogEntry{Term: term})
	}
	if len(entryTerms) > 0 {
		n.currentTerm = entryTerms[len(entryTerms)-1]
	}
	n.mu.Unlock()

	return n
}

// §5.4.1's comparison now lives in prevote.go as logIsAtLeastAsUpToDate, shared
// with the pre-vote receiver so the poll answers by the rules the real vote
// applies. The table-driven test that used to sit here exercised a second copy
// of that rule in this file; the copy is gone, and the tests below exercise the
// surviving one through the handler that matters.

func TestStaleCandidateIsRejected(t *testing.T) {
	// This node holds four entries, the last from term 3.
	n := withLog(t, 1, 2, 3, 3)

	n.mu.Lock()
	n.currentTerm = 3
	n.votedFor = None // no reason to refuse other than the log
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{
		Term:         4,
		CandidateID:  1,
		LastLogIndex: 2,
		LastLogTerm:  2,
	}, &reply)

	if reply.VoteGranted {
		t.Error("granted a vote to a candidate with a stale log")
	}

	state, term, votedFor := n.snapshotState()
	if term != 4 {
		t.Errorf("currentTerm = %d, want 4 (the newer term is adopted anyway)", term)
	}
	if state != Follower {
		t.Errorf("state = %v, want follower", state)
	}
	if votedFor != None {
		t.Errorf("votedFor = %d, want None: the vote is still available", votedFor)
	}
}

func TestHighTermCannotOverrideStaleLog(t *testing.T) {
	n := withLog(t, 1, 1, 2, 2, 3)

	n.mu.Lock()
	n.currentTerm = 3
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{
		Term:         500, // spent a long time alone, campaigning
		CandidateID:  1,
		LastLogIndex: 1, // but its log went nowhere
		LastLogTerm:  1,
	}, &reply)

	if reply.VoteGranted {
		t.Error("a large term number bought a vote it should not have")
	}
	if _, term, _ := n.snapshotState(); term != 500 {
		t.Errorf("currentTerm = %d, want 500", term)
	}
}

func TestRejectedStaleLogDoesNotResetTimer(t *testing.T) {
	n := withLog(t, 1, 2, 3)

	n.mu.Lock()
	n.currentTerm = 3
	n.votedFor = None
	n.resetElectionTimer()
	before := n.electionDeadline
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{
		Term:         3, // same term: no step-down, so the log check is the only gate
		CandidateID:  1,
		LastLogIndex: 1,
		LastLogTerm:  1,
	}, &reply)

	n.mu.Lock()
	after := n.electionDeadline
	n.mu.Unlock()

	if reply.VoteGranted {
		t.Fatal("stale-log candidate should have been rejected")
	}
	if !after.Equal(before) {
		t.Error("a log-based rejection reset the election timer")
	}
}

// PRE-VOTE CLOSED THIS, and the test stays to prove the local behaviour is
// still what it should be.
//
// It used to read: a candidate carrying an inflated term makes this node step
// down and adopt it, resetting the election timer, even though the vote is then
// refused on the log check -- so a node with a useless log could delay a healthy
// cluster's elections just by campaigning. The comment ended "when D-12 lands,
// invert this".
//
// What D-12 actually changed is UPSTREAM of here. RequestVote is unchanged and
// must stay unchanged: a real vote request is evidence, because the sender has
// already incremented and voted for itself, so adopting the term is the
// all-servers rule doing its job. Refusing to reset would be worse -- a leader
// stepping down would carry a stale deadline and immediately campaign against
// whoever deposed it, on every failover.
//
// The disruption is gone because NOBODY SENDS THIS MESSAGE ANY MORE. A
// partitioned node cannot reach term 99 without winning polls it will never
// win, so an inflated-term RequestVote has no way to arise. See
// TestPreVoteStopsAReturningNodeFromDisruptingTheCluster for the measurement,
// and TestMinorityPartitionCannotElectWithLeaderInMajority for the same
// property at cluster scale.
//
// So the assertion is unchanged and the framing is inverted: this is no longer
// a documented gap, it is the correct handling of a message that can no longer
// be produced.
func TestAnInflatedTermStillResetsTheTimerIfItEverArrives(t *testing.T) {
	n := withLog(t, 1, 2, 3)

	n.mu.Lock()
	n.currentTerm = 3
	n.resetElectionTimer()
	before := n.electionDeadline
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{
		Term:         99, // unreachable in a running cluster; constructed here
		CandidateID:  1,
		LastLogIndex: 1, // log went nowhere
		LastLogTerm:  1,
	}, &reply)

	n.mu.Lock()
	after := n.electionDeadline
	n.mu.Unlock()

	if reply.VoteGranted {
		t.Fatal("stale-log candidate should still have been refused")
	}
	if after.Equal(before) {
		t.Error("adopting a higher term did not reset the timer: a leader stepping " +
			"down would keep a stale deadline and campaign against whoever just " +
			"deposed it")
	}
}

// An up-to-date candidate is still granted the vote, so the check does not simply refuse everyone.
func TestUpToDateCandidateIsGranted(t *testing.T) {
	n := withLog(t, 1, 2, 3)

	n.mu.Lock()
	n.currentTerm = 3
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{
		Term:         4,
		CandidateID:  1,
		LastLogIndex: 3,
		LastLogTerm:  3,
	}, &reply)

	if !reply.VoteGranted {
		t.Error("refused a candidate whose log matches ours exactly")
	}
}

func TestCommittedEntrySurvivesElection(t *testing.T) {
	// The two nodes that hold the agreed entry, ending in term 2.
	holders := []*Node{
		withLog(t, 1, 2),
		withLog(t, 1, 2),
	}
	for _, h := range holders {
		h.mu.Lock()
		h.currentTerm = 2
		h.mu.Unlock()
	}

	// Node 2 missed the term-2 entry entirely.
	staleArgs := &RequestVoteArgs{
		Term:         3,
		CandidateID:  2,
		LastLogIndex: 1,
		LastLogTerm:  1,
	}

	votes := 1 // the stale candidate's own vote
	for _, h := range holders {
		var reply RequestVoteReply
		h.RequestVote(staleArgs, &reply)
		if reply.VoteGranted {
			votes++
		}
	}

	const majority = 2 // of a 3-node cluster
	if votes >= majority {
		t.Errorf("stale candidate gathered %d votes and could have won; "+
			"the agreed entry would have been overwritten", votes)
	}
}
