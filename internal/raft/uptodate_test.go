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

// The comparison rule itself, in isolation.
func TestIsUpToDate(t *testing.T) {
	tests := []struct {
		name string

		myLog []int // terms of this node's entries

		candidateLastIndex int
		candidateLastTerm  int

		want bool
	}{
		{
			// Both empty. Bootstrap: nobody has anything, everybody qualifies.
			name:               "both logs empty",
			myLog:              nil,
			candidateLastIndex: 0,
			candidateLastTerm:  0,
			want:               true,
		},
		{
			name:               "identical logs",
			myLog:              []int{1, 1, 2},
			candidateLastIndex: 3,
			candidateLastTerm:  2,
			want:               true,
		},
		{
			// Same term, candidate has more. Strictly ahead.
			name:               "same last term, candidate log is longer",
			myLog:              []int{1, 1},
			candidateLastIndex: 5,
			candidateLastTerm:  1,
			want:               true,
		},
		{
			// Same term, candidate has less. It is missing entries we hold.
			name:               "same last term, candidate log is shorter",
			myLog:              []int{1, 1, 1, 1},
			candidateLastIndex: 2,
			candidateLastTerm:  1,
			want:               false,
		},
		{
			// THE CASE THAT BREAKS "longest log wins".
			name:               "candidate has higher last term but SHORTER log",
			myLog:              []int{1, 1, 1, 1},
			candidateLastIndex: 2,
			candidateLastTerm:  2,
			want:               true,
		},
		{
			// The mirror image. A long log does not rescue a stale term.
			name:               "candidate has lower last term but LONGER log",
			myLog:              []int{1, 2},
			candidateLastIndex: 9,
			candidateLastTerm:  1,
			want:               false,
		},
		{
			// Candidate has nothing; we have entries.
			name:               "candidate log is empty, ours is not",
			myLog:              []int{1},
			candidateLastIndex: 0,
			candidateLastTerm:  0,
			want:               false,
		},
		{
			// We have nothing; candidate does.
			name:               "our log is empty, candidate has entries",
			myLog:              nil,
			candidateLastIndex: 3,
			candidateLastTerm:  2,
			want:               true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := withLog(t, tc.myLog...)

			n.mu.Lock()
			got := n.isUpToDate(tc.candidateLastIndex, tc.candidateLastTerm)
			n.mu.Unlock()

			if got != tc.want {
				t.Errorf("isUpToDate(index=%d, term=%d) = %v, want %v",
					tc.candidateLastIndex, tc.candidateLastTerm, got, tc.want)
			}
		})
	}
}

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

// DOCUMENTS A KNOWN GAP, deliberately. This asserts current behaviour rather
// than ideal behaviour.
//
// A candidate carrying a higher term makes this node step down and adopt that
// term, and becomeFollower resets the election timer -- even though the vote is
// then refused on the log check. So a node with a useless log can delay a
// healthy cluster's elections simply by campaigning with an inflated term.
//
// Why the reset stays: without it, a leader stepping down would carry a stale
// deadline (the ticker never refreshes a leader's) and would instantly campaign
// against whoever just deposed it. That trades a rare disruption for one on
// every single failover.
//
// The real fix is Prevote (task D-12): a candidate first asks whether it COULD
// win, without incrementing anyone's term. When D-12 lands, this test should
// start failing -- and that failure is the proof Prevote works. Invert it then.
func TestHigherTermResetsTimerEvenWhenRefused(t *testing.T) {
	n := withLog(t, 1, 2, 3)

	n.mu.Lock()
	n.currentTerm = 3
	n.resetElectionTimer()
	before := n.electionDeadline
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{
		Term:         99, // inflated by campaigning alone in a partition
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
		t.Error("expected the term adoption to reset the timer (see D-12); " +
			"if Prevote is now implemented, invert this assertion")
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
