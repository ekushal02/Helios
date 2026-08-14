package raft

import (
	"testing"
	"time"
)

// The four cases of receiver implementation, plus the term-adoption case that makes them work.
func TestRequestVoteGrantingRules(t *testing.T) {
	tests := []struct {
		name string

		// node state before the RPC arrives
		nodeTerm     int
		nodeVotedFor int

		// the incoming request
		argsTerm        int
		argsCandidateID int

		// what should happen
		wantGranted  bool
		wantTerm     int
		wantVotedFor int
	}{
		{
			// Rule 1. The candidate is campaigning in a term this node has already moved past.
			name:            "reject: candidate term is stale",
			nodeTerm:        5,
			nodeVotedFor:    None,
			argsTerm:        3,
			argsCandidateID: 2,
			wantGranted:     false,
			wantTerm:        5,
			wantVotedFor:    None,
		},
		{
			// Rule 2, no vote yet. The straightforward yes.
			name:            "grant: same term, no vote cast yet",
			nodeTerm:        5,
			nodeVotedFor:    None,
			argsTerm:        5,
			argsCandidateID: 2,
			wantGranted:     true,
			wantTerm:        5,
			wantVotedFor:    2,
		},
		{
			// Rule 2, already committed elsewhere. One vote per term is what makes two leaders in one term impossible.
			name:            "reject: already voted for a different candidate",
			nodeTerm:        5,
			nodeVotedFor:    1,
			argsTerm:        5,
			argsCandidateID: 2,
			wantGranted:     false,
			wantTerm:        5,
			wantVotedFor:    1, // the original vote stands
		},
		{
			// Rule 2, same candidate again. Networks duplicate messages; re-confirming a vote already given costs nothing and is safe.
			name:            "grant: already voted for THIS candidate",
			nodeTerm:        5,
			nodeVotedFor:    2,
			argsTerm:        5,
			argsCandidateID: 2,
			wantGranted:     true,
			wantTerm:        5,
			wantVotedFor:    2,
		},
		{
			// All Servers rule. The newer term arrives, this node adopts it, and its old vote is wiped -- so it can vote in the new election.
			name:            "grant: newer term wipes a previous vote",
			nodeTerm:        5,
			nodeVotedFor:    1,
			argsTerm:        6,
			argsCandidateID: 2,
			wantGranted:     true,
			wantTerm:        6,
			wantVotedFor:    2,
		},
		{
			// A newer term is adopted even when the vote goes to someone this node has never heard of.
			name:            "grant: newer term, no previous vote",
			nodeTerm:        2,
			nodeVotedFor:    None,
			argsTerm:        9,
			argsCandidateID: 4,
			wantGranted:     true,
			wantTerm:        9,
			wantVotedFor:    4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := NewNode(0, []int{1, 2}, silentPeers(), 1)

			n.mu.Lock()
			n.currentTerm = tc.nodeTerm
			n.votedFor = tc.nodeVotedFor
			n.mu.Unlock()

			var reply RequestVoteReply
			n.RequestVote(&RequestVoteArgs{
				Term:        tc.argsTerm,
				CandidateID: tc.argsCandidateID,
			}, &reply)

			if reply.VoteGranted != tc.wantGranted {
				t.Errorf("VoteGranted = %v, want %v", reply.VoteGranted, tc.wantGranted)
			}
			if reply.Term != tc.wantTerm {
				t.Errorf("reply.Term = %d, want %d", reply.Term, tc.wantTerm)
			}

			_, gotTerm, gotVotedFor := n.snapshotState()
			if gotTerm != tc.wantTerm {
				t.Errorf("node currentTerm = %d, want %d", gotTerm, tc.wantTerm)
			}
			if gotVotedFor != tc.wantVotedFor {
				t.Errorf("node votedFor = %d, want %d", gotVotedFor, tc.wantVotedFor)
			}
		})
	}
}

// A candidate or leader that sees a newer term must step down before answering.
func TestRequestVoteStepsDownOnNewerTerm(t *testing.T) {
	for _, start := range []State{Candidate, Leader} {
		t.Run(start.String(), func(t *testing.T) {
			n := NewNode(0, []int{1, 2}, silentPeers(), 1)

			n.mu.Lock()
			n.state = start
			n.currentTerm = 4
			n.votedFor = n.id
			n.mu.Unlock()

			var reply RequestVoteReply
			n.RequestVote(&RequestVoteArgs{Term: 7, CandidateID: 1}, &reply)

			state, term, votedFor := n.snapshotState()
			if state != Follower {
				t.Errorf("state = %v, want follower after seeing a newer term", state)
			}
			if term != 7 {
				t.Errorf("currentTerm = %d, want 7", term)
			}
			if votedFor != 1 {
				t.Errorf("votedFor = %d, want 1", votedFor)
			}
			if !reply.VoteGranted {
				t.Error("should have granted the vote after stepping down")
			}
		})
	}
}

// A GRANTED vote resets the election timer: this node has backed someone and should give that election room to finish.
func TestGrantedVoteResetsElectionTimer(t *testing.T) {

	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.currentTerm = 1
	n.electionDeadline = time.Now() // expiring right now
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{Term: 1, CandidateID: 1}, &reply)

	if !reply.VoteGranted {
		t.Fatal("expected the vote to be granted")
	}
	assertTimerWasReset(t, n, "granted vote")
}

// A REJECTED stale request must NOT reset the timer.
func TestRejectedStaleVoteDoesNotResetTimer(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.currentTerm = 5
	n.resetElectionTimer()
	before := n.electionDeadline
	n.mu.Unlock()

	time.Sleep(2 * time.Millisecond)

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{Term: 2, CandidateID: 1}, &reply)

	n.mu.Lock()
	after := n.electionDeadline
	n.mu.Unlock()

	if reply.VoteGranted {
		t.Fatal("stale request should have been rejected")
	}
	if !after.Equal(before) {
		t.Error("a rejected stale request reset the election timer")
	}
}

// Only one candidate can win a given term. This is Election Safety, checked directly at the level of a single voter.
func TestOnlyOneVotePerTerm(t *testing.T) {
	n := NewNode(0, []int{1, 2, 3, 4}, silentPeers(), 1)

	n.mu.Lock()
	n.currentTerm = 3
	n.mu.Unlock()

	granted := 0
	for _, candidate := range []int{1, 2, 3, 4} {
		var reply RequestVoteReply
		n.RequestVote(&RequestVoteArgs{Term: 3, CandidateID: candidate}, &reply)
		if reply.VoteGranted {
			granted++
		}
	}

	if granted != 1 {
		t.Errorf("granted %d votes in one term, want exactly 1", granted)
	}
}

// Concurrent requests must not both win. Without the mutex, two candidates arriving at once could each see votedFor == None.
func TestConcurrentVoteRequests(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.currentTerm = 1
	n.mu.Unlock()

	results := make(chan bool, 2)
	for _, candidate := range []int{1, 2} {
		go func(id int) {
			var reply RequestVoteReply
			n.RequestVote(&RequestVoteArgs{Term: 1, CandidateID: id}, &reply)
			results <- reply.VoteGranted
		}(candidate)
	}

	granted := 0
	for i := 0; i < 2; i++ {
		if <-results {
			granted++
		}
	}

	if granted != 1 {
		t.Errorf("granted %d concurrent votes, want exactly 1", granted)
	}
}
