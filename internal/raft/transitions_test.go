package raft

import (
	"testing"
	"time"
)

// --- Becoming leader on a majority -----------------------------------------

// The vote threshold, checked across cluster sizes.
func TestBecomesLeaderOnlyAtMajority(t *testing.T) {
	tests := []struct {
		name        string
		peers       int
		grantingIDs map[int]bool
		wantLeader  bool
	}{
		{
			name:        "3 nodes, 1 peer vote: self+1 = 2 = majority",
			peers:       2,
			grantingIDs: map[int]bool{1: true},
			wantLeader:  true,
		},
		{
			name:        "3 nodes, 0 peer votes: self alone is not a majority",
			peers:       2,
			grantingIDs: map[int]bool{},
			wantLeader:  false,
		},
		{
			name:        "5 nodes, 2 peer votes: self+2 = 3 = majority",
			peers:       4,
			grantingIDs: map[int]bool{1: true, 2: true},
			wantLeader:  true,
		},
		{
			name:        "5 nodes, 1 peer vote: self+1 = 2, one short",
			peers:       4,
			grantingIDs: map[int]bool{1: true},
			wantLeader:  false,
		},
		{
			name:        "5 nodes, all 4 peers: comfortably over",
			peers:       4,
			grantingIDs: map[int]bool{1: true, 2: true, 3: true, 4: true},
			wantLeader:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			peers := make([]int, tc.peers)
			for i := range peers {
				peers[i] = i + 1
			}

			stub := newStubTransport(func(to int, args *RequestVoteArgs) (RequestVoteReply, bool) {
				return RequestVoteReply{
					Term:        args.Term,
					VoteGranted: tc.grantingIDs[to],
				}, true
			})

			n := NewNode(0, peers, stub, 1)

			n.mu.Lock()
			n.becomeCandidate()
			n.mu.Unlock()

			time.Sleep(80 * time.Millisecond)

			state, _, _ := n.snapshotState()
			gotLeader := state == Leader

			if gotLeader != tc.wantLeader {
				t.Errorf("state = %v (leader=%v), want leader=%v",
					state, gotLeader, tc.wantLeader)
			}
		})
	}
}

// becomeLeader is guarded to Candidate.
func TestBecomeLeaderOnlyFromCandidate(t *testing.T) {
	for _, start := range []State{Follower, Leader} {
		t.Run("from_"+start.String(), func(t *testing.T) {
			n := NewNode(0, []int{1, 2}, silentPeers(), 1)

			n.mu.Lock()
			n.state = start
			n.becomeLeader()
			got := n.state
			n.mu.Unlock()

			if got != start {
				t.Errorf("state changed from %v to %v; becomeLeader should be a "+
					"no-op outside Candidate", start, got)
			}
		})
	}
}

// Every state must step down, from every RPC direction.
func TestStepDownOnHigherTerm(t *testing.T) {
	for _, start := range []State{Follower, Candidate, Leader} {
		t.Run(start.String(), func(t *testing.T) {
			n := NewNode(0, []int{1, 2}, silentPeers(), 1)

			n.mu.Lock()
			n.state = start
			n.currentTerm = 4
			n.votedFor = n.id
			stepped := n.stepDownIfStale(9)
			n.mu.Unlock()

			if !stepped {
				t.Error("stepDownIfStale returned false for a newer term")
			}

			state, term, votedFor := n.snapshotState()
			if state != Follower {
				t.Errorf("state = %v, want follower", state)
			}
			if term != 9 {
				t.Errorf("currentTerm = %d, want 9", term)
			}
			if votedFor != None {
				t.Errorf("votedFor = %d, want None: a new term is a new election", votedFor)
			}
		})
	}
}

// An equal or lower term changes nothing at all.
func TestNoStepDownOnEqualOrLowerTerm(t *testing.T) {
	for _, incoming := range []int{5, 4, 1} {
		n := NewNode(0, []int{1, 2}, silentPeers(), 1)

		n.mu.Lock()
		n.state = Leader
		n.currentTerm = 5
		n.votedFor = n.id
		stepped := n.stepDownIfStale(incoming)
		n.mu.Unlock()

		if stepped {
			t.Errorf("term %d: stepped down against currentTerm 5", incoming)
		}

		state, term, votedFor := n.snapshotState()
		if state != Leader || term != 5 || votedFor != n.id {
			t.Errorf("term %d: state changed (state=%v term=%d votedFor=%d)",
				incoming, state, term, votedFor)
		}
	}
}

func TestSameTermStepDownKeepsVote(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.state = Candidate
	n.currentTerm = 5
	n.votedFor = n.id // voted for itself in term 5
	n.becomeFollower(5)
	n.mu.Unlock()

	state, term, votedFor := n.snapshotState()

	if state != Follower {
		t.Errorf("state = %v, want follower", state)
	}
	if term != 5 {
		t.Errorf("currentTerm = %d, want 5 (unchanged)", term)
	}
	if votedFor != n.id {
		t.Errorf("votedFor = %d, want %d: the vote in this term is already spent",
			votedFor, n.id)
	}
}

func TestNoSecondVoteAfterSameTermStepDown(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.state = Candidate
	n.currentTerm = 5
	n.votedFor = n.id
	n.becomeFollower(5) // lost term 5 to a rival
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{Term: 5, CandidateID: 2}, &reply)

	if reply.VoteGranted {
		t.Error("granted a second vote in term 5; two leaders become possible")
	}
}

// By contrast, a step-down that DOES advance the term frees the vote.
func TestHigherTermStepDownFreesVote(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.state = Candidate
	n.currentTerm = 5
	n.votedFor = n.id
	n.becomeFollower(6)
	n.mu.Unlock()

	if _, _, votedFor := n.snapshotState(); votedFor != None {
		t.Fatalf("votedFor = %d, want None after the term advanced", votedFor)
	}

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{Term: 6, CandidateID: 2}, &reply)

	if !reply.VoteGranted {
		t.Error("refused a vote in the new term; the old vote should not carry over")
	}
}

// A leader that steps down starts timing out again like any follower.
func TestSteppedDownLeaderGetsAFreshTimer(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 3
	n.electionDeadline = time.Now().Add(-time.Hour) // badly stale
	n.stepDownIfStale(7)
	deadline := n.electionDeadline
	n.mu.Unlock()

	if !deadline.After(time.Now()) {
		t.Error("a stepped-down leader kept a deadline in the past and would " +
			"campaign against the node that just deposed it")
	}
}

func TestStepsDownFromVoteReply(t *testing.T) {
	stub := newStubTransport(func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
		return RequestVoteReply{Term: 42, VoteGranted: false}, true
	})
	n := NewNode(0, []int{1, 2}, stub, 1)

	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	waitForState(t, n, Follower, 200*time.Millisecond)

	if _, term, votedFor := n.snapshotState(); term != 42 || votedFor != None {
		t.Errorf("after stepping down: term=%d votedFor=%d, want 42 and None",
			term, votedFor)
	}
}
