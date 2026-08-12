package raft

import "testing"

func TestNewNode(t *testing.T) {
	n := NewNode(1)

	if n.state != Follower {
		t.Errorf("expected follower, got %v", n.state)
	}

	if n.currentTerm != 0 {
		t.Errorf("expected term 0, got %d", n.currentTerm)
	}

	if n.votedFor != None {
		t.Errorf("expected no vote, got %d", n.votedFor)
	}

	if n.lastLogIndex() != 0 {
		t.Errorf("expected last log index 0, got %d", n.lastLogIndex())
	}

	if n.lastLogTerm() != 0 {
		t.Errorf("expected last log term 0, got %d", n.lastLogTerm())
	}
}