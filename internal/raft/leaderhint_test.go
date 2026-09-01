package raft

import (
	"path/filepath"
	"testing"
	"time"
)

// leaderHintNoopTransport never sends anything -- a single-node cluster
// (peers == nil) never has anyone to send to. Named distinctly from any
// stub transport this package's other test files may already define, to
// avoid a duplicate-symbol collision within the same package.
type leaderHintNoopTransport struct{}

func (leaderHintNoopTransport) SendRequestVote(int, *RequestVoteArgs, *RequestVoteReply) bool {
	return false
}
func (leaderHintNoopTransport) SendPreVote(int, *PreVoteArgs, *PreVoteReply) bool { return false }
func (leaderHintNoopTransport) SendAppendEntries(int, *AppendEntriesArgs, *AppendEntriesReply) bool {
	return false
}
func (leaderHintNoopTransport) SendInstallSnapshot(int, *InstallSnapshotArgs, *InstallSnapshotReply) bool {
	return false
}

// TestLeaderHintIsNoneBeforeAnyElection locks in the zero-state answer a
// gRPC server (F-2) gets asking before this node has ever heard from, or
// become, a leader -- the exact moment a NotLeader response's own
// leader_id detail has nothing better to report, and must say so rather
// than report a fabricated or zero-valued node ID that looks like a
// real answer.
func TestLeaderHintIsNoneBeforeAnyElection(t *testing.T) {
	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := OpenNode(7, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	if got := n.LeaderHint(); got != None {
		t.Errorf("LeaderHint() before any election = %d, want None (%d)", got, None)
	}
}

// TestLeaderHintIsSelfAfterWinningAnElection is the other half: once a
// single-node cluster has elected itself (its only possible outcome),
// LeaderHint must report its own id -- the case a gRPC server actually
// needs the field for once a real multi-node cluster exists and this
// node is asked to redirect a client to whichever OTHER node currently
// leads.
func TestLeaderHintIsSelfAfterWinningAnElection(t *testing.T) {
	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	const id = 7
	n, err := OpenNode(id, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	n.Start()
	defer n.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, isLeader := n.Submit([]byte("probe")); isLeader {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if got := n.LeaderHint(); got != id {
		t.Errorf("LeaderHint() after winning its own election = %d, want %d (self)", got, id)
	}
}
