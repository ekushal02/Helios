package raft

import (
	"path/filepath"
	"testing"
	"time"
)

// TestStatusBeforeAnyElection locks in the zero-state answer a fresh
// node reports before it has ever heard from, or become, a leader --
// every field here is deterministic even this early, since NewNode/
// OpenNode already sets this node's own single-member configuration
// (Voters) before Start is ever called.
func TestStatusBeforeAnyElection(t *testing.T) {
	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := OpenNode(7, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	st := n.Status()
	if st.ID != 7 {
		t.Errorf("ID = %d, want 7", st.ID)
	}
	if st.State != Follower {
		t.Errorf("State = %v, want Follower", st.State)
	}
	if st.Term != 0 {
		t.Errorf("Term = %d, want 0", st.Term)
	}
	if st.LeaderID != None {
		t.Errorf("LeaderID = %d, want None (%d)", st.LeaderID, None)
	}
	if st.CommitIndex != 0 || st.LastApplied != 0 {
		t.Errorf("CommitIndex/LastApplied = %d/%d, want 0/0", st.CommitIndex, st.LastApplied)
	}
	if st.LogLength != 1 {
		t.Errorf("LogLength = %d, want 1 (just the leading placeholder entry)", st.LogLength)
	}
	if st.SnapshotIndex != 0 || st.SnapshotTerm != 0 {
		t.Errorf("SnapshotIndex/SnapshotTerm = %d/%d, want 0/0", st.SnapshotIndex, st.SnapshotTerm)
	}
	if len(st.Voters) != 1 || st.Voters[0] != 7 {
		t.Errorf("Voters = %v, want [7]", st.Voters)
	}
}

// TestStatusAfterWinningAnElectionAndSubmitting proves every field
// that SHOULD change once this node has real activity behind it
// actually does: State becomes Leader, Term and LogLength advance,
// CommitIndex/LastApplied catch up to a real submitted entry.
func TestStatusAfterWinningAnElectionAndSubmitting(t *testing.T) {
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

	// LastApplied only advances when the applier hands a committed
	// entry over to a real consumer of n.ApplyCh() -- in production
	// that consumer is kvstore.Machine.run(); this test has no
	// Machine, so without a drain goroutine here, the applier would
	// block forever trying to hand over the very first entry and
	// LastApplied would never leave 0, no matter how long Submit's own
	// commit succeeds. CommitIndex has no such dependency (it advances
	// from the replication/quorum protocol alone), which is why only
	// this half needs its own consumer.
	go func() {
		for range n.ApplyCh() {
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	var submittedIndex int
	for time.Now().Before(deadline) {
		if idx, _, isLeader := n.Submit([]byte("probe")); isLeader {
			submittedIndex = idx
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if submittedIndex == 0 {
		t.Fatal("node did not become leader within 3s")
	}

	// A single-node cluster commits and applies its own entries
	// immediately (its own vote is already a majority); poll briefly
	// rather than assume zero latency.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().LastApplied >= submittedIndex {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	st := n.Status()
	if st.State != Leader {
		t.Errorf("State = %v, want Leader", st.State)
	}
	if st.Term < 1 {
		t.Errorf("Term = %d, want >= 1 (an election must have advanced it)", st.Term)
	}
	if st.LeaderID != id {
		t.Errorf("LeaderID = %d, want %d (self)", st.LeaderID, id)
	}
	if st.CommitIndex < submittedIndex {
		t.Errorf("CommitIndex = %d, want >= %d", st.CommitIndex, submittedIndex)
	}
	if st.LastApplied < submittedIndex {
		t.Errorf("LastApplied = %d, want >= %d", st.LastApplied, submittedIndex)
	}
	if st.LogLength < submittedIndex+1 { // +1 for the leading placeholder
		t.Errorf("LogLength = %d, want >= %d (placeholder plus every entry up to and including the submitted one)", st.LogLength, submittedIndex+1)
	}
	if len(st.Voters) != 1 || st.Voters[0] != id {
		t.Errorf("Voters = %v, want [%d]", st.Voters, id)
	}
}

// TestStatusVotersIsACopy proves a caller mutating the returned slice
// cannot corrupt this Node's own configuration -- Status's own doc
// promises a copy, checked directly rather than trusted.
func TestStatusVotersIsACopy(t *testing.T) {
	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := OpenNode(1, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	st := n.Status()
	st.Voters[0] = 999 // mutate the caller's own copy

	st2 := n.Status()
	if st2.Voters[0] == 999 {
		t.Fatal("mutating Status()'s returned Voters slice affected a later call -- Voters is not an independent copy")
	}
}