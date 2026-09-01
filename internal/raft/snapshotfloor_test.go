package raft

import (
	"path/filepath"
	"testing"
	"time"
)

// TestSnapshotFloorIsZeroBeforeAnySnapshot locks in the natural
// zero-state answer for a freshly-opened node that has never taken or
// installed a snapshot -- the exact moment Watch's own retained-history
// floor (internal/kvstore) has nothing to report either.
func TestSnapshotFloorIsZeroBeforeAnySnapshot(t *testing.T) {
	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := OpenNode(1, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	index, term := n.SnapshotFloor()
	if index != 0 || term != 0 {
		t.Errorf("SnapshotFloor() = (%d, %d), want (0, 0)", index, term)
	}
}

// TestSnapshotFloorAdvancesAfterASnapshot proves the accessor reflects
// a real snapshot once one has actually been taken, via n.Snapshot
// directly -- the same public entry point kvstore.Machine's own take()
// uses -- rather than reaching into unexported fields to fake the
// state.
func TestSnapshotFloorAdvancesAfterASnapshot(t *testing.T) {
	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := OpenNode(1, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	n.Start()
	defer n.Stop()

	deadline := time.Now().Add(3 * time.Second)
	var idx int
	for time.Now().Before(deadline) {
		if i, _, isLeader := n.Submit([]byte("probe")); isLeader {
			idx = i
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if idx == 0 {
		t.Fatal("node did not become leader within 3s")
	}

	if err := n.Snapshot(idx, []byte("fake-image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	gotIndex, _ := n.SnapshotFloor()
	if gotIndex != idx {
		t.Errorf("SnapshotFloor() index = %d, want %d", gotIndex, idx)
	}
}
