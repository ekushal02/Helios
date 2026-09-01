package kvstore

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// noopTransport never sends anything -- a single-node cluster (peers ==
// nil) never has anyone to send to, so every method here is unreachable
// in these tests, present only to satisfy raft.Transport's interface.
type noopTransport struct{}

func (noopTransport) SendRequestVote(int, *raft.RequestVoteArgs, *raft.RequestVoteReply) bool {
	return false
}
func (noopTransport) SendPreVote(int, *raft.PreVoteArgs, *raft.PreVoteReply) bool { return false }
func (noopTransport) SendAppendEntries(int, *raft.AppendEntriesArgs, *raft.AppendEntriesReply) bool {
	return false
}
func (noopTransport) SendInstallSnapshot(int, *raft.InstallSnapshotArgs, *raft.InstallSnapshotReply) bool {
	return false
}

// newTestNode builds a real, single-node raft.Node backed by real
// on-disk FileStorage (its own subdirectory, separate from the
// Machine's own data directory the way a real deployment keeps Raft's
// log apart from the state machine's data) and starts it. A one-node
// cluster is its own majority, so it elects itself leader on its own
// within one election timeout, with nothing to wait on but that timer --
// the identical "single node as its own control" shape
// TestSingleNodeAppliesEveryPutInOrder (e2e_test.go) already uses inside
// package raft itself.
func newTestNode(t *testing.T, dir string) *raft.Node {
	t.Helper()
	storage, err := raft.NewFileStorage(filepath.Join(dir, "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := raft.OpenNode(1, nil, noopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	n.Start()
	t.Cleanup(n.Stop)
	return n
}

// waitForLeader polls Submit with a harmless probe write until this
// node reports isLeader -- a single-node cluster always eventually wins
// its own election, so this is a bounded wait, not a real retry loop.
func waitForLeader(t *testing.T, n *raft.Node, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, _, isLeader := n.Submit(encodePut([]byte("__probe__"), nil, 0, 0)); isLeader {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("node did not become leader within %v", within)
}

func newTestMachine(t *testing.T, dir string, n *raft.Node, opts Options) *Machine {
	t.Helper()
	cache := sstable.NewBlockCache(1 << 20)
	m, err := NewMachine(n, filepath.Join(dir, "kv"), cache, opts)
	if err != nil {
		t.Fatalf("NewMachine: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func TestPutThenGetRoundTrips(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	if err := m.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, ok, err := m.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "1" {
		t.Fatalf("Get(a) = (%q, ok=%v), want (\"1\", true)", value, ok)
	}
	if fault := m.Fault(); fault != "" {
		t.Fatalf("Fault() = %q, want empty", fault)
	}
}

func TestDeleteThenGetReportsAbsent(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	if err := m.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := m.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	value, ok, err := m.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok || value != nil {
		t.Fatalf("Get(a) after Delete = (%q, ok=%v), want (nil, false)", value, ok)
	}
}

func TestGetOnAKeyNeverWrittenReportsAbsent(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	value, ok, err := m.Get([]byte("never-written"))
	if err != nil || ok || value != nil {
		t.Fatalf("Get(never-written) = (%q, ok=%v, err=%v), want (nil, false, nil)", value, ok, err)
	}
}

func TestGetLeaseReadAgreesWithGet(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	if err := m.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, ok, leaseValid, err := m.GetLeaseRead([]byte("a"))
	if err != nil {
		t.Fatalf("GetLeaseRead: %v", err)
	}
	if !leaseValid {
		t.Fatal("GetLeaseRead: leaseValid = false, want true (a fresh single-node leader should have a valid lease)")
	}
	if !ok || string(value) != "1" {
		t.Fatalf("GetLeaseRead(a) = (%q, ok=%v), want (\"1\", true)", value, ok)
	}
}

// TestManyPutsAndOverwritesEndUpCorrect is the end-to-end property this
// whole task exists to prove: a real sequence of writes, some of them
// overwrites and deletes, through the real apply path, comes out exactly
// right on read -- the same "last write wins" property
// TestThreeNodesEndUpWithIdenticalStateMachines (e2e_test.go) checks
// against a plain map, checked here against the full storage engine
// instead.
func TestManyPutsAndOverwritesEndUpCorrect(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	const (
		ops      = 300
		keyCount = 20
	)
	want := map[string]string{}
	deleted := map[string]bool{}

	for i := 0; i < ops; i++ {
		key := fmt.Sprintf("k%02d", i%keyCount)
		if i%7 == 0 && want[key] != "" {
			if err := m.Delete([]byte(key)); err != nil {
				t.Fatalf("Delete(%d): %v", i, err)
			}
			delete(want, key)
			deleted[key] = true
			continue
		}
		val := fmt.Sprintf("v%04d", i)
		if err := m.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		want[key] = val
		deleted[key] = false
	}

	for key, wantVal := range want {
		value, ok, err := m.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !ok || string(value) != wantVal {
			t.Errorf("Get(%q) = (%q, ok=%v), want (%q, true)", key, value, ok, wantVal)
		}
	}
	for key, wasDeleted := range deleted {
		if !wasDeleted {
			continue
		}
		if _, stillThere := want[key]; stillThere {
			continue // overwritten again after its delete
		}
		_, ok, err := m.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if ok {
			t.Errorf("Get(%q): ok = true, want false (deleted and never rewritten)", key)
		}
	}
	if fault := m.Fault(); fault != "" {
		t.Fatalf("Fault() = %q, want empty", fault)
	}
}
