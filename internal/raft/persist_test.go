package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// failingStorage lets a test watch what a node does when the disk says no.
type failingStorage struct {
	noSnapshots
	inner Storage
	fail  error
	saves int
}

func (s *failingStorage) Save(b []byte) error {
	s.saves++
	if s.fail != nil {
		return s.fail
	}
	return s.inner.Save(b)
}

func (s *failingStorage) Load() ([]byte, error) { return s.inner.Load() }

// =============================================================================
// Encoding
// =============================================================================

func TestStateRoundTripsThroughEncoding(t *testing.T) {
	want := persistentState{
		CurrentTerm: 9,
		VotedFor:    2,
		Log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: []byte("put a 1")},
			{Term: 4, NoOp: true},
			{Term: 9, Command: []byte("put b 2")},
		},
	}

	b, err := encodeState(want)
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	got, err := decodeState(b)
	if err != nil {
		t.Fatalf("decodeState: %v", err)
	}

	if got.CurrentTerm != want.CurrentTerm || got.VotedFor != want.VotedFor {
		t.Errorf("term/vote = %d/%d, want %d/%d",
			got.CurrentTerm, got.VotedFor, want.CurrentTerm, want.VotedFor)
	}
	if len(got.Log) != len(want.Log) {
		t.Fatalf("log length = %d, want %d", len(got.Log), len(want.Log))
	}
	for i := range want.Log {
		if got.Log[i].Term != want.Log[i].Term ||
			got.Log[i].NoOp != want.Log[i].NoOp ||
			string(got.Log[i].Command) != string(want.Log[i].Command) {
			t.Errorf("log[%d] = %+v, want %+v", i, got.Log[i], want.Log[i])
		}
	}
}

// A vote for node 0 is the interesting case: 0 is gob's omitted value, so a
// decoder that reused a non-zero target would silently report the wrong vote.
func TestVoteForNodeZeroSurvivesEncoding(t *testing.T) {
	b, err := encodeState(persistentState{CurrentTerm: 3, VotedFor: 0, Log: []LogEntry{{Term: 0}}})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	got, err := decodeState(b)
	if err != nil {
		t.Fatalf("decodeState: %v", err)
	}
	if got.VotedFor != 0 {
		t.Errorf("votedFor = %d, want 0", got.VotedFor)
	}
	if got.VotedFor == None {
		t.Error("a vote for node 0 decoded as no vote at all")
	}
}

func TestDecodeRejectsDamagedRecords(t *testing.T) {
	good, err := encodeState(persistentState{CurrentTerm: 5, VotedFor: 1, Log: []LogEntry{{Term: 0}, {Term: 5}}})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}

	damage := map[string]func([]byte) []byte{
		"empty":            func(b []byte) []byte { return nil },
		"header only":      func(b []byte) []byte { return b[:headerLen] },
		"truncated body":   func(b []byte) []byte { return b[:len(b)-3] },
		"wrong magic":      func(b []byte) []byte { c := clone(b); c[1] = 'X'; return c },
		"wrong version":    func(b []byte) []byte { c := clone(b); c[7] = 9; return c },
		"flipped bit":      func(b []byte) []byte { c := clone(b); c[len(c)-1] ^= 0x01; return c },
		"garbage entirely": func(b []byte) []byte { return []byte("this is not a raft state file") },
	}

	for name, break_ := range damage {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeState(break_(good)); !errors.Is(err, ErrCorruptState) {
				t.Errorf("err = %v, want ErrCorruptState", err)
			}
		})
	}
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }

// =============================================================================
// FileStorage
// =============================================================================

func TestLoadOnAnEmptyDirectoryMeansFreshNotBroken(t *testing.T) {
	s, err := NewFileStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	b, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if b != nil {
		t.Errorf("Load returned %d bytes, want nil", len(b))
	}
}

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if err := s.Save([]byte("first")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save([]byte("second")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// A brand new handle, as a restart would build.
	s2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	b, err := s2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(b) != "second" {
		t.Errorf("Load = %q, want %q", b, "second")
	}
}

// The residue of a Save that was killed before its rename must not be mistaken
// for state. This is the file a hard kill leaves behind.
func TestAbandonedTempFileIsNeverAdopted(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if err := s.Save([]byte("the acknowledged record")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Simulate the crash: a half-written temp file, no rename.
	tmp := filepath.Join(dir, tempFileName)
	if err := os.WriteFile(tmp, []byte("half a rec"), 0o600); err != nil {
		t.Fatalf("writing temp: %v", err)
	}

	s2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage after crash: %v", err)
	}
	b, err := s2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(b) != "the acknowledged record" {
		t.Errorf("Load = %q, want the pre-crash record", b)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Error("temp file survived NewFileStorage; a later Save would inherit its tail")
	}
}

// =============================================================================
// Node
// =============================================================================

func TestOpenNodeRestoresTermVoteAndLog(t *testing.T) {
	store := NewMemoryStorage()

	saved := persistentState{
		CurrentTerm: 7,
		VotedFor:    2,
		Log:         []LogEntry{{Term: 0}, {Term: 3, Command: []byte("x")}, {Term: 7, Command: []byte("y")}},
	}
	b, err := encodeState(saved)
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	if err := store.Save(b); err != nil {
		t.Fatalf("Save: %v", err)
	}

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.currentTerm != 7 {
		t.Errorf("currentTerm = %d, want 7", n.currentTerm)
	}
	if n.votedFor != 2 {
		t.Errorf("votedFor = %d, want 2", n.votedFor)
	}
	if got := n.lastLogIndex(); got != 2 {
		t.Errorf("lastLogIndex = %d, want 2", got)
	}
	if got := n.lastLogTerm(); got != 7 {
		t.Errorf("lastLogTerm = %d, want 7", got)
	}
	if n.state != Follower {
		t.Errorf("state = %v, want follower: leadership is volatile", n.state)
	}
	// Volatile by Figure 2, and the applier depends on it.
	if n.commitIndex != 0 || n.lastApplied != 0 {
		t.Errorf("commit/applied = %d/%d, want 0/0", n.commitIndex, n.lastApplied)
	}
}

// The single most dangerous shortcut in this whole task is treating an
// unreadable state file as a fresh node.
func TestOpenNodeRefusesToStartOnCorruptState(t *testing.T) {
	store := NewMemoryStorage()
	if err := store.Save([]byte("not a raft state record")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("err = %v, want ErrCorruptState", err)
	}
	if n != nil {
		t.Error("OpenNode returned a node alongside the error; a caller ignoring err gets a term-0 voter")
	}
}

func TestPersistIsSkippedWhenNothingChanged(t *testing.T) {
	store := &failingStorage{inner: NewMemoryStorage()}
	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	n.mu.Lock()
	n.persistIfDirty()
	n.persistIfDirty()
	n.mu.Unlock()

	if store.saves != 0 {
		t.Errorf("saves = %d, want 0: an unchanged node must not fsync per RPC", store.saves)
	}

	n.mu.Lock()
	n.currentTerm = 4
	n.markDirty()
	n.persistIfDirty()
	n.persistIfDirty()
	n.mu.Unlock()

	if store.saves != 1 {
		t.Errorf("saves = %d, want 1", store.saves)
	}
}

func TestPersistFailureStopsTheNode(t *testing.T) {
	store := &failingStorage{inner: NewMemoryStorage(), fail: errors.New("disk full")}
	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Error("a node that cannot persist carried on regardless")
		}
	}()

	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm = 1
	n.markDirty()
	n.persistIfDirty()
}
