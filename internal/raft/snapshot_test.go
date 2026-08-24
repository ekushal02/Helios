package raft

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// noSnapshots
// =============================================================================

// noSnapshots gives a test double the snapshot half of Storage without each one
// growing its own copy. Embed it; the doubles that care about crash semantics
// can override.
type noSnapshots struct {
	mu   sync.Mutex
	snap []byte
}

func (s *noSnapshots) SaveSnapshot(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = append([]byte(nil), b...)
	return nil
}

func (s *noSnapshots) LoadSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snap == nil {
		return nil, nil
	}
	return append([]byte(nil), s.snap...), nil
}

// =============================================================================
// The record
// =============================================================================

func TestSnapshotRoundTripsThroughEncoding(t *testing.T) {
	want := Snapshot{
		LastIncludedIndex: 4096,
		LastIncludedTerm:  7,
		Data:              []byte("a=1\nb=2\nc=3\n"),
	}

	b, err := encodeSnapshot(want)
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	got, err := decodeSnapshot(b)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}

	if got.LastIncludedIndex != want.LastIncludedIndex {
		t.Errorf("LastIncludedIndex = %d, want %d", got.LastIncludedIndex, want.LastIncludedIndex)
	}
	if got.LastIncludedTerm != want.LastIncludedTerm {
		t.Errorf("LastIncludedTerm = %d, want %d", got.LastIncludedTerm, want.LastIncludedTerm)
	}
	if !bytes.Equal(got.Data, want.Data) {
		t.Errorf("Data = %q, want %q", got.Data, want.Data)
	}
}

// An empty image at a real index is legal and is not the same as no snapshot.
// A state machine whose keys have all been deleted looks exactly like this.
func TestAnEmptyImageIsStillASnapshot(t *testing.T) {
	b, err := encodeSnapshot(Snapshot{LastIncludedIndex: 12, LastIncludedTerm: 3})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	got, err := decodeSnapshot(b)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}
	if got.LastIncludedIndex != 12 || got.LastIncludedTerm != 3 {
		t.Errorf("floor = (%d, %d), want (12, 3)", got.LastIncludedIndex, got.LastIncludedTerm)
	}
	if len(got.Data) != 0 {
		t.Errorf("Data = %q, want empty", got.Data)
	}
}

// Large images are the normal case, and the framing must not care.
func TestALargeImageSurvivesTheFraming(t *testing.T) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i * 7)
	}

	b, err := encodeSnapshot(Snapshot{LastIncludedIndex: 9, LastIncludedTerm: 2, Data: data})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	got, err := decodeSnapshot(b)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Errorf("a %d-byte image did not survive the round trip", len(data))
	}
}

// The decoded image must not alias the buffer it was read out of.
func TestDecodeCopiesTheImage(t *testing.T) {
	b, err := encodeSnapshot(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("original")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	got, err := decodeSnapshot(b)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}

	for i := range b {
		b[i] = 0xFF
	}
	if string(got.Data) != "original" {
		t.Errorf("Data = %q after the source buffer was overwritten: decode aliased it", got.Data)
	}
}

func TestDecodeRejectsDamagedSnapshots(t *testing.T) {
	good, err := encodeSnapshot(Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 2, Data: []byte("payload")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}

	damage := map[string]func([]byte) []byte{
		"empty":          func(b []byte) []byte { return nil },
		"header only":    func(b []byte) []byte { return b[:snapshotHeaderLen] },
		"truncated body": func(b []byte) []byte { return b[:len(b)-3] },
		"wrong magic":    func(b []byte) []byte { c := clone(b); c[1] = 'X'; return c },
		"wrong version":  func(b []byte) []byte { c := clone(b); c[7] = 9; return c },
		"flipped bit":    func(b []byte) []byte { c := clone(b); c[len(c)-1] ^= 0x01; return c },
		"state record":   func(b []byte) []byte { s, _ := encodeState(persistentState{CurrentTerm: 1}); return s },
		"not a record":   func(b []byte) []byte { return []byte("this is not a snapshot") },
	}

	for name, break_ := range damage {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSnapshot(break_(good)); !errors.Is(err, ErrCorruptSnapshot) {
				t.Errorf("err = %v, want ErrCorruptSnapshot", err)
			}
		})
	}
}

// A header with a field left at zero is not a snapshot of nothing; it is a
// snapshot nobody filled in. Adopting it would move a node's floor backwards to
// the sentinel and silently discard the image it was supposed to describe.
func TestAnUnsetFloorIsRefused(t *testing.T) {
	cases := map[string]Snapshot{
		"no index":       {LastIncludedIndex: 0, LastIncludedTerm: 3, Data: []byte("x")},
		"no term":        {LastIncludedIndex: 9, LastIncludedTerm: 0, Data: []byte("x")},
		"neither":        {Data: []byte("x")},
		"negative index": {LastIncludedIndex: -1, LastIncludedTerm: 3},
	}

	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := encodeSnapshot(s); !errors.Is(err, ErrCorruptSnapshot) {
				t.Errorf("encodeSnapshot err = %v, want ErrCorruptSnapshot", err)
			}
		})
	}
}

// =============================================================================
// Storage
// =============================================================================

func TestSnapshotAndStateAreIndependentRecords(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	state, err := encodeState(persistentState{CurrentTerm: 4, VotedFor: 1, Log: []LogEntry{{Term: 0}}})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	snap, err := encodeSnapshot(Snapshot{LastIncludedIndex: 8, LastIncludedTerm: 3, Data: []byte("image")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}

	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Rewriting one must not disturb the other. This is the whole point of the
	// split: the state record is rewritten on every term change, and a snapshot
	// must not be re-emitted alongside it.
	for i := 0; i < 5; i++ {
		if err := s.Save(state); err != nil {
			t.Fatalf("Save %d: %v", i, err)
		}
	}

	reopened, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	b, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	got, err := decodeSnapshot(b)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}
	if got.LastIncludedIndex != 8 || string(got.Data) != "image" {
		t.Errorf("snapshot came back as (%d, %q), want (8, %q)",
			got.LastIncludedIndex, got.Data, "image")
	}
}

func TestLoadSnapshotOnAFreshDirectoryMeansNoneNotBroken(t *testing.T) {
	s, err := NewFileStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	b, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if b != nil {
		t.Errorf("LoadSnapshot returned %d bytes, want nil", len(b))
	}
}

// The residue of a SaveSnapshot killed before its rename is not a snapshot.
func TestAbandonedSnapshotTempFileIsNeverAdopted(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	good, err := encodeSnapshot(Snapshot{LastIncludedIndex: 6, LastIncludedTerm: 2, Data: []byte("kept")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	if err := s.SaveSnapshot(good); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	tmp := filepath.Join(dir, snapshotTempName)
	if err := os.WriteFile(tmp, []byte("half an image"), 0o600); err != nil {
		t.Fatalf("writing temp: %v", err)
	}

	reopened, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage after crash: %v", err)
	}
	b, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	got, err := decodeSnapshot(b)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}
	if string(got.Data) != "kept" {
		t.Errorf("Data = %q, want the pre-crash image", got.Data)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Error("snapshot temp file survived NewFileStorage")
	}
}

// =============================================================================
// The ordering rule
// =============================================================================

// THE CRASH WINDOW THE ORDERING RULE EXISTS FOR.
//
// A snapshot is written, and the process dies before the truncated log record
// follows it. The log still holds every entry the snapshot covers, so the pair
// is redundant rather than contradictory and the node recovers.
//
// The reverse order has no test because it has no acceptable outcome: entries
// discarded with nothing to account for them is lost committed state. That is
// why SaveSnapshot goes first, and why the obligation is written on the
// interface rather than left to whoever calls it next.
func TestASnapshotWrittenWithoutTheTruncationIsRecoverable(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	// The log as it stood before the snapshot: entries 1..5, none discarded.
	full := []LogEntry{{Term: 0}}
	for i := 1; i <= 5; i++ {
		full = append(full, LogEntry{Term: 2, Command: []byte{byte(i)}})
	}
	state, err := encodeState(persistentState{CurrentTerm: 2, VotedFor: 0, Log: full})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	if err := s.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// The snapshot lands. The Save that would discard 1..3 never happens.
	snap, err := encodeSnapshot(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 2, Data: []byte("image")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, s)
	if err != nil {
		t.Fatalf("OpenNode refused a redundant but consistent pair: %v", err)
	}
	defer n.Stop()

	n.mu.Lock()
	defer n.mu.Unlock()

	idx, term := n.snapshotFloor()
	if idx != 3 || term != 2 {
		t.Errorf("floor = (%d, %d), want (3, 2)", idx, term)
	}
	if n.lastLogIndex() != 5 {
		t.Errorf("lastLogIndex = %d, want 5: the untruncated tail must survive", n.lastLogIndex())
	}
}

// The pair that cannot arise from any correct sequence of writes: a log that
// stops short of the floor. Refusing to start is the only honest response --
// entries are missing and nothing left on disk knows what they were.
func TestALogThatStopsShortOfTheFloorIsRefused(t *testing.T) {
	store := NewMemoryStorage()

	state, err := encodeState(persistentState{
		CurrentTerm: 2,
		VotedFor:    0,
		Log:         []LogEntry{{Term: 0}, {Term: 2}},
	})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	if err := store.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	snap, err := encodeSnapshot(Snapshot{LastIncludedIndex: 9, LastIncludedTerm: 2, Data: []byte("image")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	if err := store.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err == nil {
		if n != nil {
			n.Stop()
		}
		t.Fatal("OpenNode started on a log that does not reach the snapshot floor")
	}
	if n != nil {
		t.Error("OpenNode returned a node alongside the error")
	}
}

// A floor whose term disagrees with the entry the log still holds at that index
// means the two records come from different histories.
func TestASnapshotFromADifferentHistoryIsRefused(t *testing.T) {
	store := NewMemoryStorage()

	state, err := encodeState(persistentState{
		CurrentTerm: 5,
		VotedFor:    0,
		Log:         []LogEntry{{Term: 0}, {Term: 2}, {Term: 2}, {Term: 4}},
	})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	if err := store.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// The log says index 3 is term 4. The snapshot claims term 3.
	snap, err := encodeSnapshot(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 3, Data: []byte("image")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	if err := store.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	if _, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store); err == nil {
		t.Fatal("OpenNode accepted a snapshot whose floor term contradicts the log")
	}
}

func TestOpenNodeRefusesACorruptSnapshot(t *testing.T) {
	store := NewMemoryStorage()
	if err := store.SaveSnapshot([]byte("not a snapshot record")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err = %v, want ErrCorruptSnapshot", err)
	}
	if n != nil {
		t.Error("OpenNode returned a node alongside the error: a caller ignoring " +
			"err gets a node whose state machine silently rewound to empty")
	}
}

// =============================================================================
// Recovery
// =============================================================================

// A node with no snapshot has its floor at the sentinel. This is not a special
// case: (0, 0) is exactly log[0]'s index and term, which is why every existing
// consistency check at index 0 already behaves correctly.
func TestANodeWithNoSnapshotHasItsFloorAtTheSentinel(t *testing.T) {
	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, NewMemoryStorage())
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	n.mu.Lock()
	defer n.mu.Unlock()

	idx, term := n.snapshotFloor()
	if idx != 0 || term != 0 {
		t.Errorf("floor = (%d, %d), want (0, 0)", idx, term)
	}
	if n.hasSnapshot() {
		t.Error("hasSnapshot true on a node that has never taken one")
	}
	if n.commitIndex != 0 || n.lastApplied != 0 {
		t.Errorf("commit/applied = %d/%d, want 0/0", n.commitIndex, n.lastApplied)
	}
}

// SCOPE FENCE (snapshot delivery to the state machine).
//
// commitIndex and lastApplied move to the floor, because everything a snapshot
// covers is by definition committed and applied. That is correct only once the
// image is handed to the state machine on restart, and nothing does that yet --
// ApplyMsg has no snapshot arm, so a restarted node currently believes it is
// caught up while its state machine is empty.
//
// It is safe today only because nothing writes a snapshot outside these tests.
// When the delivery path lands, extend this test to assert the image reaches
// ApplyCh before any entry above the floor does.
func TestOpenNodeAdoptsTheSnapshotFloor(t *testing.T) {
	store := NewMemoryStorage()

	full := []LogEntry{{Term: 0}}
	for i := 1; i <= 6; i++ {
		full = append(full, LogEntry{Term: 3, Command: []byte{byte(i)}})
	}
	state, err := encodeState(persistentState{CurrentTerm: 3, VotedFor: 1, Log: full})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	if err := store.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	snap, err := encodeSnapshot(Snapshot{LastIncludedIndex: 4, LastIncludedTerm: 3, Data: []byte("kv image")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	if err := store.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	n.mu.Lock()
	defer n.mu.Unlock()

	idx, term := n.snapshotFloor()
	if idx != 4 || term != 3 {
		t.Errorf("floor = (%d, %d), want (4, 3)", idx, term)
	}
	if !n.hasSnapshot() {
		t.Error("hasSnapshot false with a snapshot on disk")
	}
	if n.commitIndex != 4 {
		t.Errorf("commitIndex = %d, want 4: everything a snapshot covers is committed", n.commitIndex)
	}
	if n.lastApplied != 4 {
		t.Errorf("lastApplied = %d, want 4: leaving it at 0 would tell the applier "+
			"to redeliver entries that will not exist once truncation lands", n.lastApplied)
	}
	if n.currentTerm != 3 || n.votedFor != 1 {
		t.Errorf("term/vote = %d/%d, want 3/1: the state record must still be adopted",
			n.currentTerm, n.votedFor)
	}
}

// A node that comes up with an image on disk hands it to the state machine
// without anything else prompting it.
//
// THE BUG THIS PINS. The applier is started inside NewNode, so it is already
// running when OpenNode parks the image. If OpenNode advanced commitIndex by
// assignment rather than through commitTo, the applier's first pass would find
// nothing, park on applyNotify, and never wake -- a node that is up, holding a
// loaded image, with a state machine that stays empty. No error and nothing in
// the log, and entirely dependent on which goroutine wins a scheduling race.
func TestARestartDeliversTheImageWithoutPrompting(t *testing.T) {
	store := NewMemoryStorage()

	blob, err := encodeSnapshot(Snapshot{
		LastIncludedIndex: 12, LastIncludedTerm: 3, Data: []byte("image"),
	})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	if err := store.SaveSnapshot(blob); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	full := []LogEntry{{Term: 0}}
	for i := 1; i <= 15; i++ {
		full = append(full, LogEntry{Term: 3, Command: []byte{byte(i)}})
	}
	state, err := encodeState(persistentState{CurrentTerm: 3, VotedFor: 1, Log: full})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	if err := store.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	select {
	case msg, ok := <-n.ApplyCh():
		if !ok {
			t.Fatal("apply channel closed")
		}
		if !msg.SnapshotValid {
			t.Fatalf("first delivery was index %d, want the image", msg.CommandIndex)
		}
		if msg.SnapshotIndex != 12 {
			t.Errorf("image index = %d, want 12", msg.SnapshotIndex)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the image never arrived: OpenNode parked it without waking the applier")
	}
}
