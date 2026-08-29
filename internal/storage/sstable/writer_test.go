package sstable

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

func TestFlushThenReadBackEveryKey(t *testing.T) {
	m := memtable.NewWithSeed(1)
	const n = 5000
	want := make(map[string]string, n)
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := make([]byte, 1+rng.Intn(64))
		rng.Read(value)
		m.Put([]byte(key), value)
		want[key] = string(value)
	}

	path := filepath.Join(t.TempDir(), "flush.sst")
	info, err := Flush(m, path)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if info.Entries != n {
		t.Fatalf("info.Entries = %d, want %d", info.Entries, n)
	}
	if info.DataBlocks < 2 {
		t.Fatalf("info.DataBlocks = %d, want at least 2 (test data should span multiple blocks)", info.DataBlocks)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for key, wantValue := range want {
		value, tombstone, ok, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%q): ok = false, want true", key)
		}
		if tombstone {
			t.Fatalf("Get(%q): tombstone = true, want false", key)
		}
		if string(value) != wantValue {
			t.Fatalf("Get(%q) = %q, want %q", key, value, wantValue)
		}
	}
}

func TestFlushPreservesTombstones(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Delete([]byte("a"))

	path := filepath.Join(t.TempDir(), "flush.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	value, tombstone, ok, err := r.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if !ok || !tombstone {
		t.Fatalf("Get(a) = (value=%q, tombstone=%v, ok=%v), want (_, true, true)", value, tombstone, ok)
	}
	if value != nil {
		t.Fatalf("Get(a): value = %q, want nil for a tombstone", value)
	}

	value, tombstone, ok, err = r.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	if !ok || tombstone || string(value) != "2" {
		t.Fatalf("Get(b) = (value=%q, tombstone=%v, ok=%v), want (\"2\", false, true)", value, tombstone, ok)
	}
}

func TestGetOnMissingKeyReportsNotFoundNotTombstone(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("z"), []byte("2"))

	path := filepath.Join(t.TempDir(), "flush.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for _, key := range []string{"middle", "aa", "zzz", "\x00"} {
		value, tombstone, ok, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if ok {
			t.Fatalf("Get(%q): ok = true, want false", key)
		}
		if tombstone || value != nil {
			t.Fatalf("Get(%q) on a missing key returned tombstone=%v value=%q, want the zero value", key, tombstone, value)
		}
	}
}

func TestFlushRefusesAnEmptyMemtable(t *testing.T) {
	m := memtable.NewWithSeed(1)
	path := filepath.Join(t.TempDir(), "flush.sst")
	if _, err := Flush(m, path); err == nil {
		t.Fatal("Flush on an empty memtable: err = nil, want ErrEmptySource")
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("Flush on an empty memtable left a file at path despite returning an error")
	}
}

func TestFlushRefusesToOverwriteAnExistingFile(t *testing.T) {
	m1 := memtable.NewWithSeed(1)
	m1.Put([]byte("a"), []byte("1"))
	path := filepath.Join(t.TempDir(), "flush.sst")
	if _, err := Flush(m1, path); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	m2 := memtable.NewWithSeed(2)
	m2.Put([]byte("b"), []byte("2"))
	_, err := Flush(m2, path)
	if err == nil {
		t.Fatal("second Flush to the same path: err = nil, want ErrFileExists")
	}

	// The original file must be completely untouched.
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open after refused overwrite: %v", err)
	}
	defer r.Close()
	value, _, ok, err := r.Get([]byte("a"))
	if err != nil || !ok || string(value) != "1" {
		t.Fatalf("Get(a) after refused overwrite = (%q, ok=%v, err=%v), want (\"1\", true, nil)", value, ok, err)
	}
}

// TestWriteRejectsOutOfOrderSource exercises Write directly against a
// hand-built Source, since memtable.Iterator can never itself produce
// entries out of order -- this is the ErrOutOfOrder guard doing its job
// against the case the type system cannot rule out on its own.
func TestWriteRejectsOutOfOrderSource(t *testing.T) {
	src := &fakeSource{entries: []blockEntry{
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("a"), Value: []byte("1")},
	}}
	path := filepath.Join(t.TempDir(), "flush.sst")
	_, err := Write(src, path)
	if err == nil {
		t.Fatal("Write with out-of-order keys: err = nil, want ErrOutOfOrder")
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Fatal("Write with out-of-order keys left a partial file at path")
	}
	if _, statErr := os.Stat(path + ".tmp"); statErr == nil {
		t.Fatal("Write with out-of-order keys left its temp file behind")
	}
}

func TestNoBlockExceedsTargetSizeByMoreThanOneEntry(t *testing.T) {
	m := memtable.NewWithSeed(1)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k%06d", i)
		m.Put([]byte(key), make([]byte, 32))
	}
	path := filepath.Join(t.TempDir(), "flush.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// A single small entry is nowhere near targetBlockSize, so every
	// block should have room for many entries and none should wildly
	// exceed the target -- generously bounded here since the last entry
	// admitted into a block is not itself capped.
	for _, e := range r.index {
		if e.BlockLength > 2*targetBlockSize {
			t.Fatalf("block length %d far exceeds target %d", e.BlockLength, targetBlockSize)
		}
	}
	if len(r.index) < 5 {
		t.Fatalf("expected several blocks for %d small entries, got %d", 2000, len(r.index))
	}
}

// TestIndexIsSortedByLastKey pins the invariant Get's binary search
// depends on.
func TestIndexIsSortedByLastKey(t *testing.T) {
	m := memtable.NewWithSeed(1)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k%06d", i)
		m.Put([]byte(key), make([]byte, 32))
	}
	path := filepath.Join(t.TempDir(), "flush.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if !sort.SliceIsSorted(r.index, func(i, j int) bool {
		return string(r.index[i].LastKey) < string(r.index[j].LastKey)
	}) {
		t.Fatal("index entries are not sorted by LastKey")
	}
}

func TestOpenRejectsANonSSTableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-an-sstable")
	if err := os.WriteFile(path, []byte("this is not an sstable, and is shorter than a footer besides being the wrong bytes"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open on a non-SSTable file: err = nil, want ErrNotSSTable")
	}
}

func TestOpenRejectsFileShorterThanAFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "too-short")
	if err := os.WriteFile(path, []byte("short"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open on a file shorter than one footer: err = nil, want an error")
	}
}

func TestOpenRejectsAnIndexThatExtendsPastEndOfFile(t *testing.T) {
	// A syntactically valid footer whose IndexOffset/IndexLength point
	// past the (footer-less) portion of the file -- the shape a footer
	// separated from its own file by a bug, rather than corrupted in
	// place, would take.
	path := filepath.Join(t.TempDir(), "bad-index-bounds")
	buf := encodeFooter(footer{IndexOffset: 0, IndexLength: 1_000_000})
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open with an index extending past EOF: err = nil, want an error")
	}
}

func TestReaderPathAndNumBlocks(t *testing.T) {
	m := memtable.NewWithSeed(1)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k%06d", i)
		m.Put([]byte(key), make([]byte, 32))
	}
	path := filepath.Join(t.TempDir(), "flush.sst")
	info, err := Flush(m, path)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if r.Path() != path {
		t.Fatalf("Path() = %q, want %q", r.Path(), path)
	}
	if r.NumBlocks() != info.DataBlocks {
		t.Fatalf("NumBlocks() = %d, want %d (info.DataBlocks)", r.NumBlocks(), info.DataBlocks)
	}
}

// fakeSource lets tests drive Write directly without going through a real
// Memtable, for cases -- like out-of-order input -- a Memtable can never
// itself produce.
type fakeSource struct {
	entries []blockEntry
	i       int
}

func (s *fakeSource) Next() bool {
	if s.i >= len(s.entries) {
		return false
	}
	s.i++
	return true
}

func (s *fakeSource) Key() []byte     { return s.entries[s.i-1].Key }
func (s *fakeSource) Value() []byte   { return s.entries[s.i-1].Value }
func (s *fakeSource) Tombstone() bool { return s.entries[s.i-1].Tombstone }
func (s *fakeSource) Err() error      { return nil }

// failingSource yields a handful of well-formed entries and then reports
// a failure through Err rather than exhausting cleanly -- the shape a
// disk-backed sstable.Iterator (§13.8) takes when a block read fails
// partway through a scan, which is exactly the case Source.Err was added
// to let Write detect.
type failingSource struct {
	entries []blockEntry
	i       int
	failAt  int // Next returns false (as if failed) once i reaches this
	err     error
}

func (s *failingSource) Next() bool {
	if s.i >= s.failAt || s.i >= len(s.entries) {
		return false
	}
	s.i++
	return true
}

func (s *failingSource) Key() []byte     { return s.entries[s.i-1].Key }
func (s *failingSource) Value() []byte   { return s.entries[s.i-1].Value }
func (s *failingSource) Tombstone() bool { return s.entries[s.i-1].Tombstone }
func (s *failingSource) Err() error      { return s.err }

// TestWriteRefusesToPublishWhenTheSourceFails is the test
// Source.Err's own doc promises exists: a source that stops partway
// through with a real failure must not produce a file that looks like a
// complete, valid SSTable missing only the entries after the failure.
func TestWriteRefusesToPublishWhenTheSourceFails(t *testing.T) {
	wantErr := errors.New("simulated disk read failure")
	src := &failingSource{
		entries: []blockEntry{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
			{Key: []byte("c"), Value: []byte("3")},
		},
		failAt: 2, // yields "a" and "b" successfully, then fails
		err:    wantErr,
	}
	path := filepath.Join(t.TempDir(), "flush.sst")
	_, err := Write(src, path)
	if err == nil {
		t.Fatal("Write against a failing source: err = nil, want the propagated error")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write error = %v, want it to wrap %v", err, wantErr)
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Fatal("Write against a failing source left a truncated file at path -- must publish nothing at all")
	}
	if _, statErr := os.Stat(path + ".tmp"); statErr == nil {
		t.Fatal("Write against a failing source left its temp file behind")
	}
}

// TestWriteRefusesToPublishWhenTheSourceFailsImmediately checks the
// edge case where a source fails on its very first call, yielding zero
// entries -- this must be reported as the failure it is, not as
// ErrEmptySource, which would misdescribe a real I/O failure as an
// ordinary "nothing to write" case.
func TestWriteRefusesToPublishWhenTheSourceFailsImmediately(t *testing.T) {
	wantErr := errors.New("simulated failure on the first read")
	src := &failingSource{failAt: 0, err: wantErr}
	path := filepath.Join(t.TempDir(), "flush.sst")
	_, err := Write(src, path)
	if err == nil {
		t.Fatal("Write against an immediately-failing source: err = nil, want the propagated error")
	}
	if errors.Is(err, ErrEmptySource) {
		t.Fatal("Write against an immediately-failing source returned ErrEmptySource -- a real failure must not be misreported as an ordinary empty source")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write error = %v, want it to wrap %v", err, wantErr)
	}
}
