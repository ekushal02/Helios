package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

func TestFlushIfFullDoesNothingBelowThreshold(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("1")) // ApproxSize = 2 (1-byte key + 1-byte value)

	path := filepath.Join(t.TempDir(), "flush.sst")
	flushed, info, err := FlushIfFull(m, 1<<20, path)
	if err != nil {
		t.Fatalf("FlushIfFull: %v", err)
	}
	if flushed {
		t.Fatal("FlushIfFull below threshold: flushed = true, want false")
	}
	if info != nil {
		t.Fatalf("FlushIfFull below threshold: info = %+v, want nil", info)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("FlushIfFull below threshold created a file anyway")
	}
}

func TestFlushIfFullFlushesAtOrAboveThreshold(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("aaaaaaaaaa"), []byte("bbbbbbbbbb")) // ApproxSize = 20

	path := filepath.Join(t.TempDir(), "flush.sst")
	flushed, info, err := FlushIfFull(m, 20, path)
	if err != nil {
		t.Fatalf("FlushIfFull: %v", err)
	}
	if !flushed {
		t.Fatal("FlushIfFull at threshold: flushed = false, want true")
	}
	if info == nil || info.Entries != 1 {
		t.Fatalf("FlushIfFull at threshold: info = %+v", info)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	value, _, ok, err := r.Get([]byte("aaaaaaaaaa"))
	if err != nil || !ok || string(value) != "bbbbbbbbbb" {
		t.Fatalf("Get after FlushIfFull = (%q, ok=%v, err=%v)", value, ok, err)
	}
}

func TestFlushIfFullPropagatesAWriteError(t *testing.T) {
	m1 := memtable.NewWithSeed(1)
	m1.Put([]byte("a"), []byte("1"))
	path := filepath.Join(t.TempDir(), "flush.sst")
	if _, err := Flush(m1, path); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	m2 := memtable.NewWithSeed(2)
	m2.Put([]byte("b"), []byte("2"))
	flushed, info, err := FlushIfFull(m2, 1, path) // path already occupied
	if err == nil {
		t.Fatal("FlushIfFull against an occupied path: err = nil, want ErrFileExists")
	}
	if flushed {
		t.Fatal("FlushIfFull against an occupied path: flushed = true, want false")
	}
	if info != nil {
		t.Fatalf("FlushIfFull against an occupied path: info = %+v, want nil", info)
	}
}

// TestFlushIfFullLeavesTheMemtableAlone documents, with a test, the
// deliberate omission FlushIfFull's own doc comment argues for: nothing
// about calling it changes what the memtable it was handed still reports.
func TestFlushIfFullLeavesTheMemtableAlone(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	before := m.ApproxSize()

	path := filepath.Join(t.TempDir(), "flush.sst")
	if flushed, _, err := FlushIfFull(m, 1, path); err != nil || !flushed {
		t.Fatalf("FlushIfFull: flushed=%v err=%v", flushed, err)
	}

	if got := m.ApproxSize(); got != before {
		t.Fatalf("ApproxSize after FlushIfFull = %d, want unchanged %d", got, before)
	}
	if _, _, ok := m.Get([]byte("a")); !ok {
		t.Fatal("Get(a) after FlushIfFull on the source memtable: ok = false, want true (Flush must not mutate m)")
	}
}
