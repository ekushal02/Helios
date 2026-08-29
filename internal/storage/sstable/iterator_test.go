package sstable

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

func TestIteratorVisitsEveryEntryInOrder(t *testing.T) {
	m := memtable.NewWithSeed(1)
	const n = 5000
	type kv struct {
		key   string
		value string
	}
	want := make([]kv, 0, n)
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := make([]byte, 1+rng.Intn(64))
		rng.Read(value)
		m.Put([]byte(key), value)
		want = append(want, kv{key, string(value)})
	}

	path := filepath.Join(t.TempDir(), "iter.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	it := r.NewIterator()
	i := 0
	for it.Next() {
		if i >= len(want) {
			t.Fatalf("iterator produced more entries than were written (at least %d, want %d)", i+1, len(want))
		}
		if string(it.Key()) != want[i].key {
			t.Fatalf("entry %d: key = %q, want %q", i, it.Key(), want[i].key)
		}
		if it.Tombstone() {
			t.Fatalf("entry %d (%q): Tombstone() = true, want false", i, it.Key())
		}
		if string(it.Value()) != want[i].value {
			t.Fatalf("entry %d (%q): value mismatch", i, it.Key())
		}
		i++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err() after a clean iteration: %v", err)
	}
	if i != len(want) {
		t.Fatalf("iterator produced %d entries, want %d", i, len(want))
	}
}

func TestIteratorSurfacesTombstones(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Delete([]byte("a"))

	path := filepath.Join(t.TempDir(), "iter.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	it := r.NewIterator()
	if !it.Next() {
		t.Fatal("expected a first entry")
	}
	if string(it.Key()) != "a" || !it.Tombstone() || it.Value() != nil {
		t.Fatalf("entry 0 = (key=%q, tombstone=%v, value=%q), want (\"a\", true, nil)", it.Key(), it.Tombstone(), it.Value())
	}
	if !it.Next() {
		t.Fatal("expected a second entry")
	}
	if string(it.Key()) != "b" || it.Tombstone() || string(it.Value()) != "2" {
		t.Fatalf("entry 1 = (key=%q, tombstone=%v, value=%q), want (\"b\", false, \"2\")", it.Key(), it.Tombstone(), it.Value())
	}
	if it.Next() {
		t.Fatal("expected exactly two entries")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err() after a clean iteration: %v", err)
	}
}

func TestIteratorOnAnEmptyBlockRangeStillTerminates(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("only"), []byte("v"))
	path := filepath.Join(t.TempDir(), "iter.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	it := r.NewIterator()
	count := 0
	for it.Next() {
		count++
	}
	if count != 1 {
		t.Fatalf("got %d entries from a single-key SSTable, want 1", count)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err(): %v", err)
	}
	// Calling Next again past exhaustion must keep reporting false, not
	// panic or wrap back around.
	if it.Next() {
		t.Fatal("Next() after exhaustion returned true")
	}
}

// TestIteratorReportsCorruptionThroughErrRatherThanSilence is the
// Source.Err contract (writer.go) checked directly against the one real
// implementation that can actually fail: flip a bit inside a data block
// on disk and confirm the iterator stops with a non-nil Err, rather than
// quietly reporting fewer entries than the file actually has.
func TestIteratorReportsCorruptionThroughErrRatherThanSilence(t *testing.T) {
	m := memtable.NewWithSeed(1)
	for i := 0; i < 2000; i++ {
		m.Put([]byte(fmt.Sprintf("k%06d", i)), make([]byte, 32))
	}
	path := filepath.Join(t.TempDir(), "iter.sst")
	info, err := Flush(m, path)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if info.DataBlocks < 2 {
		t.Fatalf("test setup: want at least 2 data blocks, got %d", info.DataBlocks)
	}

	// Corrupt a byte inside the first data block (well before the
	// footer/index, which start at BlockOffset of the first block plus
	// the sum of all block lengths -- corrupting byte 4 is safely inside
	// any non-trivial block's entry bytes).
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, 4); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open on a file corrupted only inside a data block (footer/index untouched): %v", err)
	}
	defer r.Close()

	it := r.NewIterator()
	seen := 0
	for it.Next() {
		seen++
	}
	if err := it.Err(); err == nil {
		t.Fatalf("Err() after iterating a corrupted file: nil, want a corruption error (silently saw %d entries)", seen)
	}
	if seen >= 2000 {
		t.Fatalf("iterator reported all 2000 entries despite corruption -- the corrupt byte should have broken at least one block's CRC")
	}
}
