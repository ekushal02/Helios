package sstable

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

// TestFindBlockOnAHandBuiltIndex exercises findBlock's binary search in
// isolation, against an index built by hand rather than by flushing a
// real memtable. This is the point of splitting findBlock out of Get at
// all: the block-selection logic can be checked without touching disk,
// or even constructing a valid SSTable file, at every boundary that
// matters -- before the first block, exactly on a LastKey, in the gap
// between two blocks, and past the last block.
func TestFindBlockOnAHandBuiltIndex(t *testing.T) {
	r := &Reader{index: []indexEntry{
		{LastKey: []byte("d"), BlockOffset: 0, BlockLength: 10},
		{LastKey: []byte("h"), BlockOffset: 10, BlockLength: 20},
		{LastKey: []byte("m"), BlockOffset: 30, BlockLength: 15},
	}}

	tests := []struct {
		key       string
		wantFound bool
		wantLast  string // r.index[i].LastKey, only checked if wantFound
	}{
		{"a", true, "d"},  // before the first block's LastKey -> first block
		{"d", true, "d"},  // exactly on a boundary -> that block, not the next
		{"e", true, "h"},  // in the gap between two blocks -> the next block
		{"h", true, "h"},  // exactly on the second boundary
		{"i", true, "m"},  // in the gap before the last block
		{"m", true, "m"},  // exactly on the last boundary
		{"n", false, ""},  // past every block -> not found
		{"zz", false, ""}, // well past every block
	}

	for _, tc := range tests {
		e, ok := r.findBlock([]byte(tc.key))
		if ok != tc.wantFound {
			t.Errorf("findBlock(%q): ok = %v, want %v", tc.key, ok, tc.wantFound)
			continue
		}
		if ok && string(e.LastKey) != tc.wantLast {
			t.Errorf("findBlock(%q) = block with LastKey %q, want %q", tc.key, e.LastKey, tc.wantLast)
		}
	}
}

// TestFindBlockOnAnEmptyIndex is the degenerate case: no blocks at all.
// Write refuses to ever produce this (ErrEmptySource), but findBlock is
// tested here as its own unit and should not assume its caller enforced
// that.
func TestFindBlockOnAnEmptyIndex(t *testing.T) {
	r := &Reader{index: nil}
	if _, ok := r.findBlock([]byte("anything")); ok {
		t.Fatal("findBlock on an empty index: ok = true, want false")
	}
}

// buildMultiBlockSSTable flushes n small, distinct keys into an SSTable
// and opens it, returning the Reader and the keys in insertion order. n
// is chosen by each caller to be comfortably larger than one block's
// worth of entries, so the tests below are actually exercising a
// multi-block file rather than accidentally testing only the
// single-block case.
func buildMultiBlockSSTable(t *testing.T, n int) (*Reader, []string) {
	t.Helper()
	m := memtable.NewWithSeed(1)
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%08d", i)
		m.Put([]byte(key), make([]byte, 64))
		keys = append(keys, key)
	}
	path := filepath.Join(t.TempDir(), "flush.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r, keys
}

// TestGetAtEveryBlockBoundary is TestFindBlockOnAHandBuiltIndex's
// counterpart against a real file: a key equal to a block's LastKey must
// come back from Get as found and live, not accidentally routed to the
// wrong block or lost in the handoff between findBlock and the scan that
// follows it.
func TestGetAtEveryBlockBoundary(t *testing.T) {
	r, _ := buildMultiBlockSSTable(t, 5000)
	if len(r.index) < 3 {
		t.Fatalf("test setup produced only %d blocks; want at least 3 for boundaries to mean anything", len(r.index))
	}

	for _, blockIdx := range []int{0, len(r.index) / 2, len(r.index) - 1} {
		key := r.index[blockIdx].LastKey
		value, tombstone, ok, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) at block %d's LastKey: %v", key, blockIdx, err)
		}
		if !ok || tombstone {
			t.Fatalf("Get(%q) at block %d's LastKey: ok=%v tombstone=%v, want ok=true tombstone=false", key, blockIdx, ok, tombstone)
		}
		if len(value) != 64 {
			t.Fatalf("Get(%q) at block %d's LastKey: len(value) = %d, want 64", key, blockIdx, len(value))
		}
	}
}

// TestGetInTheGapBetweenBlocksIsNotFound picks a key that findBlock will
// correctly route to the block AFTER a boundary (the gap-routing case
// TestFindBlockOnAHandBuiltIndex checks in isolation) and confirms the
// block scan that follows agrees the key was never written -- the two
// steps have to be consistent with each other, not just individually
// correct.
func TestGetInTheGapBetweenBlocksIsNotFound(t *testing.T) {
	r, _ := buildMultiBlockSSTable(t, 5000)
	if len(r.index) < 3 {
		t.Fatalf("test setup produced only %d blocks, want at least 3", len(r.index))
	}

	for i := 0; i < len(r.index)-1; i++ {
		// keys are "k%08d", fixed width -- appending a byte produces a
		// key strictly between this block's LastKey and the next key
		// ever written, which is exactly the gap a real file has between
		// consecutive integers formatted this way.
		gapKey := append(bytes.Clone(r.index[i].LastKey), '5')

		_, _, ok, err := r.Get(gapKey)
		if err != nil {
			t.Fatalf("Get(%q) in the gap after block %d: %v", gapKey, i, err)
		}
		if ok {
			t.Fatalf("Get(%q) in the gap after block %d: ok = true, want false", gapKey, i)
		}
	}
}

// TestGetOnFirstAndLastKeyInFile checks the two ends of the whole file,
// as distinct from the two ends of one block above.
func TestGetOnFirstAndLastKeyInFile(t *testing.T) {
	r, keys := buildMultiBlockSSTable(t, 5000)

	if _, _, ok, err := r.Get([]byte(keys[0])); err != nil || !ok {
		t.Fatalf("Get on the first key in the file: ok=%v err=%v", ok, err)
	}
	if _, _, ok, err := r.Get([]byte(keys[len(keys)-1])); err != nil || !ok {
		t.Fatalf("Get on the last key in the file: ok=%v err=%v", ok, err)
	}
}

// TestGetBeforeTheFirstKeyInTheFile checks the case findBlock's
// "a" -> first-block test case establishes in isolation: a key smaller
// than anything ever written still routes to a real block (the first
// one) rather than short-circuiting to not-found before ever touching
// disk, and that block's scan correctly reports the key absent.
func TestGetBeforeTheFirstKeyInTheFile(t *testing.T) {
	r, keys := buildMultiBlockSSTable(t, 5000)

	before := []byte("!") // sorts before "k00000000", the first key written
	if before[0] >= keys[0][0] {
		t.Fatalf("test setup: %q does not sort before the first key %q", before, keys[0])
	}
	_, _, ok, err := r.Get(before)
	if err != nil {
		t.Fatalf("Get(%q): %v", before, err)
	}
	if ok {
		t.Fatalf("Get(%q): ok = true, want false (never written)", before)
	}
}