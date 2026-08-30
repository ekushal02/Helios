package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

func TestOpenWithCacheReturnsCorrectValues(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Delete([]byte("c"))

	path := filepath.Join(t.TempDir(), "cached.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	cache := NewBlockCache(1 << 20)
	r, err := OpenWithCache(path, cache)
	if err != nil {
		t.Fatalf("OpenWithCache: %v", err)
	}
	defer r.Close()

	value, tombstone, ok, err := r.Get([]byte("a"))
	if err != nil || !ok || tombstone || string(value) != "1" {
		t.Fatalf("Get(a) = (%q, ts=%v, ok=%v, err=%v), want (\"1\", false, true, nil)", value, tombstone, ok, err)
	}
	value, tombstone, ok, err = r.Get([]byte("c"))
	if err != nil || !ok || !tombstone {
		t.Fatalf("Get(c) = (%q, ts=%v, ok=%v, err=%v), want (_, true, true, nil)", value, tombstone, ok, err)
	}
	_, _, ok, err = r.Get([]byte("never-written"))
	if err != nil || ok {
		t.Fatalf("Get(never-written) = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
}

// TestCacheHitAvoidsTouchingDiskAtAll is the test that actually proves
// the cache does what it claims: read a key once (populating the
// cache), then make the underlying file impossible to read correctly
// (truncate it to nothing) and confirm a second Get for the SAME key
// still succeeds with the right value -- which is only possible if that
// second Get never touched the file at all.
func TestCacheHitAvoidsTouchingDiskAtAll(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("value-a"))
	path := filepath.Join(t.TempDir(), "cached.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	cache := NewBlockCache(1 << 20)
	r, err := OpenWithCache(path, cache)
	if err != nil {
		t.Fatalf("OpenWithCache: %v", err)
	}
	defer r.Close()

	// First Get: a real miss, populates the cache.
	value, _, ok, err := r.Get([]byte("a"))
	if err != nil || !ok || string(value) != "value-a" {
		t.Fatalf("first Get(a) = (%q, ok=%v, err=%v), want (\"value-a\", true, nil)", value, ok, err)
	}
	if cache.Hits() != 0 || cache.Misses() != 1 {
		t.Fatalf("after first Get: Hits()=%d Misses()=%d, want Hits=0 Misses=1", cache.Hits(), cache.Misses())
	}

	// Truncate the file to zero bytes -- any Get that actually touches
	// disk now must fail (there is no footer, no index, nothing).
	if err := os.Truncate(path, 0); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	value, _, ok, err = r.Get([]byte("a"))
	if err != nil || !ok || string(value) != "value-a" {
		t.Fatalf("second Get(a), against a truncated file, = (%q, ok=%v, err=%v), want (\"value-a\", true, nil) -- "+
			"the cache hit should have made the truncation irrelevant", value, ok, err)
	}
	if cache.Hits() != 1 || cache.Misses() != 1 {
		t.Fatalf("after second Get: Hits()=%d Misses()=%d, want Hits=1 Misses=1", cache.Hits(), cache.Misses())
	}
}

// TestCacheIsSharedAcrossMultipleFiles confirms a single BlockCache
// correctly serves blocks from two different SSTables without either
// file's blocks colliding with or shadowing the other's -- the entire
// point of keying by (path, blockIndex) rather than blockIndex alone.
func TestCacheIsSharedAcrossMultipleFiles(t *testing.T) {
	dir := t.TempDir()
	cache := NewBlockCache(1 << 20)

	m1 := memtable.NewWithSeed(1)
	m1.Put([]byte("k"), []byte("from-file-1"))
	path1 := filepath.Join(dir, "1.sst")
	if _, err := Flush(m1, path1); err != nil {
		t.Fatalf("Flush(1): %v", err)
	}

	m2 := memtable.NewWithSeed(2)
	m2.Put([]byte("k"), []byte("from-file-2")) // same key, different file, different value
	path2 := filepath.Join(dir, "2.sst")
	if _, err := Flush(m2, path2); err != nil {
		t.Fatalf("Flush(2): %v", err)
	}

	r1, err := OpenWithCache(path1, cache)
	if err != nil {
		t.Fatalf("OpenWithCache(1): %v", err)
	}
	defer r1.Close()
	r2, err := OpenWithCache(path2, cache)
	if err != nil {
		t.Fatalf("OpenWithCache(2): %v", err)
	}
	defer r2.Close()

	value, _, ok, err := r1.Get([]byte("k"))
	if err != nil || !ok || string(value) != "from-file-1" {
		t.Fatalf("r1.Get(k) = (%q, ok=%v, err=%v), want (\"from-file-1\", true, nil)", value, ok, err)
	}
	value, _, ok, err = r2.Get([]byte("k"))
	if err != nil || !ok || string(value) != "from-file-2" {
		t.Fatalf("r2.Get(k) = (%q, ok=%v, err=%v), want (\"from-file-2\", true, nil) -- "+
			"the shared cache must not have confused file 1's block for file 2's", value, ok, err)
	}
	if cache.Len() != 2 {
		t.Fatalf("cache.Len() = %d, want 2 (one block cached per file)", cache.Len())
	}
}

// TestCacheAcrossManyBlocksStillReturnsCorrectValues checks the cache
// integration against a file with several data blocks, not just one --
// every block gets its own cache key, and a Get for a key in any block
// must still find exactly that block's entry, not some other cached
// block's.
func TestCacheAcrossManyBlocksStillReturnsCorrectValues(t *testing.T) {
	m := memtable.NewWithSeed(1)
	const n = 3000
	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%06d", i)
		value := fmt.Sprintf("v%06d", i)
		m.Put([]byte(key), []byte(value))
		want[key] = value
	}
	path := filepath.Join(t.TempDir(), "many.sst")
	info, err := Flush(m, path)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if info.DataBlocks < 2 {
		t.Fatalf("test setup: want at least 2 data blocks, got %d", info.DataBlocks)
	}

	cache := NewBlockCache(1 << 20)
	r, err := OpenWithCache(path, cache)
	if err != nil {
		t.Fatalf("OpenWithCache: %v", err)
	}
	defer r.Close()

	// Two passes: the first populates the cache (all misses), the
	// second should be entirely cache hits.
	for pass := 0; pass < 2; pass++ {
		for key, wantValue := range want {
			value, tombstone, ok, err := r.Get([]byte(key))
			if err != nil || !ok || tombstone || string(value) != wantValue {
				t.Fatalf("pass %d: Get(%q) = (%q, ts=%v, ok=%v, err=%v), want (%q, false, true, nil)",
					pass, key, value, tombstone, ok, err, wantValue)
			}
		}
	}
	if cache.Misses() != int64(info.DataBlocks) {
		t.Fatalf("cache.Misses() = %d, want %d (exactly one miss per block, on the first pass)", cache.Misses(), info.DataBlocks)
	}
	if cache.Hits() != int64(n)*2-int64(info.DataBlocks) {
		t.Fatalf("cache.Hits() = %d, want %d", cache.Hits(), int64(n)*2-int64(info.DataBlocks))
	}
}

func TestOpenWithoutCacheStillWorksExactlyAsOpenDoes(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	path := filepath.Join(t.TempDir(), "nocache.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r, err := OpenWithCache(path, nil)
	if err != nil {
		t.Fatalf("OpenWithCache(path, nil): %v", err)
	}
	defer r.Close()
	value, _, ok, err := r.Get([]byte("a"))
	if err != nil || !ok || string(value) != "1" {
		t.Fatalf("Get(a) with a nil cache = (%q, ok=%v, err=%v), want (\"1\", true, nil)", value, ok, err)
	}
}
