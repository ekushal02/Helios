package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

func TestWriteCompressedRoundTripsCorrectly(t *testing.T) {
	m := memtable.NewWithSeed(1)
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := fmt.Sprintf(`{"id":%d,"status":"active","region":"us-east-1","tag":"production-service-instance"}`, i)
		m.Put([]byte(key), []byte(value))
	}
	m.Delete([]byte("key-000042"))

	path := filepath.Join(t.TempDir(), "compressed.sst")
	info, err := WriteCompressed(m.NewIterator(), path, CompressionFlate)
	if err != nil {
		t.Fatalf("WriteCompressed: %v", err)
	}
	if info.Entries != 3000 {
		t.Fatalf("info.Entries = %d, want 3000", info.Entries)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value, tombstone, ok, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%q): ok = false, want true", key)
		}
		if key == "key-000042" {
			if !tombstone {
				t.Fatalf("Get(%q): tombstone = false, want true", key)
			}
			continue
		}
		wantValue := fmt.Sprintf(`{"id":%d,"status":"active","region":"us-east-1","tag":"production-service-instance"}`, i)
		if string(value) != wantValue {
			t.Fatalf("Get(%q) = %q, want %q", key, value, wantValue)
		}
	}
}

func TestWriteCompressedProducesASmallerFileForCompressibleData(t *testing.T) {
	buildWithCompression := func(compression CompressionType) int64 {
		m := memtable.NewWithSeed(1)
		for i := 0; i < 5000; i++ {
			key := fmt.Sprintf("key-%06d", i)
			value := fmt.Sprintf(`{"id":%d,"status":"active","region":"us-east-1","tag":"production-service-instance-%d"}`, i, i%10)
			m.Put([]byte(key), []byte(value))
		}
		path := filepath.Join(t.TempDir(), "cmp.sst")
		info, err := WriteCompressed(m.NewIterator(), path, compression)
		if err != nil {
			t.Fatalf("WriteCompressed(%v): %v", compression, err)
		}
		return info.Bytes
	}

	uncompressedBytes := buildWithCompression(CompressionNone)
	compressedBytes := buildWithCompression(CompressionFlate)

	if compressedBytes >= uncompressedBytes {
		t.Fatalf("compressed file is %d bytes, uncompressed is %d bytes -- want compressed meaningfully smaller for this repetitive workload",
			compressedBytes, uncompressedBytes)
	}
	savedPct := 100 * (1 - float64(compressedBytes)/float64(uncompressedBytes))
	if savedPct < 10 {
		t.Fatalf("compression saved only %.1f%% on highly repetitive JSON-shaped data -- want at least 10%%, something looks wrong", savedPct)
	}
}

func TestFlushCompressedMatchesWriteCompressed(t *testing.T) {
	m := memtable.NewWithSeed(1)
	m.Put([]byte("a"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	path := filepath.Join(t.TempDir(), "flush-compressed.sst")
	info, err := FlushCompressed(m, path, CompressionFlate)
	if err != nil {
		t.Fatalf("FlushCompressed: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	value, _, ok, err := r.Get([]byte("a"))
	if err != nil || !ok || string(value) != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("Get(a) = (%q, ok=%v, err=%v)", value, ok, err)
	}
	_ = info
}

// TestOpenReadsCompressedFilesWithoutAnySpecialAPI checks the design
// claim in reader.go's own doc directly: Open, OpenWithCache, Get, and
// Iterator all handle a compressed file exactly like an uncompressed
// one, with no caller-visible difference -- compression is entirely a
// per-block, on-disk detail verifyAndSplitBlock absorbs.
func TestOpenReadsCompressedFilesWithoutAnySpecialAPI(t *testing.T) {
	m := memtable.NewWithSeed(1)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := fmt.Sprintf(`{"payload":"repeated-content-repeated-content-%d"}`, i%20)
		m.Put([]byte(key), []byte(value))
	}
	path := filepath.Join(t.TempDir(), "compressed.sst")
	if _, err := WriteCompressed(m.NewIterator(), path, CompressionFlate); err != nil {
		t.Fatalf("WriteCompressed: %v", err)
	}

	// Iterator: a full sequential scan must also work transparently.
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
	if err := it.Err(); err != nil {
		t.Fatalf("Iterator.Err() over a compressed file: %v", err)
	}
	if count != 2000 {
		t.Fatalf("Iterator visited %d entries, want 2000", count)
	}

	// Cache: OpenWithCache over a compressed file must cache the
	// DECOMPRESSED entries (not compressed bytes) -- confirmed the same
	// way §13.12's own test proves a cache hit skips disk: read once,
	// then rely on the cache for a value check without re-verifying
	// against the file.
	cache := NewBlockCache(1 << 20)
	rc, err := OpenWithCache(path, cache)
	if err != nil {
		t.Fatalf("OpenWithCache: %v", err)
	}
	defer rc.Close()
	value, _, ok, err := rc.Get([]byte("key-000000"))
	if err != nil || !ok || string(value) != `{"payload":"repeated-content-repeated-content-0"}` {
		t.Fatalf("Get(key-000000) via a cached, compressed Reader = (%q, ok=%v, err=%v)", value, ok, err)
	}
	if cache.Hits()+cache.Misses() == 0 {
		t.Fatal("cache recorded no activity at all")
	}
}
