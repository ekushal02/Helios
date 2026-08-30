package sstable

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

// compressionMeasureNumKeys and compressionMeasureNumReads are fixed,
// not caller-configurable -- the same reason every other measurement's
// workload constants in this codebase are fixed (§13.9, §13.11, §13.12):
// comparing compressed against uncompressed requires holding everything
// else about the workload constant.
const (
	compressionMeasureNumKeys  = 5000
	compressionMeasureNumReads = 20000
)

// compressionMeasureValue is a JSON-shaped, moderately repetitive value
// -- realistic in the sense that real stored records (log lines,
// serialized objects, structured rows) usually share far more literal
// text across records than random bytes would, which is exactly the
// redundancy DEFLATE is good at removing and exactly what an
// incompressible workload (padding with zeros, or random bytes) would
// fail to show at all. Field values vary by i so the record isn't
// literally identical every time, only its shape and most of its text.
func compressionMeasureValue(i int) []byte {
	return []byte(fmt.Sprintf(
		`{"id":%d,"event":"page_view","status":"success","region":"us-east-1","service":"checkout-service","user_agent":"Mozilla/5.0 (compatible)"}`,
		i))
}

// buildCompressionMeasureSSTable flushes compressionMeasureNumKeys
// sequential keys, each with compressionMeasureValue(i), into path with
// the given compression, returning the resulting file's total size and
// how long the flush itself took -- the write-side (compress) CPU cost.
func buildCompressionMeasureSSTable(t *testing.T, dir string, compression CompressionType) (path string, bytes int64, writeDuration time.Duration) {
	t.Helper()
	m := memtable.NewWithSeed(1)
	for i := 0; i < compressionMeasureNumKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		m.Put([]byte(key), compressionMeasureValue(i))
	}

	name := "none.sst"
	if compression == CompressionFlate {
		name = "flate.sst"
	}
	path = filepath.Join(dir, name)

	start := time.Now()
	info, err := WriteCompressed(m.NewIterator(), path, compression)
	writeDuration = time.Since(start)
	if err != nil {
		t.Fatalf("WriteCompressed(%v): %v", compression, err)
	}
	return path, info.Bytes, writeDuration
}

// TestCompressionSpaceSavingAgainstCPUCost IS THE MEASUREMENT THIS TASK
// EXISTS TO PRODUCE: how much disk space per-block flate compression
// saves against a realistic, moderately-redundant workload, and what it
// costs in CPU time to write (compress) and read (decompress).
//
// SPACE SAVED IS DETERMINISTIC AND ASSERTED; CPU TIME IS LOGGED ONLY,
// FOR THE SAME REASON EVERY PRIOR LATENCY MEASUREMENT IN THIS CODEBASE
// DRAWS THAT LINE (§13.9, §13.12). The exact byte count a fixed, seeded
// workload compresses to depends only on this package's own
// deterministic compressFlate -- no timing involved -- so it is checked
// directly. How long that compression takes, in wall-clock time, depends
// on the machine running the test, so it is reported for a human (or
// DESIGN.md §13.13) to read, not asserted against a hard threshold.
//
// Read-side (decompression) latency is measured WITHOUT a block cache
// (§13.12) deliberately: a cache would absorb the decompression cost
// after each block's first read, diluting exactly the cost this
// measurement exists to show across a read-heavy workload. Compression's
// CPU cost on reads is real regardless of whether a cache happens to be
// in front of it -- this measurement isolates it on purpose.
func TestCompressionSpaceSavingAgainstCPUCost(t *testing.T) {
	dir := t.TempDir()

	nonePath, noneBytes, noneWriteDur := buildCompressionMeasureSSTable(t, dir, CompressionNone)
	flatePath, flateBytes, flateWriteDur := buildCompressionMeasureSSTable(t, dir, CompressionFlate)

	if flateBytes >= noneBytes {
		t.Fatalf("compressed file (%d bytes) is not smaller than uncompressed (%d bytes) -- expected real savings for this workload", flateBytes, noneBytes)
	}
	savedPct := 100 * (1 - float64(flateBytes)/float64(noneBytes))
	t.Logf("file size: uncompressed=%d compressed=%d saved=%.1f%%", noneBytes, flateBytes, savedPct)
	t.Logf("write (compress) duration: uncompressed=%v compressed=%v", noneWriteDur, flateWriteDur)

	if savedPct < 20 {
		t.Fatalf("compression saved only %.1f%% on a moderately redundant JSON-shaped workload, want at least 20%% -- something looks wrong", savedPct)
	}

	// Read-side: point-lookup latency against both files, no cache, so
	// decompression's cost is paid on every single Get rather than only
	// the first touch of each block.
	rNone, err := Open(nonePath)
	if err != nil {
		t.Fatalf("Open(uncompressed): %v", err)
	}
	defer rNone.Close()
	rFlate, err := Open(flatePath)
	if err != nil {
		t.Fatalf("Open(compressed): %v", err)
	}
	defer rFlate.Close()

	noneLatencies := make([]time.Duration, compressionMeasureNumReads)
	flateLatencies := make([]time.Duration, compressionMeasureNumReads)
	for i := 0; i < compressionMeasureNumReads; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i%compressionMeasureNumKeys))

		start := time.Now()
		_, _, ok, err := rNone.Get(key)
		noneLatencies[i] = time.Since(start)
		if err != nil || !ok {
			t.Fatalf("uncompressed Get(%q): ok=%v err=%v", key, ok, err)
		}

		start = time.Now()
		_, _, ok, err = rFlate.Get(key)
		flateLatencies[i] = time.Since(start)
		if err != nil || !ok {
			t.Fatalf("compressed Get(%q): ok=%v err=%v", key, ok, err)
		}
	}

	// summarizeLatency and percentileDur are defined in
	// cache_latency_test.go (§13.12) and reused here as-is, both files
	// being in the same package -- comparing latencies the identical way
	// two measurements in this codebase already report them.
	summarizeLatency(t, "read: uncompressed", noneLatencies)
	summarizeLatency(t, "read: compressed", flateLatencies)

	const smokeTestCeiling = 5 * time.Second
	for _, latencies := range [][]time.Duration{noneLatencies, flateLatencies} {
		for _, d := range latencies {
			if d > smokeTestCeiling {
				t.Fatalf("a read took %v, want under %v -- looks like a real stall, not measurement noise", d, smokeTestCeiling)
			}
		}
	}
}
