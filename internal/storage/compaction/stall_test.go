package compaction

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/ekushal02/helios/internal/storage/engine"
	"github.com/ekushal02/helios/internal/storage/manifest"
	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/sstable"
	"github.com/ekushal02/helios/internal/storage/wal"
)

// seedLargeL0 builds n SSTable files, each with keysPerFile distinct
// keys and a modest value size, in dir, and returns a Manifest listing
// them all at level 0 in newest-first order. The goal is enough real
// merge work that a compaction draining this L0 takes long enough to
// plausibly overlap with a concurrent write workload for the whole
// duration of that workload -- a compaction that finishes in a
// millisecond would tell this measurement nothing.
func seedLargeL0(t *testing.T, dir string, n, keysPerFile int) *manifest.Manifest {
	t.Helper()
	var files []string
	for f := 0; f < n; f++ {
		m := memtable.NewWithSeed(int64(f))
		value := make([]byte, 128)
		for i := 0; i < keysPerFile; i++ {
			key := fmt.Sprintf("file%02d-key-%08d", f, i)
			m.Put([]byte(key), value)
		}
		name := fmt.Sprintf("seed-%03d.sst", f)
		if _, err := sstable.Flush(m, filepath.Join(dir, name)); err != nil {
			t.Fatalf("Flush(%s): %v", name, err)
		}
		// Prepend: each successive file is "newer" than the ones before
		// it, matching the newest-first convention every level already
		// requires (§13.6, §13.8).
		files = append([]string{name}, files...)
	}
	return &manifest.Manifest{Levels: [][]string{files}}
}

// writeLatencies runs n sequential Put calls through a fresh
// engine.Writer -- its own WAL and memtable, in dir but touching none of
// the SSTable files or manifest a concurrent compaction would be
// working on -- and returns each call's latency in the order the calls
// were made.
func writeLatencies(t *testing.T, dir string, n int) []time.Duration {
	t.Helper()
	walPath := filepath.Join(dir, "measure.wal")
	m := memtable.NewWithSeed(99)
	w, err := engine.RecoverMemtable(walPath, wal.SyncAlways, m)
	if err != nil {
		t.Fatalf("RecoverMemtable: %v", err)
	}
	defer w.Close()
	writer := engine.NewWriter(w, m)

	value := make([]byte, 128)
	latencies := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("write-%08d", i))
		start := time.Now()
		if err := writer.Put(key, value); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		latencies[i] = time.Since(start)
	}
	return latencies
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

// summarize logs mean/p50/p99/max for a set of latencies -- the real
// numbers this task's own instructions ask to be reported, not asserted
// against a hard-coded threshold (see the test doc below for why).
func summarize(t *testing.T, label string, latencies []time.Duration) {
	t.Helper()
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	max := time.Duration(0)
	for _, d := range sorted {
		total += d
		if d > max {
			max = d
		}
	}
	mean := total / time.Duration(len(sorted))
	t.Logf("%-32s mean=%-10v p50=%-10v p99=%-10v max=%-10v",
		label, mean, percentile(sorted, 0.50), percentile(sorted, 0.99), max)
}

// TestWriteLatencyWithAndWithoutBackgroundCompaction IS THE MEASUREMENT
// THIS TASK EXISTS TO PRODUCE: does a compaction goroutine running
// concurrently in the background measurably stall a live write path.
//
// Method: seed a large L0 (enough real merge work that compaction takes
// a nontrivial amount of wall-clock time -- see seedLargeL0), then run
// the identical N-write workload through a completely separate
// engine.Writer twice: once with no compaction running at all
// (baseline), once with a Background compactor (§13.9) actively
// draining the seeded L0 concurrently. Compare the two latency
// distributions.
//
// THIS TEST DELIBERATELY DOES NOT ASSERT A TIGHT STATISTICAL BOUND ON
// THE LATENCY NUMBERS THEMSELVES. Unlike the Bloom filter's
// false-positive rate (§13.5) -- a quantity a formula predicts exactly,
// which is why that measurement DOES assert a derived tolerance band --
// write latency under concurrent disk I/O is inherently machine- and
// environment-dependent. A hard-coded millisecond threshold here would
// be flaky on a slower CI runner or a busier laptop, and would test this
// sandbox's disk, not the actual claim. The honest way to report this is
// the numbers themselves, logged via t.Logf, for a human (or a design
// doc) to read and judge -- see DESIGN.md §13.9 for the real numbers
// from an actual run. The only hard assertion here is a generous
// smoke-test ceiling: no single write may take longer than
// smokeTestCeiling, which exists to catch an actual hang or deadlock (a
// real bug this test should still fail loudly on), not to make a
// statistical claim about ordinary contention.
func TestWriteLatencyWithAndWithoutBackgroundCompaction(t *testing.T) {
	const (
		numSeedFiles     = 6
		keysPerSeedFile  = 8000
		numWrites        = 2000
		smokeTestCeiling = 5 * time.Second
	)

	baselineDir := t.TempDir()
	baseline := writeLatencies(t, baselineDir, numWrites)
	summarize(t, "baseline (no compaction)", baseline)

	concurrentDir := t.TempDir()
	manifestPath := filepath.Join(concurrentDir, "MANIFEST")
	m := seedLargeL0(t, concurrentDir, numSeedFiles, keysPerSeedFile)
	if err := manifest.Save(manifestPath, m); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	bg := StartBackground(manifestPath, concurrentDir, DefaultOptions, time.Millisecond)
	concurrent := writeLatencies(t, concurrentDir, numWrites)
	bg.Stop()
	summarize(t, "concurrent (background compaction)", concurrent)

	if err := bg.Err(); err != nil {
		t.Fatalf("background compaction reported an error: %v", err)
	}
	if bg.Cycles() == 0 {
		t.Fatal("background compaction never completed a single cycle during the concurrent run -- " +
			"this measurement needs it to have actually competed for resources, not merely been started")
	}
	t.Logf("compaction cycles completed during the concurrent run: %d", bg.Cycles())

	for i, d := range concurrent {
		if d > smokeTestCeiling {
			t.Fatalf("write %d took %v with background compaction running, want under %v -- "+
				"this looks like a real stall or deadlock, not ordinary contention noise", i, d, smokeTestCeiling)
		}
	}
}

// TestConcurrentWritesAndBackgroundCompactionProduceNoRaceOrCorruption
// is the correctness companion to the measurement above: running a
// write workload and a background compactor against the same directory
// at the same time must never be caught by -race (confirming the "share
// no lock" argument in Background's own doc actually holds in practice,
// not just on paper), and the compaction must still produce a correct,
// readable result once it's done.
func TestConcurrentWritesAndBackgroundCompactionProduceNoRaceOrCorruption(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	m := seedLargeL0(t, dir, 5, 2000)
	if err := manifest.Save(manifestPath, m); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	bg := StartBackground(manifestPath, dir, DefaultOptions, time.Millisecond)
	_ = writeLatencies(t, dir, 500) // exercised purely for its concurrent side effect here
	bg.Stop()

	if err := bg.Err(); err != nil {
		t.Fatalf("background compaction reported an error: %v", err)
	}
	if bg.Cycles() == 0 {
		t.Fatal("background compaction never ran during the concurrent workload")
	}

	got, err := manifest.Load(manifestPath)
	if err != nil {
		t.Fatalf("manifest.Load: %v", err)
	}
	if len(got.Levels[0]) != 0 {
		t.Fatalf("L0 = %v, want empty -- the seeded backlog should have been fully compacted", got.Levels[0])
	}
	if len(got.Levels) < 2 || len(got.Levels[1]) != 1 {
		t.Fatalf("L1 = %+v, want exactly one file", got.Levels)
	}

	r, err := sstable.Open(filepath.Join(dir, got.Levels[1][0]))
	if err != nil {
		t.Fatalf("Open(%s): %v", got.Levels[1][0], err)
	}
	defer r.Close()
	_, tombstone, ok, err := r.Get([]byte("file00-key-00000000"))
	if err != nil || !ok || tombstone {
		t.Fatalf("Get on the compacted output = (ok=%v, tombstone=%v, err=%v), want (true, false, nil)", ok, tombstone, err)
	}
}
