package sstable

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

// cacheLatencyNumKeys, cacheLatencyValueSize, and cacheLatencyNumReads
// are fixed, not caller-configurable, for the same reason
// amplification.go's workload constants are fixed (§13.11): comparing
// cache sizes against each other requires holding everything else about
// the workload constant.
const (
	cacheLatencyNumKeys   = 5000
	cacheLatencyValueSize = 200
	cacheLatencyNumReads  = 20000
)

// buildCacheLatencySSTable flushes cacheLatencyNumKeys sequential keys,
// each with a cacheLatencyValueSize-byte value, into a single SSTable
// and returns its path and the exact number of cacheable bytes (summed
// key+value bytes across every entry) -- the same quantity
// blockEntriesSize (reader.go) computes per block, totaled here so the
// three cache sizes below can be chosen as fractions of it.
func buildCacheLatencySSTable(t *testing.T, dir string) (path string, cacheableBytes int64) {
	t.Helper()
	m := memtable.NewWithSeed(1)
	value := make([]byte, cacheLatencyValueSize)
	for i := 0; i < cacheLatencyNumKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		m.Put([]byte(key), value)
		cacheableBytes += int64(len(key) + len(value))
	}
	path = filepath.Join(dir, "cache-latency.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return path, cacheableBytes
}

// zipfKeys generates cacheLatencyNumReads key indices from a fixed,
// seeded Zipfian distribution -- a skewed access pattern (a minority of
// keys read far more often than the rest) rather than uniform random,
// because a block cache's entire benefit disappears under uniform
// access to a working set larger than the cache: every access would be
// a first (and only) touch regardless of cache size, at which point
// there is nothing "with a cache" could ever do better than "without
// one." Real workloads are skewed -- some keys really are hotter than
// others -- and a cache's value only shows up when the measurement
// reflects that.
func zipfKeys(t *testing.T) []int {
	t.Helper()
	r := rand.New(rand.NewSource(7))
	z := rand.NewZipf(r, 1.05, 1, uint64(cacheLatencyNumKeys-1))
	if z == nil {
		t.Fatal("rand.NewZipf returned nil -- invalid parameters")
	}
	keys := make([]int, cacheLatencyNumReads)
	for i := range keys {
		keys[i] = int(z.Uint64())
	}
	return keys
}

// runReadWorkload runs the same fixed sequence of Gets (by key index,
// from zipfKeys) against r and returns each call's latency.
func runReadWorkload(t *testing.T, r *Reader, keyIndices []int) []time.Duration {
	t.Helper()
	latencies := make([]time.Duration, len(keyIndices))
	for i, k := range keyIndices {
		key := []byte(fmt.Sprintf("key-%08d", k))
		start := time.Now()
		_, _, ok, err := r.Get(key)
		latencies[i] = time.Since(start)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%q): ok = false, want true (every key in this range was written)", key)
		}
	}
	return latencies
}

func percentileDur(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func summarizeLatency(t *testing.T, label string, latencies []time.Duration) {
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
	t.Logf("%-28s mean=%-12v p50=%-12v p99=%-12v max=%-12v",
		label, mean, percentileDur(sorted, 0.50), percentileDur(sorted, 0.99), max)
}

// TestReadLatencyWithAndWithoutBlockCacheAtThreeSizes IS THE
// MEASUREMENT THIS TASK EXISTS TO PRODUCE: read latency against an
// SSTable with no block cache, and with a block cache at three sizes
// (a small fraction of the data, a moderate fraction, and enough to hold
// everything), all driven by the identical Zipfian-skewed access
// pattern.
//
// LATENCY ITSELF IS LOGGED, NOT ASSERTED, FOR THE SAME REASON §13.9's
// WRITE-STALL MEASUREMENT DOESN'T ASSERT A THRESHOLD: real time depends
// on the machine it runs on. Hit rate, by contrast, depends only on the
// fixed Zipfian seed, the fixed cache sizes, and this package's own
// deterministic LRU (blockcache.LRU, §13.12) -- it is asserted directly,
// the same split §13.11's amplification measurement drew between
// deterministic byte counts (asserted) and real-time latency (logged
// only).
func TestReadLatencyWithAndWithoutBlockCacheAtThreeSizes(t *testing.T) {
	dir := t.TempDir()
	path, cacheableBytes := buildCacheLatencySSTable(t, dir)
	keyIndices := zipfKeys(t)

	// No cache at all.
	rNoCache, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rNoCache.Close()
	noCacheLatencies := runReadWorkload(t, rNoCache, keyIndices)
	summarizeLatency(t, "no cache", noCacheLatencies)

	// Three cache sizes, as fractions of the exact cacheable byte total:
	// small (10%, meaningfully smaller than the working set), medium
	// (50%), and large (150%, comfortably enough to hold every block at
	// once regardless of per-block framing overhead).
	fractions := []float64{0.10, 0.50, 1.50}
	labels := []string{"small (10%)", "medium (50%)", "large (150%)"}

	type cacheResult struct {
		label     string
		bytes     int64
		hits      int64
		misses    int64
		hitRate   float64
		latencies []time.Duration
	}
	var results []cacheResult

	for i, frac := range fractions {
		cacheBytes := int64(float64(cacheableBytes) * frac)
		cache := NewBlockCache(cacheBytes)
		r, err := OpenWithCache(path, cache)
		if err != nil {
			t.Fatalf("OpenWithCache(%s): %v", labels[i], err)
		}
		latencies := runReadWorkload(t, r, keyIndices)
		r.Close()

		summarizeLatency(t, "cache: "+labels[i], latencies)
		t.Logf("%-28s bytes=%-10d hits=%-8d misses=%-8d hitRate=%.3f",
			"cache: "+labels[i], cacheBytes, cache.Hits(), cache.Misses(), cache.HitRate())

		results = append(results, cacheResult{
			label: labels[i], bytes: cacheBytes,
			hits: cache.Hits(), misses: cache.Misses(), hitRate: cache.HitRate(),
			latencies: latencies,
		})
	}

	// The core claim, checked directly: a larger cache must never have a
	// LOWER hit rate than a smaller one, against the identical access
	// pattern. Strict inequality isn't required between every adjacent
	// pair (two sizes could plausibly tie if neither is a bottleneck),
	// but the smallest-to-largest direction is the whole reason a cache
	// size is a tunable at all.
	smallest, largest := results[0], results[len(results)-1]
	if smallest.hitRate > largest.hitRate {
		t.Errorf("smallest cache (%s) had a HIGHER hit rate (%.3f) than the largest (%s, %.3f) -- "+
			"more cache capacity should never hit less often against the same access pattern",
			smallest.label, smallest.hitRate, largest.label, largest.hitRate)
	}
	// The largest cache (150% of the exact cacheable byte total) should
	// hold every block at once, so after the very first access to each
	// block, every subsequent access to it must be a hit -- the miss
	// count should equal roughly the number of distinct blocks touched,
	// nowhere near the total read count.
	if largest.misses > int64(cacheLatencyNumKeys) {
		t.Errorf("largest cache: misses = %d, want well under %d (the total key count) -- "+
			"a cache sized to hold everything should only ever miss once per block, not repeatedly",
			largest.misses, cacheLatencyNumKeys)
	}

	// Smoke-test ceiling only -- catches an actual hang, not a
	// statistical claim about ordinary timing.
	const smokeTestCeiling = 5 * time.Second
	for _, r := range results {
		for _, d := range r.latencies {
			if d > smokeTestCeiling {
				t.Fatalf("%s: a read took %v, want under %v -- looks like a real stall, not measurement noise", r.label, d, smokeTestCeiling)
			}
		}
	}
}
