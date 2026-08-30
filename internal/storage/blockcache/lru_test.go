package blockcache

import (
	"sync"
	"testing"
)

// sizeOfInt treats every int value as costing 1 unit -- lets tests
// reason about eviction purely in terms of entry count, which is easier
// to state expectations for than trying to pick byte sizes that produce
// a specific eviction order.
func sizeOfInt(int) int64 { return 1 }

func TestGetOnEmptyCacheMisses(t *testing.T) {
	c := New[string, int](10, sizeOfInt)
	if _, ok := c.Get("a"); ok {
		t.Fatal("Get on an empty cache: ok = true, want false")
	}
	if c.Misses() != 1 || c.Hits() != 0 {
		t.Fatalf("Misses()=%d Hits()=%d, want Misses=1 Hits=0", c.Misses(), c.Hits())
	}
}

func TestPutThenGetRoundTrips(t *testing.T) {
	c := New[string, int](10, sizeOfInt)
	c.Put("a", 1)
	v, ok := c.Get("a")
	if !ok || v != 1 {
		t.Fatalf("Get(a) = (%d, %v), want (1, true)", v, ok)
	}
	if c.Hits() != 1 || c.Misses() != 0 {
		t.Fatalf("Hits()=%d Misses()=%d, want Hits=1 Misses=0", c.Hits(), c.Misses())
	}
}

func TestEvictsLeastRecentlyUsedFirst(t *testing.T) {
	c := New[string, int](2, sizeOfInt) // room for exactly 2 entries
	c.Put("a", 1)
	c.Put("b", 2)
	// Touch "a" so "b" becomes the least recently used.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("Get(a) before eviction: ok = false, want true")
	}
	c.Put("c", 3) // must evict "b", not "a"

	if _, ok := c.Get("b"); ok {
		t.Fatal("Get(b) after it should have been evicted: ok = true, want false")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("Get(a), which was touched more recently than b: ok = false, want true")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("Get(c), just inserted: ok = false, want true")
	}
}

func TestPutUpdatingAnExistingKeyAdjustsSizeCorrectly(t *testing.T) {
	sizeOfString := func(s string) int64 { return int64(len(s)) }
	c := New[string, string](10, sizeOfString)
	c.Put("k", "short") // size 5
	if got := c.Bytes(); got != 5 {
		t.Fatalf("Bytes() after first Put = %d, want 5", got)
	}
	c.Put("k", "muchlonger") // size 10, same key -- must replace, not add
	if got := c.Bytes(); got != 10 {
		t.Fatalf("Bytes() after updating the same key = %d, want 10 (replaced, not summed)", got)
	}
	if got := int64(c.Len()); got != 1 {
		t.Fatalf("Len() = %d, want 1 (still one distinct key)", got)
	}
}

func TestSingleEntryLargerThanBudgetIsStillCached(t *testing.T) {
	sizeOversized := func(int) int64 { return 1000 }
	oversized := New[string, int](1, sizeOversized)
	oversized.Put("huge", 42)

	v, ok := oversized.Get("huge")
	if !ok || v != 42 {
		t.Fatalf("Get(huge) on a cache holding one oversized entry = (%d, %v), want (42, true)", v, ok)
	}
	if got := oversized.Bytes(); got != 1000 {
		t.Fatalf("Bytes() = %d, want 1000 (the single oversized entry, budget notwithstanding)", got)
	}
}

func TestBudgetOfZeroCachesNothing(t *testing.T) {
	c := New[string, int](0, sizeOfInt)
	c.Put("a", 1)
	if _, ok := c.Get("a"); ok {
		t.Fatal("Get(a) on a zero-budget cache: ok = true, want false")
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len() on a zero-budget cache after a Put = %d, want 0", got)
	}
}

func TestNegativeBudgetCachesNothing(t *testing.T) {
	c := New[string, int](-5, sizeOfInt)
	c.Put("a", 1)
	if _, ok := c.Get("a"); ok {
		t.Fatal("Get(a) on a negative-budget cache: ok = true, want false")
	}
}

func TestHitRate(t *testing.T) {
	c := New[string, int](10, sizeOfInt)
	if got := c.HitRate(); got != 0 {
		t.Fatalf("HitRate() with no Get calls yet = %v, want 0", got)
	}
	c.Put("a", 1)
	c.Get("a")    // hit
	c.Get("a")    // hit
	c.Get("nope") // miss
	if got, want := c.HitRate(), 2.0/3.0; got != want {
		t.Fatalf("HitRate() = %v, want %v", got, want)
	}
}

func TestLenAndBytesReflectEvictionAndUpdates(t *testing.T) {
	c := New[string, int](3, sizeOfInt)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Put("c", 3)
	if got := c.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	c.Put("d", 4) // evicts "a" (least recently used, never touched)
	if got := c.Len(); got != 3 {
		t.Fatalf("Len() after eviction = %d, want 3", got)
	}
	if got := c.Bytes(); got != 3 {
		t.Fatalf("Bytes() after eviction = %d, want 3", got)
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("Get(a) after it should have been evicted: ok = true, want false")
	}
}

// TestConcurrentGetAndPutIsRaceFree exercises many goroutines hammering
// Get and Put on the same cache simultaneously -- run with -race, this
// is the actual proof behind the type doc's "safe for concurrent use"
// claim, not just the claim itself.
func TestConcurrentGetAndPutIsRaceFree(t *testing.T) {
	c := New[int, int](50, sizeOfInt)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := (g*500 + i) % 100
				c.Put(key, key)
				c.Get(key)
			}
		}(g)
	}
	wg.Wait()
	// No specific assertion beyond "the race detector found nothing" --
	// the exact final contents depend on goroutine interleaving and
	// aren't the point of this test.
}
