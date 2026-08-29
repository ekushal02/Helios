package bloom

import (
	"fmt"
	"testing"
)

// TestNoFalseNegatives checks the one invariant a Bloom filter is not
// allowed to violate under any circumstance: every key that was Added
// must Test true. This is exhaustive, not sampled -- there is no
// tolerance for this to fail even once.
func TestNoFalseNegatives(t *testing.T) {
	const n = 20000
	f := New(n, 10)
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		f.Add(keys[i])
	}
	for i, key := range keys {
		if !f.Test(key) {
			t.Fatalf("Test(%q) = false after Add, want true (key %d of %d)", key, i, n)
		}
	}
}

// TestEmptyFilterNeverMatches checks the other end: a Filter nothing has
// ever been Added to has every bit clear, so Test must reject everything
// -- if this ever returns true, either a bit is set that shouldn't be,
// or Test's read of the array is wrong independent of what Add does.
func TestEmptyFilterNeverMatches(t *testing.T) {
	f := New(1000, 10)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("never-added-%d", i))
		if f.Test(key) {
			t.Fatalf("Test(%q) on an empty filter = true, want false", key)
		}
	}
}

// TestSizingRoundsUpToAWholeNumberOfBytes pins the invariant Add and
// Test's bit-indexing math depends on implicitly: f.bits has exactly
// numBits/8 elements, with no remainder.
func TestSizingRoundsUpToAWholeNumberOfBytes(t *testing.T) {
	for _, tc := range []struct{ numKeys, bitsPerKey int }{
		{1, 1}, {7, 3}, {1000, 10}, {100000, 8}, {3, 1},
	} {
		f := New(tc.numKeys, tc.bitsPerKey)
		if f.NumBits()%8 != 0 {
			t.Errorf("New(%d, %d).NumBits() = %d, not a multiple of 8", tc.numKeys, tc.bitsPerKey, f.NumBits())
		}
		if uint64(len(f.bits))*8 != f.NumBits() {
			t.Errorf("New(%d, %d): len(bits)*8 = %d, want NumBits() = %d", tc.numKeys, tc.bitsPerKey, len(f.bits)*8, f.NumBits())
		}
	}
}

// TestDegenerateInputsAreClampedNotRejected checks New's documented
// fail-soft behavior on non-positive numKeys or bitsPerKey: a small,
// valid filter, never a panic.
func TestDegenerateInputsAreClampedNotRejected(t *testing.T) {
	for _, tc := range []struct{ numKeys, bitsPerKey int }{
		{0, 10}, {-5, 10}, {1000, 0}, {1000, -3}, {0, 0}, {-1, -1},
	} {
		f := New(tc.numKeys, tc.bitsPerKey)
		if f.NumBits() == 0 {
			t.Errorf("New(%d, %d).NumBits() = 0, want a positive size", tc.numKeys, tc.bitsPerKey)
		}
		if f.K() < 1 {
			t.Errorf("New(%d, %d).K() = %d, want >= 1", tc.numKeys, tc.bitsPerKey, f.K())
		}
		// Must still behave correctly, not just avoid panicking.
		f.Add([]byte("a"))
		if !f.Test([]byte("a")) {
			t.Errorf("New(%d, %d): Add/Test round trip failed on a degenerate filter", tc.numKeys, tc.bitsPerKey)
		}
	}
}

// TestOptimalKIsClampedToTheDocumentedRange pins OptimalK's [1, 30]
// bound at both ends.
func TestOptimalKIsClampedToTheDocumentedRange(t *testing.T) {
	if k := OptimalK(0); k != 1 {
		t.Errorf("OptimalK(0) = %d, want 1", k)
	}
	if k := OptimalK(-100); k != 1 {
		t.Errorf("OptimalK(-100) = %d, want 1", k)
	}
	if k := OptimalK(1_000_000); k != 30 {
		t.Errorf("OptimalK(1_000_000) = %d, want 30 (clamped)", k)
	}
	// A middle-of-the-road setting should land near, not at the
	// extremes -- 10 bits/key * ln2 ≈ 6.93, so 6 or 7.
	if k := OptimalK(10); k != 6 && k != 7 {
		t.Errorf("OptimalK(10) = %d, want 6 or 7 (10 * ln2 ≈ 6.93)", k)
	}
}

// TestTwoFiltersBuiltIdenticallyAgree checks that hashing has no hidden
// randomness (no seed, no map iteration order, nothing time-based): two
// Filters built and populated the same way must agree on every Test
// call, bit for bit. This is what makes the measurement in
// measure_test.go reproducible from one run to the next, and what would
// make a serialized filter (future work -- see the package doc) portable
// across processes at all.
func TestTwoFiltersBuiltIdenticallyAgree(t *testing.T) {
	build := func() *Filter {
		f := New(5000, 10)
		for i := 0; i < 5000; i++ {
			f.Add([]byte(fmt.Sprintf("k%06d", i)))
		}
		return f
	}
	a, b := build(), build()

	for i := 0; i < 6000; i++ {
		key := []byte(fmt.Sprintf("k%06d", i)) // 0..4999 were added, 5000..5999 were not
		if a.Test(key) != b.Test(key) {
			t.Fatalf("Test(%q) disagreed between two identically-built filters", key)
		}
	}
}
