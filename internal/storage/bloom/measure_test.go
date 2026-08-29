package bloom

import (
	"fmt"
	"math"
	"testing"
)

// TestFalsePositiveRateAgainstTheoreticalCurve is the measurement this
// package exists to produce, in the same "first real number" tradition
// as the election-time distribution (§10) and the recovery-time
// measurement at gigabyte scale (§10): a claim that this implementation
// follows the standard Bloom filter false-positive formula is only
// worth as much as a number showing it does, at more than one point on
// the curve.
//
// Method: build a filter over numKeys keys drawn from one namespace
// ("pos-..."), then Test it against numNegative keys drawn from a
// disjoint namespace ("neg-...") that were never added. Every true
// result on a neg- key is, by construction, a false positive -- there is
// no possibility of a true positive contaminating the count, since the
// two namespaces never overlap. The observed rate is compared against
// TheoreticalFalsePositiveRate for the same numKeys and bitsPerKey.
//
// The tolerance band is derived, not guessed: the number of false
// positives among numNegative independent Bernoulli trials, each with
// success probability p = the theoretical rate, is a Binomial(numNegative,
// p) random variable with standard deviation sqrt(p(1-p)/numNegative).
// The band is the theoretical rate plus or minus six of those standard
// deviations (astronomically unlikely to be exceeded by chance alone)
// plus a 10% relative margin on top, to absorb the fact that FNV-1/1a
// are fast, well-distributed hashes but not the uniform-random oracle
// the formula's derivation assumes. A real implementation bug -- wrong
// bit indexing, a broken hash, k off by one -- moves the observed rate
// by far more than this band allows; six sigma plus 10% is there to make
// this test reliable across runs and machines, not to hide a real
// discrepancy.
func TestFalsePositiveRateAgainstTheoreticalCurve(t *testing.T) {
	const (
		numKeys     = 50_000
		numNegative = 200_000
	)
	// Three points on the curve, deliberately spread wide rather than
	// clustered around one "recommended" value: 6 is an aggressive,
	// space-favoring setting; 10 is the value LevelDB and RocksDB both
	// ship as their default; 14 is an accuracy-favoring setting. A
	// reader should be able to see the rate fall by roughly an order of
	// magnitude across the three, which a narrower spread wouldn't show.
	bitsPerKeySettings := []int{6, 10, 14}

	posKeys := make([][]byte, numKeys)
	for i := range posKeys {
		posKeys[i] = []byte(fmt.Sprintf("pos-%08d", i))
	}
	negKeys := make([][]byte, numNegative)
	for i := range negKeys {
		negKeys[i] = []byte(fmt.Sprintf("neg-%08d", i))
	}

	t.Logf("%-12s %-4s %-10s %-14s %-14s %-8s", "bits/key", "k", "bits", "theoretical", "observed", "ratio")

	for _, bitsPerKey := range bitsPerKeySettings {
		f := New(numKeys, bitsPerKey)
		for _, key := range posKeys {
			f.Add(key)
		}

		falsePositives := 0
		for _, key := range negKeys {
			if f.Test(key) {
				falsePositives++
			}
		}
		observed := float64(falsePositives) / float64(numNegative)
		theoretical := TheoreticalFalsePositiveRate(numKeys, bitsPerKey)

		sd := math.Sqrt(theoretical * (1 - theoretical) / float64(numNegative))
		margin := 6*sd + 0.10*theoretical
		lower, upper := theoretical-margin, theoretical+margin
		if lower < 0 {
			lower = 0
		}

		t.Logf("%-12d %-4d %-10d %-14.5f %-14.5f %-8.3f",
			bitsPerKey, f.K(), f.NumBits(), theoretical, observed, observed/theoretical)

		if observed < lower || observed > upper {
			t.Errorf("bits/key=%d: observed FPR %.5f (%d/%d) outside [%.5f, %.5f] around theoretical %.5f",
				bitsPerKey, observed, falsePositives, numNegative, lower, upper, theoretical)
		}
	}
}

// TestFalsePositiveRateFallsAsBitsPerKeyRises is a cheap, coarse sanity
// check on the shape of the curve rather than its exact values: more
// space per key must never make the filter WORSE. If this ever fails,
// something is wrong with OptimalK or sizeFor regardless of what the
// precise numbers above say.
func TestFalsePositiveRateFallsAsBitsPerKeyRises(t *testing.T) {
	const numKeys = 20_000
	settings := []int{4, 8, 12, 16, 20}
	var prev float64 = 1.0
	for _, bitsPerKey := range settings {
		rate := TheoreticalFalsePositiveRate(numKeys, bitsPerKey)
		if rate >= prev {
			t.Fatalf("TheoreticalFalsePositiveRate(%d, %d) = %.6f, want strictly less than the previous setting's %.6f",
				numKeys, bitsPerKey, rate, prev)
		}
		prev = rate
	}
}
