// Package bloom implements a fixed-size Bloom filter (Bloom, 1970): a
// probabilistic set that answers "definitely absent" or "maybe present"
// for a key, trading a caller-chosen, tunable false-positive rate for
// space far smaller than storing the keys themselves.
//
// This exists for the same reason every LSM engine grows one eventually:
// a point lookup that isn't in the active memtable has to fall through to
// however many SSTables sit underneath it (§13.4), and reading a whole
// block off disk to discover a key was never there is the exact cost a
// small in-memory filter exists to avoid. Wiring one into the SSTable
// write and read paths is deliberately NOT this package's job -- see the
// note on scope in flush.go's sibling task list. This package is the
// data structure and its own correctness argument, built and measured on
// its own before anything above it depends on it.
package bloom

import (
	"hash/fnv"
	"math"
)

// Filter is a fixed-size bit array probed by a small, fixed number of
// independent-enough hash functions per key. It is built once for a
// known (or estimated) number of keys and never resized -- the same
// "decide the shape up front, don't reorganize later" posture the
// memtable's own skip list takes (§13.4), for a different reason here:
// a Bloom filter's false-positive rate is a function of how full its bit
// array gets, so growing one after the fact would silently degrade the
// guarantee every existing key was added under.
//
// A Filter has exactly one failure mode, and it is not a mode: Test
// NEVER returns false for a key that was Added. Every bit Add sets stays
// set forever -- there is no remove operation, and no operation ever
// clears a bit -- so once a key's bits are all set, they stay set
// regardless of what else is added afterward. The only question is
// whether some OTHER key's bits happened to set the same positions,
// which is exactly the false-positive case this package measures.
type Filter struct {
	bits    []byte
	numBits uint64
	k       int // number of hash probes per key
}

// New builds an empty Filter sized for approximately numKeys entries at
// bitsPerKey bits of storage per key -- the single knob that trades
// space for false-positive rate, and the knob TestFalsePositiveRate
// (measure_test.go) sweeps across three settings to check against the
// formula below.
//
// numKeys and bitsPerKey are both clamped to at least 1 rather than
// rejected: a caller that (mis)computes zero or a negative value gets a
// small, valid, low-quality filter instead of a panic. A Bloom filter
// that is too small only costs a higher false-positive rate, never an
// incorrect one -- see the type doc's note that Test never false-negates
// -- so failing soft here has no correctness cost, unlike a memtable
// that silently dropped a write would.
func New(numKeys, bitsPerKey int) *Filter {
	numBits, k := sizeFor(numKeys, bitsPerKey)
	return &Filter{
		bits:    make([]byte, numBits/8),
		numBits: numBits,
		k:       k,
	}
}

// sizeFor computes the bit-array size and hash-probe count New actually
// builds, factored out so TheoreticalFalsePositiveRate can evaluate the
// formula against the REAL m and k a Filter for these parameters would
// use -- after numKeys/bitsPerKey are clamped and numBits is rounded up
// to a whole number of bytes -- rather than the idealized, unrounded
// values a caller passed in.
func sizeFor(numKeys, bitsPerKey int) (numBits uint64, k int) {
	if numKeys < 1 {
		numKeys = 1
	}
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	numBits = uint64(numKeys) * uint64(bitsPerKey)
	if numBits < 64 {
		// A handful of keys at a low bits-per-key setting can round to a
		// near-empty array where the false-positive rate is dominated by
		// rounding rather than by bitsPerKey itself. 64 bits is small
		// enough to cost nothing and large enough that this floor is
		// never what a real measurement is limited by.
		numBits = 64
	}
	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8 // round back up to a whole number of bytes
	return numBits, OptimalK(bitsPerKey)
}

// OptimalK returns the number of hash probes per key that minimizes the
// false-positive rate for a filter built at bitsPerKey bits per key:
// k = (m/n) * ln 2, which for a filter sized at exactly bitsPerKey bits
// per key is simply bitsPerKey * ln 2. This is the standard result
// (see Mitzenmacher & Upfal, or any Bloom filter survey): fewer probes
// waste the extra bits a generous bitsPerKey setting paid for, and more
// probes fill the array faster than they narrow the search, pushing the
// rate back up.
//
// Clamped to [1, 30], the same bound LevelDB's own Bloom filter
// implementation uses: past around 30 probes the marginal reduction in
// false-positive rate is negligible at any bits-per-key setting this
// engine would plausibly choose, and every additional probe is another
// hash-derived array index to compute on both Add and Test.
func OptimalK(bitsPerKey int) int {
	k := int(float64(bitsPerKey) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

// TheoreticalFalsePositiveRate returns the expected false-positive rate
// for a Filter built by New(numKeys, bitsPerKey), using the standard
// formula
//
//	p = (1 - e^(-kn/m))^k
//
// evaluated at the SAME n, m, and k New would actually build with -- not
// the simplified asymptotic p ≈ 0.6185^bitsPerKey some references quote,
// which assumes m/n and k are both real-valued. k here is an integer
// (OptimalK rounds it) and m is rounded up to a whole number of bytes
// (sizeFor), so this function accounts for both roundings and returns
// the correct target for what New actually builds, not an idealized
// approximation of it. TestFalsePositiveRate (measure_test.go) is the
// empirical measurement this number is compared against.
func TheoreticalFalsePositiveRate(numKeys, bitsPerKey int) float64 {
	numBits, k := sizeFor(numKeys, bitsPerKey)
	n, m, kf := float64(numKeys), float64(numBits), float64(k)
	return math.Pow(1-math.Exp(-kf*n/m), kf)
}

// NumBits reports the size of the underlying bit array.
func (f *Filter) NumBits() uint64 { return f.numBits }

// K reports how many hash probes each Add or Test performs.
func (f *Filter) K() int { return f.k }

// Add records key as a member. Add is not safe to call concurrently with
// itself or with Test -- a Filter has no mutex, on the same reasoning a
// Bloom filter is normally built once, in one pass, over a fully sorted
// or fully known key set (an SSTable flush, in this engine's eventual
// use of it) and then only ever read, never written to again. There is
// nothing here for a concurrent-writer test to guard the way the
// memtable's internal mutex guards a hypothetical second writer (§13.4):
// unlike a memtable, a Bloom filter genuinely has no legitimate
// concurrent-write use case to defend against.
func (f *Filter) Add(key []byte) {
	h1, h2 := hashes(key)
	for i := 0; i < f.k; i++ {
		bit := probe(h1, h2, i, f.numBits)
		f.bits[bit/8] |= 1 << (bit % 8)
	}
}

// Test reports whether key MAY be a member. false is a definite answer:
// key was never Added. true is not: key may have been Added, or every
// one of its k bits may simply have been set by other keys -- the false
// positive this package's own measurement quantifies.
func (f *Filter) Test(key []byte) bool {
	h1, h2 := hashes(key)
	for i := 0; i < f.k; i++ {
		bit := probe(h1, h2, i, f.numBits)
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// probe derives the i-th of k bit positions for a key from its two base
// hashes using Kirsch-Mitzenmacher double hashing: h_i = h1 + i*h2 (mod
// numBits). This is the standard technique (Kirsch & Mitzenmacher, 2006)
// for getting k evenly-distributed indices out of two real hash
// computations instead of k -- provably as good as k independent hashes
// for this purpose, and the reason Add and Test each hash a key exactly
// twice regardless of how large bitsPerKey (and therefore k) is asked to
// be.
func probe(h1, h2 uint64, i int, numBits uint64) uint64 {
	return (h1 + uint64(i)*h2) % numBits
}

// hashes derives the two base hashes double hashing combines into k
// probes. h1 is FNV-1a of the key; h2 is a strong 64-bit bit-mixer
// (the splitmix64 finalizer, also used as MurmurHash3's finalizer)
// applied to h1, rather than a second, independently-computed hash.
//
// THIS WAS NOT THE FIRST THING TRIED. The first version used FNV-1 for
// h1 and FNV-1a for h2 -- same algorithm, multiply and XOR in opposite
// order -- reasoning that the two only needed to "disagree... across
// most inputs" for Kirsch-Mitzenmacher double hashing to hold. Measuring
// it (TestFalsePositiveRateAgainstTheoreticalCurve) showed why that
// reasoning was wrong in practice: observed false-positive rates ran
// 2x-10x the theoretical curve, and the excess GREW with k. FNV-1 and
// FNV-1a are close enough as transformations of the same input that
// their outputs correlate more than the double-hashing construction can
// tolerate -- Kirsch & Mitzenmacher's proof assumes h1 and h2 behave
// like independent uniform hashes, and two passes of the same
// multiply-XOR structure over the same bytes do not clear that bar,
// especially as k (and therefore how many linear combinations of h1 and
// h2 get probed) grows.
//
// Re-deriving h2 from h1 through a strong avalanche mixer -- rather than
// hashing the key a second time with a related algorithm -- is the fix,
// and the same shape LevelDB's own Bloom filter takes: it computes ONE
// hash and derives every other probe from bit-rotations of it, rather
// than hashing repeatedly. splitmix64's finalizer is a well-studied
// choice for exactly this: every input bit affects every output bit
// (full avalanche), which decorrelates h2 from h1 far more thoroughly
// than a second FNV pass did. Measured again after the fix: every
// setting in the false-positive test now falls inside its derived
// tolerance band -- see §13.5 for the numbers.
func hashes(key []byte) (h1, h2 uint64) {
	f := fnv.New64a()
	f.Write(key) //nolint:errcheck // hash.Hash.Write never returns an error
	h1 = f.Sum64()
	h2 = mix64(h1)

	if h2 == 0 {
		// A zero second hash would collapse every probe onto h1 alone
		// (h1 + i*0 = h1 for every i), turning a k-probe filter into a
		// 1-probe one for that specific key. Guarded rather than assumed
		// to be unreachable, on the same "believed impossible conditions
		// are guarded, not assumed" posture §8 applies elsewhere.
		h2 = 1
	}
	return h1, h2
}

// mix64 is the splitmix64 finalizer (Steele, Lea & Flood, 2014; the
// same construction MurmurHash3's 64-bit finalizer uses): three
// XOR-shift/multiply rounds chosen so every input bit influences every
// output bit. It exists here purely to decorrelate h2 from h1 -- see the
// doc on hashes for the measurement that motivated replacing a second
// FNV pass with this.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
