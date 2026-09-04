package linearize

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// t0 anchors every hand-built Operation's own Start/End below to a fixed,
// arbitrary base time -- the exact wall-clock instant never matters, only
// the relative ordering, but a fixed base keeps every test's own timeline
// readable as small integers rather than real Now() values.
var t0 = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func getOp(client int, startMS, endMS int, key string, value string, found bool) Operation {
	return Operation{
		ClientID: client, Start: at(startMS), End: at(endMS),
		Input: KVInput{Op: KVGet, Key: key}, Output: KVOutput{Value: value, Found: found},
	}
}

func putOp(client int, startMS, endMS int, key, value string) Operation {
	return Operation{
		ClientID: client, Start: at(startMS), End: at(endMS),
		Input: KVInput{Op: KVPut, Key: key, Value: value}, Output: KVOutput{},
	}
}

func deleteOp(client int, startMS, endMS int, key string) Operation {
	return Operation{
		ClientID: client, Start: at(startMS), End: at(endMS),
		Input: KVInput{Op: KVDelete, Key: key}, Output: KVOutput{},
	}
}

func TestEmptyHistoryIsTriviallyLinearizable(t *testing.T) {
	res, err := CheckLinearizability(nil, KVModel{})
	if err != nil {
		t.Fatalf("CheckLinearizability(nil): %v", err)
	}
	if !res.Linearizable {
		t.Fatal("an empty history should vacuously linearize")
	}
}

// TestSequentialNonOverlappingHistoryIsLinearizable is the base case: no
// concurrency at all, one real-time order, and it must be found.
func TestSequentialNonOverlappingHistoryIsLinearizable(t *testing.T) {
	ops := []Operation{
		putOp(0, 0, 10, "a", "1"),
		getOp(0, 20, 30, "a", "1", true),
		putOp(0, 40, 50, "a", "2"),
		getOp(0, 60, 70, "a", "2", true),
		deleteOp(0, 80, 90, "a"),
		getOp(0, 100, 110, "a", "", false),
	}
	res, err := CheckLinearizability(ops, KVModel{})
	if err != nil {
		t.Fatalf("CheckLinearizability: %v", err)
	}
	if !res.Linearizable {
		t.Fatalf("a plain, non-overlapping, internally-consistent history was reported non-linearizable: %+v", res.Witness)
	}
}

// TestConcurrentOperationsOnDifferentKeysAreLinearizable checks that
// overlap alone is never a problem -- concurrent writes to DIFFERENT keys
// never conflict, in any order.
func TestConcurrentOperationsOnDifferentKeysAreLinearizable(t *testing.T) {
	ops := []Operation{
		putOp(0, 0, 100, "a", "1"), // both span [0,100] -- fully concurrent
		putOp(1, 0, 100, "b", "2"),
		getOp(0, 110, 120, "a", "1", true),
		getOp(1, 110, 120, "b", "2", true),
	}
	res, err := CheckLinearizability(ops, KVModel{})
	if err != nil {
		t.Fatalf("CheckLinearizability: %v", err)
	}
	if !res.Linearizable {
		t.Fatalf("concurrent writes to different keys should always linearize: %+v", res.Witness)
	}
}

// TestStaleReadIsNotLinearizable is the classic counterexample every
// linearizability checker must catch: a read that starts strictly AFTER a
// write to the same key has already completed, but does not see that
// write. No linearization can explain this -- the write must precede the
// read in real time, and once applied, the read's own recorded output no
// longer matches what the model produces.
func TestStaleReadIsNotLinearizable(t *testing.T) {
	ops := []Operation{
		putOp(0, 0, 10, "x", "1"),        // completes at t=10
		getOp(1, 20, 30, "x", "", false), // starts at t=20, claims x was never found
	}
	res, err := CheckLinearizability(ops, KVModel{})
	if err != nil {
		t.Fatalf("CheckLinearizability: %v", err)
	}
	if res.Linearizable {
		t.Fatal("a read that missed an already-completed write to the same key must not linearize")
	}
	if res.Witness.OpIndex != 1 {
		t.Errorf("Witness.OpIndex = %d, want 1 (the stale Get)", res.Witness.OpIndex)
	}
	if res.Witness.ClientID != 1 {
		t.Errorf("Witness.ClientID = %d, want 1", res.Witness.ClientID)
	}
}

// TestConcurrentWritesRequireTheCorrectOrder is the positive counterpart to
// the stale-read test: two concurrent Puts to the SAME key, where real
// time alone does not decide the order, followed by a read that pins down
// which one must have gone last. The search has to actually FIND that one
// specific ordering among the two real-time-legal candidates, not just
// notice that ordering exists in principle.
func TestConcurrentWritesRequireTheCorrectOrder(t *testing.T) {
	ops := []Operation{
		putOp(0, 0, 100, "x", "a"), // concurrent with the next
		putOp(1, 0, 100, "x", "b"),
		getOp(2, 110, 120, "x", "b", true), // pins Put(x,b) as the LAST write
	}
	res, err := CheckLinearizability(ops, KVModel{})
	if err != nil {
		t.Fatalf("CheckLinearizability: %v", err)
	}
	if !res.Linearizable {
		t.Fatalf("a valid ordering exists (a then b) and must be found: %+v", res.Witness)
	}
}

// TestConcurrentWritesRequireTheCorrectOrder's own negative twin: the same
// shape, but the trailing read claims a value NEITHER concurrent write
// produced -- no ordering of the two writes can explain it.
func TestConcurrentWritesWithAnImpossibleReadIsNotLinearizable(t *testing.T) {
	ops := []Operation{
		putOp(0, 0, 100, "x", "a"),
		putOp(1, 0, 100, "x", "b"),
		getOp(2, 110, 120, "x", "c", true), // neither write produced "c"
	}
	res, err := CheckLinearizability(ops, KVModel{})
	if err != nil {
		t.Fatalf("CheckLinearizability: %v", err)
	}
	if res.Linearizable {
		t.Fatal("a read claiming a value neither concurrent write produced must not linearize")
	}
}

// TestOperationCountAboveLimitIsRejected locks in maxOperations' own
// documented, structural limit -- an error, not a silent wrong answer or a
// panic.
func TestOperationCountAboveLimitIsRejected(t *testing.T) {
	ops := make([]Operation, maxOperations+1)
	for i := range ops {
		ops[i] = putOp(0, i, i+1, fmt.Sprintf("k%d", i), "v")
	}
	if _, err := CheckLinearizability(ops, KVModel{}); err == nil {
		t.Fatalf("CheckLinearizability with %d operations: err = nil, want an error (limit is %d)", len(ops), maxOperations)
	}
}

// TestMalformedOperationIsRejected checks the End-before-Start sanity
// guard directly -- malformed input produces an explicit error, not a
// meaningless answer computed from it anyway.
func TestMalformedOperationIsRejected(t *testing.T) {
	ops := []Operation{putOp(0, 100, 50, "a", "1")} // End (50) before Start (100)
	if _, err := CheckLinearizability(ops, KVModel{}); err == nil {
		t.Fatal("CheckLinearizability with End before Start: err = nil, want an error")
	}
}

// =============================================================================
// Randomized correctness check
// =============================================================================

// TestRandomizedKnownGoodHistoriesAlwaysLinearize builds many histories by
// a construction that GUARANTEES at least one valid linearization exists
// (draw a random sequential trace, apply it to a real KVModel to know what
// each operation's own Output must be, THEN spread overlapping [Start,
// End] windows around operations that do not conflict) and confirms the
// checker agrees on every one. This is not a test of one hand-picked
// scenario; it is many, so a narrow bug that only one specific shape of
// history would expose has many independent chances to be caught.
func TestRandomizedKnownGoodHistoriesAlwaysLinearize(t *testing.T) {
	rng := rand.New(rand.NewSource(20260904))
	const trials = 30
	const opsPerTrial = 12

	for trial := 0; trial < trials; trial++ {
		keys := []string{"a", "b", "c"}
		model := map[string]string{}
		var ops []Operation

		clock := 0
		for i := 0; i < opsPerTrial; i++ {
			key := keys[rng.Intn(len(keys))]
			start := clock
			// A real op takes 1-5ms; the NEXT op's own Start is allowed to
			// overlap by up to 3ms into this one, so some real concurrency
			// actually happens, not just back-to-back sequential timing.
			dur := 1 + rng.Intn(5)
			end := start + dur
			clock = end - rng.Intn(min(3, dur))

			switch rng.Intn(3) {
			case 0: // Get
				value, found := model[key]
				ops = append(ops, getOp(i%3, start, end, key, value, found))
			case 1: // Put
				value := fmt.Sprintf("v%d", i)
				model[key] = value
				ops = append(ops, putOp(i%3, start, end, key, value))
			case 2: // Delete
				delete(model, key)
				ops = append(ops, deleteOp(i%3, start, end, key))
			}
		}

		res, err := CheckLinearizability(ops, KVModel{})
		if err != nil {
			t.Fatalf("trial %d: CheckLinearizability: %v", trial, err)
		}
		if !res.Linearizable {
			t.Errorf("trial %d: a history constructed to have a known-valid linearization (the exact sequential order it was generated in) was reported non-linearizable: witness=%+v",
				trial, res.Witness)
		}
	}
}
