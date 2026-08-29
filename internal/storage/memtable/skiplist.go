// Package memtable implements the in-memory, sorted table that sits in
// front of the LSM engine's SSTables: every write lands here first, reads
// check it before falling through to on-disk data, and a flush turns its
// sorted contents into the SSTable byte layout DESIGN.md §13.2 already
// fixes on paper.
//
// The data structure is a skip list (Pugh, 1990), built from scratch
// rather than pulled in as a dependency, on the same principle as every
// other seam in this codebase: the concurrency contract a memtable needs
// -- lock-free reads, one writer at a time -- is exactly the contract a
// skip list gives for free once its forward pointers are atomic, and nothing
// off the shelf documents that contract as precisely as writing it does.
package memtable

import (
	"bytes"
	"math/rand"
	"sync/atomic"
)

// maxLevel bounds how tall the skip list's tower of forward-pointer lanes
// can grow. With levelProbability = 0.25, level L supports roughly 4^L
// entries before a lookup's expected cost starts climbing above O(log n);
// 32 levels cover on the order of 10^19 entries, which is to say the bound
// is not the thing that will ever be reached. This matches the parameters
// Redis's skip list uses, for the same reason: they are well-exercised
// numbers, not ones this package needed to rediscover.
const maxLevel = 32

// levelProbability is the chance a node promoted to level i is also
// promoted to level i+1. Lower means a flatter, cheaper-per-insert list
// with longer expected search chains at level 0; higher means more
// pointers per node in exchange for shorter searches. 0.25 is the
// standard choice balancing the two -- level count concentrates around
// 1/(1-p) ≈ 1.33 average pointers per node, and the tail that does reach
// deep levels is exactly the tail that makes long-range search jumps
// cheap.
const levelProbability = 0.25

// entryValue is what a node's value actually is: the current value bytes,
// and whether this key is a tombstone (a recorded deletion) rather than a
// live write. Both fields must become visible to a reader together or not
// at all -- a reader that saw a new value paired with a stale tombstone
// bit, or vice versa, would either resurrect a deleted key or silently
// drop a write. Boxing them in one struct and swapping the struct pointer
// atomically is what makes that impossible: there is no intermediate
// state a concurrent Get can observe between the old entryValue and the
// new one.
type entryValue struct {
	value     []byte
	tombstone bool
}

// node is one key in the skip list. Its key and the length of its forward
// slice are fixed at construction and never change; only entry is ever
// updated in place, and only ever via an atomic pointer swap.
type node struct {
	key     []byte
	entry   atomic.Pointer[entryValue]
	forward []atomic.Pointer[node] // one lane per level this node participates in, index 0 upward
}

// search walks the skip list from the head down to level 0, stopping at
// the last node on each level whose key is strictly less than key.
// preds[i] is that stopping point at level i for every i below the list's
// current height; candidate is the node at key if one exists, else nil.
//
// search performs only atomic loads and never blocks on anything. That is
// what makes it safe to call from any number of goroutines at once, and
// safe to call while a single writer is splicing a new node in
// concurrently: a reader either observes a forward pointer before the
// splice (and walks past where the new node will go) or after it (and
// walks through the new node, which is always fully initialized before it
// is published -- see (*Memtable).insert). There is no ordering in which a
// reader can observe a node that is only half there.
func search(head *node, height int32, key []byte) (preds [maxLevel]*node, candidate *node) {
	x := head
	for i := height - 1; i >= 0; i-- {
		for {
			next := x.forward[i].Load()
			if next == nil || bytes.Compare(next.key, key) >= 0 {
				break
			}
			x = next
		}
		preds[i] = x
	}
	if next := preds[0].forward[0].Load(); next != nil && bytes.Equal(next.key, key) {
		candidate = next
	}
	return preds, candidate
}

// randomLevel draws a level for a newly inserted node: 1 with probability
// (1-p), 2 with probability p(1-p), and so on up to maxLevel, where a run
// of maxLevel consecutive successes is capped rather than extended
// further. rng is not safe for concurrent use by design -- see the doc on
// Memtable.rng for why that is fine here.
func randomLevel(rng *rand.Rand) int {
	level := 1
	for level < maxLevel && rng.Float64() < levelProbability {
		level++
	}
	return level
}