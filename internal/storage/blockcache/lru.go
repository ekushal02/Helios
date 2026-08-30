// Package blockcache implements a generic, byte-size-bounded cache with
// least-recently-used eviction, built entirely from stdlib
// (container/list for the recency ordering, a map for O(1) lookup) --
// no new dependency, on the same precedent every other hand-rolled data
// structure in this engine already set (the skip list, §13.4; the
// hand-written SVG in cmd/ampplot, §13.11).
//
// It is called blockcache and lives under internal/storage because its
// one intended use is caching SSTable data blocks (§13.12), but the
// type itself, LRU, is fully generic and knows nothing about blocks,
// SSTables, or keys in the storage-engine sense -- it is reusable for
// any comparable key and any value whose approximate size a caller can
// name.
package blockcache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// entry is the payload stored at each container/list element -- the
// list itself only holds *entry values, so eviction (from the back) and
// promotion (MoveToFront) never need to know K or V beyond what's on
// this struct.
type entry[K comparable, V any] struct {
	key   K
	value V
	size  int64
}

// LRU is a fixed-capacity cache bounded by total value size (as reported
// by the sizeFn given to New), not by entry count -- entries can be very
// different sizes (an SSTable's data blocks are usually close to
// targetBlockSize, §13.2, but not exactly it), and a count-bounded cache
// would let total memory swing with whatever happened to be cached
// rather than staying within a caller-chosen budget.
//
// Safe for concurrent use by any number of goroutines: every operation
// holds a single mutex for its whole duration. This is a coarser lock
// than the finest possible (a sharded or lock-free LRU could do better
// under heavy contention), a deliberate simplification recorded in
// DESIGN.md §12 rather than solved here -- correctness first, on the
// same priority §13.2's original linear block scan gave over an unbuilt
// binary search.
type LRU[K comparable, V any] struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	sizeFn   func(V) int64
	ll       *list.List // front = most recently used, back = least
	items    map[K]*list.Element

	hits   atomic.Int64
	misses atomic.Int64
}

// New returns an empty LRU bounded to maxBytes total, as measured by
// sizeFn applied to each value. maxBytes <= 0 means "cache nothing" --
// Put is a no-op and Get always misses -- rather than a panic or an
// unbounded cache, so a caller that computes a zero or negative budget
// (an empty configuration, say) gets a harmless disabled cache instead
// of a surprise. This is checked in Put itself, ahead of Put's normal
// insert-then-evict sequence, specifically because that sequence's own
// "never evict the last remaining entry" leniency (see Put's doc) would
// otherwise leave exactly one entry cached even at a zero budget.
func New[K comparable, V any](maxBytes int64, sizeFn func(V) int64) *LRU[K, V] {
	return &LRU[K, V]{
		maxBytes: maxBytes,
		sizeFn:   sizeFn,
		ll:       list.New(),
		items:    make(map[K]*list.Element),
	}
}

// Get reports whether key is cached, and if so, its value -- and marks
// it most-recently-used, the same as any LRU cache's Get does, since a
// cache that never promoted on read would just be a fixed First-N cache
// wearing an LRU's name.
func (c *LRU[K, V]) Get(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, found := c.items[key]; found {
		c.ll.MoveToFront(el)
		c.hits.Add(1)
		return el.Value.(*entry[K, V]).value, true
	}
	c.misses.Add(1)
	var zero V
	return zero, false
}

// Put inserts or updates key's value, evicting least-recently-used
// entries as needed to stay within maxBytes.
//
// A SINGLE ENTRY LARGER THAN maxBytes IS STILL STORED, ALONE, RATHER
// THAN REJECTED OR IMMEDIATELY EVICTED. The eviction loop below never
// removes the last remaining entry, even one that alone exceeds the
// budget -- the alternative (evicting everything, including the entry
// Put was just asked to insert) would make Put silently a no-op for any
// value bigger than the configured budget, which is a worse surprise
// than one oversized entry temporarily pushing memory use above
// maxBytes. This is a real, reachable case, not a hypothetical: an
// SSTable data block is usually near targetBlockSize (§13.2) but is
// explicitly allowed to exceed it by one entry, so a cache sized smaller
// than the largest block that can occur would otherwise never
// successfully cache anything at all.
func (c *LRU[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.maxBytes <= 0 {
		// Handled as its own case, ahead of the insert-then-evict logic
		// below: that logic's "never evict the last remaining entry"
		// leniency (see the doc above) would otherwise leave exactly one
		// entry cached even at a zero or negative budget, since Put
		// always inserts before the eviction loop ever runs and that
		// loop refuses to evict a lone entry. A non-positive budget
		// means "cache nothing," not "cache exactly one thing" -- so
		// this is checked first, before there is anything to evict.
		return
	}

	size := c.sizeFn(value)
	if el, found := c.items[key]; found {
		old := el.Value.(*entry[K, V])
		c.curBytes += size - old.size
		old.value = value
		old.size = size
		c.ll.MoveToFront(el)
	} else {
		el := c.ll.PushFront(&entry[K, V]{key: key, value: value, size: size})
		c.items[key] = el
		c.curBytes += size
	}

	for c.curBytes > c.maxBytes && c.ll.Len() > 1 {
		back := c.ll.Back()
		e := back.Value.(*entry[K, V])
		c.ll.Remove(back)
		delete(c.items, e.key)
		c.curBytes -= e.size
	}
}

// Len reports how many entries are currently cached.
func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Bytes reports the current total size of every cached value, as
// measured by the sizeFn given to New.
func (c *LRU[K, V]) Bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// Hits and Misses count every Get call since New, for measurement and
// observability (§13.12's read-latency comparison uses both directly)
// -- not reset by Put, and not part of the eviction accounting itself.
func (c *LRU[K, V]) Hits() int64   { return c.hits.Load() }
func (c *LRU[K, V]) Misses() int64 { return c.misses.Load() }

// HitRate returns Hits / (Hits + Misses), or 0 if Get has never been
// called -- 0 rather than NaN, since "no data yet" and "always missed"
// should not be indistinguishable from a division by zero to a caller
// just logging or asserting on this number.
func (c *LRU[K, V]) HitRate() float64 {
	hits, misses := c.hits.Load(), c.misses.Load()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}
