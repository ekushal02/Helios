package memtable

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Memtable is a sorted, in-memory key/value table backed by a skip list.
// Any number of goroutines may call Get or iterate concurrently, with each
// other and with a single in-flight Put or Delete, without taking a lock.
// Writers are serialized by an internal mutex.
//
// A Memtable is not resized or reorganized once created; the caller
// decides when it is full and switches to a fresh one before flushing the
// old one to an SSTable. This package reports the size that decision
// should be based on (see ApproxSize) but does not make the decision
// itself -- deciding when a given size is "full," and doing the swap, is
// package sstable's FlushIfFull and the layer above it, not this one.
type Memtable struct {
	mu     sync.Mutex // serializes writers only; Get and iteration never take it
	head   *node
	height atomic.Int32 // number of active levels, always >= 1
	length atomic.Int64 // distinct keys currently tracked, tombstones included
	size   atomic.Int64 // approximate key+value bytes tracked; see ApproxSize

	// rng is a per-Memtable seeded source, touched only from insert while
	// mu is held. This is the same rule Raft's election timer follows
	// (DESIGN.md §10): never the global rand, so that a whole run --
	// here, a whole memtable's worth of level choices -- replays
	// identically from one seed, and two memtables never share state
	// that would make one's insert order affect another's level
	// distribution.
	rng *rand.Rand
}

// ApproxSize returns an approximate count of the key and value bytes
// currently held, tombstones included. It is the quantity a caller should
// threshold against to decide a Memtable is full -- not Len, which counts
// keys, not bytes, and would treat a memtable of one 10MB value the same
// as one holding a single byte. This is the same lesson the Raft log's own
// compaction trigger already paid for (DESIGN.md §10): measuring
// logLength() instead of the discardable-entry count it actually needed
// produced roughly 160x more snapshots than the metric that was actually
// wanted, and the fix was to measure the quantity the decision is really
// about rather than the cheapest proxy for it. Key and value bytes are
// exactly what a flush writes to disk (DESIGN.md §13.2), so they are the
// right proxy for "how big will the SSTable be" in a way an entry count
// never is.
//
// The number is approximate in one direction only: it counts every byte
// this Memtable's writer has ever added or replaced, but the skip list's
// own bookkeeping -- node structs, forward-pointer slices, the entryValue
// box -- is not included. That overhead is real but roughly constant per
// key regardless of value size, so it does not change where the threshold
// should sit so much as it means the true resident size is always
// somewhat larger than what this method reports.
//
// Deciding *when* a Memtable is full enough to act on this number, and
// switching a fresh Memtable into the write path before flushing the old
// one, stays outside this package for the same reason it always has (see
// the type doc): this method only answers "how big," never "is that big
// enough."
func (m *Memtable) ApproxSize() int64 {
	return m.size.Load()
}

// New returns an empty Memtable seeded from the current time.
func New() *Memtable {
	return NewWithSeed(time.Now().UnixNano())
}

// NewWithSeed returns an empty Memtable whose level choices are
// deterministic for a given seed and a given sequence of inserts. Tests
// use this to make skip-list shape (and therefore which code paths a run
// exercises) reproducible.
func NewWithSeed(seed int64) *Memtable {
	head := &node{forward: make([]atomic.Pointer[node], maxLevel)}
	m := &Memtable{head: head, rng: rand.New(rand.NewSource(seed))}
	m.height.Store(1)
	return m
}

// Put inserts key with value, or overwrites the value already stored for
// key. The key and value bytes are copied in; the caller's slices may be
// reused or mutated immediately after Put returns.
func (m *Memtable) Put(key, value []byte) {
	m.upsert(key, value, false)
}

// Delete records key as deleted. A subsequent Get on this Memtable reports
// the key as a tombstone rather than as absent -- the distinction an LSM
// read path needs to know it must not fall through to an older SSTable
// that still holds the value being deleted (DESIGN.md §13, once the read
// path above the memtable exists).
func (m *Memtable) Delete(key []byte) {
	m.upsert(key, nil, true)
}

func (m *Memtable) upsert(key, value []byte, tombstone bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	height := m.height.Load()
	preds, existing := search(m.head, height, key)

	entry := &entryValue{value: copyBytes(value), tombstone: tombstone}

	if existing != nil {
		// The key is already here. Adjust size by the difference before
		// swapping the value in -- old and new are never both counted, and
		// the key's own bytes were already charged once when this node
		// was first inserted and do not change on an overwrite.
		old := existing.entry.Load()
		m.size.Add(valueSize(entry) - valueSize(old))
		// Swap its value atomically rather than touching any forward
		// pointer -- a concurrent reader that holds a reference to this
		// node reads entry.Load() exactly once and gets either the old
		// entryValue or the new one, in full, never a mix of the two
		// (see entryValue's doc).
		existing.entry.Store(entry)
		return
	}
	m.size.Add(int64(len(key)) + valueSize(entry))

	level := randomLevel(m.rng)
	if int32(level) > height {
		// Nothing has ever been linked above the old height, so head is
		// trivially the correct predecessor at every new level being
		// activated -- there is no other candidate to search for.
		for i := height; i < int32(level); i++ {
			preds[i] = m.head
		}
		m.height.Store(int32(level))
	}

	n := &node{key: copyBytes(key), forward: make([]atomic.Pointer[node], level)}
	n.entry.Store(entry)

	for i := 0; i < level; i++ {
		// n's own forward pointers are set before n is reachable from
		// anywhere, which needs no atomics: nothing else can see n yet.
		n.forward[i].Store(preds[i].forward[i].Load())
		// Publishing n at level i is a single atomic store. A concurrent
		// reader loading preds[i].forward[i] observes either the old
		// successor or n -- and if it observes n, n.forward[i] was
		// already set to the correct next node one line above, so the
		// reader can keep walking without ever seeing n mid-splice.
		preds[i].forward[i].Store(n)
	}
	m.length.Add(1)
}

// Get looks up key. ok is false if key has never been written to this
// Memtable at all -- the caller should keep searching an older level.
// ok is true with tombstone true if key was deleted here -- the caller
// must stop searching older levels rather than falling through to a value
// this Memtable has already superseded. ok is true with tombstone false
// and value set otherwise.
//
// The returned value slice is not copied and must be treated as read-only,
// on the same obligation DESIGN.md §10 places on LogEntry.Command: Put
// already copied it in once, and copying it again on every read would pay
// an allocation for a guarantee the caller can keep for free by not
// mutating what it was handed.
func (m *Memtable) Get(key []byte) (value []byte, tombstone bool, ok bool) {
	_, candidate := search(m.head, m.height.Load(), key)
	if candidate == nil {
		return nil, false, false
	}
	e := candidate.entry.Load()
	return e.value, e.tombstone, true
}

// Len reports the number of distinct keys currently tracked, including
// tombstones. It is not a byte-size estimate; see the type doc.
func (m *Memtable) Len() int {
	return int(m.length.Load())
}

// ApplyPut and ApplyDelete are what let a Memtable be rebuilt from a WAL
// without this package ever importing package wal: they are exactly the
// two operations engine.RecoverMemtable calls from inside the closure it
// hands to wal.Recover, one per WAL record type, which is the actual
// wiring described here since v1.12 rather than the "wal.Sink interface"
// and "wal.RecoverAndOpen" that earlier versions of this comment claimed
// -- neither of which ever existed in package wal. See
// engine.RecoverMemtable's own doc for the correction in full.
func (m *Memtable) ApplyPut(key, value []byte) { m.Put(key, value) }
func (m *Memtable) ApplyDelete(key []byte)     { m.Delete(key) }

// valueSize is the byte contribution an entryValue makes toward
// ApproxSize: the value's length, or zero for a tombstone, which carries
// no value at all once flushed (see block.go's data block entry framing).
func valueSize(e *entryValue) int64 {
	if e.tombstone {
		return 0
	}
	return int64(len(e.value))
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
