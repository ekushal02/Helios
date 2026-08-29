package memtable

// Iterator walks a Memtable's entries in ascending key order. It is the
// producer side of a flush: an SSTable writer will drain one of these to
// build the sorted data blocks DESIGN.md §13.2 lays out.
//
// Iterator performs the same lock-free traversal as Get, along the
// level-0 lane that always links every node in order (every node
// participates in level 0 regardless of its random height). It may
// therefore run concurrently with Get calls and with a single in-flight
// writer.
//
// It does not provide snapshot isolation. A key inserted after the
// iterator is created may or may not be observed, depending on whether the
// writer's splice at level 0 lands ahead of or behind the iterator's
// current position, and a value updated after its key was already visited
// is not re-read. Neither case can produce a torn read -- an entry update
// is a single atomic pointer swap, so any read is either the old
// entryValue or the new one in full -- but the sequence of keys observed
// is not a guaranteed snapshot of the Memtable at any single instant.
// That is the right contract for flushing a Memtable that has already
// been switched out of the write path (nothing writes to it once a newer
// Memtable is taking writes instead), and the wrong one for any caller
// that needs a consistent point-in-time view of one still being written
// to.
type Iterator struct {
	node *node // the current entry, or head before the first call to Next
}

// NewIterator returns an iterator positioned before the first entry. Call
// Next to advance to the first, and each entry after it, in ascending key
// order.
func (m *Memtable) NewIterator() *Iterator {
	return &Iterator{node: m.head}
}

// Next advances the iterator to the next entry and reports whether one
// was found. Key, Value, and Tombstone are only valid to call after Next
// has returned true.
func (it *Iterator) Next() bool {
	it.node = it.node.forward[0].Load()
	return it.node != nil
}

// Key returns the current entry's key. The returned slice must be treated
// as read-only, for the same reason Get's returned value must be.
func (it *Iterator) Key() []byte {
	return it.node.key
}

// Value returns the current entry's value. Undefined if Tombstone is
// true -- a deleted key carries no value to read.
func (it *Iterator) Value() []byte {
	return it.node.entry.Load().value
}

// Tombstone reports whether the current entry is a recorded deletion
// rather than a live value.
func (it *Iterator) Tombstone() bool {
	return it.node.entry.Load().tombstone
}

// Err always returns nil. A Memtable is walked entirely in memory --
// there is no I/O for a traversal to fail on -- so this exists purely to
// satisfy sstable.Source (§13.2), which added Err in v1.13 once an
// SSTable-backed Iterator (a Source that genuinely can fail mid-scan)
// existed for the first time. See sstable.Source's own doc for why every
// implementation, fallible or not, now carries this method.
func (it *Iterator) Err() error {
	return nil
}
