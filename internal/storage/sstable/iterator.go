package sstable

import "fmt"

// Iterator walks every entry in an SSTable, in ascending key order,
// decoding one data block at a time. It is Reader's counterpart to
// Get: where Get answers "is this one key here," Iterator answers
// "what's here, in order," which a point lookup was never built to do
// and a compaction (§13.8) cannot avoid needing.
//
// Iterator satisfies Source (writer.go), which is the entire reason it
// exists rather than some SSTable-specific scan type: a compaction reads
// several SSTables, merges them (Merge, merge.go), and hands the merged
// result straight back to Write to produce a new file, and none of that
// pipeline needs to know an SSTable-backed Source looks any different
// from a memtable-backed one. Reader.Get and Iterator share the same
// block-verification code (verifyAndSplitBlock, decodeBlockEntries) --
// Iterator just walks every block in file order instead of binary-
// searching the index for one.
//
// An Iterator holds no lock and shares the Reader's already-open file
// handle via ReadAt, which does not move a shared cursor -- concurrent
// Iterators over the same Reader, or an Iterator running alongside Get
// calls, do not interfere with each other, on the same reasoning
// Reader's own doc gives for why it is safe for concurrent use.
type Iterator struct {
	r        *Reader
	blockIdx int // next data block to load, once entries is exhausted
	entries  []blockEntry
	entryIdx int // next entry within entries
	cur      blockEntry
	err      error
}

// NewIterator returns an Iterator over r, positioned before the first
// entry. Call Next to advance.
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{r: r}
}

// Next advances to the next entry and reports whether one was found.
// Key, Value, and Tombstone are only valid to call after Next has
// returned true. Once Next returns false, call Err to find out whether
// iteration ended because the SSTable was fully consumed (Err returns
// nil) or because a block failed to read or verify partway through
// (Err returns that failure) -- collapsing those two outcomes is exactly
// the bug Source.Err (writer.go) was added to close off.
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false // already failed; stay failed rather than retry
	}
	for {
		if it.entryIdx < len(it.entries) {
			it.cur = it.entries[it.entryIdx]
			it.entryIdx++
			return true
		}
		if it.blockIdx >= len(it.r.index) {
			return false // every block consumed cleanly
		}

		e := it.r.index[it.blockIdx]
		raw := make([]byte, e.BlockLength)
		if _, err := it.r.file.ReadAt(raw, int64(e.BlockOffset)); err != nil {
			it.err = fmt.Errorf("sstable: iterator: read block %d: %w", it.blockIdx, err)
			return false
		}
		body, err := verifyAndSplitBlock(raw)
		if err != nil {
			it.err = fmt.Errorf("sstable: iterator: %s: block %d: %w", it.r.path, it.blockIdx, err)
			return false
		}
		entries, err := decodeBlockEntries(body)
		if err != nil {
			it.err = fmt.Errorf("sstable: iterator: %s: block %d: %w", it.r.path, it.blockIdx, err)
			return false
		}

		it.entries = entries
		it.entryIdx = 0
		it.blockIdx++
	}
}

func (it *Iterator) Key() []byte     { return it.cur.Key }
func (it *Iterator) Value() []byte   { return it.cur.Value }
func (it *Iterator) Tombstone() bool { return it.cur.Tombstone }

// Err reports the failure that stopped iteration early, or nil if the
// SSTable was consumed in full. See Next's doc for why this must always
// be checked once Next returns false, not just assumed to mean "done."
func (it *Iterator) Err() error { return it.err }
