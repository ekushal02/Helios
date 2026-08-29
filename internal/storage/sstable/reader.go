package sstable

import (
	"bytes"
	"fmt"
	"os"
	"sort"
)

// Reader opens an existing SSTable and answers point lookups against it.
// It holds the file open and the index in memory; data blocks are read
// from disk on demand, one per Get, which is the whole point of having an
// index: a lookup costs one seek and one read, never a scan of the file.
//
// A Reader is read-only and safe for concurrent use by any number of
// goroutines -- opening does all the mutation this type ever does, and
// every field after that is either immutable (the index) or accessed
// through *os.File.ReadAt, which does not share or move a file cursor
// across concurrent callers the way Read/Seek would.
type Reader struct {
	path  string
	file  *os.File
	index []indexEntry
}

// Open reads path's footer and index into memory and returns a Reader
// ready to answer Get calls. It does not read any data block yet -- doing
// that eagerly would defeat the reason the index exists.
//
// Open returns ErrNotSSTable if the trailing magic does not match, which
// covers both "this is not a Helios SSTable" and "this file is truncated
// badly enough that even the footer is gone" -- Write's own atomicity
// guarantee is what should make the second case unreachable in practice,
// but Open does not trust that guarantee blindly any more than Replay
// trusts a WAL record's Length field without also checking its CRC.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}
	success := false
	defer func() {
		if !success {
			f.Close()
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	if info.Size() < footerSize {
		return nil, fmt.Errorf("sstable: %s: %w", path, ErrNotSSTable)
	}

	footerBuf := make([]byte, footerSize)
	if _, err := f.ReadAt(footerBuf, info.Size()-footerSize); err != nil {
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	ft, err := decodeFooter(footerBuf)
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: %w", path, err)
	}

	if ft.IndexOffset+ft.IndexLength > uint64(info.Size()-footerSize) {
		return nil, fmt.Errorf("sstable: %s: index extends past end of file: %w", path, ErrNotSSTable)
	}
	indexBuf := make([]byte, ft.IndexLength)
	if _, err := f.ReadAt(indexBuf, int64(ft.IndexOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read index: %w", err)
	}
	index, err := decodeIndex(indexBuf)
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: decode index: %w", path, err)
	}

	success = true
	return &Reader{path: path, file: f, index: index}, nil
}

// Close releases the underlying file.
func (r *Reader) Close() error {
	return r.file.Close()
}

// Path returns the file this Reader was opened from.
func (r *Reader) Path() string { return r.path }

// NumBlocks reports how many data blocks the file holds.
func (r *Reader) NumBlocks() int { return len(r.index) }

// Get looks up key. The three-outcome contract matches
// (*memtable.Memtable).Get exactly, and for the same reason: a caller
// merging this SSTable underneath one or more memtables (DESIGN.md §13.3)
// needs to tell "never written here" apart from "deleted here, stop
// looking further down," and collapsing that distinction is the read bug
// that whole design already argues against.
func (r *Reader) Get(key []byte) (value []byte, tombstone bool, ok bool, err error) {
	// The index holds one entry per block, keyed by that block's LAST
	// key, in block order -- so the first entry whose LastKey is >= key
	// is the only block key could possibly be in. bytes.Compare gives
	// the ordering sort.Search needs.
	i := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].LastKey, key) >= 0
	})
	if i == len(r.index) {
		return nil, false, false, nil // past the last block's last key
	}

	raw := make([]byte, r.index[i].BlockLength)
	if _, err := r.file.ReadAt(raw, int64(r.index[i].BlockOffset)); err != nil {
		return nil, false, false, fmt.Errorf("sstable: read block: %w", err)
	}
	body, err := verifyAndSplitBlock(raw)
	if err != nil {
		return nil, false, false, fmt.Errorf("sstable: %s: %w", r.path, err)
	}
	entries, err := decodeBlockEntries(body)
	if err != nil {
		return nil, false, false, fmt.Errorf("sstable: %s: %w", r.path, err)
	}

	// Linear scan within the block. §13.2 leaves prefix-compressed,
	// binary-searchable blocks as an open question; a block is bounded by
	// targetBlockSize, so this scan is bounded too, and correctness comes
	// first for a task whose job is producing a file a reader can trust
	// at all.
	for _, e := range entries {
		if bytes.Equal(e.Key, key) {
			return e.Value, e.Tombstone, true, nil
		}
	}
	return nil, false, false, nil
}
