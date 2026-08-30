package sstable

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/ekushal02/helios/internal/storage/blockcache"
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
// across concurrent callers the way Read/Seek would. cache, when present,
// is itself safe for concurrent use (blockcache.LRU, §13.12) and may be
// shared by any number of Readers over any number of files at once.
type Reader struct {
	path  string
	file  *os.File
	index []indexEntry
	cache *BlockCache // nil means "no cache" -- every Get reads and decodes from disk
}

// blockCacheKey identifies one data block, uniquely, across every file a
// shared BlockCache might ever be asked to hold blocks from.
//
// A CACHE KEYED THIS WAY CAN NEVER GO STALE, WHICH IS WHY THIS PACKAGE
// NEEDS NO INVALIDATION LOGIC AT ALL -- ONLY AN EVICTION ONE. An SSTable
// is written once and never modified again after Write publishes it
// (§13.2's whole ErrFileExists argument); the bytes at (path, blockIndex)
// today are the bytes that will be there for as long as the file exists
// at all. Compaction (§13.8) never edits a file in place either -- it
// always produces an entirely new file and leaves old ones for deletion,
// never mutation. A cache entry keyed by (path, blockIndex) is therefore
// either correct forever or eligible for eviction; there is no third
// state ("correct, but the underlying data changed") a cache normally has
// to defend against, because nothing in this engine can produce it.
type blockCacheKey struct {
	path       string
	blockIndex int
}

// BlockCache is the concrete cache type Reader uses: a blockcache.LRU
// (§13.12) keyed by blockCacheKey and holding a decoded []blockEntry per
// block -- decoded, not raw bytes, so a cache hit skips CRC verification
// and entry parsing entirely, not just the disk read. Exported as a type
// alias so a caller can hold and pass around a *BlockCache without ever
// needing to name blockCacheKey or blockEntry, both unexported.
type BlockCache = blockcache.LRU[blockCacheKey, []blockEntry]

// NewBlockCache returns a BlockCache bounded to maxBytes, sized by the
// summed key and value bytes of each block's decoded entries -- the
// same "count key and value bytes, not structural overhead" convention
// (*Memtable).ApproxSize already established (§13.3), applied here to a
// cached block instead of a whole memtable.
func NewBlockCache(maxBytes int64) *BlockCache {
	return blockcache.New[blockCacheKey, []blockEntry](maxBytes, blockEntriesSize)
}

func blockEntriesSize(entries []blockEntry) int64 {
	var size int64
	for _, e := range entries {
		size += int64(len(e.Key) + len(e.Value))
	}
	return size
}

// Open reads path's footer and index into memory and returns a Reader
// ready to answer Get calls, with no block cache -- every Get reads and
// decodes its block from disk. It does not read any data block yet --
// doing that eagerly would defeat the reason the index exists.
//
// Open returns ErrNotSSTable if the trailing magic does not match, which
// covers both "this is not a Helios SSTable" and "this file is truncated
// badly enough that even the footer is gone" -- Write's own atomicity
// guarantee is what should make the second case unreachable in practice,
// but Open does not trust that guarantee blindly any more than Replay
// trusts a WAL record's Length field without also checking its CRC.
func Open(path string) (*Reader, error) {
	return open(path, nil)
}

// OpenWithCache is Open, but every data block this Reader reads is first
// checked against, and after a miss stored into, cache -- which may
// already hold blocks from other Readers over other files, since a
// BlockCache is keyed by path as well as block index (see blockCacheKey's
// doc) and is safe for exactly this kind of sharing.
func OpenWithCache(path string, cache *BlockCache) (*Reader, error) {
	return open(path, cache)
}

func open(path string, cache *BlockCache) (*Reader, error) {
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
	return &Reader{path: path, file: f, index: index, cache: cache}, nil
}

// Close releases the underlying file. It does not touch the cache -- a
// shared BlockCache may still be holding, and serving, this file's
// blocks for other Readers, and a cache entry keyed by path stays
// correct even after this particular *os.File handle closes (see
// blockCacheKey's doc on why staleness is not a concern this package
// has to solve).
func (r *Reader) Close() error {
	return r.file.Close()
}

// Path returns the file this Reader was opened from.
func (r *Reader) Path() string { return r.path }

// NumBlocks reports how many data blocks the file holds.
func (r *Reader) NumBlocks() int { return len(r.index) }

// Get looks up key. The three-outcome contract matches
// (*memtable.Memtable).Get exactly, and for the same reason: a caller
// merging this SSTable underneath one or more memtables (DESIGN.md §13.4)
// needs to tell "never written here" apart from "deleted here, stop
// looking further down," and collapsing that distinction is the read bug
// that whole design already argues against.
//
// Get is two steps, deliberately kept separate rather than one fused loop:
// findBlock (below) does a binary search over the in-memory index to
// locate the single block key could be in, then loadBlock reads (or, on
// a cache hit, skips reading) that one block and Get scans its entries.
// The split is not just style -- findBlock's contract (which block, if
// any) is independent of how a block's bytes are verified, parsed, or
// cached, and TestFindBlock* in reader_test.go exercises it directly
// against a hand-built index with no file on disk at all, which a single
// fused function would not allow.
func (r *Reader) Get(key []byte) (value []byte, tombstone bool, ok bool, err error) {
	idx, e, found := r.findBlock(key)
	if !found {
		return nil, false, false, nil // past every block's last key
	}

	entries, err := r.loadBlock(idx, e)
	if err != nil {
		return nil, false, false, err
	}

	// Linear scan within the block. §13.2 leaves prefix-compressed,
	// binary-searchable blocks as an open question; a block is bounded by
	// targetBlockSize, so this scan is bounded too, and correctness comes
	// first for a task whose job is producing a file a reader can trust
	// at all.
	for _, en := range entries {
		if bytes.Equal(en.Key, key) {
			return en.Value, en.Tombstone, true, nil
		}
	}
	return nil, false, false, nil
}

// loadBlock returns the decoded entries for the block described by e, at
// position blockIdx within r.index -- consulting r.cache first, if one
// is set, and populating it on a miss.
//
// ONLY Get USES THE CACHE; Iterator (iterator.go) DOES NOT, DELIBERATELY.
// A full sequential scan -- the only thing Iterator is for, used by
// compaction's merges (§13.8) -- reads every block exactly once no
// matter what, so there is nothing a cache could save it from re-reading
// during that same scan, and populating the cache with blocks a merge
// happens to pass through would only evict genuinely-reusable
// point-lookup entries for other keys that were never going to be
// re-read by that merge anyway.
func (r *Reader) loadBlock(blockIdx int, e indexEntry) ([]blockEntry, error) {
	key := blockCacheKey{path: r.path, blockIndex: blockIdx}
	if r.cache != nil {
		if entries, hit := r.cache.Get(key); hit {
			return entries, nil
		}
	}

	raw := make([]byte, e.BlockLength)
	if _, err := r.file.ReadAt(raw, int64(e.BlockOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read block: %w", err)
	}
	body, err := verifyAndSplitBlock(raw)
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: %w", r.path, err)
	}
	entries, err := decodeBlockEntries(body)
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: %w", r.path, err)
	}

	if r.cache != nil {
		r.cache.Put(key, entries)
	}
	return entries, nil
}

// findBlock returns the position (blockIdx) and index metadata of the
// one data block that could contain key -- the first block, in file
// order, whose LastKey is >= key -- or ok=false if key sorts past every
// block's last key, meaning no block on disk can hold it.
//
// This is the entire on-disk-format reason the index is sorted by
// LastKey (§13.2): a block holds every key greater than the previous
// block's LastKey and up to and including its own, so the blocks
// partition the whole key space in order, and the first LastKey that is
// not less than key identifies the one partition key could fall in --
// whether or not key is actually present in it. bytes.Compare gives
// sort.Search the ordering it needs; the search touches only r.index,
// already resident in memory since Open, so a call that turns out to
// find nothing costs no disk read at all.
func (r *Reader) findBlock(key []byte) (blockIdx int, e indexEntry, ok bool) {
	i := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].LastKey, key) >= 0
	})
	if i == len(r.index) {
		return 0, indexEntry{}, false
	}
	return i, r.index[i], true
}
