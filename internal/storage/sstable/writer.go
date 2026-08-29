package sstable

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// targetBlockSize is the size, in encoded entry bytes, a data block is
// allowed to fill up to before the writer closes it out and opens a new
// one. It bounds how much a reader must buffer to satisfy one Get -- the
// same reason the WAL's own design doc (§13.1) gives for why LevelDB
// fragments its log into 32KB blocks, applied here to reads instead of
// writes. 4KB is a conservative starting point (a typical filesystem page)
// and not yet measured against this engine's own workload; see DESIGN.md
// §12 for that as an open question, the same way the WAL's own sync
// policy was measured after the fact rather than guessed correctly up
// front.
//
// A block is allowed to exceed this size by exactly one entry: the check
// only refuses to *start* a new entry in an already-over-target block, on
// purpose. Closing a block that has zero entries in it to satisfy a byte
// budget would leave a key that can never be written at all whenever a
// single entry's encoded size exceeds targetBlockSize outright -- the
// same "always make forward progress on the offending record" argument
// the WAL's fragmentation-avoidance note (§13.1) already makes for why a
// record is never split mid-flight.
const targetBlockSize = 4096

// Source is what Write drains into an SSTable: a forward-only cursor over
// entries already in ascending key order. *memtable.Iterator satisfies
// this structurally, the same way *memtable.Memtable satisfies wal.Sink
// structurally (DESIGN.md §13.4) -- this package needs memtable's
// iteration shape, not an import of it, and Source is that shape written
// down. (flush.go is the one file in this package that does import
// memtable, to offer the memtable-specific convenience of Flush and
// FlushIfFull; Write itself stays decoupled.)
//
// Err REPORTS A MID-ITERATION FAILURE, ADDED IN v1.13, AFTER Source
// GAINED ITS FIRST FALLIBLE IMPLEMENTATION. Every Source before
// sstable.Iterator (§13.8) was pure in-memory -- a *memtable.Iterator
// walks pointers already resident in RAM and cannot fail -- so Write's
// original loop, `for src.Next() { ... }`, never checked for a failure
// because there was structurally nothing that could produce one. An
// SSTable-backed Iterator reads blocks off disk mid-scan, which very much
// can fail (a corrupt block, an I/O error), and Next() returning false is
// the same observable outcome whether iteration finished cleanly or broke
// partway through. Without Err, a compaction merging several SSTables
// (§13.8) could silently produce a truncated output file and report
// success. Every Source must now report nil once exhausted cleanly, and
// the true cause once Next has returned false because of a failure --
// Write checks this immediately after its loop ends and refuses to
// publish a file if it is non-nil. *memtable.Iterator's Err trivially
// always returns nil, at zero real cost, rather than being exempted from
// the interface it was the original, and only, implementation of.
type Source interface {
	Next() bool
	Key() []byte
	Value() []byte
	Tombstone() bool
	Err() error
}

// Info summarizes a successfully written SSTable, for a caller that wants
// to record it in a manifest or decide what to do next -- neither of
// which exists yet; see DESIGN.md §12.
type Info struct {
	Path       string
	Entries    int
	DataBlocks int
	Bytes      int64
	MinKey     []byte
	MaxKey     []byte
}

var (
	// ErrOutOfOrder means Source produced a key that did not strictly
	// increase over the previous one. Every entry this package's own
	// Source implementations produce is already sorted (memtable.Iterator
	// walks the skip list's ordered level-0 lane), so this can only fire
	// against a caller-supplied Source that violates its contract -- a
	// believed-impossible condition guarded rather than assumed, on the
	// same principle DESIGN.md §8 applies to Raft's apply path.
	ErrOutOfOrder = errors.New("sstable: source produced keys out of order")

	// ErrFileExists means Write was asked to publish to a path that
	// already has a file at it. An SSTable is immutable once written
	// (see the package doc); silently overwriting one that a manifest or
	// another reader might already be holding open would violate that,
	// so this is refused rather than clobbered.
	ErrFileExists = errors.New("sstable: refusing to overwrite an existing file")
)

// Write drains src, in order, into a new immutable SSTable at path:
// sorted data blocks (block.go), an index over them (index.go), and a
// trailing footer, published all at once via the same write-temp / fsync
// / rename / fsync-directory sequence FileStorage uses for Raft's own
// persistent state (DESIGN.md §5) -- for the same reason. A write of more
// than one sector is not atomic, so a crash mid-write must never be
// allowed to leave a reader an SSTable whose footer, index, or trailing
// blocks are missing but whose leading bytes look like a valid file.
//
// UNLIKE THE WAL, THIS FILE IS ASSEMBLED ONCE, ALL AT ONCE. The WAL is
// append-only and a reader tolerates a torn tail by design (§13.1); an
// SSTable has no such tolerance because its footer is the only way in and
// it is meaningless until every block and the index behind it exist. That
// is exactly the case rename(2) exists for: build the whole thing under a
// name nothing is looking at yet, then publish it in one atomic step.
//
// Write returns ErrEmptySource if src yields nothing at all -- see the
// doc on that error for why an empty SSTable is refused rather than
// produced.
func Write(src Source, path string) (*Info, error) {
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileExists, path)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: create %s: %w", tmp, err)
	}
	// A temp file is never authoritative, the same rule NewFileStorage
	// applies to its own leftovers: if this function returns before
	// success is set true, whatever landed in tmp is the residue of a
	// write nothing acknowledged, and belongs deleted rather than left
	// to be mistaken for a real (if oddly named) file later.
	success := false
	defer func() {
		if !success {
			f.Close()
			os.Remove(tmp)
		}
	}()

	w := bufio.NewWriter(f)
	var (
		offset  int64
		index   []indexEntry
		block   []byte
		info    Info
		prevKey []byte
	)

	flushBlock := func(lastKey []byte) error {
		if len(block) == 0 {
			return nil
		}
		finalized := finalizeBlock(block)
		n, err := w.Write(finalized)
		if err != nil {
			return fmt.Errorf("sstable: write block: %w", err)
		}
		index = append(index, indexEntry{
			LastKey:     lastKey,
			BlockOffset: uint64(offset),
			BlockLength: uint32(n),
		})
		offset += int64(n)
		info.DataBlocks++
		block = block[:0]
		return nil
	}

	for src.Next() {
		key := src.Key()
		if prevKey != nil && bytes.Compare(key, prevKey) <= 0 {
			return nil, fmt.Errorf("%w: %q did not strictly follow %q", ErrOutOfOrder, key, prevKey)
		}

		var entryBytes []byte
		if src.Tombstone() {
			entryBytes = encodeDeleteEntry(nil, key)
		} else {
			entryBytes = encodePutEntry(nil, key, src.Value())
		}

		// Close the current block first if it already has something in
		// it and this entry would push it over target -- see the doc on
		// targetBlockSize for why an already-empty block is never closed
		// on this check.
		if len(block) > 0 && len(block)+len(entryBytes) > targetBlockSize {
			if err := flushBlock(prevKey); err != nil {
				return nil, err
			}
		}
		block = append(block, entryBytes...)

		info.Entries++
		if info.MinKey == nil {
			info.MinKey = key
		}
		info.MaxKey = key
		prevKey = key
	}

	if err := src.Err(); err != nil {
		// Checked before anything below is committed to disk, and before
		// the empty-source check: Next returning false because a source
		// failed partway through -- or failed on its very first call,
		// yielding zero entries -- must never be indistinguishable from a
		// source that simply ran out of entries cleanly. Every entry
		// successfully drained before the failure is discarded along
		// with the rest of this write; see the doc on Source.Err for why
		// a partial, silently-short file is exactly the outcome this
		// check exists to prevent.
		return nil, fmt.Errorf("sstable: source failed: %w", err)
	}
	if info.Entries == 0 {
		return nil, ErrEmptySource
	}

	if err := flushBlock(prevKey); err != nil {
		return nil, err
	}

	indexOffset := offset
	var indexBuf []byte
	for _, e := range index {
		indexBuf = encodeIndexEntry(indexBuf, e)
	}
	n, err := w.Write(indexBuf)
	if err != nil {
		return nil, fmt.Errorf("sstable: write index: %w", err)
	}
	offset += int64(n)

	footerBuf := encodeFooter(footer{
		IndexOffset: uint64(indexOffset),
		IndexLength: uint64(n),
	})
	if _, err := w.Write(footerBuf); err != nil {
		return nil, fmt.Errorf("sstable: write footer: %w", err)
	}
	offset += int64(len(footerBuf))

	if err := w.Flush(); err != nil {
		return nil, fmt.Errorf("sstable: flush: %w", err)
	}
	// Contents durable before the name is switched -- doing these in the
	// other order would let a crash expose a file at path that a reader
	// could open and find truncated.
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("sstable: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("sstable: close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, fmt.Errorf("sstable: rename into place: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("sstable: fsync directory: %w", err)
	}

	success = true
	info.Path = path
	info.Bytes = offset
	return &info, nil
}

// syncDir flushes a directory's metadata so that a rename into it
// survives a crash. Duplicated from raft.syncDir rather than shared,
// deliberately: the storage engine and Raft's persistent state are
// separate durability islands (DESIGN.md §13's opening paragraph), and
// this package does not import package raft for the same one-way-only
// reason memtable does not import wal.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		// Windows cannot fsync a directory handle. Helios targets unix,
		// so everywhere else this is a real failure and must not be
		// swallowed -- see raft.syncDir, whose comment this mirrors.
		if runtime.GOOS == "windows" {
			return nil
		}
		return err
	}
	return nil
}
