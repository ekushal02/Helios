// Package sstable implements the immutable, sorted-on-disk file a memtable
// flushes into once it is full. The byte layout is fixed on paper in
// DESIGN.md §13.2; this package is the block writer, index builder, and
// reader that turn that layout into working code.
//
// An SSTable is written exactly once, in one pass, and never modified after
// that -- "immutable" is not a suggestion here, it is why the format has no
// in-place update path at all. A later flush produces a new file; an older
// one is only ever deleted outright, by compaction, once every key it holds
// is superseded. Nothing in this package rewrites an existing file.
package sstable

import "errors"

// entryType discriminates a data block entry the same way wal.RecordType
// discriminates a WAL record, and is deliberately its own type rather than
// an import of wal.RecordType: this package depends on memtable (to drain
// one via its Iterator) but not on wal, keeping the dependency one-way in
// the same spirit as memtable not importing wal (DESIGN.md §13.3).
type entryType uint8

const (
	entryPut    entryType = 0
	entryDelete entryType = 1
)

// Sizes of the fixed-width fields used throughout this package's framing.
const (
	typeSize   = 1
	lenSize    = 4 // KeyLen, ValueLen, and index BlockLength are all uint32
	offsetSize = 8 // BlockOffset and the two footer fields are uint64
	crcSize    = 4

	// footerSize is IndexOffset(8) + IndexLength(8) + Magic(8).
	footerSize = offsetSize + offsetSize + 8
)

// magic identifies a well-formed Helios SSTable, read from the last 8 bytes
// of the file. It plays the same role magic[4] plays in Raft's persistent
// state record (DESIGN.md §5) and CRC32 plays in a WAL record header
// (§13.1): a reader that opens the wrong file, a truncated one, or one from
// an incompatible future version learns that immediately from a mismatch
// instead of decoding garbage as a plausible-looking footer.
var magic = [8]byte{'H', 'E', 'L', 'I', 'O', 'S', 'S', 'T'}

// ErrNotSSTable means the trailing 8 bytes of a file did not match magic --
// either it is not a Helios SSTable, or it is truncated badly enough that
// the footer itself is gone.
var ErrNotSSTable = errors.New("sstable: bad magic")

// ErrCorruptBlock means a data block's bytes were read in full but did not
// match its trailing CRC32.
var ErrCorruptBlock = errors.New("sstable: corrupt block")

// ErrEmptySource means Write was asked to build an SSTable from a Source
// that produced nothing. An SSTable with zero data blocks is
// representable on disk -- index and footer alone -- but nothing upstream
// ever has a legitimate reason to produce one, and admitting it silently
// would only hide a caller bug: an already-flushed memtable handed back
// in, or a fresh one flushed before anything was ever put in it.
var ErrEmptySource = errors.New("sstable: refusing to write an empty SSTable")
