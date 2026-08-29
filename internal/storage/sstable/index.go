package sstable

import (
	"encoding/binary"
)

// An index entry, one per data block, keyed by that block's last key so a
// lookup binary-searches the index in memory and then reads exactly one
// data block off disk:
//
//	+-------------+---------------+-----------------+------------------+
//	| KeyLen(4B)  | LastKey(...)  | BlockOffset(8B) | BlockLength(4B)  |
//	+-------------+---------------+-----------------+------------------+
//
// BlockLength is the full on-disk size of the block, CRC included -- a
// reader that has read this one index entry can read exactly BlockLength
// bytes starting at BlockOffset and hand the whole thing to
// verifyAndSplitBlock without a second seek.
type indexEntry struct {
	LastKey     []byte
	BlockOffset uint64
	BlockLength uint32
}

func encodeIndexEntry(dst []byte, e indexEntry) []byte {
	dst = appendUint32Prefixed(dst, e.LastKey)
	var offBuf [offsetSize]byte
	binary.LittleEndian.PutUint64(offBuf[:], e.BlockOffset)
	dst = append(dst, offBuf[:]...)
	var lenBuf [lenSize]byte
	binary.LittleEndian.PutUint32(lenBuf[:], e.BlockLength)
	dst = append(dst, lenBuf[:]...)
	return dst
}

// decodeIndex parses every entry out of a fully-read index block. Like
// decodeBlockEntries, it trusts its input: the index block is covered by
// nothing of its own (see the footer doc for why), but it is read in one
// fixed-length slice sized exactly by IndexLength, so a short read is
// already an io error the caller surfaces before this function ever runs,
// and a positive-length slice that isn't a well-formed sequence of index
// entries cannot arise from this package's own writer.
func decodeIndex(raw []byte) ([]indexEntry, error) {
	var entries []indexEntry
	for off := 0; off < len(raw); {
		lastKey, next, err := readUint32Prefixed(raw, off)
		if err != nil {
			return nil, err
		}
		off = next

		if off+offsetSize+lenSize > len(raw) {
			return nil, ErrCorruptBlock
		}
		blockOffset := binary.LittleEndian.Uint64(raw[off : off+offsetSize])
		off += offsetSize
		blockLength := binary.LittleEndian.Uint32(raw[off : off+lenSize])
		off += lenSize

		entries = append(entries, indexEntry{
			LastKey:     lastKey,
			BlockOffset: blockOffset,
			BlockLength: blockLength,
		})
	}
	return entries, nil
}

// A footer, fixed size, at the very end of the file:
//
//	+--------------------+--------------------+-----------+
//	| IndexOffset (8B)    | IndexLength (8B)   | Magic(8B) |
//	+--------------------+--------------------+-----------+
//
// A reader opens the file, seeks to len(file) - footerSize, and finds
// everything else from there without needing a preceding table of
// contents. See format.go's doc on magic for why the last field exists,
// and writer.go for why the footer is necessarily the last thing written
// rather than a header.
type footer struct {
	IndexOffset uint64
	IndexLength uint64
}

func encodeFooter(f footer) []byte {
	buf := make([]byte, 0, footerSize)
	var offBuf [offsetSize]byte
	binary.LittleEndian.PutUint64(offBuf[:], f.IndexOffset)
	buf = append(buf, offBuf[:]...)
	binary.LittleEndian.PutUint64(offBuf[:], f.IndexLength)
	buf = append(buf, offBuf[:]...)
	buf = append(buf, magic[:]...)
	return buf
}

func decodeFooter(raw []byte) (footer, error) {
	if len(raw) != footerSize {
		return footer{}, ErrCorruptBlock
	}
	var m [8]byte
	copy(m[:], raw[footerSize-8:])
	if m != magic {
		return footer{}, ErrNotSSTable
	}
	return footer{
		IndexOffset: binary.LittleEndian.Uint64(raw[0:8]),
		IndexLength: binary.LittleEndian.Uint64(raw[8:16]),
	}, nil
}
