package sstable

import (
	"encoding/binary"
	"hash/crc32"
)

// A data block is a run of sorted entries followed by a checksum over every
// byte that precedes it:
//
//	+------------------------------------------------------------+
//	| entry | entry | ... | entry                                 |
//	+------------------------------------------------------------+
//	| BlockCRC32 (4B)                                              |
//	+------------------------------------------------------------+
//
// A Put entry:
//
//	+---------+-------------+------------+---------------+--------------+
//	| Type(1B)| KeyLen(4B)  | Key(...)   | ValueLen(4B)  | Value(...)   |
//	+---------+-------------+------------+---------------+--------------+
//
// A Delete entry -- no value field at all, not a zero-length one:
//
//	+---------+-------------+------------+
//	| Type(1B)| KeyLen(4B)  | Key(...)   |
//	+---------+-------------+------------+
//
// THE TYPE BYTE IS A DEVIATION FROM THE ORIGINAL PAPER DESIGN (DESIGN.md
// v1.7's §13.2), WHICH HAD NO PER-ENTRY DISCRIMINATOR. That draft mirrored
// only the WAL's Put payload, because the WAL already tells a Put and a
// Delete apart with its own record-level Type field (§13.1) and it was easy
// to assume a data block could get away with the same trick by proxy. It
// cannot: a WAL is a sequence of independently framed records, each with
// its own Type, but a data block's entries share one CRC and one physical
// run -- there is no outer framing left to carry the discriminator once the
// WAL's record boundary is gone. Without a type field, a memtable
// tombstone flushed into an SSTable would be indistinguishable from a
// legitimate empty-string value, which is exactly the read bug §13.3's
// three-outcome Get contract exists to prevent one layer up. Sentinel
// encodings (ValueLen = 0xFFFFFFFF, say) were rejected for the same reason
// the WAL rejected delimiter-scanning for keys and values: a value is
// opaque as far as this format is concerned, and a sentinel is only safe
// until something legitimately produces the bytes it claimed no one would.
// A one-byte, always-present type field costs one byte per entry and rules
// the ambiguity out structurally instead of by convention.
func encodePutEntry(dst []byte, key, value []byte) []byte {
	dst = append(dst, byte(entryPut))
	dst = appendUint32Prefixed(dst, key)
	dst = appendUint32Prefixed(dst, value)
	return dst
}

func encodeDeleteEntry(dst []byte, key []byte) []byte {
	dst = append(dst, byte(entryDelete))
	dst = appendUint32Prefixed(dst, key)
	return dst
}

// appendUint32Prefixed appends b's length as a little-endian uint32
// followed by b itself, the same length-prefix convention record.go uses
// for WAL payloads and for the same reason: the field is opaque, so a
// length prefix is the only delimiter that cannot collide with content.
func appendUint32Prefixed(dst []byte, b []byte) []byte {
	var lenBuf [lenSize]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
	dst = append(dst, lenBuf[:]...)
	dst = append(dst, b...)
	return dst
}

// blockEntry is one decoded data block entry, surfaced while scanning a
// block during a read.
type blockEntry struct {
	Key       []byte
	Value     []byte // nil for a Delete entry
	Tombstone bool
}

// decodeBlockEntries parses every entry out of a block's body (the bytes
// before its trailing CRC, already verified by the caller). It has no
// reason to fail on well-formed input: a block that passed its CRC check
// was written whole by this package's own writer, in this package's own
// framing, so a short or malformed entry inside it is a corruption the CRC
// should already have caught, not a case this parser needs to defend
// against a second time.
func decodeBlockEntries(body []byte) ([]blockEntry, error) {
	var entries []blockEntry
	for off := 0; off < len(body); {
		if off+typeSize > len(body) {
			return nil, ErrCorruptBlock
		}
		t := entryType(body[off])
		off += typeSize

		key, next, err := readUint32Prefixed(body, off)
		if err != nil {
			return nil, err
		}
		off = next

		switch t {
		case entryPut:
			value, next, err := readUint32Prefixed(body, off)
			if err != nil {
				return nil, err
			}
			off = next
			entries = append(entries, blockEntry{Key: key, Value: value})
		case entryDelete:
			entries = append(entries, blockEntry{Key: key, Tombstone: true})
		default:
			return nil, ErrCorruptBlock
		}
	}
	return entries, nil
}

func readUint32Prefixed(body []byte, off int) (field []byte, next int, err error) {
	if off+lenSize > len(body) {
		return nil, 0, ErrCorruptBlock
	}
	n := binary.LittleEndian.Uint32(body[off : off+lenSize])
	off += lenSize
	end := off + int(n)
	if end < off || end > len(body) { // end < off catches uint32 overflow on 32-bit int
		return nil, 0, ErrCorruptBlock
	}
	return body[off:end], end, nil
}

// finalizeBlock appends body's checksum, producing the complete on-disk
// bytes for one data block. The CRC covers every entry byte and nothing
// else -- not a length prefix, because the block's own length is carried
// externally by the index entry that points at it (§13.2's BlockLength),
// the same division of responsibility the WAL record header uses between
// Length and CRC (§13.1).
func finalizeBlock(body []byte) []byte {
	crc := crc32.ChecksumIEEE(body)
	var crcBuf [crcSize]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	return append(body, crcBuf[:]...)
}

// verifyAndSplitBlock checks raw's trailing CRC against its body and, if it
// matches, returns the body alone (with the CRC trimmed off) for
// decodeBlockEntries to parse.
func verifyAndSplitBlock(raw []byte) (body []byte, err error) {
	if len(raw) < crcSize {
		return nil, ErrCorruptBlock
	}
	body = raw[:len(raw)-crcSize]
	wantCRC := binary.LittleEndian.Uint32(raw[len(raw)-crcSize:])
	gotCRC := crc32.ChecksumIEEE(body)
	if gotCRC != wantCRC {
		return nil, ErrCorruptBlock
	}
	return body, nil
}
