package sstable

import (
	"encoding/binary"
	"hash/crc32"
)

// A data block is a run of sorted entries, optionally compressed as a
// whole, preceded by a one-byte marker saying which, and followed by a
// checksum over both:
//
//	+-------------------+------------------------------------------+----------------+
//	| CompressionType(1B)| entry | entry | ... | entry  (raw or compressed) | BlockCRC32(4B) |
//	+-------------------+------------------------------------------+----------------+
//
// See CompressionType's own doc (compress.go) for the two values this
// byte can take and why compression, when used, is flate rather than a
// self-checksumming container format like gzip. THE COMPRESSION-TYPE
// BYTE WAS ADDED AFTER THE TYPE BYTE BELOW, NOT AT THE SAME TIME --
// this block-level byte says how the WHOLE block's bytes are stored;
// the per-entry Type byte below says whether one entry is a Put or a
// Delete once the block's bytes are already in their true, decompressed
// form. The two solve different problems at different layers and are
// not interchangeable, despite both being one-byte discriminators.
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
// legitimate empty-string value, which is exactly the read bug §13.4's
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

// finalizeBlock compresses body (if compression is not CompressionNone
// and actually helps -- see below), prepends a one-byte CompressionType
// marker, and appends a checksum, producing the complete on-disk bytes
// for one data block. The CRC covers the type byte and the payload that
// follows it, whichever form that payload is in -- not a length prefix,
// because the block's own length is carried externally by the index
// entry that points at it (§13.2's BlockLength), the same division of
// responsibility the WAL record header uses between Length and CRC
// (§13.1).
//
// THE CRC COVERS THE ON-DISK (POSSIBLY COMPRESSED) BYTES, NOT THE
// ORIGINAL ENTRY BYTES -- CHECKED BEFORE DECOMPRESSION IS EVER
// ATTEMPTED, NOT AFTER. Corruption happens to whatever bytes are
// actually on the physical medium, which are the compressed ones once
// compression is in play; verifying those first, and only decompressing
// once they're known-good, means this package never hands an
// unverified byte stream to a decompressor. A decompressor asked to
// make sense of arbitrary corrupted input is exactly the kind of
// component a format like this should never trust blindly -- the same
// reasoning maxDecompressedBlockSize's own doc (compress.go) applies to
// bounding what a verified-but-still-adversarial stream could claim to
// expand to.
//
// COMPRESSION IS SKIPPED, EVEN IF REQUESTED, WHEN IT DOESN'T ACTUALLY
// SAVE SPACE. A block of already-compressed or near-random data can
// come out of flate LARGER than it went in (a few bytes of framing
// overhead with nothing to compress away), and decompression has a real
// CPU cost a reader would pay on every future Get for no benefit at
// all in that case. finalizeBlock compresses first, compares sizes, and
// only keeps the compressed form -- falling back to CompressionNone --
// if it is strictly smaller.
func finalizeBlock(body []byte, compression CompressionType) ([]byte, error) {
	payload := body
	actual := CompressionNone

	if compression == CompressionFlate {
		compressed, err := compressFlate(body)
		if err != nil {
			return nil, err
		}
		if len(compressed) < len(body) {
			payload = compressed
			actual = CompressionFlate
		}
	}

	out := make([]byte, 0, typeSize+len(payload)+crcSize)
	out = append(out, byte(actual))
	out = append(out, payload...)

	crc := crc32.ChecksumIEEE(out)
	var crcBuf [crcSize]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	out = append(out, crcBuf[:]...)
	return out, nil
}

// verifyAndSplitBlock checks raw's trailing CRC against everything ahead
// of it (the compression-type byte and the payload together), and if it
// matches, returns the ORIGINAL entry bytes for decodeBlockEntries to
// parse -- transparently decompressing first if the type byte says the
// payload isn't stored raw. Every caller (Reader.loadBlock, Iterator.Next)
// is unaffected by this: a block's compression is a per-block, on-disk
// implementation detail this function fully absorbs, never something a
// reader needs to know or ask about.
func verifyAndSplitBlock(raw []byte) (body []byte, err error) {
	if len(raw) < typeSize+crcSize {
		return nil, ErrCorruptBlock
	}
	withoutCRC := raw[:len(raw)-crcSize]
	wantCRC := binary.LittleEndian.Uint32(raw[len(raw)-crcSize:])
	gotCRC := crc32.ChecksumIEEE(withoutCRC)
	if gotCRC != wantCRC {
		return nil, ErrCorruptBlock
	}

	compression := CompressionType(withoutCRC[0])
	payload := withoutCRC[typeSize:]

	switch compression {
	case CompressionNone:
		return payload, nil
	case CompressionFlate:
		return decompressFlate(payload)
	default:
		return nil, ErrCorruptBlock
	}
}
