package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// RecordType identifies what a WAL record represents.
type RecordType uint8

const (
	// RecordPut records a key/value write.
	RecordPut RecordType = 0
	// RecordDelete records a tombstone for a key.
	RecordDelete RecordType = 1
)

// Record framing, written immediately before the payload:
//
//	+-----------+-------------+-----------+------------------------+
//	| CRC32(4B) | Length(4B)  | Type(1B)  | Payload (Length bytes) |
//	+-----------+-------------+-----------+------------------------+
//
// CRC32 covers Type followed by Payload -- not Length, and not itself --
// so a reader can tell a corrupted length prefix from corrupted data: a
// flipped bit in Length changes how many payload bytes get read, and
// whatever lands in Type+Payload as a result will not match the CRC
// either way. There is nothing to gain from also covering Length; a
// wrong Length either produces a CRC mismatch or a short read, and both
// are handled identically (see Replay).
//
// Length is the payload length alone, not payload+type, so a reader can
// size its allocation before it has looked at Type.
//
// This is deliberately simpler than a block-fragmented WAL (LevelDB's
// 32KB-block, FULL/FIRST/MIDDLE/LAST scheme): fragmentation exists to
// bound how much a reader buffers per block and to let a corrupt block
// be skipped without losing the whole file. Helios's records are
// single key/value pairs, not the several-hundred-KB blocks the scheme
// was built for, and a corrupt record here already ends replay at that
// point rather than skipping past it (see Replay) -- fragmentation
// would add a reassembly state machine to buy nothing at this size.
// Revisit if a record ever needs to hold something larger than a
// reasonable buffer.
const headerSize = 4 + 4 + 1 // crc + length + type

var (
	// ErrCorruptRecord means a record's bytes were read in full but its
	// CRC did not match. A WAL is read in strict order with no random
	// access, so a broken record poisons everything after it, never
	// anything before it.
	ErrCorruptRecord = errors.New("wal: corrupt record")
)

// encodeRecord serializes a record type and payload into the on-disk
// framing described above.
func encodeRecord(t RecordType, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	buf[8] = byte(t)
	copy(buf[headerSize:], payload)

	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))

	crc := crc32.ChecksumIEEE(buf[8 : headerSize+len(payload)])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	return buf
}

// encodePutPayload lays out a Put record's payload as:
//
//	+-------------+------------+---------------+--------------+
//	| KeyLen(4B)  | Key(...)   | ValueLen(4B)  | Value(...)   |
//	+-------------+------------+---------------+--------------+
//
// Both lengths are explicit rather than relying on a separator byte,
// because a key or value is an opaque byte string as far as the WAL is
// concerned and must never be scanned for a delimiter that could
// legitimately appear inside it.
func encodePutPayload(key, value []byte) []byte {
	buf := make([]byte, 4+len(key)+4+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:], key)
	off := 4 + len(key)
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(value)))
	copy(buf[off+4:], value)
	return buf
}

// encodeDeletePayload lays out a Delete record's payload as:
//
//	+-------------+------------+
//	| KeyLen(4B)  | Key(...)   |
//	+-------------+------------+
func encodeDeletePayload(key []byte) []byte {
	buf := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:], key)
	return buf
}

func decodePutPayload(payload []byte) (key, value []byte, err error) {
	if len(payload) < 4 {
		return nil, nil, ErrCorruptRecord
	}
	keyLen := binary.LittleEndian.Uint32(payload[0:4])
	if uint32(len(payload)) < 4+keyLen+4 {
		return nil, nil, ErrCorruptRecord
	}
	key = payload[4 : 4+keyLen]
	off := 4 + keyLen
	valLen := binary.LittleEndian.Uint32(payload[off : off+4])
	if uint32(len(payload)) < off+4+valLen {
		return nil, nil, ErrCorruptRecord
	}
	value = payload[off+4 : off+4+valLen]
	return key, value, nil
}

func decodeDeletePayload(payload []byte) (key []byte, err error) {
	if len(payload) < 4 {
		return nil, ErrCorruptRecord
	}
	keyLen := binary.LittleEndian.Uint32(payload[0:4])
	if uint32(len(payload)) < 4+keyLen {
		return nil, ErrCorruptRecord
	}
	return payload[4 : 4+keyLen], nil
}
