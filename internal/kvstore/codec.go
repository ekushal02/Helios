// Package kvstore is the layer apply.go's own doc names but never
// built: "everything below this line is agreement; everything above it
// is the key-value store." internal/raft never imports this package,
// and never will -- Machine is a consumer of the public raft.Node API
// (Submit, ApplyCh, ReadIndex, ReadLease, SnapshotNotify, Snapshot),
// exactly as any other caller of that API would be, with no special
// access. This is Phase H: e2e_test.go's own kvMachine, since v1.0 of
// that file, has carried the comment "a throwaway encoding, deliberately
// ... Phase H brings a real one." This package is that real one, and the
// storage engine (§13) wired in behind it in place of a plain map.
package kvstore

import (
	"encoding/binary"
	"fmt"
)

// opType discriminates a Command the same way entryType discriminates an
// SSTable block entry (sstable/format.go) and RecordType discriminates a
// WAL record (wal/record.go) -- a byte at the front of an opaque,
// length-prefixed payload, the same framing convention every layer below
// this one already uses, applied here at the one remaining boundary that
// still needed it: what a client actually asked for.
type opType byte

const (
	opPut    opType = 1
	opDelete opType = 2
)

// encodePut and encodeDelete build the []byte a client hands to
// raft.Node.Submit. The wire shape --
//
//	Put:    [opType(1B)][keyLen(4B)][key][valueLen(4B)][value]
//	Delete: [opType(1B)][keyLen(4B)][key]
//
// -- is deliberately the same length-prefixed, opaque-payload shape
// record.go's WAL payloads and block.go's entry framing already use, for
// the same reason both of those give: a key or a value is opaque as far
// as this format is concerned, so a length prefix is the only delimiter
// that cannot collide with content. A Delete carries no value field at
// all, not a zero-length one -- matching the same distinction §13.2's
// data block entries draw between "no value" and "empty value," and for
// the identical reason: an empty string is a legitimate value a client
// might actually Put, and conflating it with "this is a delete" would be
// exactly the read bug the WAL and SSTable formats were both built to
// avoid one layer down.
func encodePut(key, value []byte) []byte {
	buf := make([]byte, 0, 1+4+len(key)+4+len(value))
	buf = append(buf, byte(opPut))
	buf = appendLenPrefixed(buf, key)
	buf = appendLenPrefixed(buf, value)
	return buf
}

func encodeDelete(key []byte) []byte {
	buf := make([]byte, 0, 1+4+len(key))
	buf = append(buf, byte(opDelete))
	buf = appendLenPrefixed(buf, key)
	return buf
}

func appendLenPrefixed(dst, b []byte) []byte {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
	dst = append(dst, lenBuf[:]...)
	dst = append(dst, b...)
	return dst
}

// decodedCommand is one applied command, already split into what a
// caller needs to hand to engine.Writer -- either Put(Key, Value) or
// Delete(Key), decided by Tombstone, mirroring blockEntry's own shape in
// sstable/block.go (§13.2) rather than inventing a third way to spell
// "this is a Put or a Delete" in a fourth format this codebase would
// then have to keep consistent with the other three.
type decodedCommand struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

// decodeCommand reverses encodePut/encodeDelete. Every command this
// package's own apply loop ever decodes came from its own encodePut or
// encodeDelete moments earlier in the same process (via Submit), so a
// malformed command reaching here means either a bug in this package or
// a byte-level corruption Raft's own log persistence (§5) failed to
// catch -- both real possibilities worth a clear error, not a panic that
// takes the apply goroutine down with it.
func decodeCommand(cmd []byte) (decodedCommand, error) {
	if len(cmd) < 1 {
		return decodedCommand{}, fmt.Errorf("kvstore: empty command")
	}
	op := opType(cmd[0])
	rest := cmd[1:]

	key, rest, err := readLenPrefixed(rest)
	if err != nil {
		return decodedCommand{}, fmt.Errorf("kvstore: decode key: %w", err)
	}

	switch op {
	case opPut:
		value, _, err := readLenPrefixed(rest)
		if err != nil {
			return decodedCommand{}, fmt.Errorf("kvstore: decode value: %w", err)
		}
		return decodedCommand{Key: key, Value: value}, nil
	case opDelete:
		return decodedCommand{Key: key, Tombstone: true}, nil
	default:
		return decodedCommand{}, fmt.Errorf("kvstore: unknown op type %d", op)
	}
}

func readLenPrefixed(b []byte) (field, rest []byte, err error) {
	if len(b) < 4 {
		return nil, nil, fmt.Errorf("truncated length prefix")
	}
	n := binary.LittleEndian.Uint32(b[:4])
	b = b[4:]
	if uint64(n) > uint64(len(b)) {
		return nil, nil, fmt.Errorf("length prefix %d exceeds remaining %d bytes", n, len(b))
	}
	return b[:n], b[n:], nil
}
