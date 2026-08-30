package kvstore

import (
	"encoding/binary"
	"fmt"

	"github.com/ekushal02/helios/internal/storage/sstable"
)

// A snapshot image is the entire live key set, at the applied index it
// was taken at, as a flat sequence of length-prefixed key/value pairs --
// the same length-prefixed, opaque-payload convention codec.go's Command
// encoding and every layer below it already use.
//
//	[AppliedIndex(8B)][NumEntries(8B)][keyLen(4B)][key][valueLen(4B)][value]...
//
// A DELIBERATELY "LOGICAL" SNAPSHOT, NOT A "PHYSICAL" ONE -- THE
// SIMPLER OF TWO REAL DESIGNS, CHOSEN EXPLICITLY, NOT BY DEFAULT. A real
// production LSM engine typically checkpoints by referencing its own
// on-disk SSTable files directly (RocksDB's checkpoint mechanism is
// exactly this: hard-link the current manifest's files rather than
// re-serialize their contents), which is far cheaper for a large,
// mostly-immutable dataset. That approach needs InstallSnapshot's own
// RPC to carry file references or chunked file bytes rather than one
// opaque blob, which this project's Raft layer does not do yet --
// "chunking InstallSnapshot" is already an open Raft question this
// engine has carried since before this package existed. Building that
// now would mean redesigning the snapshot RPC as part of wiring in a
// state machine, two large changes at once. The logical form built here
// works within Raft's existing, unmodified contract -- Snapshot(index,
// data []byte), one blob -- iterating the current live key set (via
// sstable.Merge over the active memtable plus every open SSTable reader,
// with tombstones DROPPED rather than carried: a snapshot is the full
// ground truth as of its index, so a deleted key is correctly just
// absent, never present-as-a-tombstone) and encoding it flat. Real,
// deferred work, recorded as an open question rather than solved here.
func encodeSnapshotImage(appliedIndex int, merged sstable.Source) ([]byte, error) {
	var numEntries uint64
	var body []byte

	for merged.Next() {
		if merged.Tombstone() {
			// Merge is asked to drop tombstones (dropTombstones=true, in
			// the caller); this branch should be unreachable, and is
			// guarded rather than assumed for the same "believed
			// impossible is checked, not trusted" reason §8 gives for
			// Raft's own invariants.
			return nil, fmt.Errorf("kvstore: encode snapshot: merged source yielded a tombstone despite dropTombstones")
		}
		body = appendLenPrefixed(body, merged.Key())
		body = appendLenPrefixed(body, merged.Value())
		numEntries++
	}
	if err := merged.Err(); err != nil {
		return nil, fmt.Errorf("kvstore: encode snapshot: %w", err)
	}

	buf := make([]byte, 0, 16+len(body))
	var hdr [16]byte
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(appliedIndex))
	binary.LittleEndian.PutUint64(hdr[8:16], numEntries)
	buf = append(buf, hdr[:]...)
	buf = append(buf, body...)
	return buf, nil
}

// decodeSnapshotImage reverses encodeSnapshotImage, calling put(key,
// value) once per entry in the image, in the order they were encoded.
// It does not itself decide how those Puts become durable -- the caller
// (Machine.installSnapshot) supplies put, backed by a fresh
// engine.Writer, so replaying an image goes through the exact same
// WAL-then-memtable path an ordinary client write does (§13.7's whole
// delete-is-a-write argument, applied here to "install-is-a-write" for
// the same underlying reason: nothing about how the data arrived changes
// what durability it needs).
func decodeSnapshotImage(data []byte, put func(key, value []byte) error) (appliedIndex int, err error) {
	if len(data) < 16 {
		return 0, fmt.Errorf("kvstore: decode snapshot: %d bytes, want at least 16 for the header", len(data))
	}
	idx := binary.LittleEndian.Uint64(data[0:8])
	numEntries := binary.LittleEndian.Uint64(data[8:16])
	rest := data[16:]

	for i := uint64(0); i < numEntries; i++ {
		key, next, err := readLenPrefixed(rest)
		if err != nil {
			return 0, fmt.Errorf("kvstore: decode snapshot: entry %d key: %w", i, err)
		}
		rest = next
		value, next, err := readLenPrefixed(rest)
		if err != nil {
			return 0, fmt.Errorf("kvstore: decode snapshot: entry %d value: %w", i, err)
		}
		rest = next

		if err := put(key, value); err != nil {
			return 0, fmt.Errorf("kvstore: decode snapshot: entry %d: %w", i, err)
		}
	}
	if len(rest) != 0 {
		return 0, fmt.Errorf("kvstore: decode snapshot: %d trailing bytes after %d entries", len(rest), numEntries)
	}
	return int(idx), nil
}
