// Package wal implements the write-ahead log the LSM storage engine appends
// to before a write lands in the memtable. It is the durability boundary
// for that engine, on the same principle §5 of DESIGN.md applies to Raft's
// own persistent state: nothing above this layer may report success to a
// caller before the bytes proving it are on stable storage.
//
// This is a separate durability island from Raft's persistent log. Raft
// guarantees a command is agreed on and will not be un-committed; the WAL
// guarantees that once the state machine has accepted a command, restarting
// the state machine does not forget it before the next flush to an
// SSTable. The two exist for different failures and neither substitutes
// for the other.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// SyncPolicy controls when Append forces a record to stable storage.
type SyncPolicy int

const (
	// SyncAlways fsyncs after every Append. Every acknowledged write
	// survives a crash; throughput is bounded by one fsync per write.
	// This is the correct default for anything a caller intends to keep.
	SyncAlways SyncPolicy = iota

	// SyncNever flushes to the OS buffer but never fsyncs; durability is
	// whatever the OS's own writeback schedule happens to provide. For
	// tests and throughput measurement only -- a node run this way can
	// lose any write since the last incidental flush.
	SyncNever

	// SyncBatch flushes to the OS buffer on every Append but leaves the
	// fsync itself to an explicit Sync call, so a caller driving many
	// concurrent writers can coalesce them behind one flush the same way
	// persistIfDirty coalesces Raft's own persistent-state writes. Wiring
	// up the ticker or write-batcher that calls Sync is left to the
	// caller; this package only provides the primitive.
	SyncBatch
)

// WAL is an append-only log of Put/Delete records, framed as described in
// record.go.
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	w      *bufio.Writer
	policy SyncPolicy
	offset int64 // next record's file offset

	pendingSync bool // true between a buffered write and its fsync
}

// Open creates or appends to the WAL at path. An existing file is kept
// as-is and only ever appended to -- Open never truncates and never scans
// it, so recovering prior records is a separate, explicit call to Replay.
func Open(path string, policy SyncPolicy) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat %s: %w", path, err)
	}
	return &WAL{
		file:   f,
		w:      bufio.NewWriter(f),
		policy: policy,
		offset: info.Size(),
	}, nil
}

// Append writes a Put record and applies the configured sync policy. It
// returns the byte offset the record starts at, which a caller may keep as
// a recovery watermark -- e.g. "everything before this offset is already
// reflected in a flushed SSTable, so replay can skip it."
func (w *WAL) Append(key, value []byte) (int64, error) {
	return w.appendRecord(RecordPut, encodePutPayload(key, value))
}

// AppendDelete writes a Delete (tombstone) record.
func (w *WAL) AppendDelete(key []byte) (int64, error) {
	return w.appendRecord(RecordDelete, encodeDeletePayload(key))
}

func (w *WAL) appendRecord(t RecordType, payload []byte) (int64, error) {
	buf := encodeRecord(t, payload)

	w.mu.Lock()
	defer w.mu.Unlock()

	off := w.offset
	if _, err := w.w.Write(buf); err != nil {
		return off, fmt.Errorf("wal: write: %w", err)
	}
	w.offset += int64(len(buf))
	w.pendingSync = true

	switch w.policy {
	case SyncAlways:
		if err := w.flushAndSyncLocked(); err != nil {
			return off, err
		}
	case SyncNever, SyncBatch:
		// Flush to the OS buffer so a concurrent reader of the same
		// file sees the bytes; the fsync itself is either skipped
		// entirely (SyncNever) or left for an explicit Sync call
		// (SyncBatch). This is a visibility guarantee, not a
		// durability one.
		if err := w.w.Flush(); err != nil {
			return off, fmt.Errorf("wal: flush: %w", err)
		}
	default:
		return off, fmt.Errorf("wal: unknown sync policy %d", w.policy)
	}

	return off, nil
}

// Sync forces any buffered, unsynced records to stable storage. Under
// SyncAlways every Append has already done this, so Sync is a cheap no-op
// when nothing is pending. Under SyncBatch this is the call a ticker or
// write-batcher makes to flush the accumulated window. Under SyncNever it
// still honors an explicit request -- an explicit call is not the
// background policy the caller opted out of.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushAndSyncLocked()
}

// flushAndSyncLocked must be called with mu held.
func (w *WAL) flushAndSyncLocked() error {
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if !w.pendingSync {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	w.pendingSync = false
	return nil
}

// Close flushes and closes the underlying file. It does not force a sync
// beyond what the configured policy already guarantees; a caller that
// wants a durable close under SyncBatch or SyncNever should call Sync
// first.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: flush on close: %w", err)
	}
	return w.file.Close()
}

// Recover performs the startup sequence for a WAL: replay every
// well-formed record in path, in order, invoking fn for each one, and
// return a WAL opened and ready to accept new appends.
//
// This is the entry point a node's boot path calls, rather than Replay
// directly, because replaying is only half of startup. The other half is
// putting the file into a state new writes can safely land in, and that
// requires truncating away whatever Replay refused to read.
//
// Truncation here is not cosmetic, and skipping it is a real bug, not a
// style choice. Replay leaves a torn or corrupt tail physically in place
// -- correct for a read-only pass, where the bad bytes are inert. But a
// writer that reopened the file in append mode without truncating first
// would place every new record *after* that bad tail rather than in place
// of it. The file would then read, in order: good records, one corrupt or
// torn record, and then the new records written since. The next restart's
// Replay stops at the same old corruption it stopped at before -- because
// nothing about the bytes changed, only what was appended after them --
// and every record written after the "recovery" that never actually
// recovered is silently lost on every subsequent restart, forever. The
// corruption does not just cost the tail it damaged; left untruncated, it
// permanently blinds replay to everything appended after it.
//
// Truncating first closes that gap: the new tail begins exactly where the
// last known-good record ended, so nothing appended post-recovery can ever
// be shadowed by corruption that record already argued past.
//
// Truncation is safe to interrupt. If the process dies between measuring
// validUpTo and finishing the truncate, the next Recover call replays the
// same file and arrives at the same validUpTo -- Replay is a deterministic
// function of the bytes on disk, and truncating to a length the file is
// already at or below is a no-op. There is no window in which a crash
// mid-truncate can lose a record Replay had already reported as valid.
func Recover(path string, policy SyncPolicy, fn func(Entry) error) (*WAL, error) {
	validUpTo, err := Replay(path, fn)
	if err != nil {
		return nil, fmt.Errorf("wal: recover %s: replay: %w", path, err)
	}
	if err := truncateToValid(path, validUpTo); err != nil {
		return nil, fmt.Errorf("wal: recover %s: truncate stale tail: %w", path, err)
	}
	return Open(path, policy)
}

// truncateToValid drops any bytes in path beyond validUpTo -- the torn or
// corrupt tail Replay stopped at, if any. A missing file is not an error:
// there is nothing to truncate, and Open (called by Recover right after)
// creates it fresh. Truncating to a length the file is already at or
// under is deliberately a no-op, which is what makes Recover idempotent
// across a crash between replay and truncation.
func truncateToValid(path string, validUpTo int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("wal: open for truncate: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat before truncate: %w", err)
	}
	if info.Size() <= validUpTo {
		return nil
	}
	if err := f.Truncate(validUpTo); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	// The truncate is a metadata change on an existing file, not a
	// rename over one -- there is no directory entry to flush, only the
	// file's own size. Sync makes that size change durable before this
	// process starts appending past it.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync after truncate: %w", err)
	}
	return nil
}

// Entry is one successfully decoded WAL record, surfaced during Replay.
type Entry struct {
	Offset int64 // file offset the record started at
	Type   RecordType
	Key    []byte
	Value  []byte // nil for RecordDelete
}

// Replay reads path from the beginning and calls fn for every well-formed
// record it finds, in order. It stops -- without returning an error for
// this case -- at the first record that is torn (the file ends mid-record,
// the shape a crash between two writes leaves) or corrupt (the CRC does
// not match bytes that were read in full). A WAL is written by exactly one
// appender in strictly increasing order, so once a record fails to
// validate, everything after it in the file is either never written or
// the residue of a write that was interrupted -- and nothing past that
// point can be trusted regardless of whether it happens to decode cleanly.
//
// Replay returns the offset of the first byte it did not consume. That is
// where a subsequent writer should resume appending; Replay does not
// truncate the file itself, since a torn tail left in place is otherwise
// harmless and Replay already refuses to read past it.
func Replay(path string, fn func(Entry) error) (validUpTo int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("wal: open %s for replay: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var offset int64

	for {
		header := make([]byte, headerSize)
		n, rerr := io.ReadFull(r, header)
		if rerr == io.EOF && n == 0 {
			return offset, nil // clean end of file
		}
		if rerr != nil {
			return offset, nil // partial header: torn tail
		}

		length := binary.LittleEndian.Uint32(header[4:8])
		recordType := RecordType(header[8])

		payload := make([]byte, length)
		if _, rerr := io.ReadFull(r, payload); rerr != nil {
			return offset, nil // header claimed more than the file has: torn tail
		}

		gotCRC := binary.LittleEndian.Uint32(header[0:4])
		wantCRC := crc32.ChecksumIEEE(append([]byte{header[8]}, payload...))
		if gotCRC != wantCRC {
			// A full record's worth of bytes that don't check out
			// is corruption, not truncation, but Replay's job is
			// recovery, not diagnosis, and both stop it the same
			// way. A caller that needs to tell them apart can
			// inspect the file itself past validUpTo.
			return offset, nil
		}

		entry := Entry{Offset: offset, Type: recordType}
		switch recordType {
		case RecordPut:
			key, value, derr := decodePutPayload(payload)
			if derr != nil {
				return offset, nil
			}
			entry.Key, entry.Value = key, value
		case RecordDelete:
			key, derr := decodeDeletePayload(payload)
			if derr != nil {
				return offset, nil
			}
			entry.Key = key
		default:
			return offset, nil // unknown type: cannot be a record this writer produced
		}

		if err := fn(entry); err != nil {
			return offset, err
		}

		offset += int64(headerSize) + int64(length)
	}
}