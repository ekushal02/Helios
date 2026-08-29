package engine

import (
	"fmt"

	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/wal"
)

// Writer is the write path: the sequence every client write crosses
// before Reader (§13.6), or a fresh RecoverMemtable, can ever see it.
// Where Reader merges several read-only sources, Writer owns exactly the
// two a live write actually touches -- one *wal.WAL and one
// *memtable.Memtable -- and does exactly one thing with them, twice:
// append the record, then apply it, in that order, every time.
//
// # Why the order is WAL first, memtable second, never the reverse
//
// The WAL's own package doc states the durability boundary plainly:
// "nothing above this layer may report success to a caller before the
// bytes proving it are on stable storage." Put and Delete both honor
// that literally -- the memtable is touched only after Append or
// AppendDelete has already returned successfully, so a crash at any
// point before that append lands has cost the caller an error return
// and nothing else; the record was never made durable, so nothing was
// silently lost that a caller believed had succeeded. A crash AFTER the
// append but before the memtable update is exactly what RecoverMemtable
// exists to repair on the next startup -- see its own doc, and
// TestDeleteSurvivesACrashBetweenAppendAndApply, which provokes this
// window directly rather than only arguing it.
//
// # Why Delete is not a special case of this type
//
// Put and Delete are the same two-step sequence, differing only in
// which WAL method and which Memtable method get called -- there is no
// third step, no different ordering, and no branch that treats a delete
// as anything other than an ordinary write whose payload happens to
// mean "absent" instead of carrying a value. See DESIGN.md §13.7 for the
// fuller argument of why deletion in an LSM engine has to work this way
// -- in short, because an older, already-durable copy of this key may
// exist in a tier this Writer cannot see or touch (an immutable
// memtable, or an SSTable already on disk), so nothing can be
// un-written; only a new, more-recent fact can be recorded that shadows
// the old one when Reader walks the tiers newest-first. A Delete that
// quietly returned without appending anything -- reasoning "the
// memtable doesn't currently hold this key, so there is nothing to
// remove" -- would be exactly the bug this design forecloses: it has no
// way of knowing whether an older tier holds it, and skipping the
// append is indistinguishable, on crash-recovery, from the delete
// never having been requested at all.
type Writer struct {
	wal *wal.WAL
	mem *memtable.Memtable
}

// NewWriter pairs an already-open WAL with the memtable it durably backs.
// Neither is validated against the other -- a caller that pairs a WAL
// and a Memtable that did not come from the same RecoverMemtable call
// (or the same fresh pair at startup) gets whatever inconsistency that
// mismatch implies; enforcing the pairing would need state neither type
// currently carries about its own provenance, and nothing in this
// package's own use of Writer ever constructs one any other way.
func NewWriter(w *wal.WAL, m *memtable.Memtable) *Writer {
	return &Writer{wal: w, mem: m}
}

// Put durably records key=value, then makes it visible to Get. See the
// type doc for why the WAL append happens first.
func (w *Writer) Put(key, value []byte) error {
	if _, err := w.wal.Append(key, value); err != nil {
		return fmt.Errorf("engine: write path: put: %w", err)
	}
	w.mem.Put(key, value)
	return nil
}

// Delete durably records key as deleted, then makes that tombstone
// visible to Get -- the identical two-step sequence Put follows, with
// AppendDelete and Memtable.Delete standing in for Append and
// Memtable.Put. See the type doc's second section for why this counts
// as a write rather than some other kind of operation, and DESIGN.md
// §13.7 for the argument in full.
func (w *Writer) Delete(key []byte) error {
	if _, err := w.wal.AppendDelete(key); err != nil {
		return fmt.Errorf("engine: write path: delete: %w", err)
	}
	w.mem.Delete(key)
	return nil
}

// RecoverMemtable rebuilds m from the WAL at path, replaying every
// well-formed record into it via ApplyPut/ApplyDelete, and returns the
// *wal.WAL positioned to keep accepting new Appends after it -- exactly
// what a node's startup path needs to hand to NewWriter.
//
// THIS FUNCTION IS THE FIX FOR A DOCUMENTATION BUG THAT PREDATES THIS
// TASK. memtable.go's own doc comment, since v1.8, has described "a
// node's startup path... calling wal.RecoverAndOpen(path, policy,
// memtable)" as though that function already existed, and as though
// Memtable's ApplyPut/ApplyDelete satisfied a wal.Sink interface. Neither
// was true: package wal has never defined a Sink interface, has no
// RecoverAndOpen function, and wal.Recover takes a plain
// func(wal.Entry) error callback, not a Memtable or any interface at
// all. The wiring was correctly designed in prose and never actually
// built. RecoverMemtable is that wiring, finally written down as code:
// it is the func(wal.Entry) error closure the doc always implied,
// living here in package engine rather than in wal or memtable, because
// engine is the one package in this codebase allowed to depend on both
// (see reader.go's doc on the same one-way-dependency discipline
// memtable not importing wal already established, and sstable not
// importing raft before that).
func RecoverMemtable(path string, policy wal.SyncPolicy, m *memtable.Memtable) (*wal.WAL, error) {
	w, err := wal.Recover(path, policy, func(e wal.Entry) error {
		switch e.Type {
		case wal.RecordPut:
			m.ApplyPut(e.Key, e.Value)
		case wal.RecordDelete:
			m.ApplyDelete(e.Key)
		default:
			return fmt.Errorf("engine: recover %s: unknown record type %v at offset %d", path, e.Type, e.Offset)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("engine: recover memtable: %w", err)
	}
	return w, nil
}
