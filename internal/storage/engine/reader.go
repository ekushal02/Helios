// Package engine implements the read path across every tier a key might
// currently live in, tying together the layers §13 built and measured in
// isolation: the memtable (§13.4), SSTables (§13.2), and — not yet, see
// this package's own doc on Reader — the Bloom filter (§13.5).
package engine

import (
	"fmt"

	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// memtableSource is the read-only shape a *memtable.Memtable already
// has, narrowed to just Get. Reader depends on this instead of the
// concrete type so its own tests can drive the merge logic below against
// small hand-built fakes — particularly the tombstone-shadowing and
// error-propagation cases, which are awkward to provoke through a real
// Memtable or a real on-disk SSTable on every test run — on the same
// "narrow interface for a one-way dependency" precedent sstable.Source
// already set in writer.go.
type memtableSource interface {
	Get(key []byte) (value []byte, tombstone bool, ok bool)
}

// sstableSource is the read-only shape a *sstable.Reader already has.
// Unlike memtableSource, its Get can fail — a torn or corrupt block —
// and Reader.Get treats that very differently from an ordinary miss; see
// the doc there.
type sstableSource interface {
	Get(key []byte) (value []byte, tombstone bool, ok bool, err error)
}

// Reader is the read path across one active (still being written to)
// memtable, zero or more immutable memtables (frozen, waiting to be
// flushed), and zero or more SSTables already on disk. §13.4's memtable
// doc and §13.2's SSTable doc each sketch this shape in isolation;
// Reader is where that sketch becomes the actual merge.
//
// A Reader owns none of its sources' lifecycles. It does not decide when
// a memtable becomes immutable, does not trigger a flush, does not track
// which SSTable is newest, and does not discover or persist which files
// exist — all of that is the still-open orchestration question §12
// records under "SSTable file naming and manifest are not designed." A
// Reader is handed its sources already in the right order and its only
// job is the merge: check them in that order and stop at the first one
// with anything to say about a key.
//
// A READER DOES NOT CONSULT A BLOOM FILTER, DELIBERATELY, EVEN THOUGH
// §13.5 BUILT ONE. Wiring the filter into a read is real work — deciding
// where per-SSTable filter bytes live in the file format, loading them in
// Open, and checking one before ever calling an SSTable's Get — none of
// which this task touches. See §12's open question on this; skipping an
// SSTable's Get because the filter says "definitely absent" is the next
// task the filter's own existence sets up, not this one.
type Reader struct {
	active    memtableSource
	immutable []memtableSource // newest-frozen first
	sstables  []sstableSource  // newest-flushed first
}

// NewReader builds a Reader over the given sources. immutable and
// sstables must already be in newest-first order — Reader trusts that
// ordering rather than re-deriving it, the same way §13.2's Write trusts
// (but also checks, via ErrOutOfOrder) that its own input arrives
// sorted rather than re-sorting it. Establishing the order is cheap once,
// upstream, for whoever assembles this list; re-discovering it on every
// read would not be.
//
// active may be nil, meaning "no memtable is currently being written to"
// — a legitimate state (nothing has been written since the engine
// started, say) that Get handles by skipping straight to immutable and
// then to the SSTables, rather than by panicking on a nil dereference.
// Elements of immutable and sstables are assumed non-nil; NewReader does
// not defend against a nil element the way it defends against a nil
// active, because a caller assembling either slice already has to look
// at each element to put it in order, unlike active, which a caller may
// reasonably pass straight from "the field that might not be set yet."
func NewReader(active *memtable.Memtable, immutable []*memtable.Memtable, sstables []*sstable.Reader) *Reader {
	r := &Reader{
		immutable: make([]memtableSource, len(immutable)),
		sstables:  make([]sstableSource, len(sstables)),
	}
	// A nil *memtable.Memtable stored directly into the memtableSource
	// interface field would NOT compare equal to a nil interface --
	// classic Go gotcha -- so the nil check has to happen on the
	// concrete pointer, before it is ever boxed into the interface.
	if active != nil {
		r.active = active
	}
	for i, m := range immutable {
		r.immutable[i] = m
	}
	for i, s := range sstables {
		r.sstables[i] = s
	}
	return r
}

// Get looks up key across every tier, newest to oldest, stopping at the
// first tier that has ANYTHING to say about it — a live value or a
// tombstone both stop the search; only a tier reporting the key as never
// seen lets the search continue to the next, older tier. This is the
// same three-outcome discipline (*memtable.Memtable).Get and
// (*sstable.Reader).Get each already keep, and for the same reason (see
// §13.2, §13.4): collapsing "deleted here" into "not found here" would
// let the search fall through to a stale value still sitting in an older
// tier — the canonical LSM read bug every layer beneath this one was
// already built to avoid. Get is where that discipline finally pays for
// itself: a Reader built over two-outcome sources could not implement
// this method correctly at all.
//
// Get's own return is two-outcome, not three — a caller outside this
// package has no use for the distinction between "never written" and
// "written, then deleted"; both mean the same thing to a client's Get,
// and passing the tombstone bit further out would just relocate the
// same collapsing risk instead of eliminating it.
//
// AN ERROR FROM AN SSTABLE HALTS THE SEARCH; IT DOES NOT FALL THROUGH TO
// AN OLDER SSTABLE. A corrupt or unreadable block in the SSTable Get is
// currently reading might be hiding a tombstone, or a newer value, for
// the key being looked up — there is no way to know without successfully
// reading it — so treating that error as "this tier said nothing" and
// moving on to an older tier could silently return stale data in place
// of the error a caller needs to see. This is the same reasoning that
// makes a torn WAL tail something Recover stops at rather than reads
// past (§13.1), one layer up.
func (r *Reader) Get(key []byte) (value []byte, ok bool, err error) {
	if r.active != nil {
		if value, tombstone, found := r.active.Get(key); found {
			if tombstone {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	for _, m := range r.immutable {
		if m == nil {
			continue
		}
		if value, tombstone, found := m.Get(key); found {
			if tombstone {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	for _, s := range r.sstables {
		if s == nil {
			continue
		}
		value, tombstone, found, err := s.Get(key)
		if err != nil {
			return nil, false, fmt.Errorf("engine: read path: %w", err)
		}
		if found {
			if tombstone {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	return nil, false, nil
}
