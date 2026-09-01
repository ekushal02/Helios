package kvstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/ekushal02/helios/internal/storage/sstable"
)

// KeyValue is one entry returned by Scan/ScanLeaseRead -- the plain Go
// shape every other Machine read already returns (Get's (value, ok,
// err), the "same shape one layer up" pattern every boundary in this
// project repeats), not the generated protobuf KeyValue Server converts
// this into.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// defaultScanPageSize is UNMEASURED against a real workload -- joining
// every other "asserted, not yet chosen" constant on DESIGN.md §12's
// list. Callers that pass limit <= 0 to Scan/ScanLeaseRead get this
// value; Server.Scan applies the identical default when a ScanRequest's
// own Limit is unset, so the two layers never disagree about what "no
// limit specified" means.
const defaultScanPageSize = 100

// Scan is a linearizable range read (n.ReadIndex, the identical barrier
// Get already uses) over [startKey, endKey), merged across the active
// memtable and every open SSTable reader in the same newest-first order
// buildImage already assembles them in -- the sorted, deduplicated,
// tombstone-free view of live keys Merge (§13.8) was built to produce.
// endKey empty means no upper bound. Returns at most limit pairs;
// nextCursor is empty when the range is exhausted, or the key to pass
// as startKey on the next call to continue -- see scanLocked's own doc
// for the pagination contract that makes this correct given the
// storage engine's iterators have no Seek (§13.2/§13.4's own iterator
// docs: NewIterator always starts before the first entry).
func (m *Machine) Scan(startKey, endKey []byte, limit int) (pairs []KeyValue, nextCursor []byte, err error) {
	if limit <= 0 {
		limit = defaultScanPageSize
	}
	idx, term, isLeader := m.n.ReadIndex()
	if !isLeader {
		return nil, nil, ErrNotLeader
	}
	if !m.waitForApplied(idx, term, defaultReadTimeout) {
		return nil, nil, ErrReadTimedOut
	}
	return m.scanLocked(startKey, endKey, limit)
}

// ScanLeaseRead is Scan via the lease path (n.ReadLease) instead of a
// log barrier -- the identical relationship GetLeaseRead has to Get,
// carrying the same correctness caveat (§9's bounded-clock assumption)
// and the same leaseValid signal for "not usable right now, distinct
// from not leader."
func (m *Machine) ScanLeaseRead(startKey, endKey []byte, limit int) (pairs []KeyValue, nextCursor []byte, leaseValid bool, err error) {
	if limit <= 0 {
		limit = defaultScanPageSize
	}
	idx, until, leaseOK := m.n.ReadLease()
	if !leaseOK {
		return nil, nil, false, nil
	}
	if !time.Now().Before(until) {
		return nil, nil, false, nil
	}
	if !m.waitForApplied(idx, 0, defaultReadTimeout) {
		return nil, nil, true, ErrReadTimedOut
	}
	pairs, nextCursor, err = m.scanLocked(startKey, endKey, limit)
	return pairs, nextCursor, true, err
}

// scanLocked does the actual walk, after the caller's own barrier or
// lease has already confirmed it is safe to read local state.
//
// PAGINATION WITHOUT SEEK -- THE REAL CONSTRAINT THIS DESIGN WORKS
// WITHIN, NOT AROUND. Neither memtable.Iterator nor sstable.Iterator
// exposes a Seek method; both always start "positioned before the
// first entry" (their own docs' exact words) and must be walked from
// there with Next. So nextCursor is not an opaque offset or a resume
// token in the usual sense -- it IS the next call's startKey, used
// exactly as this call's own startKey is: an inclusive lower bound the
// merged walk skips forward to by comparison, not by seeking. A key
// returned in page N is never re-examined in page N+1, because
// nextCursor is set to the FIRST key beyond page N's own limit, which
// that key has not been returned yet and belongs, inclusively, to the
// next page.
//
// THE REAL COST OF THIS, NAMED RATHER THAN HIDDEN: each page still
// walks from the very beginning of the merged key space, skipping
// everything before its own startKey by comparison -- O(n) per page,
// not O(log n + limit). A full scan across P pages costs O(n·P) rather
// than O(n), the identical shape as re-scanning from scratch each time.
// A real fix needs Seek on both iterator types (binary search into the
// SSTable block index directly, and a jump into the memtable's skip
// list via its own search), a change to two packages this task has no
// other reason to touch -- recorded as an open question (DESIGN.md
// §12) rather than attempted here.
func (m *Machine) scanLocked(startKey, endKey []byte, limit int) (pairs []KeyValue, nextCursor []byte, err error) {
	m.mu.Lock()
	// Sources captured under the lock, drained unlocked below -- the
	// identical shape and identical reasoning buildImage already uses:
	// each *sstable.Reader is safe for concurrent use with Get, and a
	// *memtable.Iterator walks a structure only ever appended to or
	// superseded, never mutated in place.
	sources := append([]sstable.Source{m.active.NewIterator()}, sstableSourcesLocked(m.reconcileSSTReadersLocked())...)
	m.mu.Unlock()

	merged := sstable.Merge(sources, true) // dropTombstones: a Scan never surfaces a deleted key
	pairs = make([]KeyValue, 0, limit)

	for merged.Next() {
		key := merged.Key()
		if bytes.Compare(key, startKey) < 0 {
			continue // before this page's own inclusive lower bound
		}
		if len(endKey) > 0 && bytes.Compare(key, endKey) >= 0 {
			break // reached the exclusive upper bound; nothing further is in range
		}
		if len(pairs) >= limit {
			// The page is already full. This key has not been
			// returned -- it becomes the next page's own inclusive
			// startKey, and is not consumed further from merged; the
			// next call's own fresh Merge will re-walk to it, the
			// real O(n)-per-page cost this function's own doc names.
			nextCursor = append([]byte(nil), key...)
			break
		}
		pairs = append(pairs, KeyValue{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), merged.Value()...),
		})
	}
	if err := merged.Err(); err != nil {
		return nil, nil, fmt.Errorf("kvstore: scan: %w", err)
	}
	return pairs, nextCursor, nil
}
