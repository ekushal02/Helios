package kvstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/storage/engine"
	"github.com/ekushal02/helios/internal/storage/manifest"
	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// ErrNotLeader means this node is not currently the leader, and a caller
// must find the real one and try again (raft.Node.Submit's own F-2
// contract, surfaced here for Get the same way it already is for Put and
// Delete).
var ErrNotLeader = errors.New("kvstore: not leader")

// ErrReadTimedOut means a read's own barrier never applied within the
// given deadline -- either this node was deposed mid-read (see
// waitForApplied's own doc) or the state machine is falling behind the
// committed log for some other reason. Distinguishable from ErrNotLeader
// (know immediately, no network round trip) precisely because it can
// only be discovered after paying for the round trip.
var ErrReadTimedOut = errors.New("kvstore: read barrier did not apply in time")

// defaultReadTimeout bounds how long Get waits for its own barrier to
// apply before giving up -- generous enough for an ordinary commit round
// trip, short enough that a caller is not left blocked indefinitely
// behind a partitioned or crashed node. Not yet measured against a real
// workload, the same "asserted, not chosen" status every other constant
// on §12's open-questions list carries.
const defaultReadTimeout = 5 * time.Second

// Put durably records key=value through n.Submit, waits for it to apply,
// and reports whether the write actually landed as this caller's own
// submission (see raft.Node.Submit's own doc on the claim-ticket
// contract) rather than being silently overwritten by a later leader.
func (m *Machine) Put(key, value []byte) error {
	return m.write(encodePut(key, value))
}

// Delete durably records key as deleted, the identical write path Put
// takes -- see DESIGN.md §13.7 for the whole argument on why a delete is
// a write, not some other kind of operation, all the way up to this,
// its client-facing surface.
func (m *Machine) Delete(key []byte) error {
	return m.write(encodeDelete(key))
}

func (m *Machine) write(cmd []byte) error {
	idx, term, isLeader := m.n.Submit(cmd)
	if !isLeader {
		return ErrNotLeader
	}
	if !m.waitForApplied(idx, term, defaultReadTimeout) {
		return ErrReadTimedOut
	}
	return nil
}

// Get is a linearizable read via the safe path: a log barrier
// (n.ReadIndex, read.go's own full protocol description), waited for,
// then a local read once the barrier's own term is confirmed still
// current. See GetLeaseRead for the cheaper, clock-dependent
// alternative.
func (m *Machine) Get(key []byte) (value []byte, ok bool, err error) {
	idx, term, isLeader := m.n.ReadIndex()
	if !isLeader {
		return nil, false, ErrNotLeader
	}
	if !m.waitForApplied(idx, term, defaultReadTimeout) {
		return nil, false, ErrReadTimedOut
	}
	return m.readLocked(key)
}

// GetLeaseRead is Get via the lease path (n.ReadLease) instead of a log
// barrier: no round trip, no log write, correct only under the
// bounded-clock-drift assumption DESIGN.md §9 states in full. leaseValid
// reports whether the lease was actually usable right now (a follower,
// or a leader whose lease has expired, cannot serve this way at all,
// which is different from "not leader" and different from "read timed
// out" -- distinguishing it is the whole reason this method exists
// alongside Get rather than folding lease into it silently).
func (m *Machine) GetLeaseRead(key []byte) (value []byte, ok bool, leaseValid bool, err error) {
	idx, until, leaseOK := m.n.ReadLease()
	if !leaseOK {
		return nil, false, false, nil
	}
	if !time.Now().Before(until) {
		return nil, false, false, nil
	}
	// term == 0: correctness on the lease path comes from ReadLease's own
	// gates (leader, current-term commit, unexpired lease), not a term
	// comparison the way the barrier path uses one -- see
	// waitForApplied's own doc for why 0 is safe as a "skip the term
	// check" sentinel.
	if !m.waitForApplied(idx, 0, defaultReadTimeout) {
		return nil, false, true, ErrReadTimedOut
	}
	value, ok, err = m.readLocked(key)
	return value, ok, true, err
}

func (m *Machine) readLocked(key []byte) ([]byte, bool, error) {
	m.mu.Lock()
	active := m.active
	sstReaders := m.reconcileSSTReadersLocked()
	m.mu.Unlock()

	r := engine.NewReader(active, nil, sstReaders)
	value, ok, err := r.Get(key)
	if err != nil {
		return nil, false, fmt.Errorf("kvstore: get: %w", err)
	}
	return value, ok, nil
}

// reconcileSSTReadersLocked brings m.sstReaders into agreement with the
// manifest currently on disk, then returns it in the manifest's own
// newest-first order (§13.6, §13.8) -- opening a reader for any file the
// manifest now names that this Machine has not seen before, and
// untracking (see the doc further down on why not also closing) any
// reader for a file the manifest no longer names. Caller must hold m.mu.
//
// A REAL BUG, FOUND AND FIXED BEFORE THIS TASK SHIPPED, NOT AFTER: THE
// FIRST VERSION OF THIS FUNCTION ONLY READ m.sstReaders; IT NEVER
// UPDATED IT. That was correct for files freezeAndFlushLocked itself
// added, since that function updates m.sstReaders directly, but silently
// wrong for files compaction.Background (§13.9) produces or deletes --
// Background runs on its own goroutine, entirely independent of this
// Machine's own apply loop, and the first version had no path by which a
// compaction's output file ever got opened here at all. The failure mode
// was not a crash or an error: the old code looked up each manifest
// entry in m.sstReaders and silently skipped any name it didn't
// recognize, so once Background replaced an L0 backlog with one merged
// L1 file, every key that had ever lived in that backlog started
// returning not-found -- a correct-looking, silently wrong answer,
// caught only because TestBackgroundCompactionDrainsL0AfterEnoughFlushes
// checks Get after compaction runs, not merely that compaction ran at
// all. See DESIGN.md §13.14's note on this for the fuller account of why
// "the compaction happened" and "the data is still readable" are two
// different claims that both needed a test.
func (m *Machine) reconcileSSTReadersLocked() []*sstable.Reader {
	manifestPath := filepath.Join(m.dir, manifestName)
	mf, err := manifest.Load(manifestPath)
	if err != nil {
		// A load failure here means the manifest this Machine's own
		// writes maintain is unreadable -- serve from whatever is
		// currently open rather than fail every read outright; the next
		// successful load recovers full reconciliation.
		out := make([]*sstable.Reader, 0, len(m.sstReaders))
		for _, r := range m.sstReaders {
			out = append(out, r)
		}
		return out
	}

	referenced := make(map[string]bool)
	var out []*sstable.Reader
	for _, level := range mf.Levels {
		for _, name := range level {
			referenced[name] = true
			r, alreadyOpen := m.sstReaders[name]
			if !alreadyOpen {
				opened, err := sstable.OpenWithCache(filepath.Join(m.dir, name), m.cache)
				if err != nil {
					// The manifest names a file this reconciliation
					// cannot open right now -- most plausibly a race with
					// Background mid-write of a NEW file that has not
					// finished its own atomic publish yet (§13.2's
					// temp-file-then-rename sequence means a reader
					// attempting to open it too early sees ErrNotSSTable,
					// not a torn file). Skip it for this call; the next
					// reconciliation, on the next read, tries again.
					continue
				}
				m.sstReaders[name] = opened
				r = opened
			}
			out = append(out, r)
		}
	}

	// Superseded readers are untracked here, NOT closed. A reader
	// captured by an earlier, still-in-flight readLocked or buildImage
	// call (both intentionally drain their sources unlocked, see those
	// functions' own docs) could be mid-Get on the exact *sstable.Reader
	// this loop is about to stop tracking; closing it here would hand
	// that in-flight call a read on a closed file descriptor. Untracking
	// without closing is safe because of the same property §13.10's
	// Recover already leans on: an already-open file descriptor keeps
	// working after its file is unlinked on disk (ordinary POSIX
	// semantics), so an in-flight read finishes correctly against
	// whatever it already had open, and simply never gets picked up by a
	// future read once this loop stops returning it. THE COST IS REAL
	// AND UNPAID HERE: these file descriptors are never explicitly
	// closed, which leaks one per superseded SSTable over a long-running
	// node's lifetime -- bounded by how many compactions run, not by
	// read volume, but a real resource leak nonetheless, recorded as an
	// open question rather than solved with the reference-counting a
	// correct close would need.
	for name := range m.sstReaders {
		if !referenced[name] {
			delete(m.sstReaders, name)
		}
	}

	return out
}

// waitForApplied blocks until appliedIndex reaches idx, then reports
// whether the term recorded at idx matches term -- false means this node
// was deposed between issuing the read (or write) and the barrier
// landing, and the caller must not trust local state that follows.
// term == 0 skips the term check entirely, for GetLeaseRead's use, whose
// own correctness comes from ReadLease's gates rather than a term
// comparison -- 0 is never a real term (Raft's own terms start at 1, the
// same convention DESIGN.md §5 states), so it cannot collide with one.
func (m *Machine) waitForApplied(idx, term int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		if m.appliedIndex >= idx {
			if term == 0 {
				m.mu.Unlock()
				return true
			}
			gotTerm, known := m.appliedTerms[idx]
			m.mu.Unlock()
			return known && gotTerm == term
		}
		m.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	return false
}

// Fault returns the first internal anomaly this Machine has recorded --
// a malformed command, a storage-engine error mid-apply, a snapshot that
// failed to encode or decode -- or "" if none has occurred. Mirrors
// kvMachine's own fault field (e2e_test.go) and snapshotMachine's
// (installsnapshot_test.go), promoted here from a test-only convenience
// to a real health signal a production caller can poll.
func (m *Machine) Fault() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fault
}

// AppliedIndex reports the highest index this Machine has applied.
func (m *Machine) AppliedIndex() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appliedIndex
}

// -----------------------------------------------------------------------
// Snapshots
// -----------------------------------------------------------------------

// take answers m.n.SnapshotNotify(): build an image of the current live
// key set and hand it to m.n.Snapshot. See snapshot.go's own doc on
// encodeSnapshotImage for why this is a "logical" snapshot (the full
// live key set, tombstones dropped) rather than a "physical" one
// referencing SSTable files directly.
func (m *Machine) take() {
	blob, appliedIndex, err := m.buildImage()
	if err != nil {
		if blob == nil && appliedIndex == 0 && err == errNothingAppliedYet {
			return // matches snapshotMachine's own guard: nothing to snapshot yet
		}
		m.mu.Lock()
		if m.fault == "" {
			m.fault = fmt.Sprintf("take snapshot: %v", err)
		}
		m.mu.Unlock()
		return
	}

	// Never called while holding m.mu -- Snapshot takes n.mu, and the
	// applier may be blocked trying to hand this Machine an entry; the
	// identical deadlock snapshotMachine's own take() (installsnapshot_test.go)
	// already documents avoiding.
	if err := m.n.Snapshot(appliedIndex, blob); err != nil {
		m.mu.Lock()
		if m.fault == "" {
			m.fault = fmt.Sprintf("Snapshot(%d) refused: %v", appliedIndex, err)
		}
		m.mu.Unlock()
	}
}

var errNothingAppliedYet = errors.New("kvstore: nothing applied yet")

// buildImage does the encoding half of take(), factored out so a test
// can obtain exactly the bytes take() would have handed to n.Snapshot
// without needing a real SnapshotNotify cycle to fire first.
func (m *Machine) buildImage() (blob []byte, appliedIndex int, err error) {
	m.mu.Lock()
	if m.appliedIndex == 0 {
		m.mu.Unlock()
		return nil, 0, errNothingAppliedYet
	}
	appliedIndex = m.appliedIndex
	sources := append([]sstable.Source{m.active.NewIterator()}, sstableSourcesLocked(m.reconcileSSTReadersLocked())...)
	m.mu.Unlock()

	// Encoded OUTSIDE the lock, deliberately: this can read every live
	// key in the engine, and holding m.mu for that whole duration would
	// block every apply and every read for as long as it takes. The
	// sources captured above are safe to drain unlocked -- each
	// *sstable.Reader is independently safe for concurrent use with Get
	// (§13.2's own doc), and *memtable.Iterator walks a structure that is
	// never mutated in place, only ever appended to or superseded
	// (§13.4).
	merged := sstable.Merge(sources, true) // dropTombstones: a snapshot is ground truth
	blob, err = encodeSnapshotImage(appliedIndex, merged)
	if err != nil {
		return nil, 0, err
	}
	return blob, appliedIndex, nil
}

func sstableSourcesLocked(readers []*sstable.Reader) []sstable.Source {
	out := make([]sstable.Source, len(readers))
	for i, r := range readers {
		out[i] = r.NewIterator()
	}
	return out
}

// installSnapshot answers a SnapshotValid ApplyMsg: RULE 8 on the
// consumer side (installsnapshot_test.go's own phrase for it) -- the
// image REPLACES this Machine's entire on-disk state, never merges into
// it, because Raft only ever sends an image covering entries this node
// has not already reached.
//
// Replacement is done by wiping this Machine's own tracked files (every
// SSTable it opened, its manifest, its active WAL -- never a blanket
// directory removal, so a caller's own unrelated files sharing the same
// disk are never at risk) and replaying the image through a fresh
// Writer's ordinary Put path, exactly the WAL-then-memtable durability
// sequence a live client write takes (§13.7). This is what lets
// installation reuse everything already built rather than inventing a
// second, snapshot-only way to make data durable.
func (m *Machine) installSnapshot(msg raft.ApplyMsg) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.activeWAL.Close(); err != nil {
		m.fault = fmt.Sprintf("install snapshot at %d: close active WAL: %v", msg.SnapshotIndex, err)
		return
	}
	activeWALPath := filepath.Join(m.dir, activeWALName)
	if err := os.Remove(activeWALPath); err != nil && !os.IsNotExist(err) {
		m.fault = fmt.Sprintf("install snapshot at %d: remove active WAL: %v", msg.SnapshotIndex, err)
		return
	}
	for name, r := range m.sstReaders {
		r.Close()
		if err := os.Remove(filepath.Join(m.dir, name)); err != nil && !os.IsNotExist(err) {
			m.fault = fmt.Sprintf("install snapshot at %d: remove %s: %v", msg.SnapshotIndex, name, err)
			return
		}
	}
	m.sstReaders = make(map[string]*sstable.Reader)

	manifestPath := filepath.Join(m.dir, manifestName)
	if err := manifest.Save(manifestPath, manifest.New()); err != nil {
		m.fault = fmt.Sprintf("install snapshot at %d: reset manifest: %v", msg.SnapshotIndex, err)
		return
	}

	fresh := memtable.NewWithSeed(time.Now().UnixNano())
	freshWAL, err := engine.RecoverMemtable(activeWALPath, m.opts.WALSyncPolicy, fresh)
	if err != nil {
		m.fault = fmt.Sprintf("install snapshot at %d: start fresh memtable: %v", msg.SnapshotIndex, err)
		return
	}
	freshWriter := engine.NewWriter(freshWAL, fresh)

	imageIndex, err := decodeSnapshotImage(msg.Snapshot, freshWriter.Put)
	if err != nil {
		m.fault = fmt.Sprintf("install snapshot at %d: decode: %v", msg.SnapshotIndex, err)
		freshWAL.Close()
		return
	}
	if imageIndex != msg.SnapshotIndex {
		m.fault = fmt.Sprintf("install snapshot: image says it covers index %d, Raft says %d", imageIndex, msg.SnapshotIndex)
		freshWAL.Close()
		return
	}

	m.active = fresh
	m.activeWAL = freshWAL
	m.writer = freshWriter
	m.recordApplied(msg.SnapshotIndex, msg.SnapshotTerm)
}
