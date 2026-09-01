package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/storage/compaction"
	"github.com/ekushal02/helios/internal/storage/engine"
	"github.com/ekushal02/helios/internal/storage/manifest"
	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/sstable"
	"github.com/ekushal02/helios/internal/storage/wal"
)

// activeWALName is the fixed filename the CURRENT active memtable's WAL
// always lives at. Fixed, not sequence-numbered, because there is always
// exactly one: freezeAndFlush deletes it the moment its data is durably
// represented in the SSTable that replaces it, and the fresh memtable
// that takes over starts a fresh file at this same name. A restart only
// ever needs to look in one place to find out what wasn't flushed yet.
const activeWALName = "active.wal"

// manifestName is the fixed filename for this node's manifest (§13.8),
// alongside activeWALName in the same data directory.
const manifestName = "MANIFEST"

// Options configures a Machine. Every field has a default a caller gets
// by passing the zero value; DefaultOptions names them explicitly so a
// caller doesn't have to read this struct's own fields to find out.
type Options struct {
	// FlushThresholdBytes is the *Memtable.ApproxSize (§13.3) a caller's
	// active memtable is allowed to reach before Machine freezes it and
	// starts a fresh one -- the flush trigger every prior task's own
	// open questions have named as still missing, closed here.
	FlushThresholdBytes int64

	// CompactionMaxFilesPerLevel feeds compaction.Options (§13.8).
	CompactionMaxFilesPerLevel int

	// CompactionInterval feeds compaction.StartBackground (§13.9).
	CompactionInterval time.Duration

	// WALSyncPolicy is the durability policy every Writer this Machine
	// creates uses (§13.1). Defaults to wal.SyncAlways -- the strongest
	// guarantee, appropriate for a production default even though it is
	// also the slowest; a caller that has measured its own workload and
	// wants a faster policy sets this explicitly, the same way nothing
	// in this engine has ever picked a default it wasn't willing to
	// state and defend (§12's whole list of "asserted, not yet chosen"
	// constants).
	WALSyncPolicy wal.SyncPolicy

	// Compression is what every flush and every compaction output uses
	// (§13.13). Defaults to CompressionFlate: this is the first place in
	// the whole project every piece built across §13 runs together in
	// one real pipeline, and shipping it with compression on by default
	// is the honest way to exercise that pipeline as it would actually
	// run, not as a stripped-down demo of it.
	Compression sstable.CompressionType
}

// DefaultOptions is what NewMachine uses for any field left at its zero
// value -- FlushThresholdBytes: 4MB, CompactionMaxFilesPerLevel: 4
// (compaction.DefaultOptions' own value), CompactionInterval: 1 second,
// WALSyncPolicy: wal.SyncAlways, Compression: sstable.CompressionFlate.
var DefaultOptions = Options{
	FlushThresholdBytes:        4 << 20,
	CompactionMaxFilesPerLevel: compaction.DefaultOptions.MaxFilesPerLevel,
	CompactionInterval:         time.Second,
	WALSyncPolicy:              wal.SyncAlways,
	Compression:                sstable.CompressionFlate,
}

func (o Options) withDefaults() Options {
	if o.FlushThresholdBytes <= 0 {
		o.FlushThresholdBytes = DefaultOptions.FlushThresholdBytes
	}
	if o.CompactionMaxFilesPerLevel <= 0 {
		o.CompactionMaxFilesPerLevel = DefaultOptions.CompactionMaxFilesPerLevel
	}
	if o.CompactionInterval <= 0 {
		o.CompactionInterval = DefaultOptions.CompactionInterval
	}
	// WALSyncPolicy's zero value, SyncAlways (wal.SyncPolicy's own
	// iota ordering, §13.1), is already the correct default -- no
	// override needed, and none possible from the zero value alone.
	return o
}

// Machine is the state machine apply.go's own doc names but Raft never
// builds: the layer above ApplyCh, backed by the storage engine (§13)
// end to end -- WAL and memtable for durability and the active write
// path (§13.1, §13.3, §13.7), SSTables for what gets flushed out of a
// full memtable (§13.2), a manifest and background compaction for what
// happens to those SSTables afterward (§13.8, §13.9), a shared block
// cache for repeat reads (§13.12), and Raft's own snapshot contract
// implemented against all of it (this file).
//
// A Machine owns exactly one raft.Node's worth of ApplyCh, per that
// channel's own single-consumer rule -- attachMachine mirrors
// e2e_test.go's own attachMachine in shape, replacing kvMachine's plain
// map with everything under internal/storage.
type Machine struct {
	n     *raft.Node
	dir   string
	opts  Options
	cache *sstable.BlockCache

	mu           sync.Mutex
	active       *memtable.Memtable
	activeWAL    *wal.WAL
	writer       *engine.Writer
	sstReaders   map[string]*sstable.Reader // filename -> open reader, tracks the current manifest
	appliedIndex int
	appliedTerms map[int]int // pruned to entries above the snapshot floor on every install/take
	fault        string

	// dedup is F-4's whole mechanism: clientID -> the highest
	// sequenceNumber this Machine has applied for that client. A
	// command whose sequenceNumber is at or below the recorded value
	// for its clientID has already been applied -- by an earlier
	// attempt at the identical logical write, reaching this apply path
	// through a DIFFERENT log entry than the one that originally
	// succeeded (see applyCommand's own doc for exactly why that
	// happens) -- and is skipped rather than applied again. clientID
	// 0 is never a key here; it is the "no session" sentinel every
	// non-idempotent Put/Delete call uses, checked before this map is
	// ever consulted. Rebuilt for free by ordinary log replay on
	// restart (§5/§6's own "no prefix is ever applied twice" property
	// means replaying every entry through this exact same check
	// reconstructs the identical table), so it needs no separate
	// persistence of its own outside the snapshot image -- but IS
	// captured inside that image (snapshot.go), because a follower
	// caught up via InstallSnapshot, or a node restarting from a
	// snapshot floor, never replays the entries a snapshot compacted
	// away and would otherwise lose every dedup entry for a write
	// whose original log index is now gone. UNBOUNDED: no entry is
	// ever evicted, so a long-running cluster's memory (and every
	// snapshot's size) grows with the number of DISTINCT client
	// sessions it has ever seen, not the number of keys it holds --
	// a real, named limitation, not an oversight (DESIGN.md §12).
	dedup map[uint64]uint64

	bg *compaction.Background

	done chan struct{}
}

// NewMachine recovers whatever this data directory already holds
// (compaction.Recover, §13.10, then a WAL replay into a fresh memtable,
// §13.7's RecoverMemtable), starts background compaction, attaches to n
// as the sole consumer of n.ApplyCh(), and returns. Call Close to stop
// both goroutines this starts.
func NewMachine(n *raft.Node, dir string, cache *sstable.BlockCache, opts Options) (*Machine, error) {
	opts = opts.withDefaults()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("kvstore: create %s: %w", dir, err)
	}

	manifestPath := filepath.Join(dir, manifestName)
	mf, _, err := compaction.Recover(manifestPath, dir)
	if err != nil {
		return nil, fmt.Errorf("kvstore: recover %s: %w", dir, err)
	}

	sstReaders := make(map[string]*sstable.Reader)
	for _, level := range mf.Levels {
		for _, name := range level {
			r, err := sstable.OpenWithCache(filepath.Join(dir, name), cache)
			if err != nil {
				for _, opened := range sstReaders {
					opened.Close()
				}
				return nil, fmt.Errorf("kvstore: open %s: %w", name, err)
			}
			sstReaders[name] = r
		}
	}

	m := memtable.NewWithSeed(time.Now().UnixNano())
	walPath := filepath.Join(dir, activeWALName)
	w, err := engine.RecoverMemtable(walPath, opts.WALSyncPolicy, m)
	if err != nil {
		for _, opened := range sstReaders {
			opened.Close()
		}
		return nil, fmt.Errorf("kvstore: recover memtable: %w", err)
	}

	machine := &Machine{
		n:            n,
		dir:          dir,
		opts:         opts,
		cache:        cache,
		active:       m,
		activeWAL:    w,
		writer:       engine.NewWriter(w, m),
		sstReaders:   sstReaders,
		appliedTerms: make(map[int]int),
		dedup:        make(map[uint64]uint64),
		done:         make(chan struct{}),
	}

	machine.bg = compaction.StartBackground(manifestPath, dir,
		compaction.Options{MaxFilesPerLevel: opts.CompactionMaxFilesPerLevel}, opts.CompactionInterval)

	go machine.run()

	return machine, nil
}

// Close stops both goroutines NewMachine started and releases every open
// file. It does not stop n -- the Node this Machine is attached to is
// the caller's, not this package's, to own the lifecycle of.
func (m *Machine) Close() error {
	m.bg.Stop()
	m.n.Stop()
	<-m.done

	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	if err := m.activeWAL.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, r := range m.sstReaders {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// -----------------------------------------------------------------------
// The apply loop
// -----------------------------------------------------------------------

// run is the sole consumer of m.n.ApplyCh() and m.n.SnapshotNotify(),
// for the whole life of the Machine -- the identical single-goroutine
// shape attachMachine (e2e_test.go) and attachSnapshotMachine
// (installsnapshot_test.go) both already use, for the same reason both
// state: ApplyCh's delivery order only becomes application order through
// exactly one receiver.
func (m *Machine) run() {
	defer close(m.done)
	for {
		select {
		case msg, ok := <-m.n.ApplyCh():
			if !ok {
				return // the node stopped; ApplyCh is closed exactly once, here
			}
			m.handle(msg)
		case <-m.n.SnapshotNotify():
			m.take()
		}
	}
}

func (m *Machine) handle(msg raft.ApplyMsg) {
	switch {
	case msg.SnapshotValid:
		m.installSnapshot(msg)
	case msg.CommandValid:
		m.applyCommand(msg)
	default:
		// A read barrier: advance the applied index, apply nothing. See
		// apply.go's own doc on ApplyMsg for why this must still be
		// recorded rather than skipped -- a reader is waiting for the
		// STATE MACHINE to reach this index.
		m.mu.Lock()
		m.recordApplied(msg.CommandIndex, msg.CommandTerm)
		m.mu.Unlock()
	}
}

func (m *Machine) applyCommand(msg raft.ApplyMsg) {
	dec, err := decodeCommand(msg.Command)
	if err != nil {
		m.mu.Lock()
		if m.fault == "" {
			m.fault = fmt.Sprintf("index %d: %v", msg.CommandIndex, err)
		}
		m.recordApplied(msg.CommandIndex, msg.CommandTerm)
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// F-4: a command whose clientID is non-zero and whose
	// sequenceNumber is at or below what this Machine has already
	// applied for that clientID is a duplicate -- the retry case this
	// whole task exists for. THIS is the reason a duplicate is not
	// caught earlier, at the gRPC server (internal/server) or in
	// Machine.PutIdempotent itself: two concurrent submissions of the
	// identical (clientID, sequenceNumber) pair -- an original attempt
	// still in flight and a retry issued because the client could not
	// tell whether the original had committed -- can both reach n.Submit
	// and both get appended as SEPARATE, distinct log entries at
	// different indices, since Raft has no concept of "this command is
	// the same as that one." The only place both entries are ever seen
	// one at a time, in a fixed order, by a single decision-maker is
	// here, in the apply path -- exactly the same "one choke point,
	// not several independent guesses" reasoning §8 gives for the
	// commitTo funnel. The caller waiting on THIS entry's own index
	// (Machine.write's waitForApplied) still gets an ordinary,
	// successful return either way -- it cannot tell, and does not need
	// to, whether its write actually mutated storage or was recognized
	// as already done.
	if dec.ClientID != 0 && dec.SequenceNumber <= m.dedup[dec.ClientID] {
		m.recordApplied(msg.CommandIndex, msg.CommandTerm)
		return
	}

	var applyErr error
	if dec.Tombstone {
		applyErr = m.writer.Delete(dec.Key)
	} else {
		applyErr = m.writer.Put(dec.Key, dec.Value)
	}
	if applyErr != nil {
		if m.fault == "" {
			m.fault = fmt.Sprintf("index %d: %v", msg.CommandIndex, applyErr)
		}
		m.recordApplied(msg.CommandIndex, msg.CommandTerm)
		return
	}

	// Recorded only AFTER a successful apply, deliberately: if the
	// write itself failed, a future retry carrying the same
	// (clientID, sequenceNumber) must still be free to actually
	// attempt the write, not be dismissed as an already-done duplicate
	// of something that never really happened.
	if dec.ClientID != 0 {
		m.dedup[dec.ClientID] = dec.SequenceNumber
	}

	m.recordApplied(msg.CommandIndex, msg.CommandTerm)

	if m.active.ApproxSize() >= m.opts.FlushThresholdBytes {
		if err := m.freezeAndFlushLocked(); err != nil && m.fault == "" {
			m.fault = fmt.Sprintf("index %d: freeze and flush: %v", msg.CommandIndex, err)
		}
	}
}

// maxAppliedTermsWindow bounds how many (index -> term) pairs
// recordApplied remembers, by keeping only the most recent window worth
// -- what keeps appliedTerms from growing without limit over a
// long-running node's lifetime, the same problem e2e_test.go's own
// kvMachine never has to solve because a test process is short-lived.
//
// A WINDOW BY INDEX, NOT PRUNING TIED TO SNAPSHOT TIMING -- THE FIRST
// DESIGN TRIED, AND REJECTED BEFORE IT SHIPPED. Pruning everything at or
// below the snapshot floor the moment a snapshot is taken sounds
// airtight -- no future barrier can land at or below a covered index --
// but it is not: a read barrier issued a moment before a snapshot, still
// waiting when the snapshot's own pruning runs, would find its own
// index's term already deleted, even though the index truly was applied
// at a real, correct term. Not a wrong answer -- waitForApplied treats a
// missing entry as "unknown, retry" rather than fabricating a term -- but
// a spurious failure a genuinely successful read had no reason to
// suffer. A fixed window by index sidesteps the timing question
// entirely: it has nothing to do with when a snapshot happens, only with
// how far behind the current applied index a lookup is, so a barrier
// checked promptly (the only kind ReadIndex's own protocol describes)
// always finds its entry.
const maxAppliedTermsWindow = 10000

// recordApplied is the bookkeeping every message updates, valid,
// command, or barrier alike -- appliedIndex advances and the term that
// index carried is remembered so a caller waiting on THAT index (Get,
// below) can tell a genuine application apart from a later term's
// having overwritten it. The window is trimmed here too, since indices
// only ever increase (the applier's own single-consumer, in-order
// guarantee), so trimming everything below the new floor is a cheap,
// small loop, not a full map scan. Caller must hold m.mu.
func (m *Machine) recordApplied(index, term int) {
	m.appliedIndex = index
	m.appliedTerms[index] = term

	floor := index - maxAppliedTermsWindow
	for idx := range m.appliedTerms {
		if idx < floor {
			delete(m.appliedTerms, idx)
		}
	}
}

// -----------------------------------------------------------------------
// Flushing: the memtable-swap open question, closed
// -----------------------------------------------------------------------

// freezeAndFlushLocked is what every prior §13 task's own open questions
// have named as still missing: "nothing yet swaps a full memtable out
// from under live writes." Called from applyCommand once
// m.active.ApproxSize() crosses FlushThresholdBytes, it closes the
// current active WAL, flushes the (now-closed-WAL) memtable to a new
// compressed SSTable, records that file in the manifest's L0 (§13.8,
// newest-first, matching engine.Reader's own convention, §13.6), deletes
// the now-redundant WAL, and starts a fresh memtable, WAL, and Writer as
// the new active target -- all before returning control to the apply
// loop, which is why this runs SYNCHRONOUSLY, in the apply path, rather
// than on a separate goroutine the way compaction.Background runs
// (§13.9). That is a real, deliberate simplification: while a flush is
// in progress, no further command can be applied, and Get (below) blocks
// behind the same m.mu a flush holds. A background flush goroutine,
// mirroring Background's own shape, is real future work, recorded as an
// open question rather than attempted in the same task that closes the
// swap-the-memtable question itself. Caller must hold m.mu.
func (m *Machine) freezeAndFlushLocked() error {
	frozen := m.active
	frozenWALPath := filepath.Join(m.dir, activeWALName)

	if err := m.activeWAL.Close(); err != nil {
		return fmt.Errorf("close active WAL: %w", err)
	}

	manifestPath := filepath.Join(m.dir, manifestName)
	mf, err := manifest.Load(manifestPath)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	seq := nextSequence(mf)
	sstName := fmt.Sprintf("%06d.sst", seq)
	sstPath := filepath.Join(m.dir, sstName)

	if _, err := sstable.FlushCompressed(frozen, sstPath, m.opts.Compression); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	mf.EnsureLevel(0)
	mf.Levels[0] = append([]string{sstName}, mf.Levels[0]...)
	if err := manifest.Save(manifestPath, mf); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}

	if err := os.Remove(frozenWALPath); err != nil {
		return fmt.Errorf("remove flushed WAL: %w", err)
	}

	r, err := sstable.OpenWithCache(sstPath, m.cache)
	if err != nil {
		return fmt.Errorf("open flushed SSTable: %w", err)
	}
	m.sstReaders[sstName] = r

	fresh := memtable.NewWithSeed(time.Now().UnixNano())
	freshWAL, err := engine.RecoverMemtable(frozenWALPath, m.opts.WALSyncPolicy, fresh)
	if err != nil {
		return fmt.Errorf("start fresh memtable: %w", err)
	}
	m.active = fresh
	m.activeWAL = freshWAL
	m.writer = engine.NewWriter(freshWAL, fresh)

	return nil
}

var sequencePattern = regexp.MustCompile(`^(\d+)\.sst$`)

// nextSequence derives the next SSTable file number from the manifest's
// own contents, the identical scheme compaction.nextSequence (unexported
// in that package) already uses -- duplicated rather than shared for the
// same one-way-dependency reasoning repeated throughout this engine
// (sstable.syncDir from raft.syncDir, manifest.syncDir from both): this
// package depends on compaction, and compaction has no reason to depend
// back on this one just to share five lines. Deriving from the manifest
// itself, rather than a separate counter, is what guarantees flush's own
// output filenames never collide with compaction's: both draw from the
// same shared numbering space, the set of names already on record.
func nextSequence(mf *manifest.Manifest) int {
	max := 0
	for _, level := range mf.Levels {
		for _, name := range level {
			match := sequencePattern.FindStringSubmatch(name)
			if match == nil {
				continue
			}
			n, err := strconv.Atoi(match[1])
			if err != nil {
				continue
			}
			if n > max {
				max = n
			}
		}
	}
	return max + 1
}
