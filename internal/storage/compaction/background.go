package compaction

import (
	"fmt"
	"sync"
	"time"
)

// Background runs compaction cycles on a fixed interval, in their own
// goroutine, so a caller's write path (engine.Writer, §13.7) never has
// to trigger or wait on a compaction synchronously.
//
// NOTHING ABOUT ENGINE.WRITER'S PUT OR DELETE TOUCHES THE MANIFEST OR
// AN SSTABLE FILE AT ALL, WHICH IS WHY THIS TYPE NEEDS NO LOCK SHARED
// WITH engine.Writer SPECIFICALLY. engine.Writer's only durable state is
// its own *wal.WAL and *memtable.Memtable (§13.7); Run only ever touches
// files already on disk plus the manifest. The two share no lock, no
// field, nothing -- so a Background compactor was never going to
// contend with Writer.Put or Writer.Delete for anything in-process, by
// construction rather than by any synchronization added here.
//
// THIS STOPPED BEING THE WHOLE STORY THE MOMENT A SECOND MANIFEST WRITER
// EXISTED, AND THE CODE DID NOT CATCH UP TO THAT UNTIL A REAL FAILURE
// FORCED IT TO. kvstore.Machine's own flush trigger
// (freezeAndFlushLocked) is not engine.Writer -- it loads this exact
// manifest, appends its own newly-flushed file to level 0, and saves,
// entirely independently of whatever cycle this type's own loop is
// running at the same moment. Two unsynchronized load-modify-save
// cycles against the same file is a lost-update race by definition:
// whichever Save lands second silently wins, either reverting a
// just-completed compaction's manifest update back to a stale view that
// still names files Run has already deleted (kvstore's own
// TestFlushTriggersOnceTheActiveMemtableExceedsThreshold, "L0 holds 10
// files" with a run of keys permanently unreadable), or erasing a
// flush's brand-new file the instant CompactLevel clears the whole
// level it just wrote into (the same test's other failure mode, "L0 has
// no files ... the flush trigger never fired," when it had). See
// manifestMu's own doc, and kvstore/machine.go's freezeAndFlushLocked,
// for the fix -- exactly the "different, unbuilt layer above this
// package" manifest.go's own doc names as where this coordination
// belongs, now built rather than left implicit.
//
// What a Background compactor still separately competes for, and always
// did, is the same physical disk every concurrent WAL fsync is also
// competing for -- unrelated to the in-process race above, and exactly
// what stall_test.go measures directly, rather than assumes away
// because "there's no shared lock."
type Background struct {
	manifestPath string
	dir          string
	opts         Options
	interval     time.Duration

	stopCh   chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	// manifestMu serializes every load-modify-save cycle against
	// manifestPath, across BOTH writers that touch it: this type's own
	// Run calls (drainOneCycle, below) and kvstore.Machine's flush path
	// (via the exported Lock/Unlock this field backs). Neither writer
	// tolerates the other's update disappearing underneath it -- see
	// this type's own doc for the two concrete failure shapes an
	// unguarded race between them produced.
	manifestMu sync.Mutex

	mu      sync.Mutex
	lastErr error
	cycles  int
}

// StartBackground begins running compaction cycles against manifestPath
// and dir every interval, and returns immediately -- the loop runs in
// its own goroutine until Stop is called.
func StartBackground(manifestPath, dir string, opts Options, interval time.Duration) *Background {
	b := &Background{
		manifestPath: manifestPath,
		dir:          dir,
		opts:         opts,
		interval:     interval,
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}
	go b.loop()
	return b
}

// Lock and Unlock give a caller that ALSO mutates the manifest this
// Background is running compactions against -- currently, exactly one:
// kvstore.Machine's own flush trigger -- a way to hold out this type's
// own compaction cycles for the duration of its own load-modify-save
// sequence, and vice versa. A matched Lock/Unlock pair, rather than a
// single "run this under lock" callback, because the caller's own
// sequence spans several steps that must all happen atomically with
// respect to a concurrent Run (load the manifest, derive the next
// sequence number from it, write the new SSTable that number names,
// append it to level 0, save) -- a callback taking a closure over that
// whole sequence would work too, but would hide, rather than name, that
// what is actually being held here is the manifest, not some smaller
// unit of work.
func (b *Background) Lock()   { b.manifestMu.Lock() }
func (b *Background) Unlock() { b.manifestMu.Unlock() }

func (b *Background) loop() {
	defer close(b.done)
	// Drain any existing backlog immediately on start rather than
	// waiting a full interval before ever checking -- a node that starts
	// up with a backlog already built (or, in a test, one seeded
	// directly) shouldn't sit idle for up to one interval before its
	// first compaction.
	b.drainOneCycle()

	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.drainOneCycle()
		}
	}
}

// maxDrainCycles bounds how many Run calls drainOneCycle will make
// before giving up and reporting an error, rather than looping until
// PickLevel finally says there is nothing left.
//
// THIS CAP EXISTS BECAUSE OF A HAZARD FOUND WHILE DESIGNING THIS TYPE,
// NOT AS A PRECAUTION AGAINST SOMETHING PURELY HYPOTHETICAL.
// CompactLevel always produces exactly one output file (§13.8's
// documented simplification), which means every level above L0 can only
// ever hold zero or one files under the current design -- true
// cascading (one compaction pushing the level below it over ITS OWN
// threshold) cannot happen with any threshold of 1 or more, since a
// level holding at most one file can never exceed a threshold that
// isn't 0. But `Options{MaxFilesPerLevel: 0}` is a value nothing
// currently rejects, and under it, PickLevel considers a level with
// even one file "over threshold" -- so a single leftover file would get
// compacted one level deeper, forever: the manifest grows one level
// longer and that one file gets rewritten every single cycle, without
// ever converging. A caller passing 0 is almost certainly a
// configuration mistake, not a real requirement, but "almost certainly"
// is exactly the phrase this codebase's own "guarded, not assumed"
// principle (§8) exists to not settle for. maxDrainCycles converts an
// unbounded spin into a bounded, reported failure --
// TestDrainOneCycleStopsAtTheSafetyCapRatherThanSpinningForever provokes
// this directly with MaxFilesPerLevel: 0 and confirms the loop
// terminates instead of hanging.
const maxDrainCycles = 64

// drainOneCycle keeps calling Run until PickLevel has nothing left to
// compact, an error occurs, Stop is requested, or maxDrainCycles is
// reached. Looping here, rather than doing at most one Run call per
// tick, is the right shape for a future in which a compaction genuinely
// can leave the level below it also over threshold -- not reachable
// under the current one-output-file design (see maxDrainCycles's doc),
// but the loop costs nothing extra when a single Run call already
// clears everything, which is what happens today.
func (b *Background) drainOneCycle() {
	for i := 0; i < maxDrainCycles; i++ {
		select {
		case <-b.stopCh:
			return
		default:
		}
		// See manifestMu's own doc: held across the whole call, matching
		// exactly what a concurrent kvstore.Machine flush now also holds
		// it across, so the two load-modify-save cycles against
		// manifestPath can never interleave.
		b.manifestMu.Lock()
		compacted, err := Run(b.manifestPath, b.dir, b.opts)
		b.manifestMu.Unlock()
		if err != nil {
			b.setErr(fmt.Errorf("compaction: background cycle: %w", err))
			return
		}
		if !compacted {
			return
		}
		b.mu.Lock()
		b.cycles++
		b.mu.Unlock()
	}
	b.setErr(fmt.Errorf("compaction: background: hit the %d-cycle safety cap without converging -- "+
		"check Options.MaxFilesPerLevel is not 0 (see maxDrainCycles's doc)", maxDrainCycles))
}

func (b *Background) setErr(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastErr = err
}

// Stop signals the background loop to exit and blocks until it has.
// Unlike Raft's own ticker shutdown (election.go's Node.Stop), which is
// fire-and-forget, this one waits: a caller -- tests, especially --
// needs the guarantee that no further Run call will touch the manifest
// or an SSTable file once Stop returns, not just that a stop was
// requested. Safe to call more than once.
func (b *Background) Stop() {
	b.stopOnce.Do(func() { close(b.stopCh) })
	<-b.done
}

// Err returns the most recent error a compaction cycle produced, or nil
// if every cycle so far has succeeded (including "nothing needed
// compacting," which is success, not an error -- see Run's own doc).
func (b *Background) Err() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastErr
}

// Cycles reports how many compaction cycles have completed successfully
// since Start. For tests and observability, not a stable API surface.
func (b *Background) Cycles() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cycles
}