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
// NOTHING ABOUT PUT OR DELETE TOUCHES THE MANIFEST OR AN SSTABLE FILE AT
// ALL, WHICH IS WHY THIS TYPE NEEDS NO LOCK SHARED WITH A WRITER.
// engine.Writer's only durable state is its own *wal.WAL and
// *memtable.Memtable (§13.7); Run only ever touches files already on
// disk plus the manifest. The two share no lock, no field, nothing --
// so a Background compactor was never going to contend with Writer.Put
// or Writer.Delete for anything in-process, by construction rather than
// by any synchronization added here. What a Background compactor CAN
// still compete for is the same physical disk every concurrent WAL
// fsync is also competing for, which is exactly what stall_test.go
// measures directly, rather than assumes away because "there's no
// shared lock."
type Background struct {
	manifestPath string
	dir          string
	opts         Options
	interval     time.Duration

	stopCh   chan struct{}
	stopOnce sync.Once
	done     chan struct{}

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
		compacted, err := Run(b.manifestPath, b.dir, b.opts)
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
