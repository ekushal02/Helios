package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fullSystemNumKeys, fullSystemDeleteEveryN, and fullSystemNumWriters are
// fixed, not caller-configurable in the ordinary sense -- the identical
// reasoning every other large measurement's workload constants in this
// codebase already give (§13.9, §13.11, §13.12, §13.13): the point of
// running this once, at a stated scale, is a real number at that scale,
// not a parameter a future run might silently change.
//
// fullSystemNumKeys IS overridable, but only through HELIOS_FULLSYSTEM_KEYS,
// deliberately not a test flag or a second exported constant -- an
// environment variable is loud enough not to be forgotten, and this
// exists for exactly one purpose: getting a real, fast throughput number
// at a SMALLER scale on a given machine, through the EXACT SAME code
// path the real one-million-key run takes, before committing hours to
// the full run. A first attempt at this test, on real hardware, hit a
// two-hour timeout at roughly 19% complete -- not a deadlock (the
// goroutine dump showed ordinary lock contention from many writers
// queued behind one serialized apply loop, not a lock nobody ever
// releases), but real, underestimated slowness: Submit's own
// n.persistIfDirty (submit.go, §5) fsyncs Raft's persistent log
// synchronously, on top of the storage engine's own WAL fsync
// (wal.SyncAlways, §13.1) that Machine.applyCommand triggers separately
// -- two fsyncs per write, not the one this test's own original estimate
// accounted for. Run a smaller scale first:
//
//	HELIOS_FULLSYSTEM_KEYS=20000 go test ./internal/kvstore/... -run TestFullSystemOneMillionKeys -v -timeout 30m
//
// and extrapolate from the real, reported puts/sec before deciding how
// long a full one-million-key run actually needs.
var fullSystemNumKeys = func() int {
	if v := os.Getenv("HELIOS_FULLSYSTEM_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 1_000_000
}()

const (
	fullSystemValueSize    = 100
	fullSystemDeleteEveryN = 100 // 1% of keys deleted after being written

	// fullSystemNumWriters WAS 64, ON THE REASONING THAT SUBMIT IS
	// NON-BLOCKING AND MANY GOROUTINES COULD PIPELINE THROUGH IT FOR
	// REAL THROUGHPUT. THAT REASONING WAS WRONG FOR THIS SYSTEM, FOUND
	// FROM THE SAME FIRST FULL-SCALE RUN THAT SURFACED THE TWO-FSYNC
	// COST ABOVE. Submit is non-blocking only in the sense that it
	// doesn't wait for replication or application (submit.go's own
	// doc) -- it is NOT cheap to call from many goroutines at once,
	// because appendChecked holds n.mu for the full duration of its own
	// synchronous persistIfDirty fsync before returning. Every one of
	// 64 concurrent callers was therefore just queuing behind that one
	// lock, contributing no real throughput and producing a goroutine
	// dump with 60+ nearly-identical stacks on the timeout that hit
	// this test's first real run -- unreadable for no benefit. 8 is
	// small enough to keep the dump legible if this ever times out
	// again, and large enough that this test still isn't purely
	// sequential; it is not a throughput optimization, since there
	// isn't a real one available to this test without touching Raft's
	// own persistIfDirty (§12, out of scope here -- see this test's own
	// doc above).
	fullSystemNumWriters = 8
)

func fullSystemKey(i int) []byte {
	return []byte(fmt.Sprintf("fullsystem-key-%08d", i))
}

func fullSystemValue(i int) []byte {
	// A fixed-shape, deterministic value -- long enough to be a
	// realistic record size, cheap to regenerate from i alone rather
	// than held in a map, which is what keeps this test's own memory
	// footprint independent of fullSystemNumKeys instead of growing
	// with it.
	return []byte(fmt.Sprintf("value-%08d-%s", i, fullSystemPadding))
}

// fullSystemPadding pads every value out to a realistic, fixed size --
// computed once, not per call, since fullSystemValue runs two million
// times over the course of this test (once per key during the write
// phase, twice more during each of the two read-back passes).
var fullSystemPadding = func() string {
	const prefix = "value-00000000-"
	pad := fullSystemValueSize - len(prefix)
	if pad < 0 {
		pad = 0
	}
	b := make([]byte, pad)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}()

func fullSystemDeleted(i int) bool {
	return i%fullSystemDeleteEveryN == 0
}

// TestFullSystemOneMillionKeys IS THE CAPSTONE TEST THIS TASK EXISTS TO
// PRODUCE: one million distinct keys, written through a real single-node
// Raft cluster and the full storage engine beneath it, verified, the
// whole system stopped and reopened from disk, and verified again.
// Everything built across §13 and §14 is exercised together, at a scale
// none of this project's other tests attempt.
//
// SKIPPED IN SHORT MODE, THE SAME "_test.go FILES ARE EXCLUDED FROM
// go build AT ZERO RUNTIME COST; THE RIGHT RESPONSE TO SUITE GROWTH IS
// testing.Short() TIERING, NOT DELETION" CONVENTION THIS PROJECT HAS
// FOLLOWED SINCE THE RAFT PHASE. Get a real throughput number for the
// machine this runs on BEFORE committing to a full one-million-key run
// -- the same code path, at a scale that finishes in minutes:
//
//	HELIOS_FULLSYSTEM_KEYS=20000 go test ./internal/kvstore/... -run TestFullSystemOneMillionKeys -v -timeout 30m
//
// EXTRAPOLATE FROM BOTH THE WRITE PHASE AND THE RESTART PHASE, NOT JUST
// THE WRITE PHASE. §14.9's finding, restated as a planning consequence:
// the restart phase now correctly waits for (and reports) the apply
// loop's full catch-up through the redelivered history, which costs
// roughly what the write and delete phases combined already cost. A
// smaller run's own reported total -- write + delete + restart, not
// write alone -- is what to scale up, or the full run's timeout will be
// budgeted for roughly half of what it actually needs:
//
//	go test ./internal/kvstore/... -run TestFullSystemOneMillionKeys -v -timeout 0
//
// -timeout 0 disables the timeout entirely and lets the run take as
// long as it needs -- given the uncertainty in any extrapolation, this
// is the safer choice over guessing a specific duration and having a
// nearly-complete run fail on the clock instead of finishing.
//
// A FIRST ATTEMPT AT THE FULL RUN, ON REAL HARDWARE, HIT A TWO-HOUR
// TIMEOUT AT ROUGHLY 19% COMPLETE -- NOT A DEADLOCK, A GENUINE, LARGER-
// THAN-ESTIMATED COST, CONFIRMED RATHER THAN ASSUMED. The goroutine dump
// `go test` produces on a timeout showed dozens of writer goroutines
// queued on the same two mutexes (this Machine's own, and raft.Node's)
// -- exactly what ordinary contention behind one serialized bottleneck
// looks like, not a lock that never releases; the log index each
// goroutine was waiting on was climbing steadily across the dump, not
// frozen at one value. The real cause: every Put crosses TWO fsync
// boundaries, not the one this comment originally accounted for --
// raft.Node.Submit's own n.persistIfDirty (submit.go, §5) fsyncs Raft's
// persistent log synchronously, and Machine.applyCommand separately
// triggers the storage engine's own WAL fsync (wal.SyncAlways, §13.1).
// Confirmed, not just theorized: a diagnostic run at 20,000 keys on a
// second, unrelated machine reproduced materially the same order-of-
// magnitude slowness. See DESIGN.md §14.9 for the fuller argument,
// including why this may be fixable together with that section's own
// finding -- Raft's own log is arguably already sufficient durability,
// which would make the storage engine's own per-write fsync redundant
// rather than load-bearing, IF the restart-side accounting §14.9
// already flags as unfinished were fixed at the same time. Not attempted
// here; recorded, not guessed at.
//
// RAFT'S OWN SNAPSHOTTING IS DISABLED FOR THIS TEST, DELIBERATELY, NOT
// AS AN OVERSIGHT. §14.4 already documents why a snapshot here is
// LOGICAL, not physical: `take` re-encodes the ENTIRE current live key
// set into one blob on every cycle. At this test's scale, that cost
// grows with the live set across the run -- by the last snapshot before
// compaction reclaims deleted keys, it would be re-encoding on the order
// of a million records, repeatedly, for a mechanism this test is not
// actually trying to exercise. What IS being exercised here is the
// storage engine's OWN restart path -- compaction.Recover (§13.10) and
// WAL replay (§13.7's RecoverMemtable) -- which needs no Raft snapshot
// to have ever been taken at all. `TestSnapshotTakeAndInstallRoundTrips`
// already checks the Raft-snapshot path in isolation, at a scale that
// doesn't confound it with this cost; this test isolates the other one.
//
// RESTART CURRENTLY REAPPLIES THE ENTIRE COMMITTED HISTORY A SECOND
// TIME, NOT JUST WHATEVER WASN'T YET FLUSHED -- A REAL FINDING FROM
// BUILDING THIS TEST, NOT A DEFECT IN THE TEST ITSELF. `lastApplied` is
// volatile Raft state (§5's persistent/volatile distinction) and resets
// to zero on every restart; nothing currently tells a freshly-reopened
// `raft.Node` "the state machine already durably reflects everything up
// to index N," so `ApplyCh` redelivers every committed entry from the
// beginning, on top of whatever `compaction.Recover`/`RecoverMemtable`
// already reconstructed from disk. This is NOT a correctness bug --
// Put/Delete are idempotent under replay in the same order, which is
// exactly why `TestRestartRecoversAllAppliedState` (smaller scale) never
// caught it: the FINAL values are identical either way. It is a real
// efficiency problem, invisible at small scale and unmissable at this
// one: catch-up costs roughly as much as the original write phase, not
// the near-instant recovery a well-designed system would achieve. See
// DESIGN.md §14.9 for the full argument and why fixing it properly
// (durably tracking the applied-index high-water mark without adding a
// second fsync to every write) is real, deliberately deferred follow-up
// work, not attempted here.
//
// A SECOND BUG, IN THIS TEST ITSELF, WAS FOUND FROM A REAL RUN FAILING
// -- NOT REASONED OUT IN ADVANCE EITHER. NewMachine starts the apply
// loop with `go machine.run()` and returns immediately; it does not
// wait for the redelivery above to actually finish. An earlier version
// of this test measured "restart" as the time until NewMachine
// returned, which only ever captured the fast, SYNCHRONOUS half of a
// restart -- while the slow half (the apply loop working through
// everything Raft redelivers) ran invisibly, overlapping with whatever
// came next: this test's own post-restart verification. At a real
// 20,000-key scale, that produced exactly the failure a premature
// verification phase would: the first several dozen `GetLeaseRead`
// calls timed out waiting for their own read barrier, because the apply
// loop was still minutes behind and 5 seconds (`defaultReadTimeout`,
// read_snapshot.go) is nowhere near long enough to wait it out. Every
// one of those reads was correctly refusing to answer from a state that
// hadn't caught up yet -- not a correctness bug, a timing bug in this
// test's own phase ordering. Fixed by capturing `AppliedIndex()` before
// closing the first Machine and polling the second one until it
// reaches that same value, with a budget derived from the write and
// delete phases this same run already measured (§14.9's own claim,
// checked directly: catch-up costs roughly what produced the backlog),
// before verification is allowed to start at all. That wait is also
// what "restart" now actually reports.
func TestFullSystemOneMillionKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("full-system test: 1,000,000 keys, skipped in short mode -- run explicitly, see this test's own doc")
	}

	dir := t.TempDir()
	n1 := newTestNode(t, dir)
	n1.SetSnapshotThreshold(0) // disabled -- see the type doc above
	waitForLeader(t, n1, 3*time.Second)
	m1 := newTestMachine(t, dir, n1, DefaultOptions)

	t.Logf("=== full-system test: %d keys, %d writers, value size %d ===", fullSystemNumKeys, fullSystemNumWriters, fullSystemValueSize)

	// --- Phase 1: write ---------------------------------------------------
	writeStart := time.Now()
	fullSystemWritePhase(t, m1)
	writeDuration := time.Since(writeStart)
	t.Logf("write phase:  %d puts in %v (%.0f puts/sec)", fullSystemNumKeys, writeDuration, float64(fullSystemNumKeys)/writeDuration.Seconds())

	// --- Phase 2: delete a sample --------------------------------------
	deleteStart := time.Now()
	numDeleted := fullSystemDeletePhase(t, m1)
	deleteDuration := time.Since(deleteStart)
	t.Logf("delete phase: %d deletes in %v (%.0f deletes/sec)", numDeleted, deleteDuration, float64(numDeleted)/deleteDuration.Seconds())

	fullSystemReportManifest(t, dir, "after write+delete, before restart")

	// --- Phase 3: read back, before restart -----------------------------
	readStart := time.Now()
	fullSystemVerifyPhase(t, m1)
	readDuration := time.Since(readStart)
	t.Logf("read-back phase (pre-restart):  %d keys checked in %v (%.0f gets/sec)", fullSystemNumKeys, readDuration, float64(fullSystemNumKeys)/readDuration.Seconds())

	// A small sample through the SAFE (barrier) read path too -- not all
	// one million, which would add a million read-barrier entries to the
	// log for no benefit; GetLeaseRead above already exercises the real
	// storage read path at full scale, and this just confirms the other
	// read path still agrees with it.
	fullSystemSpotCheckSafeReads(t, m1, 200)

	// --- Phase 4: restart --------------------------------------------------
	// Captured BEFORE Close, deliberately: this is the target the apply
	// loop on the far side of the restart needs to reach before ANY read
	// against m2 can be trusted to see the full history. See the fix
	// this whole capture-and-wait exists to correct, in the paragraph
	// below.
	appliedBeforeClose := m1.AppliedIndex()

	restartStart := time.Now()
	if err := m1.Close(); err != nil {
		t.Fatalf("m1.Close: %v", err)
	}
	n2 := newTestNode(t, dir)
	n2.SetSnapshotThreshold(0)
	waitForLeader(t, n2, 3*time.Second)
	m2 := newTestMachine(t, dir, n2, DefaultOptions)
	setupDuration := time.Since(restartStart)

	// THE REAL RESTART COST IS MEASURED HERE, NOT ABOVE -- A BUG IN THIS
	// TEST'S OWN FIRST VERSION, FOUND FROM A REAL RUN FAILING, NOT
	// REASONED OUT IN ADVANCE. NewMachine starts the apply loop with `go
	// machine.run()` and returns immediately; it does not wait for
	// Raft's own redelivered backlog (§14.9's finding: the ENTIRE
	// committed history, not just the unflushed tail) to actually finish
	// reapplying. setupDuration above only ever measured the SYNCHRONOUS
	// half of a restart -- compaction.Recover and WAL replay, both
	// genuinely fast -- while the slow half (the apply loop working
	// through everything Raft redelivers) was happening ASYNCHRONOUSLY,
	// invisible to any timer that stops the moment NewMachine returns.
	// A real 20,000-key run caught this directly: 144 of the very first
	// post-restart reads failed with a read-barrier timeout, because
	// GetLeaseRead's default 5-second wait is nowhere near long enough
	// for an apply loop that -- at this system's real, measured
	// per-write cost (§14.10) -- needs several MINUTES to catch up on
	// this many redelivered entries. Not a correctness bug -- every one
	// of those reads was correctly refusing to answer rather than
	// answering from stale state -- but a timing bug in this test,
	// since the verification phase started before the system it was
	// verifying had actually finished catching up. Fixed by waiting for
	// AppliedIndex to reach what it held before the restart, with no
	// per-read timeout in the way, before verification starts at all --
	// and reporting THIS wait as what "restart" actually costs.
	// Bounded by the measured phases that just ran, not a fixed guess --
	// §14.9's own claim is that catch-up costs roughly what the writes
	// that produced the backlog cost, so a fixed deadline picked without
	// reference to this run's own observed rate would either be far too
	// short (as an earlier fixed 1-hour bound was, for any run slower
	// than roughly 5.6 puts/sec at one million keys) or an arbitrary
	// guess in the other direction. 3x the combined write and delete
	// phase duration, plus a flat ten-minute floor for a run small
	// enough that 3x would otherwise be unreasonably tight, is generous
	// without being unbounded.
	catchupBudget := 3*(writeDuration+deleteDuration) + 10*time.Minute
	catchupDeadline := time.Now().Add(catchupBudget)
	for m2.AppliedIndex() < appliedBeforeClose && time.Now().Before(catchupDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	restartDuration := time.Since(restartStart)
	if got := m2.AppliedIndex(); got < appliedBeforeClose {
		t.Fatalf("post-restart catch-up did not complete within its %v budget (3x the write+delete phase duration): AppliedIndex() = %d, want at least %d -- see this test's own note on §14.9",
			catchupBudget, got, appliedBeforeClose)
	}
	t.Logf("restart (setup: %v, full catch-up via ApplyCh redelivery: %v, total: %v) -- see DESIGN.md §14.9",
		setupDuration, restartDuration-setupDuration, restartDuration)

	// --- Phase 5: read back, after restart ------------------------------
	postReadStart := time.Now()
	fullSystemVerifyPhase(t, m2)
	postReadDuration := time.Since(postReadStart)
	t.Logf("read-back phase (post-restart): %d keys checked in %v (%.0f gets/sec)", fullSystemNumKeys, postReadDuration, float64(fullSystemNumKeys)/postReadDuration.Seconds())

	fullSystemSpotCheckSafeReads(t, m2, 200)
	fullSystemReportManifest(t, dir, "after restart")

	if fault := m2.Fault(); fault != "" {
		t.Fatalf("Fault() after restart = %q, want empty", fault)
	}

	t.Logf("=== full-system test complete: %d keys, %d deleted, correct before and after restart ===",
		fullSystemNumKeys, numDeleted)
}

// fullSystemWritePhase writes every non-deleted-later key through a pool
// of fullSystemNumWriters goroutines, each pulling the next index from a
// shared atomic counter and calling Put.
//
// A SMALL POOL, NOT A THROUGHPUT OPTIMIZATION -- SEE fullSystemNumWriters'
// OWN DOC FOR WHY 64 WAS TRIED FIRST AND WAS WRONG. Submit returning as
// soon as an entry reaches the log (submit.go's own doc) does not make
// concurrent callers cheap: appendChecked holds n.mu for the full
// duration of its own synchronous persistIfDirty fsync, so many
// goroutines calling Submit at once queue behind that one lock rather
// than achieving any real pipelining. This pool exists so the test isn't
// purely sequential and so a future timeout's own goroutine dump stays
// legible, not because concurrency measurably helps here.
// fullSystemProgressLogPath is a FIXED location OUTSIDE t.TempDir()
// deliberately -- the whole point is surviving whatever happens to the
// test's own temp directory (cleaned up on normal completion, left
// behind or not depending on how the process dies). A real 18-hour run
// that had to be killed mid-write produced nothing quotable, because
// every t.Logf call inside a loop is buffered by the testing framework
// and only flushed when the TEST completes -- pass, fail, or exit. A
// test that never completes never flushes anything, dead or alive. This
// writes progress with a plain, unbuffered fmt.Fprintf instead, which
// appears in the terminal in real time and, mirrored to this file,
// survives even if the terminal itself is gone by the time anyone reads
// it back.
var fullSystemProgressLogPath = filepath.Join(os.TempDir(), "helios-fullsystem-progress.log")

// fullSystemProgress starts a ticker that reports completed/total,
// elapsed time, rate, and a naive linear ETA every interval, both to
// stdout and to fullSystemProgressLogPath, until the returned stop
// function is called. label distinguishes which phase is reporting
// (write, delete, verify) when more than one runs in the same process.
//
// A NAIVE LINEAR ETA, STATED AS SUCH, NOT A PROMISE. §14.10's own
// finding is that this system's real per-operation cost is not constant
// -- compaction cost compounds as L1 grows, so a rate measured early in
// a run understates the true remaining time. The ETA printed here is
// the honest, simple "at the CURRENT rate" number, not a corrected
// prediction; a reader comparing it against how long the run actually
// takes is itself more evidence for the same finding, not a bug in this
// reporter.
func fullSystemProgress(label string, total int, completed *atomic.Int64, interval time.Duration) (stop func()) {
	start := time.Now()
	done := make(chan struct{})
	logFile, err := os.OpenFile(fullSystemProgressLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		logFile = nil // progress still prints to stdout even if the file can't be opened
	}

	report := func() {
		completedNow := completed.Load()
		elapsed := time.Since(start)
		rate := float64(completedNow) / elapsed.Seconds()
		var etaStr string
		if rate > 0 && completedNow < int64(total) {
			eta := time.Duration(float64(int64(total)-completedNow)/rate) * time.Second
			etaStr = eta.Round(time.Second).String()
		} else {
			etaStr = "unknown"
		}
		line := fmt.Sprintf("[fullsystem %s] %s %d/%d (%.1f%%) elapsed=%v rate=%.2f/s naive_eta=%s\n",
			label, time.Now().Format(time.RFC3339), completedNow, total, 100*float64(completedNow)/float64(total),
			elapsed.Round(time.Second), rate, etaStr)
		fmt.Fprint(os.Stdout, line)
		if logFile != nil {
			fmt.Fprint(logFile, line)
		}
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				report()
			case <-done:
				report() // final line at whatever count was reached
				if logFile != nil {
					logFile.Close()
				}
				return
			}
		}
	}()

	return func() { close(done) }
}

func fullSystemWritePhase(t *testing.T, m *Machine) {
	t.Helper()
	var next atomic.Int64
	var completed atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, fullSystemNumWriters)

	stopProgress := fullSystemProgress("write", fullSystemNumKeys, &completed, 2*time.Minute)
	defer stopProgress()

	worker := func() {
		defer wg.Done()
		for {
			i := int(next.Add(1)) - 1
			if i >= fullSystemNumKeys {
				return
			}
			if err := fullSystemPutWithRetry(m, fullSystemKey(i), fullSystemValue(i)); err != nil {
				select {
				case errCh <- fmt.Errorf("key %d: %w", i, err):
				default:
				}
				return
			}
			completed.Add(1)
		}
	}

	wg.Add(fullSystemNumWriters)
	for w := 0; w < fullSystemNumWriters; w++ {
		go worker()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("write phase: %v", err)
	}
}

// fullSystemDeletePhase deletes every key for which fullSystemDeleted
// reports true, the same worker-pool shape as the write phase.
func fullSystemDeletePhase(t *testing.T, m *Machine) int {
	t.Helper()
	var next atomic.Int64
	var count atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, fullSystemNumWriters)

	worker := func() {
		defer wg.Done()
		for {
			i := int(next.Add(1)) - 1
			if i >= fullSystemNumKeys {
				return
			}
			if !fullSystemDeleted(i) {
				continue
			}
			if err := fullSystemDeleteWithRetry(m, fullSystemKey(i)); err != nil {
				select {
				case errCh <- fmt.Errorf("key %d: %w", i, err):
				default:
				}
				return
			}
			count.Add(1)
		}
	}

	wg.Add(fullSystemNumWriters)
	for w := 0; w < fullSystemNumWriters; w++ {
		go worker()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("delete phase: %v", err)
	}
	return int(count.Load())
}

// fullSystemPutWithRetry and fullSystemDeleteWithRetry retry on
// ErrNotLeader a small, bounded number of times -- defensive, not
// expected to ever actually retry in a stable single-node cluster after
// its one election, the same posture waitForLeader's own retry loop
// takes toward a transient hiccup rather than a real, ongoing problem.
func fullSystemPutWithRetry(m *Machine, key, value []byte) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = m.Put(key, value)
		if err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return err
}

func fullSystemDeleteWithRetry(m *Machine, key []byte) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = m.Delete(key)
		if err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return err
}

// fullSystemVerifyPhase checks every one of the one million key slots
// through GetLeaseRead (§14.3) -- the cheap read path, no log entry per
// call, the only tractable choice at this scale (a million safe/barrier
// reads would double the log for no benefit Get's own already-tested
// correctness needs). A concurrent worker pool again, sized the same
// small way the write phase's own pool is (fullSystemNumWriters' own
// doc) -- legibility if this times out, not a throughput claim.
func fullSystemVerifyPhase(t *testing.T, m *Machine) {
	t.Helper()
	var next atomic.Int64
	var mismatches atomic.Int64
	var wg sync.WaitGroup
	const maxLoggedMismatches = 20

	worker := func() {
		defer wg.Done()
		for {
			i := int(next.Add(1)) - 1
			if i >= fullSystemNumKeys {
				return
			}
			key := fullSystemKey(i)
			value, ok, leaseValid, err := m.GetLeaseRead(key)
			if err != nil || !leaseValid {
				if n := mismatches.Add(1); n <= maxLoggedMismatches {
					t.Errorf("GetLeaseRead(%q): ok=%v leaseValid=%v err=%v", key, ok, leaseValid, err)
				}
				continue
			}
			if fullSystemDeleted(i) {
				if ok {
					if n := mismatches.Add(1); n <= maxLoggedMismatches {
						t.Errorf("key %d (%q): ok = true, want false (deleted)", i, key)
					}
				}
				continue
			}
			want := fullSystemValue(i)
			if !ok || string(value) != string(want) {
				if n := mismatches.Add(1); n <= maxLoggedMismatches {
					t.Errorf("key %d (%q) = (%q, ok=%v), want (%q, true)", i, key, value, ok, want)
				}
			}
		}
	}

	wg.Add(fullSystemNumWriters)
	for w := 0; w < fullSystemNumWriters; w++ {
		go worker()
	}
	wg.Wait()

	if n := mismatches.Load(); n > 0 {
		t.Fatalf("%d key(s) out of %d did not verify correctly (first %d shown above)", n, fullSystemNumKeys, maxLoggedMismatches)
	}
}

// fullSystemSpotCheckSafeReads checks n keys, evenly spread across the
// key space, through the safe barrier read path (Get, not
// GetLeaseRead) -- confirming that path still agrees with the bulk
// lease-read verification above, without paying for a million barrier
// entries to check it everywhere.
func fullSystemSpotCheckSafeReads(t *testing.T, m *Machine, n int) {
	t.Helper()
	stride := fullSystemNumKeys / n
	if stride < 1 {
		stride = 1
	}
	for i := 0; i < fullSystemNumKeys; i += stride {
		key := fullSystemKey(i)
		value, ok, err := m.Get(key)
		if err != nil {
			t.Errorf("Get(%q): %v", key, err)
			continue
		}
		if fullSystemDeleted(i) {
			if ok {
				t.Errorf("Get(%q): ok = true, want false (deleted)", key)
			}
			continue
		}
		want := fullSystemValue(i)
		if !ok || string(value) != string(want) {
			t.Errorf("Get(%q) = (%q, ok=%v), want (%q, true)", key, value, ok, want)
		}
	}
}

// fullSystemReportManifest logs the current on-disk shape -- file
// counts per level and total bytes -- purely for the DESIGN.md write-up
// this test's own results feed; not a correctness check.
func fullSystemReportManifest(t *testing.T, dir, label string) {
	t.Helper()
	kvDir := filepath.Join(dir, "kv")
	var totalBytes int64
	var fileCount int
	err := filepath.Walk(kvDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		totalBytes += info.Size()
		fileCount++
		return nil
	})
	if err != nil {
		t.Logf("%s: could not walk %s: %v", label, kvDir, err)
		return
	}
	t.Logf("%s: %d files on disk, %d bytes total (%.1f MB)", label, fileCount, totalBytes, float64(totalBytes)/(1<<20))
}