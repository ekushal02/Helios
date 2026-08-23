package raft

import (
	"bytes"
	"testing"
	"time"
)

// =============================================================================
// Fixtures
// =============================================================================

// compactionNode builds a leader holding `entries` real commands, nothing
// running, with everything applied so a snapshot is legal at any index.
func compactionNode(t *testing.T, term, entries int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm = term
	n.state = Leader
	for i := 1; i <= entries; i++ {
		n.log = append(n.log, LogEntry{Term: term, Command: []byte{byte(i)}})
	}
	n.commitIndex = entries
	n.lastApplied = entries
	n.initLeaderState()

	return n
}

// =============================================================================
// The seam
// =============================================================================

// Before any snapshot, the floor is the sentinel and every accessor must agree
// with the arithmetic the package used before this file existed.
func TestAccessorsMatchPositionBeforeAnySnapshot(t *testing.T) {
	n := compactionNode(t, 3, 5)

	n.mu.Lock()
	defer n.mu.Unlock()

	if got := n.firstLogIndex(); got != 1 {
		t.Errorf("firstLogIndex = %d, want 1", got)
	}
	if got := n.lastLogIndex(); got != 5 {
		t.Errorf("lastLogIndex = %d, want 5", got)
	}
	if got := n.logLength(); got != 5 {
		t.Errorf("logLength = %d, want 5", got)
	}
	if got := n.termAt(0); got != 0 {
		t.Errorf("termAt(0) = %d, want 0: the sentinel is the floor", got)
	}
	for i := 1; i <= 5; i++ {
		if got := n.termAt(i); got != 3 {
			t.Errorf("termAt(%d) = %d, want 3", i, got)
		}
		if got := n.entryAt(i).Command[0]; got != byte(i) {
			t.Errorf("entryAt(%d).Command = %d, want %d", i, got, i)
		}
	}
}

// After compaction the same indices must return the same answers, minus the
// ones discarded. This is the property the whole seam exists for.
func TestIndicesKeepTheirMeaningAcrossTheFloor(t *testing.T) {
	n := compactionNode(t, 3, 8)

	if err := n.Snapshot(5, []byte("image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if got := n.firstLogIndex(); got != 6 {
		t.Errorf("firstLogIndex = %d, want 6", got)
	}
	if got := n.lastLogIndex(); got != 8 {
		t.Errorf("lastLogIndex = %d, want 8: compaction must not change where the log ends", got)
	}
	if got := n.logLength(); got != 3 {
		t.Errorf("logLength = %d, want 3", got)
	}
	if got := len(n.log); got != 4 {
		t.Errorf("len(log) = %d, want 4 (floor plus three entries)", got)
	}

	// The floor answers for its own index and nothing below it.
	if got := n.termAt(5); got != 3 {
		t.Errorf("termAt(5) = %d, want 3: the floor keeps its term", got)
	}
	if n.hasEntryAt(5) {
		t.Error("hasEntryAt(5) true: the floor is a term, not an entry")
	}
	if !n.isBelowFloor(4) {
		t.Error("isBelowFloor(4) false after compacting through 5")
	}

	// Everything above the floor is untouched and still at its own index.
	for i := 6; i <= 8; i++ {
		if !n.hasEntryAt(i) {
			t.Fatalf("hasEntryAt(%d) false after compaction", i)
		}
		if got := n.entryAt(i).Command[0]; got != byte(i) {
			t.Errorf("entryAt(%d).Command = %d, want %d: the offset is wrong", i, got, i)
		}
	}
}

// Out of range fails closed. Returning 0 would be the dangerous answer: 0 is the
// sentinel's term, so a bug would read as agreement at the bottom of the log.
func TestTermAtFailsClosedOutOfRange(t *testing.T) {
	n := compactionNode(t, 3, 4)

	if err := n.Snapshot(2, []byte("image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, idx := range []int{0, 1, 99} {
		if got := n.termAt(idx); got != -1 {
			t.Errorf("termAt(%d) = %d, want -1: an out-of-range term must never "+
				"match a real one", idx, got)
		}
	}
}

// entriesFrom hands out a copy, because what it returns goes to a network
// goroutine while the log keeps changing.
func TestEntriesFromDoesNotAliasTheLog(t *testing.T) {
	n := compactionNode(t, 3, 4)

	n.mu.Lock()
	got := n.entriesFrom(2)
	n.log = n.log[:2] // a truncation, as rule 3 would do
	n.mu.Unlock()

	if len(got) != 3 {
		t.Fatalf("entriesFrom returned %d entries, want 3", len(got))
	}
	if got[0].Command[0] != 2 {
		t.Errorf("first entry = %d, want 2: the result was a window into the log",
			got[0].Command[0])
	}
}

// Compaction may never eat into itself. Everything at or below the floor is
// committed by definition, and no rule of Figure 2 removes a committed entry.
func TestTruncateRefusesToCutIntoTheFloor(t *testing.T) {
	n := compactionNode(t, 3, 6)

	if err := n.Snapshot(4, []byte("image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	before := len(n.log)
	n.truncateFrom(3) // below the floor
	n.truncateFrom(4) // the floor itself

	if len(n.log) != before {
		t.Errorf("log went from %d to %d entries: a truncation reached into the "+
			"snapshot floor", before, len(n.log))
	}
	if n.lastIncludedIndex != 4 {
		t.Errorf("floor moved to %d, want 4", n.lastIncludedIndex)
	}
}

// =============================================================================
// Taking a snapshot
// =============================================================================

func TestSnapshotWritesTheImageBeforeShorteningTheLog(t *testing.T) {
	store := newRecordingStorage()

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	n.mu.Lock()
	n.currentTerm = 4
	n.state = Leader
	for i := 1; i <= 10; i++ {
		n.log = append(n.log, LogEntry{Term: 4, Command: []byte{byte(i)}})
	}
	n.commitIndex, n.lastApplied = 10, 10
	n.initLeaderState()
	n.mu.Unlock()

	if err := n.Snapshot(7, []byte("kv image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// The image is on the storage.
	snap, found, err := loadSnapshot(store)
	if err != nil {
		t.Fatalf("loadSnapshot: %v", err)
	}
	if !found {
		t.Fatal("no snapshot was written")
	}
	if snap.LastIncludedIndex != 7 || snap.LastIncludedTerm != 4 {
		t.Errorf("floor on disk = (%d, %d), want (7, 4)",
			snap.LastIncludedIndex, snap.LastIncludedTerm)
	}
	if !bytes.Equal(snap.Data, []byte("kv image")) {
		t.Errorf("image = %q, want %q", snap.Data, "kv image")
	}

	// And the shortened log is on the storage too, not just in memory. Without
	// the persist at the end of Snapshot, a restart would rebuild the full log
	// beside a floor that says three of its entries are gone.
	ps, found, err := loadState(store)
	if err != nil {
		t.Fatalf("loadState: %v", err)
	}
	if !found {
		t.Fatal("no state record was written")
	}
	if len(ps.Log) != 4 {
		t.Errorf("durable log holds %d entries, want 4 (floor plus 8, 9, 10)", len(ps.Log))
	}
	if ps.Log[0].Term != 4 {
		t.Errorf("durable floor term = %d, want 4", ps.Log[0].Term)
	}
}

// A node comes back with the floor where it left it, and with the entries above
// it intact.
func TestCompactedStateSurvivesARestart(t *testing.T) {
	store := NewMemoryStorage()

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	n.mu.Lock()
	n.currentTerm = 6
	n.state = Leader
	for i := 1; i <= 9; i++ {
		n.log = append(n.log, LogEntry{Term: 6, Command: []byte{byte(i)}})
	}
	n.commitIndex, n.lastApplied = 9, 9
	n.initLeaderState()
	n.markDirty()
	n.persistIfDirty()
	n.mu.Unlock()

	if err := n.Snapshot(6, []byte("image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	n.Stop()

	restarted, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode after compaction: %v", err)
	}
	defer restarted.Stop()

	restarted.mu.Lock()
	defer restarted.mu.Unlock()

	idx, term := restarted.snapshotFloor()
	if idx != 6 || term != 6 {
		t.Errorf("floor = (%d, %d), want (6, 6)", idx, term)
	}
	if got := restarted.firstLogIndex(); got != 7 {
		t.Errorf("firstLogIndex = %d, want 7", got)
	}
	if got := restarted.lastLogIndex(); got != 9 {
		t.Errorf("lastLogIndex = %d, want 9", got)
	}
	for i := 7; i <= 9; i++ {
		if got := restarted.entryAt(i).Command[0]; got != byte(i) {
			t.Errorf("entryAt(%d) = %d after restart, want %d", i, got, i)
		}
	}
	// Everything the snapshot covers is committed and applied by definition.
	if restarted.commitIndex != 6 || restarted.lastApplied != 6 {
		t.Errorf("commit/applied = %d/%d, want 6/6",
			restarted.commitIndex, restarted.lastApplied)
	}
}

func TestSnapshotRefusesIndicesItCannotHonour(t *testing.T) {
	t.Run("above commitIndex", func(t *testing.T) {
		n := compactionNode(t, 3, 8)

		// Committed through 8, so 9 is an entry a future leader could still
		// overwrite. An image containing it would make that overwrite
		// unrecoverable.
		if err := n.Snapshot(9, []byte("image")); err == nil {
			t.Error("accepted a snapshot at index 9 with only 8 committed: the " +
				"image would cover an entry that can still be overwritten")
		}
	})

	// THE ONE-INDEX RACE, MADE DELIBERATE.
	//
	// The applier hands a message to the state machine and only then reacquires
	// n.mu to record the delivery, so a machine that snapshots the moment it
	// applies is routinely one index ahead of lastApplied. Refusing there would
	// reject a correct caller on stale bookkeeping. What must still hold is that
	// the index is committed, and that Raft's own lastApplied is dragged up to
	// the floor so the applier never tries to rebuild a discarded batch.
	t.Run("one ahead of lastApplied but committed", func(t *testing.T) {
		n := compactionNode(t, 3, 8)

		n.mu.Lock()
		n.lastApplied = 5 // the applier has not yet recorded index 6
		n.mu.Unlock()

		if err := n.Snapshot(6, []byte("image")); err != nil {
			t.Fatalf("refused a committed index the caller says it applied: %v", err)
		}

		n.mu.Lock()
		defer n.mu.Unlock()
		if n.lastIncludedIndex != 6 {
			t.Errorf("floor = %d, want 6", n.lastIncludedIndex)
		}
		if n.lastApplied != 6 {
			t.Errorf("lastApplied = %d, want 6: leaving it below the floor would "+
				"have the applier rebuild a batch from entries just discarded",
				n.lastApplied)
		}
	})

	t.Run("backwards", func(t *testing.T) {
		n := compactionNode(t, 3, 8)

		if err := n.Snapshot(6, []byte("first")); err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		// A second, staler image. A no-op rather than an error: the state
		// machine may be signalled twice and neither call is a fault.
		if err := n.Snapshot(3, []byte("stale")); err != nil {
			t.Errorf("a stale snapshot returned an error, want a no-op: %v", err)
		}

		n.mu.Lock()
		defer n.mu.Unlock()
		if n.lastIncludedIndex != 6 {
			t.Errorf("floor = %d, want 6: a stale image moved it backwards", n.lastIncludedIndex)
		}
	})
}

// =============================================================================
// The threshold
// =============================================================================

func TestTheThresholdSignalsOnceTheLogOutgrowsIt(t *testing.T) {
	n := compactionNode(t, 3, 0)
	n.SetSnapshotThreshold(5)

	n.mu.Lock()
	for i := 1; i <= 4; i++ {
		n.log = append(n.log, LogEntry{Term: 3, Command: []byte{byte(i)}})
	}
	n.commitIndex, n.lastApplied = 4, 4
	n.maybeSignalSnapshot()
	n.mu.Unlock()

	select {
	case <-n.SnapshotNotify():
		t.Fatal("signalled at 4 entries with a threshold of 5")
	case <-time.After(50 * time.Millisecond):
	}

	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 3, Command: []byte{5}})
	n.commitIndex, n.lastApplied = 5, 5
	n.maybeSignalSnapshot()
	n.mu.Unlock()

	select {
	case <-n.SnapshotNotify():
	case <-time.After(2 * time.Second):
		t.Fatal("no signal at 5 entries with a threshold of 5")
	}
}

// The threshold measures entries ABOVE THE FLOOR, not lastLogIndex. Measuring
// the index would signal forever after the first compaction, since the index
// keeps climbing while the log stays short.
func TestTheThresholdMeasuresTheLogNotTheIndex(t *testing.T) {
	n := compactionNode(t, 3, 20)
	n.SetSnapshotThreshold(10)

	if err := n.Snapshot(18, []byte("image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Drain anything the pre-compaction state left pending.
	select {
	case <-n.SnapshotNotify():
	default:
	}

	n.mu.Lock()
	n.maybeSignalSnapshot()
	n.mu.Unlock()

	select {
	case <-n.SnapshotNotify():
		t.Fatal("signalled with two entries above a floor at 18: the threshold " +
			"is reading lastLogIndex rather than the length of the log")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAZeroThresholdDisablesCompaction(t *testing.T) {
	n := compactionNode(t, 3, 50)
	n.SetSnapshotThreshold(0)

	n.mu.Lock()
	n.maybeSignalSnapshot()
	n.mu.Unlock()

	select {
	case <-n.SnapshotNotify():
		t.Fatal("signalled with the threshold disabled")
	case <-time.After(50 * time.Millisecond):
	}
}

// =============================================================================
// The gap this task opens
// =============================================================================

// SCOPE FENCE (InstallSnapshot).
//
// A follower whose nextIndex has fallen below the floor needs entries that no
// longer exist. There is nothing honest to send it, so buildAppendEntries
// refuses rather than shipping a message claiming a PrevLogIndex the follower
// has never reached, which it would reject forever.
//
// Refusing is correct but it is not a repair. That follower is stuck until
// InstallSnapshot exists. When it lands, this test should be inverted: the
// leader must send an image instead of nothing.
func TestALeaderRefusesToBuildBelowTheFloor(t *testing.T) {
	n := compactionNode(t, 5, 12)

	if err := n.Snapshot(9, []byte("image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// This follower is believed to hold nothing past index 3.
	n.nextIndex[1] = 4
	if args := n.buildAppendEntries(1, 5); args != nil {
		t.Errorf("built a message with PrevLogIndex %d for a follower below the "+
			"floor at %d: it can only be rejected", args.PrevLogIndex, n.lastIncludedIndex)
	}

	// A follower at the floor is still serviceable: the check at index 9 can be
	// answered from lastIncludedTerm, which is exactly why that field is stored.
	n.nextIndex[2] = 10
	args := n.buildAppendEntries(2, 5)
	if args == nil {
		t.Fatal("refused a follower sitting exactly at the floor, which the " +
			"floor's term can answer for")
	}
	if args.PrevLogIndex != 9 || args.PrevLogTerm != 5 {
		t.Errorf("prev = (%d, %d), want (9, 5) from the floor",
			args.PrevLogIndex, args.PrevLogTerm)
	}
	if len(args.Entries) != 3 {
		t.Errorf("carried %d entries, want 3 (10, 11, 12)", len(args.Entries))
	}
}
