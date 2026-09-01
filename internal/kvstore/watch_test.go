package kvstore

import (
	"testing"
	"time"
)

func TestWatchDeliversLiveEventsAfterSubscribing(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	// waitForLeader's own probe write was submitted directly to n
	// BEFORE this Machine existed to consume it -- it is still queued,
	// applied asynchronously by Machine.run() the moment that goroutine
	// gets scheduled, which races against this test subscribing to
	// Watch(0) immediately below. A Get forces a real read barrier
	// (n.ReadIndex + waitForApplied), which cannot return until every
	// earlier-indexed entry -- including the probe -- has already
	// applied, since Machine.run() is a single, strictly-ordered
	// consumer (§8's own reasoning, applied here). Get itself is a
	// read, never routed through applyCommand's write branch, so it
	// triggers no notify() of its own to worry about.
	if _, _, err := m.Get([]byte("__sync__")); err != nil {
		t.Fatalf("Get (sync past the probe): %v", err)
	}

	_, live, cancel, ok := m.Watch(0)
	if !ok {
		t.Fatal("Watch(0): ok = false, want true")
	}
	defer cancel()

	if err := m.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	select {
	case ev := <-live:
		if ev.Tombstone || string(ev.Key) != "a" || string(ev.Value) != "1" {
			t.Fatalf("event = %+v, want Put(a, 1)", ev)
		}
		if ev.Revision <= 0 {
			t.Errorf("event.Revision = %d, want > 0", ev.Revision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the live event")
	}
}

// TestWatchStartRevisionZeroNeverReplaysThePast is startRevision's own
// zero-value contract, checked directly: a write that already happened
// BEFORE Watch(0) was ever called must not appear on the live channel
// -- 0 means "from now," not "from the beginning."
func TestWatchStartRevisionZeroNeverReplaysThePast(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	if err := m.Put([]byte("before"), []byte("1")); err != nil {
		t.Fatalf("Put(before): %v", err)
	}

	_, live, cancel, ok := m.Watch(0)
	if !ok {
		t.Fatal("Watch(0): ok = false")
	}
	defer cancel()

	if err := m.Put([]byte("after"), []byte("2")); err != nil {
		t.Fatalf("Put(after): %v", err)
	}

	select {
	case ev := <-live:
		if string(ev.Key) != "after" {
			t.Fatalf("first delivered event = %q, want \"after\" (\"before\" must not have been replayed)", ev.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the live event")
	}
}

// TestWatchReplaysFromAPastRevisionThenContinuesLive is startRevision's
// other half: a positive value must replay everything at or after it
// from the retained history, then keep delivering new events live,
// with no gap and no duplicate at the boundary.
//
// b's own exact revision is captured from a FIRST watch's own
// delivered WatchEvent -- ground truth from the mechanism itself, not
// reconstructed from Machine.AppliedIndex() at some later point. An
// earlier version of this test read AppliedIndex() only after all
// three Puts (a, b, c) had already completed, which captures c's own
// commit point (or later, since a read barrier can itself advance it
// further) -- not b's. Watch(revB) with that inflated value then
// legitimately found nothing at or after it, since nothing had been
// written that far yet. Fixed by reading each event's own Revision
// field directly off the wire the mechanism actually delivers, the
// same "checked, not assumed" standard §8 states for Raft's own
// invariants.
func TestWatchReplaysFromAPastRevisionThenContinuesLive(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	// Sync past waitForLeader's own probe entry before subscribing --
	// see TestWatchDeliversLiveEventsAfterSubscribing's own comment.
	if _, _, err := m.Get([]byte("__sync__")); err != nil {
		t.Fatalf("Get (sync past the probe): %v", err)
	}

	_, live1, cancel1, ok := m.Watch(0)
	if !ok {
		t.Fatal("Watch(0): ok = false")
	}

	for _, k := range []string{"a", "b", "c"} {
		if err := m.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	var delivered []WatchEvent
	for i := 0; i < 3; i++ {
		select {
		case ev := <-live1:
			delivered = append(delivered, ev)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d/3 on the first watch", i+1)
		}
	}
	cancel1()

	if len(delivered) != 3 || string(delivered[0].Key) != "a" || string(delivered[1].Key) != "b" || string(delivered[2].Key) != "c" {
		t.Fatalf("first watch delivered %+v, want a, b, c in order", delivered)
	}
	revB := delivered[1].Revision

	// A second, independent watch, requesting replay from b's own
	// exact revision.
	replay, live2, cancel2, ok := m.Watch(revB)
	if !ok {
		t.Fatalf("Watch(%d): ok = false", revB)
	}
	defer cancel2()

	if len(replay) < 2 {
		t.Fatalf("replay = %+v, want at least b and c", replay)
	}
	if string(replay[0].Key) != "b" {
		t.Fatalf("replay[0].Key = %q, want \"b\"", replay[0].Key)
	}
	for _, ev := range replay {
		if string(ev.Key) == "a" {
			t.Errorf("replay includes %q, which committed before the requested start_revision", ev.Key)
		}
	}

	if err := m.Put([]byte("d"), []byte("v")); err != nil {
		t.Fatalf("Put(d): %v", err)
	}
	select {
	case ev := <-live2:
		if string(ev.Key) != "d" {
			t.Fatalf("live event = %q, want \"d\"", ev.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the live event following replay")
	}
}

// TestWatchReportsGapWhenStartRevisionHasBeenEvicted is the honest
// "resync required" case: a small retained-history capacity, enough
// writes to evict everything before a since-requested start_revision,
// and Watch must say so (ok=false) rather than silently start the
// watch with a hole in it.
func TestWatchReportsGapWhenStartRevisionHasBeenEvicted(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	opts := DefaultOptions
	opts.WatchHistoryCapacity = 5
	m := newTestMachine(t, dir, n, opts)

	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		if err := m.Put(key, []byte("v")); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// Revision 1 is certainly evicted by now -- WatchHistoryCapacity=5
	// against 20+ applied commands (the writes above plus
	// waitForLeader's own probe).
	_, _, _, ok := m.Watch(1)
	if ok {
		t.Fatal("Watch(1): ok = true, want false -- revision 1 should have been evicted from a 5-entry history")
	}
}

func TestWatchDistinguishesPutFromDelete(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	if err := m.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, live, cancel, ok := m.Watch(0)
	if !ok {
		t.Fatal("Watch(0): ok = false")
	}
	defer cancel()

	if err := m.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	select {
	case ev := <-live:
		if !ev.Tombstone {
			t.Fatal("event.Tombstone = false, want true (this was a Delete)")
		}
		if string(ev.Key) != "a" {
			t.Fatalf("event.Key = %q, want \"a\"", ev.Key)
		}
		if len(ev.Value) != 0 {
			t.Fatalf("event.Value = %q, want empty for a Delete", ev.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the delete event")
	}
}

// TestSlowWatcherIsDisconnectedRatherThanBlockingTheApplyPath is
// notify's own core correctness property, checked directly: a watcher
// that never drains its channel must not be able to stall applyCommand
// -- every OTHER write must keep succeeding promptly regardless.
func TestSlowWatcherIsDisconnectedRatherThanBlockingTheApplyPath(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	opts := DefaultOptions
	opts.WatchSubscriberBufferSize = 2 // small and deliberate: easy to overflow on purpose
	m := newTestMachine(t, dir, n, opts)

	_, live, cancel, ok := m.Watch(0)
	if !ok {
		t.Fatal("Watch(0): ok = false")
	}
	defer cancel()

	// Never drain `live` -- write enough to overflow its buffer several
	// times over. Every Put below must still return promptly; a
	// blocking notify would hang this test until it eventually times
	// out, which is exactly the failure mode being guarded against.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			key := []byte{byte('a' + (i % 26))}
			if err := m.Put(key, []byte("v")); err != nil {
				t.Errorf("Put(%d): %v", i, err)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("50 Puts did not complete within 5s -- a slow watcher appears to have blocked the apply path")
	}

	// The watcher should have been disconnected once its buffer
	// overflowed -- its channel closed, not silently still open and
	// backlogged.
	select {
	case _, chOk := <-live:
		if chOk {
			// Still delivering whatever fit before disconnection is
			// fine to drain past; keep reading until closure or a
			// reasonable bound.
			drained := 1
			for chOk && drained < 10 {
				_, chOk = <-live
				drained++
			}
			if chOk {
				t.Fatal("watcher's channel is still open after 50 unread events on a 2-slot buffer -- notify should have disconnected it")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("watcher's channel neither delivered nor closed")
	}
}

func TestWatchChannelClosesOnMachineClose(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	// Sync past waitForLeader's own probe entry before subscribing --
	// see TestWatchDeliversLiveEventsAfterSubscribing's own comment on
	// why. Without this, the probe can still be sitting in the live
	// channel's buffer when Close runs, and draining it below would be
	// misread as "the channel delivered a value after Close" when it
	// actually arrived before.
	if _, _, err := m.Get([]byte("__sync__")); err != nil {
		t.Fatalf("Get (sync past the probe): %v", err)
	}

	_, live, cancel, ok := m.Watch(0)
	if !ok {
		t.Fatal("Watch(0): ok = false")
	}
	defer cancel()

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case _, chOk := <-live:
		if chOk {
			t.Fatal("live channel delivered a value after Close instead of being closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live channel did not close within 2s of Machine.Close")
	}
}

// TestWatchReportsGapBelowTheSnapshotFloorEvenWithoutRingBufferEviction
// is the real bug a first draft of this mechanism actually had, fixed
// via watchState.markFloor: WatchHistoryCapacity here is deliberately
// large enough that ring-buffer eviction never triggers on its own --
// so before markFloor existed, a start_revision below a freshly-
// advanced snapshot floor would have silently returned ok=true with an
// INCOMPLETE replay (nothing below the floor, no indication anything
// was missing), rather than correctly detecting the gap. This test
// fails without the markFloor wiring in Machine.take, and passes with
// it.
func TestWatchReportsGapBelowTheSnapshotFloorEvenWithoutRingBufferEviction(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	opts := DefaultOptions
	opts.WatchHistoryCapacity = 10000 // far more than this test will ever write; eviction-by-capacity must not be what causes the result below
	m := newTestMachine(t, dir, n, opts)

	if err := m.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	appliedAtA := m.AppliedIndex()

	// take() is this package's own unexported snapshot-taking method
	// (read_snapshot.go) -- called directly, whitebox, rather than
	// waiting on SnapshotNotify's own timing, since this test needs
	// the floor to have advanced at a KNOWN point, deterministically.
	m.take()

	floor, _ := m.n.SnapshotFloor()
	if floor < appliedAtA {
		t.Fatalf("SnapshotFloor() = %d after take(), want >= %d (the index \"a\" committed at) -- take() did not actually advance the floor", floor, appliedAtA)
	}

	_, _, _, ok := m.Watch(1) // revision 1 is now at or below the floor
	if ok {
		t.Fatal("Watch(1): ok = true, want false -- revision 1 is at or below the snapshot floor and can never be completely replayed, " +
			"but the ring buffer (capacity 10000) never evicted anything on its own, so only markFloor catches this")
	}
}