package kvstore

import (
	"testing"
	"time"

	"github.com/ekushal02/helios/internal/raft"
)

// TestPutIdempotentSuppressesADuplicateSequenceNumber is the mechanism,
// checked directly and airtight: the SECOND call uses a DIFFERENT value
// at the SAME (clientID, sequenceNumber). If dedup were not working,
// the key would end up holding "v2" -- the last write physically
// applied. Because it does, the key stays "v1": the second call was
// recognized as an already-applied duplicate and never touched storage
// at all, not merely "produced the same-looking result by coincidence."
func TestPutIdempotentSuppressesADuplicateSequenceNumber(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	const clientID = 42
	if err := m.PutIdempotent([]byte("a"), []byte("v1"), clientID, 1); err != nil {
		t.Fatalf("first PutIdempotent: %v", err)
	}
	if err := m.PutIdempotent([]byte("a"), []byte("v2"), clientID, 1); err != nil { // same seq, different value
		t.Fatalf("duplicate PutIdempotent: %v", err)
	}

	value, ok, err := m.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "v1" {
		t.Fatalf("Get(a) = (%q, ok=%v), want (\"v1\", true) -- the duplicate at seq=1 must not have overwritten the original", value, ok)
	}
}

// TestPutIdempotentAppliesAHigherSequenceNumber is the other half:
// dedup must not become a ratchet that blocks every FUTURE write from
// the same client, only ones at or below a sequence number already
// seen.
func TestPutIdempotentAppliesAHigherSequenceNumber(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	const clientID = 42
	if err := m.PutIdempotent([]byte("a"), []byte("v1"), clientID, 1); err != nil {
		t.Fatalf("PutIdempotent seq=1: %v", err)
	}
	if err := m.PutIdempotent([]byte("a"), []byte("v2"), clientID, 2); err != nil {
		t.Fatalf("PutIdempotent seq=2: %v", err)
	}

	value, ok, err := m.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "v2" {
		t.Fatalf("Get(a) = (%q, ok=%v), want (\"v2\", true) -- a genuinely new sequence number must still apply", value, ok)
	}
}

// TestDifferentClientIDsHaveIndependentDedupState confirms the dedup
// table is keyed by clientID, not shared: two different clients issuing
// the identical sequenceNumber must not be confused with each other.
func TestDifferentClientIDsHaveIndependentDedupState(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	if err := m.PutIdempotent([]byte("a"), []byte("from-client-1"), 1, 1); err != nil {
		t.Fatalf("PutIdempotent (client 1): %v", err)
	}
	if err := m.PutIdempotent([]byte("b"), []byte("from-client-2"), 2, 1); err != nil {
		t.Fatalf("PutIdempotent (client 2): %v", err)
	}

	for key, want := range map[string]string{"a": "from-client-1", "b": "from-client-2"} {
		value, ok, err := m.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !ok || string(value) != want {
			t.Errorf("Get(%q) = (%q, ok=%v), want (%q, true)", key, value, ok, want)
		}
	}
}

// TestPutIdempotentRejectsClientIDZero locks in the sentinel contract:
// 0 means "no session," never a real one, so a caller must not be
// allowed to accidentally land every non-idempotent write in the same
// dedup bucket by passing it here.
func TestPutIdempotentRejectsClientIDZero(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	if err := m.PutIdempotent([]byte("a"), []byte("v1"), 0, 1); err == nil {
		t.Fatal("PutIdempotent with clientID=0: err = nil, want an error")
	}
}

// TestPlainPutIsNeverDeduped is Put's own backward-compatibility
// contract, checked directly: two ordinary Put calls to the same key
// must both actually apply, exactly as they did before this task --
// clientID=0 must never collide with itself as if it were a real
// session.
func TestPlainPutIsNeverDeduped(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	if err := m.Put([]byte("a"), []byte("v1")); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := m.Put([]byte("a"), []byte("v2")); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	value, ok, err := m.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "v2" {
		t.Fatalf("Get(a) = (%q, ok=%v), want (\"v2\", true) -- an ordinary Put must never be treated as a duplicate", value, ok)
	}
}

// TestRetryDoesNotResurrectAConcurrentWritersNewerValue is the actual
// production bug this whole task exists to prevent, reproduced exactly:
// client A writes v1 and it commits. Client B then writes v2 to the
// SAME key -- a genuinely newer, different write. Client A's retry (as
// if its own original ack had been lost over the network, the real
// scenario client.Client's retry loop responds to) resends the
// IDENTICAL (clientID, sequenceNumber, key, v1) it sent the first time.
// Without dedup, that resend would silently overwrite B's newer v2 back
// to A's stale v1. With it, A's retry is recognized as already applied
// and is a no-op -- B's v2 survives.
func TestRetryDoesNotResurrectAConcurrentWritersNewerValue(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	const clientA, clientB = 1, 2

	if err := m.PutIdempotent([]byte("shared-key"), []byte("v1-from-A"), clientA, 1); err != nil {
		t.Fatalf("A's original write: %v", err)
	}
	if err := m.PutIdempotent([]byte("shared-key"), []byte("v2-from-B"), clientB, 1); err != nil {
		t.Fatalf("B's newer write: %v", err)
	}
	// A never learned its own write committed (its ack was "lost"), so
	// its client library retries with the SAME sequence number and the
	// SAME stale value it originally sent.
	if err := m.PutIdempotent([]byte("shared-key"), []byte("v1-from-A"), clientA, 1); err != nil {
		t.Fatalf("A's retry: %v", err)
	}

	value, ok, err := m.Get([]byte("shared-key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "v2-from-B" {
		t.Fatalf("Get(shared-key) = (%q, ok=%v), want (\"v2-from-B\", true) -- "+
			"A's retry must not have resurrected its own stale write over B's newer one", value, ok)
	}
}

// TestDedupTableSurvivesSnapshotInstall proves F-4's own harder half:
// the dedup entry for a write survives even after the log entry that
// originally carried it is compacted away and a fresh Machine is caught
// up purely from a snapshot image -- exactly the InstallSnapshot path a
// lagging follower or a restarting node takes, with no log replay at
// all standing in to rebuild the table the ordinary way.
func TestDedupTableSurvivesSnapshotInstall(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	const clientID = 7
	if err := m.PutIdempotent([]byte("k"), []byte("original"), clientID, 1); err != nil {
		t.Fatalf("PutIdempotent: %v", err)
	}

	blob, appliedIndex, err := m.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}

	dir2 := t.TempDir()
	n2 := newTestNode(t, dir2)
	waitForLeader(t, n2, 3*time.Second)
	m2 := newTestMachine(t, dir2, n2, DefaultOptions)

	// Stop n2 before installing directly, the identical race-avoidance
	// TestSnapshotTakeAndInstallRoundTrips (integration_test.go) already
	// documents in full: m2.run() must not still be consuming its own
	// real ApplyCh concurrently with a direct, out-of-band
	// installSnapshot call.
	n2.Stop()
	<-m2.done

	m2.installSnapshot(raft.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      blob,
		SnapshotIndex: appliedIndex,
		SnapshotTerm:  1,
	})

	// A "retry" of the exact same write, now arriving at m2 -- a
	// Machine that never itself processed the original log entry, only
	// the snapshot image built from a DIFFERENT Machine that did. If
	// the dedup table had not been captured in the image, m2 would have
	// no record of clientID 7's sequence 1 at all, and would incorrectly
	// apply this as a genuine new write.
	if err := m2.PutIdempotent([]byte("k"), []byte("resurrected-stale-value"), clientID, 1); err != nil {
		t.Fatalf("m2 PutIdempotent (should be deduped, not erred): %v", err)
	}

	value, ok, err := m2.readLocked([]byte("k"))
	if err != nil {
		t.Fatalf("readLocked: %v", err)
	}
	if !ok || string(value) != "original" {
		t.Fatalf("m2's key after the \"retry\" = (%q, ok=%v), want (\"original\", true) -- "+
			"the dedup table did not survive the snapshot install", value, ok)
	}
}
