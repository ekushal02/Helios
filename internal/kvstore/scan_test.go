package kvstore

import (
	"fmt"
	"testing"
	"time"
)

func TestScanReturnsAllKeysInRangeInSortedOrder(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	keys := []string{"c", "a", "e", "b", "d"} // written out of order
	for _, k := range keys {
		if err := m.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	// Bounded at "a", not an unbounded nil start: waitForLeader's own
	// probe write (key "__probe__") is a REAL, durable key in this
	// same Machine -- and "_" (0x5F) sorts before "a" (0x61), so an
	// unbounded scan would silently pick it up as an extra pair. That
	// probe key is real test infrastructure, not a bug to work around
	// in Scan itself; bounding this test's own range to what it
	// actually wrote is the correct fix, not a scan.go change.
	pairs, nextCursor, err := m.Scan([]byte("a"), nil, 10)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(nextCursor) != 0 {
		t.Errorf("nextCursor = %q, want empty (the whole range fit in one page)", nextCursor)
	}
	want := []string{"a", "b", "c", "d", "e"}
	if len(pairs) != len(want) {
		t.Fatalf("Scan returned %d pairs, want %d", len(pairs), len(want))
	}
	for i, k := range want {
		if string(pairs[i].Key) != k || string(pairs[i].Value) != "v-"+k {
			t.Errorf("pairs[%d] = (%q, %q), want (%q, %q)", i, pairs[i].Key, pairs[i].Value, k, "v-"+k)
		}
	}
}

func TestScanRespectsStartAndEndKeyBounds(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := m.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	// [b, d): inclusive start, exclusive end -- b and c, not a, d, or e.
	pairs, _, err := m.Scan([]byte("b"), []byte("d"), 10)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	got := keysOf(pairs)
	want := []string{"b", "c"}
	if !equalStrings(got, want) {
		t.Fatalf("Scan(b, d) keys = %v, want %v", got, want)
	}
}

func TestScanOnEmptyRangeReturnsNoPairs(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	for _, k := range []string{"a", "b", "c"} {
		if err := m.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	// end <= start: no key can ever satisfy both bounds -- this must
	// fall out of the ordinary comparison logic, not need its own
	// special-cased validation (scan.go's own doc on this).
	pairs, nextCursor, err := m.Scan([]byte("z"), []byte("a"), 10)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(pairs) != 0 || len(nextCursor) != 0 {
		t.Fatalf("Scan(z, a) = (%d pairs, cursor=%q), want (0, empty)", len(pairs), nextCursor)
	}
}

// TestScanExcludesDeletedKeys is Get's own tombstone contract, checked
// for Scan: a key Deleted after being Put must never appear in a range
// that would otherwise include it.
func TestScanExcludesDeletedKeys(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	for _, k := range []string{"a", "b", "c"} {
		if err := m.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := m.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete(b): %v", err)
	}

	// Bounded at "a" -- see TestScanReturnsAllKeysInRangeInSortedOrder's
	// own comment above on why an unbounded scan would pick up
	// waitForLeader's own "__probe__" key.
	pairs, _, err := m.Scan([]byte("a"), nil, 10)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	got := keysOf(pairs)
	want := []string{"a", "c"}
	if !equalStrings(got, want) {
		t.Fatalf("Scan after Delete(b) keys = %v, want %v (b must not appear)", got, want)
	}
}

// TestScanPaginatesAcrossMultipleCallsWithoutGapsOrDuplicates is the
// point of this whole task: a small limit forces several pages, each
// call's nextCursor feeding the next call's startKey exactly the way
// Server.Scan (and client.Client.Scan) will actually use it, and the
// pages assembled together must equal a single unlimited Scan exactly
// -- no key missing, none repeated, order preserved.
func TestScanPaginatesAcrossMultipleCallsWithoutGapsOrDuplicates(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	const count = 47 // deliberately not a multiple of the page size below
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if err := m.Put([]byte(key), []byte("v")); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	const pageSize = 5
	var paginated []KeyValue
	// Starts at "a", not nil -- see TestScanReturnsAllKeysInRangeInSortedOrder's
	// own comment on why an unbounded scan would pick up waitForLeader's
	// "__probe__" key. This test's own comparison (paginated vs. an
	// unlimited scan) would actually still pass either way, since both
	// sides would pick up the same extra key symmetrically -- but
	// leaving it unbounded means the exact page count asserted below
	// would only be correct by coincidence for this specific count and
	// pageSize, not as a matter of the test's own design.
	cursor := []byte("a")
	pages := 0
	for {
		pages++
		if pages > count { // generous ceiling against an infinite-loop bug
			t.Fatalf("did not converge after %d pages -- pagination is looping", pages)
		}
		pairs, next, err := m.Scan(cursor, nil, pageSize)
		if err != nil {
			t.Fatalf("Scan (page %d): %v", pages, err)
		}
		paginated = append(paginated, pairs...)
		if len(next) == 0 {
			break
		}
		cursor = next
	}

	full, fullCursor, err := m.Scan([]byte("a"), nil, count+1)
	if err != nil {
		t.Fatalf("Scan (unlimited): %v", err)
	}
	if len(fullCursor) != 0 {
		t.Fatalf("unlimited Scan's own nextCursor = %q, want empty", fullCursor)
	}

	if len(paginated) != len(full) {
		t.Fatalf("paginated Scan returned %d pairs across %d pages, want %d (matching a single unlimited Scan)",
			len(paginated), pages, len(full))
	}
	for i := range full {
		if string(paginated[i].Key) != string(full[i].Key) || string(paginated[i].Value) != string(full[i].Value) {
			t.Errorf("pairs[%d]: paginated = (%q, %q), unlimited = (%q, %q)",
				i, paginated[i].Key, paginated[i].Value, full[i].Key, full[i].Value)
		}
	}

	wantPages := (count + pageSize - 1) / pageSize
	if pages != wantPages {
		t.Errorf("took %d pages to exhaust %d keys at page size %d, want %d", pages, count, pageSize, wantPages)
	}
}

// TestScanMergesAcrossMemtableAndFlushedSSTables proves the merge is
// real, not just correct against an all-in-memtable dataset: force a
// flush partway through (the identical FlushThresholdBytes trick
// integration_test.go's own flush tests use), then Scan a range that
// spans keys now living in the flushed SSTable and keys still only in
// the fresh active memtable, plus an overwrite that must show the
// newer (memtable) value rather than the older (SSTable) one.
func TestScanMergesAcrossMemtableAndFlushedSSTables(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	opts := DefaultOptions
	opts.FlushThresholdBytes = 300
	m := newTestMachine(t, dir, n, opts)

	// Enough padded writes to force at least one flush -- b ends up in
	// the flushed SSTable, still holding its ORIGINAL value there.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d-padded-so-a-flush-actually-happens", i)
		if err := m.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// Overwrite one already-flushed key -- the NEW value lives only in
	// the fresh active memtable; the OLD one is still sitting in the
	// SSTable underneath it. A correct merge must return the new one.
	if err := m.Put([]byte("key-05"), []byte("overwritten-after-the-flush")); err != nil {
		t.Fatalf("overwrite Put: %v", err)
	}
	// A brand-new key, never flushed at all -- memtable only.
	if err := m.Put([]byte("key-99"), []byte("never-flushed")); err != nil {
		t.Fatalf("Put(key-99): %v", err)
	}

	// Bounded at "a" -- see TestScanReturnsAllKeysInRangeInSortedOrder's
	// own comment on why an unbounded scan would pick up
	// waitForLeader's "__probe__" key. "key-NN" sorts after "a", so
	// nothing this test actually wrote is excluded by this bound.
	pairs, _, err := m.Scan([]byte("a"), nil, 100)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	byKey := make(map[string]string, len(pairs))
	for _, p := range pairs {
		byKey[string(p.Key)] = string(p.Value)
	}
	if got := byKey["key-05"]; got != "overwritten-after-the-flush" {
		t.Errorf("key-05 = %q, want the overwritten value (the merge served a stale SSTable copy)", got)
	}
	if got := byKey["key-99"]; got != "never-flushed" {
		t.Errorf("key-99 = %q, want \"never-flushed\" (a memtable-only key was missed)", got)
	}
	if len(pairs) != 21 { // 20 originals + key-99, key-05 overwritten not duplicated
		t.Errorf("Scan returned %d pairs, want 21 (20 original keys plus key-99, key-05 counted once)", len(pairs))
	}
}

func TestScanLeaseReadAgreesWithScan(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	waitForLeader(t, n, 3*time.Second)
	m := newTestMachine(t, dir, n, DefaultOptions)

	for _, k := range []string{"a", "b", "c"} {
		if err := m.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	// Bounded at "a" -- see TestScanReturnsAllKeysInRangeInSortedOrder's
	// own comment on why an unbounded scan would pick up
	// waitForLeader's "__probe__" key.
	pairs, _, leaseValid, err := m.ScanLeaseRead([]byte("a"), nil, 10)
	if err != nil {
		t.Fatalf("ScanLeaseRead: %v", err)
	}
	if !leaseValid {
		t.Fatal("ScanLeaseRead: leaseValid = false, want true (a fresh single-node leader should have a valid lease)")
	}
	if got := keysOf(pairs); !equalStrings(got, []string{"a", "b", "c"}) {
		t.Fatalf("ScanLeaseRead keys = %v, want [a b c]", got)
	}
}

func keysOf(pairs []KeyValue) []string {
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = string(p.Key)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}