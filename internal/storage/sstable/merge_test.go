package sstable

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
)

// staticSource is a Source over a fixed, already-sorted slice of
// entries -- the simplest possible input to Merge, letting these tests
// state exact expected output without building real SSTables.
type staticSource struct {
	entries []blockEntry
	i       int
}

func (s *staticSource) Next() bool {
	if s.i >= len(s.entries) {
		return false
	}
	s.i++
	return true
}
func (s *staticSource) Key() []byte     { return s.entries[s.i-1].Key }
func (s *staticSource) Value() []byte   { return s.entries[s.i-1].Value }
func (s *staticSource) Tombstone() bool { return s.entries[s.i-1].Tombstone }
func (s *staticSource) Err() error      { return nil }

func put(key, value string) blockEntry {
	return blockEntry{Key: []byte(key), Value: []byte(value)}
}
func del(key string) blockEntry {
	return blockEntry{Key: []byte(key), Tombstone: true}
}

// drain pulls every entry out of a Source into a slice, for tests to
// assert against.
func drain(t *testing.T, src Source) []blockEntry {
	t.Helper()
	var out []blockEntry
	for src.Next() {
		out = append(out, blockEntry{
			Key:       append([]byte(nil), src.Key()...),
			Value:     append([]byte(nil), src.Value()...),
			Tombstone: src.Tombstone(),
		})
	}
	if err := src.Err(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	return out
}

func assertEntries(t *testing.T, got []blockEntry, want []blockEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d\ngot:  %+v\nwant: %+v", len(got), len(want), got, want)
	}
	for i := range want {
		g, w := got[i], want[i]
		if string(g.Key) != string(w.Key) {
			t.Fatalf("entry %d: key = %q, want %q", i, g.Key, w.Key)
		}
		if g.Tombstone != w.Tombstone {
			t.Fatalf("entry %d (%q): tombstone = %v, want %v", i, g.Key, g.Tombstone, w.Tombstone)
		}
		if !g.Tombstone && string(g.Value) != string(w.Value) {
			t.Fatalf("entry %d (%q): value = %q, want %q", i, g.Key, g.Value, w.Value)
		}
	}
}

func TestMergeOfDisjointSourcesInterleavesInKeyOrder(t *testing.T) {
	a := &staticSource{entries: []blockEntry{put("a", "1"), put("c", "3")}}
	b := &staticSource{entries: []blockEntry{put("b", "2"), put("d", "4")}}

	got := drain(t, Merge([]Source{a, b}, false))
	assertEntries(t, got, []blockEntry{put("a", "1"), put("b", "2"), put("c", "3"), put("d", "4")})
}

// TestMergeNewestSourceWinsOnDuplicateKey is the core correctness claim:
// sources are given newest-first, and on a collision the lowest-indexed
// source must win, matching engine.Reader's own convention exactly.
func TestMergeNewestSourceWinsOnDuplicateKey(t *testing.T) {
	newer := &staticSource{entries: []blockEntry{put("k", "newer-value")}}
	older := &staticSource{entries: []blockEntry{put("k", "older-value")}}

	got := drain(t, Merge([]Source{newer, older}, false))
	assertEntries(t, got, []blockEntry{put("k", "newer-value")})
}

// TestMergeCollapsesADuplicateKeyToOneEntry checks the flip side: a key
// present in every source must appear exactly once in the output, never
// once per source -- Write's ErrOutOfOrder guard would reject a repeated
// key, so this is load-bearing, not cosmetic.
func TestMergeCollapsesADuplicateKeyToOneEntry(t *testing.T) {
	a := &staticSource{entries: []blockEntry{put("k", "1")}}
	b := &staticSource{entries: []blockEntry{put("k", "2")}}
	c := &staticSource{entries: []blockEntry{put("k", "3")}}

	got := drain(t, Merge([]Source{a, b, c}, false))
	assertEntries(t, got, []blockEntry{put("k", "1")})
}

func TestMergeTombstoneWinsOverOlderValue(t *testing.T) {
	newer := &staticSource{entries: []blockEntry{del("k")}}
	older := &staticSource{entries: []blockEntry{put("k", "stale")}}

	got := drain(t, Merge([]Source{newer, older}, false))
	assertEntries(t, got, []blockEntry{del("k")})
}

// TestMergeKeepsTombstonesByDefault checks that a tombstone survives a
// merge unless the caller explicitly asks for it to be dropped -- the
// safe default, since keeping a tombstone is never incorrect, only
// occasionally wasteful, while dropping one incorrectly can resurrect
// stale data (§13.8).
func TestMergeKeepsTombstonesByDefault(t *testing.T) {
	a := &staticSource{entries: []blockEntry{del("gone")}}
	got := drain(t, Merge([]Source{a}, false))
	assertEntries(t, got, []blockEntry{del("gone")})
}

func TestMergeDropsTombstonesWhenAsked(t *testing.T) {
	a := &staticSource{entries: []blockEntry{put("keep", "v"), del("gone")}}
	got := drain(t, Merge([]Source{a}, true))
	assertEntries(t, got, []blockEntry{put("keep", "v")})
}

// TestMergeDroppingATombstoneDoesNotResurrectAnOlderValue is the case
// that makes dropTombstones dangerous if the caller passes it
// incorrectly: a tombstone shadowing a live value in an OLDER source,
// with dropTombstones=true, must still make the key disappear entirely
// -- not silently fall through to the older source's stale value once
// the tombstone itself is discarded. Merge resolves the winner BEFORE
// deciding whether to drop it, which is what this test pins down.
func TestMergeDroppingATombstoneDoesNotResurrectAnOlderValue(t *testing.T) {
	newer := &staticSource{entries: []blockEntry{del("k")}}
	older := &staticSource{entries: []blockEntry{put("k", "must-not-reappear")}}

	got := drain(t, Merge([]Source{newer, older}, true))
	assertEntries(t, got, nil) // the key must vanish entirely, not resurface as "must-not-reappear"
}

func TestMergeOfEmptySourcesProducesNothing(t *testing.T) {
	a := &staticSource{}
	b := &staticSource{}
	got := drain(t, Merge([]Source{a, b}, false))
	if len(got) != 0 {
		t.Fatalf("got %d entries from two empty sources, want 0", len(got))
	}
}

func TestMergeOfNoSourcesProducesNothing(t *testing.T) {
	got := drain(t, Merge(nil, false))
	if len(got) != 0 {
		t.Fatalf("got %d entries from zero sources, want 0", len(got))
	}
}

func TestMergeOfOneSourcePassesThroughUnchanged(t *testing.T) {
	a := &staticSource{entries: []blockEntry{put("a", "1"), put("b", "2"), del("c")}}
	got := drain(t, Merge([]Source{a}, false))
	assertEntries(t, got, []blockEntry{put("a", "1"), put("b", "2"), del("c")})
}

// TestMergeManySourcesWithOverlappingKeys is the realistic case: several
// sources, keys both disjoint and overlapping, tombstones mixed in,
// checked against a hand-computed expected result rather than just
// spot-checked.
func TestMergeManySourcesWithOverlappingKeys(t *testing.T) {
	// Newest first, matching the required calling convention.
	s0 := &staticSource{entries: []blockEntry{put("b", "s0-b"), del("d")}}
	s1 := &staticSource{entries: []blockEntry{put("a", "s1-a"), put("b", "s1-b-stale")}}
	s2 := &staticSource{entries: []blockEntry{put("c", "s2-c"), put("d", "s2-d-stale"), put("e", "s2-e")}}

	got := drain(t, Merge([]Source{s0, s1, s2}, false))
	assertEntries(t, got, []blockEntry{
		put("a", "s1-a"), // only in s1
		put("b", "s0-b"), // s0 (newer) wins over s1's stale copy
		put("c", "s2-c"), // only in s2
		del("d"),         // s0's tombstone wins over s2's stale copy
		put("e", "s2-e"), // only in s2
	})
}

// failingMergeSource fails partway through, the same shape
// failingSource in writer_test.go takes, for testing Merge's own
// error-propagation rather than Write's.
type failingMergeSource struct {
	entries []blockEntry
	i       int
	failAt  int
	err     error
}

func (s *failingMergeSource) Next() bool {
	if s.i >= s.failAt || s.i >= len(s.entries) {
		return false
	}
	s.i++
	return true
}
func (s *failingMergeSource) Key() []byte     { return s.entries[s.i-1].Key }
func (s *failingMergeSource) Value() []byte   { return s.entries[s.i-1].Value }
func (s *failingMergeSource) Tombstone() bool { return s.entries[s.i-1].Tombstone }
func (s *failingMergeSource) Err() error      { return s.err }

// TestMergeStopsAndPropagatesWhenASourceFails checks that one broken
// source ends the whole merge, even though other sources still have
// healthy data left -- see merger.Next's own comment for why continuing
// with the healthy sources would still produce a silently incomplete
// result.
func TestMergeStopsAndPropagatesWhenASourceFails(t *testing.T) {
	wantErr := errors.New("simulated read failure")
	healthy := &staticSource{entries: []blockEntry{put("a", "1"), put("z", "2")}}
	broken := &failingMergeSource{
		entries: []blockEntry{put("m", "ok")},
		failAt:  1, // yields "m" successfully, then fails on the next call
		err:     wantErr,
	}

	merged := Merge([]Source{healthy, broken}, false)
	var got []blockEntry
	for merged.Next() {
		got = append(got, blockEntry{Key: append([]byte(nil), merged.Key()...)})
	}
	if err := merged.Err(); !errors.Is(err, wantErr) {
		t.Fatalf("Err() = %v, want it to wrap %v", err, wantErr)
	}
	// "a" sorts before "m", so it should have been emitted before the
	// failure was ever encountered; "z" must not appear, since the merge
	// has to stop once broken fails, before ever reaching it.
	for _, e := range got {
		if string(e.Key) == "z" {
			t.Fatal("merge emitted an entry from after the point of failure")
		}
	}
}

// TestMergeErrDuringPrimeIsReportedOnFirstNext checks the edge case
// where a source fails on its very first call, during Merge's own
// construction (primeAll) rather than mid-iteration.
func TestMergeErrDuringPrimeIsReportedOnFirstNext(t *testing.T) {
	wantErr := errors.New("failed immediately")
	broken := &failingMergeSource{failAt: 0, err: wantErr}
	healthy := &staticSource{entries: []blockEntry{put("a", "1")}}

	merged := Merge([]Source{healthy, broken}, false)
	if merged.Next() {
		t.Fatal("Next() = true, want false immediately given a source that failed during priming")
	}
	if err := merged.Err(); !errors.Is(err, wantErr) {
		t.Fatalf("Err() = %v, want it to wrap %v", err, wantErr)
	}
}

// TestMergeThenWriteProducesAValidSSTable is the integration case: build
// two real SSTables with overlapping and tombstoned keys, merge their
// Iterators, hand the result straight to Write, and confirm the produced
// file reads back with exactly the merged answer -- proving Merge's
// output actually satisfies Write's own ErrOutOfOrder contract (strictly
// increasing keys) rather than only appearing to in these hand-built
// tests above.
func TestMergeThenWriteProducesAValidSSTable(t *testing.T) {
	newer := buildTestSSTable(t, map[string]testEntry{
		"b": {value: "newer-b"},
		"d": {tombstone: true},
	})
	older := buildTestSSTable(t, map[string]testEntry{
		"a": {value: "older-a"},
		"b": {value: "older-b-stale"},
		"c": {value: "older-c"},
		"d": {value: "older-d-stale"},
	})

	merged := Merge([]Source{newer.NewIterator(), older.NewIterator()}, false)
	path := filepath.Join(t.TempDir(), "compacted.sst")
	info, err := Write(merged, path)
	if err != nil {
		t.Fatalf("Write(merged): %v", err)
	}
	if info.Entries != 4 {
		t.Fatalf("info.Entries = %d, want 4 (a, b, c, d)", info.Entries)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open(compacted): %v", err)
	}
	defer r.Close()

	want := map[string]testEntry{
		"a": {value: "older-a"},
		"b": {value: "newer-b"}, // newer wins
		"c": {value: "older-c"},
		"d": {tombstone: true}, // newer's tombstone wins, older's value gone
	}
	for key, w := range want {
		value, tombstone, ok, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%q): ok = false, want true", key)
		}
		if tombstone != w.tombstone {
			t.Fatalf("Get(%q): tombstone = %v, want %v", key, tombstone, w.tombstone)
		}
		if !w.tombstone && string(value) != w.value {
			t.Fatalf("Get(%q) = %q, want %q", key, value, w.value)
		}
	}
}

type testEntry struct {
	value     string
	tombstone bool
}

// buildTestSSTable flushes a small memtable built from kvs (values, or a
// Delete for entries marked tombstone) and opens it as a Reader.
func buildTestSSTable(t *testing.T, kvs map[string]testEntry) *Reader {
	t.Helper()
	m := memtable.NewWithSeed(1)
	for k, e := range kvs {
		if e.tombstone {
			m.Delete([]byte(k))
		} else {
			m.Put([]byte(k), []byte(e.value))
		}
	}
	path := filepath.Join(t.TempDir(), "src.sst")
	if _, err := Flush(m, path); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}
