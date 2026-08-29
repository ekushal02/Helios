package engine

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// fakeMemtableSource lets tests drive Reader's merge logic against exact,
// hand-built tiers rather than only against real Memtables -- particularly
// useful for the tombstone-shadowing cases, where building a real Memtable
// into a specific state is more code for no more confidence than a map.
type fakeMemtableSource map[string]fakeEntry

type fakeEntry struct {
	value     []byte
	tombstone bool
}

func (f fakeMemtableSource) Get(key []byte) (value []byte, tombstone bool, ok bool) {
	e, ok := f[string(key)]
	if !ok {
		return nil, false, false
	}
	return e.value, e.tombstone, true
}

// fakeSSTableSource is fakeMemtableSource's counterpart for the SSTable
// tier, with the one thing a real sstable.Reader has that a memtable
// never does: a Get that can fail.
type fakeSSTableSource struct {
	entries map[string]fakeEntry
	err     error // if set, every Get fails with this error regardless of key
}

func (f *fakeSSTableSource) Get(key []byte) (value []byte, tombstone bool, ok bool, err error) {
	if f.err != nil {
		return nil, false, false, f.err
	}
	e, ok := f.entries[string(key)]
	if !ok {
		return nil, false, false, nil
	}
	return e.value, e.tombstone, true, nil
}

func TestActiveMemtableTakesPriorityOverEverythingElse(t *testing.T) {
	active := fakeMemtableSource{"k": {value: []byte("active")}}
	immutable := fakeMemtableSource{"k": {value: []byte("immutable")}}
	sst := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("sstable")}}}

	r := &Reader{active: active, immutable: []memtableSource{immutable}, sstables: []sstableSource{sst}}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || !ok || string(value) != "active" {
		t.Fatalf("Get(k) = (%q, ok=%v, err=%v), want (\"active\", true, nil)", value, ok, err)
	}
}

func TestImmutableMemtableTakesPriorityOverSSTables(t *testing.T) {
	immutable := fakeMemtableSource{"k": {value: []byte("immutable")}}
	sst := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("sstable")}}}

	r := &Reader{immutable: []memtableSource{immutable}, sstables: []sstableSource{sst}}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || !ok || string(value) != "immutable" {
		t.Fatalf("Get(k) = (%q, ok=%v, err=%v), want (\"immutable\", true, nil)", value, ok, err)
	}
}

func TestNewestImmutableMemtableWinsOverOlder(t *testing.T) {
	newer := fakeMemtableSource{"k": {value: []byte("newer")}}
	older := fakeMemtableSource{"k": {value: []byte("older")}}

	// immutable is documented as newest-first -- newer must be index 0.
	r := &Reader{immutable: []memtableSource{newer, older}}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || !ok || string(value) != "newer" {
		t.Fatalf("Get(k) = (%q, ok=%v, err=%v), want (\"newer\", true, nil)", value, ok, err)
	}
}

func TestNewestSSTableWinsOverOlder(t *testing.T) {
	newer := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("newer")}}}
	older := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("older")}}}

	r := &Reader{sstables: []sstableSource{newer, older}}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || !ok || string(value) != "newer" {
		t.Fatalf("Get(k) = (%q, ok=%v, err=%v), want (\"newer\", true, nil)", value, ok, err)
	}
}

// TestTombstoneInActiveMemtableShadowsOlderValue is the read path's whole
// reason for existing, in miniature: a delete must win over every value
// sitting underneath it, in every tier, not just be treated as "nothing
// to report here, keep looking."
func TestTombstoneInActiveMemtableShadowsOlderValue(t *testing.T) {
	active := fakeMemtableSource{"k": {tombstone: true}}
	immutable := fakeMemtableSource{"k": {value: []byte("stale")}}
	sst := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("even-staler")}}}

	r := &Reader{active: active, immutable: []memtableSource{immutable}, sstables: []sstableSource{sst}}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || ok || value != nil {
		t.Fatalf("Get(k) = (%q, ok=%v, err=%v), want (nil, false, nil) -- the tombstone must shadow both older tiers", value, ok, err)
	}
}

func TestTombstoneInImmutableMemtableShadowsSSTable(t *testing.T) {
	immutable := fakeMemtableSource{"k": {tombstone: true}}
	sst := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("stale")}}}

	r := &Reader{immutable: []memtableSource{immutable}, sstables: []sstableSource{sst}}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || ok || value != nil {
		t.Fatalf("Get(k) = (%q, ok=%v, err=%v), want (nil, false, nil)", value, ok, err)
	}
}

func TestTombstoneInNewerSSTableShadowsOlderSSTable(t *testing.T) {
	newer := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {tombstone: true}}}
	older := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("stale")}}}

	r := &Reader{sstables: []sstableSource{newer, older}}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || ok || value != nil {
		t.Fatalf("Get(k) = (%q, ok=%v, err=%v), want (nil, false, nil)", value, ok, err)
	}
}

func TestKeyFoundOnlyInOldestSSTable(t *testing.T) {
	newer := &fakeSSTableSource{entries: map[string]fakeEntry{}}
	older := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("v")}}}

	r := &Reader{
		active:    fakeMemtableSource{},
		immutable: []memtableSource{fakeMemtableSource{}},
		sstables:  []sstableSource{newer, older},
	}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || !ok || string(value) != "v" {
		t.Fatalf("Get(k) = (%q, ok=%v, err=%v), want (\"v\", true, nil) -- must fall all the way through to the oldest SSTable", value, ok, err)
	}
}

func TestKeyNeverWrittenAnywhereReturnsNotFound(t *testing.T) {
	r := &Reader{
		active:    fakeMemtableSource{},
		immutable: []memtableSource{fakeMemtableSource{}},
		sstables:  []sstableSource{&fakeSSTableSource{entries: map[string]fakeEntry{}}},
	}
	value, ok, err := r.Get([]byte("nope"))
	if err != nil || ok || value != nil {
		t.Fatalf("Get(nope) = (%q, ok=%v, err=%v), want (nil, false, nil)", value, ok, err)
	}
}

// TestSSTableReadErrorHaltsSearchAndDoesNotFallThrough is the other half
// of the read path's core contract, alongside the tombstone tests above:
// an error from a newer SSTable must not be treated as "that tier had
// nothing to say," because it might have had the answer and simply
// failed to deliver it. Falling through to an older SSTable that HAS a
// (possibly stale) answer would silently return wrong data instead of
// surfacing the read failure.
func TestSSTableReadErrorHaltsSearchAndDoesNotFallThrough(t *testing.T) {
	wantErr := errors.New("simulated corrupt block")
	newer := &fakeSSTableSource{err: wantErr}
	older := &fakeSSTableSource{entries: map[string]fakeEntry{"k": {value: []byte("would-be-wrong-if-returned")}}}

	r := &Reader{sstables: []sstableSource{newer, older}}
	value, ok, err := r.Get([]byte("k"))
	if err == nil {
		t.Fatal("Get(k) with a failing newer SSTable: err = nil, want the propagated error")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Get(k) error = %v, want it to wrap %v", err, wantErr)
	}
	if ok || value != nil {
		t.Fatalf("Get(k) = (%q, ok=%v), want the zero value alongside the error, not the older SSTable's answer", value, ok)
	}
}

func TestNilActiveMemtableIsSkippedGracefully(t *testing.T) {
	immutable := fakeMemtableSource{"k": {value: []byte("v")}}
	r := &Reader{immutable: []memtableSource{immutable}}
	// active is the zero value (nil interface) here, standing in for
	// NewReader(nil, ...) -- see the dedicated NewReader test below for
	// the actual nil-pointer-through-NewReader path.
	value, ok, err := r.Get([]byte("k"))
	if err != nil || !ok || string(value) != "v" {
		t.Fatalf("Get(k) with a nil active tier = (%q, ok=%v, err=%v), want (\"v\", true, nil)", value, ok, err)
	}
}

func TestNewReaderWithNilActiveMemtableDoesNotPanic(t *testing.T) {
	// This is the case NewReader's own doc calls out: a nil
	// *memtable.Memtable passed as active must not become a non-nil
	// interface wrapping a nil pointer, which would panic the moment
	// Get called a method on it.
	r := NewReader(nil, nil, nil)
	value, ok, err := r.Get([]byte("anything"))
	if err != nil || ok || value != nil {
		t.Fatalf("Get on a completely empty Reader = (%q, ok=%v, err=%v), want (nil, false, nil)", value, ok, err)
	}
}

// TestNilElementsInImmutableAndSSTableTiersAreSkipped exercises the
// defensive nil checks inside Get's two loops -- guarded rather than
// assumed unreachable, on the same posture §8 takes toward Raft's own
// believed-impossible states, even though NewReader's own doc says a
// caller assembling these slices is expected not to include one.
func TestNilElementsInImmutableAndSSTableTiersAreSkipped(t *testing.T) {
	found := fakeMemtableSource{"k": {value: []byte("v")}}
	r := &Reader{
		immutable: []memtableSource{nil, found},
		sstables:  []sstableSource{nil},
	}
	value, ok, err := r.Get([]byte("k"))
	if err != nil || !ok || string(value) != "v" {
		t.Fatalf("Get(k) with a nil element ahead of the real one = (%q, ok=%v, err=%v), want (\"v\", true, nil)", value, ok, err)
	}

	// A key found nowhere, with nil entries in both tiers, must still
	// cleanly report not-found rather than panicking on the nil.
	value, ok, err = r.Get([]byte("missing"))
	if err != nil || ok || value != nil {
		t.Fatalf("Get(missing) = (%q, ok=%v, err=%v), want (nil, false, nil)", value, ok, err)
	}
}

// TestReadPathAcrossRealMemtablesAndSSTables exercises NewReader's actual
// wiring -- not just the interfaces the tests above drive directly -- by
// running the same tier-priority and tombstone-shadowing scenarios
// against real *memtable.Memtable and *sstable.Reader values, the way a
// caller in this codebase actually has them.
func TestReadPathAcrossRealMemtablesAndSSTables(t *testing.T) {
	active := memtable.NewWithSeed(1)
	active.Put([]byte("in-active"), []byte("active-value"))
	active.Delete([]byte("deleted-in-active")) // shadows a live value below

	frozen := memtable.NewWithSeed(2)
	frozen.Put([]byte("in-frozen"), []byte("frozen-value"))
	frozen.Put([]byte("deleted-in-active"), []byte("stale-value-in-frozen"))

	newerSST := buildSSTable(t, map[string]string{"in-newer-sstable": "newer-sstable-value"})
	olderSST := buildSSTable(t, map[string]string{
		"in-older-sstable": "older-sstable-value",
		"in-newer-sstable": "would-be-shadowed", // must lose to newerSST
	})

	r := NewReader(active, []*memtable.Memtable{frozen}, []*sstable.Reader{newerSST, olderSST})

	tests := []struct {
		key       string
		wantValue string
		wantOK    bool
	}{
		{"in-active", "active-value", true},
		{"in-frozen", "frozen-value", true},
		{"in-newer-sstable", "newer-sstable-value", true}, // newer must win over older
		{"in-older-sstable", "older-sstable-value", true},
		{"deleted-in-active", "", false}, // tombstone in active shadows frozen's stale copy
		{"never-written", "", false},
	}
	for _, tc := range tests {
		value, ok, err := r.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", tc.key, err)
		}
		if ok != tc.wantOK {
			t.Errorf("Get(%q): ok = %v, want %v", tc.key, ok, tc.wantOK)
			continue
		}
		if ok && string(value) != tc.wantValue {
			t.Errorf("Get(%q) = %q, want %q", tc.key, value, tc.wantValue)
		}
	}
}

// buildSSTable flushes a small memtable built from the given key/value
// pairs and opens it, for tests that need a real *sstable.Reader without
// each spelling out the flush/open boilerplate.
func buildSSTable(t *testing.T, kvs map[string]string) *sstable.Reader {
	t.Helper()
	m := memtable.NewWithSeed(1)
	for k, v := range kvs {
		m.Put([]byte(k), []byte(v))
	}
	path := filepath.Join(t.TempDir(), "test.sst")
	if _, err := sstable.Flush(m, path); err != nil {
		t.Fatalf("sstable.Flush: %v", err)
	}
	r, err := sstable.Open(path)
	if err != nil {
		t.Fatalf("sstable.Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}
