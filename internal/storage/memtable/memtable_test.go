package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestPutGet(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("b"), []byte("2"))
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("c"), []byte("3"))

	for _, tc := range []struct{ key, want string }{
		{"a", "1"}, {"b", "2"}, {"c", "3"},
	} {
		value, tombstone, ok := m.Get([]byte(tc.key))
		if !ok {
			t.Fatalf("Get(%q): ok = false, want true", tc.key)
		}
		if tombstone {
			t.Fatalf("Get(%q): tombstone = true, want false", tc.key)
		}
		if string(value) != tc.want {
			t.Fatalf("Get(%q) = %q, want %q", tc.key, value, tc.want)
		}
	}
}

func TestGetMissingKeyIsNotFoundNotTombstone(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))

	value, tombstone, ok := m.Get([]byte("nope"))
	if ok {
		t.Fatalf("Get on a never-written key: ok = true, want false")
	}
	if tombstone {
		t.Fatalf("Get on a never-written key: tombstone = true, want false")
	}
	if value != nil {
		t.Fatalf("Get on a never-written key: value = %q, want nil", value)
	}
}

func TestOverwriteUpdatesValueWithoutGrowingLength(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	if got := m.Len(); got != 1 {
		t.Fatalf("Len after first put = %d, want 1", got)
	}

	m.Put([]byte("a"), []byte("2"))
	if got := m.Len(); got != 1 {
		t.Fatalf("Len after overwrite = %d, want 1 (same key, not a new entry)", got)
	}

	value, tombstone, ok := m.Get([]byte("a"))
	if !ok || tombstone || string(value) != "2" {
		t.Fatalf("Get after overwrite = (%q, %v, %v), want (\"2\", false, true)", value, tombstone, ok)
	}
}

func TestDeleteIsATombstoneNotAnAbsence(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	m.Delete([]byte("a"))

	value, tombstone, ok := m.Get([]byte("a"))
	if !ok {
		t.Fatalf("Get after Delete: ok = false, want true (a tombstone is still a record)")
	}
	if !tombstone {
		t.Fatalf("Get after Delete: tombstone = false, want true")
	}
	if value != nil {
		t.Fatalf("Get after Delete: value = %q, want nil", value)
	}
}

func TestDeleteThenPutRevives(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	m.Delete([]byte("a"))
	m.Put([]byte("a"), []byte("2"))

	value, tombstone, ok := m.Get([]byte("a"))
	if !ok || tombstone || string(value) != "2" {
		t.Fatalf("Get after delete-then-put = (%q, %v, %v), want (\"2\", false, true)", value, tombstone, ok)
	}
}

func TestPutCopiesKeyAndValue(t *testing.T) {
	m := NewWithSeed(1)
	key := []byte("a")
	value := []byte("1")
	m.Put(key, value)

	// Mutate the caller's buffers after Put returns; the stored entry
	// must be unaffected.
	key[0] = 'z'
	value[0] = '9'

	got, _, ok := m.Get([]byte("a"))
	if !ok || string(got) != "1" {
		t.Fatalf("Get(\"a\") after mutating caller buffers = (%q, %v), want (\"1\", true)", got, ok)
	}
	if _, _, ok := m.Get([]byte("z")); ok {
		t.Fatalf("Get(\"z\") = true; Put must have copied the key, not aliased it")
	}
}

func TestIteratorVisitsEveryKeyInSortedOrder(t *testing.T) {
	const n = 5000
	m := NewWithSeed(42)
	reference := make(map[string]string, n)

	rng := rand.New(rand.NewSource(7))
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", rng.Intn(1_000_000))
		value := fmt.Sprintf("value-%d", i)
		m.Put([]byte(key), []byte(value))
		reference[key] = value
		keys = append(keys, key)
	}

	// De-duplicate and sort the reference the same way the skip list
	// must have: distinct keys, ascending.
	dedup := make(map[string]bool, len(keys))
	var wantKeys []string
	for _, k := range keys {
		if !dedup[k] {
			dedup[k] = true
			wantKeys = append(wantKeys, k)
		}
	}
	sort.Strings(wantKeys)

	if got := m.Len(); got != len(wantKeys) {
		t.Fatalf("Len() = %d, want %d distinct keys", got, len(wantKeys))
	}

	var gotKeys []string
	it := m.NewIterator()
	var prev []byte
	for it.Next() {
		k := it.Key()
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Fatalf("iterator out of order: %q then %q", prev, k)
		}
		prev = append([]byte(nil), k...)
		gotKeys = append(gotKeys, string(k))

		wantValue := reference[string(k)]
		if got := string(it.Value()); got != wantValue {
			t.Fatalf("iterator value for %q = %q, want %q", k, got, wantValue)
		}
		if it.Tombstone() {
			t.Fatalf("iterator reported %q as a tombstone; nothing was deleted", k)
		}
	}

	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("iterator visited %d keys, want %d", len(gotKeys), len(wantKeys))
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("key %d = %q, want %q", i, gotKeys[i], wantKeys[i])
		}
	}
}

func TestIteratorSkipsNothingIncludingTombstones(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Delete([]byte("a"))

	var gotKeys []string
	var tombstoned map[string]bool = map[string]bool{}
	it := m.NewIterator()
	for it.Next() {
		gotKeys = append(gotKeys, string(it.Key()))
		if it.Tombstone() {
			tombstoned[string(it.Key())] = true
		}
	}
	if len(gotKeys) != 2 {
		t.Fatalf("iterator visited %v, want 2 entries (a tombstone is still an entry)", gotKeys)
	}
	if !tombstoned["a"] {
		t.Fatalf("iterator did not report %q as a tombstone", "a")
	}
	if tombstoned["b"] {
		t.Fatalf("iterator reported %q as a tombstone; it was never deleted", "b")
	}
}

func TestRandomLevelStaysInBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	seenAboveOne := false
	for i := 0; i < 100_000; i++ {
		level := randomLevel(rng)
		if level < 1 || level > maxLevel {
			t.Fatalf("randomLevel() = %d, want a value in [1, %d]", level, maxLevel)
		}
		if level > 1 {
			seenAboveOne = true
		}
	}
	if !seenAboveOne {
		t.Fatalf("randomLevel() returned 1 every time across 100,000 draws; the coin flip looks broken")
	}
}

func TestEmptyMemtable(t *testing.T) {
	m := New()
	if got := m.Len(); got != 0 {
		t.Fatalf("Len() on empty Memtable = %d, want 0", got)
	}
	if _, _, ok := m.Get([]byte("anything")); ok {
		t.Fatalf("Get on empty Memtable: ok = true, want false")
	}
	if m.NewIterator().Next() {
		t.Fatalf("iterator on empty Memtable found an entry")
	}
}