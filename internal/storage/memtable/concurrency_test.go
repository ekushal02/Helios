package memtable

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentReadsAreRaceFree is the baseline case this package exists
// to make cheap: many goroutines calling Get at once, against a Memtable
// nothing is writing to, taking no lock at all. Run with -race; a data
// race here would mean the "lock-free reads" claim in the package doc is
// false.
func TestConcurrentReadsAreRaceFree(t *testing.T) {
	const n = 2000
	m := NewWithSeed(1)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		m.Put([]byte(key), []byte(key))
	}

	const readers = 32
	var wg sync.WaitGroup
	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("key-%05d", i)
				value, tombstone, ok := m.Get([]byte(key))
				if !ok {
					t.Errorf("Get(%q): ok = false, want true", key)
					return
				}
				if tombstone {
					t.Errorf("Get(%q): tombstone = true, want false", key)
					return
				}
				if string(value) != key {
					t.Errorf("Get(%q) = %q, want %q", key, value, key)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentReadsDuringWrites is the scenario the memtable's whole
// design is for: one writer inserting new keys while many readers call
// Get, with no lock shared between them. Every Get must either report the
// key absent (the writer hasn't reached it yet) or report it present with
// its correct, complete value -- never present-with-wrong-value and never
// a panic from touching a half-linked node.
func TestConcurrentReadsDuringWrites(t *testing.T) {
	const n = 20_000
	m := NewWithSeed(2)

	keys := make([]string, n)
	values := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%06d", i)
		values[i] = fmt.Sprintf("value-%06d", i)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	const readers = 16
	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func(seed int) {
			defer wg.Done()
			// Walk the key space repeatedly in a different starting
			// position per goroutine, so different readers are likely
			// racing the writer at different points in the list.
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				idx := i % n
				value, tombstone, ok := m.Get([]byte(keys[idx]))
				if ok {
					if tombstone {
						t.Errorf("Get(%q): tombstone = true, want false; nothing is ever deleted in this test", keys[idx])
						return
					}
					if string(value) != values[idx] {
						t.Errorf("Get(%q) = %q, want %q (torn or wrong read)", keys[idx], value, values[idx])
						return
					}
				}
				i++
			}
		}(r * 997) // stagger starting points; 997 is just an odd stride
	}

	// The writer: a realistic single-writer insert loop, exactly the
	// usage this package is designed around.
	for i := 0; i < n; i++ {
		m.Put([]byte(keys[i]), []byte(values[i]))
	}
	close(stop)
	wg.Wait()

	if got := m.Len(); got != n {
		t.Fatalf("Len() after the writer finished = %d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		value, tombstone, ok := m.Get([]byte(keys[i]))
		if !ok || tombstone || string(value) != values[i] {
			t.Fatalf("final Get(%q) = (%q, %v, %v), want (%q, false, true)", keys[i], value, tombstone, ok, values[i])
		}
	}
}

// TestConcurrentReadsDuringUpdateDetectTornValue targets the update path
// specifically, not just insertion of new keys: many readers repeatedly
// Get the same key while one writer repeatedly overwrites its value, and
// every read must see one full value or the other, never a mix of bytes
// from both. Each value below is filled with a single repeated byte, so a
// torn read -- part of one value's bytes and part of another's -- shows up
// as a value whose bytes are not all equal, which no correct read can ever
// produce given entryValue is swapped as a single pointer.
func TestConcurrentReadsDuringUpdateDetectTornValue(t *testing.T) {
	m := NewWithSeed(3)
	key := []byte("hot-key")
	const valueSize = 256
	const writes = 20_000

	makeValue := func(b byte) []byte {
		v := make([]byte, valueSize)
		for i := range v {
			v[i] = b
		}
		return v
	}
	m.Put(key, makeValue(0))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	const readers = 16
	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				value, tombstone, ok := m.Get(key)
				if !ok {
					t.Errorf("Get(%q): ok = false; the key was written before any goroutine started", key)
					return
				}
				if tombstone {
					t.Errorf("Get(%q): tombstone = true; nothing deletes this key in this test", key)
					return
				}
				if len(value) != valueSize {
					t.Errorf("Get(%q): len(value) = %d, want %d", key, len(value), valueSize)
					return
				}
				want := value[0]
				for i, b := range value {
					if b != want {
						t.Errorf("Get(%q): torn value, byte %d = %d, want %d (all bytes should match)", key, i, b, want)
						return
					}
				}
			}
		}()
	}

	for i := 0; i < writes; i++ {
		m.Put(key, makeValue(byte(i)))
	}
	close(stop)
	wg.Wait()
}

// TestConcurrentIteratorDuringWrites confirms iteration is also safe to
// run alongside a writer: it must never panic and must never observe keys
// out of order, even though (as documented) it makes no snapshot-isolation
// promise about which keys it sees.
func TestConcurrentIteratorDuringWrites(t *testing.T) {
	const n = 10_000
	m := NewWithSeed(4)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	const iterators = 8
	wg.Add(iterators)
	for g := 0; g < iterators; g++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				it := m.NewIterator()
				var prev []byte
				for it.Next() {
					k := it.Key()
					if prev != nil && string(prev) >= string(k) {
						t.Errorf("iterator observed out-of-order keys: %q then %q", prev, k)
						return
					}
					prev = append([]byte(nil), k...)
					_ = it.Value()
					_ = it.Tombstone()
				}
			}
		}()
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		m.Put([]byte(key), []byte(key))
	}
	close(stop)
	wg.Wait()
}

// TestConcurrentWritersAreGuardedNotAssumed. Production only ever has one
// writer (the apply goroutine, per DESIGN.md §8's single-applier
// principle) driving Put/Delete. That is an assumption about how this
// type is used, not a property enforced by its API, so -- on the same
// "believed-impossible conditions are guarded, not assumed" reasoning
// DESIGN.md §8 applies to Raft's own believed-impossible states -- the
// internal mutex is checked here under concurrent writers it should never
// actually see in production, rather than left untested because it should
// never come up.
func TestConcurrentWritersAreGuardedNotAssumed(t *testing.T) {
	const writers = 16
	const perWriter = 2000
	m := NewWithSeed(5)

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%02d-k%05d", w, i)
				m.Put([]byte(key), []byte(key))
			}
		}(w)
	}
	wg.Wait()

	if got, want := m.Len(), writers*perWriter; got != want {
		t.Fatalf("Len() = %d, want %d (every writer's keys are disjoint, so nothing should collide)", got, want)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			key := fmt.Sprintf("w%02d-k%05d", w, i)
			value, tombstone, ok := m.Get([]byte(key))
			if !ok || tombstone || string(value) != key {
				t.Fatalf("Get(%q) = (%q, %v, %v), want (%q, false, true)", key, value, tombstone, ok, key)
			}
		}
	}
}