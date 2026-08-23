package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Building the three policies
// =============================================================================

// A policy under test, plus how to construct and tear it down.
type policyCase struct {
	name  string
	build func(tb testing.TB, dir string) (Storage, func())
}

var policyCases = []policyCase{
	{"always", func(tb testing.TB, dir string) (Storage, func()) {
		s, err := NewFileStorageWithPolicy(dir, SyncAlways)
		if err != nil {
			tb.Fatalf("NewFileStorageWithPolicy: %v", err)
		}
		return s, func() {}
	}},
	{"batch", func(tb testing.TB, dir string) (Storage, func()) {
		base, err := NewFileStorageWithPolicy(dir, SyncAlways)
		if err != nil {
			tb.Fatalf("NewFileStorageWithPolicy: %v", err)
		}
		bs := newBatchedStorage(base)
		return bs, bs.Close
	}},
	{"never", func(tb testing.TB, dir string) (Storage, func()) {
		s, err := NewFileStorageWithPolicy(dir, SyncNever)
		if err != nil {
			tb.Fatalf("NewFileStorageWithPolicy: %v", err)
		}
		return s, func() {}
	}},
}

// benchBlob builds a record the size a real node would be writing: a 256-entry
// log of small commands. Size is FIXED across the whole benchmark so the
// comparison isolates the flush rather than measuring the O(n) whole-log
// rewrite, which is a separate cost with a separate fix.
func benchBlob(tb testing.TB, entries int) []byte {
	tb.Helper()

	log := make([]LogEntry, entries+1)
	log[0] = LogEntry{Term: 0}
	for i := 1; i <= entries; i++ {
		log[i] = LogEntry{Term: 7, Command: []byte(fmt.Sprintf("put key%06d value%06d", i, i))}
	}
	blob, err := encodeState(persistentState{CurrentTerm: 7, VotedFor: 1, Log: log})
	if err != nil {
		tb.Fatalf("encodeState: %v", err)
	}
	return blob
}

// =============================================================================
// Contract tests
// =============================================================================
//
// The batched storage is only interesting if it still keeps Save's promise.
// These run before any number from the benchmark means anything.

func TestBatchedStorageIsDurableOnReturn(t *testing.T) {
	base, err := NewFileStorageWithPolicy(t.TempDir(), SyncAlways)
	if err != nil {
		t.Fatalf("NewFileStorageWithPolicy: %v", err)
	}
	s := newBatchedStorage(base)
	defer s.Close()

	for i := 0; i < 20; i++ {
		want := []byte(fmt.Sprintf("record-%02d", i))
		if err := s.Save(want); err != nil {
			t.Fatalf("Save %d: %v", i, err)
		}

		// Read through the BASE, not the wrapper: the question is whether the
		// bytes reached the storage underneath by the time Save returned.
		got, err := base.Load()
		if err != nil {
			t.Fatalf("Load %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("after Save %d the disk holds %q, want %q: Save returned "+
				"before the record was written", i, got, want)
		}
	}
}

// The whole point of the wrapper: many callers, few trips to the disk, and
// every caller still blocked until its state was durable.
func TestBatchedStorageCoalescesConcurrentSaves(t *testing.T) {
	base, err := NewFileStorageWithPolicy(t.TempDir(), SyncAlways)
	if err != nil {
		t.Fatalf("NewFileStorageWithPolicy: %v", err)
	}
	s := newBatchedStorage(base)
	defer s.Close()

	const writers = 64
	blob := benchBlob(t, 64)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Save(blob); err != nil {
				t.Errorf("Save: %v", err)
			}
		}()
	}
	wg.Wait()

	saves, flushes := s.stats()
	if saves != writers {
		t.Errorf("saves = %d, want %d", saves, writers)
	}
	if flushes >= saves {
		t.Errorf("%d saves cost %d flushes: nothing coalesced, so the wrapper "+
			"is adding a goroutine hop for no benefit", saves, flushes)
	}
	t.Logf("%d concurrent saves served by %d flushes", saves, flushes)
}

// SyncNever gives up durability and NOTHING else. In particular it still writes
// through a temp file and a rename, so a reader never sees a half-written
// record and no temp file is left to be mistaken for one.
func TestSyncNeverIsStillAtomic(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorageWithPolicy(dir, SyncNever)
	if err != nil {
		t.Fatalf("NewFileStorageWithPolicy: %v", err)
	}

	if err := s.Save([]byte("first")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save([]byte("second")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Load = %q, want %q", got, "second")
	}
	if _, err := os.Stat(filepath.Join(dir, tempFileName)); !os.IsNotExist(err) {
		t.Error("temp file survived a Save under SyncNever")
	}
	if s.Policy() != SyncNever {
		t.Errorf("Policy() = %v, want never", s.Policy())
	}
}

// =============================================================================
// The disk-level measurement
// =============================================================================

// BenchmarkStorageSave measures the storage layer with concurrency SUPPLIED BY
// THE BENCHMARK rather than by Raft.
//
// That is the only way to see what group commit is worth, because the node
// cannot currently produce concurrent writes: persistIfDirty runs under n.mu,
// so a node has at most one Save outstanding. These numbers are the ceiling the
// lock is denying, not the throughput the node has today. For that, see
// TestFsyncPolicyOnTheWritePath.
//
// The writers=1 row is the honest apples-to-apples comparison of the three
// policies; the wider rows show how each scales when callers pile up.
func BenchmarkStorageSave(b *testing.B) {
	blob := benchBlob(b, 256)
	b.Logf("record size %d bytes", len(blob))

	for _, pc := range policyCases {
		for _, writers := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("%s/writers=%d", pc.name, writers), func(b *testing.B) {
				store, cleanup := pc.build(b, b.TempDir())
				defer cleanup()

				b.SetBytes(int64(len(blob)))
				b.ResetTimer()
				start := time.Now()

				var wg sync.WaitGroup
				for w := 0; w < writers; w++ {
					share := b.N / writers
					if w < b.N%writers {
						share++
					}
					wg.Add(1)
					go func(n int) {
						defer wg.Done()
						for i := 0; i < n; i++ {
							if err := store.Save(blob); err != nil {
								b.Errorf("Save: %v", err)
								return
							}
						}
					}(share)
				}
				wg.Wait()

				elapsed := time.Since(start)
				b.StopTimer()

				b.ReportMetric(float64(b.N)/elapsed.Seconds(), "writes/s")
				if bs, ok := store.(*batchedStorage); ok {
					saves, flushes := bs.stats()
					if saves > 0 {
						b.ReportMetric(float64(flushes)/float64(saves), "flush/write")
					}
				}
			})
		}
	}
}

// =============================================================================
// The write-path measurement
// =============================================================================

// TestFsyncPolicyOnTheWritePath drives Submit on a real single-node leader and
// reports how many commands a second each policy sustains.
//
// A Test rather than a Benchmark on purpose. The log grows by one entry per
// submission and every persist rewrites the whole thing, so the cost per write
// climbs as the run goes on. Under go test -bench that is a trap: the framework
// raises b.N based on the fast early iterations and then runs a length of
// benchmark nobody intended. A fixed amount of work keeps the three policies
// comparable and keeps the run bounded.
//
// The number therefore includes the O(n) rewrite as well as the flush. That is
// realistic for the code as it stands today, and the gap between this and
// BenchmarkStorageSave is itself the finding.
func TestFsyncPolicyOnTheWritePath(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement, not an assertion; runs in the full suite")
	}

	const (
		commands = 1000
		clients  = 64
	)

	for _, pc := range policyCases {
		t.Run(pc.name, func(t *testing.T) {
			store, cleanup := pc.build(t, t.TempDir())
			defer cleanup()

			n, err := OpenNode(0, nil, newStubTransport(nil), 1, store)
			if err != nil {
				t.Fatalf("OpenNode: %v", err)
			}
			defer n.Stop()

			// Somebody has to drain, or the applier parks on its first send and
			// the run measures a node with a dead state machine.
			go func() {
				for range n.ApplyCh() {
				}
			}()

			n.Start()

			deadline := time.Now().Add(3 * time.Second)
			for {
				if st, _, _ := n.snapshotState(); st == Leader {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("a sole node never became leader")
				}
				time.Sleep(2 * time.Millisecond)
			}

			cmd := []byte("put key000001 value000001")

			start := time.Now()
			var wg sync.WaitGroup
			for c := 0; c < clients; c++ {
				share := commands / clients
				if c < commands%clients {
					share++
				}
				wg.Add(1)
				go func(k int) {
					defer wg.Done()
					for i := 0; i < k; i++ {
						if _, _, ok := n.Submit(cmd); !ok {
							t.Errorf("leader stopped accepting commands")
							return
						}
					}
				}(share)
			}
			wg.Wait()
			elapsed := time.Since(start)

			n.mu.Lock()
			entries := n.lastLogIndex()
			n.mu.Unlock()

			t.Logf("%s: %d commands from %d clients in %v -- %.0f commands/s, "+
				"%.3f ms/command, final log %d entries",
				pc.name, commands, clients, elapsed.Round(time.Millisecond),
				float64(commands)/elapsed.Seconds(),
				float64(elapsed.Microseconds())/float64(commands)/1000,
				entries)

			if bs, ok := store.(*batchedStorage); ok {
				saves, flushes := bs.stats()
				t.Logf("%s: %d saves served by %d flushes (%.2f flush/write)",
					pc.name, saves, flushes, float64(flushes)/float64(saves))
			}
		})
	}
}
