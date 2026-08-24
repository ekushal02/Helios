package raft

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// What "recovery" actually costs
// =============================================================================
//
// A restart is three separate things, and they scale with different inputs:
//
//   1. OpenNode. Read and checksum the image, read and gob-decode the log tail,
//      reconcile the two floors. Bounded by the image.
//   2. Handing the image to the state machine. One channel send; the machine's
//      own rebuild is on top of this and is application-specific, so it is
//      deliberately excluded -- see recoveryMachine.
//   3. Replaying the tail. This does NOT happen on restart. commitIndex comes
//      back at the floor and everything above it is uncommitted until a leader
//      says otherwise, so the tail is loaded and then waits. The measurement
//      simulates that first message from the leader with commitTo.
//
// BYTES ARE MEASURED ALONGSIDE SECONDS, and may turn out to be the number that
// matters. encodeSnapshot builds a payload buffer and then an output buffer,
// both image-sized, on top of the caller's copy; decodeSnapshot copies the
// payload out again. Whether that is affordable at a gigabyte is exactly what
// this is for.
//
// WARM CACHE. The records are written moments before they are read, so the
// kernel still holds them. These are therefore best-case figures for the read
// itself; a genuinely cold restart adds a disk read of the same size. Defeating
// the page cache needs root on macOS, so the honest thing is to say so rather
// than to pretend.

const oneMB = 1 << 20

// recoveryCases is the shape of the experiment. The image is the variable; the
// tail is held constant except in the first row, which inverts that to show
// what a long log costs on its own.
//
// THE 1 GB ROW NEEDS ROOM. Building it peaks around three times the image and
// recovering it around twice, so a machine with less than about 8 GB free will
// swap and the timings will measure the swap. Drop the row rather than trusting
// a number taken under pressure.
var recoveryCases = []struct {
	name    string
	imageMB int
	tail    int
}{
	{"no image, 200k entry tail", 0, 200_000},
	{"1 MB image", 1, 2_000},
	{"100 MB image", 100, 2_000},
	{"1 GB image", 1024, 2_000},
}

// =============================================================================
// A consumer that does no work
// =============================================================================

// recoveryMachine counts what arrives and nothing else.
//
// A real state machine decodes the image and rebuilds itself, and at a gigabyte
// that cost would dominate every figure here. It is also entirely the
// application's, and Helios cannot make it faster. Excluding it makes these
// numbers measure the part Raft owns -- which is the part worth optimising.
type recoveryMachine struct {
	mu          sync.Mutex
	imageBytes  int
	imageIndex  int
	entries     int
	highest     int
	fault       string
	sawSnapshot bool

	done chan struct{}
}

func newRecoveryMachine() *recoveryMachine {
	return &recoveryMachine{done: make(chan struct{})}
}

func (m *recoveryMachine) run(n *Node) {
	defer close(m.done)

	for msg := range n.ApplyCh() {
		m.mu.Lock()
		switch {
		case msg.SnapshotValid:
			if m.sawSnapshot {
				m.fault = "a second image arrived where one was expected"
			}
			m.sawSnapshot = true
			m.imageBytes = len(msg.Snapshot)
			m.imageIndex = msg.SnapshotIndex
			m.highest = msg.SnapshotIndex
		default:
			if msg.CommandIndex != m.highest+1 {
				m.fault = fmt.Sprintf("index %d arrived after %d", msg.CommandIndex, m.highest)
			}
			m.entries++
			m.highest = msg.CommandIndex
		}
		m.mu.Unlock()
	}
}

func (m *recoveryMachine) snapshot() (highest, entries, imageBytes, imageIndex int, sawImage bool, fault string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.highest, m.entries, m.imageBytes, m.imageIndex, m.sawSnapshot, m.fault
}

func (m *recoveryMachine) waitFor(index int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if h, _, _, _, _, _ := m.snapshot(); h >= index {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// =============================================================================
// Building the state a restart finds
// =============================================================================

type writeCost struct {
	encode    time.Duration
	save      time.Duration
	allocated uint64
	blobBytes int
}

// buildDurableState writes exactly what a node that had compacted and then
// accepted `tail` more entries would leave behind: an image at some floor, and
// a state record whose log runs from that floor.
//
// Written directly rather than by driving a cluster, because the point is to
// control the sizes precisely and because a cluster would spend far longer
// producing a gigabyte than recovering one.
func buildDurableState(t *testing.T, store *FileStorage, imageMB, tail int) (floor, lastIndex int, w writeCost) {
	t.Helper()

	const term = 7

	// A plausible floor. Only its arithmetic matters, not its provenance.
	if imageMB > 0 {
		floor = 500_000
	}

	log := make([]LogEntry, 0, tail+1)
	log = append(log, LogEntry{Term: termOrSentinel(term, floor)}) // the floor entry
	for i := 1; i <= tail; i++ {
		log = append(log, LogEntry{
			Term:    term,
			Command: []byte(fmt.Sprintf("key%08d=value%08d", i, i)),
		})
	}
	lastIndex = floor + tail

	if imageMB > 0 {
		image := makeImage(imageMB * oneMB)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)

		start := time.Now()
		blob, err := encodeSnapshot(Snapshot{
			LastIncludedIndex: floor,
			LastIncludedTerm:  term,
			Data:              image,
		})
		w.encode = time.Since(start)
		if err != nil {
			t.Fatalf("encodeSnapshot: %v", err)
		}
		w.blobBytes = len(blob)

		runtime.ReadMemStats(&after)
		w.allocated = after.TotalAlloc - before.TotalAlloc

		// Released before the write so the peak is the encode, not the encode
		// plus a copy nobody needs any more.
		image = nil

		start = time.Now()
		if err := store.SaveSnapshot(blob); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		w.save = time.Since(start)

		blob = nil
	}

	state, err := encodeState(persistentState{
		CurrentTerm:       term,
		VotedFor:          1,
		Log:               log,
		LastIncludedIndex: floor,
	})
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	if err := store.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	log = nil
	state = nil
	runtime.GC()

	return floor, lastIndex, w
}

// termOrSentinel gives the floor entry its term. A node that never compacted
// has the original sentinel at term 0; one that did carries the term of the
// entry the image accounts for.
func termOrSentinel(term, floor int) int {
	if floor == 0 {
		return 0
	}
	return term
}

// makeImage fills by doubling rather than by looping over every byte, which at
// a gigabyte is the difference between a second of setup and several.
func makeImage(size int) []byte {
	image := make([]byte, size)
	if size == 0 {
		return image
	}
	for i := 0; i < 4096 && i < size; i++ {
		image[i] = byte(i * 31)
	}
	for filled := 4096; filled < size; filled *= 2 {
		copy(image[filled:], image[:filled])
	}
	return image
}

// =============================================================================
// The measurement
// =============================================================================

// TestRecoveryTime reports what a restart costs at each image size.
//
// A Test rather than a Benchmark for the reason the fsync measurement gives:
// the work is not repeatable at constant cost, and letting the framework scale
// an iteration count against a gigabyte produces a run nobody intended.
func TestRecoveryTime(t *testing.T) {
	// OPT-IN, AND NOT MERELY BECAUSE IT IS SLOW.
	//
	// This reports ALLOCATION, and the race detector adds shadow memory to
	// every allocation it sees -- so a figure taken in the default suite is not
	// a distorted version of the answer, it is a different quantity. Publishing
	// it would be worse than publishing nothing.
	//
	// The gigabyte row also peaks around three times the image while building
	// and twice while recovering, on top of whatever the rest of the suite is
	// already holding. Under that pressure the numbers measure the machine's
	// memory pressure rather than Helios, and the row can take minutes.
	//
	// So it runs when someone asks for numbers, not on every commit.
	if testing.Short() || os.Getenv("HELIOS_MEASURE") == "" {
		t.Skip("measurement, not an assertion: reads and writes up to a gigabyte " +
			"and reports allocation, which -race distorts. HELIOS_MEASURE=1 to " +
			"take fresh figures.")
	}

	for _, c := range recoveryCases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()

			store, err := NewFileStorage(dir)
			if err != nil {
				t.Fatalf("NewFileStorage: %v", err)
			}

			floor, lastIndex, w := buildDurableState(t, store, c.imageMB, c.tail)

			// A fresh handle, exactly as a restarting process builds.
			reopened, err := NewFileStorage(dir)
			if err != nil {
				t.Fatalf("NewFileStorage: %v", err)
			}

			// ---- phase 1: OpenNode ----------------------------------------
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)

			start := time.Now()
			n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, reopened)
			openTook := time.Since(start)

			runtime.ReadMemStats(&after)
			openAllocated := after.TotalAlloc - before.TotalAlloc
			heapAfterOpen := after.HeapAlloc

			if err != nil {
				t.Fatalf("OpenNode: %v", err)
			}
			defer n.Stop()

			n.mu.Lock()
			gotFloor := n.lastIncludedIndex
			gotLast := n.lastLogIndex()
			gotCommit := n.commitIndex
			n.mu.Unlock()

			if gotFloor != floor {
				t.Fatalf("floor = %d, want %d", gotFloor, floor)
			}
			if gotLast != lastIndex {
				t.Fatalf("lastLogIndex = %d, want %d", gotLast, lastIndex)
			}
			// The tail is loaded but NOT committed: commitIndex comes back at
			// the floor and a leader has to re-establish the rest.
			if gotCommit != floor {
				t.Fatalf("commitIndex = %d, want the floor at %d", gotCommit, floor)
			}

			// ---- phase 2: the image reaches the state machine --------------
			machine := newRecoveryMachine()
			go machine.run(n)

			imageStart := time.Now()
			var imageTook time.Duration
			if c.imageMB > 0 {
				if !machine.waitFor(floor, 5*time.Minute) {
					// WHICH FAILURE IS THIS, because the two want opposite
					// responses and a bare timeout cannot tell them apart.
					//
					// Still parked: the applier was never woken, which means
					// something advanced commitIndex without going through
					// commitTo. That is a bug, it reproduces at any size, and
					// OpenNode is where it hid last time.
					//
					// Already taken: the image was delivered and something
					// downstream is slow. At a gigabyte under memory pressure
					// that is a resource problem, not a defect.
					n.mu.Lock()
					parked := n.pendingSnapshot != nil
					commit, applied := n.commitIndex, n.lastApplied
					n.mu.Unlock()

					t.Fatalf("the image never reached the state machine in 5m: "+
						"still parked = %v, commitIndex %d, lastApplied %d, floor %d",
						parked, commit, applied, floor)
				}
				imageTook = time.Since(imageStart)
			}

			// ---- phase 3: the tail, once a leader says it may be applied ---
			//
			// commitTo is standing in for the first AppendEntries after the
			// restart. Nothing else in a restart makes the tail applicable.
			tailStart := time.Now()
			n.mu.Lock()
			n.commitTo(lastIndex)
			n.mu.Unlock()

			if !machine.waitFor(lastIndex, 5*time.Minute) {
				h, entries, _, _, _, _ := machine.snapshot()
				t.Fatalf("tail replay stalled at %d of %d (%d entries)", h, lastIndex, entries)
			}
			tailTook := time.Since(tailStart)

			highest, entries, imageBytes, imageIndex, sawImage, fault := machine.snapshot()
			if fault != "" {
				t.Fatalf("state machine: %s", fault)
			}
			if highest != lastIndex {
				t.Errorf("machine reached %d, want %d", highest, lastIndex)
			}
			if entries != c.tail {
				t.Errorf("machine applied %d entries, want %d", entries, c.tail)
			}
			if c.imageMB > 0 {
				if !sawImage {
					t.Error("no image reached the state machine")
				}
				if imageIndex != floor {
					t.Errorf("image index %d, want %d", imageIndex, floor)
				}
				if imageBytes != c.imageMB*oneMB {
					t.Errorf("image was %d bytes, want %d", imageBytes, c.imageMB*oneMB)
				}
			}

			// ---- report ---------------------------------------------------
			t.Logf("floor %d, tail %d entries, image %d MB", floor, c.tail, c.imageMB)
			if c.imageMB > 0 {
				t.Logf("  taking it   encode %v, save %v, %s allocated, blob %s",
					round(w.encode), round(w.save), mib(w.allocated), mib(uint64(w.blobBytes)))
			}
			t.Logf("  OpenNode    %v, %s allocated, %s heap after",
				round(openTook), mib(openAllocated), mib(heapAfterOpen))
			if c.imageMB > 0 {
				t.Logf("  image up    %v", round(imageTook))
				t.Logf("  throughput  %.0f MB/s through OpenNode",
					float64(c.imageMB)/openTook.Seconds())
			}
			t.Logf("  tail replay %v for %d entries (%.0f entries/s)",
				round(tailTook), c.tail, float64(c.tail)/tailTook.Seconds())
			t.Logf("  restart     %v to a usable node, %v to a caught-up state machine",
				round(openTook), round(openTook+imageTook+tailTook))
		})
	}
}

func round(d time.Duration) time.Duration {
	if d < time.Millisecond {
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Millisecond)
}

func mib(b uint64) string {
	return fmt.Sprintf("%.1f MB", float64(b)/float64(oneMB))
}
