package compaction

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ekushal02/helios/internal/storage/manifest"
)

func TestBackgroundCompactsAnAlreadyOverThresholdManifestOnStart(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	f1 := writeSSTable(t, dir, "1.sst", map[string]kv{"a": val("1")})
	f2 := writeSSTable(t, dir, "2.sst", map[string]kv{"b": val("2")})
	f3 := writeSSTable(t, dir, "3.sst", map[string]kv{"c": val("3")})
	if err := manifest.Save(manifestPath, &manifest.Manifest{Levels: [][]string{{f3, f2, f1}}}); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	// The interval is deliberately long -- an hour -- so the assertion
	// below is only satisfiable by the immediate start-of-day drain in
	// loop(), not by ever reaching a tick.
	bg := StartBackground(manifestPath, dir, Options{MaxFilesPerLevel: 2}, time.Hour)
	defer bg.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for bg.Cycles() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if bg.Cycles() == 0 {
		t.Fatal("background compactor never ran a cycle against an already-over-threshold manifest")
	}
	if err := bg.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}

	got, err := manifest.Load(manifestPath)
	if err != nil {
		t.Fatalf("manifest.Load: %v", err)
	}
	if len(got.Levels[0]) != 0 {
		t.Fatalf("L0 = %v, want empty after the background compaction", got.Levels[0])
	}
}

func TestBackgroundDoesNothingWhenNothingNeedsCompacting(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	f := writeSSTable(t, dir, "1.sst", map[string]kv{"a": val("1")})
	if err := manifest.Save(manifestPath, &manifest.Manifest{Levels: [][]string{{f}}}); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	bg := StartBackground(manifestPath, dir, Options{MaxFilesPerLevel: 4}, 2*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	bg.Stop()

	if bg.Cycles() != 0 {
		t.Fatalf("Cycles() = %d, want 0 -- one file is under the threshold", bg.Cycles())
	}
	if err := bg.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, f)); statErr != nil {
		t.Fatalf("the untouched file was disturbed: %v", statErr)
	}
}

func TestBackgroundCompactsRepeatedlyOnEachTick(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	if err := manifest.Save(manifestPath, manifest.New()); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	bg := StartBackground(manifestPath, dir, Options{MaxFilesPerLevel: 1}, 2*time.Millisecond)
	defer bg.Stop()

	// Seed a fresh over-threshold L0 three separate times, well after
	// Start, so each compaction can only be credited to the ticker, not
	// the one-time start-of-day drain.
	for round := 0; round < 3; round++ {
		m, err := manifest.Load(manifestPath)
		if err != nil {
			t.Fatalf("manifest.Load: %v", err)
		}
		a := writeSSTable(t, dir, seedName(round, "a"), map[string]kv{"a": val("1")})
		b := writeSSTable(t, dir, seedName(round, "b"), map[string]kv{"b": val("2")})
		m.Levels[0] = append(m.Levels[0], a, b)
		if err := manifest.Save(manifestPath, m); err != nil {
			t.Fatalf("manifest.Save: %v", err)
		}

		deadline := time.Now().Add(5 * time.Second)
		for bg.Cycles() <= round && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if bg.Cycles() <= round {
			t.Fatalf("round %d: Cycles() = %d, want > %d", round, bg.Cycles(), round)
		}
	}
	if err := bg.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func seedName(round int, suffix string) string {
	return strings.Join([]string{"seed", string(rune('0' + round)), suffix}, "-") + ".sst"
}

func TestBackgroundReportsAnErrorWithoutHangingTheLoop(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	// Reference files in the manifest that do not exist on disk --
	// CompactLevel's own Open call will fail.
	if err := manifest.Save(manifestPath, &manifest.Manifest{Levels: [][]string{{"missing.sst", "also-missing.sst"}}}); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	bg := StartBackground(manifestPath, dir, Options{MaxFilesPerLevel: 1}, 2*time.Millisecond)

	deadline := time.Now().Add(5 * time.Second)
	for bg.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := bg.Err(); err == nil {
		t.Fatal("Err() = nil, want the propagated open error")
	}

	// The loop must still be stoppable cleanly after an error.
	stopped := make(chan struct{})
	go func() {
		bg.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5s after a compaction error")
	}
}

func TestDrainOneCycleStopsAtTheSafetyCapRatherThanSpinningForever(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	f := writeSSTable(t, dir, "1.sst", map[string]kv{"a": val("1")})
	if err := manifest.Save(manifestPath, &manifest.Manifest{Levels: [][]string{{f}}}); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	// MaxFilesPerLevel: 0 is exactly the degenerate configuration
	// maxDrainCycles's doc describes: a level with even one file is
	// "over threshold" forever, one level deeper each cycle. This must
	// terminate -- within a bounded, small number of Run calls -- rather
	// than hang until the test's own global timeout.
	b := &Background{manifestPath: manifestPath, dir: dir, opts: Options{MaxFilesPerLevel: 0}}
	b.stopCh = make(chan struct{})
	done := make(chan struct{})
	go func() {
		b.drainOneCycle()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("drainOneCycle did not terminate within 10s against MaxFilesPerLevel: 0 -- the safety cap did not engage")
	}

	err := b.Err()
	if err == nil {
		t.Fatal("Err() = nil, want the safety-cap error")
	}
	if !strings.Contains(err.Error(), "safety cap") && !strings.Contains(err.Error(), "MaxFilesPerLevel") {
		t.Fatalf("Err() = %v, want it to reference the safety cap", err)
	}
	if b.Cycles() != maxDrainCycles {
		t.Fatalf("Cycles() = %d, want exactly %d (the cap)", b.Cycles(), maxDrainCycles)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	if err := manifest.Save(manifestPath, manifest.New()); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}
	bg := StartBackground(manifestPath, dir, Options{MaxFilesPerLevel: 4}, time.Millisecond)
	bg.Stop()
	bg.Stop() // must not panic or hang
}
