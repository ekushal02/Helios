package compaction

import (
	"testing"

	"github.com/ekushal02/helios/internal/storage/manifest"
)

func filesOf(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "f.sst"
	}
	return out
}

func TestPickLevelReturnsFalseWhenNothingIsOverThreshold(t *testing.T) {
	m := &manifest.Manifest{Levels: [][]string{filesOf(2), filesOf(1), filesOf(0)}}
	if _, ok := PickLevel(m, 4); ok {
		t.Fatal("PickLevel: ok = true, want false -- no level exceeds the threshold")
	}
}

func TestPickLevelReturnsTheLowestOverThresholdLevel(t *testing.T) {
	m := &manifest.Manifest{Levels: [][]string{
		filesOf(1), // under threshold
		filesOf(9), // over
		filesOf(9), // also over, but level 1 must win
	}}
	level, ok := PickLevel(m, 4)
	if !ok {
		t.Fatal("PickLevel: ok = false, want true")
	}
	if level != 1 {
		t.Fatalf("PickLevel = %d, want 1 (the lowest over-threshold level)", level)
	}
}

func TestPickLevelThresholdIsStrictlyGreaterThan(t *testing.T) {
	m := &manifest.Manifest{Levels: [][]string{filesOf(4)}}
	if _, ok := PickLevel(m, 4); ok {
		t.Fatal("PickLevel with exactly maxFilesPerLevel files: ok = true, want false (threshold is exceeded, not met)")
	}
	m = &manifest.Manifest{Levels: [][]string{filesOf(5)}}
	if _, ok := PickLevel(m, 4); !ok {
		t.Fatal("PickLevel with one more than maxFilesPerLevel: ok = false, want true")
	}
}

func TestPickLevelOnAnEmptyManifest(t *testing.T) {
	m := manifest.New()
	if _, ok := PickLevel(m, 4); ok {
		t.Fatal("PickLevel on a brand-new, empty manifest: ok = true, want false")
	}
}
