package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOnAMissingFileReturnsAnEmptyManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	m, err := Load(path)
	if err != nil {
		t.Fatalf("Load on a missing file: %v", err)
	}
	if len(m.Levels) != 1 || len(m.Levels[0]) != 0 {
		t.Fatalf("Load on a missing file = %+v, want one empty level", m)
	}
}

func TestSaveThenLoadRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	m := &Manifest{Levels: [][]string{
		{"l0-b.sst", "l0-a.sst"},
		{"l1-x.sst"},
		{},
	}}
	if err := Save(path, m); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Levels) != len(m.Levels) {
		t.Fatalf("Load got %d levels, want %d", len(got.Levels), len(m.Levels))
	}
	for i := range m.Levels {
		if len(got.Levels[i]) != len(m.Levels[i]) {
			t.Fatalf("level %d: got %v, want %v", i, got.Levels[i], m.Levels[i])
		}
		for j := range m.Levels[i] {
			if got.Levels[i][j] != m.Levels[i][j] {
				t.Fatalf("level %d entry %d: got %q, want %q", i, j, got.Levels[i][j], m.Levels[i][j])
			}
		}
	}
}

func TestSaveOverwritesAPreviousManifestAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	first := &Manifest{Levels: [][]string{{"old.sst"}}}
	if err := Save(path, first); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	second := &Manifest{Levels: [][]string{{"new.sst"}}}
	if err := Save(path, second); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Levels) != 1 || len(got.Levels[0]) != 1 || got.Levels[0][0] != "new.sst" {
		t.Fatalf("Load after overwrite = %+v, want [[\"new.sst\"]]", got.Levels)
	}
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Fatal("Save left its temp file behind")
	}
}

func TestLoadRejectsANonManifestFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	if err := os.WriteFile(path, []byte("this is not json at all {{{"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Load(path); !errors.Is(err, ErrNotManifest) {
		t.Fatalf("Load on a corrupt file: err = %v, want ErrNotManifest", err)
	}
}

func TestCloneIsIndependentOfTheOriginal(t *testing.T) {
	m := &Manifest{Levels: [][]string{{"a.sst"}}}
	clone := m.Clone()
	clone.Levels[0][0] = "mutated.sst"
	clone.Levels = append(clone.Levels, []string{"new-level.sst"})

	if m.Levels[0][0] != "a.sst" {
		t.Fatalf("original was mutated through the clone's slice: %q", m.Levels[0][0])
	}
	if len(m.Levels) != 1 {
		t.Fatalf("original gained a level through the clone's append: %d levels", len(m.Levels))
	}
}

func TestEnsureLevelGrowsWithEmptyLevels(t *testing.T) {
	m := New()
	m.EnsureLevel(3)
	if len(m.Levels) != 4 {
		t.Fatalf("EnsureLevel(3): len(Levels) = %d, want 4", len(m.Levels))
	}
	for i, level := range m.Levels {
		if len(level) != 0 {
			t.Fatalf("level %d = %v, want empty", i, level)
		}
	}

	// Idempotent: calling it again for an already-valid index changes
	// nothing.
	m.Levels[2] = []string{"x.sst"}
	m.EnsureLevel(1)
	if len(m.Levels) != 4 || len(m.Levels[2]) != 1 {
		t.Fatalf("EnsureLevel with an already-valid index disturbed existing state: %+v", m.Levels)
	}
}

func TestNewIsASingleEmptyLevel(t *testing.T) {
	m := New()
	if len(m.Levels) != 1 || len(m.Levels[0]) != 0 {
		t.Fatalf("New() = %+v, want one empty level", m.Levels)
	}
}
