package compaction

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/manifest"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

func TestRecoverOnAHealthyDirectoryChangesNothing(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	f1 := writeSSTable(t, dir, "1.sst", map[string]kv{"a": val("1")})
	f2 := writeSSTable(t, dir, "2.sst", map[string]kv{"b": val("2")})
	want := &manifest.Manifest{Levels: [][]string{{f1}, {f2}}}
	if err := manifest.Save(manifestPath, want); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	got, removed, err := Recover(manifestPath, dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none -- nothing here is an orphan", removed)
	}
	if len(got.Levels) != 2 || got.Levels[0][0] != f1 || got.Levels[1][0] != f2 {
		t.Fatalf("Recover returned %+v, want the manifest unchanged", got.Levels)
	}
	// Both files must still be exactly as they were.
	for _, f := range []string{f1, f2} {
		if _, statErr := os.Stat(filepath.Join(dir, f)); statErr != nil {
			t.Fatalf("%s was disturbed on a healthy directory: %v", f, statErr)
		}
	}
}

func TestRecoverRemovesAnOrphanedSSTableFile(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	referenced := writeSSTable(t, dir, "referenced.sst", map[string]kv{"a": val("1")})
	// A completed, valid SSTable that the manifest simply never
	// mentions -- exactly what a crash between the manifest swap and
	// Run's own cleanup step leaves behind.
	orphan := writeSSTable(t, dir, "orphan.sst", map[string]kv{"b": val("2")})
	if err := manifest.Save(manifestPath, &manifest.Manifest{Levels: [][]string{{referenced}}}); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	_, removed, err := Recover(manifestPath, dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(removed) != 1 || removed[0] != orphan {
		t.Fatalf("removed = %v, want exactly [%q]", removed, orphan)
	}
	if _, statErr := os.Stat(filepath.Join(dir, orphan)); statErr == nil {
		t.Fatal("orphan file still exists after Recover")
	}
	if _, statErr := os.Stat(filepath.Join(dir, referenced)); statErr != nil {
		t.Fatalf("referenced file was removed: %v", statErr)
	}
}

func TestRecoverRemovesLeftoverTempFiles(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	if err := manifest.Save(manifestPath, manifest.New()); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}
	// Simulate a process killed mid-write: a .tmp file sstable.Write's
	// own defer-based cleanup never got to run for, because the process
	// itself died rather than Write returning an error.
	tmpPath := filepath.Join(dir, "000007.sst.tmp")
	if err := os.WriteFile(tmpPath, []byte("partial garbage"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, removed, err := Recover(manifestPath, dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(removed) != 1 || removed[0] != "000007.sst.tmp" {
		t.Fatalf("removed = %v, want exactly the leftover .tmp file", removed)
	}
	if _, statErr := os.Stat(tmpPath); statErr == nil {
		t.Fatal(".tmp file still exists after Recover")
	}
}

func TestRecoverReturnsAnErrorWhenAReferencedFileIsMissing(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	f := writeSSTable(t, dir, "gone.sst", map[string]kv{"a": val("1")})
	if err := manifest.Save(manifestPath, &manifest.Manifest{Levels: [][]string{{f}}}); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, f)); err != nil {
		t.Fatalf("os.Remove: %v", err)
	}

	_, _, err := Recover(manifestPath, dir)
	if err == nil {
		t.Fatal("Recover with a missing referenced file: err = nil, want an error")
	}
}

func TestRecoverReturnsAnErrorWhenAReferencedFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	f := writeSSTable(t, dir, "corrupt.sst", map[string]kv{"a": val("1")})
	if err := manifest.Save(manifestPath, &manifest.Manifest{Levels: [][]string{{f}}}); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	// Corrupt the trailing magic so sstable.Open fails outright.
	path := filepath.Join(dir, f)
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatalf("Stat: %v", statErr)
	}
	fh, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := fh.WriteAt([]byte{0xFF}, info.Size()-1); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, _, err = Recover(manifestPath, dir)
	if err == nil {
		t.Fatal("Recover with a corrupt referenced file: err = nil, want an error")
	}
}

func TestRecoverOnADoesNotErrorOnAMissingManifest(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST") // never created
	m, removed, err := Recover(manifestPath, dir)
	if err != nil {
		t.Fatalf("Recover with no manifest yet: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none", removed)
	}
	if len(m.Levels) != 1 || len(m.Levels[0]) != 0 {
		t.Fatalf("Recover with no manifest yet returned %+v, want a fresh empty manifest", m.Levels)
	}
}

func TestRecoverDoesNotTouchUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	if err := manifest.Save(manifestPath, manifest.New()); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}
	// A file this package has no business touching -- neither a .sst
	// nor a .tmp file.
	other := filepath.Join(dir, "README.txt")
	if err := os.WriteFile(other, []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, removed, err := Recover(manifestPath, dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none", removed)
	}
	if _, statErr := os.Stat(other); statErr != nil {
		t.Fatalf("unrelated file was removed: %v", statErr)
	}
}

// TestRecoverClosesTheExactCrashWindowRunDocuments is the end-to-end
// case this task exists for: reproduce, directly, the crash Run's own
// doc (§13.8) describes -- write the merged file, save the new
// manifest, but DO NOT run the cleanup step Run would normally run next
// -- and confirm Recover finishes the job the crash interrupted.
func TestRecoverClosesTheExactCrashWindowRunDocuments(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	f1 := writeSSTable(t, dir, "1.sst", map[string]kv{"a": val("1")})
	f2 := writeSSTable(t, dir, "2.sst", map[string]kv{"b": val("2")})
	f3 := writeSSTable(t, dir, "3.sst", map[string]kv{"c": val("3")})
	initial := &manifest.Manifest{Levels: [][]string{{f3, f2, f1}}}
	if err := manifest.Save(manifestPath, initial); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	// Steps 1 and 2 of Run's own sequence, by hand -- write the merged
	// file, save the new manifest -- deliberately stopping before step
	// 3 (deleting f1/f2/f3), simulating a crash exactly there.
	newManifest, _, oldFiles, err := CompactLevel(initial, dir, 0)
	if err != nil {
		t.Fatalf("CompactLevel: %v", err)
	}
	if err := manifest.Save(manifestPath, newManifest); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}
	for _, old := range oldFiles {
		if _, statErr := os.Stat(old); statErr != nil {
			t.Fatalf("test setup: old file %s should still be present before Recover runs: %v", old, statErr)
		}
	}

	got, removed, err := Recover(manifestPath, dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(removed) != 3 {
		t.Fatalf("removed = %v, want all 3 old files cleaned up", removed)
	}
	for _, old := range oldFiles {
		if _, statErr := os.Stat(old); statErr == nil {
			t.Fatalf("old file %s still exists after Recover -- should have been cleaned up", old)
		}
	}

	// The manifest's own new state -- the compacted output -- must be
	// intact and correct.
	newFile := got.Levels[1][0]
	r, err := sstable.Open(filepath.Join(dir, newFile))
	if err != nil {
		t.Fatalf("Open(%s) after Recover: %v", newFile, err)
	}
	defer r.Close()
	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		value, tombstone, ok, err := r.Get([]byte(key))
		if err != nil || !ok || tombstone || string(value) != want {
			t.Fatalf("Get(%q) after Recover = (%q, ts=%v, ok=%v, err=%v), want (%q, false, true, nil)", key, value, tombstone, ok, err, want)
		}
	}
}

func TestRecoverErrorsAreDistinguishableFromEachOther(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	if err := os.WriteFile(manifestPath, []byte("not json {{{"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, _, err := Recover(manifestPath, dir); !errors.Is(err, manifest.ErrNotManifest) {
		t.Fatalf("Recover on a corrupt manifest: err = %v, want it to wrap manifest.ErrNotManifest", err)
	}
}
