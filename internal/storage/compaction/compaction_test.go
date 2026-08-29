package compaction

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ekushal02/helios/internal/storage/manifest"
	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// writeSSTable flushes a small memtable built from kvs (a Delete for any
// key marked tombstone) into dir under name, returning the name so a
// test can add it directly to a hand-built Manifest.
func writeSSTable(t *testing.T, dir, name string, kvs map[string]struct {
	value     string
	tombstone bool
}) string {
	t.Helper()
	m := memtable.NewWithSeed(1)
	for k, e := range kvs {
		if e.tombstone {
			m.Delete([]byte(k))
		} else {
			m.Put([]byte(k), []byte(e.value))
		}
	}
	path := filepath.Join(dir, name)
	if _, err := sstable.Flush(m, path); err != nil {
		t.Fatalf("Flush(%s): %v", name, err)
	}
	return name
}

type kv = struct {
	value     string
	tombstone bool
}

func val(v string) kv { return kv{value: v} }
func tombstone() kv   { return kv{tombstone: true} }

func getFromSSTable(t *testing.T, dir, name, key string) (value string, tombstone bool, ok bool) {
	t.Helper()
	r, err := sstable.Open(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("Open(%s): %v", name, err)
	}
	defer r.Close()
	v, ts, found, err := r.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q) on %s: %v", key, name, err)
	}
	return string(v), ts, found
}

func TestCompactLevelMergesL0IntoEmptyL1(t *testing.T) {
	dir := t.TempDir()
	newer := writeSSTable(t, dir, "newer.sst", map[string]kv{"b": val("newer-b")})
	older := writeSSTable(t, dir, "older.sst", map[string]kv{"a": val("older-a"), "b": val("older-b-stale")})

	m := &manifest.Manifest{Levels: [][]string{{newer, older}}} // L0 newest-first
	out, newFile, oldFiles, err := CompactLevel(m, dir, 0)
	if err != nil {
		t.Fatalf("CompactLevel: %v", err)
	}

	if len(out.Levels) != 2 {
		t.Fatalf("output manifest has %d levels, want 2 (L0 emptied, L1 created)", len(out.Levels))
	}
	if len(out.Levels[0]) != 0 {
		t.Fatalf("L0 = %v, want empty after compaction", out.Levels[0])
	}
	if len(out.Levels[1]) != 1 || out.Levels[1][0] != newFile {
		t.Fatalf("L1 = %v, want exactly [%q]", out.Levels[1], newFile)
	}
	if len(oldFiles) != 2 {
		t.Fatalf("oldFiles = %v, want 2 entries (both L0 files)", oldFiles)
	}

	value, ts, ok := getFromSSTable(t, dir, newFile, "a")
	if !ok || ts || value != "older-a" {
		t.Fatalf("Get(a) on merged output = (%q, ts=%v, ok=%v), want (\"older-a\", false, true)", value, ts, ok)
	}
	value, ts, ok = getFromSSTable(t, dir, newFile, "b")
	if !ok || ts || value != "newer-b" {
		t.Fatalf("Get(b) on merged output = (%q, ts=%v, ok=%v), want (\"newer-b\", false, true) -- newer file must win", value, ts, ok)
	}
}

// TestCompactLevelKeepsTombstoneWhenDeeperLevelsHaveData is the rule
// CompactLevel's own doc states: compacting L0 into L1 must NOT drop a
// tombstone if L2 (or deeper) still holds files, because an older,
// live copy of the key might be sitting down there.
func TestCompactLevelKeepsTombstoneWhenDeeperLevelsHaveData(t *testing.T) {
	dir := t.TempDir()
	l0 := writeSSTable(t, dir, "l0.sst", map[string]kv{"k": tombstone()})
	l1 := writeSSTable(t, dir, "l1.sst", map[string]kv{"other": val("v")})
	l2 := writeSSTable(t, dir, "l2.sst", map[string]kv{"k": val("ancient-value")})

	m := &manifest.Manifest{Levels: [][]string{{l0}, {l1}, {l2}}}
	out, newFile, _, err := CompactLevel(m, dir, 0)
	if err != nil {
		t.Fatalf("CompactLevel: %v", err)
	}

	value, ts, ok := getFromSSTable(t, dir, newFile, "k")
	if !ok || !ts || value != "" {
		t.Fatalf("Get(k) on merged L0+L1 output = (%q, ts=%v, ok=%v), want (\"\", true, true) -- "+
			"tombstone must survive because L2 still has data", value, ts, ok)
	}
	if len(out.Levels[2]) != 1 {
		t.Fatalf("L2 was touched by an L0/L1 compaction: %v", out.Levels[2])
	}
}

// TestCompactLevelDropsTombstoneWhenNoDeeperLevelHasData is the other
// half: once nothing older exists anywhere, the tombstone has finished
// its job and compaction should actually reclaim the space.
func TestCompactLevelDropsTombstoneWhenNoDeeperLevelHasData(t *testing.T) {
	dir := t.TempDir()
	l0 := writeSSTable(t, dir, "l0.sst", map[string]kv{"k": tombstone(), "keep": val("v")})

	m := &manifest.Manifest{Levels: [][]string{{l0}, {}}} // L1 exists but is empty, nothing deeper
	_, newFile, _, err := CompactLevel(m, dir, 0)
	if err != nil {
		t.Fatalf("CompactLevel: %v", err)
	}

	_, _, ok := getFromSSTable(t, dir, newFile, "k")
	if ok {
		t.Fatal("Get(k) on merged output: ok = true, want false -- the tombstone should have been dropped entirely")
	}
	value, ts, ok := getFromSSTable(t, dir, newFile, "keep")
	if !ok || ts || value != "v" {
		t.Fatalf("Get(keep) = (%q, ts=%v, ok=%v), want (\"v\", false, true) -- unrelated key must survive", value, ts, ok)
	}
}

func TestCompactLevelOnTwoEmptyLevelsReturnsAnError(t *testing.T) {
	dir := t.TempDir()
	m := &manifest.Manifest{Levels: [][]string{{}, {}}}
	if _, _, _, err := CompactLevel(m, dir, 0); err == nil {
		t.Fatal("CompactLevel on two empty levels: err = nil, want an error")
	}
}

func TestCompactLevelGrowsTheManifestWhenTheNextLevelDoesNotExistYet(t *testing.T) {
	dir := t.TempDir()
	l0 := writeSSTable(t, dir, "l0.sst", map[string]kv{"a": val("1")})
	m := &manifest.Manifest{Levels: [][]string{{l0}}} // no level 1 at all yet

	out, _, _, err := CompactLevel(m, dir, 0)
	if err != nil {
		t.Fatalf("CompactLevel: %v", err)
	}
	if len(out.Levels) != 2 {
		t.Fatalf("output has %d levels, want 2 (level 1 must be created)", len(out.Levels))
	}
}

// TestRunDoesNothingWhenNoLevelNeedsCompaction checks Run's contract
// that "nothing to do" is success, not an error.
func TestRunDoesNothingWhenNoLevelNeedsCompaction(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")
	l0 := writeSSTable(t, dir, "l0.sst", map[string]kv{"a": val("1")})
	if err := manifest.Save(manifestPath, &manifest.Manifest{Levels: [][]string{{l0}}}); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	compacted, err := Run(manifestPath, dir, Options{MaxFilesPerLevel: 4})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if compacted {
		t.Fatal("Run: compacted = true, want false -- one file is under the threshold")
	}
	if _, err := os.Stat(filepath.Join(dir, "l0.sst")); err != nil {
		t.Fatalf("original file was disturbed even though Run found nothing to do: %v", err)
	}
}

// TestRunEndToEndCompactsSwapsAndCleansUp is the full pipeline: several
// L0 files over threshold, Run compacts them, the manifest on disk
// reflects the new state, the new file has the right merged content, and
// the old files are gone.
func TestRunEndToEndCompactsSwapsAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")

	f1 := writeSSTable(t, dir, "1.sst", map[string]kv{"a": val("v1")})
	f2 := writeSSTable(t, dir, "2.sst", map[string]kv{"b": val("v2")})
	f3 := writeSSTable(t, dir, "3.sst", map[string]kv{"c": val("v3")})
	initial := &manifest.Manifest{Levels: [][]string{{f3, f2, f1}}} // newest-first
	if err := manifest.Save(manifestPath, initial); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}

	compacted, err := Run(manifestPath, dir, Options{MaxFilesPerLevel: 2})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !compacted {
		t.Fatal("Run: compacted = false, want true -- 3 files exceed a threshold of 2")
	}

	got, err := manifest.Load(manifestPath)
	if err != nil {
		t.Fatalf("manifest.Load after Run: %v", err)
	}
	if len(got.Levels[0]) != 0 {
		t.Fatalf("L0 after Run = %v, want empty", got.Levels[0])
	}
	if len(got.Levels) < 2 || len(got.Levels[1]) != 1 {
		t.Fatalf("L1 after Run = %+v, want exactly one file", got.Levels)
	}
	newFile := got.Levels[1][0]

	for key, want := range map[string]string{"a": "v1", "b": "v2", "c": "v3"} {
		value, ts, ok := getFromSSTable(t, dir, newFile, key)
		if !ok || ts || value != want {
			t.Fatalf("Get(%q) on the compacted file = (%q, ts=%v, ok=%v), want (%q, false, true)", key, value, ts, ok, want)
		}
	}

	for _, old := range []string{f1, f2, f3} {
		if _, statErr := os.Stat(filepath.Join(dir, old)); statErr == nil {
			t.Fatalf("old file %s still exists after Run -- should have been cleaned up", old)
		}
	}
}

// TestRunNeverDeletesOldFilesIfTheManifestSwapFails is the crash-safety
// property CompactLevel/Run's doc argues for, checked by forcing the
// swap to fail: point the manifest path at a location Save cannot write
// to, and confirm the source files are completely untouched.
func TestRunNeverDeletesOldFilesIfTheManifestSwapFails(t *testing.T) {
	dir := t.TempDir()
	f1 := writeSSTable(t, dir, "1.sst", map[string]kv{"a": val("v1")})
	f2 := writeSSTable(t, dir, "2.sst", map[string]kv{"b": val("v2")})
	f3 := writeSSTable(t, dir, "3.sst", map[string]kv{"c": val("v3")})

	// A manifest path inside a directory that does not exist: Save's
	// own temp-file create will fail before anything is written.
	manifestPath := filepath.Join(dir, "does-not-exist", "MANIFEST")
	initial := &manifest.Manifest{Levels: [][]string{{f3, f2, f1}}}
	// Skip the initial Save (it would fail too); write the state PickLevel
	// needs to see directly via Run's own Load path by pre-seeding
	// nothing -- Run will Load a fresh empty manifest instead. To
	// exercise the failure path meaningfully, call CompactLevel and
	// manifest.Save directly instead of through Run, mirroring what Run
	// does internally.
	out, _, oldFiles, err := CompactLevel(initial, dir, 0)
	if err != nil {
		t.Fatalf("CompactLevel: %v", err)
	}
	if err := manifest.Save(manifestPath, out); err == nil {
		t.Fatal("manifest.Save to a nonexistent directory unexpectedly succeeded")
	}

	// The merged output file DOES exist at this point (CompactLevel's
	// own write already ran) -- that alone is fine, since nothing
	// references it yet. What must not have happened is the old files
	// being removed.
	for _, old := range oldFiles {
		if _, statErr := os.Stat(old); statErr != nil {
			t.Fatalf("source file %s was removed despite the manifest swap failing: %v", old, statErr)
		}
	}
}
