package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ekushal02/helios/internal/storage/manifest"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// Recover loads the manifest at manifestPath and reconciles it against
// what dir actually holds -- the piece this task's own title asks for.
// §13.8's `Run` already made a crash mid-compaction safe (the manifest
// is never swapped to a state that names a file that doesn't exist);
// this is what makes that same crash recoverable in the operational
// sense, not just harmless: converging a data directory back to
// "nothing but what the manifest actually needs," and catching, rather
// than silently trusting, any file the manifest claims exists but
// doesn't.
//
// RECOVER MUST BE CALLED BEFORE ANY COMPACTION -- Run, Background, or a
// concurrent call to Recover itself -- HAS STARTED AGAINST THE SAME
// DIRECTORY. It deletes files a live compaction may have legitimately
// created but not yet referenced from the manifest (its own in-progress
// merged output, its own in-progress .tmp file); calling Recover
// concurrently with one would delete out from under it. This is a
// precondition Recover does not and cannot check on its own -- there is
// no way to distinguish "a stale orphan from a past crash" from "a
// legitimate file a compaction currently running in another goroutine
// is about to reference" by looking at the file alone. It is intended to
// run once, at startup, before any compaction begins -- the same moment
// engine.RecoverMemtable (§13.7) runs, and for the identical reason: both
// are what a node does with on-disk state before anything is allowed to
// touch it live.
//
// Two kinds of mismatch between the manifest and disk are treated very
// differently:
//
//   - A FILE ON DISK THE MANIFEST DOESN'T REFERENCE is an orphan --
//     exactly what Run's own doc (§13.8) says a crash between the
//     manifest swap and its cleanup step leaves behind: real, harmless,
//     wasted disk space. Recover deletes it. A leftover ".tmp" file --
//     from sstable.Write or manifest.Save being interrupted mid-write,
//     which their own atomicity already means can never be a completed,
//     referenced file -- is deleted unconditionally, without even being
//     checked against the manifest, since a .tmp file is by
//     construction never a legitimate steady-state artifact regardless
//     of what the manifest says.
//   - A FILE THE MANIFEST REFERENCES BUT DISK DOESN'T HAVE, OR HAS BUT
//     CANNOT OPEN AS A VALID SSTABLE, is an error, not a cleanup
//     opportunity. Given sstable.Write's and manifest.Save's own
//     atomicity guarantees (§13.2, §13.8), this can never legitimately
//     happen -- a manifest is only ever saved after the file it names is
//     already durably, completely on disk. A missing or corrupt
//     referenced file means a bug in this package, external
//     interference with the data directory, or real disk corruption,
//     none of which Recover can safely paper over by pretending the
//     file was never named. This is the same "believed impossible is
//     guarded, not assumed" posture §8 takes toward Raft's own
//     invariants, applied here to this package's own.
//
// Every referenced file is opened (sstable.Open, which verifies its
// footer and index), not merely stat'd -- catching a present-but-corrupt
// file at startup instead of the first time some later Get happens to
// read the wrong block.
func Recover(manifestPath, dir string) (m *manifest.Manifest, removedOrphans []string, err error) {
	m, err = manifest.Load(manifestPath)
	if err != nil {
		return nil, nil, fmt.Errorf("compaction: recover: load manifest: %w", err)
	}

	referenced := make(map[string]bool)
	for _, level := range m.Levels {
		for _, name := range level {
			referenced[name] = true
		}
	}

	for name := range referenced {
		path := filepath.Join(dir, name)
		r, openErr := sstable.Open(path)
		if openErr != nil {
			return nil, nil, fmt.Errorf("compaction: recover: manifest references %s but it did not open cleanly: %w", name, openErr)
		}
		if closeErr := r.Close(); closeErr != nil {
			return nil, nil, fmt.Errorf("compaction: recover: close %s: %w", name, closeErr)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("compaction: recover: read %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()

		switch {
		case strings.HasSuffix(name, ".tmp"):
			// Never a legitimate steady-state file regardless of the
			// manifest's content -- sstable.Write and manifest.Save each
			// only ever leave one behind by being interrupted mid-write.
		case strings.HasSuffix(name, ".sst") && !referenced[name]:
			// A completed SSTable the manifest no longer names: an
			// orphan from Run's documented crash window.
		default:
			continue // not this package's file to remove
		}

		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return nil, nil, fmt.Errorf("compaction: recover: remove orphan %s: %w", name, err)
		}
		removedOrphans = append(removedOrphans, name)
	}

	return m, removedOrphans, nil
}
