// Package manifest is the source of truth for which SSTable files exist
// and which level each belongs to -- the piece DESIGN.md §12 has recorded
// since v1.9 as "not designed," closed now because compaction (§13.8)
// cannot atomically swap something that does not exist.
//
// A Manifest is deliberately the smallest thing that can answer "what
// exists right now": a list of file names per level, nothing else. It
// does not track byte sizes, key ranges, sequence numbers, or which
// files a still-running compaction is in the middle of replacing --
// those are all real questions a more capable manifest would answer, and
// all recorded as open questions rather than solved here. This one
// answers the question a Reader (§13.6) and a compaction both actually
// need right now: given a directory, which files should be opened, in
// which order, per level.
package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// Manifest lists, per level, the SSTable files that exist -- index 0 is
// level 0 (where a flush lands, files overlapping and kept newest-first,
// matching engine.Reader's own convention (§13.6)); index i for i >= 1 is
// level i, whose files are the output of compacting level i-1 into it
// (§13.8) and are consequently in ascending, non-overlapping key order
// as a natural side effect of Merge's own output order, not because
// anything here enforces it.
//
// A Manifest is a plain, JSON-serializable value, not a live handle on
// anything -- Load returns one, a caller mutates a copy, and Save
// persists a new one. There is no in-memory "the manifest" a Manifest
// keeps synchronized with disk on your behalf; that coordination belongs
// to whatever orchestrates compaction (§12's open question on this),
// which is a different, unbuilt layer above this package.
type Manifest struct {
	Levels [][]string `json:"levels"`
}

// New returns an empty Manifest: one level, level 0, with no files in
// it. Every real Manifest starts here before a single flush has run.
func New() *Manifest {
	return &Manifest{Levels: [][]string{{}}}
}

// Clone returns a deep copy, so a caller building a new Manifest to Save
// (typically after a compaction) never mutates the one still describing
// what is currently on disk out from under a concurrent reader of it.
func (m *Manifest) Clone() *Manifest {
	out := &Manifest{Levels: make([][]string, len(m.Levels))}
	for i, level := range m.Levels {
		out.Levels[i] = append([]string(nil), level...)
	}
	return out
}

// EnsureLevel grows Levels with empty levels, if necessary, so that
// index i is valid to read or write. Compacting level L into L+1 needs
// this the first time L+1 doesn't exist yet -- the "first compaction
// ever" case, where there is no level 1 until this call creates it.
func (m *Manifest) EnsureLevel(i int) {
	for len(m.Levels) <= i {
		m.Levels = append(m.Levels, []string{})
	}
}

// ErrNotManifest means the bytes at a path did not parse as a Manifest --
// covers both "this file is something else" and "this file is truncated
// or corrupted badly enough to fail JSON decoding."
var ErrNotManifest = errors.New("manifest: file does not decode as a manifest")

// Load reads and decodes the Manifest at path. A missing file is not an
// error -- it returns New()'s empty Manifest instead, the same "nothing
// on disk yet is a valid starting state, not a failure" posture Open
// takes nowhere in this codebase yet, and Recover (§13.1) takes for a
// missing WAL.
func Load(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return New(), nil
		}
		return nil, fmt.Errorf("manifest: read %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest: %s: %w", path, ErrNotManifest)
	}
	if len(m.Levels) == 0 {
		m.Levels = [][]string{{}}
	}
	return &m, nil
}

// Save persists m to path atomically: written to a temp file, fsynced,
// renamed over path, and the containing directory fsynced -- the
// identical write-temp/fsync/rename/fsync-dir sequence FileStorage uses
// for Raft's own persistent state (DESIGN.md §5) and sstable.Write uses
// for a flushed file (§13.2), for the same reason repeated a third time
// here: a reader must never be able to open path partway through a
// write and see a manifest that is neither the old, complete state nor
// the new one -- especially here, since a manifest that named a file
// which doesn't exist (a half-written rename) would break every Reader
// and every future compaction that trusted it.
//
// THIS IS THE "ATOMICALLY SWAP THE MANIFEST" STEP §13.8 IS BUILT AROUND.
// Compact calls Save exactly once, after a compaction's merged output
// file is already durably on disk (sstable.Write's own atomicity has
// already run by then) and before any of the files the new Manifest no
// longer references are deleted -- see Compact's own doc for why that
// ordering, and not some other one, is the only safe one.
func Save(path string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest: encode: %w", err)
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("manifest: create %s: %w", tmp, err)
	}
	success := false
	defer func() {
		if !success {
			f.Close()
			os.Remove(tmp)
		}
	}()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("manifest: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("manifest: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest: close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("manifest: rename into place: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("manifest: fsync directory: %w", err)
	}

	success = true
	return nil
}

// syncDir is duplicated from sstable.syncDir (itself duplicated from
// raft.syncDir) rather than shared, on the same one-way-dependency
// reasoning repeated at each prior duplication: this package does not
// import sstable, and sstable does not import this package, so each
// keeps its own copy of a five-line function rather than either
// depending on the other just to share it.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		if runtime.GOOS == "windows" {
			return nil
		}
		return err
	}
	return nil
}
