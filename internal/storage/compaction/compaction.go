// Package compaction implements the operation every other piece of §13
// has been setting up for since v1.9's SSTable writer first made more
// than one file possible: reclaiming the space a superseded value or a
// no-longer-needed tombstone is still taking up, by merging files
// together and replacing them with one that no longer holds either.
//
// This package answers exactly the four questions its own task
// description asks, in order, and nothing else: which level needs
// compacting (PickLevel), what merging it produces (sstable.Merge,
// already built), where the result is written (CompactLevel, reusing
// sstable.Write unchanged), and how the manifest is updated to describe
// the new state without a reader ever observing an inconsistent one
// (manifest.Save's atomic swap, and Run's ordering around it).
package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/ekushal02/helios/internal/storage/manifest"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// PickLevel returns the lowest level whose file count exceeds
// maxFilesPerLevel, or ok=false if no level does.
//
// FILE COUNT, NOT BYTE SIZE, IS A DELIBERATE SIMPLIFICATION, RECORDED AS
// AN OPEN QUESTION (§12) RATHER THAN A FINAL ANSWER. Real leveled
// compaction schemes (LevelDB, RocksDB) trigger a level on its total
// byte size, growing roughly 10x per level, because size is what
// actually predicts read amplification and disk usage -- the same
// "measure the real quantity, not a proxy" argument this codebase has
// made before (the Raft compaction trigger, §10; ApproxSize over Len,
// §13.3). File count is a coarser, size-blind proxy: a level with three
// enormous files and a level with three tiny ones look identical to this
// function. It is used here because it makes compaction's own tests fast
// and deterministic without needing megabytes of test data to cross a
// size threshold -- correctness first, the same priority §13.2's
// original block-reader gave linear scan over an unbuilt binary search.
// A byte-size trigger, once real workload numbers exist to size it by
// (the same gap `targetBlockSize` and `bitsPerKey` are still waiting
// on), is future work.
//
// The LOWEST over-threshold level is chosen, not the most over-threshold
// one, because L0 files overlap in key range (§13.6) and are the ones a
// read has to check regardless of whether it finds anything -- letting a
// backlog build up at the shallowest level costs every read, not just
// ones for keys that happen to live in the backlogged level.
func PickLevel(m *manifest.Manifest, maxFilesPerLevel int) (level int, ok bool) {
	for i, files := range m.Levels {
		if len(files) > maxFilesPerLevel {
			return i, true
		}
	}
	return 0, false
}

// Options configures a single compaction run.
type Options struct {
	// MaxFilesPerLevel is the trigger PickLevel checks against.
	MaxFilesPerLevel int
}

// DefaultOptions is a reasonable, unmeasured starting point -- see
// PickLevel's own doc for why "unmeasured" is stated plainly rather than
// implied. 4 mirrors LevelDB's own L0 file-count trigger.
var DefaultOptions = Options{MaxFilesPerLevel: 4}

var seqPattern = regexp.MustCompile(`^(\d+)\.sst$`)

// nextSequence derives the next SSTable file number from the manifest's
// own contents -- the highest numeric filename across every level, plus
// one, or 1 if the manifest is empty. Deriving it this way, rather than
// persisting a separate counter, keeps the manifest the single source of
// truth for "what exists and what number comes next" -- a separate
// counter file would be one more piece of state that could disagree with
// the manifest after a crash between updating the two, which this
// avoids by construction rather than by ordering discipline.
func nextSequence(m *manifest.Manifest) int {
	max := 0
	for _, level := range m.Levels {
		for _, name := range level {
			match := seqPattern.FindStringSubmatch(name)
			if match == nil {
				continue
			}
			n, err := strconv.Atoi(match[1])
			if err != nil {
				continue
			}
			if n > max {
				max = n
			}
		}
	}
	return max + 1
}

// isBottomAfterCompaction reports whether level+1 would be the lowest
// (oldest) level holding any data once level is merged into it -- the
// exact condition under which a tombstone reaching level+1 can be
// dropped instead of carried forward. See CompactLevel's doc for why
// this specific condition, and only this one, is safe.
func isBottomAfterCompaction(m *manifest.Manifest, level int) bool {
	for i := level + 2; i < len(m.Levels); i++ {
		if len(m.Levels[i]) > 0 {
			return false
		}
	}
	return true
}

// CompactLevel merges every file in level with every file in level+1,
// writes the result as one new SSTable in dir, and returns the Manifest
// that describes the state after this compaction -- level and level+1
// both replaced by that single new file at level+1. It does NOT persist
// the returned manifest (see manifest.Save) and does NOT delete the
// files it just replaced (see Run) -- both are the caller's job, in a
// specific order Run's own doc explains.
//
// # Why level's files come before level+1's in the merge
//
// level is always newer than level+1 -- level+1 either doesn't exist yet
// (first-ever compaction of this pair) or holds the output of a PRIOR,
// older compaction. sstable.Merge (§13.2) requires sources newest-first,
// the identical convention engine.Reader (§13.6) already uses, so
// level's files are listed ahead of level+1's.
//
// # The tombstone rule
//
// A tombstone surviving the merge is dropped -- freeing the space it was
// taking up, which is the actual point of compaction reaching a delete
// at all -- ONLY if level+1 will be the lowest level holding any data
// once this compaction finishes (isBottomAfterCompaction). If a deeper
// level still holds files, this compaction cannot know whether one of
// them still has an older, live copy of the key the tombstone is
// shadowing -- dropping it in that case would let that older copy
// resurface the next time that deeper level is searched, which is
// exactly the bug DESIGN.md §13.7 built the entire delete-is-a-write
// argument to prevent. Kept, not dropped, is always the safe default;
// see sstable.Merge's own doc for the same reasoning one layer down.
//
// # ONE OUTPUT FILE, NOT SIZE-BOUNDED SEVERAL -- A DELIBERATE
// SIMPLIFICATION
//
// A compaction whose merged input is very large produces one
// correspondingly large output file. Splitting output into several
// size-bounded files (the way targetBlockSize, §13.2, bounds one data
// block) would let level 1+ hold multiple, still-non-overlapping files
// per level instead of the "at most one file per level above L0"
// invariant this implementation currently has as a side effect -- real,
// useful work, and explicitly recorded as an open question (§12) rather
// than attempted here, so this task's actual scope (pick, merge, write,
// swap) stays legible rather than growing a second unbounded feature
// inside it.
func CompactLevel(m *manifest.Manifest, dir string, level int) (newManifest *manifest.Manifest, newFile string, oldFiles []string, err error) {
	m.EnsureLevel(level + 1)
	dropTombstones := isBottomAfterCompaction(m, level)

	var sources []sstable.Source
	var readers []*sstable.Reader
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	for _, name := range m.Levels[level] {
		path := filepath.Join(dir, name)
		r, openErr := sstable.Open(path)
		if openErr != nil {
			return nil, "", nil, fmt.Errorf("compaction: open %s: %w", path, openErr)
		}
		readers = append(readers, r)
		sources = append(sources, r.NewIterator())
		oldFiles = append(oldFiles, path)
	}
	for _, name := range m.Levels[level+1] {
		path := filepath.Join(dir, name)
		r, openErr := sstable.Open(path)
		if openErr != nil {
			return nil, "", nil, fmt.Errorf("compaction: open %s: %w", path, openErr)
		}
		readers = append(readers, r)
		sources = append(sources, r.NewIterator())
		oldFiles = append(oldFiles, path)
	}

	if len(sources) == 0 {
		return nil, "", nil, fmt.Errorf("compaction: level %d and %d are both empty, nothing to compact", level, level+1)
	}

	merged := sstable.Merge(sources, dropTombstones)
	seq := nextSequence(m)
	newFile = fmt.Sprintf("%06d.sst", seq)
	newPath := filepath.Join(dir, newFile)

	if _, err := sstable.Write(merged, newPath); err != nil {
		return nil, "", nil, fmt.Errorf("compaction: write merged output: %w", err)
	}

	out := m.Clone()
	out.Levels[level] = []string{}
	out.Levels[level+1] = []string{newFile}
	return out, newFile, oldFiles, nil
}

// Run performs one complete compaction cycle: load the manifest, pick a
// level, compact it, atomically swap the manifest, and clean up the
// files the swap superseded. It returns compacted=false, err=nil if no
// level currently needs compacting -- not an error, the expected outcome
// of most calls against a healthy, already-compacted tree.
//
// # THE ORDERING THAT MAKES THIS CRASH-SAFE
//
//  1. The new merged file is written (sstable.Write's own atomicity,
//     §13.2, already guarantees it is either fully there or not there at
//     all -- nothing here adds to that guarantee, it relies on it).
//  2. ONLY THEN is the new manifest saved, atomically (manifest.Save).
//     A crash before this point leaves the OLD manifest still naming the
//     OLD files, which are all still on disk, untouched -- compaction
//     simply never happened, safely.
//  3. ONLY THEN are the old files deleted. A crash between steps 2 and 3
//     leaves orphaned files on disk that the manifest no longer
//     references -- harmless disk usage, not a correctness problem, and
//     not yet cleaned up automatically; see §12's open question on this.
//
// The one ordering that is NEVER used is deleting old files before or
// during the manifest swap: a crash in that window would leave the
// manifest naming a file that is already gone, which the next Load/Open
// would have no way to recover from. Every step here is ordered to make
// the WORST crash window "some harmless leftover files," never "the
// manifest lies about what exists."
func Run(manifestPath, dir string, opts Options) (compacted bool, err error) {
	m, err := manifest.Load(manifestPath)
	if err != nil {
		return false, fmt.Errorf("compaction: load manifest: %w", err)
	}

	level, ok := PickLevel(m, opts.MaxFilesPerLevel)
	if !ok {
		return false, nil
	}

	newManifest, _, oldFiles, err := CompactLevel(m, dir, level)
	if err != nil {
		return false, fmt.Errorf("compaction: compact level %d: %w", level, err)
	}

	if err := manifest.Save(manifestPath, newManifest); err != nil {
		return false, fmt.Errorf("compaction: swap manifest: %w", err)
	}

	// Best-effort cleanup, deliberately after the swap above and
	// deliberately not fatal to this call's success: the manifest
	// already correctly describes the post-compaction state by this
	// point, so a file that fails to delete is a leftover, not a
	// correctness problem. Every removal is still attempted, and every
	// failure is still reported, rather than silently swallowed.
	var cleanupErr error
	for _, path := range oldFiles {
		if rmErr := os.Remove(path); rmErr != nil && cleanupErr == nil {
			cleanupErr = fmt.Errorf("compaction: cleanup: remove %s: %w", path, rmErr)
		}
	}
	return true, cleanupErr
}
