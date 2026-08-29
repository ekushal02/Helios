// amplification.go measures the two numbers every LSM engine's
// compaction policy trades off against each other: write amplification
// (how many times more the engine physically writes a byte than the
// user logically wrote it) and space amplification (how much more disk
// an engine's current live data occupies than the minimal representation
// of that same data would need). Both are standard LSM terms (see any
// RocksDB or LevelDB tuning guide) with standard definitions:
//
//	write amplification = total physical bytes written / total logical bytes written
//	space amplification = total on-disk bytes (current, live)  / minimal bytes for that same live data
//
// A value of 1.0 in either is the unreachable ideal -- every byte
// written exactly once, every byte of disk space holding exactly one
// live copy of exactly one key. This package's own compaction (§13.8)
// is what pulls space amplification down at the cost of write
// amplification (rewriting data to reclaim the space stale copies and
// tombstones were holding), so the two are inherently in tension: more
// frequent, more aggressive compaction should be expected to lower
// space amplification and raise write amplification, and less frequent
// compaction the reverse. MeasureAmplification exists to check that
// expectation against real numbers rather than assert it from theory
// alone.
package compaction

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/ekushal02/helios/internal/storage/engine"
	"github.com/ekushal02/helios/internal/storage/manifest"
	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/sstable"
	"github.com/ekushal02/helios/internal/storage/wal"
)

// AmplificationResult is what MeasureAmplification reports for one
// compaction configuration.
type AmplificationResult struct {
	MaxFilesPerLevel int

	LogicalBytesWritten  int64 // every Put/Delete payload the workload issued, ever
	PhysicalBytesWritten int64 // WAL + every flush + every compaction's output, total
	WriteAmplification   float64

	FinalOnDiskBytes   int64 // every currently-live SSTable file, after full convergence
	FinalLogicalBytes  int64 // minimal bytes to represent the current live key set exactly once
	SpaceAmplification float64

	// PeakOnDiskBytes is the highest total on-disk footprint observed at
	// any point DURING the run -- not after full convergence. See the
	// doc on MeasureAmplification for why this, not FinalOnDiskBytes,
	// turns out to be the number that actually differentiates
	// maxFilesPerLevel settings from each other.
	PeakOnDiskBytes        int64
	PeakSpaceAmplification float64
}

// amplificationWorkload's parameters are fixed rather than
// caller-configurable: the three configurations this task compares are
// meant to isolate the effect of ONE variable, maxFilesPerLevel, against
// an identical write pattern -- varying the workload too would make the
// three results incomparable. 2,000 keys, 20,000 operations (roughly a
// 5% delete rate, the rest overwrite-heavy Puts across a small enough
// key space that most writes really are overwrites, which is what makes
// both amplification numbers meaningful at all: a workload of entirely
// distinct keys would have almost nothing for compaction to reclaim),
// flushed to a new SSTable every 1,000 operations.
const (
	amplificationNumKeys      = 2000
	amplificationNumOps       = 20000
	amplificationFlushEvery   = 1000
	amplificationValueSize    = 100
	amplificationDeleteEveryN = 20 // roughly 1 in 20 operations is a Delete
)

// MeasureAmplification runs the fixed workload above against a fresh,
// temporary data directory, compacting with the given maxFilesPerLevel
// threshold, and returns the resulting write and space amplification.
//
// SPACE AMPLIFICATION AT FULL CONVERGENCE TURNS OUT NOT TO
// DIFFERENTIATE maxFilesPerLevel AT ALL -- WHICH IS A REAL, CORRECT
// PROPERTY OF THIS PACKAGE'S SIMPLIFIED COMPACTION SCHEME, FOUND WHILE
// BUILDING THIS MEASUREMENT, NOT A FLAW IN THE MEASUREMENT ITSELF.
// CompactLevel always performs a FULL merge of everything in the two
// levels it touches (§13.8's documented simplification, not a
// partial-range one), so once every configuration is drained to full
// convergence, all three arrive at the exact same fully-deduplicated
// steady state: there is only one canonical "everything merged,
// nothing stale left" representation of a given live key set, and the
// path taken to reach it (how many intermediate compactions ran, or
// when) cannot change the destination. FinalOnDiskBytes and
// SpaceAmplification measure exactly that destination, and are
// expected to come out equal, or very nearly so, across all three
// configurations -- that is not this measurement failing to show a
// difference, it is the difference genuinely not existing at that
// snapshot.
//
// The trade-off this task is actually looking for shows up DURING the
// run instead: PeakOnDiskBytes and PeakSpaceAmplification track the
// highest total on-disk footprint observed at any point before
// convergence, sampled right after every flush and before that cycle's
// compaction (if any) runs. A larger maxFilesPerLevel lets more
// un-merged, un-deduplicated L0 backlog accumulate before compaction
// ever reclaims it, which is exactly the classic LSM space-amplification
// story (how much extra disk headroom a deployment needs to provision
// for the backlog between compactions) -- FinalOnDiskBytes is "the
// floor," PeakOnDiskBytes is "the ceiling a real, ongoing workload
// actually has to live with."
//
// Deterministic, not measured-with-tolerance like write latency
// (§13.9): every byte count here follows directly from a fixed, seeded
// random sequence and this package's own deterministic merge and
// compaction logic, with no real-time disk latency in the loop at all.
// Run this function (or TestWriteAndSpaceAmplificationAcrossThreeConfigurations,
// which calls it) on any machine and the numbers should match exactly,
// unlike a latency measurement -- there is no noise here to average out.
//
// Each "flush chunk" of amplificationFlushEvery operations uses its own
// fresh WAL, memtable, and engine.Writer rather than one continuously
// growing WAL a production node would rotate on flush -- production
// memtable-swap-and-flush wiring does not exist yet (§12's open
// question on this), and building it is out of scope for a measurement
// task. This does not change what is being measured: the WAL bytes for
// each chunk are counted exactly once, at the point they were durably
// written, which is what "physical bytes written" means regardless of
// how long a WAL segment is later kept around.
func MeasureAmplification(maxFilesPerLevel int) (*AmplificationResult, error) {
	dir, err := os.MkdirTemp("", "helios-amplification-*")
	if err != nil {
		return nil, fmt.Errorf("compaction: measure amplification: %w", err)
	}
	defer os.RemoveAll(dir)

	manifestPath := filepath.Join(dir, "MANIFEST")
	if err := manifest.Save(manifestPath, manifest.New()); err != nil {
		return nil, fmt.Errorf("compaction: measure amplification: %w", err)
	}

	result := &AmplificationResult{MaxFilesPerLevel: maxFilesPerLevel}
	liveState := make(map[string][]byte) // nil = deleted; absent = never written
	rng := rand.New(rand.NewSource(42))
	value := make([]byte, amplificationValueSize)
	seq := 0

	opsIssued := 0
	for opsIssued < amplificationNumOps {
		chunkOps := amplificationFlushEvery
		if remaining := amplificationNumOps - opsIssued; remaining < chunkOps {
			chunkOps = remaining
		}

		walPath := filepath.Join(dir, fmt.Sprintf("chunk-%d.wal", seq))
		m := memtable.NewWithSeed(int64(seq))
		w, err := engine.RecoverMemtable(walPath, wal.SyncNever, m)
		if err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}
		writer := engine.NewWriter(w, m)

		for i := 0; i < chunkOps; i++ {
			keyN := rng.Intn(amplificationNumKeys)
			key := []byte(fmt.Sprintf("key-%06d", keyN))

			if rng.Intn(amplificationDeleteEveryN) == 0 {
				if err := writer.Delete(key); err != nil {
					w.Close()
					return nil, fmt.Errorf("compaction: measure amplification: %w", err)
				}
				result.LogicalBytesWritten += int64(len(key))
				liveState[string(key)] = nil
			} else {
				if err := writer.Put(key, value); err != nil {
					w.Close()
					return nil, fmt.Errorf("compaction: measure amplification: %w", err)
				}
				result.LogicalBytesWritten += int64(len(key) + len(value))
				liveState[string(key)] = value
			}
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}

		walInfo, err := os.Stat(walPath)
		if err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}
		result.PhysicalBytesWritten += walInfo.Size()
		if err := os.Remove(walPath); err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}

		sstPath := filepath.Join(dir, fmt.Sprintf("flush-%06d.sst", seq))
		flushInfo, err := sstable.Flush(m, sstPath)
		if err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}
		result.PhysicalBytesWritten += flushInfo.Bytes

		mf, err := manifest.Load(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}
		mf.Levels[0] = append([]string{filepath.Base(sstPath)}, mf.Levels[0]...)
		if err := manifest.Save(manifestPath, mf); err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}

		// Sampled right here, after this cycle's flush is already
		// reflected in the manifest but before this cycle's compaction
		// (if any) has had a chance to reclaim anything -- the worst
		// this configuration's backlog gets before relief arrives.
		onDiskNow, err := totalOnDiskBytes(mf, dir)
		if err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}
		if onDiskNow > result.PeakOnDiskBytes {
			result.PeakOnDiskBytes = onDiskNow
		}

		if err := drainAndTally(manifestPath, dir, maxFilesPerLevel, &result.PhysicalBytesWritten); err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}

		opsIssued += chunkOps
		seq++
	}

	// Force any leftover L0 backlog into L1 for the space-amplification
	// snapshot -- a single, explicit, bounded call, deliberately NOT a
	// drainAndTally loop at threshold 0.
	//
	// WITHOUT THIS, THE THREE CONFIGURATIONS ARE NOT ACTUALLY COMPARABLE,
	// AND THE FIRST VERSION OF THIS FUNCTION GOT CAUGHT MAKING EXACTLY
	// THAT MISTAKE. amplificationNumOps / amplificationFlushEvery is a
	// fixed number of flush chunks (20); a larger maxFilesPerLevel means
	// L0 needs more accumulated files before a compaction ever triggers,
	// so the workload can end with a small backlog of un-merged,
	// un-deduplicated L0 files sitting there simply because the run
	// ended before enough new flushes arrived to cross the threshold
	// again. That leftover backlog's size is an artifact of how many
	// flush chunks a fixed-length workload happened to produce relative
	// to the threshold, not a property of the compaction policy being
	// measured -- and it was large enough, in an early run of this
	// function, to make maxFilesPerLevel=8's measured space
	// amplification come out identical to maxFilesPerLevel=2's, for
	// reasons that had nothing to do with either policy.
	//
	// A drainAndTally CALL AT THRESHOLD 0 WAS THE FIRST FIX TRIED, AND IT
	// WAS WRONG: PickLevel at threshold 0 considers ANY level with even
	// one file "over threshold," and since a compaction always leaves
	// exactly one file at the level it writes into (§13.8), the very
	// next iteration finds THAT level over threshold too, cascading one
	// level deeper forever -- the identical MaxFilesPerLevel: 0 hazard
	// Background's own maxDrainCycles guards against (§13.9), reproduced
	// here because drainAndTally has no such cap. The actual fix needed
	// is much narrower than "drain at threshold 0": only level 0 can
	// ever legitimately have a leftover backlog at the end of this
	// workload (every level below it is always left at 0 or 1 files by
	// construction), so CompactLevel is called on level 0 directly, at
	// most once, bypassing PickLevel and any threshold entirely, rather
	// than looping.
	if final, err := manifest.Load(manifestPath); err != nil {
		return nil, fmt.Errorf("compaction: measure amplification: %w", err)
	} else if len(final.Levels[0]) > 0 {
		newManifest, newFile, oldFiles, err := CompactLevel(final, dir, 0)
		if err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: final L0 drain: %w", err)
		}
		info, err := os.Stat(filepath.Join(dir, newFile))
		if err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}
		result.PhysicalBytesWritten += info.Size()
		if err := manifest.Save(manifestPath, newManifest); err != nil {
			return nil, fmt.Errorf("compaction: measure amplification: %w", err)
		}
		for _, old := range oldFiles {
			if err := os.Remove(old); err != nil {
				return nil, fmt.Errorf("compaction: measure amplification: %w", err)
			}
		}
	}

	finalManifest, err := manifest.Load(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("compaction: measure amplification: %w", err)
	}
	result.FinalOnDiskBytes, err = totalOnDiskBytes(finalManifest, dir)
	if err != nil {
		return nil, fmt.Errorf("compaction: measure amplification: %w", err)
	}

	for key, v := range liveState {
		if v == nil {
			continue // deleted -- contributes nothing to the minimal representation
		}
		result.FinalLogicalBytes += int64(len(key) + len(v))
	}

	result.WriteAmplification = float64(result.PhysicalBytesWritten) / float64(result.LogicalBytesWritten)
	result.SpaceAmplification = float64(result.FinalOnDiskBytes) / float64(result.FinalLogicalBytes)
	result.PeakSpaceAmplification = float64(result.PeakOnDiskBytes) / float64(result.FinalLogicalBytes)
	return result, nil
}

// totalOnDiskBytes sums the size of every file m currently references,
// across every level.
func totalOnDiskBytes(m *manifest.Manifest, dir string) (int64, error) {
	var total int64
	for _, level := range m.Levels {
		for _, name := range level {
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil {
				return 0, err
			}
			total += info.Size()
		}
	}
	return total, nil
}

// drainAndTally repeatedly picks and compacts a level -- mirroring Run's
// own pick/merge/write/swap/cleanup sequence exactly, including its
// crash-safety ordering (§13.8) -- until PickLevel has nothing left,
// adding each compaction's output file size to *physicalBytes as it
// goes. It is not a call to Run itself because Run's public return
// (compacted bool, err error) does not expose the output file's size,
// and changing that signature to serve one measurement harness would
// affect every existing caller and test of Run for no benefit to them.
func drainAndTally(manifestPath, dir string, maxFilesPerLevel int, physicalBytes *int64) error {
	for {
		m, err := manifest.Load(manifestPath)
		if err != nil {
			return err
		}
		level, ok := PickLevel(m, maxFilesPerLevel)
		if !ok {
			return nil
		}
		newManifest, newFile, oldFiles, err := CompactLevel(m, dir, level)
		if err != nil {
			return err
		}
		info, err := os.Stat(filepath.Join(dir, newFile))
		if err != nil {
			return err
		}
		*physicalBytes += info.Size()

		if err := manifest.Save(manifestPath, newManifest); err != nil {
			return err
		}
		for _, old := range oldFiles {
			if err := os.Remove(old); err != nil {
				return err
			}
		}
	}
}
