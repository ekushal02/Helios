package compaction

import "testing"

// TestWriteAndSpaceAmplificationAcrossThreeConfigurations is the
// measurement this task exists to produce: run the identical workload
// (amplification.go) against three different maxFilesPerLevel settings
// and compare the write and space amplification each one produces.
//
// The three values -- 2 (aggressive), 4 (LevelDB's own L0 default,
// already this package's DefaultOptions), and 8 (lazy) -- are chosen to
// bracket DefaultOptions rather than sit arbitrarily: one tighter, one
// looser, so the middle result can be read against the two that bound
// it.
//
// UNLIKE THE WRITE-STALL MEASUREMENT (§13.9), THIS TEST'S NUMBERS ARE
// FULLY DETERMINISTIC AND ASSERTED, NOT JUST LOGGED. Write latency
// depends on real disk timing, which varies machine to machine -- that
// is why §13.9 only logs its numbers. Amplification here depends on
// nothing but a fixed seed and this package's own deterministic merge
// and compaction logic; running this test on any machine should produce
// byte-for-byte identical results. That determinism is exactly what
// lets this test assert the actual claim compaction is built to deliver
// -- write amplification rising and space amplification falling as
// maxFilesPerLevel drops -- as a hard pass/fail check, not just a number
// to eyeball.
func TestWriteAndSpaceAmplificationAcrossThreeConfigurations(t *testing.T) {
	configs := []int{2, 4, 8}
	results := make([]*AmplificationResult, len(configs))

	t.Logf("%-18s %-14s %-16s %-6s   %-12s %-14s %-6s   %-10s %-6s",
		"maxFilesPerLevel", "logical bytes", "physical bytes", "WA", "on-disk bytes", "live logical bytes", "SA", "peak bytes", "peakSA")
	for i, mfl := range configs {
		r, err := MeasureAmplification(mfl)
		if err != nil {
			t.Fatalf("MeasureAmplification(%d): %v", mfl, err)
		}
		results[i] = r
		t.Logf("%-18d %-14d %-16d %-6.3f   %-12d %-14d %-6.3f   %-10d %-6.3f",
			mfl, r.LogicalBytesWritten, r.PhysicalBytesWritten, r.WriteAmplification,
			r.FinalOnDiskBytes, r.FinalLogicalBytes, r.SpaceAmplification,
			r.PeakOnDiskBytes, r.PeakSpaceAmplification)
	}

	// The core trade-off claim, checked directly: tightening the
	// threshold (more frequent compaction) must not make write
	// amplification go down, and must not make PEAK space amplification
	// go up. FinalSpaceAmplification is deliberately not asserted here
	// -- see MeasureAmplification's own doc for why full convergence is
	// expected to land all three configurations on the same number, and
	// asserting a strict inequality there would be asserting a
	// difference this scheme's own design does not actually produce.
	// Strict monotonicity isn't asserted either -- two adjacent
	// configurations could plausibly tie if a threshold change happens
	// not to change how many compactions actually ran -- but the
	// trade-off's DIRECTION, end to end (the tightest vs. the loosest
	// configuration), is exactly the claim compaction's own existence
	// rests on, and is asserted as a hard requirement here.
	tightest, loosest := results[0], results[len(results)-1]
	if tightest.WriteAmplification < loosest.WriteAmplification {
		t.Errorf("tightest config (maxFilesPerLevel=%d) had LOWER write amplification (%.3f) than the loosest (maxFilesPerLevel=%d, %.3f) -- "+
			"more frequent compaction should never write less, not more",
			tightest.MaxFilesPerLevel, tightest.WriteAmplification, loosest.MaxFilesPerLevel, loosest.WriteAmplification)
	}
	if tightest.PeakSpaceAmplification > loosest.PeakSpaceAmplification {
		t.Errorf("tightest config (maxFilesPerLevel=%d) had HIGHER peak space amplification (%.3f) than the loosest (maxFilesPerLevel=%d, %.3f) -- "+
			"more frequent compaction should never let a bigger backlog accumulate, not a smaller one",
			tightest.MaxFilesPerLevel, tightest.PeakSpaceAmplification, loosest.MaxFilesPerLevel, loosest.PeakSpaceAmplification)
	}

	// Sanity bounds no correct implementation should ever violate,
	// regardless of configuration: amplification can't go below 1.0 in
	// either direction (you cannot write less than what was logically
	// requested, or store live data in less space than the data itself
	// needs), and a value that's absurdly high would indicate a real bug
	// (an accidental quadratic rewrite, say) rather than an expected
	// trade-off.
	for _, r := range results {
		if r.WriteAmplification < 1.0 {
			t.Errorf("maxFilesPerLevel=%d: write amplification %.3f < 1.0, which is not physically possible", r.MaxFilesPerLevel, r.WriteAmplification)
		}
		if r.SpaceAmplification < 1.0 {
			t.Errorf("maxFilesPerLevel=%d: space amplification %.3f < 1.0, which is not physically possible", r.MaxFilesPerLevel, r.SpaceAmplification)
		}
		if r.PeakSpaceAmplification < r.SpaceAmplification {
			t.Errorf("maxFilesPerLevel=%d: peak space amplification %.3f is LOWER than the final, fully-converged value %.3f -- "+
				"the peak, sampled mid-run before compaction has caught up, can never be smaller than the fully-compacted floor",
				r.MaxFilesPerLevel, r.PeakSpaceAmplification, r.SpaceAmplification)
		}
		const sanityCeiling = 100.0
		if r.WriteAmplification > sanityCeiling {
			t.Errorf("maxFilesPerLevel=%d: write amplification %.3f exceeds the %.0fx sanity ceiling -- looks like a real bug, not an expected trade-off", r.MaxFilesPerLevel, r.WriteAmplification, sanityCeiling)
		}
		if r.PeakSpaceAmplification > sanityCeiling {
			t.Errorf("maxFilesPerLevel=%d: peak space amplification %.3f exceeds the %.0fx sanity ceiling -- looks like a real bug, not an expected trade-off", r.MaxFilesPerLevel, r.PeakSpaceAmplification, sanityCeiling)
		}
	}
}

func TestMeasureAmplificationIsFullyDeterministic(t *testing.T) {
	a, err := MeasureAmplification(4)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	b, err := MeasureAmplification(4)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if *a != *b {
		t.Fatalf("two runs at the same configuration produced different results:\n  run 1: %+v\n  run 2: %+v", a, b)
	}
}
