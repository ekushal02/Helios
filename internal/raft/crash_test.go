//go:build unix

package raft

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// The hard kill
// =============================================================================
//
// WHAT THIS PROVES. That a SIGKILL landing at an arbitrary point inside Save
// leaves a state file that (a) still decodes, (b) is internally consistent —
// term and log came from the same record — and (c) holds at least every term
// the process had already announced as durable. Together those are the
// atomicity half of the Figure 2 requirement, and they are what the
// write-temp / fsync / rename / fsync-dir sequence buys.
//
// WHAT THIS DOES NOT PROVE. Durability against power loss. SIGKILL destroys a
// process, not a page cache: bytes handed to the kernel survive it whether or
// not anyone called fsync, so this test would pass just as happily with the
// Sync calls deleted. Testing those needs the write path to be interrupted
// below the kernel — a device-mapper log-writes target, or a machine someone
// unplugs. That belongs with the kill-during-fsync injector; the fsync calls
// here are what it will have to exercise.
//
// HOW IT RUNS. The test binary re-executes itself. The child sees
// HELIOS_CRASH_CHILD in its environment, never enters the testing framework,
// and instead saves a rising sequence of terms into a shared directory,
// printing one line per term AFTER Save has returned. The parent kills it after
// a random interval, drains the pipe, and checks the file against the highest
// term it was promised. Twelve rounds share one directory, so each round is
// also a restart of the previous round's corpse.

const (
	crashChildEnv = "HELIOS_CRASH_CHILD"
	crashDirEnv   = "HELIOS_CRASH_DIR"

	// The child's log length cycles rather than growing without bound: this is
	// a durability test, not a benchmark of the O(n) whole-log rewrite.
	crashLogCycle = 500
)

func TestHardKillLosesNoAcknowledgedState(t *testing.T) {
	if os.Getenv(crashChildEnv) == "1" {
		runCrashChild() // never returns
		return
	}
	if testing.Short() {
		t.Skip("spawns child processes and sleeps; runs in the full suite")
	}

	dir := t.TempDir()
	rng := rand.New(rand.NewSource(20260822))

	prevOnDisk := 0
	roundsWithProgress := 0

	for round := 1; round <= 12; round++ {
		live := time.Duration(15+rng.Intn(160)) * time.Millisecond
		acked := runChildUntilKilled(t, dir, live)

		// A fresh handle, exactly as a restarting node builds. Note that this
		// is also what discards any temp file the kill left behind.
		store, err := NewFileStorage(dir)
		if err != nil {
			t.Fatalf("round %d: NewFileStorage: %v", round, err)
		}
		b, err := store.Load()
		if err != nil {
			t.Fatalf("round %d: Load: %v", round, err)
		}

		if b == nil {
			if acked > 0 {
				t.Fatalf("round %d: child acknowledged term %d, disk holds nothing", round, acked)
			}
			continue // killed before its first Save; nothing was promised
		}

		ps, err := decodeState(b)
		if err != nil {
			t.Fatalf("round %d: state did not decode after a hard kill: %v", round, err)
		}

		if ps.CurrentTerm < acked {
			t.Fatalf("round %d: lost an acknowledged term: disk has %d, child had announced %d",
				round, ps.CurrentTerm, acked)
		}
		if ps.CurrentTerm < prevOnDisk {
			t.Fatalf("round %d: term went backwards across restarts: %d then %d",
				round, prevOnDisk, ps.CurrentTerm)
		}
		if err := checkRecordIsWhole(ps); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}

		if acked > 0 {
			roundsWithProgress++
		}
		prevOnDisk = ps.CurrentTerm
	}

	// Anti-vacuity. If the child never got a Save in before the kill, every
	// assertion above was trivially satisfied and the test proved nothing.
	if roundsWithProgress < 8 {
		t.Fatalf("only %d of 12 rounds saw an acknowledged write; the test is not exercising Save", roundsWithProgress)
	}
	if prevOnDisk == 0 {
		t.Fatal("nothing was ever persisted")
	}
}

// runChildUntilKilled starts a child against dir, lets it run, SIGKILLs it, and
// returns the highest term the child announced as durable before it died.
func runChildUntilKilled(t *testing.T, dir string, live time.Duration) int {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestHardKillLosesNoAcknowledgedState$")
	cmd.Env = append(os.Environ(), crashChildEnv+"=1", crashDirEnv+"="+dir)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting child: %v", err)
	}

	var highest int64
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if !strings.HasPrefix(line, "ACK ") {
				continue
			}
			if term, err := strconv.Atoi(strings.TrimPrefix(line, "ACK ")); err == nil {
				atomic.StoreInt64(&highest, int64(term))
			}
		}
	}()

	time.Sleep(live)

	// SIGKILL. No deferred functions, no flush, no opportunity to finish a
	// write or clean up a temp file — which is the entire point.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("killing child: %v", err)
	}

	// Drain before Wait: Wait closes the pipe. Every line still in the buffer
	// was printed after its Save returned, so it counts as acknowledged.
	<-drained
	_ = cmd.Wait() // always an error: signal: killed

	return int(atomic.LoadInt64(&highest))
}

// checkRecordIsWhole looks for a record spliced from two different saves.
//
// The child writes every entry beyond the sentinel at the current term and
// sizes the log from the term, so a head-of-new / tail-of-old blob shows up as
// a length or a term that does not match. A plain in-place overwrite fails here
// long before the checksum does.
func checkRecordIsWhole(ps persistentState) error {
	wantLen := 1 + ps.CurrentTerm%crashLogCycle
	if len(ps.Log) != wantLen {
		return fmt.Errorf("torn record: term %d implies a log of %d entries, found %d",
			ps.CurrentTerm, wantLen, len(ps.Log))
	}
	if ps.VotedFor != ps.CurrentTerm%5 {
		return fmt.Errorf("torn record: term %d implies votedFor %d, found %d",
			ps.CurrentTerm, ps.CurrentTerm%5, ps.VotedFor)
	}
	if len(ps.Log) > 0 && ps.Log[0].Term != 0 {
		return fmt.Errorf("sentinel entry has term %d, want 0", ps.Log[0].Term)
	}
	for i := 1; i < len(ps.Log); i++ {
		if ps.Log[i].Term != ps.CurrentTerm {
			return fmt.Errorf("torn record: log[%d].Term = %d, term is %d",
				i, ps.Log[i].Term, ps.CurrentTerm)
		}
	}
	return nil
}

// =============================================================================
// Child
// =============================================================================

// runCrashChild is the process the parent kills. It never returns.
//
// It resumes from whatever is on disk, so successive rounds against one
// directory form a single rising sequence of terms across many hard kills —
// which is what makes the monotonicity assertion in the parent meaningful.
func runCrashChild() {
	dir := os.Getenv(crashDirEnv)
	if dir == "" {
		fmt.Fprintln(os.Stderr, "child: no crash dir")
		os.Exit(2)
	}

	store, err := NewFileStorage(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: NewFileStorage: %v\n", err)
		os.Exit(2)
	}

	start := 0
	if b, err := store.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "child: Load: %v\n", err)
		os.Exit(3)
	} else if b != nil {
		ps, err := decodeState(b)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: inherited state does not decode: %v\n", err)
			os.Exit(4)
		}
		start = ps.CurrentTerm
	}

	for term := start + 1; ; term++ {
		entries := make([]LogEntry, 1+term%crashLogCycle)
		for i := 1; i < len(entries); i++ {
			entries[i] = LogEntry{Term: term}
		}

		b, err := encodeState(persistentState{
			CurrentTerm: term,
			VotedFor:    term % 5,
			Log:         entries,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: encodeState: %v\n", err)
			os.Exit(5)
		}
		if err := store.Save(b); err != nil {
			fmt.Fprintf(os.Stderr, "child: Save: %v\n", err)
			os.Exit(6)
		}

		// Only now is the parent allowed to expect this term back. os.Stdout
		// is unbuffered, so the promise cannot sit in userspace past the kill.
		fmt.Printf("ACK %d\n", term)
	}
}
