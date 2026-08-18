package raft

import (
	"bytes"
	"sort"
	"testing"
)

// Checkers for the invariants the paper states in §5.3 and §5.4. They are kept
// separate from any one task's tests because every phase from here on has a
// reason to assert them: Phase D must show snapshotting preserves them, Phase E
// must show a restart from disk does, Phase H must show membership changes do.
//
// Each takes named logs so a failure says WHICH pair broke, not just that
// something did.

// namedLog pairs a log with a label for diagnostics.
type namedLog struct {
	name string
	log  []LogEntry
}

func (n *Node) logCopy() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry(nil), n.log...)
}

func entriesEqual(a, b LogEntry) bool {
	return a.Term == b.Term && bytes.Equal(a.Command, b.Command)
}

func logsEqual(a, b []LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !entriesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// assertLogMatching checks the Log Matching Property (§5.3) across every pair of
// logs given:
//
//	If two logs contain an entry with the same index and term, then the logs are
//	identical in all entries up through that index.
//
// This is the invariant the whole consistency check rests on. It is what makes
// verifying ONE position equivalent to verifying the entire prefix, and if it
// ever fails, every AppendEntries acceptance in the system was unjustified.
//
// Note that it says nothing about indices beyond the matching one. Two logs can
// satisfy Log Matching while having completely different tails -- that is
// exactly Figure 7 cases (c) and (d) after repair.
func assertLogMatching(t *testing.T, logs ...namedLog) {
	t.Helper()

	sort.Slice(logs, func(i, j int) bool { return logs[i].name < logs[j].name })

	for i := 0; i < len(logs); i++ {
		for j := i + 1; j < len(logs); j++ {
			a, b := logs[i], logs[j]

			shorter := len(a.log)
			if len(b.log) < shorter {
				shorter = len(b.log)
			}

			for idx := 0; idx < shorter; idx++ {
				if a.log[idx].Term != b.log[idx].Term {
					continue
				}
				// Same index, same term: everything up to here must be equal.
				if !logsEqual(a.log[:idx+1], b.log[:idx+1]) {
					t.Errorf("Log Matching violated between %s and %s at index %d "+
						"(both term %d):\n  %s = %v\n  %s = %v",
						a.name, b.name, idx, a.log[idx].Term,
						a.name, termsOf(a.log[:idx+1]),
						b.name, termsOf(b.log[:idx+1]))
					return
				}
			}
		}
	}
}

// assertPrefixIntact checks that a log still begins with the given entries.
//
// Used to pin the one bug in log repair that cannot be recovered from: a
// follower truncating away entries that were already committed. Every other
// mistake here costs round trips or bandwidth; this one loses data that clients
// were told was durable, and no later message repairs it because the leader
// believes the follower still has them.
func assertPrefixIntact(t *testing.T, name string, log []LogEntry, want []LogEntry) {
	t.Helper()

	if len(log) < len(want) {
		t.Errorf("%s: log has %d entries, shorter than the %d-entry committed "+
			"prefix: committed entries were truncated",
			name, len(log)-1, len(want)-1)
		return
	}
	if !logsEqual(log[:len(want)], want) {
		t.Errorf("%s: committed prefix was rewritten\n  have %v\n  want %v",
			name, termsOf(log[:len(want)]), termsOf(want))
	}
}
