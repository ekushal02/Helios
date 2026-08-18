package raft

import (
	"fmt"
	"sort"
	"testing"
)

// A monitor for the safety property that matters most, checked continuously
// over a run rather than asserted at one moment:
//
//	State Machine Safety (§5.4.3): if a server has applied a log entry at a
//	given index, no other server will ever apply a different entry for the
//	same index.
//
// Helios does not apply entries yet, so the monitor watches commitIndex, which
// is the decision that releases an entry to be applied. Once a node commits an
// entry at an index, no node may ever hold a DIFFERENT entry at that index
// within its own committed prefix.
//
// The qualifier is load-bearing. Nodes routinely hold divergent UNCOMMITTED
// entries -- that is the normal state of a follower whose leader was deposed,
// and Figure 8 depends on exactly such an entry surviving on S5 for two whole
// terms. A monitor that flagged any divergence at any index would fire on a
// correct implementation.

// commitLedger accumulates every entry that any node has ever committed, keyed
// by index, and reports the first contradiction it sees.
type commitLedger struct {
	committed map[int]LogEntry
	by        map[int]string // which node first committed each index, for messages
}

func newCommitLedger() *commitLedger {
	return &commitLedger{
		committed: map[int]LogEntry{},
		by:        map[int]string{},
	}
}

// observe records every node's committed prefix and checks it against
// everything recorded so far. Returns a description of the first violation
// found, or "" if the run is still safe.
//
// Returning a string rather than calling t.Errorf is deliberate: the mutation
// test needs to assert that a violation IS detected, which it cannot do if the
// monitor fails the test itself.
func (cl *commitLedger) observe(nodes map[string]*Node, when string) string {
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic reporting

	for _, name := range names {
		n := nodes[name]

		n.mu.Lock()
		commit := n.commitIndex
		log := append([]LogEntry(nil), n.log...)
		n.mu.Unlock()

		if commit > len(log)-1 {
			return fmt.Sprintf("%s: at %s, commitIndex %d exceeds its log of %d entries",
				name, when, commit, len(log)-1)
		}

		for idx := 1; idx <= commit; idx++ {
			entry := log[idx]

			prior, seen := cl.committed[idx]
			if !seen {
				cl.committed[idx] = entry
				cl.by[idx] = name
				continue
			}

			if !entriesEqual(prior, entry) {
				return fmt.Sprintf(
					"STATE MACHINE SAFETY VIOLATED at %s\n"+
						"  index %d was committed by %s as term %d (%q)\n"+
						"  %s now has term %d (%q) at that index, inside its own "+
						"committed prefix (commitIndex %d)\n"+
						"  an entry acknowledged to a client has been overwritten",
					when, idx, cl.by[idx], prior.Term, prior.Command,
					name, entry.Term, entry.Command, commit)
			}
		}
	}
	return ""
}

// mustBeSafe fails the test if the monitor found anything.
func (cl *commitLedger) mustBeSafe(t *testing.T, nodes map[string]*Node, when string) {
	t.Helper()
	if v := cl.observe(nodes, when); v != "" {
		t.Fatal(v)
	}
}
