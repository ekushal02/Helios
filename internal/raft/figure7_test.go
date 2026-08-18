package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// --- helpers ---------------------------------------------------------------

// logFromTerms builds a log from the terms of indices 1..n, with the sentinel
// at index 0.
//
// Commands are derived from the term and index so that every entry in the
// system is byte-distinct. Terms alone are not enough to catch a merge that
// stores the right NUMBER of entries but the wrong ones: a leader log of
// 1,1,1,4,4,... has repeated terms, so an off-by-one in the slice being
// appended can leave a log whose terms are correct and whose contents are not.
func logFromTerms(terms ...int) []LogEntry {
	log := []LogEntry{{Term: 0}}
	for i, term := range terms {
		log = append(log, LogEntry{
			Term:    term,
			Command: []byte(fmt.Sprintf("t%d@%d", term, i+1)),
		})
	}
	return log
}

// termsOf drops the sentinel so failures print the shape Figure 7 draws.
func termsOf(log []LogEntry) []int {
	terms := make([]int, 0, len(log)-1)
	for _, e := range log[1:] {
		terms = append(terms, e.Term)
	}
	return terms
}

func (n *Node) logTerms() []int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return termsOf(n.log)
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// followerWithEntries builds a follower holding exactly the given log,
// sentinel included.
func followerWithEntries(t *testing.T, term int, log []LogEntry) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(term)), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = term
	n.log = append([]LogEntry(nil), log...)
	n.resetElectionTimer()
	n.mu.Unlock()

	return n
}

// repair simulates the leader-side backoff loop that C-7 will implement: start
// at the leader's last index and walk nextIndex down until the follower stops
// rejecting.
//
// This is a stand-in, not the real thing. Until nextIndex advances on success
// (C-6) and backs off on rejection (C-7), a real leader facing a diverged
// follower resends the same rejected message forever. Writing the loop here
// first makes the follower testable and doubles as a specification: if the real
// implementation needs materially more structure than this, the extra structure
// wants justifying. The two things it will legitimately need are a term-based
// bail-out (a rejection carrying a higher term means step down, not back off)
// and not blocking the heartbeat loop while repairing a slow follower.
//
// onStep runs after each round trip, so tests can assert invariants that must
// hold throughout repair rather than only at the end.
func repairObserved(t *testing.T, f *Node, leaderLog []LogEntry, leaderTerm int, onStep func(attempt int)) int {
	t.Helper()

	next := len(leaderLog) // == leader's lastLogIndex + 1
	limit := len(leaderLog) + 1

	for attempts := 1; ; attempts++ {
		if attempts > limit {
			t.Fatalf("repair did not converge in %d round trips; the sentinel "+
				"should guarantee termination. follower log %v", limit, f.logTerms())
		}

		prev := next - 1

		var reply AppendEntriesReply
		f.AppendEntries(&AppendEntriesArgs{
			Term:         leaderTerm,
			LeaderID:     1,
			PrevLogIndex: prev,
			PrevLogTerm:  leaderLog[prev].Term,
			Entries:      append([]LogEntry(nil), leaderLog[next:]...),
		}, &reply)

		if onStep != nil {
			onStep(attempts)
		}
		if reply.Success {
			return attempts
		}
		if next <= 1 {
			t.Fatalf("repair bottomed out at the sentinel without success; "+
				"follower log %v", f.logTerms())
		}
		next--
	}
}

func repair(t *testing.T, f *Node, leaderLog []LogEntry, leaderTerm int) int {
	t.Helper()
	return repairObserved(t, f, leaderLog, leaderTerm, nil)
}

// --- Figure 7 ---------------------------------------------------------------

// The leader for term 8, indices 1..10.
func figure7Leader() []LogEntry {
	return logFromTerms(1, 1, 1, 4, 4, 5, 5, 6, 6, 6)
}

// figure7Followers is the paper's catalogue, in the paper's order.
func figure7Followers() []struct {
	name  string
	terms []int
} {
	return []struct {
		name  string
		terms []int
	}{
		{"a", []int{1, 1, 1, 4, 4, 5, 5, 6, 6}},
		{"b", []int{1, 1, 1, 4}},
		{"c", []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}},
		{"d", []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}},
		{"e", []int{1, 1, 1, 4, 4, 4, 4}},
		{"f", []int{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}},
	}
}

// Every follower shares indices 1..3 at term 1, and nothing above that is
// common to all six. So 3 is the highest index that could plausibly have been
// committed before the term-8 leader took over -- which makes log[:4] the
// prefix that repair must never touch, in any scenario.
const figure7CommittedThrough = 3

// THE FIGURE 7 TEST.
//
// Every way a follower's log can be wrong when a new leader takes over: missing
// entries (a, b), extra uncommitted entries (c, d), and both at once (e, f).
//
// Phase one repairs against the leader's log as it stands, and asserts
// agreement on every index the leader SENT -- not identical logs. Followers c
// and d keep their entries at 11 and 12, because no message covered those
// positions. That is correct: those entries are uncommitted, and Log Matching
// constrains only indices someone has claimed.
//
// Phase two has the leader append at index 11, which is the first message to
// address those positions. It conflicts, and they go.
func TestFigure7Repair(t *testing.T) {
	cases := []struct {
		name            string
		follower        []int
		wantAfterRepair []int
		// wantAttempts is the distance from the leader's last index down to the
		// first agreeing position. It is the cost of one-index-at-a-time
		// backoff, and the quantitative argument for fast backup.
		wantAttempts int
	}{
		{
			name:            "a: one entry behind",
			follower:        []int{1, 1, 1, 4, 4, 5, 5, 6, 6},
			wantAfterRepair: []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6},
			wantAttempts:    2, // prev=10 too short, prev=9 matches
		},
		{
			name:            "b: six entries behind",
			follower:        []int{1, 1, 1, 4},
			wantAfterRepair: []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6},
			wantAttempts:    7, // prev=10 down to prev=4
		},
		{
			name:            "c: one extra uncommitted entry",
			follower:        []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6},
			wantAfterRepair: []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}, // 11 survives
			wantAttempts:    1,                                      // prev=10 matches immediately
		},
		{
			name:            "d: two extra uncommitted entries from a later term",
			follower:        []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7},
			wantAfterRepair: []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}, // 11,12 survive
			wantAttempts:    1,
		},
		{
			name:            "e: diverged from index 6",
			follower:        []int{1, 1, 1, 4, 4, 4, 4},
			wantAfterRepair: []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6},
			wantAttempts:    6, // prev=10..8 too short, 7 and 6 mismatch, 5 matches
		},
		{
			name:            "f: diverged from index 4, longer than the leader",
			follower:        []int{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3},
			wantAfterRepair: []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6},
			wantAttempts:    8, // prev=10 down to prev=3
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leaderLog := figure7Leader()
			f := followerWithEntries(t, 8, logFromTerms(tc.follower...))

			// The committed prefix, in the leader's version. It must survive
			// every round trip of repair, not merely be correct at the end.
			committed := leaderLog[:figure7CommittedThrough+1]

			attempts := repairObserved(t, f, leaderLog, 8, func(attempt int) {
				assertPrefixIntact(t, fmt.Sprintf("follower after attempt %d", attempt),
					f.logCopy(), committed)
			})

			if got := f.logTerms(); !equalInts(got, tc.wantAfterRepair) {
				t.Errorf("after repair log = %v, want %v", got, tc.wantAfterRepair)
			}
			if attempts != tc.wantAttempts {
				t.Errorf("repair took %d round trips, want %d", attempts, tc.wantAttempts)
			}

			// Terms are not enough. Every entry carries a distinct command, so
			// this catches a merge that appended the right count from the wrong
			// offset -- invisible in a terms-only comparison of a log with
			// repeated terms.
			got := f.logCopy()
			sent := len(leaderLog)
			if len(got) < sent {
				t.Fatalf("log has %d entries, shorter than the %d the leader sent",
					len(got)-1, sent-1)
			}
			if !logsEqual(got[:sent], leaderLog) {
				t.Errorf("entry contents differ from the leader over the sent range")
			}

			// Phase two: the leader appends in its own term.
			leaderLog = append(leaderLog, LogEntry{Term: 8, Command: []byte("t8@11")})
			repair(t, f, leaderLog, 8)

			if !logsEqual(f.logCopy(), leaderLog) {
				t.Errorf("after the leader appended, log = %v, want %v",
					f.logTerms(), termsOf(leaderLog))
			}
		})
	}
}

// The figure as a whole, rather than one row at a time.
//
// All six followers are repaired against ONE leader log, then checked pairwise.
// Repairing each in isolation cannot catch a leader that mutates its own log
// while repairing -- each subtest would hand the next a fresh copy and the
// damage would never compound.
func TestFigure7WholeClusterConverges(t *testing.T) {
	leaderLog := figure7Leader()
	original := append([]LogEntry(nil), leaderLog...)

	logs := []namedLog{{"leader", leaderLog}}

	for _, spec := range figure7Followers() {
		f := followerWithEntries(t, 8, logFromTerms(spec.terms...))
		repair(t, f, leaderLog, 8)
		logs = append(logs, namedLog{"follower " + spec.name, f.logCopy()})
	}

	// REPAIR IS ONE-DIRECTIONAL. A follower's state must never flow back into
	// the leader's log. Nothing in the current code could do this, which is
	// precisely why it is worth pinning: it is the kind of invariant a later
	// optimisation quietly breaks.
	if !logsEqual(leaderLog, original) {
		t.Errorf("the leader's log changed during repair:\n  have %v\n  want %v",
			termsOf(leaderLog), termsOf(original))
	}

	// Every follower agrees with the leader over what the leader sent.
	for _, l := range logs[1:] {
		if len(l.log) < len(leaderLog) {
			t.Errorf("%s: %d entries, shorter than the leader's %d",
				l.name, len(l.log)-1, len(leaderLog)-1)
			continue
		}
		if !logsEqual(l.log[:len(leaderLog)], leaderLog) {
			t.Errorf("%s disagrees with the leader over the sent range: %v",
				l.name, termsOf(l.log))
		}
	}

	// And the property that gives the consistency check its meaning, across
	// every pair including the two followers still holding uncommitted tails.
	assertLogMatching(t, logs...)
}

// The cost table. Not an assertion -- a measurement, printed so the effect of
// fast backup is visible as a before-and-after rather than an argument.
func TestFigure7RepairCost(t *testing.T) {
	leaderLog := figure7Leader()

	total := 0
	t.Log("scenario                          round trips")
	for _, spec := range figure7Followers() {
		f := followerWithEntries(t, 8, logFromTerms(spec.terms...))
		n := repair(t, f, leaderLog, 8)
		total += n
		t.Logf("  (%s) %-28v %d", spec.name, spec.terms, n)
	}
	t.Logf("  total %d round trips across 6 followers, leader log of %d entries",
		total, len(leaderLog)-1)
	t.Log("  each round trip is one heartbeat interval when repair rides the tick")
}

// --- beyond the figure -----------------------------------------------------

// Six hand-picked cases are six samples. This walks a much larger slice of the
// space: an arbitrary common prefix followed by an arbitrary divergent tail.
//
// Seeded so failures reproduce. The seed is reported on failure, which is the
// only thing that makes a randomised test debuggable.
func TestRandomisedDivergenceConverges(t *testing.T) {
	const trials = 300
	const seed = 20260817

	rng := rand.New(rand.NewSource(seed))
	leaderLog := figure7Leader()
	leaderLast := len(leaderLog) - 1

	for trial := 0; trial < trials; trial++ {
		trialSeed := seed*1_000_003 + int64(trial)
		r := rand.New(rand.NewSource(trialSeed))

		// Share an arbitrary prefix of the leader's log, then diverge.
		prefix := r.Intn(leaderLast + 1) // 0..10 entries in common
		followerTerms := termsOf(leaderLog[:prefix+1])

		tail := r.Intn(6)
		for i := 0; i < tail; i++ {
			// Terms 2, 3 and 7 never appear in the leader's log at a position
			// where they could accidentally agree.
			followerTerms = append(followerTerms, []int{2, 3, 7}[r.Intn(3)])
		}

		f := followerWithEntries(t, 8, logFromTerms(followerTerms...))
		attempts := repair(t, f, leaderLog, 8)

		got := f.logCopy()
		if len(got) < len(leaderLog) || !logsEqual(got[:len(leaderLog)], leaderLog) {
			t.Fatalf("trial %d (seed %d): follower %v did not converge to the "+
				"leader over the sent range; got %v",
				trial, trialSeed, followerTerms, termsOf(got))
		}

		// Termination: repair can never need more steps than there are indices
		// to walk down, because the sentinel always matches.
		if attempts > leaderLast+1 {
			t.Fatalf("trial %d (seed %d): repair took %d round trips, more than "+
				"the %d indices available", trial, trialSeed, attempts, leaderLast+1)
		}

		assertLogMatching(t,
			namedLog{"leader", leaderLog},
			namedLog{"follower", got},
		)
		_ = rng
	}
}

// --- the out-of-order hazard ------------------------------------------------

// THE TEST THAT CATCHES BLIND TRUNCATION.
//
// A naive rule 3 -- truncate at PrevLogIndex, then append -- passes every
// Figure 7 case above and fails here.
//
// A leader sends 1..5 and then 1..8. The 1..8 message arrives first and this
// node stores eight entries. The stale 1..5 message then arrives with
// PrevLogIndex 0, which PASSES the consistency check because the sentinel always
// matches. Truncating there would destroy entries 6, 7 and 8 -- which the leader
// may already have committed on the strength of a majority including this node.
func TestStaleMessageDoesNotTruncateNewerEntries(t *testing.T) {
	leaderLog := logFromTerms(4, 4, 4, 4, 4, 4, 4, 4)
	f := followerWithEntries(t, 4, logFromTerms())

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term:         4,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      append([]LogEntry(nil), leaderLog[1:]...),
	}, &reply)

	if !reply.Success || len(f.logTerms()) != 8 {
		t.Fatalf("setup: log = %v, want 8 entries", f.logTerms())
	}

	// The delayed 1..5 message arrives. Everything in it already matches, so
	// nothing may change.
	f.AppendEntries(&AppendEntriesArgs{
		Term:         4,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      append([]LogEntry(nil), leaderLog[1:6]...),
	}, &reply)

	if !reply.Success {
		t.Error("rejected a message whose entries it already holds")
	}
	if !logsEqual(f.logCopy(), leaderLog) {
		t.Errorf("log = %v, want %v: a stale message truncated entries the "+
			"follower had already accepted", f.logTerms(), termsOf(leaderLog))
	}
}

// Re-delivering the same message must be a no-op. This is what lets the leader
// resend freely without sequence numbers or deduplication.
func TestDuplicateMessageIsIdempotent(t *testing.T) {
	leaderLog := figure7Leader()
	f := followerWithEntries(t, 8, leaderLog[:4])

	args := &AppendEntriesArgs{
		Term:         8,
		LeaderID:     1,
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		Entries:      append([]LogEntry(nil), leaderLog[4:]...),
	}

	var reply AppendEntriesReply
	for i := 0; i < 3; i++ {
		f.AppendEntries(args, &reply)
		if !reply.Success {
			t.Fatalf("delivery %d was rejected", i+1)
		}
		if !logsEqual(f.logCopy(), leaderLog) {
			t.Fatalf("after delivery %d log = %v, want %v",
				i+1, f.logTerms(), termsOf(leaderLog))
		}
	}
}

// A message that overlaps the existing log and extends past it appends only the
// new tail, without disturbing the matching prefix.
func TestPartialOverlapAppendsOnlyTheTail(t *testing.T) {
	full := logFromTerms(5, 5, 5, 5, 5)
	f := followerWithEntries(t, 5, full[:4])

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  5,
		Entries:      append([]LogEntry(nil), full[2:]...),
	}, &reply)

	if !reply.Success {
		t.Fatal("rejected a message with a matching PrevLogIndex")
	}
	if !logsEqual(f.logCopy(), full) {
		t.Errorf("log = %v, want %v", f.logTerms(), termsOf(full))
	}
}

// Truncation happens at the first differing term inside the message, not at
// PrevLogIndex and not at the start of the log.
func TestTruncationStartsAtTheFirstConflict(t *testing.T) {
	f := followerWithEntries(t, 9, logFromTerms(6, 6, 7, 7, 7))

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term:         9,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  6,
		Entries: []LogEntry{
			{Term: 6, Command: []byte("t6@2")}, // index 2, matches
			{Term: 9, Command: []byte("new3")}, // index 3, CONFLICT
			{Term: 9, Command: []byte("new4")}, // index 4
		},
	}, &reply)

	if !reply.Success {
		t.Fatal("rejected a message with a matching PrevLogIndex")
	}
	// Index 5 is gone because rule 3 deletes the conflicting entry AND ALL THAT
	// FOLLOW, not just the ones the message replaces.
	if got, want := f.logTerms(), []int{6, 6, 9, 9}; !equalInts(got, want) {
		t.Errorf("log = %v, want %v", got, want)
	}
}

// An empty message never changes the log, however far behind the leader's view
// of this follower is.
func TestHeartbeatNeverModifiesTheLog(t *testing.T) {
	f := followerWithEntries(t, 8, logFromTerms(1, 1, 1, 4, 4))
	before := f.logCopy()

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term:         8,
		LeaderID:     1,
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		Entries:      nil,
	}, &reply)

	if !reply.Success {
		t.Fatal("rejected a heartbeat whose PrevLogIndex matches")
	}
	if !logsEqual(f.logCopy(), before) {
		t.Errorf("log = %v, want %v unchanged: an empty message truncated",
			f.logTerms(), termsOf(before))
	}
}

// The follower's log must not alias the message it came from. The real
// transport gob-copies, but nothing in the receiver may depend on that.
func TestStoredEntriesDoNotAliasTheMessage(t *testing.T) {
	f := followerWithEntries(t, 5, logFromTerms())

	entries := []LogEntry{{Term: 5}, {Term: 5}}

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
	}, &reply)

	entries[0].Term = 99

	got := f.logTerms()
	if len(got) != 2 {
		t.Fatalf("log = %v, want 2 entries stored", got)
	}
	if got[0] != 5 {
		t.Errorf("log[1].Term = %d, want 5: the log aliases the message's slice", got[0])
	}
}
