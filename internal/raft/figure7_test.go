package raft

import (
	"testing"
)

// --- helpers ---------------------------------------------------------------

// logFromTerms builds a log from the terms of indices 1..n, with the sentinel
// at index 0. Commands are irrelevant to conflict handling -- only terms and
// positions decide it -- so they are left nil.
func logFromTerms(terms ...int) []LogEntry {
	log := []LogEntry{{Term: 0}}
	for _, term := range terms {
		log = append(log, LogEntry{Term: term})
	}
	return log
}

// termsOf is the inverse, dropping the sentinel so failures print the same
// shape Figure 7 draws.
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

// repair simulates the leader-side backoff loop that C-7 will implement:
// start at the leader's last index and walk nextIndex down until the follower
// stops rejecting, then let it accept everything from there.
//
// Writing it here first serves two purposes. It makes the follower testable
// before C-7 exists, and it is a working specification for that task -- if the
// real implementation cannot be described this simply, something has drifted.
//
// Returns the number of round trips repair took.
func repair(t *testing.T, f *Node, leaderLog []LogEntry, leaderTerm int) int {
	t.Helper()

	next := len(leaderLog) // == leader's lastLogIndex + 1
	for attempts := 1; ; attempts++ {
		prev := next - 1

		var reply AppendEntriesReply
		f.AppendEntries(&AppendEntriesArgs{
			Term:         leaderTerm,
			LeaderID:     1,
			PrevLogIndex: prev,
			PrevLogTerm:  leaderLog[prev].Term,
			Entries:      append([]LogEntry(nil), leaderLog[next:]...),
		}, &reply)

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

// --- Figure 7 --------------------------------------------------------------

// The leader for term 8, indices 1..10.
func figure7Leader() []LogEntry {
	return logFromTerms(1, 1, 1, 4, 4, 5, 5, 6, 6, 6)
}

// THE FIGURE 7 TEST.
//
// Every way a follower's log can be wrong when a new leader takes over: missing
// entries (a, b), extra uncommitted entries (c, d), and both at once (e, f).
//
// Phase one repairs each follower against the leader's log as it stands. Note
// what is asserted: agreement on every index the leader SENT, not identical
// logs. Followers c and d keep their trailing entries at 11 and 12, because no
// message covered those positions. That is correct -- those entries are
// uncommitted, and Log Matching says nothing about indices nobody has claimed.
//
// Phase two has the leader append at index 11, which finally conflicts with
// what c and d are holding and clears them out.
func TestFigure7Repair(t *testing.T) {
	cases := []struct {
		name     string
		follower []int
		// wantAfterRepair is the follower's whole log once repair converges,
		// INCLUDING any trailing entries the leader never addressed.
		wantAfterRepair []int
		// wantAttempts is how many round trips backoff should take. It is the
		// distance from the leader's last index down to the first agreeing
		// position, and it is why fast backup exists.
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
			f := followerWithLog(t, 8, tc.follower...)

			attempts := repair(t, f, leaderLog, 8)

			if got := f.logTerms(); !equalInts(got, tc.wantAfterRepair) {
				t.Errorf("after repair log = %v, want %v", got, tc.wantAfterRepair)
			}
			if attempts != tc.wantAttempts {
				t.Errorf("repair took %d round trips, want %d", attempts, tc.wantAttempts)
			}

			// Phase two: the leader appends at index 11 in its own term. This
			// is the first message that addresses the positions c and d are
			// still holding, so it conflicts and clears them.
			leaderLog = append(leaderLog, LogEntry{Term: 8})
			repair(t, f, leaderLog, 8)

			if got, want := f.logTerms(), termsOf(leaderLog); !equalInts(got, want) {
				t.Errorf("after the leader appended, log = %v, want %v", got, want)
			}
		})
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
	f := followerWithLog(t, 4)

	// The 1..8 message lands first.
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
	if got := f.logTerms(); len(got) != 8 {
		t.Errorf("log = %v (%d entries), want 8: a stale message truncated "+
			"entries the follower had already accepted", got, len(got))
	}
}

// Re-delivering the same message must be a no-op. This is what lets the leader
// resend freely without sequence numbers or deduplication.
func TestDuplicateMessageIsIdempotent(t *testing.T) {
	leaderLog := figure7Leader()
	f := followerWithLog(t, 8, 1, 1, 1)

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
		if got, want := f.logTerms(), termsOf(leaderLog); !equalInts(got, want) {
			t.Fatalf("after delivery %d log = %v, want %v", i+1, got, want)
		}
	}
}

// A message that overlaps the existing log and extends past it appends only the
// new tail, without disturbing the matching prefix.
func TestPartialOverlapAppendsOnlyTheTail(t *testing.T) {
	f := followerWithLog(t, 5, 5, 5, 5)

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  5,
		Entries: []LogEntry{
			{Term: 5}, // index 2, already held
			{Term: 5}, // index 3, already held
			{Term: 5}, // index 4, new
			{Term: 5}, // index 5, new
		},
	}, &reply)

	if !reply.Success {
		t.Fatal("rejected a message with a matching PrevLogIndex")
	}
	if got, want := f.logTerms(), []int{5, 5, 5, 5, 5}; !equalInts(got, want) {
		t.Errorf("log = %v, want %v", got, want)
	}
}

// Truncation happens at the first differing term inside the message, not at
// PrevLogIndex and not at the start of the log.
func TestTruncationStartsAtTheFirstConflict(t *testing.T) {
	f := followerWithLog(t, 9, 6, 6, 7, 7, 7)

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term:         9,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  6,
		Entries: []LogEntry{
			{Term: 6}, // index 2, matches
			{Term: 9}, // index 3, CONFLICT (follower has 7)
			{Term: 9}, // index 4
		},
	}, &reply)

	if !reply.Success {
		t.Fatal("rejected a message with a matching PrevLogIndex")
	}
	// Indices 3, 4 and 5 go; the leader's 3 and 4 replace them. Index 5 is gone
	// because rule 3 deletes the conflicting entry AND ALL THAT FOLLOW.
	if got, want := f.logTerms(), []int{6, 6, 9, 9}; !equalInts(got, want) {
		t.Errorf("log = %v, want %v", got, want)
	}
}

// An empty message never changes the log, however far behind the leader's view
// of this follower is.
func TestHeartbeatNeverModifiesTheLog(t *testing.T) {
	f := followerWithLog(t, 8, 1, 1, 1, 4, 4)
	before := f.logTerms()

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
	if got := f.logTerms(); !equalInts(got, before) {
		t.Errorf("log = %v, want %v unchanged: an empty message truncated", got, before)
	}
}

// The follower's log must not alias the message it came from. The real
// transport gob-copies, but nothing in the receiver may depend on that.
func TestStoredEntriesDoNotAliasTheMessage(t *testing.T) {
	f := followerWithLog(t, 5)

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

	if got := f.logTerms(); got[0] != 5 {
		t.Errorf("log[1].Term = %d, want 5: the log aliases the message's slice", got[0])
	}
}
