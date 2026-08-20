package raft

import (
	"testing"
	"time"
)

// leaderInTerm builds a leader at the given term holding the given entry terms
// at indices 1..n, with the given number of peers.
//
// A LEADER WITH NO HEARTBEAT LOOP, and that is the whole point of the fixture.
//
// becomeLeader ends with `go n.heartbeatLoop(n.currentTerm)`, which fans out
// immediately rather than waiting for the first tick. recordingTransport
// answers Success to everything. So a fixture built through becomeLeader has,
// microseconds later, replicated to every peer, advanced every matchIndex to
// lastLogIndex, and run advanceCommitIndex on its own -- before the test body
// has set a single matchIndex.
//
// That raced every assertion in this file. Most survived by coincidence: the
// background commit happened to equal the expected answer, or §5.4.2 blocked
// it. Five did not, and they passed only because the test body usually won a
// very short race. TestScanStopsBelowTheCurrentTerm lost it first, and reported
// a §5.4.2 violation that had not occurred.
//
// So the leader state is installed directly. These tests exercise the commit
// DECISION, not the send path -- TestCommitAdvancesThroughReplication covers
// that, and does it by calling advanceFollower rather than by waiting on a
// network. A fixture that also replicates is not more realistic here, it is
// just nondeterministic.
//
// The three lines below mirror becomeLeader minus the goroutine. If becomeLeader
// grows a fourth, TestTheCommitFixtureIsInertUntilTheTestActs will not catch it
// -- but every test in this file will start failing in ways that point here.
func leaderInTerm(t *testing.T, term int, peers []int, entryTerms ...int) *Node {
	t.Helper()

	n := NewNode(0, peers, newRecordingTransport(term), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = term
	for _, et := range entryTerms {
		n.log = append(n.log, LogEntry{Term: et})
	}
	n.state = Leader
	n.leaderID = n.id
	n.initLeaderState()
	n.mu.Unlock()

	return n
}

// THE GUARD FOR EVERY OTHER TEST IN THIS FILE.
//
// The log here is entirely current-term and would be committed to index 2 by a
// single fan-out. Nothing sets a matchIndex, so if commitIndex moves at all,
// something in the fixture is talking to the network and every expectation in
// this file is a coin flip again.
//
// The sleep is three heartbeat intervals rather than one, because the first
// fan-out happens on entry to the loop and the point is to catch the tick as
// well.
func TestTheCommitFixtureIsInertUntilTheTestActs(t *testing.T) {
	n := leaderInTerm(t, 7, []int{1, 2, 3, 4}, 7, 7)

	time.Sleep(3 * heartbeatInterval)

	if got := commitIndexOf(n); got != 0 {
		t.Errorf("commitIndex = %d after %v of doing nothing, want 0: the "+
			"fixture is replicating in the background, so every commit "+
			"assertion in this file is racing it",
			got, 3*heartbeatInterval)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.peers {
		if got := n.matchIndex[p]; got != 0 {
			t.Errorf("matchIndex[%d] = %d, want 0: a reply handler ran without "+
				"the test sending anything", p, got)
		}
	}
}

// setMatch installs a matchIndex picture directly, so the commit decision can
// be tested without driving replication.
func setMatch(n *Node, match map[int]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for p, m := range match {
		n.matchIndex[p] = m
	}
	n.advanceCommitIndex()
}

func commitIndexOf(n *Node) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// --- the arithmetic --------------------------------------------------------

func TestQuorumSize(t *testing.T) {
	cases := []struct {
		peers []int
		want  int
	}{
		{[]int{1, 2}, 2},             // 3 nodes
		{[]int{1, 2, 3, 4}, 3},       // 5 nodes
		{[]int{1, 2, 3, 4, 5, 6}, 4}, // 7 nodes
		{[]int{1}, 2},                // 2 nodes: both, which is why even sizes are pointless
	}

	for _, tc := range cases {
		n := NewNode(0, tc.peers, newRecordingTransport(1), 1)
		t.Cleanup(n.Stop)

		if got := n.quorumSize(); got != tc.want {
			t.Errorf("%d nodes: quorumSize = %d, want %d", len(tc.peers)+1, got, tc.want)
		}
	}
}

// The leader counts itself. matchIndex has no entry for this node, so a count
// that iterated the map alone would be short by one and never commit anything
// in a three-node cluster.
func TestReplicaCountIncludesTheLeader(t *testing.T) {
	n := leaderInTerm(t, 5, []int{1, 2}, 5, 5, 5)

	n.mu.Lock()
	defer n.mu.Unlock()

	if got := n.replicaCount(3); got != 1 {
		t.Errorf("replicaCount with no followers acknowledged = %d, want 1 "+
			"(the leader holds its own log)", got)
	}

	n.matchIndex[1] = 3
	if got := n.replicaCount(3); got != 2 {
		t.Errorf("replicaCount = %d, want 2", got)
	}

	// A follower at index 3 holds everything below it too.
	if got := n.replicaCount(1); got != 2 {
		t.Errorf("replicaCount(1) = %d, want 2: holding index 3 implies "+
			"holding index 1", got)
	}
}

// --- the basic rule --------------------------------------------------------

func TestNoCommitWithoutAMajority(t *testing.T) {
	n := leaderInTerm(t, 5, []int{1, 2, 3, 4}, 5, 5, 5) // 5 nodes, quorum 3

	setMatch(n, map[int]int{1: 3}) // leader + 1 = 2 of 5

	if got := commitIndexOf(n); got != 0 {
		t.Errorf("commitIndex = %d, want 0: two of five is not a majority", got)
	}
}

func TestCommitAtExactlyAMajority(t *testing.T) {
	n := leaderInTerm(t, 5, []int{1, 2, 3, 4}, 5, 5, 5)

	setMatch(n, map[int]int{1: 3, 2: 3}) // leader + 2 = 3 of 5

	if got := commitIndexOf(n); got != 3 {
		t.Errorf("commitIndex = %d, want 3", got)
	}
}

// The commit index is the highest index a majority holds, not the highest any
// single node holds.
func TestCommitTakesTheMajorityIndexNotTheMaximum(t *testing.T) {
	n := leaderInTerm(t, 5, []int{1, 2, 3, 4}, 5, 5, 5, 5, 5)

	// One follower is fully caught up, two are at index 2. With the leader,
	// index 2 is on three of five; index 5 is on two.
	setMatch(n, map[int]int{1: 5, 2: 2, 3: 2})

	if got := commitIndexOf(n); got != 2 {
		t.Errorf("commitIndex = %d, want 2: index 5 is on two nodes of five", got)
	}
}

func TestCommitIndexIsMonotonic(t *testing.T) {
	n := leaderInTerm(t, 5, []int{1, 2}, 5, 5, 5)

	setMatch(n, map[int]int{1: 3})
	if got := commitIndexOf(n); got != 3 {
		t.Fatalf("setup: commitIndex = %d, want 3", got)
	}

	// A follower's matchIndex could never legitimately fall, but the commit
	// decision must not depend on that holding.
	n.mu.Lock()
	n.matchIndex[1] = 1
	n.advanceCommitIndex()
	n.mu.Unlock()

	if got := commitIndexOf(n); got != 3 {
		t.Errorf("commitIndex = %d, want 3: committed entries never uncommit", got)
	}
}

// A follower must never derive commitIndex. It cannot see the other followers'
// match state, so anything it concluded would be guesswork.
func TestOnlyALeaderCommitsByCounting(t *testing.T) {
	n := leaderInTerm(t, 5, []int{1, 2}, 5, 5, 5)

	n.mu.Lock()
	n.matchIndex[1] = 3
	n.becomeFollower(6)
	n.advanceCommitIndex()
	got := n.commitIndex
	n.mu.Unlock()

	if got != 0 {
		t.Errorf("commitIndex = %d, want 0: a follower counted a majority", got)
	}
}

// --- §5.4.2, the Figure 8 restriction --------------------------------------

// THE HEADLINE TEST FOR THIS TASK.
//
// Figure 8, state (c): S1 has restarted, won term 4, and finished replicating
// its INHERITED term-2 entry at index 2 to a majority. Counting says commit.
// §5.4.2 says no.
//
// The reason is state (d). S5, holding a term-3 entry at index 2, can still win
// term 5: it asks S2, S3 and S4 for votes, and its last-log term of 3 beats
// their 2, so the §5.4.1 check passes. It then overwrites index 2 everywhere.
// Had S1 committed on the count, an acknowledged entry would be gone.
func TestFigure8OldTermEntryIsNotCommittedByCounting(t *testing.T) {
	// S1 in term 4, log: index 1 term 1, index 2 term 2. Both inherited.
	n := leaderInTerm(t, 4, []int{1, 2, 3, 4}, 1, 2)

	// Index 2 now on S1, S2 and S3 -- three of five.
	setMatch(n, map[int]int{1: 2, 2: 2})

	if got := commitIndexOf(n); got != 0 {
		t.Errorf("commitIndex = %d, want 0: index 2 is from term 2 and this "+
			"leader is in term 4, so a majority holding it proves nothing "+
			"against a rival with a higher-term entry at the same index", got)
	}
}

// Figure 8, state (e). The same leader appends an entry in its OWN term and
// gets that onto a majority. Now every node in that majority has a last-log
// term of 4, no term-3 candidate can out-rank them, and index 3 is safe.
//
// Index 2 commits at the same moment -- indirectly. By Log Matching, a node
// holding index 3 holds index 2 identically.
func TestFigure8CurrentTermEntryCommitsThePrefix(t *testing.T) {
	n := leaderInTerm(t, 4, []int{1, 2, 3, 4}, 1, 2)

	setMatch(n, map[int]int{1: 2, 2: 2})
	if got := commitIndexOf(n); got != 0 {
		t.Fatalf("setup: commitIndex = %d, want 0", got)
	}

	// The leader appends in term 4 and replicates it to the same majority.
	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 4})
	n.mu.Unlock()

	setMatch(n, map[int]int{1: 3, 2: 3})

	if got := commitIndexOf(n); got != 3 {
		t.Errorf("commitIndex = %d, want 3: a current-term entry on a majority "+
			"is safe, and carries the whole prefix with it", got)
	}
}

// The contrast that isolates the term check as the operative difference.
// Identical replication picture, identical indices; only the entry's term
// differs, and the outcome flips.
func TestCommitTurnsOnTheEntryTermAlone(t *testing.T) {
	old := leaderInTerm(t, 4, []int{1, 2, 3, 4}, 1, 2)
	setMatch(old, map[int]int{1: 2, 2: 2})

	current := leaderInTerm(t, 4, []int{1, 2, 3, 4}, 1, 4)
	setMatch(current, map[int]int{1: 2, 2: 2})

	if got := commitIndexOf(old); got != 0 {
		t.Errorf("old-term entry: commitIndex = %d, want 0", got)
	}
	if got := commitIndexOf(current); got != 2 {
		t.Errorf("current-term entry: commitIndex = %d, want 2", got)
	}
}

// A leader inheriting a long tail of old-term entries commits none of them, no
// matter how well replicated, until it appends its own. This is the
// availability cost of §5.4.2 and the reason for the no-op-on-election remedy
// noted in commit.go.
func TestInheritedEntriesStayUncommittedUntilTheLeaderAppends(t *testing.T) {
	n := leaderInTerm(t, 9, []int{1, 2}, 3, 3, 4, 4, 6)

	setMatch(n, map[int]int{1: 5, 2: 5}) // fully replicated everywhere

	if got := commitIndexOf(n); got != 0 {
		t.Errorf("commitIndex = %d, want 0: every entry predates term 9", got)
	}

	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 9})
	n.mu.Unlock()
	setMatch(n, map[int]int{1: 6, 2: 6})

	if got := commitIndexOf(n); got != 6 {
		t.Errorf("commitIndex = %d, want 6: one current-term entry releases the "+
			"whole backlog", got)
	}
}

// The scan stops at the first old-term entry rather than continuing past it.
// Terms are non-decreasing along a log, so there is nothing below worth
// examining -- and examining it would risk committing exactly what §5.4.2
// forbids.
func TestScanStopsBelowTheCurrentTerm(t *testing.T) {
	// Current-term entries on top, older ones beneath, and the majority only
	// reaches into the old ones.
	n := leaderInTerm(t, 7, []int{1, 2, 3, 4}, 5, 5, 5, 7, 7)

	setMatch(n, map[int]int{1: 3, 2: 3}) // majority holds up to index 3, term 5

	if got := commitIndexOf(n); got != 0 {
		t.Errorf("commitIndex = %d, want 0: the majority reaches only into "+
			"term-5 entries", got)
	}
}

// --- through the real send path ---------------------------------------------

// The commit decision must be reachable from replication, not only from a
// direct call. This is the path a client write actually takes.
func TestCommitAdvancesThroughReplication(t *testing.T) {
	n := leaderInTerm(t, 5, []int{1, 2}, 5, 5)

	n.mu.Lock()
	n.advanceFollower(1, &AppendEntriesArgs{PrevLogIndex: 0, Entries: make([]LogEntry, 2)})
	got := n.commitIndex
	n.mu.Unlock()

	if got != 2 {
		t.Errorf("commitIndex = %d, want 2: one follower plus the leader is a "+
			"majority of three", got)
	}
}

// commitIndex can never exceed the log. Nothing should be able to produce this,
// which is why it is worth checking: a matchIndex above lastLogIndex would mean
// a follower was credited with entries the leader does not have.
func TestCommitIndexNeverExceedsTheLog(t *testing.T) {
	n := leaderInTerm(t, 5, []int{1, 2}, 5, 5)

	setMatch(n, map[int]int{1: 99, 2: 99})

	last := n.lastLogIndexLocked()
	if got := commitIndexOf(n); got > last {
		t.Errorf("commitIndex = %d, beyond lastLogIndex %d", got, last)
	}
}

func (n *Node) lastLogIndexLocked() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastLogIndex()
}