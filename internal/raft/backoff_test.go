package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- the pure decision -----------------------------------------------------

func TestNextIndexAfterConflict(t *testing.T) {
	leader := figure7Leader() // terms 1,1,1,4,4,5,5,6,6,6

	cases := []struct {
		name          string
		current       int
		conflictIndex int
		conflictTerm  int
		want          int
		why           string
	}{
		{
			name: "follower too short", current: 11,
			conflictIndex: 5, conflictTerm: noConflictTerm,
			want: 5,
			why:  "jump straight to the end of what the follower has",
		},
		{
			name: "term the leader also has", current: 8,
			conflictIndex: 4, conflictTerm: 4,
			want: 6,
			why:  "leader's run of term 4 ends at index 5, so 1..5 are genuine",
		},
		{
			name: "term the leader has never seen", current: 11,
			conflictIndex: 7, conflictTerm: 3,
			want: 7,
			why:  "term 3 never entered the winning history; discard the whole run",
		},
		{
			name: "leader has exactly one entry of that term", current: 11,
			conflictIndex: 2, conflictTerm: 1,
			want: 4,
			why:  "leader's run of term 1 ends at index 3",
		},
		{
			name: "malformed hint pointing forwards", current: 5,
			conflictIndex: 99, conflictTerm: noConflictTerm,
			want: 4,
			why:  "must still make progress: clamped to one below current",
		},
		{
			name: "malformed hint below the sentinel", current: 3,
			conflictIndex: -7, conflictTerm: noConflictTerm,
			want: 1,
			why:  "index 0 is the sentinel and may never be resumed from",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nextIndexAfterConflict(leader, tc.current, tc.conflictIndex, tc.conflictTerm)
			if got != tc.want {
				t.Errorf("nextIndexAfterConflict(current=%d, idx=%d, term=%d) = %d, want %d\n  %s",
					tc.current, tc.conflictIndex, tc.conflictTerm, got, tc.want, tc.why)
			}
		})
	}
}

// THE SAFETY PROPERTY FOR THE WHOLE OPTIMISATION.
//
// Fast backup may only ever move nextIndex backwards. Moving it forward would
// have the leader assume agreement it never verified, skipping a real
// divergence -- and unlike a wasted round trip, that is unrecoverable.
func TestBackoffAlwaysMovesBackwards(t *testing.T) {
	leader := figure7Leader()

	for current := 1; current <= len(leader); current++ {
		for idx := -3; idx <= len(leader)+3; idx++ {
			for _, term := range []int{noConflictTerm, 1, 2, 3, 4, 5, 6, 7, 9} {
				got := nextIndexAfterConflict(leader, current, idx, term)

				if current > 1 && got >= current {
					t.Fatalf("current=%d idx=%d term=%d gave %d: backoff moved forward",
						current, idx, term, got)
				}
				if current == 1 && got != 1 {
					t.Fatalf("current=1 idx=%d term=%d gave %d: the floor must "+
						"hold at the sentinel", idx, term, got)
				}
				if got < 1 {
					t.Fatalf("current=%d idx=%d term=%d gave %d: below the sentinel",
						current, idx, term, got)
				}
			}
		}
	}
}

// --- the follower's half ---------------------------------------------------

func TestConflictHintTooShort(t *testing.T) {
	f := followerWithEntries(t, 8, logFromTerms(1, 1, 1, 4))

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term: 8, LeaderID: 1,
		PrevLogIndex: 10, PrevLogTerm: 6,
	}, &reply)

	if reply.Success {
		t.Fatal("accepted a message about an index it does not have")
	}
	if reply.ConflictTerm != noConflictTerm {
		t.Errorf("ConflictTerm = %d, want 0: the rejection was about length, "+
			"not a term disagreement", reply.ConflictTerm)
	}
	if reply.ConflictIndex != 5 {
		t.Errorf("ConflictIndex = %d, want 5 (lastLogIndex+1)", reply.ConflictIndex)
	}
}

func TestConflictHintReportsStartOfTermRun(t *testing.T) {
	// Figure 7 (f): three entries of term 2, then five of term 3.
	f := followerWithEntries(t, 8, logFromTerms(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3))

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term: 8, LeaderID: 1,
		PrevLogIndex: 10, PrevLogTerm: 6,
	}, &reply)

	if reply.Success {
		t.Fatal("accepted a message whose PrevLogTerm disagrees")
	}
	if reply.ConflictTerm != 3 {
		t.Errorf("ConflictTerm = %d, want 3 (what this node holds at index 10)",
			reply.ConflictTerm)
	}
	if reply.ConflictIndex != 7 {
		t.Errorf("ConflictIndex = %d, want 7: the FIRST index of the term 3 run, "+
			"which is what lets the leader skip all five at once", reply.ConflictIndex)
	}
}

// A term-rejection carries no hint. It is not about any log, and a leader that
// backed off on it would move nextIndex for the wrong reason.
func TestStaleTermRejectionCarriesNoHint(t *testing.T) {
	f := followerWithEntries(t, 9, logFromTerms(1, 1, 1))

	var reply AppendEntriesReply
	f.AppendEntries(&AppendEntriesArgs{
		Term: 4, LeaderID: 1,
		PrevLogIndex: 99, PrevLogTerm: 1,
	}, &reply)

	if reply.ConflictIndex != 0 || reply.ConflictTerm != 0 {
		t.Errorf("ConflictIndex=%d ConflictTerm=%d, want both 0",
			reply.ConflictIndex, reply.ConflictTerm)
	}
}

// --- the leader's half -----------------------------------------------------

func TestAdvanceOnSuccessUsesWhatWasSent(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	n.mu.Lock()
	for i := 0; i < 6; i++ {
		n.log = append(n.log, LogEntry{Term: 5})
	}
	args := &AppendEntriesArgs{PrevLogIndex: 2, Entries: make([]LogEntry, 3)}

	// The log grows AFTER the message left, as it does under a live client.
	n.log = append(n.log, LogEntry{Term: 5})

	n.advanceFollower(1, args)

	got, next := n.matchIndex[1], n.nextIndex[1]
	n.mu.Unlock()

	if got != 5 {
		t.Errorf("matchIndex = %d, want 5 (PrevLogIndex 2 + 3 entries): it must "+
			"be derived from what was SENT, not the log's current end", got)
	}
	if next != 6 {
		t.Errorf("nextIndex = %d, want 6", next)
	}
}

// Replies arrive out of order. A late reply to an older, shorter message must
// not un-prove agreement already counted toward a commit.
func TestMatchIndexIsMonotonic(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.advanceFollower(1, &AppendEntriesArgs{PrevLogIndex: 0, Entries: make([]LogEntry, 8)})
	if n.matchIndex[1] != 8 {
		t.Fatalf("setup: matchIndex = %d, want 8", n.matchIndex[1])
	}

	// The stale reply to a 1..5 message lands afterwards.
	n.advanceFollower(1, &AppendEntriesArgs{PrevLogIndex: 0, Entries: make([]LogEntry, 5)})

	if n.matchIndex[1] != 8 {
		t.Errorf("matchIndex = %d, want 8: a late reply to an older message "+
			"dragged it backwards", n.matchIndex[1])
	}
}

// The same hazard on the rejection path, where it live-locks instead of
// mis-committing: an old rejection undoes backoff progress, forever.
func TestStaleRejectionDoesNotUndoBackoff(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	n.mu.Lock()
	defer n.mu.Unlock()

	for i := 0; i < 10; i++ {
		n.log = append(n.log, LogEntry{Term: 5})
	}
	n.nextIndex[1] = 4 // backoff has already made progress

	// A rejection to the message sent when nextIndex was still 11.
	n.backOffFollower(1,
		&AppendEntriesArgs{PrevLogIndex: 10},
		&AppendEntriesReply{ConflictIndex: 9, ConflictTerm: noConflictTerm})

	if n.nextIndex[1] != 4 {
		t.Errorf("nextIndex = %d, want 4: a rejection answering a superseded "+
			"attempt moved it", n.nextIndex[1])
	}
}

// --- end to end ------------------------------------------------------------

// liveFollower routes the leader's RPCs into a real follower Node and counts
// them, so repair is measured through both real implementations rather than a
// test-local model of one of them.
type liveFollower struct {
	f *Node

	mu      sync.Mutex
	appends int
}

func (lf *liveFollower) SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return false
}

func (lf *liveFollower) SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	lf.mu.Lock()
	lf.appends++
	lf.mu.Unlock()

	lf.f.AppendEntries(args, reply)
	return true
}

func (lf *liveFollower) count() int {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	return lf.appends
}

// A real leader repairing a real follower, with no test-local backoff loop
// anywhere. This is what the Figure 7 tests could not check.
func TestLeaderRepairsFollowerEndToEnd(t *testing.T) {
	for _, spec := range figure7Followers() {
		t.Run(spec.name, func(t *testing.T) {
			follower := followerWithEntries(t, 8, logFromTerms(spec.terms...))
			lf := &liveFollower{f: follower}

			leaderLog := figure7Leader()
			leader := NewNode(0, []int{1}, lf, 1)
			t.Cleanup(leader.Stop)

			leader.mu.Lock()
			leader.log = append([]LogEntry(nil), leaderLog...)
			leader.state = Candidate
			leader.currentTerm = 8
			leader.becomeLeader()
			leader.mu.Unlock()

			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				leader.mu.Lock()
				match := leader.matchIndex[1]
				leader.mu.Unlock()

				if match == len(leaderLog)-1 {
					if !logsEqual(follower.logCopy()[:len(leaderLog)], leaderLog) {
						t.Fatalf("matchIndex reached %d but the follower's log is %v",
							match, follower.logTerms())
					}
					return
				}
				time.Sleep(time.Millisecond)
			}

			leader.mu.Lock()
			match, next := leader.matchIndex[1], leader.nextIndex[1]
			leader.mu.Unlock()
			t.Fatalf("did not converge in 2s: matchIndex=%d nextIndex=%d, follower %v",
				match, next, follower.logTerms())
		})
	}
}

// --- the measurement -------------------------------------------------------

// simulateRepair drives a real follower with a leader-side loop using the given
// backoff strategy, and returns the round trips to convergence.
//
// The follower is the real implementation; only the leader's choice of where to
// resume is swapped, which is exactly the variable under study.
func simulateRepair(t *testing.T, leaderLog []LogEntry, followerTerms []int,
	backoff func(current int, reply AppendEntriesReply) int) int {
	t.Helper()

	f := followerWithEntries(t, 8, logFromTerms(followerTerms...))
	next := len(leaderLog)

	for rounds := 1; rounds <= 64; rounds++ {
		var reply AppendEntriesReply
		f.AppendEntries(&AppendEntriesArgs{
			Term: 8, LeaderID: 1,
			PrevLogIndex: next - 1,
			PrevLogTerm:  leaderLog[next-1].Term,
			Entries:      append([]LogEntry(nil), leaderLog[next:]...),
		}, &reply)

		if reply.Success {
			if !logsEqual(f.logCopy()[:len(leaderLog)], leaderLog) {
				t.Fatalf("converged to the wrong log: %v", f.logTerms())
			}
			return rounds
		}
		next = backoff(next, reply)
	}
	t.Fatalf("repair did not converge in 64 rounds; follower %v", f.logTerms())
	return 0
}

func naiveBackoff(current int, _ AppendEntriesReply) int {
	if current > 1 {
		return current - 1
	}
	return 1
}

// THE MEASUREMENT.
//
// Both strategies over the same six scenarios, driving the same real follower.
// The saving is not uniform and that is the finding: cases (c) and (d) already
// converged in one round trip, and (a) diverged by exactly one index, so there
// was nothing for the hint to skip. Fast backup buys nothing on a healthy
// cluster -- it is insurance against the rare bad case.
func TestFastBackupRoundTripSaving(t *testing.T) {
	leaderLog := figure7Leader()
	hinted := func(current int, reply AppendEntriesReply) int {
		return nextIndexAfterConflict(leaderLog, current, reply.ConflictIndex, reply.ConflictTerm)
	}

	want := map[string]struct{ naive, fast int }{
		"a": {2, 2}, // one index behind: nothing to skip
		"b": {7, 2}, // six behind: one jump to the end of the follower's log
		"c": {1, 1}, // already agreed at index 10
		"d": {1, 1},
		"e": {6, 3}, // skip past the leader's own run of term 4
		"f": {8, 3}, // discard term 3, then term 2, each in one step
	}

	var totalNaive, totalFast int

	t.Log("scenario                            naive   fast   saved")
	for _, spec := range figure7Followers() {
		naive := simulateRepair(t, leaderLog, spec.terms, naiveBackoff)
		fast := simulateRepair(t, leaderLog, spec.terms, hinted)

		totalNaive += naive
		totalFast += fast

		t.Logf("  (%s) %-30v %3d    %3d    %3d", spec.name, spec.terms, naive, fast, naive-fast)

		w := want[spec.name]
		if naive != w.naive {
			t.Errorf("(%s) naive took %d round trips, want %d", spec.name, naive, w.naive)
		}
		if fast != w.fast {
			t.Errorf("(%s) fast backup took %d round trips, want %d", spec.name, fast, w.fast)
		}
		if fast > naive {
			t.Errorf("(%s) fast backup was SLOWER: %d vs %d", spec.name, fast, naive)
		}
	}

	t.Logf("  %-34s %3d    %3d    %3d", "total", totalNaive, totalFast, totalNaive-totalFast)
	t.Logf("  at a %v heartbeat interval: %v -> %v worst case (scenario f)",
		heartbeatInterval,
		time.Duration(want["f"].naive)*heartbeatInterval,
		time.Duration(want["f"].fast)*heartbeatInterval)

	if totalFast >= totalNaive {
		t.Errorf("fast backup saved nothing overall: %d vs %d", totalFast, totalNaive)
	}
}

// Both strategies must reach the same log. An optimisation that converges
// faster to a different answer is not an optimisation.
func TestBothStrategiesConvergeIdentically(t *testing.T) {
	leaderLog := figure7Leader()
	hinted := func(current int, reply AppendEntriesReply) int {
		return nextIndexAfterConflict(leaderLog, current, reply.ConflictIndex, reply.ConflictTerm)
	}

	for _, spec := range figure7Followers() {
		naiveNode := followerWithEntries(t, 8, logFromTerms(spec.terms...))
		fastNode := followerWithEntries(t, 8, logFromTerms(spec.terms...))

		runTo(t, naiveNode, leaderLog, naiveBackoff)
		runTo(t, fastNode, leaderLog, hinted)

		if !logsEqual(naiveNode.logCopy(), fastNode.logCopy()) {
			t.Errorf("(%s) strategies converged differently:\n  naive %v\n  fast  %v",
				spec.name, naiveNode.logTerms(), fastNode.logTerms())
		}
	}
}

func runTo(t *testing.T, f *Node, leaderLog []LogEntry, backoff func(int, AppendEntriesReply) int) {
	t.Helper()

	next := len(leaderLog)
	for rounds := 0; rounds < 64; rounds++ {
		var reply AppendEntriesReply
		f.AppendEntries(&AppendEntriesArgs{
			Term: 8, LeaderID: 1,
			PrevLogIndex: next - 1,
			PrevLogTerm:  leaderLog[next-1].Term,
			Entries:      append([]LogEntry(nil), leaderLog[next:]...),
		}, &reply)
		if reply.Success {
			return
		}
		next = backoff(next, reply)
	}
	t.Fatal(fmt.Sprintf("no convergence; follower %v", f.logTerms()))
}
