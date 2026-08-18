package raft

// Fast backup (§5.3). NOT part of Figure 2 -- Figure 2's AppendEntriesReply
// carries only Term and Success, and a leader that decrements nextIndex by one
// per rejection is correct. Everything here is a latency optimisation, and it
// must be judged by one rule: it may only ever move nextIndex BACKWARDS, and
// never past the true divergence point in a way that skips a real conflict.
// Overshooting forward would have the leader believe agreement it has not
// verified; overshooting backwards merely costs the round trips it was meant to
// save.

// noConflictTerm marks a rejection caused by the follower's log being too short
// rather than by a term mismatch. Zero is safe as a sentinel because it is the
// term of the log's index-0 sentinel, which no real entry ever carries.
const noConflictTerm = 0

// conflictHint describes WHY this node rejected an AppendEntries, so the leader
// can back off by more than one index.
//
// Two shapes, distinguished by whether conflictTerm is set:
//
//	too short      term 0, index = lastLogIndex+1. "I have nothing at or above
//	               here; start from the end of what I do have."
//	term mismatch  term = whatever this node holds at prevLogIndex, index = the
//	               first index where that term begins. "Everything from here on
//	               came from one leader's term, and it is not yours."
//
// The second case is the one that pays. Entries from a single term are
// contiguous in any log, so reporting where the run starts lets the leader
// discard the whole run in one step instead of one entry at a time.
//
// Caller must hold mu. Only meaningful when the consistency check failed.
func (n *Node) conflictHint(prevLogIndex int) (conflictIndex, conflictTerm int) {
	// Too short, including the malformed-negative case.
	if prevLogIndex < 0 || prevLogIndex > n.lastLogIndex() {
		return n.lastLogIndex() + 1, noConflictTerm
	}

	term := n.log[prevLogIndex].Term
	return n.firstIndexOfTerm(prevLogIndex, term), term
}

// firstIndexOfTerm walks back from index to the start of the contiguous run of
// entries carrying term.
//
// Never returns 0. Index 0 is the sentinel, and a leader told to resume from
// there would be claiming to replicate an entry no state machine may apply.
//
// Caller must hold mu.
func (n *Node) firstIndexOfTerm(index int, term int) int {
	i := index
	for i > 1 && n.log[i-1].Term == term {
		i--
	}
	return i
}

// nextIndexAfterConflict computes where a leader should resume for a follower
// that rejected with the given hint.
//
// Pure, so the decision can be tested exhaustively without a Node, a transport
// or a lock. current is the leader's existing nextIndex for that follower.
//
// The two branches:
//
//	The leader HAS entries in conflictTerm. Then the follower's entries in that
//	term agree with the leader's up to wherever the leader's run of it ends --
//	same index and same term means the same entry (Log Matching, §5.3). Resume
//	just past the leader's last entry of that term.
//
//	The leader has NONE. That term never entered the winning history: its
//	entries come from a leader deposed before committing anything. Discard the
//	follower's whole run of it and resume at the index the follower reported.
func nextIndexAfterConflict(log []LogEntry, current int, conflictIndex, conflictTerm int) int {
	next := conflictIndex

	if conflictTerm != noConflictTerm {
		if last := lastIndexOfTerm(log, conflictTerm); last > 0 {
			next = last + 1
		}
	}

	// Guarantee progress. A correct follower cannot produce a hint that fails
	// these -- the leader's last index of conflictTerm is necessarily below
	// prevLogIndex, since log terms are non-decreasing and the leader's term at
	// prevLogIndex differs from conflictTerm -- but a malformed or malicious
	// reply must not stall repair in a loop or send it past the sentinel.
	if next >= current {
		next = current - 1
	}
	if next < 1 {
		next = 1
	}
	return next
}

// lastIndexOfTerm returns the highest index in log carrying term, or 0 if the
// term is absent. Zero doubles as "not found" because index 0 is the sentinel
// at term 0, which no caller asks about.
func lastIndexOfTerm(log []LogEntry, term int) int {
	// Backwards: the terms being asked about are recent far more often than not,
	// and a log is scanned here only on the rejection path.
	for i := len(log) - 1; i > 0; i-- {
		if log[i].Term == term {
			return i
		}
		// Terms are non-decreasing along a log, so once past the run there is
		// nothing further back to find.
		if log[i].Term < term {
			return 0
		}
	}
	return 0
}
