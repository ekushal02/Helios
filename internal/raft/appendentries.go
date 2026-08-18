package raft

// AppendEntries is the receiving side of the AppendEntries RPC (Figure 2, §5.3).
//
// Order of operations is load-bearing and is not the order Figure 2 lists the
// rules in. Legitimacy is settled first (is the sender the leader of a term I
// accept?), and only then does the log get examined. A message can come from a
// perfectly valid leader and still be unusable by this follower, and those two
// judgements must not contaminate each other.
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// --- Rules for Servers, All Servers ---
	n.stepDownIfStale(args.Term)

	reply.Term = n.currentTerm
	reply.Success = false

	// --- Receiver rule 1: reply false if term < currentTerm (§5.1) ---
	//
	// No timer reset, and NO CONFLICT HINT: this rejection is about the sender's
	// term, not about any log. A leader that read a hint here would back off for
	// the wrong reason, and it is about to step down anyway.
	if args.Term < n.currentTerm {
		return
	}

	if n.state == Candidate {
		n.becomeFollower(args.Term)
	}

	n.leaderID = args.LeaderID

	// Reset BEFORE the log check: a follower that fails the check is behind or
	// diverged, which is normal and is exactly when the leader is repairing it.
	n.resetElectionTimer()

	// --- Receiver rule 2: the consistency check (§5.3) ---
	if !n.logMatchesAt(args.PrevLogIndex, args.PrevLogTerm) {
		// Fast backup (§5.3, not Figure 2). Tell the leader WHY, so it can back
		// off by a whole term instead of one index per round trip.
		reply.ConflictIndex, reply.ConflictTerm = n.conflictHint(args.PrevLogIndex)
		return
	}

	// --- Receiver rules 3 and 4: conflict handling and append ---
	n.mergeEntries(args.PrevLogIndex, args.Entries)

	// TODO (C-11): rule 5, if LeaderCommit > commitIndex set commitIndex to
	// min(LeaderCommit, index of last new entry). The second half of that min
	// matters and is easy to drop: this node may hold entries beyond what the
	// message covered, and must not treat them as committed on the strength of
	// a LeaderCommit that was never about them.

	reply.Success = true
}

// mergeEntries applies Figure 2's receiver rules 3 and 4 to entries beginning
// at prevLogIndex+1.
//
// THE THING THIS FUNCTION EXISTS TO AVOID.
//
// Having passed the consistency check, it is tempting to truncate outright --
// n.log = n.log[:prevLogIndex+1] -- and append. That is wrong, because RPCs
// arrive out of order.
//
// A leader sends entries 1..5, then 1..8. The 1..8 message lands first and this
// node stores eight entries. The stale 1..5 message then arrives carrying
// PrevLogIndex 0, which passes the consistency check because the sentinel always
// matches. Blind truncation would cut the log back to five and destroy 6, 7 and
// 8 -- entries the leader may already have counted toward a majority and
// committed. Nothing repairs that afterwards: the leader believes this node has
// them.
//
// So rule 3 is read literally. Truncation happens at the first index where the
// TERMS ACTUALLY DIFFER, not at prevLogIndex. Same index plus same term means
// the same entry (Log Matching, §5.3), so a message that is entirely a duplicate
// changes nothing -- the idempotence that lets Raft retry without sequence
// numbers.
//
// Caller must hold mu.
func (n *Node) mergeEntries(prevLogIndex int, entries []LogEntry) {
	for i, e := range entries {
		idx := prevLogIndex + 1 + i

		// Past the end of the log: nothing here to conflict with, so everything
		// from this point on is new. Rule 4.
		if idx > n.lastLogIndex() {
			// Copies the entry structs out of the message rather than retaining
			// the message's slice, so the log does not alias transport memory.
			n.log = append(n.log, entries[i:]...)
			return
		}

		// Same index, same term: by Log Matching this is the same entry.
		if n.log[idx].Term == e.Term {
			continue
		}

		// Believed impossible: a committed entry can never conflict, because it
		// was replicated to a majority and any leader elected afterwards must
		// have held it (§5.4.1). If this fires, the bug is upstream in the
		// election restriction or the commit rule, not here.
		if idx <= n.commitIndex {
			n.lg().Error("truncating at or below commitIndex",
				"index", idx, "commitIndex", n.commitIndex,
				"haveTerm", n.log[idx].Term, "wantTerm", e.Term)
		}

		// Rule 3: delete the conflicting entry and every one after it, then
		// append the leader's version over the same backing array.
		n.log = append(n.log[:idx], entries[i:]...)
		return
	}

	// Falling out means every entry in the message was already present with a
	// matching term. The log is untouched -- INCLUDING entries beyond what this
	// message covered, which stay put. They are uncommitted and get truncated
	// when the leader eventually sends something at their indices.
}

// logMatchesAt reports whether this node holds an entry at index whose term is
// term. It is the whole of Figure 2's receiver rule 2.
//
// Caller must hold mu.
func (n *Node) logMatchesAt(index int, term int) bool {
	// A negative index cannot arise from a correct leader, but a malformed
	// message must not panic the receiver.
	if index < 0 {
		return false
	}

	if index > n.lastLogIndex() {
		return false
	}

	// Index 0 is the sentinel, present on every node at term 0, so a leader
	// that has backed all the way off ALWAYS finds agreement here. That is what
	// guarantees log repair terminates.
	//
	// TODO (Phase D): after snapshotting, index 0 is gone and the floor becomes
	// lastIncludedIndex. A PrevLogIndex below that floor cannot be checked at
	// all and must be answered with InstallSnapshot rather than a rejection.
	return n.log[index].Term == term
}
