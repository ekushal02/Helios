package raft

import "time"

func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	defer n.persistIfDirty()

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

	if n.state == Leader {
		n.lg().Error("AppendEntries from another leader in the same term",
			"from", args.LeaderID, "term", args.Term)
		return
	}

	if n.state == Candidate {
		n.becomeFollower(args.Term)
	}

	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()

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

	// --- Receiver rule 5: adopt the leader's commit index ---
	//
	// commitIndex is not replicated state. A follower has no way to work out
	// which of its entries reached a majority, so it is simply told, on every
	// message. That is why entries and the permission to apply them usually
	// arrive SEPARATELY: the leader appends, waits for a majority, and only the
	// next message carries the higher LeaderCommit.
	//
	// THE min IS THE WHOLE RULE. lastNewIndex is derived from the MESSAGE --
	// where the consistency check landed, plus what this message carried -- and
	// never from n.lastLogIndex().
	//
	// The two differ exactly in Figure 7's cases (c) and (d), where the follower
	// holds a private tail the leader never had. A heartbeat carrying no entries
	// passes the check at PrevLogIndex and says LeaderCommit 8. Committing to 8
	// would commit that private tail, the state machine would apply it, and the
	// leader's next real message would truncate it. An entry that was applied
	// and then vanished is precisely what Raft exists to prevent, and the client
	// was told it succeeded.
	//
	// Reading it the safe way: this message proves agreement up to
	// PrevLogIndex+len(Entries) and says nothing whatsoever about the indices
	// beyond. LeaderCommit is a fact about the leader's log; lastNewIndex is the
	// prefix of that log this node can vouch for. Commit the smaller.
	//
	// commitTo (C-12) owns the rest: it refuses to move backwards, which is what
	// makes a reordered message carrying a stale LeaderCommit harmless, and it
	// wakes the applier.
	lastNewIndex := args.PrevLogIndex + len(args.Entries)
	n.commitTo(min(args.LeaderCommit, lastNewIndex))

	reply.Success = true
}

// mergeEntries applies Figure 2's receiver rules 3 and 4 to entries beginning
// at prevLogIndex+1.
//
// THE THING THIS FUNCTION EXISTS TO AVOID.
//
// Having passed the consistency check, it is tempting to truncate outright --
// drop everything past prevLogIndex -- and append. That is wrong, because RPCs
// arrive out of order.
//
// A leader sends entries 1..5, then 1..8. The 1..8 message lands first and this
// node stores eight entries. The stale 1..5 message then arrives carrying
// PrevLogIndex 0, which passes the consistency check because the floor always
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
			// Appending is a position operation and needs no translation.
			n.log = append(n.log, entries[i:]...)
			n.adoptConfigFromAppended(idx, entries[i:])
			n.markDirty()
			return
		}

		// AT OR BELOW THE FLOOR. Defensive: the consistency check passed, so
		// prevLogIndex is at least the floor and idx is at least firstLogIndex.
		// If it ever is not, the entry is one a snapshot already accounts for --
		// committed, therefore identical by Log Matching -- and the only safe
		// thing is to skip it. Truncating there would cut into committed
		// history, which truncateFrom refuses anyway.
		if idx < n.firstLogIndex() {
			continue
		}

		// Same index, same term: by Log Matching this is the same entry.
		if n.termAt(idx) == e.Term {
			continue
		}

		// Believed impossible: a committed entry can never conflict, because it
		// was replicated to a majority and any leader elected afterwards must
		// have held it (§5.4.1). If this fires, the bug is upstream in the
		// election restriction or the commit rule, not here.
		//
		// As of C-11 this check has teeth on followers too: commitIndex is no
		// longer always zero here, it is whatever the leader last said. A
		// LeaderCommit that was not capped by lastNewIndex would show up as this
		// line firing, one message later.
		if idx <= n.commitIndex {
			n.lg().Error("truncating at or below commitIndex",
				"index", idx, "commitIndex", n.commitIndex,
				"haveTerm", n.termAt(idx), "wantTerm", e.Term)
		}

		// Rule 3: delete the conflicting entry and every one after it, then
		// append the leader's version. Through truncateFrom, which owns the
		// index-to-position translation and refuses to cut into the floor.
		n.truncateFrom(idx)
		n.log = append(n.log, entries[i:]...)
		n.refreshConfiguration()
		n.markDirty()
		return
	}

	// Falling out means every entry in the message was already present with a
	// matching term. The log is untouched -- INCLUDING entries beyond what this
	// message covered, which stay put. They are uncommitted and get truncated
	// when the leader eventually sends something at their indices. Rule 5 above
	// is careful not to commit them in the meantime.
}

// logMatchesAt reports whether this node holds an entry at index whose term is
// term. It is the whole of Figure 2's receiver rule 2.
//
// THE FLOOR IS CHECKABLE; ANYTHING BELOW IT IS NOT.
//
// Before compaction, index 0 was the sentinel: present on every node at term 0,
// so a leader backing all the way off ALWAYS found agreement, which is what
// guaranteed repair terminates. After compaction the same role is played by
// lastIncludedIndex, whose term survives in the snapshot header precisely so
// this check can still be answered at the boundary -- that is why the field
// exists.
//
// An index BELOW the floor cannot be answered at all: the entry is gone. It is
// rejected here, the hint sends the leader backwards, and the leader discovers
// on its next attempt that it must send an image instead. That detour costs one
// round trip and is the honest answer; claiming a match would be a lie and
// claiming a mismatch at a specific term would be a guess.
//
// Caller must hold mu.
func (n *Node) logMatchesAt(index int, term int) bool {
	// Out of range in either direction. Checked explicitly rather than left to
	// termAt, whose out-of-range arm logs an Error -- a stale message reaching
	// below the floor is ordinary, not a fault.
	if index < n.lastIncludedIndex || index > n.lastLogIndex() {
		return false
	}

	// At the floor this returns lastIncludedTerm; above it, the entry's own.
	return n.termAt(index) == term
}
