package raft

// replicateAll fans one AppendEntries out to every follower.
func (n *Node) replicateAll(term int) {
	n.mu.Lock()

	// Leadership may have ended between the caller's decision and this lock.
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	// Build every message under the lock, ONE STRUCT PER PEER.
	msgs := make(map[int]*AppendEntriesArgs, len(n.peers))
	for _, p := range n.peers {
		msgs[p] = n.buildAppendEntries(p, term)
	}

	n.mu.Unlock() // never send RPCs holding the lock

	for peer, args := range msgs {
		go n.sendAppendEntries(peer, term, args)
	}
}

// buildAppendEntries constructs the message for one follower from this leader's current belief about where that follower's log has got to.
func (n *Node) buildAppendEntries(peer int, term int) *AppendEntriesArgs {
	next := n.nextIndexFor(peer)

	// Defensive clamp. nextIndex should always be in [1, lastLogIndex+1]:
	// initLeaderState sets it to lastLogIndex+1, and C-7's backoff must stop at
	// 1. Clamping rather than panicking because a leader that crashes on a
	// bookkeeping slip takes the cluster's availability with it -- but the
	// clamp is a bug indicator, not a design feature. If a test ever trips it,
	// the fix is in whoever wrote nextIndex, not here.
	if next < 1 {
		next = 1
	}
	if next > n.lastLogIndex()+1 {
		next = n.lastLogIndex() + 1
	}

	// The entry immediately BEFORE what is being sent. The follower must already
	// hold a matching entry here, or it rejects everything in this message
	// (C-4). At next == 1 this is the sentinel at index 0, term 0, which every
	// node has -- so the check trivially passes and repair can always bottom out.
	prevIndex := next - 1

	entries := append([]LogEntry(nil), n.log[next:]...)

	return &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  n.log[prevIndex].Term,
		Entries:      entries,

		// Sent, but nothing acts on it yet: the follower ignores it until C-11
		// and the leader never advances commitIndex until C-10. Carrying the
		// real value now means the field is exercised end to end before anything
		// depends on it.
		LeaderCommit: n.commitIndex,
	}
}

// sendAppendEntries delivers one message and handles the reply.
//
// The only reply handling in this task is the term check. Success and failure
// are otherwise ignored: advancing nextIndex/matchIndex on success is C-6, and
// backing off on rejection is C-7. Until then every fan-out resends the same
// entries, which is wasteful and harmless -- AppendEntries is idempotent, so
// delivering a message twice lands the follower in the same state as once.
func (n *Node) sendAppendEntries(peer int, term int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply

	if !n.transport.SendAppendEntries(peer, args, &reply) {
		return // dropped, partitioned or dead: the next tick tries again
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != term {
		return
	}

	// A follower reporting a newer term means this node has been replaced.
	n.stepDownIfStale(reply.Term)

	// TODO (C-6): if reply.Success, advance matchIndex[peer] to
	// args.PrevLogIndex + len(args.Entries) and nextIndex[peer] to one past it.
	// Derived from what was SENT -- the log may have grown since this left.
	//
	// TODO (C-7): if !reply.Success and the term was not the reason, walk
	// nextIndex[peer] back and retry.
	//
	// TODO (C-10): recount the majority and advance commitIndex.
}
