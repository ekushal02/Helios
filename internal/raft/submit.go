package raft

// Submit hands a command to the cluster. It is the only way a CLIENT COMMAND
// enters the replicated log.
//
// Returns the index the command was placed at, the term it was placed in, and
// whether this node is the leader. A false third return means the caller must
// find the real leader and try again; leaderID is the hint for that (F-2).
//
// NON-BLOCKING, AND THE INDEX IS A PREDICTION.
//
// Submit returns as soon as the entry is in this node's log -- on one machine,
// replicated nowhere, committed by nobody. If this leader is deposed before the
// entry commits, a new leader may overwrite that index with something else and
// the command is silently gone. Raft promises only that IF an entry commits at
// (index, term) then every node agrees on it forever; it never promises a
// particular submission gets to be that entry.
//
// So the (index, term) pair is a claim ticket, not a receipt. The caller learns
// the outcome by watching for that index to be applied WITH THAT TERM. An index
// applied carrying a different term means this submission was overwritten and
// must be reported as failed, not retried silently -- a retry could double-apply
// if the original committed after all. That machinery is Phase F.
func (n *Node) Submit(command []byte) (index int, term int, isLeader bool) {
	// COPIED BEFORE IT REACHES THE LOG. The caller owns the slice it passed and
	// may reuse it; if the log aliased that memory, a caller recycling a buffer
	// would rewrite committed history. From here on LogEntry.Command is
	// immutable, which is what lets outgoing messages copy entries shallowly.
	return n.appendAndReplicate(LogEntry{
		Command: append([]byte(nil), command...),
	})
}

// appendAndReplicate is the single path by which anything enters this leader's
// log. Submit puts a client command through it; ReadIndex puts a barrier.
//
// Extracted rather than duplicated because of the advanceCommitIndex call
// below, which is the one line in the function that is not obvious and the one
// a second copy would omit.
func (n *Node) appendAndReplicate(entry LogEntry) (index int, term int, isLeader bool) {
	n.mu.Lock()

	if n.state != Leader {
		currentTerm := n.currentTerm
		n.mu.Unlock()
		return 0, currentTerm, false
	}

	// The term is stamped here, not by the caller. An entry's term is the term
	// of the leader that created it, and only this critical section knows what
	// that is -- a caller reading currentTerm beforehand could be stamping an
	// entry with a term this node no longer holds.
	entry.Term = n.currentTerm

	n.log = append(n.log, entry)

	index = n.lastLogIndex()
	term = n.currentTerm

	n.markDirty()
	n.persistIfDirty()

	// The append is itself replication evidence, and in a single-node cluster it
	// is the only evidence there will ever be. Every other call to
	// advanceCommitIndex hangs off an AppendEntries reply, so a cluster with no
	// peers sends nothing, hears nothing, and never evaluates the commit rule --
	// the log grows and commitIndex sits at zero forever.
	//
	// A no-op for any larger cluster: one replica is not a majority, so the
	// count fails and this returns immediately.
	n.advanceCommitIndex()

	n.mu.Unlock()

	// Push it out now rather than waiting for the heartbeat tick.
	go n.replicateAll(term)

	return index, term, true
}
