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

func (n *Node) appendAndReplicate(entry LogEntry) (index, term int, isLeader bool) {
	index, term, isLeader, _ = n.appendChecked(entry, nil)
	return index, term, isLeader
}

func (n *Node) appendChecked(entry LogEntry, precondition func() error) (index, term int, isLeader bool, err error) {
	n.mu.Lock()

	if n.state != Leader {
		currentTerm := n.currentTerm
		n.mu.Unlock()
		return 0, currentTerm, false, nil
	}

	// Under the same lock that established leadership. Checking beforehand and
	// appending afterwards leaves a window for a second configuration change to
	// slip between the two.
	if precondition != nil {
		if err := precondition(); err != nil {
			currentTerm := n.currentTerm
			n.mu.Unlock()
			return 0, currentTerm, true, err
		}
	}

	entry.Term = n.currentTerm
	n.log = append(n.log, entry)
	index = n.lastLogIndex()
	term = n.currentTerm

	// In force from the append. See config.go.
	if entry.isConfig() {
		n.setConfiguration(entry.Servers, index)
	}

	n.markDirty()
	n.persistIfDirty()
	n.advanceCommitIndex()
	n.mu.Unlock()

	go n.replicateAll(term)
	return index, term, true, nil
}
