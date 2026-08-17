package raft

// Submit hands a command to the cluster. It is the only way anything enters the
// replicated log.
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
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return 0, n.currentTerm, false
	}

	cmd := append([]byte(nil), command...)

	n.log = append(n.log, LogEntry{Term: n.currentTerm, Command: cmd})

	index = n.lastLogIndex()
	term = n.currentTerm

	n.mu.Unlock()

	// Push it out now rather than waiting for the heartbeat tick.
	go n.replicateAll(term)

	return index, term, true
}
