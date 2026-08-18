package raft

// Leader commit decision (Figure 2, "Rules for Servers / Leaders", final bullet).
//
//	If there exists an N such that N > commitIndex, a majority of
//	matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N.
//
// Three conditions. The first is monotonicity, the second is the replica count,
// and the third is §5.4.2 -- the one that looks like excess caution and is not.
// See advanceCommitIndex.

// quorumSize is the number of nodes that must hold an entry for it to be safe.
//
// Cluster size is len(peers)+1: peers excludes this node. A three-node cluster
// needs two, a five-node cluster needs three.
//
// The whole safety argument rests on the fact that any two majorities of the
// same cluster intersect. A future leader needs votes from a majority, that
// majority shares at least one member with the majority holding a committed
// entry, and §5.4.1 stops that member voting for a candidate whose log is
// behind. Hence no leader can ever be elected without a committed entry.
func (n *Node) quorumSize() int {
	return (len(n.peers)+1)/2 + 1
}

// replicaCount reports how many nodes are known to hold the entry at index.
//
// Starts at 1 because the leader holds every entry in its own log by
// definition, and matchIndex deliberately has no entry for this node -- keeping
// one would mean updating it on every local append, and forgetting to is an
// off-by-one visible only as a premature commit under load. See DESIGN.md.
//
// Caller must hold mu.
func (n *Node) replicaCount(index int) int {
	count := 1
	for _, p := range n.peers {
		if n.matchIndex[p] >= index {
			count++
		}
	}
	return count
}

// advanceCommitIndex moves commitIndex as far up as the replication evidence
// allows. Called whenever a matchIndex changes.
//
// Caller must hold mu.
func (n *Node) advanceCommitIndex() {
	// Only a leader commits by counting. A follower learns commitIndex from
	// LeaderCommit (C-11) and must never derive it -- it cannot see the other
	// followers' match state, so any conclusion it drew would be guesswork.
	if n.state != Leader {
		return
	}

	quorum := n.quorumSize()

	// Downward from the top, so the first qualifying index is the highest.
	// The replica count is non-increasing as the index rises -- a node holding
	// index N holds everything below it -- so there is nothing better further
	// down.
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {

		// --- §5.4.2, THE FIGURE 8 CONDITION ---
		//
		// Only entries from THIS leader's term may be committed by counting
		// replicas. Terms are non-decreasing along a log, so the first index
		// from an older term means every index below it is older too, and the
		// scan can stop rather than continue.
		//
		// Why counting is not enough for an old entry. A majority holding entry
		// E stops a candidate that LACKS E from winning, because the two
		// majorities intersect and §5.4.1 makes the shared voter refuse. It
		// says nothing about a candidate whose log is differently shaped but
		// more up to date BY TERM. Figure 8: a leader finishes replicating an
		// inherited term-2 entry to a majority in term 4; a rival holding a
		// term-3 entry at the same index then wins the next election on the
		// strength of that higher term, and overwrites it. If this leader had
		// treated the count as a commit, an acknowledged entry would be gone.
		//
		// A CURRENT-term entry on a majority closes that hole: every node in
		// that majority now has a last-log term of at least currentTerm, so no
		// candidate carrying an older tail can out-rank them.
		if n.log[idx].Term != n.currentTerm {
			break
		}

		if n.replicaCount(idx) >= quorum {
			// Everything below commits WITH it. By Log Matching, a node holding
			// index idx holds every preceding entry identically, so committing
			// idx commits the whole prefix -- which is how entries inherited
			// from earlier terms become committed despite never qualifying on
			// their own.
			n.commitIndex = idx

			// TODO (C-12): wake the apply loop. commitIndex moving is what
			// releases entries to the state machine, and a client waiting on
			// Submit's returned index is waiting on exactly this.
			return
		}
	}

	// TODO: a leader that inherits uncommitted entries from a previous term
	// cannot commit them until it appends something of its own, so on an idle
	// cluster they sit indefinitely. The usual remedy is a no-op entry appended
	// on election (§8). Deferred: it changes what indices clients see and
	// interacts with read-only query handling, so it belongs to the task that
	// needs it rather than being smuggled in here.
}
