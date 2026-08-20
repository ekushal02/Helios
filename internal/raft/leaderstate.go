package raft

import "time"

// initLeaderState reinitialises every piece of per-follower bookkeeping a new
// leadership term starts from scratch with.
func (n *Node) initLeaderState() {
	n.nextIndex = make(map[int]int, len(n.peers))
	n.matchIndex = make(map[int]int, len(n.peers))

	// Fresh, not cleared, and for the same reason as the other two: a reply
	// handler still in flight from the previous term must not be able to write
	// a contact time into this one. Leadership carries no lease until this term
	// has completed a majority round of its own.
	n.lastContact = make(map[int]time.Time, len(n.peers))

	// Figure 2: "initialized to leader last log index + 1".
	//
	// The optimistic assumption is that every follower is already fully caught
	// up, so the very first AppendEntries carries nothing new and simply tests
	// that belief. For the usual case -- a healthy cluster that just changed
	// leaders -- the belief is correct and repair costs nothing. For a follower
	// that is behind, the consistency check fails and the fast-backup path walks
	// this value back until it finds the point where the logs agree.
	next := n.lastLogIndex() + 1

	for _, p := range n.peers {
		n.nextIndex[p] = next

		// Figure 2: "initialized to 0". This must NOT mirror nextIndex.
		//
		// A brand-new leader has proven nothing about anyone. The §5.4.1 check
		// during the election told it only that no voter was AHEAD of it; a
		// voter may be arbitrarily far behind. Starting these at the leader's
		// own last index would let the commit rule count a majority immediately
		// and commit entries held on one machine. Nothing else in Raft would
		// catch it.
		n.matchIndex[p] = 0
	}

	// This node's own log is not in the maps. It is not a peer, and there is no
	// meaningful "next entry to send to myself". The majority count therefore
	// starts its tally at 1 -- the leader always agrees with itself -- rather
	// than reading matchIndex[n.id]. See DESIGN.md; the alternative is an entry
	// for self that must be kept in sync on every local append, which is one
	// more thing to forget.
	//
	// lastContact excludes self for the same reason, and the lease arithmetic
	// counts this node separately: see leaseExpiry.
}

// nextIndexFor returns where the leader will next try sending to peer p.
func (n *Node) nextIndexFor(p int) int {
	return n.nextIndex[p]
}

// matchIndexFor returns the highest index the leader has PROVEN peer p holds in agreement.
func (n *Node) matchIndexFor(p int) int {
	return n.matchIndex[p]
}
