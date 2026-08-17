package raft

func (n *Node) initLeaderState() {
	n.nextIndex = make(map[int]int, len(n.peers))
	n.matchIndex = make(map[int]int, len(n.peers))

	// Figure 2: "initialized to leader last log index + 1".
	//
	// The optimistic assumption is that every follower is already fully caught
	// up, so the very first AppendEntries carries nothing new and simply tests
	// that belief. For the usual case -- a healthy cluster that just changed
	// leaders -- the belief is correct and repair costs nothing. For a follower
	// that is behind, the consistency check fails and C-7 walks this value back
	// until it finds the point where the logs agree.
	next := n.lastLogIndex() + 1

	for _, p := range n.peers {
		n.nextIndex[p] = next

		// Figure 2: "initialized to 0". This must NOT mirror nextIndex.
		//
		// A brand-new leader has proven nothing about anyone. The §5.4.1 check
		// during the election told it only that no voter was AHEAD of it; a
		// voter may be arbitrarily far behind. Starting these at the leader's
		// own last index would let C-10 count a majority immediately and commit
		// entries held on one machine. Nothing else in Raft would catch it.
		n.matchIndex[p] = 0
	}

	// This node's own log is not in the maps. It is not a peer, and there is no
	// meaningful "next entry to send to myself". C-10's majority count therefore
	// starts its tally at 1 -- the leader always agrees with itself -- rather
	// than reading matchIndex[n.id]. See DESIGN.md; the alternative is an entry
	// for self that must be kept in sync on every local append, which is one
	// more thing to forget.
}

// nextIndexFor returns where the leader will next try sending to peer p.
func (n *Node) nextIndexFor(p int) int {
	return n.nextIndex[p]
}

// matchIndexFor returns the highest index the leader has PROVEN peer p holds in agreement.
func (n *Node) matchIndexFor(p int) int {
	return n.matchIndex[p]
}

// TODO (C-6): on a successful AppendEntries, advance both:
//     matchIndex[p] = args.PrevLogIndex + len(args.Entries)
//     nextIndex[p]  = matchIndex[p] + 1
// Derived from what was SENT, never from lastLogIndex() at reply time -- the log
// may have grown since the RPC left, and never from max() of the old value
// unless replies can arrive out of order, which over this transport they can.
//
// TODO (C-7): on a failed AppendEntries that was not a term rejection, walk
// nextIndex[p] back and retry. It must never fall below 1: index 0 is the
// sentinel, and a leader that tries to send from 0 is claiming to replicate an
// entry that no state machine may ever apply.
