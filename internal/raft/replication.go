package raft

// replicateAll fans one AppendEntries out to every follower.
//
// term is the leadership term this fan-out belongs to, passed in rather than
// read from n.currentTerm so a fan-out started before a step-down cannot send
// messages claiming a term this node no longer holds.
//
// This is the single send path. A heartbeat is not a separate kind of message:
// it is whatever this produces when a follower happens to be caught up.
func (n *Node) replicateAll(term int) {
	n.mu.Lock()

	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	// Build every message under the lock, ONE STRUCT PER PEER. Peers sit at
	// different points in their logs, so sharing an args pointer would send
	// them each other's consistency checks.
	msgs := make(map[int]*AppendEntriesArgs, len(n.peers))
	for _, p := range n.peers {
		msgs[p] = n.buildAppendEntries(p, term)
	}

	n.mu.Unlock() // never send RPCs holding the lock

	for peer, args := range msgs {
		go n.sendAppendEntries(peer, term, args)
	}
}

// buildAppendEntries constructs the message for one follower from this leader's
// current belief about where that follower's log has got to.
//
// Caller must hold mu.
func (n *Node) buildAppendEntries(peer int, term int) *AppendEntriesArgs {
	next := n.nextIndexFor(peer)

	// Defensive clamp. nextIndex should always be in [1, lastLogIndex+1]. This
	// is a bug indicator, not a design feature: if it ever fires, the fault is
	// in whoever wrote nextIndex.
	if next < 1 {
		next = 1
	}
	if next > n.lastLogIndex()+1 {
		next = n.lastLogIndex() + 1
	}

	prevIndex := next - 1

	// Copy the entries out of n.log. A subslice would hand the network
	// goroutine a window into the live log, which a later append can reallocate
	// and a truncation can rewrite. Shallow on purpose: Command bytes are
	// immutable once appended, so sharing them is safe. See DESIGN.md.
	entries := append([]LogEntry(nil), n.log[next:]...)

	return &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  n.log[prevIndex].Term,
		Entries:      entries,

		// Followers learn what is committed from here; they never derive it.
		// Because this is read at build time, a commit decided just after this
		// message goes out reaches followers on the next tick -- which is why
		// heartbeats matter even when there is nothing to replicate.
		LeaderCommit: n.commitIndex,
	}
}

// sendAppendEntries delivers one message and applies its reply to this leader's
// per-follower bookkeeping.
func (n *Node) sendAppendEntries(peer int, term int, args *AppendEntriesArgs) {
	// A FRESH reply per call, never reused. gob omits zero values, so decoding
	// into a dirty struct leaves the previous call's values in place and they
	// read as though they came off the wire.
	var reply AppendEntriesReply

	if !n.transport.SendAppendEntries(peer, args, &reply) {
		return // dropped, partitioned or dead: the next tick tries again
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// A follower reporting a newer term means this node has been replaced. The
	// only channel through which a leader isolated in a minority finds out.
	n.stepDownIfStale(reply.Term)

	// The world may have moved on while this was in flight. Checked AFTER the
	// step-down so a stale-term reply still deposes this node, but before any
	// bookkeeping, so a deposed leader does not write leader state.
	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		n.advanceFollower(peer, args)
		return
	}

	// A rejection that is not about the term is a log disagreement.
	n.backOffFollower(peer, args, &reply)
}

// advanceFollower records what a successful reply proved. Caller must hold mu.
func (n *Node) advanceFollower(peer int, args *AppendEntriesArgs) {
	// Derived from WHAT WAS SENT, never from lastLogIndex() at reply time. The
	// log may have grown since this message left, and crediting the follower
	// with entries it never received is how a leader counts a majority for an
	// entry that exists on one machine.
	match := args.PrevLogIndex + len(args.Entries)

	// Monotonic, per Figure 2. Replies arrive out of order over a real network,
	// so a late reply to an older, shorter message must not drag matchIndex
	// backwards -- that would un-prove agreement the leader has already counted
	// toward a commit.
	if match > n.matchIndex[peer] {
		n.matchIndex[peer] = match
	}
	n.nextIndex[peer] = n.matchIndex[peer] + 1

	// The replication evidence changed, so the commit decision may have too.
	// This is the only thing that ever moves a leader's commitIndex.
	n.advanceCommitIndex()
}

// backOffFollower moves nextIndex back after a log rejection.
//
// Caller must hold mu.
func (n *Node) backOffFollower(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	current := n.nextIndexFor(peer)

	// ONLY ACT ON A REPLY THAT ANSWERS THE CURRENT ATTEMPT.
	//
	// Replies arrive out of order. A rejection from an older, higher-indexed
	// message can land after backoff has already made progress, and applying it
	// would push nextIndex back to where it started -- a live-lock where repair
	// keeps undoing itself and never converges. Tying the reply to the attempt
	// that produced it is what makes backoff monotonic.
	if args.PrevLogIndex+1 != current {
		return
	}

	n.nextIndex[peer] = nextIndexAfterConflict(
		n.log, current, reply.ConflictIndex, reply.ConflictTerm)
}
