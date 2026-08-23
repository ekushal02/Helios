package raft

import "time"

// replicateAll fans one AppendEntries out to every follower.
//
// term is the leadership term this fan-out belongs to, passed in rather than
// read from n.currentTerm so a fan-out started before a step-down cannot send
// messages claiming a term this node no longer holds.
//
// This is the single send path. A heartbeat is not a separate kind of message:
// it is whatever this produces when a follower happens to be caught up.
//
// AT MOST ONE ROUND IS IN FLIGHT PER PEER, and that is what turns a burst of
// client writes into one message.
//
// Submit returns as soon as the entry is appended and persisted, so a thousand
// concurrent clients put a thousand entries in the log in the time it takes one
// AppendEntries to reach a follower and come back. One fan-out per Submit means
// a thousand messages, each rebuilding log[nextIndex:] from a nextIndex that
// has barely moved -- a thousand messages carrying hundreds of entries each,
// gob-encoded, to ship a thousand entries. Quadratic work for linear progress.
//
// So a trigger that arrives while a round is outstanding sets a flag instead of
// starting a second round. When the round finishes it looks at the flag and, if
// set, builds ONE new message from the current nextIndex -- which by then
// covers everything that accumulated while it was away. The batching window is
// the round trip itself. There is no interval to tune, and an idle cluster is
// unaffected because there is never anything pending.
func (n *Node) replicateAll(term int) {
	n.mu.Lock()

	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	coalesce := !n.noCoalesce

	// Build every message under the lock, ONE STRUCT PER PEER. Peers sit at
	// different points in their logs, so sharing an args pointer would send
	// them each other's consistency checks.
	msgs := make(map[int]*AppendEntriesArgs, len(n.peers))
	snaps := make(map[int]*InstallSnapshotArgs)
	for _, p := range n.peers {
		if coalesce && n.replicatingTerm[p] == term {
			// A round is already out to this peer. Fold this trigger into the
			// message that round will send when it returns.
			n.replPending[p] = true
			continue
		}
		args := n.buildAppendEntries(p, term)
		if args == nil {
			// Compacted past this follower: the entries it needs are gone, so
			// the image goes instead. Same slot, same coalescing rule -- a
			// snapshot send is a round like any other.
			snapArgs := n.buildInstallSnapshot(p, term)
			if snapArgs == nil {
				continue
			}
			if coalesce {
				n.replicatingTerm[p] = term
			}
			snaps[p] = snapArgs
			continue
		}
		if coalesce {
			n.replicatingTerm[p] = term
		}
		msgs[p] = args
	}

	n.mu.Unlock() // never send RPCs holding the lock

	for peer, args := range msgs {
		if coalesce {
			go n.replicateRound(peer, term, args)
		} else {
			go n.sendAppendEntries(peer, term, args)
		}
	}
	for peer, args := range snaps {
		go n.sendInstallSnapshot(peer, term, args)
	}
}

// replicateRound sends to one peer and keeps sending for as long as work piled
// up behind the message it was waiting on.
//
// It owns the peer's in-flight slot for the whole time it runs, and it is the
// only thing that clears it. The slot holds a TERM rather than a bool so that a
// round left over from a deposed leadership cannot free a slot that a newer
// term's round is holding.
//
// A peer that stops answering therefore stops receiving until its send returns.
// That is the correct behaviour -- piling messages on an unresponsive follower
// helps nobody -- but it does mean Transport.SendAppendEntries has to bound how
// long it can block. The fake network returns immediately when a peer is
// unreachable; a real transport needs a deadline.
func (n *Node) replicateRound(peer int, term int, args *AppendEntriesArgs) {
	for args != nil {
		n.sendAppendEntries(peer, term, args)

		n.mu.Lock()

		more := n.replPending[peer]
		n.replPending[peer] = false

		if !more || n.state != Leader || n.currentTerm != term {
			// Release the slot, but only if it is still ours.
			n.releaseReplicationSlot(peer, term)
			n.mu.Unlock()
			return
		}

		// One message for everything that arrived while the last one was out.
		// Built from the CURRENT nextIndex, so a reply that landed in the
		// meantime is already accounted for.
		//
		args = n.buildAppendEntries(peer, term)
		if args == nil {
			// The follower fell below the floor while this round was in flight.
			// Hand the slot straight to a snapshot send rather than releasing
			// it and waiting for a tick to notice.
			snapArgs := n.buildInstallSnapshot(peer, term)
			if snapArgs == nil {
				n.releaseReplicationSlot(peer, term)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			n.sendInstallSnapshot(peer, term, snapArgs)
			return
		}

		n.mu.Unlock()
	}
}

// buildAppendEntries constructs the message for one follower from this leader's
// current belief about where that follower's log has got to.
//
// Caller must hold mu.
func (n *Node) buildAppendEntries(peer int, term int) *AppendEntriesArgs {
	next := n.nextIndexFor(peer)

	// THE REFUSAL COMES BEFORE THE CLAMP, and the order is the whole point.
	//
	// next at or below the floor means the entries this follower is owed have
	// been compacted away. Clamping first would raise next to firstLogIndex and
	// then find nothing wrong -- converting a real, actionable condition into a
	// message claiming a PrevLogIndex the follower has never reached, which it
	// can only reject, forever.
	//
	// The same expression carries a second, older meaning when nothing has been
	// compacted: next <= 0 is malformed and cannot come from correct code. Both
	// readings want the same answer here, which is to say so rather than paper
	// over it.
	//
	// Returning nil is not a failure: it is how this function says "this one
	// needs an image". replicateAll and replicateRound both read it that way and
	// hand the peer's slot to sendInstallSnapshot. The Info line is what makes a
	// follower falling this far behind visible in the log, since it means the
	// cluster is now paying for a full image rather than a few entries.
	if next <= n.lastIncludedIndex {
		n.lg().Info("follower is below the log floor; sending a snapshot",
			"peer", peer, "nextIndex", next, "floor", n.lastIncludedIndex)
		return nil
	}

	// Clamping upward is still just a bug indicator: nextIndex should never
	// exceed lastLogIndex+1, and if it does the fault is in whoever wrote it.
	if next > n.lastLogIndex()+1 {
		next = n.lastLogIndex() + 1
	}

	prevIndex := next - 1

	return &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  n.termAt(prevIndex),

		// Copied out of the log rather than sliced: a subslice would hand the
		// network goroutine a window into the live log, which a later append
		// can reallocate and a truncation can rewrite. See entriesFrom.
		Entries: n.entriesFrom(next),

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

	// Stamped BEFORE the send, for the read lease. The follower resets its
	// election timer when it receives this, which is at or after now, so dating
	// the contact from here understates the lease rather than overstating it.
	// See noteContact.
	sentAt := time.Now()

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

	// ANY reply counts as contact, including a log rejection.
	//
	// The lease cares whether the follower's election timer was reset, not
	// whether its log agreed. A failed consistency check resets the timer
	// before running the check -- that is deliberate, so a lagging follower does
	// not campaign mid-repair -- so a rejection proves exactly what the lease
	// needs. The one rejection that withholds the reset is the stale-term case,
	// and that one deposes this node on the line above, so it cannot reach here.
	n.noteContact(peer, sentAt)

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

	// The floor travels with the slice: nextIndexAfterConflict is pure, so it
	// cannot ask the Node where position 0 sits and has to be told.
	n.nextIndex[peer] = nextIndexAfterConflict(
		n.log, n.lastIncludedIndex, current, reply.ConflictIndex, reply.ConflictTerm)
}
