package raft

import "time"

// =============================================================================
// The RPC
// =============================================================================

// InstallSnapshotArgs carries a leader's state-machine image to a follower that
// has fallen behind the leader's log floor.
//
// WHEN THIS IS THE ONLY ANSWER. A follower is repaired by AppendEntries walking
// nextIndex backwards until the consistency check passes. That walk needs the
// entries to still exist. Once compaction has discarded them the leader has
// nothing to send and no honest message to construct: one built from the floor
// would claim a PrevLogIndex the follower has never reached and be rejected
// forever. The image is what replaces the entries.
//
// A DEVIATION FROM FIGURE 13: no Offset or Done. The paper chunks the image so
// a receiver can bound how much it buffers. Helios sends it whole, because the
// transport already materialises a whole message and chunking would add a
// reassembly state machine -- with its own partial-transfer and
// leader-change-mid-transfer cases -- to buy nothing at the sizes this system
// currently reaches. It becomes necessary when an image stops fitting
// comfortably in one RPC; that is a real limit and it is not measured yet.
type InstallSnapshotArgs struct {
	Term     int
	LeaderID int

	// LastIncludedIndex and LastIncludedTerm describe where the image sits, and
	// mean exactly what they mean in the stored record. See Snapshot.
	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte
}

// InstallSnapshotReply carries only the term, which is the one thing a follower
// can tell a leader that the leader must act on.
//
// There is no Success field, and its absence is deliberate. A follower that
// rejects on term deposes the leader, and the leader learns that from Term. A
// follower that accepts has nothing to negotiate: the image is not a proposal.
// Any other failure is the follower's own problem, and a retry on the next
// round is the whole remedy.
type InstallSnapshotReply struct {
	Term int
}

// =============================================================================
// Leader side
// =============================================================================

// buildInstallSnapshot constructs the image message for a follower that has
// fallen below the floor. Returns nil when there is nothing to send.
//
// Caller must hold mu.
//
// THE IMAGE IS READ FROM STORAGE, NOT HELD IN MEMORY. The leader keeps no copy:
// Snapshot hands the bytes to the storage layer and lets go, because holding
// one would double the memory the compaction was taken to release. The cost is
// a read and a decode under n.mu on this path. It is bounded by the coalescing
// rule -- one round in flight per peer -- so a stuck follower costs one read
// per round rather than one per tick. If that stops being affordable the fix is
// to cache the blob beside the floor, not to read it outside the lock: the
// floor can move between the read and the send.
func (n *Node) buildInstallSnapshot(peer int, term int) *InstallSnapshotArgs {
	if !n.hasSnapshot() {
		// Believed impossible: buildAppendEntries only defers here when
		// nextIndex has fallen at or below the floor, and with no snapshot the
		// floor is 0, which nextIndex can never be at or below.
		n.lg().Error("follower is below the floor but this leader has no snapshot",
			"peer", peer, "floor", n.lastIncludedIndex)
		return nil
	}

	// ONE ATTEMPT PER HEARTBEAT INTERVAL, PER PEER.
	//
	// Everything below reads the whole image back from storage and checksums
	// it, under n.mu. replicateAll runs on every client write and not just on
	// the tick, so a follower that is down while the cluster is under load
	// would have a full image rebuilt for it once per submitted command --
	// thousands of multi-megabyte reads to feed a peer that is not listening.
	//
	// The throttle is on the ATTEMPT rather than on success, because an
	// unreachable peer fails instantly and it is precisely that instant retry
	// which makes the storm. One per heartbeat is the cadence ordinary
	// replication already retries at, so a follower that is merely behind loses
	// nothing worth measuring.
	if time.Since(n.snapshotSentAt[peer]) < heartbeatInterval {
		return nil
	}
	n.snapshotSentAt[peer] = time.Now()

	blob, err := n.storage.LoadSnapshot()
	if err != nil {
		n.lg().Error("cannot read the snapshot to send", "peer", peer, "err", err)
		return nil
	}
	if blob == nil {
		n.lg().Error("floor is set but no snapshot is stored",
			"peer", peer, "floor", n.lastIncludedIndex)
		return nil
	}

	snap, err := decodeSnapshot(blob)
	if err != nil {
		n.lg().Error("stored snapshot does not decode", "peer", peer, "err", err)
		return nil
	}

	return &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              snap.Data,
	}
}

// sendInstallSnapshot delivers an image and credits the follower for it.
//
// It holds the peer's replication slot for the whole call, exactly as
// replicateRound does, and releases it before returning. As far as coalescing
// is concerned a snapshot send is a round like any other.
func (n *Node) sendInstallSnapshot(peer int, term int, args *InstallSnapshotArgs) {
	var reply InstallSnapshotReply

	// Stamped before the send, for the same reason as in sendAppendEntries: a
	// follower resets its election timer on receipt, which is at or after now.
	sentAt := time.Now()

	ok := n.transport.SendInstallSnapshot(peer, args, &reply)

	n.mu.Lock()

	if !ok {
		n.releaseReplicationSlot(peer, term)
		n.mu.Unlock()
		return
	}

	n.stepDownIfStale(reply.Term)

	if n.state != Leader || n.currentTerm != term {
		n.releaseReplicationSlot(peer, term)
		n.mu.Unlock()
		return
	}

	n.noteContact(peer, sentAt)

	// THE FOLLOWER NOW HOLDS EVERYTHING THROUGH LastIncludedIndex, which is as
	// strong a proof as a successful AppendEntries. Credited from what was
	// SENT, never from the leader's floor at reply time -- the floor may have
	// moved on while this was in flight, exactly as the log may have grown in
	// advanceFollower.
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	n.nextIndex[peer] = n.matchIndex[peer] + 1

	n.advanceCommitIndex()
	n.releaseReplicationSlot(peer, term)

	n.mu.Unlock()

	// The follower is at the floor, but the log has almost certainly grown past
	// it. Push the tail now rather than waiting a tick: a node that has just
	// been handed a whole image should not then idle for a heartbeat interval.
	n.replicateAll(term)
}

// releaseReplicationSlot frees a peer's in-flight marker, if it is still ours.
// Caller must hold mu. See replicateRound for why the slot holds a term.
func (n *Node) releaseReplicationSlot(peer int, term int) {
	if n.replicatingTerm[peer] == term {
		n.replicatingTerm[peer] = 0
	}
	n.replPending[peer] = false
}

// =============================================================================
// Follower side
// =============================================================================

// InstallSnapshot is the receiver, Figure 13.
//
// The ordering inside mirrors Node.Snapshot, for the same reason: the image is
// made durable before anything is discarded, so a crash part-way leaves a
// snapshot beside a log that still covers it rather than the reverse.
func (n *Node) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	defer n.persistIfDirty() // LIFO: flushes under the lock, before the reply is readable

	reply.Term = n.currentTerm

	// Rule 1. A stale leader learns its term is over and nothing else happens.
	if args.Term < n.currentTerm {
		return
	}

	// A message from a current leader, so this node follows it whatever it
	// thought it was. The election timer is reset for the same reason
	// AppendEntries resets it before running its log check: a node being
	// repaired must not campaign mid-repair.
	n.stepDownIfStale(args.Term)
	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	// AN IMAGE THIS NODE HAS ALREADY OUTGROWN IS DISCARDED, NOT APPLIED.
	//
	// Installing it would rewind the state machine to an earlier point and
	// hand the applier a floor below what it has already delivered. A leader
	// can send a stale image quite legitimately: it may have decided to send
	// before a burst of AppendEntries it had already put on the wire arrived.
	if args.LastIncludedIndex <= n.commitIndex {
		return
	}

	blob, err := encodeSnapshot(Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	})
	if err != nil {
		n.lg().Error("refusing a malformed snapshot", "leader", args.LeaderID, "err", err)
		return
	}

	// Durable before anything is discarded. A failure here is not fatal the way
	// a failed state write is: nothing has been promised to anyone, the node
	// simply has not accepted the image, and the leader retries next round.
	if err := n.storage.SaveSnapshot(blob); err != nil {
		n.lg().Error("cannot store the snapshot", "leader", args.LeaderID, "err", err)
		return
	}

	// RULE 6 IS AN OPTIMISATION, NOT A CORRECTNESS RULE, and it matters more
	// than it looks. If this node already holds the entry at LastIncludedIndex
	// with a matching term then by Log Matching everything before it agrees
	// too, and the entries AFTER it are just as valid as they were a moment
	// ago. Discarding them would throw away a tail the leader must then resend,
	// turning a cheap repair into a full image plus a full retransmission.
	if n.hasEntryAt(args.LastIncludedIndex) &&
		n.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		n.compactTo(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		// Rule 7. Nothing here agrees with the image, so nothing here survives.
		n.log = []LogEntry{{Term: args.LastIncludedTerm}}
		n.lastIncludedIndex = args.LastIncludedIndex
		n.lastIncludedTerm = args.LastIncludedTerm
	}

	// Rule 8, deferred to the applier. The image cannot be handed up from here:
	// this holds n.mu, and sending on an unbuffered channel under the lock is
	// the deadlock the single-applier design exists to prevent. Parking it and
	// signalling keeps one goroutine as the only sender on applyCh, which is
	// what makes delivery ordering mean anything.
	n.pendingSnapshot = &Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}

	// Through the funnel, so the applier is signalled and the §8 invariant
	// holds. lastApplied is deliberately NOT moved here: the applier advances
	// it once the image has actually been handed over, which is the same
	// "applied means past tense" rule the entry path follows.
	n.commitTo(args.LastIncludedIndex)

	n.markDirty()
}
