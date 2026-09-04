package raft

import (
	"sort"
	"time"
)

// ReadIndex begins a linearizable read.
//
// It appends a barrier entry in the current term and returns the index that
// entry landed at. The caller reads its own state machine only after observing
// that index applied CARRYING THE RETURNED TERM. The same claim ticket Submit
// hands out, used for a different purpose: an index that comes back under a
// different term means this node was deposed and the read must start over
// rather than be served.
//
// The full protocol, on the layer above:
//
//	idx, term, isLeader := n.ReadIndex()
//	if !isLeader          -> redirect to the leader
//	wait for the state machine to reach idx
//	if the term applied at idx != term -> deposed; start over
//	read local state
//
// Raft returns an index and nothing else, because Raft does not hold the data.
// Everything above ApplyCh does. A ReadIndex that returned a value would mean
// this file re-deriving state the applier has already established, and the two
// derivations would eventually disagree -- the same argument that keeps the
// state machine out of n.log.
//
// This is the SAFE path and the fallback for the lease path below. It costs a
// log write and a round trip; ReadLease costs nothing and is correct only under
// an assumption about clocks.
//
// =============================================================================
// THE ONE WAY TO DEADLOCK THIS
// =============================================================================
//
// NEVER PERFORM A READ ON THE GOROUTINE THAT CONSUMES ApplyCh.
//
// Step two of the protocol waits for the state machine to reach an index, and
// the state machine only reaches indices because some goroutine is draining
// ApplyCh. If that is the same goroutine, it is waiting for a delivery it is
// itself responsible for making, and neither ever happens. The applier upstream
// parks on an unbuffered send, so the stall is silent and total: no error, no
// timeout, nothing in the log.
//
// The rule for the server layer: one goroutine owns ApplyCh and does nothing
// but apply. Reads are served from other goroutines, which read the state
// machine under its own lock. That is the same separation ApplyCh's doc comment
// already requires for a different reason -- exactly one consumer, so that
// delivery order becomes application order -- and it happens to make this
// deadlock unreachable too.
//
// It applies to the lease path identically. ReadLease itself never blocks, but
// step two of ITS protocol is the same wait.
//
// =============================================================================
// WHY A BARRIER, AND WHY LOCAL STATE IS NOT AN ANSWER
// =============================================================================
//
// The tempting implementation is: if n.state == Leader, read the map. It is one
// comparison, it needs no network, and it is wrong three separate ways. The
// long version is DESIGN.md §9; the short version is here because this is where
// someone will be standing when they decide to delete this function.
//
// 1. LEADERSHIP IS NOT A LOCAL FACT. A leader partitioned into a minority does
// not know it. It keeps state == Leader until something tells it otherwise, and
// nothing can: the majority elected a successor and is committing writes on the
// far side of a cut it cannot see. Its own log and commitIndex are perfectly
// self-consistent. It serves confident, stale answers for as long as the
// partition lasts. No local check detects this, because every local check is a
// statement about the past -- "I was leader when I last heard from someone".
// Only a fresh majority round is evidence about now, and committing the barrier
// IS that round: a deposed leader cannot get a majority to accept an entry in
// its term, so the read never completes instead of completing wrongly.
//
// 2. A LEADER CAN BE BEHIND ITS OWN commitIndex. commitIndex moves in a reply
// handler; lastApplied moves when the applier hands the entry over. Between
// those two the node holds a write it has acknowledged and has not applied.
// Reading the map there omits a write the client was already told succeeded.
// Waiting for the barrier to APPLY, not merely commit, closes it.
//
// 3. A FRESH LEADER IS MISSING WRITES ITS PREDECESSOR ACKNOWLEDGED. §5.4.2
// forbids committing an inherited entry by counting replicas, so a new leader
// holds entries that were committed by the leader before it and are not yet
// committed here. Its state machine does not have them. The barrier fixes this
// as a side effect: it is a current-term entry, so committing it commits the
// whole prefix beneath it by Log Matching.
//
// And on followers, before anyone asks: a follower's state machine is behind by
// construction. It applies what LeaderCommit told it, which trails the leader
// by at least one message. Write to the leader, get an acknowledgement, read
// from a follower, see the old value. That is not a race to be narrowed; it is
// the design.
//
// NOTE WHICH OF THE THREE THE LEASE REMOVES. Only the first, and only under an
// assumption. Two and three are structural and ReadLease gates on them
// explicitly.
func (n *Node) ReadIndex() (index int, term int, isLeader bool) {
	return n.appendAndReplicate(LogEntry{NoOp: true})
}

// =============================================================================
// Lease reads
// =============================================================================

const (
	// maxClockDriftPercent bounds how far any two nodes' clocks may differ in
	// RATE -- not in absolute offset, which is irrelevant here because nothing
	// compares timestamps across machines. Ten percent is enormously
	// conservative for NTP-synchronised hardware, where drift is measured in
	// parts per million. It is not conservative at all against a descheduled
	// process, which is the real hazard; see the note on pauses below.
	maxClockDriftPercent = 10

	// leaseDuration is how long a completed majority round entitles this node
	// to answer reads from local state.
	//
	// THE DERIVATION, because the factor is the whole safety argument.
	//
	// A follower resets its election timer when it RECEIVES a message, and will
	// not campaign until electionTimeoutMin has elapsed ON ITS OWN CLOCK. This
	// node must therefore stop trusting the lease before that instant, measured
	// on ITS clock, in the worst case where its own clock is slow by the drift
	// bound and the follower's is fast by the same.
	//
	// Leader's clock slow by d: a lease of L takes L/(1-d) real time.
	// Follower's clock fast by d: a timeout of E takes E/(1+d) real time.
	// Safe when L/(1-d) < E/(1+d), i.e. L < E * (1-d)/(1+d).
	//
	// With d = 10%, that is electionTimeoutMin * 90/110 ~= 123ms against a
	// 150ms floor. Integer arithmetic rather than a float expression, because a
	// float conversion of a Duration is not a constant expression in Go.
	leaseDuration = electionTimeoutMin * (100 - maxClockDriftPercent) / (100 + maxClockDriftPercent)
)

// ReadLease reports whether this node may serve a read from local state without
// a round trip.
//
// Returns the index the state machine must reach first, the instant the
// permission expires, and whether it was granted at all. A false third return
// is not an error: it means fall back to ReadIndex, which is always available.
//
// =============================================================================
// WHAT IT IS SAFE TO CONCLUDE FROM A COMPLETED ROUND
// =============================================================================
//
// If a majority of the cluster answered this node's AppendEntries, then every
// one of those nodes received it, and every one of them reset its election
// timer at or after the moment it was SENT. None of them can campaign for a
// full election timeout after that. An election needs a majority of votes, and
// any two majorities intersect, so no candidate can assemble one without a vote
// from a node whose timer this leader just reset. Therefore no other leader can
// exist during the lease, and this node's state is authoritative.
//
// The whole argument is contingent on the two clocks agreeing about how long
// "a full election timeout" is. See DESIGN.md §9 for what that costs.
//
// =============================================================================
// THE TWO GATES THE LEASE DOES NOT REMOVE
// =============================================================================
//
// The lease answers ReadIndex's objection 1 and nothing else. Objections 2 and
// 3 are structural, so both are checked here rather than assumed away:
//
//   - The caller must still wait for the state machine to reach the returned
//     index. commitIndex moves in a reply handler, lastApplied when the applier
//     hands over; between them sits an acknowledged write that has not run.
//
//   - This node must have committed an entry in its OWN term, or the read is
//     refused outright. §5.4.2 means a fresh leader has not yet committed the
//     entries its predecessor committed, so its commitIndex does not cover
//     them and its state machine is missing acknowledged writes. Once ANY
//     current-term entry commits, the whole prefix commits with it by Log
//     Matching -- and every inherited entry sits below any current-term entry,
//     so that single condition covers all of them.
//
// That second gate is the same dependency the paper's cheaper ReadIndex has on
// the no-op entry appended at election. Helios does not have that no-op, so a
// leader cannot serve lease reads until a client write happens to land. On a
// write-idle cluster the lease is simply unavailable and every read pays for a
// barrier, which is correct and is the price of the deferral.
//
// Caller must hold nothing.
func (n *Node) ReadLease() (readIndex int, until time.Time, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return 0, time.Time{}, false
	}

	// Checked before anything below reads lastContact: a rollback detected
	// here clears it, so leaseExpiry (next) sees the cleared map rather than
	// stale, frozen, pre-rollback entries. See detectClockRollback's own
	// doc for why this ordering is load-bearing, not incidental.
	now := n.clock.Now()
	n.detectClockRollback(now)

	// Gate two: nothing committed, or the newest committed entry is inherited.
	if n.commitIndex == 0 || n.log[n.commitIndex].Term != n.currentTerm {
		return 0, time.Time{}, false
	}

	until = n.leaseExpiry(now)
	if !now.Before(until) {
		return 0, time.Time{}, false
	}

	return n.commitIndex, until, true
}

// detectClockRollback compares observed against the highest value this node
// has ever seen on the lease-critical path and, if observed is EARLIER,
// clears every recorded lastContact entry.
//
// =============================================================================
// WHY THIS EXISTS: "A LEADER WHOSE CLOCK JUMPS BACKWARDS" (Phase G-6)
// =============================================================================
//
// noteContact's own monotonicity guard -- "if sentAt.After(n.lastContact[peer])"
// -- exists to stop a late reply to an old message from dragging the lease
// backwards (its own doc comment explains why). That SAME guard, under a
// clock that jumps backwards, becomes the bug: every sentAt recorded AFTER
// the jump is, numerically, EARLIER than whatever was recorded before it, so
// the guard refuses every single one of them, forever. lastContact freezes
// at its pre-jump values. leaseExpiry keeps computing an expiry from those
// frozen values, and clock.Now() -- ALSO past the jump, so ALSO smaller --
// keeps satisfying now.Before(until) against that frozen expiry, potentially
// indefinitely. To this node, everything looks internally consistent: its
// own clock, its own lastContact, its own comparison. What it cannot see is
// that real wall-clock time -- and every OTHER node's own, unrolled-back
// clock -- kept moving the entire time. A new leader can be elected and
// commit writes on the other side of that gap while this node still
// believes its lease is live. See
// TestReadLeaseIsSafeWhenTheLeadersClockJumpsBackward for this traced all
// the way through, and DESIGN.md §26 for the fuller argument.
//
// =============================================================================
// WHY THIS IS CALLED ONLY FROM ReadLease, NEVER FROM noteContact
// =============================================================================
//
// It was called from noteContact too, briefly, on the theory that catching a
// rollback as soon as any new contact arrived was strictly more defensive
// than waiting for the next ReadLease call. That version broke healthy
// leaders in real, unmodified operation: replicateAll fans AppendEntries out
// to every peer AT ONCE, one goroutine per peer, each stamping its own sentAt
// independently before its own RPC round-trips. Two peers' round trips do
// not complete in the order their sentAt values were captured in -- network
// and scheduling variance sees to that -- so noteContact for the peer that
// left LATER but answered FIRST can be followed by noteContact for the peer
// that left EARLIER but answered SLOWER, presenting this function with an
// observed time that is smaller than one it already saw, despite the clock
// never having done anything but advance. Comparing against a single global
// high-water mark from inside noteContact could not tell that apart from a
// real rollback, and cleared a perfectly healthy leader's lastContact on
// essentially any round where two peers' replies happened to arrive out of
// send order -- which, empirically, was often enough to make
// TestALeaseIsRefusedUntilSomethingCommitsInThisTerm and three of its
// siblings fail intermittently the first time this ran against a real
// scheduler rather than this package's own single-machine test harness.
//
// ReadLease does not have this problem: every call holds n.mu for its own
// entire body, so two calls' own now := n.clock.Now() reads are strictly
// ordered by the lock itself, in true chronological order, never
// interleaved with each other. A smaller observation there really is a
// rollback. noteContact keeps its own, PER-PEER monotonicity guard --
// correct, and unrelated to this -- and touches nothing global.
//
// The fix does not try to reconstruct what the clock "really" did. It
// forgets: a rollback proves this node's own clock is no longer trustworthy
// for dating ANY prior evidence, so every prior contact is discarded, and
// the lease must be re-earned exactly the way a freshly-elected leader
// already has to -- no lease until a majority answers again, this time
// timestamped by a clock this node has actual reason to trust. A clock that
// jumps FORWARD needs no equivalent handling: it only makes the lease expire
// sooner, which is the one direction this whole mechanism was already
// designed to be safe to be wrong in (see leaseDuration's own derivation,
// and noteContact's).
//
// Caller must hold n.mu, and must be ReadLease specifically -- see above.
func (n *Node) detectClockRollback(observed time.Time) {
	if !n.maxObservedNow.IsZero() && observed.Before(n.maxObservedNow) {
		for p := range n.lastContact {
			delete(n.lastContact, p)
		}
	}
	if observed.After(n.maxObservedNow) {
		n.maxObservedNow = observed
	}
}

// leaseExpiry returns the instant this node must stop trusting its own state,
// or the zero time if it has not heard from enough peers to have a lease at
// all. now is the caller's own already-read clock.Now(), reused here rather
// than read a second time.
//
// THE kTH MOST RECENT CONTACT, not the most recent. One peer answering does not
// make a majority; the lease runs from the moment the majority was last
// complete. With five nodes a leader needs two peers, so the lease is dated
// from the SECOND most recent contact -- the older of the two that make up the
// quorum.
//
// Caller must hold mu.
func (n *Node) leaseExpiry(now time.Time) time.Time {
	// Peers needed besides this node, which always agrees with itself.
	need := n.quorumSize() - 1

	// A single-node cluster is its own majority and nobody else can be elected,
	// so there is nothing to wait to hear from. The lease is always live.
	if need <= 0 {
		return now.Add(leaseDuration)
	}

	heard := make([]time.Time, 0, len(n.peers))
	for _, p := range n.peers {
		if t, ok := n.lastContact[p]; ok {
			heard = append(heard, t)
		}
	}
	if len(heard) < need {
		return time.Time{}
	}

	sort.Slice(heard, func(i, j int) bool { return heard[i].After(heard[j]) })
	return heard[need-1].Add(leaseDuration)
}

// noteContact records that peer answered a message sent at sentAt.
//
// DATED FROM THE SEND, NOT THE REPLY, and the direction matters. The follower
// reset its timer when it received the message, which is at or after sentAt, so
// its deadline is at or after sentAt + electionTimeoutMin. Dating the contact
// from the send therefore understates how long this node may trust the lease,
// which is the only direction it is safe to be wrong in. Dating it from the
// reply would overstate it by a full one-way latency.
//
// Monotonic, for the same reason matchIndex is: replies arrive out of order, and
// a late answer to an older message must not drag the lease backwards.
//
// Caller must hold mu.
func (n *Node) noteContact(peer int, sentAt time.Time) {
	if sentAt.After(n.lastContact[peer]) {
		n.lastContact[peer] = sentAt
	}
}