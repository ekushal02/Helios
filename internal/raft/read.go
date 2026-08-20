package raft

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
// =============================================================================
// WHY A BARRIER, AND WHY LOCAL STATE IS NOT AN ANSWER
// =============================================================================
//
// The tempting implementation is: if n.state == Leader, read the map. It is one
// comparison, it needs no network, and it is wrong three separate ways. The
// long version is DESIGN.md §8; the short version is here because this is where
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
// =============================================================================
// COST, AND WHAT WOULD MAKE IT CHEAPER
// =============================================================================
//
// This barrier is a real log write: an append, a round trip to a majority, and
// an apply, per read. The paper's ReadIndex is cheaper -- record commitIndex,
// confirm leadership with a heartbeat round, wait for lastApplied to reach the
// recorded index, and never touch the log. That version depends on the leader
// already knowing its commitIndex is current, which is only true once it has
// committed something in its own term. Which is the no-op-on-election this
// implementation does not have (DESIGN.md §10).
//
// So the log write is not a naive first draft; it is what is correct without
// the no-op. It also does the §5.4.2 flush for free, which is why the read path
// and the no-op are one decision rather than two.
//
// Lease-based reads are cheaper still and are a different bargain entirely:
// they trade a network round for an assumption about clock drift, and are
// unsafe if that assumption breaks. Not on the roadmap until there is a
// measurement saying the barrier costs too much.
func (n *Node) ReadIndex() (index int, term int, isLeader bool) {
	return n.appendAndReplicate(LogEntry{NoOp: true})
}