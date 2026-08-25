package raft

import (
	"context"
	"time"
)

// =============================================================================
// Pre-vote (dissertation §9.6), and §6's third issue
// =============================================================================
//
// THE DISRUPTION.
//
// A node cut off from its cluster times out, campaigns, and increments its
// term. Nobody answers, so it times out again, and again. Its term climbs for
// as long as the partition lasts -- unbounded, at roughly one increment per
// election timeout.
//
// When the partition heals it sends RequestVote at that inflated term. Every
// node it reaches, the healthy leader included, obeys the all-servers rule and
// adopts the higher term, so the leader steps down. The returning node cannot
// win -- §5.4.1 refuses it, its log is behind -- but a working cluster has just
// lost its leader and must hold an election for nothing. Writes pause for an
// election timeout. Nothing is unsafe; it is purely availability spent on a
// node that had no claim.
//
// THE FIX. Before incrementing anything, ask the question the increment was
// going to test: "if I campaigned at term+1, would you vote for me?" Only a
// majority of yeses justifies the real election.
//
// A pre-vote MUTATES NOTHING on the receiver. No term adoption, no vote
// recorded, no election timer reset, nothing persisted. That is the whole
// property -- it is a poll, not a message from a leader -- and it is why a
// partitioned node asking a thousand times costs the cluster nothing.
//
// LEADER STICKINESS COMES WITH IT, and it is not optional. Pre-vote alone
// handles a returning node whose log is BEHIND, because §5.4.1 refuses it. It
// does nothing about one whose log is current -- a node partitioned briefly, or
// one removed from the configuration and still running (§6's third issue). So a
// responder also refuses while it believes a leader exists: §6's "servers
// disregard RequestVote RPCs when they believe a current leader exists",
// applied here where every election begins.
//
// WHY ONLY HERE, and not in RequestVote too. Every election in Helios starts
// with a pre-vote, so the gate already sits at the only entrance. Putting it on
// the real handler as well would let a follower refuse a candidate that a
// majority has already agreed should campaign -- a liveness cost buying no
// safety this file does not already provide.
//
// THE COST, and it is real. A follower will not help depose a leader it heard
// from less than electionTimeoutMin ago. After a genuine leader failure the
// first pre-candidate fires at its own random deadline, 150-300ms after the
// last heartbeat, and responders measure the same interval from the same event
// -- so the margin is a network hop. An unlucky draw at the bottom of the range
// loses the round and retries one timeout later. Elections after a crash are
// therefore sometimes one timeout slower than they were.

// PreVoteArgs asks whether a real campaign would succeed.
//
// Term is the term the sender is PROPOSING, one above its own -- not a term it
// holds. That is the single most important difference from RequestVote, and the
// receiver must never treat it as evidence of anything.
type PreVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// PreVoteReply is an opinion, not a commitment. Granting one binds the sender
// to nothing: it may still refuse the real vote that follows, and two nodes may
// be granted a pre-vote for the same term without violating anything, because
// neither has been recorded.
type PreVoteReply struct {
	Term        int
	VoteGranted bool
}

// =============================================================================
// Receiver
// =============================================================================

// PreVote answers whether this node would vote for the sender at the proposed
// term.
//
// NOTHING IN THIS FUNCTION MUTATES STATE. Not currentTerm, not votedFor, not
// the election timer, and nothing is persisted. Read it as a query and keep it
// that way -- a single stepDownIfStale here would reintroduce exactly the
// disruption the mechanism exists to prevent, and every test in this file would
// still pass except the one that measures term inflation.
func (n *Node) PreVote(args *PreVoteArgs, reply *PreVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	// The proposed term must genuinely be newer. A sender that is behind
	// proposes a term this node has already seen, and campaigning at it would
	// achieve nothing.
	if args.Term <= n.currentTerm {
		return
	}

	// LEADER STICKINESS. A node that has heard from a leader recently believes
	// one exists, and will not help depose it. A leader believes it most of
	// all.
	if n.hasCurrentLeader() {
		return
	}

	// §5.4.1, the same restriction the real vote applies. Answering the
	// hypothetical honestly means answering it by the rules that would govern
	// the real thing.
	if !n.logIsAtLeastAsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return
	}

	reply.VoteGranted = true
}

// hasCurrentLeader reports whether this node believes a leader is in office.
//
// Dated from the last message ACCEPTED FROM A LEADER, not from the election
// deadline. The deadline is randomised over a range and is also reset by
// granting a vote, so reading it here would make the answer depend on which
// timeout this node happened to draw and on who else has been campaigning.
//
// Caller must hold mu.
func (n *Node) hasCurrentLeader() bool {
	if n.state == Leader {
		return true
	}
	if n.leaderID == None {
		return false
	}
	return time.Since(n.lastLeaderContact) < electionTimeoutMin
}

// logIsAtLeastAsUpToDate is §5.4.1, and there must be exactly one of it.
//
// "Up to date" is defined by the LAST ENTRY, comparing term first and only then
// length. A longer log is not a better one: entries from a deposed leader's
// term can pile up on a node that never committed any of them, and length alone
// would let that node outrank one holding fewer, newer, committed entries.
//
// Caller must hold mu.
func (n *Node) logIsAtLeastAsUpToDate(lastLogIndex, lastLogTerm int) bool {
	myTerm := n.lastLogTerm()
	if lastLogTerm != myTerm {
		return lastLogTerm > myTerm
	}
	return lastLogIndex >= n.lastLogIndex()
}

// =============================================================================
// Candidate side
// =============================================================================

// beginPreVote runs when the election deadline expires. It is what the ticker
// calls instead of becomeCandidate.
//
// Caller must hold mu.
func (n *Node) beginPreVote() {
	// Reset first, so a refused round retries on the ordinary schedule rather
	// than hammering.
	n.resetElectionTimer()

	// A single-node cluster is its own majority: there is nobody to poll and
	// nothing it could learn. Straight to the real election.
	if n.noPreVote || n.quorumSize() <= 1 {
		n.becomeCandidate()
		return
	}

	// One round at a time. A round always ends -- runPreVote is bounded by the
	// same context timeout runElection uses -- so this cannot wedge.
	if n.preVoting {
		return
	}

	term := n.currentTerm + 1
	args := &PreVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	peers := append([]int(nil), n.peers...)

	n.preVoting = true
	go n.runPreVote(term, args, peers)
}

// runPreVote polls the peers and, on a majority, starts the real election.
//
// NOTHING HERE IS PERSISTED, because nothing here is promised. Compare
// becomeCandidate, which must flush before its first RequestVote leaves: a
// candidate that crashes after voting for itself and comes back at the old term
// would vote twice. A pre-candidate has voted for nothing, so there is nothing
// a crash could make it forget -- which is also why this costs no fsync per
// timeout, and a partitioned node polling forever costs no disk at all.
func (n *Node) runPreVote(term int, args *PreVoteArgs, peers []int) {
	defer func() {
		n.mu.Lock()
		n.preVoting = false
		n.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), electionTimeoutMax)
	defer cancel()

	type result struct {
		reply *PreVoteReply
		ok    bool
	}
	results := make(chan result, len(peers))

	for _, peer := range peers {
		go func(to int) {
			var reply PreVoteReply
			ok := n.transport.SendPreVote(to, args, &reply)
			select {
			case results <- result{&reply, ok}:
			case <-ctx.Done():
			}
		}(peer)
	}

	grants := 1 // a node always pre-votes for itself

	for range peers {
		select {
		case <-ctx.Done():
			return // the window closed; the ticker will poll again
		case <-n.stopCh:
			return
		case r := <-results:
			if !r.ok {
				continue
			}

			n.mu.Lock()

			// term-1 is this node's term when the round started. A leader, or a
			// term that moved underneath, means the question is stale.
			if n.state == Leader || n.currentTerm != term-1 {
				n.mu.Unlock()
				return
			}

			// THE ONE MUTATION A PRE-VOTE ROUND CAN CAUSE, and it belongs to
			// the all-servers rule rather than to pre-voting: a reply carrying
			// a real term above this node's says it is behind, and a node that
			// learns that must step down whatever it was doing.
			if r.reply.Term > n.currentTerm {
				n.becomeFollower(r.reply.Term)
				n.persistIfDirty()
				n.mu.Unlock()
				return
			}

			if r.reply.VoteGranted {
				grants++
				if grants >= n.quorumSize() {
					// A majority says a real campaign would succeed. Only now
					// does the term move.
					n.becomeCandidate()
					n.mu.Unlock()
					return
				}
			}

			n.mu.Unlock()
		}
	}
	// Fell through without a majority: no term was spent finding that out,
	// which is the entire point.
}
