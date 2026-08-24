package raft

import (
	"context"
	"time"
)

// Timing constants.
const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond

	//tickInterval is how often the ticker checks the deadline.
	tickInterval = 10 * time.Millisecond
)

// randomElectionTimeout draws a timeout from [min, max).
func (n *Node) randomElectionTimeout() time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(n.rng.Int63n(span))
}

// resetElectionTimer pushes the deadline out by a fresh random timeout.
func (n *Node) resetElectionTimer() {
	n.electionDeadline = time.Now().Add(n.randomElectionTimeout())
}

// majority is the number of votes needed to win, counting this node itself.
func (n *Node) majority() int {
	return n.quorumSize()
}

// Start begins the node's background loops.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	go n.ticker()
}

// Stop shuts the background loops down
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
	})
}

// ticker is the single goroutine that watches the election deadline.
func (n *Node) ticker() {
	t := time.NewTicker(tickInterval)
	defer t.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case <-t.C:
			n.mu.Lock()

			// A leader never times itself out; it is the one sending heartbeats.
			if n.state == Leader {
				n.mu.Unlock()
				continue
			}

			// A server outside the current configuration must not campaign.
			if !n.inConfig {
				n.resetElectionTimer()
				n.mu.Unlock()
				continue
			}

			if time.Now().After(n.electionDeadline) {
				n.becomeCandidate()
			}

			n.mu.Unlock()
		}
	}
}

// stepDownIfStale implements the rule that governs every state:
// "if an RPC request or response contains term T > currentTerm, set currentTerm = T and convert to follower"
func (n *Node) stepDownIfStale(term int) bool {
	if term <= n.currentTerm {
		return false
	}
	n.becomeFollower(term)
	return true
}

// becomeCandidate performs the follower-or-candidate to candidate transition.
func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.leaderID = None
	n.currentTerm++
	n.votedFor = n.id // a real vote, counted toward majority()
	n.resetElectionTimer()

	n.markDirty()
	n.persistIfDirty()

	// Snapshot everything the election needs WHILE the lock is held, so
	// runElection never has to touch node state to build its request.
	term := n.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	peers := append([]int(nil), n.peers...)

	// A single-node cluster has already won: its own vote is a majority.
	if n.majority() <= 1 {
		n.becomeLeader()
		return
	}

	go n.runElection(term, args, peers)
}

// becomeFollower steps down and adopts a newer term.
func (n *Node) becomeFollower(term int) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.leaderID = None
		n.votedFor = None

		n.markDirty()
	}

	n.state = Follower
	n.resetElectionTimer()
}

// becomeLeader takes leadership after winning a majority.
func (n *Node) becomeLeader() {
	if n.state != Candidate {
		return
	}
	n.state = Leader

	n.leaderID = n.id

	n.initLeaderState()

	go n.heartbeatLoop(n.currentTerm)

}

// runElection asks every peer for a vote in parallel and counts the answers.
func (n *Node) runElection(term int, args *RequestVoteArgs, peers []int) {

	ctx, cancel := context.WithTimeout(context.Background(), electionTimeoutMax)
	defer cancel()

	type result struct {
		reply *RequestVoteReply
		ok    bool
	}
	results := make(chan result, len(peers))

	for _, peer := range peers {
		go func(to int) {
			var reply RequestVoteReply
			ok := n.transport.SendRequestVote(to, args, &reply)
			select {
			case results <- result{&reply, ok}:
			case <-ctx.Done():
			}
		}(peer)
	}

	votes := 1 //this node already voted for itself in becomeCandidate

	for range peers {
		select {
		case <-ctx.Done():
			return // election window closed; the ticker will start a new one
		case <-n.stopCh:
			return
		case r := <-results:
			if !r.ok {
				continue
			}

			n.mu.Lock()

			if n.state != Candidate || n.currentTerm != term {
				n.mu.Unlock()
				return
			}

			// The peer knows about a newer term, so this election is void.
			if r.reply.Term > n.currentTerm {
				n.becomeFollower(r.reply.Term)
				n.mu.Unlock()
				return
			}

			if r.reply.VoteGranted {
				votes++
				if votes >= n.majority() {
					n.becomeLeader()
					n.mu.Unlock()
					return
				}
			}

			n.mu.Unlock()
		}
	}
	// Fell through without a majority. Stay a candidate; the election timer fires again and campaigns at a higher term.
}
