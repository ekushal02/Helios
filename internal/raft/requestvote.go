package raft

// isUpToDate reports whether a candidate's log is at least as up to date as this node's,
func (n *Node) isUpToDate(candidateLastIndex, candidateLastTerm int) bool {
	myTerm := n.lastLogTerm()
	myIndex := n.lastLogIndex()

	// Different last terms: the later term wins outright, whatever the lengths.
	if candidateLastTerm != myTerm {
		return candidateLastTerm > myTerm
	}

	return candidateLastIndex >= myIndex
}

// RequestVote is the receiving side of the RequestVote RPC.
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	defer n.persistIfDirty()

	// --- Rules for Servers, All Servers ---
	// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower."
	n.stepDownIfStale(args.Term)

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	// --- Receiver rule 1: reply false if term < currentTerm ---

	if args.Term < n.currentTerm {
		return
	}

	// --- Receiver rule 2, first half: votedFor is null or candidateId ---
	if n.votedFor != None && n.votedFor != args.CandidateID {
		return
	}

	// --- Receiver rule 2, second half: the candidate's log must be at least as up to date as this node's ---
	if !n.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return
	}

	n.votedFor = args.CandidateID
	n.markDirty()
	reply.VoteGranted = true

	n.resetElectionTimer()

	// TODO (D-1): votedFor has changed and must reach stable storage BEFORE
	// this reply is sent. A node that grants a vote, crashes, restarts having
	// forgotten, and grants again in the same term produces two leaders --
	// the scenario written up in DESIGN.md §5.
	//     n.persist()
}
