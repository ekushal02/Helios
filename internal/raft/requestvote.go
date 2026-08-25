package raft

// RequestVote is the receiving side of the RequestVote RPC.
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	defer n.persistIfDirty()

	// --- Rules for Servers, All Servers ---
	// "If RPC request or response contains term T > currentTerm: set
	// currentTerm = T, convert to follower."
	//
	// A REAL VOTE REQUEST, UNLIKE A POLL, IS EVIDENCE. The sender has already
	// incremented its term and voted for itself, so the term it carries is one
	// a node genuinely holds and this node is genuinely behind. PreVote must
	// never do this -- see prevote.go -- which is the whole difference between
	// the two handlers.
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

	// --- Receiver rule 2, second half: the candidate's log must be at least as
	// up to date as this node's ---
	//
	// Shared with the pre-vote receiver, deliberately, and there is exactly one
	// definition of it. Polling before campaigning is only meaningful if the
	// poll answers by the rules the real vote will apply; two copies of §5.4.1
	// that could drift would make the pre-vote a different question from the
	// one it claims to be asking. See logIsAtLeastAsUpToDate in prevote.go.
	if !n.logIsAtLeastAsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return
	}

	// NO LEADER-STICKINESS CHECK HERE, and its absence is deliberate. §6's
	// remedy for disruptive servers is applied in PreVote, which is the only
	// entrance to an election in this implementation. A candidate that reaches
	// this point has already been told by a majority that it should campaign;
	// refusing it now would be a liveness cost buying no safety.
	n.votedFor = args.CandidateID
	n.markDirty()
	reply.VoteGranted = true

	// Figure 2: granting a vote resets the timer. A node that has just helped
	// somebody else campaign should not immediately campaign itself.
	n.resetElectionTimer()
}
