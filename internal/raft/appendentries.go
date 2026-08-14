package raft

// AppendEntries is the receiving side of the AppendEntries RPC (Figure 2, §5.3).
//
// B-10 handles only the heartbeat half: recognise the leader, reset the election
// timer, step down if needed. The log consistency check and entry handling
// arrive in C-4 and C-5.
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// --- Rules for Servers, All Servers ---
	n.stepDownIfStale(args.Term)

	reply.Term = n.currentTerm
	reply.Success = false

	// --- Receiver rule 1: reply false if term < currentTerm ---
	if args.Term < n.currentTerm {
		return
	}

	if n.state == Candidate {
		n.becomeFollower(args.Term)
	}

	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	// TODO (C-4): the consistency check. Reply false unless the log holds an
	// entry at PrevLogIndex whose term is PrevLogTerm:
	//     if args.PrevLogIndex > n.lastLogIndex() ||
	//        n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	//         return
	//     }
	// TODO (C-5): delete conflicting entries and everything after them, then
	// append whatever is new.
	// TODO (C-11): advance commitIndex from args.LeaderCommit.
	//
	// Until then this reports success for any request at a current term, which
	// is harmless only because no entries exist yet.
	reply.Success = true
}
