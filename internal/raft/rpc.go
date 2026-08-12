package raft

// Transport is how a Node reaches its peers
type Transport interface {
	SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool
}

// It is the request for the RequestVote RPC, invoked by candidates
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// It is the response to a RequestVote RPC
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
