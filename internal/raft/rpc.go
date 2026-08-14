package raft

// Transport is how a Node reaches its peers
type Transport interface {
	SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
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

// AppendEntriesArgs is the request for the AppendEntries RPC.
type AppendEntriesArgs struct {
	Term int

	LeaderID int

	// PrevLogIndex is the index of the entry immediately preceding Entries, and
	// PrevLogTerm is that entry's term. Together they are the consistency check
	// a follower runs before accepting anything (C-4).
	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	// LeaderCommit is the leader's commitIndex, which is how followers learn
	// what has been committed (C-11).
	LeaderCommit int
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term int

	// Success is true if the follower held an entry matching PrevLogIndex and
	// PrevLogTerm. Until C-4 the check is not implemented and this is true for
	// any request at a current term.
	Success bool
}
