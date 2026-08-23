package raft

// Transport is how a Node reaches its peers
type Transport interface {
	SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
	SendInstallSnapshot(to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool
}

// It is the request for the RequestVote RPC, invoked by candidates
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// LogEntry is one command in the replicated log.
type LogEntry struct {
	// Term is the term in which the LEADER created this entry, not the term in which a follower received it.
	Term int

	// Command is the opaque client payload handed to the state machine on apply.
	Command []byte

	// NoOp marks an entry Raft created for its own purposes rather than one a
	// client submitted. It carries no command and the state machine must not
	// try to interpret one.
	//
	// A DEPARTURE FROM FIGURE 2, and a deliberate one. Figure 2's log holds
	// commands; the paper's no-op (§8) is described without saying how a node
	// recognises it. Two ways to do that here:
	//
	//   - A nil Command. Needs no new field, and the sentinel at index 0
	//     already uses exactly that convention. Rejected because gob collapses
	//     nil and empty -- TestHeartbeatEntriesNormaliseToNil pins that for
	//     Entries and the same is true of Command -- so an application could
	//     never legitimately commit an empty command again. Reserving part of
	//     the client's value space to encode a Raft-internal fact is the kind
	//     of coupling that is invisible until someone hits it.
	//   - This flag. Explicit, self-describing, and free on the wire: gob omits
	//     false, so a normal entry encodes exactly as it did before.
	//
	// The barrier is a Raft-level concept, not an application-level one -- a
	// leader must be able to append one before it knows anything about the
	// state machine above it -- so Raft owns the representation.
	NoOp bool
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
	// PrevLogTerm.
	Success bool

	ConflictIndex int
	ConflictTerm  int
}
