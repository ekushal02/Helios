package raft

import "time"

// =============================================================================
// SendPreVote for every transport in the suite
// =============================================================================
//
// Gathered here for the reason installsnapshot_transport_test.go gives: Go
// attaches a method to a type by package, not by file, and one place to look
// beats a dozen edits scattered through the suite.
//
// THESE THREE DELEGATE TO THEIR VOTE BEHAVIOUR. Each is configured with how its
// peers answer a vote, so answering pre-votes the same way keeps every test
// built on them meaning exactly what it meant -- the peers' opinion is
// unchanged, it is simply asked one round earlier.
//
// stubTransport is the exception and lives in stub_test.go, because delegation
// is wrong for it. silentPeers() is built on unreachable(), which refuses every
// vote: that is the point, it produces a node observable as a CANDIDATE that
// never wins. Delegating would refuse its polls too, leaving it a follower at
// term 0 and quietly changing what half of election_test.go tests. Its default
// is therefore to GRANT, which reproduces the pre-prevote behaviour exactly.

// asVoteArgs restates a pre-vote as the vote it is asking about, so a stub
// configured for one answers the other consistently.
func asVoteArgs(args *PreVoteArgs) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         args.Term,
		CandidateID:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	}
}

func delegateToVote(
	send func(int, *RequestVoteArgs, *RequestVoteReply) bool,
	to int, args *PreVoteArgs, reply *PreVoteReply,
) bool {
	var vote RequestVoteReply
	if !send(to, asVoteArgs(args), &vote) {
		return false
	}
	reply.Term, reply.VoteGranted = vote.Term, vote.VoteGranted
	return true
}

func (r *recordingTransport) SendPreVote(to int, args *PreVoteArgs, reply *PreVoteReply) bool {
	return delegateToVote(r.SendRequestVote, to, args, reply)
}

func (g *gatedTransport) SendPreVote(to int, args *PreVoteArgs, reply *PreVoteReply) bool {
	return delegateToVote(g.SendRequestVote, to, args, reply)
}

func (lf *liveFollower) SendPreVote(to int, args *PreVoteArgs, reply *PreVoteReply) bool {
	return delegateToVote(lf.SendRequestVote, to, args, reply)
}

// =============================================================================
// The fake network asks the real handler
// =============================================================================

// SendPreVote mirrors SendRequestVote exactly, and reaches the real PreVote
// receiver rather than any approximation of it. The partition tests depend on
// that: what they are measuring is a poll going unanswered.
func (e *endpoint) SendPreVote(to int, args *PreVoteArgs, reply *PreVoteReply) bool {
	target, delay, seq, dup, ok := e.net.route(kindPreVote, e.from, to)
	if !ok {
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	e.net.arrive(e.from, to, seq)

	var argsCopy PreVoteArgs
	mustRoundTrip(args, &argsCopy)

	if dup {
		// See harness_test.go's SendRequestVote for exactly what "a
		// second time" means here and why the duplicate's own reply is
		// discarded.
		var dupArgs PreVoteArgs
		mustRoundTrip(args, &dupArgs)
		e.net.countPreVote()
		var discard PreVoteReply
		target.PreVote(&dupArgs, &discard)
	}

	e.net.countPreVote()

	var replyCopy PreVoteReply
	target.PreVote(&argsCopy, &replyCopy)

	if !e.net.replyDeliverable(kindPreVote, e.from, to, seq) {
		return false
	}
	mustRoundTrip(&replyCopy, reply)
	return true
}

func (fn *fakeNetwork) countPreVote() {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.preVoteRPCs++
}

// preVotes reports how many polls were delivered. A test asserting that a
// partitioned node stopped disrupting the cluster needs this: zero would mean
// it never tried, and the quiet would prove nothing.
func (fn *fakeNetwork) preVotes() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.preVoteRPCs
}