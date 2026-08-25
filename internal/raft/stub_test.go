package raft

import (
	"sync"
	"time"
)

// stubTransport is a fake set of peers that answer however the test tells them
// to. It exists so the CANDIDATE side (B-6) can be tested before the ANSWERING
// side (B-7) is written -- testing one half of a conversation with a scripted
// stand-in for the other half.
//
// The full fakeNetwork in harness_test.go takes over once real nodes can answer
// each other.
type stubTransport struct {
	mu sync.Mutex

	answer func(to int, args *RequestVoteArgs) (RequestVoteReply, bool)

	preVoteAnswer func(to int, args *PreVoteArgs) (PreVoteReply, bool)

	appendAnswer func(to int, args *AppendEntriesArgs) (AppendEntriesReply, bool)

	calls       []int // every peer id contacted, in arrival order
	appendCalls []int
}

func newStubTransport(answer func(to int, args *RequestVoteArgs) (RequestVoteReply, bool)) *stubTransport {
	return &stubTransport{answer: answer}
}

// setAppendAnswer installs the AppendEntries responder under the lock it is read
// under. Assigning the field directly is safe only while the write happens-before
// the node exists, which is true of every current caller and is not a property
// worth relying on.
func (s *stubTransport) setAppendAnswer(f func(to int, args *AppendEntriesArgs) (AppendEntriesReply, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendAnswer = f
}

func (s *stubTransport) SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	s.mu.Lock()
	s.calls = append(s.calls, to)
	answer := s.answer
	s.mu.Unlock()

	// Deliberately called WITHOUT the stub's lock held, so a slow peer does not block the others.
	r, ok := answer(to, args)
	if ok {
		*reply = r
	}
	return ok
}

func (s *stubTransport) SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	s.mu.Lock()
	s.appendCalls = append(s.appendCalls, to)
	answer := s.appendAnswer
	s.mu.Unlock()

	if answer == nil {
		return false // unreachable by default
	}

	r, ok := answer(to, args)
	if ok {
		*reply = r
	}
	return ok

}

func (s *stubTransport) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *stubTransport) contacted() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.calls...)
}

// grantAll votes yes for everyone.
func grantAll(term int) func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
	return func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
		return RequestVoteReply{Term: term, VoteGranted: true}, true
	}
}

// denyAll votes no for everyone, without claiming a newer term.
func denyAll(term int) func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
	return func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
		return RequestVoteReply{Term: term, VoteGranted: false}, true
	}
}

// unreachable simulates every peer being cut off.
func unreachable() func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
	return func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
		return RequestVoteReply{}, false
	}
}

// slowPeer makes one specific peer take a long time while the rest answer instantly.
func slowPeer(slow int, delay time.Duration, term int) func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
	return func(to int, _ *RequestVoteArgs) (RequestVoteReply, bool) {
		if to == slow {
			time.Sleep(delay)
		}
		return RequestVoteReply{Term: term, VoteGranted: true}, true
	}
}

func (s *stubTransport) setPreVoteAnswer(f func(to int, args *PreVoteArgs) (PreVoteReply, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preVoteAnswer = f
}

func (s *stubTransport) SendPreVote(to int, args *PreVoteArgs, reply *PreVoteReply) bool {
	s.mu.Lock()
	answer := s.preVoteAnswer
	s.mu.Unlock()

	if answer == nil {
		*reply = PreVoteReply{Term: args.Term - 1, VoteGranted: true}
		return true
	}
	r, ok := answer(to, args)
	if ok {
		*reply = r
	}
	return ok
}
