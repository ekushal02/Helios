package raft

import (
	"sync"
	"testing"
	"time"
)

// waitForState polls until the node reaches want, or the deadline passes.
func waitForState(t *testing.T, n *Node, want State, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if s, _, _ := n.snapshotState(); s == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	s, term, _ := n.snapshotState()
	t.Fatalf("never reached %v within %v (state=%v term=%d)", want, within, s, term)
}

// argsRecorder captures the arguments each peer was sent.
type argsRecorder struct {
	mu   sync.Mutex
	seen []RequestVoteArgs
}

func (r *argsRecorder) record(args *RequestVoteArgs) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, *args)
}

func (r *argsRecorder) all() []RequestVoteArgs {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RequestVoteArgs(nil), r.seen...)
}

// Every peer must be contacted, exactly once.
func TestElectionContactsAllPeers(t *testing.T) {
	stub := newStubTransport(denyAll(1))
	n := NewNode(0, []int{1, 2, 3, 4}, stub, 1)

	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	time.Sleep(50 * time.Millisecond)

	if got := stub.callCount(); got != 4 {
		t.Errorf("contacted %d peers, want 4 (got %v)", got, stub.contacted())
	}
}

// The request must carry this node's identity and log position, or the receiver cannot run the up-to-date check in B-8.
func TestElectionSendsCorrectArgs(t *testing.T) {
	rec := &argsRecorder{}
	stub := newStubTransport(func(_ int, args *RequestVoteArgs) (RequestVoteReply, bool) {
		rec.record(args)
		return RequestVoteReply{Term: args.Term, VoteGranted: false}, true
	})

	n := NewNode(7, []int{1, 2}, stub, 1)
	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 3}, LogEntry{Term: 4})
	n.currentTerm = 4
	n.becomeCandidate() // term becomes 5
	n.mu.Unlock()

	time.Sleep(50 * time.Millisecond)

	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("recorded %d requests, want 2", len(got))
	}

	// Every peer must receive identical args.
	for i, seen := range got {
		if seen.Term != 5 {
			t.Errorf("request %d: Term = %d, want 5", i, seen.Term)
		}
		if seen.CandidateID != 7 {
			t.Errorf("request %d: CandidateID = %d, want 7", i, seen.CandidateID)
		}
		if seen.LastLogIndex != 2 {
			t.Errorf("request %d: LastLogIndex = %d, want 2", i, seen.LastLogIndex)
		}
		if seen.LastLogTerm != 4 {
			t.Errorf("request %d: LastLogTerm = %d, want 4", i, seen.LastLogTerm)
		}
	}
}

// A majority of yes votes wins.
func TestElectionWinsWithMajority(t *testing.T) {
	stub := newStubTransport(grantAll(1))
	n := NewNode(0, []int{1, 2}, stub, 1)

	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	waitForState(t, n, Leader, 200*time.Millisecond)
}

// A single-node cluster wins on its own vote without sending anything.
func TestSingleNodeElectsItself(t *testing.T) {
	stub := newStubTransport(denyAll(1))
	n := NewNode(0, nil, stub, 1)

	n.mu.Lock()
	n.becomeCandidate()
	state := n.state
	n.mu.Unlock()

	if state != Leader {
		t.Errorf("single node should lead immediately, got %v", state)
	}
	if got := stub.callCount(); got != 0 {
		t.Errorf("single node sent %d RPCs, want 0", got)
	}
}

// Denied by everyone: stay a candidate. The timer will retry at a higher term.
func TestElectionLosesAndStaysCandidate(t *testing.T) {
	stub := newStubTransport(denyAll(1))
	n := NewNode(0, []int{1, 2}, stub, 1)

	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	time.Sleep(50 * time.Millisecond)

	if s, _, _ := n.snapshotState(); s != Candidate {
		t.Errorf("state = %v, want candidate after losing", s)
	}
}

// Every peer unreachable: still a candidate, no panic, no hang.
func TestElectionWithAllPeersUnreachable(t *testing.T) {
	stub := newStubTransport(unreachable())
	n := NewNode(0, []int{1, 2}, stub, 1)

	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	time.Sleep(50 * time.Millisecond)

	if s, _, _ := n.snapshotState(); s != Candidate {
		t.Errorf("state = %v, want candidate when nobody answers", s)
	}
}

// A reply carrying a newer term ends the election immediately.
func TestElectionStepsDownOnHigherTerm(t *testing.T) {
	stub := newStubTransport(func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
		return RequestVoteReply{Term: 99, VoteGranted: false}, true
	})
	n := NewNode(0, []int{1, 2}, stub, 1)

	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	waitForState(t, n, Follower, 200*time.Millisecond)

	if _, term, votedFor := n.snapshotState(); term != 99 || votedFor != None {
		t.Errorf("after stepping down: term=%d votedFor=%d, want 99 and None", term, votedFor)
	}
}

func TestElectionIsParallel(t *testing.T) {
	const slowDelay = 400 * time.Millisecond

	stub := newStubTransport(slowPeer(1, slowDelay, 1))
	n := NewNode(0, []int{1, 2, 3}, stub, 1)

	start := time.Now()
	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	waitForState(t, n, Leader, slowDelay-50*time.Millisecond)

	if elapsed := time.Since(start); elapsed >= slowDelay {
		t.Errorf("election took %v: peers were contacted sequentially", elapsed)
	}
}

// A vote that arrives after the node has moved to a newer term must be discarded.
func TestStaleVoteRepliesAreIgnored(t *testing.T) {
	release := make(chan struct{})

	stub := newStubTransport(func(int, *RequestVoteArgs) (RequestVoteReply, bool) {
		<-release // hold every reply until the test says go
		return RequestVoteReply{Term: 1, VoteGranted: true}, true
	})

	n := NewNode(0, []int{1, 2}, stub, 1)

	n.mu.Lock()
	n.becomeCandidate() // term 1: replies are now in flight and blocked
	n.mu.Unlock()

	// The world moves on: this node steps down and a newer term arrives.
	n.mu.Lock()
	n.becomeFollower(5)
	n.mu.Unlock()

	close(release) // the term-1 votes finally land

	time.Sleep(50 * time.Millisecond)

	s, term, _ := n.snapshotState()
	if s == Leader {
		t.Error("won an election using votes from a term it has left")
	}
	if term != 5 {
		t.Errorf("term = %d, want 5", term)
	}
}
