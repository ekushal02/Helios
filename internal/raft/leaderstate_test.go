package raft

import (
	"testing"
	"time"
)

// leaderWithLog builds a node holding `entries` real commands (plus the sentinel) and drives it into leadership at the given term.
func leaderWithLog(t *testing.T, entries int, term int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(term)), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	for i := 1; i <= entries; i++ {
		n.log = append(n.log, LogEntry{Term: term, Command: []byte{byte(i)}})
	}
	n.state = Candidate
	n.currentTerm = term
	n.becomeLeader()
	n.mu.Unlock()

	return n
}

func TestLeaderStateInitialisedOnEmptyLog(t *testing.T) {
	n := leaderWithLog(t, 0, 3)

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, p := range n.peers {
		if got := n.nextIndexFor(p); got != 1 {
			t.Errorf("nextIndex[%d] = %d, want 1 (lastLogIndex 0 + 1)", p, got)
		}
		if got := n.matchIndexFor(p); got != 0 {
			t.Errorf("matchIndex[%d] = %d, want 0", p, got)
		}
	}
}

// With a populated log, nextIndex is one past the leader's last entry: the optimistic bet that every follower is already caught up.
func TestNextIndexIsOnePastLeaderLog(t *testing.T) {
	n := leaderWithLog(t, 4, 3) // sentinel + 4 entries => lastLogIndex 4

	n.mu.Lock()
	defer n.mu.Unlock()

	if got := n.lastLogIndex(); got != 4 {
		t.Fatalf("test setup wrong: lastLogIndex = %d, want 4", got)
	}
	for _, p := range n.peers {
		if got := n.nextIndexFor(p); got != 5 {
			t.Errorf("nextIndex[%d] = %d, want 5", p, got)
		}
	}
}

// THE SAFETY TEST.
//
// A leader with a full log must still believe it has proven NOTHING about its
// followers. If matchIndex mirrored nextIndex for symmetry, C-10 would count a
// majority on the first pass and commit entries that exist on exactly one
// machine -- entries the cluster would lose on the next crash, after clients
// were told they were durable.
func TestMatchIndexStartsAtZeroRegardlessOfLogLength(t *testing.T) {
	n := leaderWithLog(t, 9, 3)

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, p := range n.peers {
		if got := n.matchIndexFor(p); got != 0 {
			t.Errorf("matchIndex[%d] = %d, want 0: a new leader has proven "+
				"nothing about any follower", p, got)
		}
	}
}

// The invariant that must hold at every moment of a leadership term, not just
// at initialisation: what is proven is always strictly behind what will be
// tried next. C-6 and C-7 both have to preserve this.
func TestMatchIndexAlwaysBelowNextIndex(t *testing.T) {
	n := leaderWithLog(t, 6, 3)

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, p := range n.peers {
		if n.matchIndexFor(p) >= n.nextIndexFor(p) {
			t.Errorf("peer %d: matchIndex %d >= nextIndex %d",
				p, n.matchIndexFor(p), n.nextIndexFor(p))
		}
	}
}

func TestLeaderStateExcludesSelf(t *testing.T) {
	n := leaderWithLog(t, 2, 3)

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.nextIndex[n.id]; ok {
		t.Error("nextIndex has an entry for the leader itself")
	}
	if _, ok := n.matchIndex[n.id]; ok {
		t.Error("matchIndex has an entry for the leader itself")
	}
	if len(n.nextIndex) != len(n.peers) {
		t.Errorf("nextIndex has %d entries, want %d (one per peer)",
			len(n.nextIndex), len(n.peers))
	}
}

func TestLeaderStateReinitialisedOnReelection(t *testing.T) {
	n := leaderWithLog(t, 4, 3)

	// Simulate a term of successful replication.
	n.mu.Lock()
	for _, p := range n.peers {
		n.matchIndex[p] = 4
		n.nextIndex[p] = 5
	}
	oldNext := n.nextIndex // keep the reference, not a copy

	n.becomeFollower(9)
	n.log = append(n.log, LogEntry{Term: 9, Command: []byte("x")})
	n.state = Candidate
	n.becomeLeader()

	defer n.mu.Unlock()

	for _, p := range n.peers {
		if got := n.matchIndexFor(p); got != 0 {
			t.Errorf("matchIndex[%d] = %d after re-election, want 0: last "+
				"term's proof is worthless in this one", p, got)
		}
		if got := n.nextIndexFor(p); got != 6 {
			t.Errorf("nextIndex[%d] = %d after re-election, want 6", p, got)
		}
	}

	if &oldNext == &n.nextIndex {
		t.Fatal("same map header")
	}
	oldNext[1] = 999
	if n.nextIndexFor(1) == 999 {
		t.Error("a write through the previous term's map reached current state")
	}
}

// Initialisation must complete before the first heartbeat goes out, or the heartbeat reads a nil map.
func TestLeaderStateReadyBeforeFirstHeartbeat(t *testing.T) {
	seen := make(chan int, 8)

	stub := newStubTransport(denyAll(3))
	stub.appendAnswer = func(to int, args *AppendEntriesArgs) (AppendEntriesReply, bool) {
		seen <- args.PrevLogIndex
		return AppendEntriesReply{Term: 3, Success: true}, true
	}

	n := NewNode(0, []int{1, 2}, stub, 1)
	defer n.Stop()

	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 3, Command: []byte("a")})
	n.state = Candidate
	n.currentTerm = 3
	n.becomeLeader()
	n.mu.Unlock()

	select {
	case got := <-seen:
		if got != 1 {
			t.Errorf("first heartbeat carried PrevLogIndex %d, want 1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no heartbeat sent")
	}
}
