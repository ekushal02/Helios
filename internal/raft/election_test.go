package raft

import (
	"testing"
	"time"
)

// --- test observers -------------------------------------------------------

func (n *Node) snapshotState() (State, int, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm, n.votedFor
}

// --- B-5: the candidate transition ---------------------------------------

// silentPeers is the transport for tests that care about state transitions but not about voting.
func silentPeers() *stubTransport {
	return newStubTransport(unreachable())
}

func TestBecomeCandidateFromFollower(t *testing.T) {
	n := NewNode(1, []int{0, 2}, silentPeers(), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	time.Sleep(2 * time.Millisecond) //so the deadline visibly moves

	n.mu.Lock()
	n.becomeCandidate()
	after := n.electionDeadline
	n.mu.Unlock()

	state, term, votedFor := n.snapshotState()
	remaining := time.Until(after)

	if state != Candidate {
		t.Errorf("state: want candidate, got %v", state)
	}
	if term != 1 {
		t.Errorf("term: want 1, got %d", term)
	}
	if votedFor != n.id {
		t.Errorf("votedFor: want self (%d), got %d", n.id, votedFor)
	}

	if remaining <= 0 || remaining > electionTimeoutMax {
		t.Errorf("election timer was not reset correctly: remaining=%v", remaining)
	}
}

// A candidate whose election does not resolve retries at a HIGHER term.
func TestBecomeCandidateFromCandidateIncrementsTerm(t *testing.T) {
	n := NewNode(1, []int{0, 2}, silentPeers(), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.becomeCandidate()
	n.becomeCandidate()
	n.becomeCandidate()
	n.mu.Unlock()

	state, term, votedFor := n.snapshotState()

	if state != Candidate {
		t.Errorf("state: want candidate, got %v", state)
	}
	if term != 3 {
		t.Errorf("term: want 3 after three elections, got %d", term)
	}
	if votedFor != n.id {
		t.Errorf("votedFor: want self (%d), got %d", n.id, votedFor)
	}
}

// Terms must never move backwards.
func TestTermIsMonotonic(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)
	t.Cleanup(n.Stop)

	prev := 0
	for i := 0; i < 50; i++ {
		n.mu.Lock()
		n.becomeCandidate()
		got := n.currentTerm
		n.mu.Unlock()

		if got <= prev {
			t.Fatalf("term did not increase: %d -> %d", prev, got)
		}
		prev = got
	}
}

// The self-vote is a real vote, which is why cluster sizes work out.
func TestMajority(t *testing.T) {
	cases := []struct {
		peers int
		want  int
	}{
		{0, 1}, // single node: elects itself, sends nothing
		{2, 2}, // 3-node cluster
		{4, 3}, // 5-node cluster
		{6, 4}, // 7-node cluster
	}

	for _, c := range cases {
		peers := make([]int, c.peers)
		for i := range peers {
			peers[i] = i + 1
		}
		n := NewNode(0, peers, silentPeers(), 1)
		t.Cleanup(n.Stop)

		if got := n.majority(); got != c.want {
			t.Errorf("cluster of %d: majority = %d, want %d", c.peers+1, got, c.want)
		}
	}
}

// End to end through the ticker: an idle node campaigns on its own.
func TestIdleFollowerBecomesCandidate(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)
	n.Start()
	defer n.Stop()

	time.Sleep(electionTimeoutMax + 3*tickInterval)

	state, term, votedFor := n.snapshotState()

	if state != Candidate {
		t.Errorf("idle follower should have campaigned, state is %v", state)
	}
	if term < 1 {
		t.Errorf("term should have advanced, got %d", term)
	}
	if votedFor != n.id {
		t.Errorf("votedFor: want self (%d), got %d", n.id, votedFor)
	}
}

// --- B-4: timer behaviour, still required ---------------------------------

func TestElectionTimerDoesNotFireEarly(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)
	n.Start()
	defer n.Stop()

	time.Sleep(electionTimeoutMin - 4*tickInterval)

	if state, term, _ := n.snapshotState(); state != Follower || term != 0 {
		t.Fatalf("campaigned before the minimum timeout: state=%v term=%d", state, term)
	}
}

// A steady stream of valid RPCs holds the election off indefinitely.
func TestElectionTimerResetPreventsCandidacy(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)
	n.Start()
	defer n.Stop()

	deadline := time.Now().Add(2 * electionTimeoutMax)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		n.resetElectionTimer()
		n.mu.Unlock()
		time.Sleep(heartbeatInterval)
	}

	if state, term, _ := n.snapshotState(); state != Follower || term != 0 {
		t.Fatalf("campaigned despite regular resets: state=%v term=%d", state, term)
	}
}

// A leader never times itself out.
func TestLeaderDoesNotTimeOut(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 5
	n.mu.Unlock()

	n.Start()
	defer n.Stop()

	time.Sleep(electionTimeoutMax + 3*tickInterval)

	if state, term, _ := n.snapshotState(); state != Leader || term != 5 {
		t.Errorf("leader deposed itself: state=%v term=%d", state, term)
	}
}

func TestElectionTimeoutIsRandomised(t *testing.T) {
	n := NewNode(0, nil, silentPeers(), 1)
	t.Cleanup(n.Stop)

	seen := make(map[time.Duration]bool)
	n.mu.Lock()
	for i := 0; i < 200; i++ {
		d := n.randomElectionTimeout()
		if d < electionTimeoutMin || d >= electionTimeoutMax {
			n.mu.Unlock()
			t.Fatalf("timeout %v outside [%v, %v)", d, electionTimeoutMin, electionTimeoutMax)
		}
		seen[d] = true
	}
	n.mu.Unlock()

	if len(seen) < 100 {
		t.Errorf("timeouts poorly distributed: only %d distinct values in 200 draws", len(seen))
	}
}

// Same seed, same timeout sequence (task G-2).
func TestElectionTimeoutIsDeterministic(t *testing.T) {
	draw := func(seed int64) []time.Duration {
		n := NewNode(0, nil, silentPeers(), seed)
		t.Cleanup(n.Stop)
		var out []time.Duration
		n.mu.Lock()
		for i := 0; i < 20; i++ {
			out = append(out, n.randomElectionTimeout())
		}
		n.mu.Unlock()
		return out
	}

	a, b := draw(7), draw(7)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed diverged at draw %d: %v vs %v", i, a[i], b[i])
		}
	}

	c := draw(8)
	if a[0] == c[0] && a[1] == c[1] {
		t.Error("different seeds produced the same timeout sequence")
	}
}

func TestStopHaltsTimer(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.Start()

	time.Sleep(electionTimeoutMax + 3*tickInterval)
	n.Stop()

	_, before, _ := n.snapshotState()
	if before == 0 {
		t.Fatal("node never campaigned before Stop")
	}

	time.Sleep(electionTimeoutMax + 3*tickInterval)

	if _, after, _ := n.snapshotState(); after != before {
		t.Errorf("timer still firing after Stop: term %d -> %d", before, after)
	}
}
