package raft

import (
	"testing"
	"time"
)

// assertTimerWasReset checks the deadline sits in a plausible future window.
func assertTimerWasReset(t *testing.T, n *Node, context string) {
	t.Helper()

	n.mu.Lock()
	remaining := time.Until(n.electionDeadline)
	n.mu.Unlock()

	if remaining <= 0 {
		t.Errorf("%s: election timer was not reset (remaining=%v)", context, remaining)
	}
	if remaining > electionTimeoutMax {
		t.Errorf("%s: deadline %v is beyond the maximum timeout %v",
			context, remaining, electionTimeoutMax)
	}
}

// THE HEADLINE TEST FOR B-10.
//
// Once a leader emerges it must STAY leader. Before heartbeats existed, every
// follower timed out within 300ms and deposed it. Three full election timeouts
// of stability is proof the heartbeats are landing.
func TestHeartbeatsKeepLeadershipStable(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.start()

	leaderID := c.waitForSingleLeader(2 * time.Second)
	if leaderID == None {
		t.Fatalf("no leader within 2s (seed %d)", c.seed)
	}
	leader := c.nodes[leaderID]
	_, termAtWin, _ := leader.snapshotState()

	time.Sleep(3 * electionTimeoutMax)

	state, term, _ := leader.snapshotState()
	if state != Leader {
		t.Errorf("leader was deposed while healthy: state=%v", state)
	}
	if term != termAtWin {
		t.Errorf("term advanced from %d to %d: an election happened despite "+
			"heartbeats", termAtWin, term)
	}

	// And nobody else started campaigning.
	for _, n := range c.nodes {
		if n.id == leader.id {
			continue
		}
		if s, _, _ := n.snapshotState(); s != Follower {
			t.Errorf("node %d is %v, want follower under a healthy leader", n.id, s)
		}
	}
}

// The mirror image: silence the leader and followers must time out.
func TestFollowersTimeOutWhenHeartbeatsStop(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.start()

	leaderID := c.waitForSingleLeader(2 * time.Second)
	if leaderID == None {
		t.Fatalf("no leader within 2s (seed %d)", c.seed)
	}
	leader := c.nodes[leaderID]
	_, oldTerm, _ := leader.snapshotState()

	// Cut the leader off. Its heartbeats stop reaching anyone.
	c.net.disconnect(leader.id)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for term, ids := range c.leadersByTerm() {
			if term > oldTerm && ids[0] != leader.id {
				return // a new leader emerged in a later term, as it should
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no new leader after the old one was partitioned (seed %d)", c.seed)
}

// A heartbeat must push the election deadline into the future.
func TestHeartbeatResetsElectionTimer(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.currentTerm = 3
	n.electionDeadline = time.Now() // expiring right now
	n.mu.Unlock()

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{Term: 3, LeaderID: 1}, &reply)

	if !reply.Success {
		t.Fatal("a current-term heartbeat should be accepted")
	}
	assertTimerWasReset(t, n, "heartbeat")

	n.mu.Lock()
	gotLeader := n.leaderID
	n.mu.Unlock()
	if gotLeader != 1 {
		t.Errorf("leaderID = %d, want 1", gotLeader)
	}
}

// A heartbeat from a term this node has left is refused, and does NOT reset the timer.
func TestStaleHeartbeatIsRejected(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.currentTerm = 7
	n.resetElectionTimer()
	before := n.electionDeadline
	n.mu.Unlock()

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{Term: 3, LeaderID: 1}, &reply)

	n.mu.Lock()
	after := n.electionDeadline
	n.mu.Unlock()

	if reply.Success {
		t.Error("accepted a heartbeat from a stale term")
	}
	if reply.Term != 7 {
		t.Errorf("reply.Term = %d, want 7 so the stale leader steps down", reply.Term)
	}
	// Equality is safe here: asserting the deadline is UNCHANGED needs no
	// comparison between two random draws.
	if !after.Equal(before) {
		t.Error("a stale heartbeat reset the election timer")
	}
}

// THE B-9 PAYOFF.
//
// A candidate that loses to a rival IN THE SAME TERM steps down when the rival's
// heartbeat arrives -- and must keep its vote. Its vote for itself in this term
// is already spent; forgetting it would allow a second vote in one term.
func TestCandidateStepsDownOnSameTermHeartbeat(t *testing.T) {
	n := NewNode(0, []int{1, 2}, silentPeers(), 1)

	n.mu.Lock()
	n.state = Candidate
	n.currentTerm = 5
	n.votedFor = n.id // voted for itself in term 5
	n.mu.Unlock()

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{Term: 5, LeaderID: 1}, &reply)

	state, term, votedFor := n.snapshotState()

	if state != Follower {
		t.Errorf("state = %v, want follower after losing the term", state)
	}
	if term != 5 {
		t.Errorf("currentTerm = %d, want 5 (unchanged)", term)
	}
	if votedFor != n.id {
		t.Errorf("votedFor = %d, want %d: the vote in this term is spent", votedFor, n.id)
	}

	// And the consequence: no second vote in term 5.
	var vote RequestVoteReply
	n.RequestVote(&RequestVoteArgs{Term: 5, CandidateID: 2}, &vote)
	if vote.VoteGranted {
		t.Error("granted a second vote in term 5")
	}
}

// A leader alone in a minority learns it has been replaced from a heartbeat REPLY, not from anyone contacting it.
func TestLeaderStepsDownFromHeartbeatReply(t *testing.T) {
	stub := newStubTransport(denyAll(1))

	stub.appendAnswer = func(int, *AppendEntriesArgs) (AppendEntriesReply, bool) {
		return AppendEntriesReply{Term: 50, Success: false}, true
	}

	n := NewNode(0, []int{1, 2}, stub, 1)

	n.mu.Lock()
	n.state = Candidate // becomeLeader is guarded to Candidate
	n.currentTerm = 3
	n.becomeLeader()
	n.mu.Unlock()
	defer n.Stop()

	waitForState(t, n, Follower, 500*time.Millisecond)

	if _, term, _ := n.snapshotState(); term != 50 {
		t.Errorf("currentTerm = %d, want 50", term)
	}
}

// The first heartbeat goes out immediately, not one interval later.
func TestFirstHeartbeatIsImmediate(t *testing.T) {
	sent := make(chan struct{}, 8)

	stub := newStubTransport(denyAll(1))
	stub.appendAnswer = func(int, *AppendEntriesArgs) (AppendEntriesReply, bool) {
		select {
		case sent <- struct{}{}:
		default:
		}
		return AppendEntriesReply{Term: 3, Success: true}, true
	}

	n := NewNode(0, []int{1, 2}, stub, 1)

	n.mu.Lock()
	n.state = Candidate
	n.currentTerm = 3
	n.becomeLeader()
	n.mu.Unlock()
	defer n.Stop()

	select {
	case <-sent:
	case <-time.After(heartbeatInterval / 2):
		t.Error("no heartbeat within half an interval of winning")
	}
}
