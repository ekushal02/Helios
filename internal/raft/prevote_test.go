package raft

import (
	"testing"
	"time"
)

// =============================================================================
// The receiver, in isolation
// =============================================================================

func preVoteNode(t *testing.T, term, entries int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm = term
	for i := 1; i <= entries; i++ {
		n.log = append(n.log, LogEntry{Term: term, Command: []byte{byte(i)}})
	}
	return n
}

// THE PROPERTY THE WHOLE MECHANISM RESTS ON. Answering a poll must leave this
// node exactly as it was, however high the proposed term.
func TestAPreVoteChangesNothingAboutTheReceiver(t *testing.T) {
	n := preVoteNode(t, 4, 3)

	n.mu.Lock()
	n.votedFor = 2
	n.leaderID = 1
	before := struct {
		term, vote, leader int
		deadline           time.Time
	}{n.currentTerm, n.votedFor, n.leaderID, n.electionDeadline}
	n.mu.Unlock()

	var reply PreVoteReply
	n.PreVote(&PreVoteArgs{
		Term: 900, CandidateID: 1, LastLogIndex: 99, LastLogTerm: 99,
	}, &reply)

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.currentTerm != before.term {
		t.Errorf("currentTerm moved to %d on a poll proposing 900: this is the "+
			"term inflation pre-vote exists to prevent", n.currentTerm)
	}
	if n.votedFor != before.vote {
		t.Errorf("votedFor changed to %d: a pre-vote is not a vote", n.votedFor)
	}
	if n.leaderID != before.leader {
		t.Errorf("leaderID changed to %d", n.leaderID)
	}
	if !n.electionDeadline.Equal(before.deadline) {
		t.Error("the election timer was reset: a poll is not contact from a leader, " +
			"and treating it as one lets a campaigning node hold off an election " +
			"it is not going to win")
	}
	if reply.Term != before.term {
		t.Errorf("reply.Term = %d, want %d", reply.Term, before.term)
	}
}

// §5.4.1 applies to the hypothetical exactly as it would to the real thing.
func TestAPreVoteIsRefusedToABehindLog(t *testing.T) {
	n := preVoteNode(t, 4, 6)

	n.mu.Lock()
	n.leaderID = None // no leader believed in, so stickiness is not what refuses
	n.mu.Unlock()

	var reply PreVoteReply
	n.PreVote(&PreVoteArgs{
		Term: 5, CandidateID: 1, LastLogIndex: 3, LastLogTerm: 4,
	}, &reply)

	if reply.VoteGranted {
		t.Error("granted a poll to a candidate three entries behind: it would " +
			"lose the real vote, so the poll must say so")
	}

	// The positive control, or the test above passes against a handler that
	// refuses everything.
	n.PreVote(&PreVoteArgs{
		Term: 5, CandidateID: 1, LastLogIndex: 6, LastLogTerm: 4,
	}, &reply)

	if !reply.VoteGranted {
		t.Error("refused a poll from a candidate with an equally current log")
	}
}

// §6's third issue. A node that has heard from a leader will not help depose it,
// however good the caller's log is.
func TestAPreVoteIsRefusedWhileALeaderIsBelievedIn(t *testing.T) {
	n := preVoteNode(t, 4, 3)

	n.mu.Lock()
	n.leaderID = 1
	n.lastLeaderContact = time.Now()
	n.mu.Unlock()

	var reply PreVoteReply
	n.PreVote(&PreVoteArgs{
		Term: 5, CandidateID: 2, LastLogIndex: 3, LastLogTerm: 4,
	}, &reply)

	if reply.VoteGranted {
		t.Error("granted a poll while a leader was believed in: a removed or " +
			"briefly partitioned server with a current log can still depose a " +
			"healthy leader")
	}

	// Once the leader has gone quiet for a full minimum timeout, the same
	// caller is answered. Without this the cluster could never elect again.
	n.mu.Lock()
	n.lastLeaderContact = time.Now().Add(-2 * electionTimeoutMin)
	n.mu.Unlock()

	n.PreVote(&PreVoteArgs{
		Term: 5, CandidateID: 2, LastLogIndex: 3, LastLogTerm: 4,
	}, &reply)

	if !reply.VoteGranted {
		t.Error("still refusing after the leader went silent for two full " +
			"timeouts: stickiness has become a liveness bug")
	}
}

func TestAPreVoteAtAnOldTermIsRefused(t *testing.T) {
	n := preVoteNode(t, 9, 2)

	n.mu.Lock()
	n.leaderID = None
	n.mu.Unlock()

	var reply PreVoteReply
	n.PreVote(&PreVoteArgs{
		Term: 9, CandidateID: 1, LastLogIndex: 50, LastLogTerm: 50,
	}, &reply)

	if reply.VoteGranted {
		t.Error("granted a poll proposing a term this node already holds")
	}
	if reply.Term != 9 {
		t.Errorf("reply.Term = %d, want 9: the caller has to learn it is behind", reply.Term)
	}
}

// =============================================================================
// The disruption, with and without
// =============================================================================

// partitionRound cuts one follower off, lets it sulk, and reports what it did
// to its own term and what healing did to the cluster.
type partitionResult struct {
	leaderBefore, termBefore int
	victimTermAfter          int
	leaderAfter, termAfter   int
	maxGap                   time.Duration
	applied, refused         int
}

func runPartitionRound(t *testing.T, seed int64, preVote bool, sulk time.Duration) partitionResult {
	t.Helper()

	var r partitionResult

	c := newCluster(t, 3, seed)
	c.net.setDelayRange(time.Millisecond, 3*time.Millisecond)

	if !preVote {
		for _, n := range c.nodes {
			n.mu.Lock()
			n.noPreVote = true
			n.mu.Unlock()
		}
	}

	tracker := newWriteTracker()
	c.watchApplies(tracker.record)
	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}
	leaderNode := c.nodes[leader]

	c.nodes[leader].mu.Lock()
	r.leaderBefore, r.termBefore = leader, c.nodes[leader].currentTerm
	c.nodes[leader].mu.Unlock()

	tracker.watch(leader)

	stop := make(chan struct{})
	done := make(chan struct{})
	gen := &loadGenerator{}
	go gen.run(leaderNode, tracker, 2*time.Millisecond, stop, done)

	// The victim is a follower, so the majority carries on without it and this
	// measures disruption on return rather than an ordinary failover.
	victim := c.othersThan(leader)[0]
	c.net.disconnect(victim)

	time.Sleep(sulk)

	c.nodes[victim].mu.Lock()
	r.victimTermAfter = c.nodes[victim].currentTerm
	c.nodes[victim].mu.Unlock()

	c.net.reconnect(victim)

	// Long enough for an inflated term to propagate and for any election it
	// provokes to finish.
	time.Sleep(1500 * time.Millisecond)

	close(stop)
	<-done

	after := c.waitForStableCluster(5 * time.Second)
	if after == None {
		t.Fatalf("no leader after healing: %s", c.describe())
	}
	c.nodes[after].mu.Lock()
	r.leaderAfter, r.termAfter = after, c.nodes[after].currentTerm
	c.nodes[after].mu.Unlock()

	applied, _, _, _, _, maxGap, _ := tracker.stats()
	r.applied, r.maxGap, r.refused = applied, maxGap, gen.refused

	return r
}

// THE CONTROL. Without pre-voting, the partitioned node's term climbs for as
// long as the partition lasts, and healing costs the cluster its leader.
//
// This is not a test of a bug -- it is the behaviour Raft as written in Figure 2
// has, and it is safe. It is here so that the test below is measuring something
// rather than asserting that nothing happened.
func TestWithoutPreVoteAPartitionedNodeInflatesItsTermAndDeposesTheLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster with a one-second partition")
	}

	r := runPartitionRound(t, 20260909, false, time.Second)

	t.Logf("no pre-vote: leader %d term %d -> leader %d term %d",
		r.leaderBefore, r.termBefore, r.leaderAfter, r.termAfter)
	t.Logf("  victim reached term %d while partitioned (started at %d)",
		r.victimTermAfter, r.termBefore)
	t.Logf("  writes %d applied, %d refused, worst commit gap %v",
		r.applied, r.refused, round(r.maxGap))

	if r.victimTermAfter <= r.termBefore+1 {
		t.Errorf("the partitioned node reached only term %d from %d: it barely "+
			"campaigned, so this round did not set up the disruption it is "+
			"supposed to demonstrate", r.victimTermAfter, r.termBefore)
	}
	if r.termAfter <= r.termBefore {
		t.Errorf("the cluster is still at term %d after a node returned at term "+
			"%d: the inflated term did not propagate, and the comparison below "+
			"has nothing to compare against", r.termAfter, r.victimTermAfter)
	}
}

// THE POINT. With pre-voting the same partition costs the cluster nothing: the
// victim never increments, so there is no inflated term to return with, and the
// leader is undisturbed.
func TestPreVoteStopsAReturningNodeFromDisruptingTheCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster with a one-second partition")
	}

	r := runPartitionRound(t, 20260909, true, time.Second)

	t.Logf("pre-vote: leader %d term %d -> leader %d term %d",
		r.leaderBefore, r.termBefore, r.leaderAfter, r.termAfter)
	t.Logf("  victim held term %d throughout the partition", r.victimTermAfter)
	t.Logf("  writes %d applied, %d refused, worst commit gap %v",
		r.applied, r.refused, round(r.maxGap))

	// It never spent a term finding out nobody was listening.
	if r.victimTermAfter != r.termBefore {
		t.Errorf("the partitioned node moved from term %d to %d: it incremented "+
			"without winning a poll, which is the whole thing pre-vote prevents",
			r.termBefore, r.victimTermAfter)
	}

	// So the cluster never noticed it come back.
	if r.leaderAfter != r.leaderBefore || r.termAfter != r.termBefore {
		t.Errorf("leadership went from %d@%d to %d@%d: a node with nothing to "+
			"offer still cost the cluster an election",
			r.leaderBefore, r.termBefore, r.leaderAfter, r.termAfter)
	}
	if r.refused != 0 {
		t.Errorf("%d writes were refused: the leader stopped leading part-way",
			r.refused)
	}

	// ANTI-VACUITY. A cluster that committed nothing, or a victim that never
	// tried, would satisfy every assertion above.
	if r.applied < 100 {
		t.Errorf("only %d writes applied: the cluster was not busy enough for a "+
			"disruption to have shown up", r.applied)
	}
}

// The victim must actually be polling throughout the partition, or the quiet
// above proves only that it gave up.
func TestAPartitionedNodeKeepsPollingWithoutSpendingTerms(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster with a partition")
	}

	c := newCluster(t, 3, 20260910)
	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}
	victim := c.othersThan(leader)[0]

	c.nodes[victim].mu.Lock()
	termBefore := c.nodes[victim].currentTerm
	c.nodes[victim].mu.Unlock()

	c.net.disconnect(victim)
	before := c.net.preVotes()

	time.Sleep(time.Second)

	c.nodes[victim].mu.Lock()
	termAfter := c.nodes[victim].currentTerm
	c.nodes[victim].mu.Unlock()

	// Polls to a disconnected peer are ROUTED but never delivered, so the
	// counter is read at the leader's side of the cluster: what matters here is
	// that the victim kept trying and kept failing.
	t.Logf("victim %d: term %d -> %d over a second of partition, %d polls "+
		"delivered cluster-wide", victim, termBefore, termAfter, c.net.preVotes()-before)

	if termAfter != termBefore {
		t.Errorf("term moved from %d to %d while partitioned", termBefore, termAfter)
	}

	// And it recovers: once healed it must be able to campaign again if the
	// leader really does fail.
	c.net.reconnect(victim)
	c.kill(leader)

	if c.waitForStableCluster(10*time.Second) == None {
		t.Fatalf("no leader after the real one was killed: pre-voting has "+
			"stopped elections from happening at all: %s", c.describe())
	}
}
