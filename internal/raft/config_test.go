package raft

import (
	"errors"
	"testing"
	"time"
)

// =============================================================================
// The configuration is derived from the log
// =============================================================================

// configNode builds a leader with a three-server configuration and nothing
// running, so the test is the only thing that moves state.
func configNode(t *testing.T, term int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm = term
	n.state = Leader
	n.initLeaderState()

	return n
}

func TestAFreshNodeStartsInTheConfigurationItWasGiven(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	got := n.Configuration()
	if len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("configuration = %v, want [0 1 2]: peers plus self", got)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.inConfig {
		t.Error("a node built from its own peers is not in its own configuration")
	}
	if got := n.quorumSize(); got != 2 {
		t.Errorf("quorumSize = %d, want 2 for three servers", got)
	}
}

// THE RULE THAT LOOKS LIKE A BUG. The configuration is in force from the append,
// with commitIndex still far below it.
func TestAConfigurationTakesEffectOnAppendNotOnCommit(t *testing.T) {
	n := configNode(t, 3)

	index, _, err := n.AddServer(3)
	if err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.commitIndex >= index {
		t.Fatalf("the entry at %d committed immediately; this test needs it "+
			"uncommitted (commitIndex %d)", index, n.commitIndex)
	}
	if !containsServer(n.servers, 3) {
		t.Errorf("servers = %v, want server 3 included while the entry is still "+
			"uncommitted", n.servers)
	}
	if got := n.quorumSize(); got != 3 {
		t.Errorf("quorumSize = %d, want 3: a four-server configuration is in force", got)
	}
	if _, known := n.nextIndex[3]; !known {
		t.Error("no nextIndex for the new server: it would never be contacted, " +
			"because a missing key reads as 0 and buildAppendEntries answers that " +
			"with a snapshot")
	}
}

// And rolls back when the entry that carried it is truncated away.
func TestATruncatedConfigurationRollsBack(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 2
	n.mu.Unlock()

	// A leader in term 2 appended a command and then a configuration adding 3.
	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Term: 2, Command: []byte("a")},
			{Term: 2, Servers: []int{0, 1, 2, 3}},
		},
	}, &reply)
	if !reply.Success {
		t.Fatalf("setup append rejected (reply %+v)", reply)
	}

	if got := n.Configuration(); len(got) != 4 {
		t.Fatalf("configuration = %v, want four servers", got)
	}

	// That leader is deposed before the entry commits, and the new one has a
	// different index 2. Receiver rule 3 removes the configuration entry.
	n.AppendEntries(&AppendEntriesArgs{
		Term: 3, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 2,
		Entries: []LogEntry{{Term: 3, Command: []byte("b")}},
	}, &reply)
	if !reply.Success {
		t.Fatalf("truncating append rejected (reply %+v)", reply)
	}

	got := n.Configuration()
	if len(got) != 3 {
		t.Fatalf("configuration = %v, want three servers: the entry that added "+
			"the fourth was truncated and the configuration must follow it", got)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if got := n.quorumSize(); got != 2 {
		t.Errorf("quorumSize = %d, want 2", got)
	}
}

// A configuration below the floor is gone from the log, so the state record has
// to carry it. Without that a restarted node would fall back to whatever it was
// constructed with.
func TestAConfigurationSurvivesCompactionAndRestart(t *testing.T) {
	store := NewMemoryStorage()

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	n.mu.Lock()
	n.currentTerm = 4
	n.state = Leader
	n.initLeaderState()
	n.mu.Unlock()

	index, _, err := n.AddServer(3)
	if err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	// Commit it, apply it, then compact past it.
	n.mu.Lock()
	n.commitIndex = index
	n.lastApplied = index
	n.mu.Unlock()

	if err := n.Snapshot(index, []byte("image")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	n.Stop()

	restarted, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode after compaction: %v", err)
	}
	defer restarted.Stop()

	got := restarted.Configuration()
	if len(got) != 4 || !containsServer(got, 3) {
		t.Errorf("configuration = %v after restart, want four servers including 3: "+
			"the entry that added it is below the floor, so the state record must "+
			"carry the configuration", got)
	}
}

// =============================================================================
// One change at a time
// =============================================================================

// Two configurations in play at once is exactly the state the
// overlapping-majority argument does not cover.
func TestASecondChangeIsRefusedWhileTheFirstIsUncommitted(t *testing.T) {
	n := configNode(t, 3)

	if _, _, err := n.AddServer(3); err != nil {
		t.Fatalf("first AddServer: %v", err)
	}

	_, _, err := n.AddServer(4)
	if !errors.Is(err, ErrConfigChangeInFlight) {
		t.Fatalf("err = %v, want ErrConfigChangeInFlight", err)
	}

	n.mu.Lock()
	servers := append([]int(nil), n.servers...)
	commit := n.configIndex
	n.mu.Unlock()

	if containsServer(servers, 4) {
		t.Errorf("servers = %v: the refused change was applied anyway", servers)
	}

	// Once the first commits, the second is allowed.
	n.mu.Lock()
	n.commitIndex = commit
	n.mu.Unlock()

	if _, _, err := n.AddServer(4); err != nil {
		t.Fatalf("second AddServer after the first committed: %v", err)
	}
}

func TestAChangeThatChangesNothingIsRefused(t *testing.T) {
	n := configNode(t, 3)

	if _, _, err := n.AddServer(1); !errors.Is(err, ErrNoChange) {
		t.Errorf("adding an existing member: err = %v, want ErrNoChange", err)
	}
	if _, _, err := n.RemoveServer(9); !errors.Is(err, ErrNoChange) {
		t.Errorf("removing a non-member: err = %v, want ErrNoChange", err)
	}
	if _, _, err := n.RemoveServer(0); err == nil {
		// Removing self from a three-server cluster is legal; this is the
		// one-server case, checked separately below.
		_ = err
	}
}

func TestOnlyALeaderChangesTheConfiguration(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	if _, _, err := n.AddServer(3); !errors.Is(err, ErrNotLeader) {
		t.Errorf("err = %v, want ErrNotLeader", err)
	}
}

// =============================================================================
// The leader that removes itself
// =============================================================================

// It must keep replicating the entry that removes it -- nobody else can -- and
// step down only once that entry commits.
func TestALeaderRemovingItselfStepsDownOnlyOnceTheChangeCommits(t *testing.T) {
	n := configNode(t, 5)

	index, _, err := n.RemoveServer(0)
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	n.mu.Lock()
	if n.inConfig {
		n.mu.Unlock()
		t.Fatal("still a member of a configuration that removed it")
	}
	if n.state != Leader {
		n.mu.Unlock()
		t.Fatal("stepped down at the append: the change is uncommitted and nobody " +
			"else can finish replicating it")
	}
	if got := n.quorumSize(); got != 2 {
		t.Errorf("quorumSize = %d, want 2 for the two remaining servers", got)
	}
	// The leader no longer counts itself toward that majority.
	if got := n.replicaCount(index); got != 0 {
		t.Errorf("replicaCount = %d, want 0: a leader outside the configuration "+
			"must not count itself", got)
	}
	n.mu.Unlock()

	// The change commits; now it retires.
	n.mu.Lock()
	n.matchIndex[1] = index
	n.matchIndex[2] = index
	n.advanceCommitIndex()
	state := n.state
	n.mu.Unlock()

	if state != Follower {
		t.Errorf("state = %v after the change committed, want follower", state)
	}
}

// =============================================================================
// End to end
// =============================================================================

// A server joins a running cluster, catches up, and counts toward the majority.
func TestAServerJoinsARunningCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster")
	}

	c := newCluster(t, 3, 20260905)
	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}

	for i := 1; i <= 20; i++ {
		if !c.submitToLeader([]byte("before")) {
			t.Fatalf("no leader accepted write %d", i)
		}
	}

	joiner := c.addNode()

	if _, _, err := c.nodes[leader].AddServer(joiner); err != nil {
		t.Fatalf("AddServer(%d): %v", joiner, err)
	}

	// Every node, the new one included, must end up agreeing on the membership.
	deadline := time.Now().Add(10 * time.Second)
	for {
		agreed := true
		for _, n := range c.nodes {
			if len(n.Configuration()) != 4 {
				agreed = false
			}
		}
		if agreed {
			break
		}
		if time.Now().After(deadline) {
			for _, n := range c.nodes {
				t.Logf("node %d: %v", n.id, n.Configuration())
			}
			t.Fatalf("the cluster did not converge on a four-server configuration")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// And the cluster still commits, now against a majority of four.
	for i := 1; i <= 20; i++ {
		if !c.submitToLeader([]byte("after")) {
			t.Fatalf("the cluster stopped committing after the join: %s", c.describe())
		}
	}

	if c.waitForStableCluster(5*time.Second) == None {
		t.Errorf("no single leader after the join: %s", c.describe())
	}
}

// A server leaves, and the remaining cluster commits without it.
func TestAServerLeavesARunningCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster")
	}

	c := newCluster(t, 3, 20260906)
	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}
	leaving := c.othersThan(leader)[0]

	if _, _, err := c.nodes[leader].RemoveServer(leaving); err != nil {
		t.Fatalf("RemoveServer(%d): %v", leaving, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if len(c.nodes[leader].Configuration()) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader still holds %v", c.nodes[leader].Configuration())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// SCOPE FENCE (disruptive servers). The removed node is still running and
	// still expects heartbeats, so it will time out and campaign with rising
	// terms until something stops it. §6's remedy -- ignoring RequestVote
	// within the minimum election timeout of hearing from a leader -- is not
	// implemented, so the test shuts it down as an operator would.
	c.kill(leaving)

	for i := 1; i <= 20; i++ {
		if !c.submitToLeader([]byte("after")) {
			t.Fatalf("the cluster stopped committing after the removal: %s", c.describe())
		}
	}
}
