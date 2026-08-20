package raft

import (
	"errors"
	"testing"
	"time"
)

// =============================================================================
// The read path
// =============================================================================
//
// Depends on leaderfailure_test.go (C-14) for failoverCluster, and on
// e2e_test.go for kvMachine, encodePut and waitForLeader.

const readBound = 5 * time.Second

var (
	errNotLeader = errors.New("not the leader")
	errDeposed   = errors.New("deposed mid-read")
	errTooSlow   = errors.New("barrier never applied")
)

// readThrough is the whole client-side protocol, in one place, so that the
// tests below exercise the protocol rather than each re-implementing it.
//
// Note what it does NOT do: retry. A deposed read is reported, not repeated.
// Retrying a read is harmless in a way retrying a write is not, but it belongs
// to the caller, and burying it here would let a test pass while the barrier
// silently failed several times.
func readThrough(n *Node, m *kvMachine, key string, within time.Duration) (string, bool, error) {
	idx, term, isLeader := n.ReadIndex()
	if !isLeader {
		return "", false, errNotLeader
	}

	// COMMITTED AND APPLIED, not merely committed. Between those two the node
	// holds a write it has acknowledged and not yet run.
	if !m.waitForIndex(idx, within) {
		return "", false, errTooSlow
	}

	// The claim ticket. A different term at the barrier's index means a later
	// leader overwrote the slot, so this node was not leading when the read
	// would have been served.
	if got, ok := m.termAt(idx); !ok || got != term {
		return "", false, errDeposed
	}

	v, ok := m.value(key)
	return v, ok, nil
}

// =============================================================================
// The barrier itself
// =============================================================================

func TestReadIndexRejectedByNonLeader(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(3)), 1)
	defer n.Stop()

	n.mu.Lock()
	n.currentTerm = 3
	before := len(n.log)
	n.mu.Unlock()

	index, term, isLeader := n.ReadIndex()

	if isLeader {
		t.Fatal("a follower accepted a read barrier")
	}
	if index != 0 {
		t.Errorf("index = %d, want 0 on rejection", index)
	}
	if term != 3 {
		t.Errorf("term = %d, want 3 so the caller can reason about staleness", term)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != before {
		t.Errorf("follower log grew from %d to %d: a read must not write to a "+
			"log this node does not lead", before, len(n.log))
	}
}

func TestReadIndexAppendsABarrierInTheCurrentTerm(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	index, term, isLeader := n.ReadIndex()

	if !isLeader {
		t.Fatal("leader rejected a read barrier")
	}
	if index != 1 {
		t.Errorf("index = %d, want 1 (first real index; 0 is the sentinel)", index)
	}
	if term != 5 {
		t.Errorf("term = %d, want 5", term)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	entry := n.log[1]
	if !entry.NoOp {
		t.Error("the barrier was appended as a client command: the state " +
			"machine will try to decode it")
	}
	if entry.Command != nil {
		t.Errorf("barrier carries a command %q, want none", entry.Command)
	}
	if entry.Term != 5 {
		t.Errorf("barrier term = %d, want 5: a barrier committed under an old "+
			"term proves nothing about who leads now", entry.Term)
	}
}

// Reads and writes share one index space, because they share one log. A reader
// that assumed barriers were invisible would compute the wrong index for
// everything after it.
func TestBarriersAndCommandsShareTheIndexSpace(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	var got []int
	for i := 0; i < 3; i++ {
		idx, _, ok := n.Submit([]byte{byte(i)})
		if !ok {
			t.Fatal("leader stopped accepting commands")
		}
		got = append(got, idx)

		idx, _, ok = n.ReadIndex()
		if !ok {
			t.Fatal("leader stopped accepting barriers")
		}
		got = append(got, idx)
	}

	for i, idx := range got {
		if idx != i+1 {
			t.Fatalf("indices %v, want 1..6 consecutive: barriers and commands "+
				"are allocated from the same counter", got)
		}
	}
}

// A barrier must be DELIVERED, with CommandValid false. Filtering it out inside
// the applier would advance lastApplied while leaving the state machine's own
// counter behind, and every read would then wait for an index that never comes.
func TestABarrierIsDeliveredWithCommandValidFalse(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(4)), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 4
	n.log = append(n.log,
		LogEntry{Term: 4, Command: []byte("real")},
		LogEntry{Term: 4, NoOp: true},
	)
	n.mu.Unlock()

	commit(n, 2)

	first := mustApply(t, n)
	if !first.CommandValid {
		t.Error("a client command arrived with CommandValid false")
	}

	second := mustApply(t, n)
	if second.CommandIndex != 2 {
		t.Fatalf("second delivery was index %d, want 2", second.CommandIndex)
	}
	if second.CommandValid {
		t.Error("the barrier arrived with CommandValid true: a state machine " +
			"would try to decode an empty command")
	}
	if second.Command != nil {
		t.Errorf("barrier delivered a command %q, want none", second.Command)
	}
	if second.CommandTerm != 4 {
		t.Errorf("barrier term = %d, want 4: the term is what the reader "+
			"checks to know it was not deposed", second.CommandTerm)
	}
}

// =============================================================================
// The property
// =============================================================================

// READ YOUR WRITES, through the whole stack.
func TestAReadAfterAWriteSeesTheWrite(t *testing.T) {
	c, machines := failoverCluster(t, 3, 71)

	leader := c.waitForStableCluster(readBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}

	for i, val := range []string{"one", "two", "three"} {
		idx, _, isLeader := c.nodes[leader].Submit(encodePut("k", val))
		if !isLeader {
			t.Fatalf("write %d: node %d stopped leading", i, leader)
		}
		if !machines[leader].waitForIndex(idx, readBound) {
			t.Fatalf("write %d never applied", i)
		}

		got, ok, err := readThrough(c.nodes[leader], machines[leader], "k", readBound)
		if err != nil {
			t.Fatalf("read after write %d: %v", i, err)
		}
		if !ok || got != val {
			t.Errorf("read after write %d returned %q, want %q", i, got, val)
		}
	}
}

// THE HEADLINE TEST, and the one that justifies the whole file.
//
// A leader partitioned into a minority still believes it leads and still holds
// a state machine full of confident, stale data. The test proves both -- that a
// local read WOULD have been served and WOULD have been wrong -- and then shows
// the barrier refusing to complete rather than completing incorrectly.
//
// This is the failure mode that has no local remedy. There is no field to check
// and no timeout to tune, because every local fact is a statement about the
// past.
func TestAStaleLeaderCannotServeAReadItsLocalStateWouldAnswer(t *testing.T) {
	c, machines := failoverCluster(t, 5, 73)

	stale := c.waitForStableCluster(readBound)
	if stale == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}
	_, staleTerm, _ := c.nodes[stale].snapshotState()

	// A write everyone sees, so "stale" later means stale rather than empty.
	idx, _, isLeader := c.nodes[stale].Submit(encodePut("k", "old"))
	if !isLeader {
		t.Fatal("the leader stopped leading before the setup write")
	}
	for i, m := range machines {
		if !m.waitForIndex(idx, readBound) {
			t.Fatalf("node %d never applied the setup write", i)
		}
	}

	// Two of five on the old leader's side, three on the other.
	others := c.othersThan(stale)
	minority, majority := others[:1], others[1:]
	c.net.partition(append([]int{stale}, minority...), majority)

	fresh := c.waitForLeaderAmong(majority, staleTerm+1, 2*readBound)
	if fresh == None {
		t.Fatalf("the majority never elected a leader within %v: %s",
			2*readBound, c.describe())
	}

	newIdx, _, isLeader := c.nodes[fresh].Submit(encodePut("k", "new"))
	if !isLeader {
		t.Fatalf("node %d stopped leading immediately after winning", fresh)
	}
	for _, id := range majority {
		if !machines[id].waitForIndex(newIdx, readBound) {
			t.Fatalf("node %d never applied the write from the new leader", id)
		}
	}

	// --- what a local read would have done --------------------------------

	if st, _, _ := c.nodes[stale].snapshotState(); st != Leader {
		t.Fatalf("node %d already stepped down, so this run never built the "+
			"scenario: %s", stale, c.describe())
	}
	if got, _ := machines[stale].value("k"); got != "old" {
		t.Fatalf("the old leader holds %q, want the stale %q: without stale "+
			"data on this node the test proves nothing", got, "old")
	}

	// Both halves of `if n.state == Leader { return n.state[key] }` are
	// satisfied right now, and the answer it would return is wrong.

	// --- what the barrier does instead ------------------------------------

	barrierIdx, _, isLeader := c.nodes[stale].ReadIndex()
	if !isLeader {
		t.Fatal("ReadIndex was refused locally: the scenario needs the stale " +
			"leader to ACCEPT the read and then fail to complete it, which is " +
			"the distinction the whole design turns on")
	}
	if machines[stale].waitForIndex(barrierIdx, 2*time.Second) {
		t.Errorf("the barrier at index %d applied on a node that can reach "+
			"only %d of %d: it committed without a majority",
			barrierIdx, len(minority)+1, len(c.nodes))
	}

	_, _, err := readThrough(c.nodes[stale], machines[stale], "k", time.Second)
	if err == nil {
		t.Error("the stale leader served a read: it cannot have confirmed " +
			"leadership, because it cannot reach a majority")
	}

	// --- and once the partition heals -------------------------------------
	c.net.heal()

	if c.waitForSingleLeader(2*readBound) == None {
		t.Fatalf("no leader after healing: %s", c.describe())
	}

	deadline := time.Now().Add(2 * readBound)
	for {
		for i, n := range c.nodes {
			got, ok, err := readThrough(n, machines[i], "k", readBound)
			if err == nil {
				if !ok || got != "new" {
					t.Fatalf("node %d served %q after healing, want %q",
						i, got, "new")
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no node completed a read within %v of healing: %s",
				2*readBound, c.describe())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The other reason local reads are wrong, and the cheaper one to demonstrate: a
// follower's state machine trails the leader's by construction, not by
// accident. It applies what LeaderCommit told it, and LeaderCommit arrives on
// the message AFTER the one that carried the entry.
func TestAFollowerStateMachineTrailsTheLeader(t *testing.T) {
	c, machines := failoverCluster(t, 3, 79)

	leader := c.waitForStableCluster(readBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}

	behind := c.othersThan(leader)[0]
	c.net.disconnect(behind)

	idx, _, isLeader := c.nodes[leader].Submit(encodePut("k", "written"))
	if !isLeader {
		t.Fatal("the leader stopped leading before the write")
	}
	if !machines[leader].waitForIndex(idx, readBound) {
		t.Fatal("the leader never applied its own write, with a majority available")
	}

	// The write is committed and acknowledged. A read served from the cut-off
	// follower's local state returns the state before it.
	if _, ok := machines[behind].value("k"); ok {
		t.Error("the disconnected follower already holds the key: the cut did " +
			"not take, so this proves nothing")
	}
	if got := machines[behind].appliedIndex(); got >= idx {
		t.Errorf("the disconnected follower is at index %d, want below %d",
			got, idx)
	}

	// And the protocol refuses to serve from it, for the simpler reason that it
	// is not the leader at all.
	if _, _, err := readThrough(c.nodes[behind], machines[behind], "k", time.Second); err != errNotLeader {
		t.Errorf("reading from a follower gave %v, want %v", err, errNotLeader)
	}

	c.net.reconnect(behind)
	if !machines[behind].waitForIndex(idx, readBound) {
		t.Fatal("the follower never caught up after reconnecting")
	}
}

// A barrier is a current-term entry, so committing it releases every inherited
// entry beneath it by Log Matching (§5.4.2).
//
// This is the payoff that makes the read path and the deferred no-op-on-election
// one decision rather than two: C-14 and C-15 both need a current-term write
// after a leader change before anything can commit, and both fake one with a
// throwaway Put. A ReadIndex is the honest version of that flush.
func TestAReadBarrierReleasesInheritedEntries(t *testing.T) {
	c, machines := failoverCluster(t, 5, 83)

	leader := c.waitForStableCluster(readBound)
	if leader == None {
		t.Fatalf("no leader within %v: %s", readBound, c.describe())
	}
	_, oldTerm, _ := c.nodes[leader].snapshotState()

	idx, _, isLeader := c.nodes[leader].Submit(encodePut("k", "inherited"))
	if !isLeader {
		t.Fatal("the leader stopped leading before the write")
	}
	for i, m := range machines {
		if !m.waitForIndex(idx, readBound) {
			t.Fatalf("node %d never applied the setup write", i)
		}
	}

	c.kill(leader)

	fresh := c.waitForLeaderAmong(c.othersThan(leader), oldTerm+1, 2*readBound)
	if fresh == None {
		t.Fatalf("no new leader within %v: %s", 2*readBound, c.describe())
	}

	// The new leader holds entries from the old term. A read barrier is the
	// only thing this test hands it, and the read must still complete.
	got, ok, err := readThrough(c.nodes[fresh], machines[fresh], "k", 3*readBound)
	if err != nil {
		t.Fatalf("read on the new leader: %v -- if this is %v, the barrier "+
			"committed but the prefix beneath it did not", err, errTooSlow)
	}
	if !ok || got != "inherited" {
		t.Errorf("read returned %q, want %q: the entry inherited from the "+
			"previous term was not released by the barrier", got, "inherited")
	}
}