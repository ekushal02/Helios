package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// C-14: the leader dies holding a commit nobody else knows about
// =============================================================================
//
// THE WINDOW, precisely.
//
// Submit is non-blocking, so "kill the leader before it responds" has to be
// translated: there is no response. The client's receipt is the index coming
// back out of ApplyCh carrying the term Submit promised. What this file kills
// is a leader that has moved commitIndex and has not yet told anybody.
//
// That window is real and it is wide. commitTo signals the applier and sends
// nothing; LeaderCommit rides the NEXT outbound AppendEntries, which is either
// the next client write's fan-out or the next heartbeat tick. With no further
// writes, a leader sits on a private commit decision for up to a full heartbeat
// interval.
//
// WHAT MUST SURVIVE. The entry is on a majority -- that is what committing
// means -- so §5.4.1 guarantees the next leader holds it: any voting majority
// intersects the holding majority, and the shared voter refuses a candidate
// whose log is behind.
//
// WHAT MUST NOT BE EXPECTED. The new leader cannot commit it. The entry carries
// an older term, and §5.4.2 forbids committing an old-term entry by counting
// replicas, so it sits in every surviving log applied by nobody until the new
// leader appends an entry of its own term. That is the availability cost noted
// in commit.go's TODO, not data loss, and a test that forgot the follow-up
// write would hang and misread it as loss.

const failureBound = 5 * time.Second

// =============================================================================
// Harness adapters
// =============================================================================

// failoverCluster returns BOTH halves: the harness, because this file needs
// kill, partition and heal, and a state machine per node, because the property
// is about applied state rather than log contents alone. e2eCluster returns
// only the nodes and is therefore not enough here.
func failoverCluster(t *testing.T, size int, seed int64) (*cluster, []*kvMachine) {
	t.Helper()

	c := newCluster(t, size, seed)
	c.start()

	machines := make([]*kvMachine, len(c.nodes))
	for i, n := range c.nodes {
		machines[i] = attachMachine(n)
	}
	return c, machines
}

// healAroundTheDead restores the partition without resurrecting anyone.
//
// THE TRAP THIS EXISTS FOR. kill() does two things -- Stop the goroutines and
// cut the network -- because a stopped node still answers RequestVote and
// AppendEntries: the fake network calls those handlers directly on the struct,
// so there is no process to exit and no socket to close. net.heal() sets every
// pair reachable, which quietly undoes the second half and puts a dead node
// back in the voting population, where it will grant votes forever from
// whatever term it died in.
//
// Every heal in a test that has killed anything must be followed by this, or
// the cluster silently grows back a member.
func healAroundTheDead(c *cluster) {
	c.net.heal()
	for _, n := range c.nodes {
		if c.isDead(n.id) {
			c.net.disconnect(n.id)
		}
	}
}

// survivingMachines pairs the live nodes with their state machines.
//
// A killed node's applier closes applyCh, so its kvMachine goroutine has
// already returned and its applied index is frozen wherever it stopped.
// Including it in a convergence check would compare a live cluster against a
// corpse.
func survivingMachines(c *cluster, machines []*kvMachine) []*kvMachine {
	var out []*kvMachine
	for i, n := range c.nodes {
		if !c.isDead(n.id) {
			out = append(out, machines[i])
		}
	}
	return out
}

// waitForCommitIndex polls one node's commit decision.
//
// The poll interval is deliberately tiny. This is what races the heartbeat: the
// commit is observable the instant the reply handler releases mu, and the next
// message carrying LeaderCommit is up to heartbeatInterval away. Detecting
// within a fraction of a millisecond is what makes the kill land inside the
// window rather than after it.
func waitForCommitIndex(n *Node, idx int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if commitIndexOf(n) >= idx {
			return true
		}
		time.Sleep(50 * time.Microsecond)
	}
	return false
}

// logTermAt reports the term of the entry a node holds at idx.
//
// This is the log-level question, asked without going through the state
// machine: does the entry still EXIST, regardless of whether anyone has
// committed or applied it. Between the kill and the follow-up write that is the
// only form the surviving evidence takes.
func logTermAt(n *Node, idx int) (int, bool) {
	log := n.logCopy()
	if idx < 1 || idx >= len(log) {
		return 0, false
	}
	return log[idx].Term, true
}

// submitToLiveLeader finds a leader that is actually alive and hands it a
// command, retrying while the cluster settles.
//
// currentLeader in e2e_test.go is the wrong tool once anything has been killed:
// it picks the node that BELIEVES it leads the highest term, and a killed node
// still believes, in whatever term it died in, forever. That is harmless when
// nothing is dead and a trap the moment something is.
func submitToLiveLeader(c *cluster, cmd []byte, within time.Duration) (idx, term, leader int, ok bool) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if c.isDead(n.id) {
				continue
			}
			if st, _, _ := n.snapshotState(); st != Leader {
				continue
			}
			if i, tm, isLeader := n.Submit(cmd); isLeader {
				return i, tm, n.id, true
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return 0, 0, None, false
}

// =============================================================================
// The headline test
// =============================================================================

// Several trials, because the window is a race that this test wins by a wide
// margin but not by construction.
//
// Each trial asserts the safety property unconditionally. What varies is
// whether the kill landed before the heartbeat: if a tick happened to fire in
// the fraction of a millisecond between the commit and the kill, the followers
// already knew, the scenario is the easy one, and the trial proves less. Losing
// that race every time across several trials would mean something is wrong with
// the timing assumption, which is what the count at the end checks. Failing an
// individual trial on it would be a flake.
func TestCommittedEntrySurvivesLeaderDeathBeforeAnnouncement(t *testing.T) {
	trials := 3
	if testing.Short() {
		trials = 1
	}

	unannounced := 0
	for trial := 0; trial < trials; trial++ {
		seed := int64(2_000 + trial)
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			if commitWindowTrial(t, seed) {
				unannounced++
			}
		})
	}

	if unannounced == 0 {
		t.Errorf("no trial killed the leader before a follower learned the "+
			"commit: every run tested the easy case, where the followers were "+
			"already told. The kill should beat the next heartbeat by most of "+
			"%v -- if this keeps happening, check that commitTo still sends "+
			"nothing and that heartbeatInterval has not collapsed",
			heartbeatInterval)
	}
	t.Logf("%d of %d trials killed inside the unannounced window", unannounced, trials)
}

// commitWindowTrial runs one scenario and reports whether the kill landed
// before any follower learned the commit.
func commitWindowTrial(t *testing.T, seed int64) bool {
	t.Helper()

	c, machines := failoverCluster(t, 5, seed)

	leader := c.waitForStableCluster(failureBound)
	if leader == None {
		t.Fatalf("no stable leader within %v: %s", failureBound, c.describe())
	}

	// A BARE MAJORITY, ON PURPOSE. The leader can reach exactly two followers,
	// so the entry commits on three of five and survives on two of the four
	// survivors. Left fully connected it would land on all five, the election
	// afterwards would be unconstrained, and §5.4.1 would never be tested --
	// the entry would survive because everyone happened to have it.
	others := c.othersThan(leader)
	keep, cut := others[:2], others[2:]
	c.net.partition(append([]int{leader}, keep...), cut)

	key, val := "survivor", fmt.Sprintf("committed-in-seed-%d", seed)
	idx, term, isLeader := c.nodes[leader].Submit(encodePut(key, val))
	if !isLeader {
		t.Fatalf("node %d stopped leading between the settle and the write", leader)
	}

	if !waitForCommitIndex(c.nodes[leader], idx, failureBound) {
		t.Fatalf("leader %d never committed index %d within %v: it can reach "+
			"%v, which with itself is a majority of five", leader, idx,
			failureBound, keep)
	}

	// KILL FIRST, THEN LOOK. Reading the followers before the kill would spend
	// the window doing the reading; the disconnect inside kill is what actually
	// closes the door on LeaderCommit.
	c.kill(leader)

	unannounced := true
	for _, id := range keep {
		if commitIndexOf(c.nodes[id]) >= idx {
			unannounced = false
		}
	}

	// --- THE SAFETY PROPERTY, at the log level ----------------------------
	//
	// Asked before any repair and before any commit, because this is the whole
	// claim: the entry a dead leader committed is still on disk somewhere, and
	// specifically on the majority that acknowledged it. Both keepers must hold
	// it -- the commit needed three of five, the cut nodes were unreachable and
	// their matchIndex never moved, so the third and second acknowledgements
	// came from exactly these two.
	for _, id := range keep {
		got, ok := logTermAt(c.nodes[id], idx)
		if !ok {
			t.Fatalf("node %d has no entry at index %d: a committed write was "+
				"lost with the leader that committed it", id, idx)
		}
		if got != term {
			t.Fatalf("node %d holds term %d at index %d, want %d: the slot was "+
				"overwritten", id, got, idx, term)
		}
	}
	for _, id := range cut {
		if _, ok := logTermAt(c.nodes[id], idx); ok {
			t.Fatalf("node %d was partitioned away and holds index %d anyway: "+
				"the partition did not take, so this trial proved nothing "+
				"about a bare majority", id, idx)
		}
	}

	healAroundTheDead(c)

	newLeader := c.waitForSingleLeader(2 * failureBound)
	if newLeader == None {
		t.Fatalf("no leader among the survivors within %v: %s",
			2*failureBound, c.describe())
	}

	// §5.4.1, EARNING ITS KEEP. Two of the four survivors hold the entry. A
	// candidate needs three votes, so every possible majority contains at least
	// one of them, and that voter refuses anyone whose log is behind. The
	// winner is therefore forced to be a node that already has the entry --
	// which is the entire mechanism by which a commit outlives the leader that
	// made it.
	if newLeader != keep[0] && newLeader != keep[1] {
		t.Fatalf("node %d won the election without holding the committed entry "+
			"at index %d: §5.4.1 should have made %v refuse it. %s",
			newLeader, idx, keep, c.describe())
	}

	// §5.4.2, THE PART THAT LOOKS LIKE A BUG. The entry is inherited, so the
	// new leader may not commit it by counting replicas no matter how well
	// replicated it becomes. Nothing has been applied and nothing will be until
	// the new leader appends in its own term.
	//
	// Only checkable when the window was won: if a heartbeat leaked the old
	// LeaderCommit before the kill, the keepers committed and applied it then,
	// which is correct and simply a different scenario.
	if unannounced {
		for i, m := range machines {
			if c.isDead(c.nodes[i].id) {
				continue
			}
			if applied := m.appliedIndex(); applied >= idx {
				t.Fatalf("node %d applied index %d before any current-term "+
					"entry committed: §5.4.2 forbids committing an inherited "+
					"entry by counting", i, idx)
			}
		}
	}

	// THE RELEASE. One write of the new leader's own term, onto a majority,
	// and the whole inherited prefix commits with it by Log Matching.
	idx2, _, _, ok := submitToLiveLeader(c, encodePut("release", "own-term"), failureBound)
	if !ok {
		t.Fatalf("no live leader accepted the follow-up write within %v: %s",
			failureBound, c.describe())
	}

	live := survivingMachines(c, machines)
	for _, m := range live {
		if !m.waitForIndex(idx2, 2*failureBound) {
			t.Fatalf("a survivor never applied index %d, so the inherited "+
				"entry at %d never came out either", idx2, idx)
		}
	}

	// --- THE SAFETY PROPERTY, at the state machine ------------------------
	//
	// THE CLAIM TICKET, HONOURED ACROSS A LEADER CHANGE. The index comes back
	// out of ApplyCh carrying the term the DEAD leader promised for it, on
	// every survivor, including the two that were partitioned away and never
	// saw the original write.
	for i, m := range live {
		gotTerm, ok := m.termAt(idx)
		if !ok {
			t.Fatalf("survivor %d never applied index %d", i, idx)
		}
		if gotTerm != term {
			t.Fatalf("survivor %d applied index %d carrying term %d, want %d: "+
				"a different command took the committed slot", i, idx, gotTerm, term)
		}

		state, _, _, fault := m.snapshot()
		if fault != "" {
			t.Errorf("survivor %d state machine: %s", i, fault)
		}
		if state[key] != val {
			t.Errorf("survivor %d: %s = %q, want %q", i, key, state[key], val)
		}
	}

	assertSameAppliedSequence(t, live)

	return unannounced
}

// assertSameAppliedSequence compares applied sequences element by element and
// names the first position where two nodes part company.
//
// Compared as sequences rather than maps because two nodes can reach the same
// map through different orders when the writes commute, and the whole point of
// consensus is that they do not get to.
func assertSameAppliedSequence(t *testing.T, machines []*kvMachine) {
	t.Helper()
	if len(machines) < 2 {
		return
	}

	_, baseOrder, _, _ := machines[0].snapshot()
	for i := 1; i < len(machines); i++ {
		_, order, _, _ := machines[i].snapshot()

		n := len(order)
		if len(baseOrder) < n {
			n = len(baseOrder)
		}
		for j := 0; j < n; j++ {
			if order[j] != baseOrder[j] {
				t.Fatalf("survivors 0 and %d diverge at applied position %d: "+
					"%q vs %q -- State Machine Safety is violated",
					i, j, baseOrder[j], order[j])
			}
		}
		if len(order) != len(baseOrder) {
			t.Errorf("survivor %d applied %d entries, survivor 0 applied %d, "+
				"after both were given time to settle",
				i, len(order), len(baseOrder))
		}
	}
}

// =============================================================================
// The same failure under load
// =============================================================================

// Ten clients writing continuously, and the leader dies a quarter of the way
// in. This is C-13's scenario with the one thing C-13 never had: a leader
// change.
//
// Everything a client OBSERVED applied must still be applied afterwards, on
// every survivor, in the same order. Writes that were in flight when the leader
// died may or may not have landed -- that is what a claim ticket means -- and
// the clients resolve those themselves by retrying, which is why retries above
// zero is the anti-vacuity guard here.
func TestConcurrentClientsSurviveALeaderKill(t *testing.T) {
	ops := concOpsPerClient
	if testing.Short() {
		ops = 40
	}
	total := concClients * ops

	c, machines := failoverCluster(t, 5, 31)

	leader := c.waitForStableCluster(failureBound)
	if leader == None {
		t.Fatalf("no stable leader within %v: %s", failureBound, c.describe())
	}

	meter := &inFlightMeter{}
	clients := newConcClients(c.nodes, machines, meter)

	// The clients are launched here rather than through runConcClients because
	// the kill has to happen while they are running, and it has to happen from
	// the test goroutine -- t.Fatalf anywhere else is a Goexit on the wrong
	// stack.
	var wg sync.WaitGroup
	wg.Add(len(clients))
	for _, cl := range clients {
		go cl.run(ops, &wg)
	}

	// Wait for real progress rather than sleeping, so the kill lands in the
	// middle of a busy log instead of before the first write or after the last.
	killAt := total / 4
	killed := machines[leader].waitForIndex(killAt, 2*failureBound)
	if killed {
		c.kill(leader)
	}

	// wg.Wait BEFORE any assertion. Failing the test with ten client goroutines
	// still writing to t is the testWriter panic all over again.
	wg.Wait()

	if !killed {
		t.Fatalf("leader %d never applied index %d, so no kill happened and "+
			"this run tested nothing C-13 does not", leader, killAt)
	}
	for _, cl := range clients {
		if cl.fault != "" {
			t.Fatal(cl.fault)
		}
	}

	retries := totalRetries(clients)
	t.Logf("%d ops from %d clients, leader %d killed at applied index %d; "+
		"retries = %d", total, len(clients), leader, killAt, retries)

	// ANTI-VACUITY. A kill that cost nobody a retry means the clients all
	// happened to be idle across the whole election, and the run is C-13 with
	// extra steps.
	if retries == 0 {
		t.Error("no client retried across the leader kill: the failure was " +
			"invisible to every client, so nothing about recovery was exercised")
	}

	live := survivingMachines(c, machines)
	settled := mustQuietApplyIndex(t, live, 4*failureBound)

	for i, m := range live {
		if _, _, _, fault := m.snapshot(); fault != "" {
			t.Errorf("survivor %d state machine: %s", i, fault)
		}
	}

	// The C-13 fence, on the scenario it was written for.
	//
	// Note what a KILL does and does not produce. A retry after a killed leader
	// costs a round trip but no duplicate: the dead leader's uncommitted tail
	// is disconnected and dies with it, so the retried command commits exactly
	// once. Duplicates need a leader whose entries reached a majority and who
	// then lost its term without committing them -- a PARTITION, not a kill.
	// Expect this count to be exact here, and expect the fence to stay
	// unexercised until that scenario gets its own task.
	if settled < total {
		t.Errorf("applied %d entries, want at least %d: a write the clients "+
			"watched land is missing", settled, total)
	}
	if settled > total {
		t.Logf("applied %d entries for %d ops: %d duplicates survived the kill",
			settled, total, settled-total)
	}

	assertSameAppliedSequence(t, live)

	want := make(map[string]string, concClients*concKeysPerClient)
	for _, cl := range clients {
		for k, v := range cl.want {
			want[k] = v
		}
	}
	for i, m := range live {
		state, _, _, _ := m.snapshot()
		if len(state) != len(want) {
			t.Errorf("survivor %d holds %d keys, want %d", i, len(state), len(want))
		}
		for k, wantV := range want {
			if got := state[k]; got != wantV {
				t.Errorf("survivor %d: %s = %q, want %q -- a write this client "+
					"watched apply did not survive the leader change",
					i, k, got, wantV)
			}
		}
	}
}
