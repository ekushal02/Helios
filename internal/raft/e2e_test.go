package raft

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// ADAPTER
//
// The ONLY contact this file has with the cluster harness. Everything else here
// is built from the production API (Submit, ApplyCh) and snapshotState, which
// consistency_test.go already uses.
//
// If this does not compile, it is the only thing to fix.
// =============================================================================

func e2eCluster(t *testing.T, size int, seed int64) []*Node {
	t.Helper()
	c := newCluster(t, size, seed)

	// newCluster builds and wires the nodes; start() runs them. Skipping this
	// leaves every node a follower at term 0 with no ticker, and it also skips
	// the t.Cleanup(c.stop) that start() registers -- so ApplyCh never closes
	// and the state machine goroutines below outlive the test.
	c.start()

	return c.nodes
}

// =============================================================================
// A state machine, at last
// =============================================================================

// kvMachine is the thing Raft has been agreeing on behalf of all along: a plain
// map, fed by one goroutine reading one channel.
//
// ONE GOROUTINE PER NODE, and that is not a simplification for the test. It is
// the contract ApplyCh's doc comment states. A pool of readers would take
// messages 4 and 5 in order and apply them in whichever order the scheduler
// picked, and the whole point of this test would evaporate.
//
// It records errors in a field rather than calling t.Errorf. The goroutine
// outlives the test body by however long it takes Stop to close the channel,
// and t.Log after a test completes panics -- the testWriter lesson, in the one
// place where the goroutine is not the harness's.
type kvMachine struct {
	mu sync.Mutex

	state   map[string]string // the data
	order   []string          // "index:command", every message, in arrival order
	terms   map[int]int       // index -> the term the entry applied at carried
	lastIdx int               // highest index applied
	fault   string            // first anomaly seen, checked by the test body

	done chan struct{} // closed when ApplyCh closes
}

func attachMachine(n *Node) *kvMachine {
	m := &kvMachine{
		state: map[string]string{},
		terms: map[int]int{},
		done:  make(chan struct{}),
	}

	go func() {
		defer close(m.done)
		// Terminates on its own: the applier closes ApplyCh when Stop is called.
		// Nothing here needs to know about shutdown.
		for msg := range n.ApplyCh() {
			m.apply(msg)
		}
	}()

	return m
}

func (m *kvMachine) apply(msg ApplyMsg) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// The state machine's own view of the ordering guarantee. Raft promises a
	// gapless, monotonic sequence starting at 1; if that is ever untrue, the
	// state machine is the layer that gets silently corrupted, so it is the
	// right layer to notice.
	if msg.CommandIndex != m.lastIdx+1 && m.fault == "" {
		m.fault = fmt.Sprintf("applied index %d after %d: the sequence has a "+
			"gap or a repeat", msg.CommandIndex, m.lastIdx)
	}
	m.lastIdx = msg.CommandIndex

	if !msg.CommandValid && m.fault == "" {
		m.fault = fmt.Sprintf("index %d arrived with CommandValid false",
			msg.CommandIndex)
	}

	// Recorded because Submit's doc comment makes (index, term) the claim
	// ticket: an index that comes back carrying a different term means that
	// submission was overwritten by a later leader. concurrent_test.go checks
	// exactly that pair; nothing here needs it yet.
	m.terms[msg.CommandIndex] = msg.CommandTerm

	cmd := string(msg.Command)
	m.order = append(m.order, fmt.Sprintf("%d:%s", msg.CommandIndex, cmd))

	key, val, ok := decodePut(cmd)
	if !ok {
		if m.fault == "" {
			m.fault = fmt.Sprintf("index %d: undecodable command %q",
				msg.CommandIndex, cmd)
		}
		return
	}
	m.state[key] = val
}

// waitForIndex blocks until this machine has applied idx. Returns false on
// timeout so the caller can produce a useful message.
func (m *kvMachine) waitForIndex(idx int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		got := m.lastIdx
		m.mu.Unlock()
		if got >= idx {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// appliedIndex and termAt are the narrow reads: no map copy, no slice copy, no
// allocation.
//
// snapshot() answers both questions too, but it also duplicates the whole state
// map and the whole order slice while holding the lock the applier needs. That
// is free when it happens three times at the end of a test, and quadratic when
// it happens once per operation against a slice that grows by one each time.
func (m *kvMachine) appliedIndex() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIdx
}

func (m *kvMachine) termAt(idx int) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	term, ok := m.terms[idx]
	return term, ok
}

func (m *kvMachine) snapshot() (map[string]string, []string, int, string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := make(map[string]string, len(m.state))
	for k, v := range m.state {
		state[k] = v
	}
	return state, append([]string(nil), m.order...), m.lastIdx, m.fault
}

// A throwaway encoding, deliberately. Phase H brings a real one; giving this
// test a serious codec now would mean writing it twice and inviting the second
// one to be a copy of the first.
func encodePut(key, val string) []byte { return []byte("put|" + key + "|" + val) }

func decodePut(cmd string) (key, val string, ok bool) {
	parts := strings.Split(cmd, "|")
	if len(parts) != 3 || parts[0] != "put" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// =============================================================================
// Finding the leader without the harness
// =============================================================================

// currentLeader returns the index of a node that believes it leads the highest
// term seen, or -1.
//
// Believing is enough here. This test is not checking Election Safety -- B-13
// and B-14 do that across a hundred seeded trials -- it just needs somewhere to
// send a write. A node that is wrong about leading will reject the Submit or
// fail to replicate, and the retry loop moves on.
func currentLeader(nodes []*Node) int {
	best, bestTerm := -1, -1
	for i, n := range nodes {
		state, term, _ := n.snapshotState()
		if state == Leader && term > bestTerm {
			best, bestTerm = i, term
		}
	}
	return best
}

func waitForLeader(t *testing.T, nodes []*Node, within time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if i := currentLeader(nodes); i >= 0 {
			return i
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no leader within %v", within)
	return -1
}

// =============================================================================
// Waiting for the cluster to go quiet
// =============================================================================

// quietApplyIndex polls every machine's applied index and returns once they all
// agree AND the agreed value has stopped moving. It reports the last reading it
// took and whether that reading satisfied both conditions.
//
// TWO CONSECUTIVE IDENTICAL READINGS, not one. "Everyone is at 340" can be true
// while a duplicate command from an abandoned retry is still working its way to
// commit; one tick later everyone is at 341 and the sequence a caller just
// compared is stale. On an in-memory network a full poll interval of no
// movement is a long silence. If this ever proves flaky the interval is the
// knob, not the assertions built on top of it.
//
// IT TAKES NO *testing.T AND NEVER FAILS THE TEST, which is the whole reason
// there is one of these rather than two. The two callers want opposite things
// from a timeout: the end-to-end test below wants to fall through and let its
// own assertions name the actual divergence, because "did not converge in 5s"
// is a worse message than "node 2 diverged from node 0 at position 47". The
// concurrent test wants to stop immediately, because every assertion after its
// call assumes a settled cluster and would otherwise produce a page of
// consequential noise. Returning (readings, ok) lets each one have its way.
func quietApplyIndex(machines []*kvMachine, within time.Duration) ([]int, bool) {
	const poll = 25 * time.Millisecond

	deadline := time.Now().Add(within)
	idxs := make([]int, len(machines))
	prev := -1

	for {
		same := true
		for i, m := range machines {
			idxs[i] = m.appliedIndex()
			if idxs[i] != idxs[0] {
				same = false
			}
		}

		switch {
		case same && idxs[0] == prev:
			return idxs, true
		case same:
			prev = idxs[0]
		default:
			prev = -1
		}

		if !time.Now().Before(deadline) {
			return idxs, false
		}
		time.Sleep(poll)
	}
}

// =============================================================================
// The test
// =============================================================================

// THE END-TO-END PROPERTY. Everything in Phase C exists to make this true:
// hand the cluster a sequence of writes, and every node holds the same data.
//
// Twenty keys written five times each, rather than a hundred distinct keys.
// With distinct keys the final map is order-independent -- a node that applied
// entries in scrambled order would still finish with the right answer, and the
// test would prove nothing about ordering. Reusing keys means the final value
// of each one is decided by which write landed LAST, so a single misordered
// apply changes the result.
func TestThreeNodesEndUpWithIdenticalStateMachines(t *testing.T) {
	const (
		puts     = 100
		keyCount = 20
	)

	nodes := e2eCluster(t, 3, 1)

	// Attached before the first write, but it would be safe afterwards too: an
	// applier with no consumer parks on the send rather than dropping anything,
	// and delivers the moment a reader arrives.
	machines := make([]*kvMachine, len(nodes))
	for i, n := range nodes {
		machines[i] = attachMachine(n)
	}

	want := map[string]string{}
	leader := waitForLeader(t, nodes, 3*time.Second)
	leaderChanges := 0

	for i := 0; i < puts; i++ {
		key := fmt.Sprintf("k%02d", i%keyCount)
		val := fmt.Sprintf("v%03d", i)
		cmd := encodePut(key, val)

		accepted := false

		// Ten attempts, each re-finding the leader. On a quiet in-memory
		// network the first attempt always wins; the loop exists because a
		// loaded CI box can lose a heartbeat and trigger an election, and this
		// test should then be slow rather than flaky.
		for attempt := 0; attempt < 10 && !accepted; attempt++ {
			if leader < 0 {
				leader = waitForLeader(t, nodes, 3*time.Second)
				leaderChanges++
			}

			idx, _, isLeader := nodes[leader].Submit(cmd)
			if !isLeader {
				leader = -1
				continue
			}

			// SEQUENTIAL means this: the write is observed applied before the
			// next one is issued. Without it the expected final map is a race
			// rather than a calculation.
			if !machines[leader].waitForIndex(idx, 3*time.Second) {
				// Appended but never applied -- almost always a leader that lost
				// its term before reaching a majority. The entry may still be
				// sitting uncommitted in some log, which is exactly why the
				// retry is only safe for an idempotent command.
				leader = -1
				continue
			}

			accepted = true
		}

		if !accepted {
			t.Fatalf("put %d (%s=%s) never committed after ten attempts", i, key, val)
		}

		// The expected map is maintained here, in issue order, which is the
		// order a client would reason in.
		want[key] = val
	}

	// The leader is ahead by construction. Followers learn what is committed on
	// the NEXT message -- during the run that is the next Put's fan-out, and for
	// the final Put it is the next heartbeat.
	//
	// The result is deliberately discarded. Failing to converge is left for the
	// assertions below to report, so that a failure names the actual divergence
	// rather than a timeout.
	quietApplyIndex(machines, 5*time.Second)

	// --- What every node must agree on -----------------------------------
	// Node 0 is the reference for the applied SEQUENCE only. Its map is not the
	// reference for the data -- every node is checked against `want` below, so a
	// cluster that agrees perfectly on the wrong answer still fails.
	_, baseOrder, baseIdx, _ := machines[0].snapshot()

	for i, m := range machines {
		state, order, lastIdx, fault := m.snapshot()

		if fault != "" {
			t.Errorf("node %d state machine: %s", i, fault)
		}

		// Vacuity guard. Three empty maps are identical too.
		if lastIdx < puts {
			t.Errorf("node %d applied only %d entries, want at least %d: this "+
				"comparison is meaningless if nothing replicated", i, lastIdx, puts)
		}

		if len(state) != keyCount {
			t.Errorf("node %d holds %d keys, want %d", i, len(state), keyCount)
		}
		for k, v := range want {
			if state[k] != v {
				t.Errorf("node %d: %s = %q, want %q (the last write to that key)",
					i, k, state[k], v)
			}
		}

		if i == 0 {
			continue
		}

		// The stronger claim: not merely the same data, but the same SEQUENCE.
		// Two nodes can reach an identical map through different orders when
		// writes happen to commute; asserting the order catches that, and
		// reports the first index where they part company.
		if lastIdx != baseIdx {
			t.Errorf("node %d applied %d entries, node 0 applied %d", i, lastIdx, baseIdx)
		}
		for j := 0; j < len(order) && j < len(baseOrder); j++ {
			if order[j] != baseOrder[j] {
				t.Fatalf("node %d diverged from node 0 at position %d: %q vs %q",
					i, j, order[j], baseOrder[j])
			}
		}
	}

	if leaderChanges > 0 {
		t.Logf("%d leader change(s) during the run; %d entries applied for %d puts",
			leaderChanges, baseIdx, puts)
	}
}

// The same property with a single node, as a control.
//
// If the three-node test fails, this one says whether the fault is in
// replication or in the local commit-and-apply path. A one-node cluster is its
// own majority, so every Submit commits immediately and nothing crosses the
// network.
func TestSingleNodeAppliesEveryPutInOrder(t *testing.T) {
	const puts = 100

	nodes := e2eCluster(t, 1, 7)
	m := attachMachine(nodes[0])

	leader := waitForLeader(t, nodes, 3*time.Second)

	for i := 0; i < puts; i++ {
		idx, _, isLeader := nodes[leader].Submit(encodePut("k", fmt.Sprintf("v%03d", i)))
		if !isLeader {
			t.Fatalf("put %d: the only node in the cluster is not leader", i)
		}
		if !m.waitForIndex(idx, 3*time.Second) {
			t.Fatalf("put %d committed at index %d but was never applied", i, idx)
		}
	}

	state, _, lastIdx, fault := m.snapshot()
	if fault != "" {
		t.Error(fault)
	}
	if lastIdx != puts {
		t.Errorf("applied %d entries, want exactly %d: no leader change is "+
			"possible here, so there is nothing to retry", lastIdx, puts)
	}
	if got := state["k"]; got != fmt.Sprintf("v%03d", puts-1) {
		t.Errorf("k = %q, want the last write: the final value is the whole "+
			"point of applying in order", got)
	}
}