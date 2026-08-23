package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Storage that outlives its node
// =============================================================================

// crashableStorage is the harness equivalent of a disk: it belongs to a node
// SLOT, not to a Node, so it is still there when a new Node is built for the
// same id.
//
// crash() models power loss rather than a disk error. Writes after that point
// never reach the medium and Save reports success anyway, which is exactly what
// a machine losing power does -- it does not return an error, it stops being a
// computer. That is only sound because cluster.crash cuts the network FIRST:
// from that instant nothing the node says can reach a peer, so a write that
// silently vanishes cannot be one anybody relied on. Reversed, the node would
// persist nothing, reply success, and the leader would count a follower that is
// about to forget -- a lost committed entry manufactured by the harness rather
// than found in the code.
type crashableStorage struct {
	mu        sync.Mutex
	data      []byte
	crashed   bool
	saves     int
	discarded int
}

func newCrashableStorage() *crashableStorage { return &crashableStorage{} }

func (s *crashableStorage) Save(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.crashed {
		s.discarded++
		return nil
	}
	s.data = append([]byte(nil), b...)
	s.saves++
	return nil
}

func (s *crashableStorage) Load() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		return nil, nil
	}
	return append([]byte(nil), s.data...), nil
}

func (s *crashableStorage) crash() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crashed = true
}

func (s *crashableStorage) revive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crashed = false
}

// discardedWrites reports how many Saves fell into the gap between the crash
// and the node's goroutines actually stopping. Zero means the kill did not land
// mid-write and the round proved less than it looks.
func (s *crashableStorage) discardedWrites() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.discarded
}

// =============================================================================
// Crash and restart
// =============================================================================

// crash takes a node down the way a power cut would.
//
// ORDER IS THE WHOLE DESIGN. See crashableStorage. Contrast with kill(), which
// leaves the node's memory intact and is therefore not a crash at all -- it is
// an unreachable node that happens to have stopped ticking.
func (c *cluster) crash(id int) {
	c.t.Helper()

	c.net.disconnect(id)  // nothing it says from here is heard
	c.storage[id].crash() // nothing it writes from here reaches the medium
	c.nodes[id].Stop()    // and now the goroutines stop

	c.mu.Lock()
	c.dead[id] = true
	c.mu.Unlock()
}

// restart builds a new Node over the surviving storage, as a supervisor would
// after the process died.
//
// The new node keeps the old seed, so its election timeouts replay
// deterministically for a given cluster seed. It does NOT keep state, leadership
// or commitIndex: everything except the three persistent fields comes back at
// its zero value, which is the property the ledger is really checking.
func (c *cluster) restart(id int) {
	c.t.Helper()

	c.storage[id].revive()

	node, err := OpenNode(id, c.peersFor(id), c.net.endpoint(id),
		c.seed*1_000_003+int64(id), c.storage[id])
	if err != nil {
		c.t.Fatalf("node %d refused to restart: %v", id, err)
	}
	node.SetLogger(c.baseLogger)

	// Under the lock because submitToLeader reads c.nodes and c.dead from a
	// background goroutine. Every other reader of c.nodes runs on the test
	// goroutine, which is also the only caller of restart, so these two are the
	// only places that need it.
	c.mu.Lock()
	c.nodes[id] = node
	delete(c.dead, id)
	c.mu.Unlock()

	// register replaces the network's entry for this id and restores its links
	// in both directions -- the reconnect half of what crash cut.
	c.net.register(node)

	if c.applyHook != nil {
		c.watchNode(node)
	}
	node.Start()
}

func (c *cluster) peersFor(id int) []int {
	var peers []int
	for j := 0; j < len(c.nodes); j++ {
		if j != id {
			peers = append(peers, j)
		}
	}
	return peers
}

// =============================================================================
// Watching what reaches the state machines
// =============================================================================

// watchApplies attaches a consumer to every node's ApplyCh, now and after any
// restart. Must be called before start().
//
// Exactly one consumer per node, which is ApplyCh's stated requirement: two
// would receive in order and then record in whatever order the scheduler picked,
// and the ledger would report reorder violations that never happened.
func (c *cluster) watchApplies(hook func(node int, msg ApplyMsg)) {
	c.applyHook = hook
	for _, n := range c.nodes {
		c.watchNode(n)
	}
}

func (c *cluster) watchNode(n *Node) {
	hook := c.applyHook
	id := n.id
	ch := n.ApplyCh()

	c.applyWG.Add(1)
	go func() {
		defer c.applyWG.Done()
		// Terminates on its own: the applier closes applyCh when the node
		// stops, which is what makes range the complete consumer.
		for msg := range ch {
			hook(id, msg)
		}
	}()
}

// submitToLeader offers a command to whichever live node accepts it.
//
// Dead nodes are skipped explicitly. A crashed leader still believes it leads --
// nothing in Stop touches state -- so Submit would happily append to a log
// nobody will ever read.
func (c *cluster) submitToLeader(cmd []byte) bool {
	for _, n := range c.liveNodes() {
		if _, _, ok := n.Submit(cmd); ok {
			return true
		}
	}
	return false
}

func (c *cluster) liveNodes() []*Node {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		if !c.dead[n.id] {
			out = append(out, n)
		}
	}
	return out
}

// =============================================================================
// The ledger
// =============================================================================

// applyLedger records everything every state machine is told, and is where a
// lost or rewritten committed entry shows up.
//
// It reports through t.Errorf rather than t.Fatalf because it runs on the
// watcher goroutines, and FailNow is only legal on the goroutine running the
// test.
type applyLedger struct {
	t    *testing.T
	seed int64

	mu       sync.Mutex
	command  map[int]string // index -> command, as first applied by anyone
	term     map[int]int    // index -> term, likewise
	firstBy  map[int]int    // index -> which node applied it first
	high     map[int]int    // node -> highest index it has applied
	next     map[int]int    // node -> next index expected from that node
	failures int
}

func newApplyLedger(t *testing.T, seed int64) *applyLedger {
	return &applyLedger{
		t:       t,
		seed:    seed,
		command: make(map[int]string),
		term:    make(map[int]int),
		firstBy: make(map[int]int),
		high:    make(map[int]int),
		next:    make(map[int]int),
	}
}

func (l *applyLedger) record(node int, msg ApplyMsg) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// A stream that goes back to index 1 is a restarted node replaying its log,
	// which is the expected behaviour and not a gap.
	if msg.CommandIndex == 1 {
		l.next[node] = 1
	}
	want := l.next[node]
	if want == 0 {
		want = 1
	}
	if msg.CommandIndex != want {
		l.t.Errorf("seed %d: node %d applied index %d, expected %d: "+
			"the apply stream skipped or repeated",
			l.seed, node, msg.CommandIndex, want)
		l.failures++
	}
	l.next[node] = msg.CommandIndex + 1

	if msg.CommandIndex > l.high[node] {
		l.high[node] = msg.CommandIndex
	}

	key := string(msg.Command)
	if !msg.CommandValid {
		key = "<barrier>"
	}

	prev, seen := l.command[msg.CommandIndex]
	if !seen {
		l.command[msg.CommandIndex] = key
		l.term[msg.CommandIndex] = msg.CommandTerm
		l.firstBy[msg.CommandIndex] = node
		return
	}

	// THE VIOLATION THIS FILE EXISTS FOR. Two state machines were told
	// different things about the same slot in the log -- or one machine was
	// told two different things about it across a restart. Either way an entry
	// that was announced as committed did not survive.
	if prev != key {
		l.t.Errorf("seed %d: STATE MACHINE SAFETY VIOLATED at index %d: "+
			"node %d applied %q, node %d applied %q",
			l.seed, msg.CommandIndex, l.firstBy[msg.CommandIndex], prev, node, key)
		l.failures++
	}
	if l.term[msg.CommandIndex] != msg.CommandTerm {
		l.t.Errorf("seed %d: index %d applied in term %d by node %d and term %d by node %d: "+
			"the entry was overwritten after being committed",
			l.seed, msg.CommandIndex, l.term[msg.CommandIndex],
			l.firstBy[msg.CommandIndex], msg.CommandTerm, node)
		l.failures++
	}
}

func (l *applyLedger) maxIndex() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	highest := 0
	for idx := range l.command {
		if idx > highest {
			highest = idx
		}
	}
	return highest
}

func (l *applyLedger) highFor(node int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.high[node]
}

func (l *applyLedger) failureCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.failures
}

func (l *applyLedger) waitForAnyIndex(idx int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if l.maxIndex() >= idx {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

func (l *applyLedger) waitForNode(node, idx int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if l.highFor(node) >= idx {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// =============================================================================
// The test
// =============================================================================

const crashRecoveryRounds = 100

// A node is crashed while writes are in flight, restarted over whatever its
// storage kept, and must come back agreeing with everyone about every index
// that was ever applied.
//
// The cluster is three nodes, so the two survivors are a majority and writes
// keep committing throughout the outage. That is the arrangement that makes the
// test sharp: the crashed node returns to a cluster that has moved on, has to be
// caught up by the leader, and has to reconcile whatever uncommitted tail its
// disk happened to keep.
//
// A hundred seeds because the interesting variable is WHEN the crash lands --
// between the append and the persist, inside a fan-out, mid-election, or while
// the victim is the leader. No single seed reaches many of those.
func TestCrashDuringWritesLosesNoCommittedEntry(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a hundred clusters; part of the full suite")
	}

	roundsThatLostAWrite := 0
	roundsThatMadeProgress := 0

	for i := 0; i < crashRecoveryRounds; i++ {
		seed := int64(20260822 + i)
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			discarded, agreed := crashRecoveryRound(t, seed)
			if discarded > 0 {
				roundsThatLostAWrite++
			}
			if agreed > 0 {
				roundsThatMadeProgress++
			}
		})
	}

	// Anti-vacuity. A round where nothing committed, or where the crash never
	// landed near a write, satisfied every assertion above without testing
	// anything. These bounds are loose on purpose -- they catch a harness that
	// has stopped working, not a scheduler having a quiet afternoon.
	if roundsThatMadeProgress < crashRecoveryRounds {
		t.Errorf("%d of %d rounds committed nothing", crashRecoveryRounds-roundsThatMadeProgress, crashRecoveryRounds)
	}
	if roundsThatLostAWrite == 0 {
		t.Error("no round discarded a write: the crash never landed while the " +
			"victim was persisting, so nothing here tested recovery from a lost write")
	}
	t.Logf("%d of %d rounds crashed the victim mid-write", roundsThatLostAWrite, crashRecoveryRounds)
}

func crashRecoveryRound(t *testing.T, seed int64) (discarded, agreed int) {
	rng := rand.New(rand.NewSource(seed))

	c := newCluster(t, 3, seed)
	ledger := newApplyLedger(t, seed)
	c.watchApplies(ledger.record)
	c.start()

	stop := make(chan struct{})
	var writers sync.WaitGroup

	// Ordered teardown, and it has to be this order. The writers stop first so
	// nothing is submitting into a cluster that is shutting down; then the
	// nodes stop, which closes every applyCh; then the watchers drain and exit.
	// Without the last wait a watcher can call t.Errorf after the round has
	// finished, which panics rather than failing.
	defer func() {
		close(stop)
		writers.Wait()
		c.stop()
		c.applyWG.Wait()
	}()

	if leader := c.waitForStableCluster(3 * time.Second); leader == None {
		t.Fatalf("seed %d: no leader before the crash: %s", seed, c.describe())
	}

	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			c.submitToLeader([]byte(fmt.Sprintf("seed%d-cmd%d", seed, i)))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	if !ledger.waitForAnyIndex(5, 5*time.Second) {
		t.Fatalf("seed %d: only %d entries applied before the crash: %s",
			seed, ledger.maxIndex(), c.describe())
	}

	// The victim may be the leader. That round costs an election, and it is the
	// more interesting half of the distribution: a leader that crashes has
	// entries in its log that only it has seen.
	victim := rng.Intn(len(c.nodes))
	c.crash(victim)

	// The survivors are a majority, so commits must continue without the victim.
	// This is also what guarantees the victim comes back to a cluster that has
	// moved on rather than one waiting for it.
	target := ledger.maxIndex() + 5
	if !ledger.waitForAnyIndex(target, 8*time.Second) {
		t.Fatalf("seed %d: cluster stalled at index %d with node %d down, want past %d: %s",
			seed, ledger.maxIndex(), victim, target, c.describe())
	}

	c.restart(victim)

	// Everything the cluster had agreed on at the moment of restart must reach
	// the restarted node's state machine, and the ledger checks each index
	// against what everyone else applied as it arrives.
	catchUp := ledger.maxIndex()
	if !ledger.waitForNode(victim, catchUp, 10*time.Second) {
		t.Fatalf("seed %d: restarted node %d applied up to %d, cluster reached %d: %s",
			seed, victim, ledger.highFor(victim), catchUp, c.describe())
	}

	if n := ledger.failureCount(); n > 0 {
		t.FailNow() // the ledger has already reported each one
	}

	return c.storage[victim].discardedWrites(), ledger.maxIndex()
}
