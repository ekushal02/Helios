package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// A state machine that can be snapshotted
// =============================================================================

// snapshotMachine is a key-value store that takes its own image when Raft asks
// and replaces itself when Raft hands one down. It is the smallest consumer
// that exercises both halves of the snapshot contract.
//
// It owns the single ApplyCh consumer for its node, as ApplyCh requires: the
// channel guarantees delivery order, and only one receiver turns that into
// application order.
type snapshotMachine struct {
	id int
	n  *Node

	mu      sync.Mutex
	kv      map[string]string
	applied int // the index this store's contents account for
	images  int // snapshots handed DOWN and installed
	taken   int // snapshots handed UP
	fault   string

	done chan struct{}
}

type machineImage struct {
	KV      map[string]string
	Applied int
}

func attachSnapshotMachine(t *testing.T, n *Node) *snapshotMachine {
	t.Helper()

	m := &snapshotMachine{
		id:   n.id,
		n:    n,
		kv:   make(map[string]string),
		done: make(chan struct{}),
	}

	go m.run()

	// STOP THE NODE, THEN WAIT. The loop ends when applyCh closes, and applyCh
	// closes when the applier exits, which happens on Stop.
	//
	// Waiting without stopping deadlocks, and the ordering is not obvious:
	// cleanups run LIFO, so a machine attached AFTER the cluster registered its
	// own stop -- which is exactly what re-attaching after a restart does --
	// would wait for a node nobody has stopped yet. Stop is idempotent, so
	// doing it here costs nothing when the cluster gets there first.
	t.Cleanup(func() {
		n.Stop()
		<-m.done
	})

	return m
}

func (m *snapshotMachine) run() {
	defer close(m.done)

	for {
		select {
		case msg, ok := <-m.n.ApplyCh():
			if !ok {
				return // the node stopped
			}
			m.handle(msg)
		case <-m.n.SnapshotNotify():
			m.take()
		}
	}
}

func (m *snapshotMachine) handle(msg ApplyMsg) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch {
	case msg.SnapshotValid:
		// RULE 8 ON THE CONSUMER SIDE. The image REPLACES the store; it is not
		// merged into it. Anything already applied above the image's index
		// cannot exist, because Raft only sends an image a node has not reached.
		var img machineImage
		if err := gob.NewDecoder(bytes.NewReader(msg.Snapshot)).Decode(&img); err != nil {
			m.fault = fmt.Sprintf("node %d: undecodable image at index %d: %v",
				m.id, msg.SnapshotIndex, err)
			return
		}
		if img.Applied != msg.SnapshotIndex {
			m.fault = fmt.Sprintf("node %d: image says it covers index %d, Raft says %d",
				m.id, img.Applied, msg.SnapshotIndex)
			return
		}
		m.kv = img.KV
		m.applied = msg.SnapshotIndex
		m.images++

	case msg.CommandValid:
		if msg.CommandIndex != m.applied+1 {
			m.fault = fmt.Sprintf("node %d: applied index %d straight after %d",
				m.id, msg.CommandIndex, m.applied)
			return
		}
		k, v, ok := bytes.Cut(msg.Command, []byte("="))
		if !ok {
			m.fault = fmt.Sprintf("node %d: malformed command %q", m.id, msg.Command)
			return
		}
		m.kv[string(k)] = string(v)
		m.applied = msg.CommandIndex

	default:
		// A barrier: advance and apply nothing.
		m.applied = msg.CommandIndex
	}
}

// take answers the compaction signal with an image of the store as it stands.
func (m *snapshotMachine) take() {
	m.mu.Lock()
	if m.applied == 0 {
		m.mu.Unlock()
		return
	}
	img := machineImage{KV: make(map[string]string, len(m.kv)), Applied: m.applied}
	for k, v := range m.kv {
		img.KV[k] = v
	}
	m.taken++
	m.mu.Unlock()

	// Encoded and handed over OUTSIDE the machine's lock. Snapshot takes n.mu,
	// and the applier may be blocked trying to hand this machine an entry; a
	// state machine that called back into Raft while holding a lock the apply
	// path needs would deadlock the node.
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(img); err != nil {
		m.mu.Lock()
		m.fault = fmt.Sprintf("node %d: cannot encode its own image: %v", m.id, err)
		m.mu.Unlock()
		return
	}

	if err := m.n.Snapshot(img.Applied, buf.Bytes()); err != nil {
		m.mu.Lock()
		m.fault = fmt.Sprintf("node %d: Snapshot(%d) refused: %v", m.id, img.Applied, err)
		m.mu.Unlock()
	}
}

func (m *snapshotMachine) snapshot() (kv map[string]string, applied, images, taken int, fault string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	kv = make(map[string]string, len(m.kv))
	for k, v := range m.kv {
		kv[k] = v
	}
	return kv, m.applied, m.images, m.taken, m.fault
}

func (m *snapshotMachine) waitForApplied(index int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, applied, _, _, _ := m.snapshot(); applied >= index {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// =============================================================================
// The handler in isolation
// =============================================================================

// A follower with nothing in common with the image discards its whole log.
func TestInstallSnapshotReplacesAnIrrelevantLog(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 2
	for i := 1; i <= 4; i++ {
		n.log = append(n.log, LogEntry{Term: 2, Command: []byte{byte(i)}})
	}
	n.mu.Unlock()

	var reply InstallSnapshotReply
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term:              5,
		LeaderID:          1,
		LastIncludedIndex: 40,
		LastIncludedTerm:  4,
		Data:              []byte("image"),
	}, &reply)

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term != 5 {
		t.Errorf("reply.Term = %d, want 5: the follower must have stepped up", reply.Term)
	}
	if idx, term := n.snapshotFloor(); idx != 40 || term != 4 {
		t.Errorf("floor = (%d, %d), want (40, 4)", idx, term)
	}
	if n.logLength() != 0 {
		t.Errorf("logLength = %d, want 0: nothing in that log agreed with the image", n.logLength())
	}
	if n.lastLogIndex() != 40 {
		t.Errorf("lastLogIndex = %d, want 40", n.lastLogIndex())
	}
	if n.commitIndex != 40 {
		t.Errorf("commitIndex = %d, want 40", n.commitIndex)
	}
	if n.pendingSnapshot == nil {
		t.Error("no image was parked for the applier: the state machine will never see it")
	}
}

// Rule 6: a follower that already holds the entry at the image's index, with a
// matching term, keeps everything after it. Throwing that tail away would make
// the leader resend entries the follower demonstrably has.
func TestInstallSnapshotKeepsAMatchingTail(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 3
	for i := 1; i <= 8; i++ {
		n.log = append(n.log, LogEntry{Term: 3, Command: []byte{byte(i)}})
	}
	n.mu.Unlock()

	var reply InstallSnapshotReply
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term:              3,
		LeaderID:          1,
		LastIncludedIndex: 5,
		LastIncludedTerm:  3,
		Data:              []byte("image"),
	}, &reply)

	n.mu.Lock()
	defer n.mu.Unlock()

	if idx := n.lastIncludedIndex; idx != 5 {
		t.Fatalf("floor = %d, want 5", idx)
	}
	if got := n.lastLogIndex(); got != 8 {
		t.Errorf("lastLogIndex = %d, want 8: the matching tail was discarded", got)
	}
	for i := 6; i <= 8; i++ {
		if got := n.entryAt(i).Command[0]; got != byte(i) {
			t.Errorf("entryAt(%d) = %d, want %d", i, got, i)
		}
	}
}

// A stale image is dropped rather than applied. Installing it would rewind the
// state machine to a point this node has already passed.
func TestInstallSnapshotIgnoresAnImageAlreadyPassed(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 4
	for i := 1; i <= 10; i++ {
		n.log = append(n.log, LogEntry{Term: 4, Command: []byte{byte(i)}})
	}
	n.commitIndex = 10
	n.mu.Unlock()

	var reply InstallSnapshotReply
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term:              4,
		LeaderID:          1,
		LastIncludedIndex: 6,
		LastIncludedTerm:  4,
		Data:              []byte("stale"),
	}, &reply)

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.lastIncludedIndex != 0 {
		t.Errorf("floor moved to %d on an image below commitIndex", n.lastIncludedIndex)
	}
	if n.pendingSnapshot != nil {
		t.Error("a stale image was parked for the applier")
	}
}

// Rule 1. A leader from a dead term is told the current one and changes nothing.
func TestInstallSnapshotFromAStaleLeaderIsRefused(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 9
	n.mu.Unlock()

	var reply InstallSnapshotReply
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term:              4,
		LeaderID:          1,
		LastIncludedIndex: 100,
		LastIncludedTerm:  3,
		Data:              []byte("image"),
	}, &reply)

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term != 9 {
		t.Errorf("reply.Term = %d, want 9", reply.Term)
	}
	if n.lastIncludedIndex != 0 {
		t.Errorf("floor moved to %d for a leader two terms out of date", n.lastIncludedIndex)
	}
}

// =============================================================================
// The whole path
// =============================================================================

// A NODE OFFLINE FOR TEN THOUSAND ENTRIES.
//
// The victim is crashed, the cluster writes far more than any snapshot
// threshold, and the leader compacts repeatedly. When the victim returns, every
// entry it missed is gone from every surviving log: AppendEntries has nothing
// to walk back to, so the only way it can catch up is an image.
//
// The assertion is not that it catches up quickly. It is that it catches up AT
// ALL, and that its state machine ends byte-identical to the leader's -- an
// image plus the tail after it must reconstruct exactly what replaying ten
// thousand entries would have.
func TestAFollowerOfflineForTenThousandEntriesCatchesUp(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ten thousand entries through a real cluster")
	}

	const (
		total     = 10000
		threshold = 200
		seed      = 20260903
	)

	c := newCluster(t, 3, seed)

	machines := make([]*snapshotMachine, len(c.nodes))
	for i, n := range c.nodes {
		n.SetSnapshotThreshold(threshold)
		machines[i] = attachSnapshotMachine(t, n)
	}

	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}

	// The victim is a follower, so the cluster keeps its leader throughout and
	// the test measures catch-up rather than an election.
	victim := c.othersThan(leader)[0]
	c.crash(victim)

	for i := 1; i <= total; i++ {
		cmd := []byte(fmt.Sprintf("key%05d=%d", i, i))
		if !c.submitToLeader(cmd) {
			t.Fatalf("no leader accepted command %d: %s", i, c.describe())
		}
	}

	if !machines[leader].waitForApplied(total, 60*time.Second) {
		_, applied, _, _, _ := machines[leader].snapshot()
		t.Fatalf("leader applied %d of %d: %s", applied, total, c.describe())
	}

	// ANTI-VACUITY. If nothing compacted, the victim would catch up by ordinary
	// replication and this test would prove nothing about snapshots.
	c.nodes[leader].mu.Lock()
	floor := c.nodes[leader].lastIncludedIndex
	held := c.nodes[leader].logLength()
	c.nodes[leader].mu.Unlock()

	if floor == 0 {
		t.Fatal("the leader never compacted: the threshold did not take, and the " +
			"victim could have been repaired by AppendEntries alone")
	}
	_, _, _, taken, _ := machines[leader].snapshot()
	t.Logf("leader floor at %d, %d entries held, %d images taken", floor, held, taken)

	// The threshold measures what a compaction can discard, so each image
	// removes a threshold's worth and the count is bounded by total/threshold
	// plus a little slack. A number near `total` means the signal is measuring
	// the whole log instead, and the node is taking one complete image per
	// entry -- correct, and unusably slow.
	if maxImages := 4 * (total/threshold + 1); taken > maxImages {
		t.Errorf("leader took %d images for %d entries at threshold %d, want at "+
			"most %d: the compaction signal is thrashing", taken, total, threshold, maxImages)
	}

	snapshotsBefore := c.net.installSnapshots()

	c.restart(victim)

	// THE MACHINE MUST FOLLOW THE NODE. restart builds a NEW Node with a new
	// apply channel; the old machine's consumer loop ended the moment the old
	// channel closed, so leaving it attached would watch a node that no longer
	// exists and report nothing applied forever.
	//
	// A fresh machine is also the honest model rather than a convenience: a
	// restarted process comes back with an empty state machine, and rebuilding
	// it from an image plus the tail is the entire property under test.
	machines[victim] = attachSnapshotMachine(t, c.nodes[victim])

	if !machines[victim].waitForApplied(total, 60*time.Second) {
		_, applied, images, _, _ := machines[victim].snapshot()
		t.Fatalf("restarted node %d applied %d of %d after %d images: %s",
			victim, applied, total, images, c.describe())
	}

	if got := c.net.installSnapshots() - snapshotsBefore; got == 0 {
		t.Error("the victim caught up without a single InstallSnapshot, which " +
			"cannot happen if the entries it needed were really discarded")
	} else {
		t.Logf("%d InstallSnapshot RPCs delivered", got)
	}

	// The states must match exactly. An image plus the tail after it has to
	// reconstruct what replaying every entry would have produced.
	leaderKV, _, _, _, leaderFault := machines[leader].snapshot()
	victimKV, victimApplied, victimImages, _, victimFault := machines[victim].snapshot()

	if leaderFault != "" {
		t.Fatalf("leader state machine: %s", leaderFault)
	}
	if victimFault != "" {
		t.Fatalf("victim state machine: %s", victimFault)
	}
	if victimImages == 0 {
		t.Error("the victim's state machine was never handed an image, so the " +
			"snapshot never reached the layer that needed it")
	}
	if victimApplied != total {
		t.Errorf("victim applied through %d, want %d", victimApplied, total)
	}
	if len(victimKV) != len(leaderKV) {
		t.Fatalf("victim holds %d keys, leader holds %d", len(victimKV), len(leaderKV))
	}
	for k, want := range leaderKV {
		if got := victimKV[k]; got != want {
			t.Fatalf("victim %s = %q, leader has %q", k, got, want)
		}
	}
}
