package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNetwork, an in-memory switchboard between nodes.
type fakeNetwork struct {
	mu sync.Mutex

	seed int64
	rng  *rand.Rand

	nodes map[int]*Node

	reachable map[int]map[int]bool //N x N matrix

	dropRate      float64       //chance a deliverable REQUEST is dropped anyway
	replyDropRate float64       //chance a REPLY is dropped after the handler ran
	minDelay      time.Duration //lower bound on random per-message delay
	maxDelay      time.Duration //upper bound on random per-message delay

	rpcCount  int //for asserting things like "the minority got no votes"
	dropCount int //requests plus replies actually discarded

	installSnapshotRPCs int
	snapshotBytes       int
	appendRPCs          int // AppendEntries messages actually delivered
	entriesShipped      int // entries carried across all of them

	nextSeq     map[[2]int]int //next stamp per directed pair, assigned at send
	lastArrived map[[2]int]int //highest stamp delivered per directed pair
	reordered   int            //messages that arrived behind a later-sent one
}

func newFakeNetwork(seed int64) *fakeNetwork {
	return &fakeNetwork{
		seed:        seed,
		rng:         rand.New(rand.NewSource(seed)),
		nodes:       make(map[int]*Node),
		reachable:   make(map[int]map[int]bool),
		nextSeq:     make(map[[2]int]int),
		lastArrived: make(map[[2]int]int),
	}
}

// register adds a node, fully connected to every node already present.
func (fn *fakeNetwork) register(n *Node) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	fn.nodes[n.id] = n
	if fn.reachable[n.id] == nil {
		fn.reachable[n.id] = make(map[int]bool)
	}
	for other := range fn.nodes {
		fn.reachable[n.id][other] = true
		fn.reachable[other][n.id] = true
	}
}

// disconnect isolates a node in both directions.
func (fn *fakeNetwork) disconnect(id int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for other := range fn.nodes {
		fn.reachable[id][other] = false
		fn.reachable[other][id] = false
	}
}

// reconnect restores a node's links in both directions.
func (fn *fakeNetwork) reconnect(id int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for other := range fn.nodes {
		fn.reachable[id][other] = true
		fn.reachable[other][id] = true
	}
}

// partition splits the cluster into groups: nodes talk iff they share a group.
func (fn *fakeNetwork) partition(groups ...[]int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	for from := range fn.nodes {
		for to := range fn.nodes {
			fn.reachable[from][to] = false
		}
	}
	for _, g := range groups {
		for _, from := range g {
			for _, to := range g {
				fn.reachable[from][to] = true
			}
		}
	}
}

// heal restores full connectivity.
func (fn *fakeNetwork) heal() {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for from := range fn.nodes {
		for to := range fn.nodes {
			fn.reachable[from][to] = true
		}
	}
}

func (fn *fakeNetwork) setDropRate(p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.dropRate = p
}

// setReplyDropRate loses the REPLY after the receiver has already acted on the
// request.
//
// THE CASE setDropRate CANNOT PRODUCE, and the more interesting of the two. A
// lost request means the follower never saw anything. A lost reply means the
// follower appended the entries and the leader does not know: nextIndex stays
// put and the next tick resends entries the follower already holds. That path
// is the only thing in the suite that puts mergeEntries in front of a duplicate
// append, and a receiver that truncated on entries matching its own log would
// pass every other test in this package.
func (fn *fakeNetwork) setReplyDropRate(p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.replyDropRate = p
}

func (fn *fakeNetwork) setMaxDelay(d time.Duration) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.maxDelay = d
}

func (fn *fakeNetwork) setDelayRange(min, max time.Duration) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.minDelay = min
	fn.maxDelay = max
}

func (fn *fakeNetwork) rpcs() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.rpcCount
}

func (fn *fakeNetwork) drops() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.dropCount
}

func (fn *fakeNetwork) countAppend(entries int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.appendRPCs++
	fn.entriesShipped += entries
}

func (fn *fakeNetwork) appendStats() (msgs, entries int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.appendRPCs, fn.entriesShipped
}

func (fn *fakeNetwork) resetCounters() {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.rpcCount, fn.dropCount = 0, 0
	fn.appendRPCs, fn.entriesShipped = 0, 0
	fn.installSnapshotRPCs, fn.snapshotBytes = 0, 0
}

func (fn *fakeNetwork) reorderedCount() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.reordered
}

// endpoint binds the network to one node's perspective, so a Node never has to say who it is on every call.
type endpoint struct {
	net  *fakeNetwork
	from int
}

func (fn *fakeNetwork) endpoint(from int) Transport {
	return &endpoint{net: fn, from: from}
}

func (e *endpoint) SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	target, delay, seq, ok := e.net.route(e.from, to)
	if !ok {
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	e.net.arrive(e.from, to, seq)

	//Copy across the wire so sender and receiver never share memory.
	var argsCopy RequestVoteArgs
	mustRoundTrip(args, &argsCopy)

	var replyCopy RequestVoteReply
	target.RequestVote(&argsCopy, &replyCopy)

	if !e.net.replyDeliverable(to, e.from) {
		return false
	}
	mustRoundTrip(&replyCopy, reply)
	return true
}

func (e *endpoint) SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	target, delay, seq, ok := e.net.route(e.from, to)
	if !ok {
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	e.net.arrive(e.from, to, seq)

	var argsCopy AppendEntriesArgs
	mustRoundTrip(args, &argsCopy)

	e.net.countAppend(len(argsCopy.Entries))

	var replyCopy AppendEntriesReply
	target.AppendEntries(&argsCopy, &replyCopy)

	if !e.net.replyDeliverable(to, e.from) {
		return false
	}
	mustRoundTrip(&replyCopy, reply)
	return true
}

// route decides whether a message is delivered and returns the target, any
// delay to apply, and a send-order stamp for the reorder accounting.
func (fn *fakeNetwork) route(from, to int) (*Node, time.Duration, int, bool) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	fn.rpcCount++

	if !fn.reachable[from][to] {
		return nil, 0, 0, false
	}
	if fn.dropRate > 0 && fn.rng.Float64() < fn.dropRate {
		fn.dropCount++
		return nil, 0, 0, false
	}

	delay := fn.minDelay
	if fn.maxDelay > fn.minDelay {
		delay += time.Duration(fn.rng.Int63n(int64(fn.maxDelay - fn.minDelay)))
	}

	// The stamp is assigned here, under the lock, so it records the order the
	// messages were SENT. arrive records the order they were delivered. The two
	// disagreeing is the definition of a reorder.
	pair := [2]int{from, to}
	seq := fn.nextSeq[pair]
	fn.nextSeq[pair] = seq + 1

	return fn.nodes[to], delay, seq, true
}

// arrive records a delivery and counts it if an earlier-sent message to the
// same peer has already gone past.
//
// A dropped message consumes a stamp it never delivers, which leaves a
// permanent hole in the sequence. That is harmless: the hole never arrives, so
// it can neither cause nor mask a count.
func (fn *fakeNetwork) arrive(from, to, seq int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	pair := [2]int{from, to}
	if high, seen := fn.lastArrived[pair]; seen && seq < high {
		fn.reordered++
		return
	}
	fn.lastArrived[pair] = seq
}

// deliverable re-checks the return path, since the network may have been cut while the handler was running.
func (fn *fakeNetwork) deliverable(from, to int) bool {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.reachable[from][to]
}

// replyDeliverable is deliverable plus the reply-loss roll.
func (fn *fakeNetwork) replyDeliverable(from, to int) bool {
	if !fn.deliverable(from, to) {
		return false
	}

	fn.mu.Lock()
	defer fn.mu.Unlock()

	if fn.replyDropRate > 0 && fn.rng.Float64() < fn.replyDropRate {
		fn.dropCount++
		return false
	}
	return true
}

// mustRoundTrip serializes and deserializes to sever pointer sharing between caller and callee, exactly as a real transport would.
func mustRoundTrip(src, dst any) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		panic(err)
	}
	if err := gob.NewDecoder(&buf).Decode(dst); err != nil {
		panic(err)
	}
}

// cluster, the thing tests actually construct.
type cluster struct {
	t     *testing.T
	net   *fakeNetwork
	nodes []*Node
	seed  int64

	// Storage belongs to the SLOT, not the Node: it is what survives a crash
	// and what a restarted Node is built over.
	storage    []*crashableStorage
	baseLogger *slog.Logger
	applyHook  func(node int, msg ApplyMsg)
	applyWG    sync.WaitGroup

	mu   sync.Mutex   //guards dead and nodes
	dead map[int]bool //ids killed via kill()
}

// newCluster builds n fully-connected in-memory nodes.
func newCluster(t *testing.T, n int, seed int64) *cluster {
	t.Helper()
	t.Logf("cluster: n=%d seed=%d", n, seed)

	net := newFakeNetwork(seed)
	c := &cluster{t: t, net: net, seed: seed, dead: make(map[int]bool)}

	t.Cleanup(c.stop)

	base := newTestLogger(t, seed)
	c.baseLogger = base

	for i := 0; i < n; i++ {
		var peers []int
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}

		store := newCrashableStorage()
		c.storage = append(c.storage, store)

		node, err := OpenNode(i, peers, net.endpoint(i), seed*1_000_003+int64(i), store)
		if err != nil {
			t.Fatalf("building node %d: %v", i, err)
		}
		node.SetLogger(base)
		net.register(node)
		c.nodes = append(c.nodes, node)
	}

	return c
}

func (c *cluster) start() {
	c.t.Helper()
	for _, n := range c.nodes {
		n.Start()
	}
	c.t.Cleanup(c.stop)
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

// ---------------------------------------------------------------------------
// Killing nodes
// ---------------------------------------------------------------------------

// kill simulates a crash. B-12 onward.
//
// BOTH halves are necessary, and the second is the one that is easy to miss.
// Stop only closes stopCh, which ends the election ticker and the heartbeat
// loop. It does NOT stop RequestVote and AppendEntries, because the fake
// network calls those handlers directly on the struct -- there is no process to
// exit and no socket to close. A node that is stopped but still reachable goes
// on granting votes forever, so a test that "kills" three of five nodes would
// still see five voters and elect a leader from a minority.
//
// Cutting the network is what makes the node actually absent. Contrast with
// B-14, where a node is partitioned but very much alive.
//
// heal() undoes the second half. Any test that heals after a kill must re-cut
// the dead; see healAroundTheDead in leaderfailure_test.go.

func (c *cluster) kill(id int) {
	c.t.Helper()

	c.nodes[id].Stop()
	c.net.disconnect(id)

	c.mu.Lock()
	c.dead[id] = true
	c.mu.Unlock()
}

func (c *cluster) isDead(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead[id]
}

// alive returns the ids still running.
func (c *cluster) alive() []int {
	var ids []int
	for _, n := range c.nodes {
		if !c.isDead(n.id) {
			ids = append(ids, n.id)
		}
	}
	return ids
}

// leadersByTerm groups every node that believes it leads by the term it thinks it leads in.
func (c *cluster) leadersByTerm() map[int][]int {
	byTerm := make(map[int][]int)
	for _, n := range c.nodes {
		if c.isDead(n.id) {
			continue
		}
		if state, term, _ := n.snapshotState(); state == Leader {
			byTerm[term] = append(byTerm[term], n.id)
		}
	}
	return byTerm
}

// candidatesByTerm groups every live node currently campaigning by the term it is campaigning in.
func (c *cluster) candidatesByTerm() map[int][]int {
	byTerm := make(map[int][]int)
	for _, n := range c.nodes {
		if c.isDead(n.id) {
			continue
		}
		if state, term, _ := n.snapshotState(); state == Candidate {
			byTerm[term] = append(byTerm[term], n.id)
		}
	}
	return byTerm
}

// maxTermAmong reports the highest term seen in a group.
func (c *cluster) maxTermAmong(ids []int) int {
	highest := -1
	for _, id := range ids {
		if _, term, _ := c.nodes[id].snapshotState(); term > highest {
			highest = term
		}
	}
	return highest
}

// waitForSplitVote polls until at least atLeast live nodes are campaigning in the SAME term, and returns that term with the ids involved.
func (c *cluster) waitForSplitVote(atLeast int, within time.Duration) (int, []int) {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for term, ids := range c.candidatesByTerm() {
			if len(ids) >= atLeast {
				return term, ids
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return None, nil
}

// checkSingleLeader returns the id of the leader in the newest term, or None if no node currently believes it leads.
func (c *cluster) checkSingleLeader() int {
	c.t.Helper()

	byTerm := c.leadersByTerm()

	newestTerm := -1
	leader := None

	for term, ids := range byTerm {
		if len(ids) > 1 {
			c.t.Fatalf("ELECTION SAFETY VIOLATED: term %d has %d leaders %v (seed %d)",
				term, len(ids), ids, c.seed)
		}
		if term > newestTerm {
			newestTerm = term
			leader = ids[0]
		}
	}

	return leader
}

// waitForSingleLeader polls until exactly one leader exists, or the bound expires.
func (c *cluster) waitForSingleLeader(within time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if leader := c.checkSingleLeader(); leader != None {
			return leader
		}
		time.Sleep(3 * time.Millisecond)
	}
	return None
}

// leadersAmong is leadersByTerm restricted to one side of a partition.
func (c *cluster) leadersAmong(ids []int) map[int][]int {
	byTerm := make(map[int][]int)
	for _, id := range ids {
		if c.isDead(id) {
			continue
		}
		if state, term, _ := c.nodes[id].snapshotState(); state == Leader {
			byTerm[term] = append(byTerm[term], id)
		}
	}
	return byTerm
}

// waitForStableCluster polls until exactly one leader exists AND every other node has settled into follower.
// Returns the leader's id, or None if the cluster did not settle in time.
func (c *cluster) waitForStableCluster(within time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		leader := c.checkSingleLeader()
		if leader != None && c.allOthersAreFollowers(leader) {
			return leader
		}
		time.Sleep(3 * time.Millisecond)
	}
	return None
}

// waitForNewLeader polls for a leader that is neither the old one nor leading in a term at or below the old one.
func (c *cluster) waitForNewLeader(oldLeader, oldTerm int, within time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if leader := c.checkSingleLeader(); leader != None && leader != oldLeader {
			if _, term, _ := c.nodes[leader].snapshotState(); term > oldTerm {
				return leader
			}
		}
		time.Sleep(3 * time.Millisecond)
	}
	return None
}

// allOthersAreFollowers reports whether every node except the leader has stood down.
func (c *cluster) allOthersAreFollowers(leader int) bool {
	for _, n := range c.nodes {
		if n.id == leader || c.isDead(n.id) {
			continue
		}
		if state, _, _ := n.snapshotState(); state != Follower {
			return false
		}
	}
	return true
}

// othersThan returns every live node id except the given one, in order.
func (c *cluster) othersThan(id int) []int {
	var ids []int
	for _, n := range c.nodes {
		if n.id != id && !c.isDead(n.id) {
			ids = append(ids, n.id)
		}
	}
	return ids
}

// describe renders every node's state, for failure messages.
func (c *cluster) describe() string {
	var b strings.Builder
	for i, n := range c.nodes {
		state, term, votedFor := n.snapshotState()
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "node %d: %v term=%d votedFor=%d", n.id, state, term, votedFor)
		if c.isDead(n.id) {
			b.WriteString(" (KILLED)")
		}
	}
	return b.String()
}

// alignElectionDeadlines forces the given nodes to time out at the same instant.
func (c *cluster) alignElectionDeadlines(at time.Time, ids ...int) {
	c.t.Helper()

	for _, id := range ids {
		n := c.nodes[id]
		n.mu.Lock()
		n.electionDeadline = at
		n.mu.Unlock()
	}
}

// waitForLeaderAmong polls until some node in ids leads in a term of at least minTerm.
func (c *cluster) waitForLeaderAmong(ids []int, minTerm int, within time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for term, leaders := range c.leadersAmong(ids) {
			if len(leaders) > 1 {
				c.t.Fatalf("ELECTION SAFETY VIOLATED: term %d has %d leaders %v (seed %d)",
					term, len(leaders), leaders, c.seed)
			}
			if term >= minTerm {
				return leaders[0]
			}
		}
		time.Sleep(3 * time.Millisecond)
	}
	return None
}

// assertNoLeaderAmong watches a group for the whole window and fails if any member ever leads in a term above aboveTerm.
func (c *cluster) assertNoLeaderAmong(ids []int, aboveTerm int, during time.Duration) {
	c.t.Helper()

	deadline := time.Now().Add(during)
	for time.Now().Before(deadline) {
		for term, leaders := range c.leadersAmong(ids) {
			if term > aboveTerm {
				c.t.Fatalf("MINORITY ELECTED A LEADER: nodes %v lead term %d, above the "+
					"pre-partition term %d, from a group of %d in a cluster of %d (seed %d): %s",
					leaders, term, aboveTerm, len(ids), len(c.nodes), c.seed, c.describe())
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// addNode builds a server that is not yet in anyone's configuration and
// registers it with the network. It starts with an empty configuration, so it
// will not campaign; it becomes a voter when the leader's configuration entry
// reaches it.
func (c *cluster) addNode() int {
	c.t.Helper()

	id := len(c.nodes)
	store := newCrashableStorage()
	c.storage = append(c.storage, store)

	node, err := OpenNode(id, nil, c.net.endpoint(id), c.seed*1_000_003+int64(id), store)
	if err != nil {
		c.t.Fatalf("building node %d: %v", id, err)
	}
	node.SetLogger(c.baseLogger)

	c.mu.Lock()
	c.nodes = append(c.nodes, node)
	c.mu.Unlock()

	c.net.register(node)
	if c.applyHook != nil {
		c.watchNode(node)
	}
	node.mu.Lock()
	node.setConfiguration(nil, 0)
	node.baseServers = nil
	node.mu.Unlock()

	node.Start()
	return id
}
