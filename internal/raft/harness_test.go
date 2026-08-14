package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
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

	dropRate float64       //chance a deliverable message is dropped anyway
	maxDelay time.Duration //upper bound on random per-message delay

	rpcCount int //for asserting things like "the minority got no votes"
}

func newFakeNetwork(seed int64) *fakeNetwork {
	return &fakeNetwork{
		seed:      seed,
		rng:       rand.New(rand.NewSource(seed)),
		nodes:     make(map[int]*Node),
		reachable: make(map[int]map[int]bool),
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

func (fn *fakeNetwork) setMaxDelay(d time.Duration) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.maxDelay = d
}

func (fn *fakeNetwork) rpcs() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.rpcCount
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
	target, delay, ok := e.net.route(e.from, to)
	if !ok {
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	//Copy across the wire so sender and receiver never share memory.
	var argsCopy RequestVoteArgs
	mustRoundTrip(args, &argsCopy)

	var replyCopy RequestVoteReply
	target.RequestVote(&argsCopy, &replyCopy)

	if !e.net.deliverable(to, e.from) {
		return false
	}
	mustRoundTrip(&replyCopy, reply)
	return true
}

func (e *endpoint) SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	target, delay, ok := e.net.route(e.from, to)
	if !ok {
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	var argsCopy AppendEntriesArgs
	mustRoundTrip(args, &argsCopy)

	var replyCopy AppendEntriesReply
	target.AppendEntries(&argsCopy, &replyCopy)

	if !e.net.deliverable(to, e.from) {
		return false
	}
	mustRoundTrip(&replyCopy, reply)
	return true
}

// route decides whether a message is delivered and returns the target plus any delay to apply.
func (fn *fakeNetwork) route(from, to int) (*Node, time.Duration, bool) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	fn.rpcCount++

	if !fn.reachable[from][to] {
		return nil, 0, false
	}
	if fn.dropRate > 0 && fn.rng.Float64() < fn.dropRate {
		return nil, 0, false
	}

	var delay time.Duration
	if fn.maxDelay > 0 {
		delay = time.Duration(fn.rng.Int63n(int64(fn.maxDelay)))
	}
	return fn.nodes[to], delay, true
}

// deliverable re-checks the return path, since the network may have been cut while the handler was running.
func (fn *fakeNetwork) deliverable(from, to int) bool {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.reachable[from][to]
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
}

// newCluster builds n fully-connected in-memory nodes.
func newCluster(t *testing.T, n int, seed int64) *cluster {
	t.Helper()
	t.Logf("cluster: n=%d seed=%d", n, seed)

	net := newFakeNetwork(seed)
	c := &cluster{t: t, net: net, seed: seed}

	for i := 0; i < n; i++ {
		var peers []int
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		node := NewNode(i, peers, net.endpoint(i), seed+int64(i))
		net.register(node)
		c.nodes = append(c.nodes, node)
	}

	return c
}

// checkSingleLeader returns the id of the leader, or -1 if there isn't exactly one legitimate leader.
//
// TODO (yours): implement this. Walk every node, read state and currentTerm
// under that node's own lock, and count leaders.
//
// The trap: counting nodes in Leader state and asserting the count is 1 will
// give you a flaky test. Raft permits two nodes to believe they lead at the same
// moment, as long as they are in DIFFERENT terms -- a partitioned old leader has
// not yet learned it was deposed. What is forbidden is two leaders in the SAME
// term. Group by term before you assert. B-12 and B-14 will hand you exactly
// this situation.
func (c *cluster) checkSingleLeader() int {
	c.t.Helper()
	return -1
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
