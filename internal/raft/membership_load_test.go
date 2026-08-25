package raft

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// What "no downtime" has to mean to be testable
// =============================================================================
//
// Not "Submit kept returning true" -- Submit accepts the moment an entry is in
// the leader's log, which it will do happily while nothing is committing. Not
// "no writes were lost" either; Raft would not lose them during a stall, it
// would simply stop finishing them.
//
// The claim is about COMMITS, so the measurement is the wall-clock gap between
// consecutive entries reaching the state machine. A membership change that
// pauses agreement shows up as one long gap and nothing else does.
//
// WHY THIS CAN PASS AT ALL WITHOUT A CATCH-UP PHASE, and it is tighter than it
// looks. Growing three to four, a majority of the new configuration is three,
// and the three original members are exactly three -- they can commit alone
// while the joiner is still empty, with no margin whatsoever. Growing four to
// five, a majority is three against four healthy originals, which is
// comfortable.
//
// So no downtime holds only while every original member is healthy. Lose one
// during the three-to-four step and the quorum now requires the joiner, and
// commits wait for it to catch up. That is the window §6's non-voting phase
// removes and that AddServer's fence names; the second test measures it rather
// than describing it.

// =============================================================================
// Watching the write stream
// =============================================================================

// writeTracker turns the apply stream of one node into the numbers that decide
// whether the cluster stopped: per-write latency, and the gap between
// consecutive applies.
//
// It watches ONE node -- the leader -- because a follower's stream trails by at
// least a message and a gap there says nothing about whether agreement paused.
// Every node's applies still arrive here; the rest are used only to see how far
// each has got.
type writeTracker struct {
	mu sync.Mutex

	focus     int // whose stream the latency and gap figures come from
	watching  bool
	submitted map[int]time.Time

	latencies []time.Duration
	high      map[int]int

	lastApply time.Time
	maxGap    time.Duration
	maxGapAt  int

	applied int
}

func newWriteTracker() *writeTracker {
	return &writeTracker{
		focus:     None,
		submitted: make(map[int]time.Time),
		high:      make(map[int]int),
	}
}

// watch names the node whose stream the timings come from. Called once the
// leader is known.
//
// It does NOT start the gap clock. The first apply does. The interval between
// watching and the first delivery is startup -- the load generator has not
// submitted anything yet -- and counting it produced a reported "worst gap"
// that was sometimes just the test getting going, at index 1, with nothing to
// do with a membership change.
func (w *writeTracker) watch(node int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.focus = node
	w.watching = true
	w.lastApply = time.Time{}
}

func (w *writeTracker) noteSubmit(index int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.submitted[index] = time.Now()
}

func (w *writeTracker) record(node int, msg ApplyMsg) {
	now := time.Now()

	w.mu.Lock()
	defer w.mu.Unlock()

	idx := msg.CommandIndex
	if msg.SnapshotValid {
		idx = msg.SnapshotIndex
	}
	if idx > w.high[node] {
		w.high[node] = idx
	}

	if !w.watching || node != w.focus {
		return
	}

	// EVERY apply counts toward the gap, commands and configuration entries
	// alike. What is being measured is whether agreement kept moving, not what
	// it happened to be agreeing about.
	//
	// A gap needs two applies to exist, so the first one only starts the clock.
	if !w.lastApply.IsZero() {
		if gap := now.Sub(w.lastApply); gap > w.maxGap {
			w.maxGap, w.maxGapAt = gap, idx
		}
	}
	w.lastApply = now

	if msg.CommandValid {
		w.applied++
		if at, ok := w.submitted[idx]; ok {
			w.latencies = append(w.latencies, now.Sub(at))
		}
	}
}

func (w *writeTracker) highFor(node int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.high[node]
}

func (w *writeTracker) waitFor(node, index int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if w.highFor(node) >= index {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// stats returns the figures the tests assert on, latency percentiles included
// so a marginal failure is diagnosable rather than just red.
func (w *writeTracker) stats() (applied, timed int, p50, p99, maxLatency, maxGap time.Duration, maxGapAt int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	sorted := append([]time.Duration(nil), w.latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	at := func(q float64) time.Duration {
		if len(sorted) == 0 {
			return 0
		}
		i := int(float64(len(sorted)) * q)
		if i >= len(sorted) {
			i = len(sorted) - 1
		}
		return sorted[i]
	}

	if len(sorted) > 0 {
		maxLatency = sorted[len(sorted)-1]
	}
	return w.applied, len(sorted), at(0.50), at(0.99), maxLatency, w.maxGap, w.maxGapAt
}

// =============================================================================
// A client that never stops
// =============================================================================

// loadGenerator submits to one node at a fixed rate and records the index of
// everything it got accepted.
//
// It submits to the LEADER DIRECTLY rather than through submitToLeader, because
// the index is needed to time the write and only the accepting node knows it.
// A refusal therefore means this node stopped leading, which is worth counting:
// a leadership change during a membership change is not downtime, but it does
// mean the figures came from two different streams and should not be trusted.
type loadGenerator struct {
	refused int
	sent    int
}

func (g *loadGenerator) run(n *Node, w *writeTracker, every time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		default:
		}

		idx, _, ok := n.Submit([]byte(fmt.Sprintf("load%06d=%d", i, i)))
		if ok {
			w.noteSubmit(idx)
			g.sent++
		} else {
			g.refused++
		}
		time.Sleep(every)
	}
}

// =============================================================================
// Three to five, everyone healthy
// =============================================================================

// maxAcceptableGap is the line between "kept going" and "stopped".
//
// Generous on purpose. A cluster that genuinely waits for a joiner to catch up
// pauses for as long as the transfer takes, which is hundreds of milliseconds
// upward and grows with the log; a healthy one commits every few milliseconds.
// Half a second separates those cleanly without turning a scheduler hiccup
// under -race into a failure. The number worth reading is the logged maximum,
// not this bound.
const maxAcceptableGap = 500 * time.Millisecond

func TestAClusterGrowsFromThreeToFiveWithoutStopping(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster under continuous load")
	}

	const seed = 20260907

	c := newCluster(t, 3, seed)

	// A plausible link. With instant delivery a stall would be the only thing
	// with any duration at all, and the comparison would prove nothing.
	c.net.setDelayRange(time.Millisecond, 3*time.Millisecond)

	tracker := newWriteTracker()
	c.watchApplies(tracker.record)
	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}
	leaderNode := c.nodes[leader]

	tracker.watch(leader)

	stop := make(chan struct{})
	done := make(chan struct{})
	gen := &loadGenerator{}
	go gen.run(leaderNode, tracker, 2*time.Millisecond, stop, done)

	// A baseline, so the gap figure has something to be compared against that
	// is not itself part of a membership change.
	time.Sleep(300 * time.Millisecond)

	// ---- three to four ----------------------------------------------------
	//
	// The tightest step: a majority of four is three, and the three originals
	// are exactly three. They carry the cluster alone while the joiner is
	// empty, with nothing to spare.
	first := c.addNode()
	joinStart := time.Now()

	c4, _, err := leaderNode.AddServer(first)
	if err != nil {
		t.Fatalf("AddServer(%d): %v", first, err)
	}
	if !tracker.waitFor(leader, c4, 10*time.Second) {
		t.Fatalf("the configuration entry at %d never committed: %s", c4, c.describe())
	}
	if !tracker.waitFor(first, c4, 20*time.Second) {
		t.Fatalf("node %d never caught up to %d: %s", first, c4, c.describe())
	}
	firstCatchUp := time.Since(joinStart)

	// ---- four to five -----------------------------------------------------
	second := c.addNode()
	joinStart = time.Now()

	c5, _, err := leaderNode.AddServer(second)
	if err != nil {
		t.Fatalf("AddServer(%d): %v", second, err)
	}
	if !tracker.waitFor(leader, c5, 10*time.Second) {
		t.Fatalf("the configuration entry at %d never committed: %s", c5, c.describe())
	}
	if !tracker.waitFor(second, c5, 20*time.Second) {
		t.Fatalf("node %d never caught up to %d: %s", second, c5, c.describe())
	}
	secondCatchUp := time.Since(joinStart)

	// Keep writing past the last change, so the final stretch is measured under
	// the new configuration rather than ending on it.
	time.Sleep(300 * time.Millisecond)

	close(stop)
	<-done

	// ---- what happened ----------------------------------------------------
	applied, timed, p50, p99, maxLatency, maxGap, maxGapAt := tracker.stats()

	t.Logf("grew 3 -> 4 at index %d, 4 -> 5 at index %d", c4, c5)
	t.Logf("  writes      %d submitted, %d applied, %d timed, %d refused",
		gen.sent, applied, timed, gen.refused)
	t.Logf("  latency     p50 %v, p99 %v, max %v", round(p50), round(p99), round(maxLatency))
	t.Logf("  worst gap   %v, ending at index %d", round(maxGap), maxGapAt)
	t.Logf("  catch-up    node %d in %v, node %d in %v",
		first, round(firstCatchUp), second, round(secondCatchUp))

	// THE CLAIM. Agreement never paused long enough to count as an outage.
	if maxGap > maxAcceptableGap {
		t.Errorf("agreement paused for %v around index %d, which is longer than "+
			"%v: growing the cluster stopped it committing",
			maxGap, maxGapAt, maxAcceptableGap)
	}

	// A leadership change would mean the timings above came from two streams.
	if gen.refused != 0 {
		t.Errorf("the leader refused %d writes: it stopped leading part-way, so "+
			"these figures describe two different leaders", gen.refused)
	}

	// ANTI-VACUITY. If writes had stopped before the changes, every gap
	// assertion above would be about an idle cluster.
	if applied < 200 {
		t.Errorf("only %d writes applied: the load generator was not keeping the "+
			"cluster busy, so nothing here was measured under load", applied)
	}

	// And the cluster really is five servers, everywhere.
	deadline := time.Now().Add(10 * time.Second)
	for {
		agreed := true
		for _, n := range c.nodes {
			if len(n.Configuration()) != 5 {
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
			t.Fatalf("the cluster did not converge on five servers")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if c.waitForStableCluster(5*time.Second) == None {
		t.Errorf("no single leader after growing: %s", c.describe())
	}
}

// =============================================================================
// The same growth, one member short
// =============================================================================

// THE FENCE, MEASURED. AddServer admits a server as a full voter immediately,
// with no non-voting catch-up phase. The test above passes because three
// healthy originals are exactly a majority of four -- remove that margin and
// the quorum needs the joiner, so commits wait for it to be repaired.
//
// This does not assert a bound. It reports how long the pause was and how many
// entries the joiner had to be sent, because the pause is proportional to the
// second and both are what a catch-up phase would remove. On a log of a few
// thousand entries it is milliseconds; on a real one it is however long the
// transfer takes.
//
// When the catch-up phase lands, this test should keep its shape and gain an
// assertion: the pause must stop scaling with the log.
func TestAddingAServerToAWeakenedClusterPausesCommits(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster under load")
	}

	const (
		seed    = 20260908
		backlog = 1500
	)

	c := newCluster(t, 3, seed)
	c.net.setDelayRange(time.Millisecond, 3*time.Millisecond)

	tracker := newWriteTracker()
	c.watchApplies(tracker.record)
	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}
	leaderNode := c.nodes[leader]

	// A log worth catching up on. Without it the joiner is current after one
	// round trip and the pause this test exists to show is invisible.
	for i := 1; i <= backlog; i++ {
		if !c.submitToLeader([]byte(fmt.Sprintf("backlog%05d=%d", i, i))) {
			t.Fatalf("no leader accepted backlog write %d", i)
		}
	}
	if !tracker.waitFor(leader, backlog, 30*time.Second) {
		t.Fatalf("leader applied %d of %d backlog entries", tracker.highFor(leader), backlog)
	}

	// Now the margin is gone: two of the three originals remain, and a majority
	// of four is three.
	victim := c.othersThan(leader)[0]
	c.kill(victim)

	tracker.watch(leader)

	stop := make(chan struct{})
	done := make(chan struct{})
	gen := &loadGenerator{}
	go gen.run(leaderNode, tracker, 2*time.Millisecond, stop, done)

	time.Sleep(200 * time.Millisecond)

	joiner := c.addNode()
	pauseStart := time.Now()

	c4, _, err := leaderNode.AddServer(joiner)
	if err != nil {
		t.Fatalf("AddServer(%d): %v", joiner, err)
	}

	// The configuration entry cannot commit until the joiner can vouch for it,
	// which means it must first be sent every entry it is missing.
	if !tracker.waitFor(leader, c4, 60*time.Second) {
		t.Fatalf("the configuration at %d never committed: %s", c4, c.describe())
	}
	pause := time.Since(pauseStart)

	time.Sleep(200 * time.Millisecond)
	close(stop)
	<-done

	applied, timed, p50, p99, maxLatency, maxGap, maxGapAt := tracker.stats()

	t.Logf("weakened 3 -> 4 with %d entries of backlog, node %d down", backlog, victim)
	t.Logf("  writes      %d submitted, %d applied, %d timed, %d refused",
		gen.sent, applied, timed, gen.refused)
	t.Logf("  latency     p50 %v, p99 %v, max %v", round(p50), round(p99), round(maxLatency))
	t.Logf("  worst gap   %v, ending at index %d", round(maxGap), maxGapAt)
	t.Logf("  AddServer to commit: %v -- the window a catch-up phase removes", round(pause))

	// The point is that it RESUMES, not that it never paused. A cluster that
	// never recovered would be a correctness failure; one that paused is the
	// documented cost of admitting a voter before it is caught up.
	if applied == 0 {
		t.Fatal("nothing committed at all after the join: the cluster did not recover")
	}
	if gen.refused != 0 {
		t.Logf("  the leader refused %d writes, so it lost leadership during the "+
			"pause -- the figures span more than one leader", gen.refused)
	}
	// A SANITY CHECK THAT ACTUALLY HOLDS.
	//
	// The previous version compared the worst gap against p99 latency and
	// required the gap to be larger. There is no such relation. A gap is
	// apply-to-apply; a latency is submit-to-apply. Entries commit in batches,
	// so several arrive back to back with almost no gap between them while each
	// has been waiting since its own submission -- latency above gap is the
	// normal case, not a contradiction.
	//
	// What is worth checking is that submits are being matched to applies at
	// all. If the tracker were keying on the wrong index, latencies would be
	// empty and every percentile above would silently read zero.
	if timed*10 < applied*9 {
		t.Errorf("timed %d of %d applied writes: submits are not being matched to "+
			"their applies, so the latency figures are meaningless", timed, applied)
	}
}
