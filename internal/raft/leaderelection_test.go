package raft

import (
	"sort"
	"testing"
	"time"
)

// electionBound is how long a healthy cluster gets to settle on a leader.
const electionBound = 3 * time.Second

// convergeBound is how long the losers get to hear about the winner.
const convergeBound = 2 * time.Second

// 100 independent three-node clusters, each with its own seed so the election timeouts -- and therefore the ordering of events -- differ every run.
func TestSingleLeaderElected(t *testing.T) {
	const trials = 100

	elapsed := make([]time.Duration, 0, trials)

	for i := 0; i < trials; i++ {
		// A closure so each cluster is torn down before the next starts.
		func() {
			c := newCluster(t, 3, int64(i))
			defer c.stop()

			start := time.Now()
			c.start()

			leader := c.waitForSingleLeader(electionBound)
			if leader == None {
				t.Fatalf("trial %d (seed %d): no leader within %v", i, c.seed, electionBound)
			}
			elapsed = append(elapsed, time.Since(start))

			if settled := c.waitForStableCluster(convergeBound); settled == None {
				t.Fatalf("trial %d (seed %d): cluster did not settle within %v; %s",
					i, c.seed, convergeBound, c.describe())
			}
		}()
	}

	// A rough shape of the timings. B-15 turns this into a real distribution.
	sort.Slice(elapsed, func(a, b int) bool { return elapsed[a] < elapsed[b] })
	t.Logf("election time over %d trials: min=%v median=%v p95=%v max=%v",
		trials,
		elapsed[0],
		elapsed[len(elapsed)/2],
		elapsed[len(elapsed)*95/100],
		elapsed[len(elapsed)-1])
}

// The elected leader must HOLD leadership, not just reach it.
func TestElectedLeaderIsStable(t *testing.T) {
	c := newCluster(t, 3, 42)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("cluster did not settle within %v (seed %d): %s",
			electionBound, c.seed, c.describe())
	}
	_, termAtWin, _ := c.nodes[leader].snapshotState()

	// Sample repeatedly rather than sleeping once: checkSingleLeader catches a
	// same-term double leader at any instant, not just at the end.
	deadline := time.Now().Add(3 * electionTimeoutMax)
	for time.Now().Before(deadline) {
		if got := c.checkSingleLeader(); got != leader {
			t.Fatalf("leadership moved from %d to %d while healthy (seed %d)",
				leader, got, c.seed)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if _, term, _ := c.nodes[leader].snapshotState(); term != termAtWin {
		t.Errorf("term advanced from %d to %d: an election happened despite "+
			"a healthy leader", termAtWin, term)
	}
}

// A five-node cluster elects exactly one leader too.
func TestSingleLeaderElectedFiveNodes(t *testing.T) {
	const trials = 30

	for i := 0; i < trials; i++ {
		func() {
			c := newCluster(t, 5, int64(1000+i))
			defer c.stop()
			c.start()

			if leader := c.waitForStableCluster(electionBound); leader == None {
				t.Fatalf("trial %d (seed %d): cluster did not settle within %v: %s",
					i, c.seed, electionBound, c.describe())
			}
		}()
	}
}

// A single node is its own majority and leads immediately, without sending anything.
func TestSingleNodeClusterElectsItself(t *testing.T) {
	c := newCluster(t, 1, 7)
	c.start()

	if leader := c.waitForSingleLeader(electionBound); leader != 0 {
		t.Fatalf("single-node cluster leader = %d, want 0", leader)
	}
}
