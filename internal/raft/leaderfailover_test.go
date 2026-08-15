package raft

import (
	"runtime"
	"sort"
	"testing"
	"time"
)

// failoverBound is how long the cluster gets to replace a leader that has died.
const failoverBound = 4 * time.Second

// Five nodes, elect a leader, kill it, assert a different node takes over in a later term.
// 100 independent clusters, each with its own seed, so the timeouts
// -- and therefore which follower notices first and who campaigns against whom -- differ every run.
func TestNewLeaderElectedAfterLeaderKilled(t *testing.T) {
	trials := 100
	if testing.Short() {
		trials = 10
	}

	failover := make([]time.Duration, 0, trials)

	for i := 0; i < trials; i++ {
		// A closure so each cluster is torn down before the next starts.
		func() {
			c := newCluster(t, 5, int64(2000+i))
			defer c.stop()
			c.start()

			oldLeader := c.waitForStableCluster(electionBound)
			if oldLeader == None {
				t.Fatalf("trial %d (seed %d): no initial leader within %v: %s",
					i, c.seed, electionBound, c.describe())
			}
			_, oldTerm, _ := c.nodes[oldLeader].snapshotState()

			killedAt := time.Now()
			c.kill(oldLeader)

			newLeader := c.waitForNewLeader(oldLeader, oldTerm, failoverBound)
			if newLeader == None {
				t.Fatalf("trial %d (seed %d): killed leader %d at term %d, no replacement within %v: %s",
					i, c.seed, oldLeader, oldTerm, failoverBound, c.describe())
			}
			failover = append(failover, time.Since(killedAt))

			// The four survivors settle: one leader, three followers.
			settled := c.waitForStableCluster(convergeBound)
			if settled == None {
				t.Fatalf("trial %d (seed %d): survivors did not settle within %v: %s",
					i, c.seed, convergeBound, c.describe())
			}
			if settled == oldLeader {
				// Unreachable while leadersByTerm filters killed nodes.
				t.Fatalf("trial %d (seed %d): killed node %d is leading", i, c.seed, oldLeader)
			}
		}()
	}

	sort.Slice(failover, func(a, b int) bool { return failover[a] < failover[b] })
	t.Logf("failover time over %d trials: min=%v median=%v p95=%v max=%v",
		trials,
		failover[0],
		failover[len(failover)/2],
		failover[len(failover)*95/100],
		failover[len(failover)-1])
}

// Electing a replacement is not enough -- the rest of the cluster has to accept it.
func TestSurvivorsConvergeAfterFailover(t *testing.T) {
	c := newCluster(t, 5, 4242)
	c.start()

	oldLeader := c.waitForStableCluster(electionBound)
	if oldLeader == None {
		t.Fatalf("no initial leader within %v (seed %d): %s",
			electionBound, c.seed, c.describe())
	}
	_, oldTerm, _ := c.nodes[oldLeader].snapshotState()

	c.kill(oldLeader)

	newLeader := c.waitForNewLeader(oldLeader, oldTerm, failoverBound)
	if newLeader == None {
		t.Fatalf("no replacement for killed leader %d within %v (seed %d): %s",
			oldLeader, failoverBound, c.seed, c.describe())
	}
	_, newTerm, _ := c.nodes[newLeader].snapshotState()

	if newTerm <= oldTerm {
		t.Fatalf("new leader %d is at term %d, not above the old term %d",
			newLeader, newTerm, oldTerm)
	}

	deadline := time.Now().Add(convergeBound)
	for {
		lagging := None
		for _, id := range c.alive() {
			if _, term, _ := c.nodes[id].snapshotState(); term < newTerm {
				lagging = id
				break
			}
		}
		if lagging == None {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d never reached term %d within %v (seed %d): %s",
				lagging, newTerm, convergeBound, c.seed, c.describe())
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// Kill leaders until only a minority survives, then assert nothing gets elected.
func TestMinorityCannotElect(t *testing.T) {
	c := newCluster(t, 5, 99)
	c.start()

	for round := 0; round < 3; round++ {
		leader := c.waitForStableCluster(failoverBound)
		if leader == None {
			t.Fatalf("round %d: %d nodes alive, no leader within %v (seed %d): %s",
				round, len(c.alive()), failoverBound, c.seed, c.describe())
		}
		t.Logf("round %d: killing leader %d (%d alive before kill)",
			round, leader, len(c.alive()))
		c.kill(leader)
	}

	// Two of five remain. They will campaign forever, terms climbing steadily,
	// and that churn is exactly what pre-vote (D-12) exists to stop.
	if leader := c.waitForSingleLeader(2 * time.Second); leader != None {
		t.Fatalf("minority of 2/5 elected leader %d: majority is being computed over "+
			"live nodes rather than the cluster (seed %d): %s",
			leader, c.seed, c.describe())
	}
}

// 100 trials means 500 nodes started and stopped.
func TestClustersDoNotLeakGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}

	baseline := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		func() {
			c := newCluster(t, 5, int64(7000+i))
			defer c.stop()
			c.start()

			leader := c.waitForStableCluster(electionBound)
			if leader == None {
				return
			}
			_, term, _ := c.nodes[leader].snapshotState()
			c.kill(leader)
			c.waitForNewLeader(leader, term, failoverBound)
		}()
	}

	// Loops exit asynchronously, so poll rather than checking once.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines: %d, baseline %d — Stop is not shutting the loops down",
		runtime.NumGoroutine(), baseline)
}
