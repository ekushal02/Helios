package raft

import (
	"testing"
	"time"
)

// partitionWindow is how long a group is watched before the test concludes it cannot elect.
const partitionWindow = 1500 * time.Millisecond

// islandSettle is a few heartbeat intervals -- long enough for a follower to have timed out if it were going to.
const islandSettle = 400 * time.Millisecond

// Five nodes split 2/3 with the CURRENT LEADER in the minority.
// Three of five is required and only two nodes are reachable, so the minority must never produce a leader in a new term.
func TestMinorityPartitionCannotElectWithLeaderInMinority(t *testing.T) {
	trials := 10
	if testing.Short() {
		trials = 2
	}

	for i := 0; i < trials; i++ {
		func() {
			c := newCluster(t, 5, int64(4000+i))
			defer c.stop()
			c.start()

			leader := c.waitForStableCluster(electionBound)
			if leader == None {
				t.Fatalf("trial %d (seed %d): no initial leader within %v: %s",
					i, c.seed, electionBound, c.describe())
			}
			_, termBefore, _ := c.nodes[leader].snapshotState()

			others := c.othersThan(leader)
			stranded := others[0] // the one follower marooned with the old leader
			minority := []int{leader, stranded}
			majority := others[1:]
			c.net.partition(minority, majority)
			t.Logf("trial %d (seed %d): leader %d at term %d, minority %v majority %v",
				i, c.seed, leader, termBefore, minority, majority)

			replacement := c.waitForLeaderAmong(majority, termBefore+1, failoverBound)
			if replacement == None {
				t.Fatalf("trial %d (seed %d): majority %v elected nobody within %v: %s",
					i, c.seed, majority, failoverBound, c.describe())
			}

			time.Sleep(islandSettle)
			if state, term, _ := c.nodes[leader].snapshotState(); state != Leader || term != termBefore {
				t.Fatalf("trial %d (seed %d): stranded leader %d is %v at term %d, want leader "+
					"at term %d — nothing in the minority can have told it otherwise: %s",
					i, c.seed, leader, state, term, termBefore, c.describe())
			}
			if state, term, _ := c.nodes[stranded].snapshotState(); state != Follower || term != termBefore {
				t.Fatalf("trial %d (seed %d): node %d is %v at term %d, want follower at term %d — "+
					"it is still being heartbeated by node %d: %s",
					i, c.seed, stranded, state, term, termBefore, leader, c.describe())
			}

			if !forceCampaign(c, stranded, termBefore, time.Second) {
				t.Fatalf("trial %d (seed %d): could not provoke node %d into campaigning: %s",
					i, c.seed, stranded, c.describe())
			}

			c.assertNoLeaderAmong(minority, termBefore, partitionWindow)

			minorityTerm := c.maxTermAmong(minority)
			if minorityTerm <= termBefore+1 {
				t.Fatalf("trial %d (seed %d): minority %v stalled at term %d over %v — "+
					"it is not retrying, so proving it cannot win proves nothing: %s",
					i, c.seed, minority, minorityTerm, partitionWindow, c.describe())
			}
			t.Logf("trial %d: minority climbed to term %d and elected nobody; majority leader %d at term %d",
				i, minorityTerm, replacement, c.maxTermAmong(majority))
		}()
	}
}

func TestMinorityPartitionCannotElectWithLeaderInMajority(t *testing.T) {
	c := newCluster(t, 5, 4242)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no initial leader within %v (seed %d): %s", electionBound, c.seed, c.describe())
	}
	_, termBefore, _ := c.nodes[leader].snapshotState()

	others := c.othersThan(leader)
	minority := others[:2]
	majority := append([]int{leader}, others[2:]...)
	c.net.partition(minority, majority)
	t.Logf("leader %d at term %d stays with the majority %v; minority %v",
		leader, termBefore, majority, minority)

	c.assertNoLeaderAmong(minority, termBefore, partitionWindow)

	if state, term, _ := c.nodes[leader].snapshotState(); state != Leader || term != termBefore {
		t.Fatalf("leader %d is %v at term %d, want leader at term %d: losing two followers "+
			"cost it the leadership, but it never lost the majority (seed %d): %s",
			leader, state, term, termBefore, c.seed, c.describe())
	}
	if minorityTerm := c.maxTermAmong(minority); minorityTerm <= termBefore {
		t.Errorf("minority %v never advanced past term %d: it is not campaigning",
			minority, termBefore)
	}
}

// Healing the partition, which is where the cost of all that pointless campaigning is finally paid.
//
// This deliberately uses the LEADER-IN-MAJORITY split, because that is the only
// one of the two that inflates terms. Strand two followers with no leader
// between them and they campaign continuously; strand the leader with a follower
// and the pair sits frozen at the old term, ending up BEHIND the working side
// rather than ahead of it.
//
// So: the returning nodes carry terms far above anything the working side ever
// needed, the legitimate leader sees one of them, steps down, and the cluster
// runs an election it had no reason to run. Nothing here is incorrect -- safety
// holds throughout, which is why this asserts convergence rather than failure --
// but the disruption is real, and removing it is the point of pre-vote (D-12).
func TestPartitionHealsAndClusterReconverges(t *testing.T) {
	c := newCluster(t, 5, 777)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no initial leader within %v (seed %d): %s", electionBound, c.seed, c.describe())
	}
	others := c.othersThan(leader)
	stranded := others[:2]
	working := append([]int{leader}, others[2:]...)
	c.net.partition(stranded, working)

	time.Sleep(partitionWindow) // the stranded pair campaigns, and campaigns

	strandedTerm := c.maxTermAmong(stranded)
	_, workingTerm, _ := c.nodes[leader].snapshotState()
	if strandedTerm <= workingTerm {
		t.Fatalf("stranded nodes %v reached term %d against the working side's %d: "+
			"they are not campaigning, so there is no disruption to observe (seed %d): %s",
			stranded, strandedTerm, workingTerm, c.seed, c.describe())
	}
	t.Logf("at heal: working leader %d at term %d, stranded %v at term %d (+%d)",
		leader, workingTerm, stranded, strandedTerm, strandedTerm-workingTerm)

	c.net.heal()

	settled := c.waitForStableCluster(2 * failoverBound)
	if settled == None {
		t.Fatalf("cluster did not reconverge within %v after healing (seed %d): %s",
			2*failoverBound, c.seed, c.describe())
	}

	_, finalTerm, _ := c.nodes[settled].snapshotState()
	if finalTerm < strandedTerm {
		t.Errorf("final term %d is below the stranded nodes' term %d: they rejoined without "+
			"forcing the cluster to catch up", finalTerm, strandedTerm)
	}
	t.Logf("reconverged on leader %d at term %d, %d terms burned by the rejoin",
		settled, finalTerm, finalTerm-workingTerm)
}

// forceCampaign expires a node's election deadline until its term actually moves, and reports whether it did.
func forceCampaign(c *cluster, id, aboveTerm int, within time.Duration) bool {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		c.alignElectionDeadlines(time.Now(), id)
		time.Sleep(15 * time.Millisecond)
		if _, term, _ := c.nodes[id].snapshotState(); term > aboveTerm {
			return true
		}
	}
	return false
}
