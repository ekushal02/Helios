package raft

import (
	"testing"
	"time"
)

// =============================================================================
// What a partition costs, now that pre-voting exists
// =============================================================================
//
// These three tests used to assert TERM INFLATION: a stranded group campaigns,
// campaigns again, and climbs. That was correct behaviour for Figure 2 as
// written, and safe -- a minority can never win -- but the terms it burned were
// charged to the whole cluster on healing.
//
// Pre-voting removes it, so every assertion here is inverted. A stranded node
// polls, cannot assemble a majority of grants, and never increments. The
// property is no longer "it campaigns and loses" but "it never gets to
// campaign", which is strictly stronger and much easier to state.
//
// prevote_test.go measures the difference directly, with and without, on the
// same seed. This file checks the consequence in the shapes a real cluster
// takes: leader stranded with a follower, leader safe with the majority, and
// the moment of healing.

// partitionWindow is how long a group is watched before the test concludes it
// cannot elect.
const partitionWindow = 1500 * time.Millisecond

// islandSettle is a few heartbeat intervals -- long enough for a follower to
// have timed out if it were going to.
const islandSettle = 400 * time.Millisecond

// Five nodes split 2/3 with the CURRENT LEADER in the minority.
//
// Three of five is required and only two are reachable, so the minority must
// never produce a leader. With pre-voting it goes further: the minority never
// produces a CANDIDATE either.
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

			// The stranded pair is frozen: the old leader still believes it
			// leads and keeps heartbeating, so its follower's timer never even
			// expires.
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

			// NOW TAKE THE LEADER AWAY, so the stranded follower is genuinely
			// leaderless and its timer really does fire. This is the case
			// pre-voting has to answer: it polls, reaches only itself out of a
			// quorum of three, and stops there.
			c.kill(leader)

			c.assertNoLeaderAmong(minority, termBefore, partitionWindow)

			if got := c.maxTermAmong([]int{stranded}); got != termBefore {
				t.Errorf("trial %d (seed %d): stranded node %d moved from term %d to %d "+
					"with nobody to poll: it campaigned without winning a pre-vote, "+
					"which is the disruption pre-voting exists to remove: %s",
					i, c.seed, stranded, termBefore, got, c.describe())
			}
			t.Logf("trial %d: stranded node held term %d and never campaigned; "+
				"majority leader %d at term %d",
				i, termBefore, replacement, c.maxTermAmong(majority))
		}()
	}
}

// The mirror image: the leader stays with the majority, and two followers are
// stranded together with no leader between them.
//
// This is the shape that used to inflate terms fastest -- two nodes with expired
// timers and nothing to hear from. With pre-voting they poll each other, each
// grants the other, and two grants is not three.
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

	// THE INVERSION. This used to require the minority to climb; now it must
	// not move at all.
	if got := c.maxTermAmong(minority); got != termBefore {
		t.Errorf("minority %v reached term %d from %d: two nodes polling each other "+
			"is two grants against a quorum of three, so neither may campaign",
			minority, got, termBefore)
	}
	t.Logf("minority %v held term %d for %v without campaigning once",
		minority, termBefore, partitionWindow)
}

// Healing, which is where the cost of pointless campaigning used to be paid.
//
// The old version of this test asserted that the returning nodes forced the
// cluster to catch up to their inflated term, and its comment ended "removing
// it is the point of pre-vote (D-12)". That is now done, so the test asserts
// the absence: the same leader, at the same term, with nothing burned.
func TestPartitionHealsAndClusterReconverges(t *testing.T) {
	c := newCluster(t, 5, 777)
	c.start()

	leader := c.waitForStableCluster(electionBound)
	if leader == None {
		t.Fatalf("no initial leader within %v (seed %d): %s", electionBound, c.seed, c.describe())
	}
	_, workingTerm, _ := c.nodes[leader].snapshotState()

	others := c.othersThan(leader)
	stranded := others[:2]
	working := append([]int{leader}, others[2:]...)
	c.net.partition(stranded, working)

	time.Sleep(partitionWindow) // the stranded pair polls, and polls

	strandedTerm := c.maxTermAmong(stranded)
	if strandedTerm != workingTerm {
		t.Errorf("stranded nodes %v reached term %d against the working side's %d: "+
			"they incremented without winning a poll (seed %d): %s",
			stranded, strandedTerm, workingTerm, c.seed, c.describe())
	}
	t.Logf("at heal: working leader %d at term %d, stranded %v at term %d",
		leader, workingTerm, stranded, strandedTerm)

	c.net.heal()

	settled := c.waitForStableCluster(2 * failoverBound)
	if settled == None {
		t.Fatalf("cluster did not reconverge within %v after healing (seed %d): %s",
			2*failoverBound, c.seed, c.describe())
	}

	_, finalTerm, _ := c.nodes[settled].snapshotState()

	// THE PAYOFF. Nothing about the rejoin should have been visible to the
	// working side: same leader, same term, no election.
	if settled != leader || finalTerm != workingTerm {
		t.Errorf("leadership went from %d@%d to %d@%d across a heal: two nodes with "+
			"nothing to offer still cost the cluster an election",
			leader, workingTerm, settled, finalTerm)
	}
	t.Logf("reconverged on leader %d at term %d, %d terms burned by the rejoin",
		settled, finalTerm, finalTerm-workingTerm)
}
