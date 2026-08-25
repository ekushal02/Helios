package raft

import (
	"testing"
	"time"
)

// splitDelayFloor is the minimum one-way RPC latency imposed while forcing a split vote.

const splitDelayFloor = 50 * time.Millisecond

// alignLead is how far ahead the deadlines are aligned.
//
// Far enough that LEADER STICKINESS has lapsed. A node refuses a poll within
// electionTimeoutMin of hearing from a leader (prevote.go), so aligning inside
// that window produces refusals rather than a simultaneous campaign.
const alignLead = electionTimeoutMin + 60*time.Millisecond

// A split vote is not a bug and not an error path -- it is a legal outcome of two nodes campaigning at once.
// This pins down that it can happen at all, with no timing involved: two candidates in the same term, each having voted for itself, and neither will vote for the other.
func TestTwoCandidatesRejectEachOther(t *testing.T) {
	c := newCluster(t, 2, 11) // deliberately not started: no goroutines, no timers

	const term = 5
	forceCandidate(c.nodes[0], term)
	forceCandidate(c.nodes[1], term)

	for _, from := range []int{0, 1} {
		to := 1 - from
		args := &RequestVoteArgs{
			Term:         term,
			CandidateID:  from,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
		var reply RequestVoteReply
		c.nodes[to].RequestVote(args, &reply)

		if reply.VoteGranted {
			t.Fatalf("node %d granted its vote to node %d in term %d, having already voted for itself",
				to, from, term)
		}
		if reply.Term != term {
			t.Errorf("node %d replied with term %d, want %d", to, reply.Term, term)
		}
	}

	// Neither can have won: one vote each, majority is two.
	for _, n := range c.nodes {
		if state, _, votedFor := n.snapshotState(); state != Candidate || votedFor != n.id {
			t.Errorf("node %d is %v votedFor=%d, want candidate voting for itself",
				n.id, state, votedFor)
		}
	}
}

// The resolution mechanism, in isolation and without timers.
func TestLaterTermCandidateWinsOverStaleCandidate(t *testing.T) {
	c := newCluster(t, 2, 12)

	const splitTerm = 5
	forceCandidate(c.nodes[0], splitTerm)
	forceCandidate(c.nodes[1], splitTerm)

	// Node 0's timer fires first; it campaigns at splitTerm+1.
	args := &RequestVoteArgs{
		Term:         splitTerm + 1,
		CandidateID:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	var reply RequestVoteReply
	c.nodes[1].RequestVote(args, &reply)

	if !reply.VoteGranted {
		t.Fatalf("stale candidate refused a vote to a candidate in a higher term: "+
			"votedFor is not being cleared when the term advances (reply %+v)", reply)
	}
	if reply.Term != splitTerm+1 {
		t.Errorf("reply.Term = %d, want %d: the voter did not adopt the higher term",
			reply.Term, splitTerm+1)
	}

	state, term, votedFor := c.nodes[1].snapshotState()
	if state != Follower {
		t.Errorf("node 1 is %v, want follower: a candidate must step down on a higher term", state)
	}
	if term != splitTerm+1 || votedFor != 0 {
		t.Errorf("node 1 is term=%d votedFor=%d, want term=%d votedFor=0",
			term, votedFor, splitTerm+1)
	}
}

// Force a real split vote in a running cluster, then assert the cluster gets out of it on its own.
func TestForcedSplitVoteResolves(t *testing.T) {
	trials := 50
	if testing.Short() {
		trials = 5
	}

	forced := 0
	rounds := make([]int, 0, trials)

	for i := 0; i < trials; i++ {
		func() {
			c := newCluster(t, 5, int64(3000+i))
			defer c.stop()
			c.start()

			leader := c.waitForStableCluster(electionBound)
			if leader == None {
				t.Fatalf("trial %d (seed %d): no initial leader within %v: %s",
					i, c.seed, electionBound, c.describe())
			}

			c.net.setDelayRange(splitDelayFloor, splitDelayFloor)
			c.kill(leader)
			c.alignElectionDeadlines(time.Now().Add(alignLead), c.alive()...)

			splitTerm, candidates := c.waitForSplitVote(3, 2*time.Second)
			if splitTerm == None {

				c.net.setDelayRange(0, 0)
				if settled := c.waitForStableCluster(failoverBound); settled == None {
					t.Fatalf("trial %d (seed %d): no split forced AND no leader: %s",
						i, c.seed, c.describe())
				}
				return
			}
			forced++
			t.Logf("trial %d (seed %d): forced %d simultaneous candidates in term %d %v",
				i, c.seed, len(candidates), splitTerm, candidates)

			// Restore normal latency so the retries run at realistic speed.
			c.net.setDelayRange(0, 0)

			settled := c.waitForStableCluster(4 * time.Second)
			if settled == None {
				t.Fatalf("trial %d (seed %d): split vote in term %d never resolved: %s",
					i, c.seed, splitTerm, c.describe())
			}

			_, wonTerm, _ := c.nodes[settled].snapshotState()
			if wonTerm <= splitTerm {
				t.Fatalf("trial %d (seed %d): leader %d claims term %d, but term %d was split "+
					"between %v and cannot have produced a leader",
					i, c.seed, settled, wonTerm, splitTerm, candidates)
			}
			rounds = append(rounds, wonTerm-splitTerm)
		}()
	}

	worst, sum := 0, 0
	for _, r := range rounds {
		sum += r
		if r > worst {
			worst = r
		}
	}
	if len(rounds) > 0 {
		t.Logf("resolved %d forced splits: mean %.2f extra terms, worst %d",
			len(rounds), float64(sum)/float64(len(rounds)), worst)
	}

	// PRE-VOTING ROUGHLY HALVES THE RATE, and the mechanism is worth knowing.
	//
	// Aligning the deadlines used to make every node campaign at the same
	// instant. Now they all begin POLLING at the same instant, and whichever
	// assembles a majority first sends RequestVote at term+1 -- which bumps
	// every other node's currentTerm, so their in-flight runPreVote rounds
	// abort on the `currentTerm != term-1` check and never campaign at all.
	//
	// Measured at 20-25 of 50 against 40-plus before, on the same seeds. That
	// is pre-vote suppressing split votes rather than the harness failing to
	// force them, and the property this test exists for -- that a split
	// RESOLVES -- is still exercised twenty times over.
	//
	// The bar is therefore what it takes to notice the harness breaking
	// outright, not what it took to notice the timing being marginal. The
	// number to read is the count logged above.
	if forced*10 < trials*2 {
		t.Errorf("only forced a simultaneous campaign in %d of %d trials — the latency floor "+
			"(%v) is probably not comfortably above the election ticker's polling interval",
			forced, trials, splitDelayFloor)
	}
}

// The randomisation itself, measured rather than assumed.
func TestElectionTimeoutsAreRandomised(t *testing.T) {
	c := newCluster(t, 2, 3131)

	const eps = 2 * time.Millisecond
	const draws = 200

	distinct := make(map[time.Duration]bool)
	lockstep := 0

	for i := 0; i < draws; i++ {
		a := drawElectionTimeout(c.nodes[0])
		b := drawElectionTimeout(c.nodes[1])

		for id, d := range map[int]time.Duration{0: a, 1: b} {
			if d < electionTimeoutMin-eps || d > electionTimeoutMax+eps {
				t.Fatalf("node %d drew %v, outside [%v, %v]",
					id, d, electionTimeoutMin, electionTimeoutMax)
			}
			distinct[d.Round(time.Millisecond)] = true
		}
		if a == b {
			lockstep++
		}
	}

	if len(distinct) < 50 {
		t.Errorf("only %d distinct timeouts across %d draws: the timeout is barely randomised",
			len(distinct), 2*draws)
	}
	if lockstep > draws/10 {
		t.Errorf("two nodes drew identical timeouts %d times in %d rounds: their seeds are "+
			"not independent, and simultaneous campaigns will keep tying",
			lockstep, draws)
	}
	t.Logf("%d distinct timeouts over %d draws, %d exact collisions", len(distinct), 2*draws, lockstep)
}

// forceCandidate puts a node into the state the election timer would have put it in, without running the timer.
func forceCandidate(n *Node, term int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm = term
	n.votedFor = n.id
	n.state = Candidate
}

// drawElectionTimeout forces one reset and reports the timeout it picked.
func drawElectionTimeout(n *Node) time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()

	start := time.Now()
	n.resetElectionTimer()
	return n.electionDeadline.Sub(start)
}
