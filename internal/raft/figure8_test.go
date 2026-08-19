package raft

import (
	"fmt"
	"testing"
)

// Figure 8, driven step by step across five real nodes.
//
// Every election here runs through the real RequestVote, including the §5.4.1
// up-to-date check. That is not incidental: the reason S5 can overwrite an
// entry that a majority already holds is that the election restriction PERMITS
// it -- S5's term-3 tail out-ranks a term-2 tail. A test that decided the
// elections by hand would assume away the mechanism that makes Figure 8
// dangerous.
//
// Replication is delivered synchronously, one round trip at a time, so the
// narrative is deterministic. Both sides of each exchange are production code:
// buildAppendEntries, AppendEntries, advanceFollower, backOffFollower.

type sim struct {
	nodes []*Node // index 0..4 == S1..S5

	// unsafeCommit swaps in a commit rule with the §5.4.2 term check removed.
	// Used only by the mutation test, to prove the safety monitor can fail.
	unsafeCommit bool

	// commitAfterC is S1's commitIndex at the end of state (c), sampled inside
	// the narrative because that is the only moment it answers the question.
	//
	// Since C-11 a follower adopts the leader's commitIndex, so S1's value at
	// the END of the run belongs to S5: state (d) repairs S1 and hands it
	// LeaderCommit 3. Reading it there measures S5's commit decision, not S1's.
	// Under the real rule this is 0; with §5.4.2 removed it is 2.
	commitAfterC int
}

func newFigure8Sim(t *testing.T) *sim {
	t.Helper()

	// Index 1, term 1, identical on every node -- the shared history all five
	// agree on before anything interesting happens.
	genesis := LogEntry{Term: 1, Command: []byte("e1")}

	s := &sim{}
	for i := 0; i < 5; i++ {
		peers := []int{}
		for p := 0; p < 5; p++ {
			if p != i {
				peers = append(peers, p)
			}
		}

		// A transport that never reaches anyone: the heartbeat loop a leader
		// starts must not replicate behind the narrative's back. Every message
		// in this test is delivered explicitly by deliver().
		n := NewNode(i, peers, newStubTransport(denyAll(1)), int64(i+1))
		t.Cleanup(n.Stop)

		n.mu.Lock()
		n.currentTerm = 1
		n.log = append(n.log, genesis)
		n.mu.Unlock()

		s.nodes = append(s.nodes, n)
	}
	return s
}

func (s *sim) name(i int) string { return fmt.Sprintf("S%d", i+1) }

// watched returns the nodes the safety monitor inspects, named as in the paper.
func (s *sim) watched() map[string]*Node {
	m := map[string]*Node{}
	for i, n := range s.nodes {
		m[s.name(i)] = n
	}
	return m
}

// tryElect runs a real election: the candidate asks the given voters, and wins
// only if the §5.4.1 check passes on enough of them.
func (s *sim) tryElect(t *testing.T, cand int, voters []int, term int) bool {
	t.Helper()

	c := s.nodes[cand]

	c.mu.Lock()
	c.currentTerm = term
	c.state = Candidate
	c.votedFor = c.id
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  c.id,
		LastLogIndex: c.lastLogIndex(),
		LastLogTerm:  c.lastLogTerm(),
	}
	quorum := c.quorumSize()
	c.mu.Unlock()

	votes := 1 // itself
	for _, v := range voters {
		var reply RequestVoteReply
		s.nodes[v].RequestVote(args, &reply)
		if reply.VoteGranted {
			votes++
		}
	}

	if votes < quorum {
		c.mu.Lock()
		c.becomeFollower(term)
		c.mu.Unlock()
		return false
	}

	c.mu.Lock()
	c.becomeLeader() // reinitialises nextIndex and matchIndex, per Figure 2
	c.mu.Unlock()
	return true
}

func (s *sim) elect(t *testing.T, cand int, voters []int, term int) {
	t.Helper()
	if !s.tryElect(t, cand, voters, term) {
		t.Fatalf("%s failed to win term %d", s.name(cand), term)
	}
}

// appendEntry has the leader add one entry in its own current term, as Submit
// would.
func (s *sim) appendEntry(t *testing.T, leader int, command string) {
	t.Helper()

	l := s.nodes[leader]
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.state != Leader {
		t.Fatalf("%s is not leader", s.name(leader))
	}
	l.log = append(l.log, LogEntry{Term: l.currentTerm, Command: []byte(command)})
}

// deliver performs one AppendEntries round trip and feeds the reply back
// through the leader's real handlers.
func (s *sim) deliver(t *testing.T, leader, follower int) bool {
	t.Helper()

	l, f := s.nodes[leader], s.nodes[follower]

	l.mu.Lock()
	if l.state != Leader {
		l.mu.Unlock()
		t.Fatalf("%s is not leader", s.name(leader))
	}
	term := l.currentTerm
	args := l.buildAppendEntries(f.id, term)
	l.mu.Unlock()

	var reply AppendEntriesReply
	f.AppendEntries(args, &reply)

	l.mu.Lock()
	defer l.mu.Unlock()

	l.stepDownIfStale(reply.Term)
	if l.state != Leader || l.currentTerm != term {
		return false
	}

	if reply.Success {
		l.advanceFollower(f.id, args)

		// THE MUTATION. advanceFollower has already run the real rule, which
		// refuses to commit old-term entries. Running the unsafe rule after it
		// can only advance commitIndex further, so this yields exactly what a
		// build without the §5.4.2 check would produce -- without touching
		// production code.
		if s.unsafeCommit {
			unsafeAdvanceCommitIndex(l)
		}
		return true
	}

	l.backOffFollower(f.id, args, &reply)
	return false
}

// bring drives repair until the follower accepts, as a leader's retries would.
func (s *sim) bring(t *testing.T, leader, follower int) {
	t.Helper()
	for i := 0; i < 32; i++ {
		if s.deliver(t, leader, follower) {
			return
		}
	}
	t.Fatalf("%s never converged with %s", s.name(follower), s.name(leader))
}

// unsafeAdvanceCommitIndex is advanceCommitIndex WITHOUT the §5.4.2 term check:
// commit whatever a majority holds. This is the bug the rule exists to prevent,
// reproduced in test code so the safety monitor can be shown to catch it.
//
// Caller must hold mu.
func unsafeAdvanceCommitIndex(n *Node) {
	if n.state != Leader {
		return
	}
	quorum := n.quorumSize()
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		if n.replicaCount(idx) >= quorum {
			n.commitIndex = idx
			return
		}
	}
}

// figure8Narrative plays states (a) through (d), checking the safety monitor
// after every step. Returns the first violation seen, or "" if none.
//
// Shared by both tests so that the correct and the broken run differ in exactly
// one thing: the commit rule.
func figure8Narrative(t *testing.T, s *sim) string {
	t.Helper()

	ledger := newCommitLedger()
	check := func(when string) string {
		return ledger.observe(s.watched(), when)
	}

	// --- (a) S1 leads term 2 and gets index 2 onto S2 only -----------------
	s.elect(t, 0, []int{1, 2}, 2)
	s.appendEntry(t, 0, "e2-term2")
	s.bring(t, 0, 1)

	// Two of five. Not a majority under any rule.
	if got := commitIndexOf(s.nodes[0]); got != 0 {
		t.Fatalf("(a) S1 commitIndex = %d, want 0: index 2 is on two of five", got)
	}
	if v := check("state (a)"); v != "" {
		return v
	}

	// --- (b) S1 crashes; S5 wins term 3 and writes its own index 2 ---------
	//
	// S5 can win because S3 and S4 never saw index 2, so S5's log is not behind
	// theirs. This is the divergence that will still be sitting on S5 two terms
	// later, and the reason Figure 8 is dangerous.
	s.elect(t, 4, []int{2, 3}, 3)
	s.appendEntry(t, 4, "e2-term3")

	if v := check("state (b)"); v != "" {
		return v
	}

	// --- (c) S5 crashes; S1 returns, wins term 4, finishes replicating -----
	//
	// S1 gets its INHERITED term-2 entry onto S2 and S3. Index 2 is now on
	// three of five. Counting says commit. §5.4.2 says no.
	s.elect(t, 0, []int{1, 2}, 4)
	s.bring(t, 0, 1)
	s.bring(t, 0, 2)

	// The load-bearing observation of the whole scenario, taken while S1 is
	// still the leader that made the decision. Recorded rather than asserted:
	// the mutation test runs this same narrative and needs the opposite answer.
	s.commitAfterC = commitIndexOf(s.nodes[0])

	if v := check("state (c)"); v != "" {
		return v
	}

	// --- (d) S1 crashes again; S5 wins term 5 and overwrites index 2 -------
	//
	// The election that does the damage. S5's last entry is term 3; S2's and
	// S3's is term 2. Higher term wins the up-to-date comparison, so §5.4.1
	// grants the votes and S5 takes the term legitimately.
	if !s.tryElect(t, 4, []int{1, 2, 3}, 5) {
		t.Fatal("(d) S5 failed to win term 5; the scenario depends on it winning")
	}

	// S5 appends in its own term and replicates everything, repairing S1, S2
	// and S3 -- which truncates their index 2 and replaces it with term 3.
	s.appendEntry(t, 4, "e3-term5")
	s.bring(t, 4, 1)
	s.bring(t, 4, 2)
	s.bring(t, 4, 0)

	return check("state (d)")
}

// THE TEST.
//
// Under the real rule, nothing S1 did in state (c) was ever committed, so S5
// overwriting index 2 in state (d) destroys nothing anyone was promised.
func TestFigure8NoCommittedEntryIsEverOverwritten(t *testing.T) {
	s := newFigure8Sim(t)

	if v := figure8Narrative(t, s); v != "" {
		t.Error(v)
	}

	// And the specific fact the whole rule turns on.
	// The specific fact the whole rule turns on, sampled in state (c) where S1
	// was the leader deciding. Index 2 sat on three of five nodes; counting
	// alone says commit, §5.4.2 says no because term 2 is not S1's current term.
	if s.commitAfterC != 0 {
		t.Errorf("S1 commitIndex = %d at the end of state (c), want 0: it "+
			"committed an inherited term-2 entry on a majority count", s.commitAfterC)
	}

	// And the other half, which only became observable with C-11: what S1 holds
	// committed at the END of the run is S5's log, adopted as a follower. The
	// entry at index 2 must be S5's term-3 version -- the one that overwrote the
	// term-2 entry S1 never committed.
	s1 := s.nodes[0]
	s1.mu.Lock()
	committed, term2 := s1.commitIndex, s1.log[2].Term
	s1.mu.Unlock()

	if committed != 3 {
		t.Errorf("S1 commitIndex = %d after the run, want 3: state (d) repaired "+
			"it and handed it S5's LeaderCommit", committed)
	}
	if term2 != 3 {
		t.Errorf("S1 log[2].Term = %d, want 3: S5 overwrote index 2, and that "+
			"overwrite is only safe because nobody had committed the term-2 entry",
			term2)
	}
}

// THE PROOF THAT THE TEST ABOVE HAS TEETH.
//
// The same narrative, the same monitor, with one change: the commit rule drops
// the §5.4.2 term check. S1 now commits its inherited term-2 entry in state (c)
// on the strength of a majority, and state (d) overwrites it.
//
// A safety test that passes tells you nothing unless you have seen it fail for
// the right reason. If this test ever stops finding a violation, the monitor
// has gone blind and TestFigure8NoCommittedEntryIsEverOverwritten is no longer
// evidence of anything.
func TestFigure8DetectsAMissing5_4_2Check(t *testing.T) {
	s := newFigure8Sim(t)
	s.unsafeCommit = true

	violation := figure8Narrative(t, s)

	if violation == "" {
		t.Fatal("the safety monitor found nothing while the §5.4.2 check was " +
			"disabled: it cannot be relied on to catch a real regression")
	}
	t.Logf("monitor correctly caught the violation:\n%s", violation)
}

// State (e). The counterfactual: had S1 replicated an entry from its OWN term
// before crashing, index 2 would have been safe -- and S5 could no longer have
// taken term 5 at all.
//
// This is where §5.4.2 and §5.4.1 meet. Committing a current-term entry on a
// majority raises that majority's last-log term, and the election restriction
// then refuses every candidate carrying an older tail. The commit rule is not
// merely cautious; it manufactures the precise condition that makes the entry
// unreachable by any future leader.
func TestFigure8CommittingInTheCurrentTermLocksOutTheRival(t *testing.T) {
	s := newFigure8Sim(t)
	ledger := newCommitLedger()

	// (a) and (b) as before.
	s.elect(t, 0, []int{1, 2}, 2)
	s.appendEntry(t, 0, "e2-term2")
	s.bring(t, 0, 1)

	s.elect(t, 4, []int{2, 3}, 3)
	s.appendEntry(t, 4, "e2-term3")

	// (c), but S1 also appends in term 4 before replicating.
	s.elect(t, 0, []int{1, 2}, 4)
	s.appendEntry(t, 0, "e3-term4")
	s.bring(t, 0, 1)
	s.bring(t, 0, 2)

	if got := commitIndexOf(s.nodes[0]); got != 3 {
		t.Fatalf("S1 commitIndex = %d, want 3: a term-4 entry on a majority is "+
			"committable, and carries index 2 with it", got)
	}
	ledger.mustBeSafe(t, s.watched(), "state (e), after committing")

	// (d) attempted. S2 and S3 now end at term 4; S5 ends at term 3. The
	// up-to-date check refuses, and only S4 -- which has nothing past index 1 --
	// can grant. Two votes of five is not enough.
	if s.tryElect(t, 4, []int{1, 2, 3}, 5) {
		t.Fatal("S5 won term 5 despite a majority holding a higher-term entry: " +
			"the election restriction is not protecting committed data")
	}

	ledger.mustBeSafe(t, s.watched(), "state (e), after S5's failed campaign")

	// The committed entries are still there, on every node that has them.
	for i := range s.nodes {
		n := s.nodes[i]
		n.mu.Lock()
		log := append([]LogEntry(nil), n.log...)
		n.mu.Unlock()

		if len(log) > 2 && log[2].Term == 2 {
			continue // holds the committed version
		}
		if i == 4 {
			continue // S5, still diverged and uncommitted, which is allowed
		}
		if len(log) > 2 {
			t.Errorf("%s holds term %d at index 2, want the committed term 2",
				s.name(i), log[2].Term)
		}
	}
}
