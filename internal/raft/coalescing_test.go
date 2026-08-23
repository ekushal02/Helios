package raft

import (
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Why this file exists
// =============================================================================
//
// Two guards in replication.go defend against replies arriving out of order:
// the monotonic matchIndex in advanceFollower, and the current-attempt check in
// backOffFollower. Both used to be exercised by accident -- replicateAll gave
// every trigger its own goroutine, so messages to one peer overtook each other
// whenever the random delays worked out, and the unreliable run counted the
// inversions to prove it was happening.
//
// Coalescing ended that. At most one round is in flight per peer, so a second
// message is not built until the first has returned, and same-peer inversions
// are structurally impossible except across a leadership change. The guards are
// still right -- a real transport that pipelines, or a round left over from a
// deposed term, brings the hazard straight back -- but nothing produces the
// hazard on demand any more.
//
// So they are tested directly here. The reply is handed to the guard rather
// than raced into it, which is both deterministic and a stronger check than the
// old counter: it verifies WHAT the guard does, not merely that inversions
// occurred somewhere in a run that also passed.

// leaderNotSending builds a leader whose heartbeat loop is not running, so the
// test is the only thing touching per-follower state.
//
// becomeLeader would spawn heartbeatLoop, and a recordingTransport answering
// Success in the background would move matchIndex underneath the assertions.
// initLeaderState is the part of becoming leader that only initialises maps.
func leaderNotSending(t *testing.T, term, entries int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm = term
	n.state = Leader
	for i := 1; i <= entries; i++ {
		n.log = append(n.log, LogEntry{Term: term, Command: []byte{byte(i)}})
	}
	n.initLeaderState()

	return n
}

// =============================================================================
// advanceFollower
// =============================================================================

// A reply to an older, shorter message must not drag matchIndex backwards.
//
// matchIndex is the leader's only evidence for a commit. Letting it fall
// un-proves agreement that may already have been counted toward committing an
// entry, and an entry does not become uncommitted because a late packet turned
// up.
func TestALateReplyNeverDragsMatchIndexBackwards(t *testing.T) {
	n := leaderNotSending(t, 5, 5)

	n.mu.Lock()
	defer n.mu.Unlock()

	// Two messages were in flight. The later one carried all five entries; the
	// earlier one carried two. Their replies arrive in the wrong order.
	late := &AppendEntriesArgs{Term: 5, PrevLogIndex: 0, Entries: make([]LogEntry, 5)}
	early := &AppendEntriesArgs{Term: 5, PrevLogIndex: 0, Entries: make([]LogEntry, 2)}

	n.advanceFollower(1, late)
	if got := n.matchIndexFor(1); got != 5 {
		t.Fatalf("matchIndex = %d after a reply proving five entries, want 5", got)
	}

	n.advanceFollower(1, early)

	if got := n.matchIndexFor(1); got != 5 {
		t.Errorf("matchIndex fell to %d on a reply to an older, shorter message: "+
			"replication the leader has already counted toward a commit has been un-proved", got)
	}
	if got := n.nextIndexFor(1); got != 6 {
		t.Errorf("nextIndex = %d, want 6 (one past matchIndex)", got)
	}
}

// The credit a reply earns comes from WHAT WAS SENT, not from the log as it
// stands when the reply lands. The log grows while messages are in flight, and
// crediting a follower with entries it never received is how a leader counts a
// majority for an entry that exists on one machine.
func TestCreditComesFromTheMessageNotTheCurrentLog(t *testing.T) {
	n := leaderNotSending(t, 5, 2)

	sent := &AppendEntriesArgs{Term: 5, PrevLogIndex: 0, Entries: make([]LogEntry, 2)}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Eight more entries arrive after the message left.
	for i := 3; i <= 10; i++ {
		n.log = append(n.log, LogEntry{Term: 5, Command: []byte{byte(i)}})
	}

	n.advanceFollower(1, sent)

	if got := n.matchIndexFor(1); got != 2 {
		t.Errorf("matchIndex = %d, want 2: the follower acknowledged two entries, "+
			"not the %d the leader now holds", got, n.lastLogIndex())
	}
}

// =============================================================================
// backOffFollower
// =============================================================================

// A rejection that answers a superseded attempt must be ignored.
//
// Backoff walks nextIndex down. A rejection from an older, higher-indexed
// message landing after that walk has made progress would push nextIndex back
// to where it started -- repair undoing itself, forever, on a cluster that
// looks healthy.
func TestBackoffIgnoresAReplyToASupersededAttempt(t *testing.T) {
	n := leaderNotSending(t, 5, 5)

	n.mu.Lock()
	defer n.mu.Unlock()

	// Backoff has already walked this follower down to 3.
	n.nextIndex[1] = 3

	// A rejection to the attempt at nextIndex 6, still in flight from before.
	stale := &AppendEntriesArgs{Term: 5, PrevLogIndex: 5}
	n.backOffFollower(1, stale, &AppendEntriesReply{Term: 5, ConflictIndex: 1})

	if got := n.nextIndexFor(1); got != 3 {
		t.Errorf("nextIndex moved to %d on a reply answering attempt %d while the "+
			"current attempt is 3: backoff can undo its own progress and never converge",
			got, stale.PrevLogIndex+1)
	}
}

// The positive control. Without this the test above passes just as well against
// a backOffFollower that ignores every reply and never repairs anything.
func TestBackoffActsOnAReplyToTheCurrentAttempt(t *testing.T) {
	n := leaderNotSending(t, 5, 5)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.nextIndex[1] = 3

	current := &AppendEntriesArgs{Term: 5, PrevLogIndex: 2} // answers attempt 3
	n.backOffFollower(1, current, &AppendEntriesReply{Term: 5, ConflictIndex: 1})

	if got := n.nextIndexFor(1); got == 3 {
		t.Error("nextIndex did not move on a reply that answered the current attempt: " +
			"the guard is rejecting everything and repair cannot make progress")
	}
}

// =============================================================================
// Coalescing itself
// =============================================================================

// gatedTransport holds the first AppendEntries open so a test can pile work up
// behind it, then let it through.
type gatedTransport struct {
	mu     sync.Mutex
	sent   []*AppendEntriesArgs
	opened bool

	gate chan struct{}
	term int
}

func newGatedTransport(term int) *gatedTransport {
	return &gatedTransport{gate: make(chan struct{}), term: term}
}

func (g *gatedTransport) SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	*reply = RequestVoteReply{Term: g.term, VoteGranted: false}
	return true
}

func (g *gatedTransport) SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	g.mu.Lock()
	g.sent = append(g.sent, args) // recorded BEFORE blocking, so a waiter sees it
	first := !g.opened
	g.opened = true
	g.mu.Unlock()

	if first {
		<-g.gate
	}

	*reply = AppendEntriesReply{Term: g.term, Success: true}
	return true
}

func (g *gatedTransport) messages() []*AppendEntriesArgs {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]*AppendEntriesArgs(nil), g.sent...)
}

func (g *gatedTransport) waitForMessages(t *testing.T, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(g.messages()) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%d messages sent within %v, want %d", len(g.messages()), within, want)
}

// Many triggers arriving while a round is out produce ONE follow-up message
// carrying everything that accumulated, not one message per trigger.
//
// This is the property the throughput measurement observes statistically. Here
// it is exact: ten triggers, two messages.
func TestTriggersDuringARoundCollapseIntoOneMessage(t *testing.T) {
	tr := newGatedTransport(5)

	n := NewNode(0, []int{1}, tr, 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 5
	n.state = Leader
	for i := 1; i <= 5; i++ {
		n.log = append(n.log, LogEntry{Term: 5, Command: []byte{byte(i)}})
	}
	n.initLeaderState()
	n.nextIndex[1] = 1 // this follower holds nothing yet
	n.mu.Unlock()

	// Round one leaves and is held open by the gate.
	n.replicateAll(5)
	tr.waitForMessages(t, 1, 2*time.Second)

	// Three more client writes land, and ten triggers fire while the round is
	// still out. replicateAll marks the peer's slot synchronously, so every one
	// of these sees it held regardless of goroutine scheduling.
	n.mu.Lock()
	for i := 6; i <= 8; i++ {
		n.log = append(n.log, LogEntry{Term: 5, Command: []byte{byte(i)}})
	}
	n.mu.Unlock()

	for i := 0; i < 10; i++ {
		n.replicateAll(5)
	}

	if got := len(tr.messages()); got != 1 {
		t.Fatalf("%d messages sent while a round was in flight, want 1", got)
	}

	close(tr.gate)
	tr.waitForMessages(t, 2, 2*time.Second)

	// No heartbeat loop is running, so nothing else can send. Anything arriving
	// in this window would be a trigger that failed to coalesce.
	time.Sleep(100 * time.Millisecond)

	msgs := tr.messages()
	if len(msgs) != 2 {
		t.Fatalf("%d messages for one round plus ten triggers, want 2", len(msgs))
	}
	if got := len(msgs[0].Entries); got != 5 {
		t.Errorf("first message carried %d entries, want 5", got)
	}
	if got := len(msgs[1].Entries); got != 3 {
		t.Errorf("follow-up carried %d entries, want 3: it should cover everything "+
			"that arrived while the first round was out, in one message", got)
	}
	if msgs[1].PrevLogIndex != 5 {
		t.Errorf("follow-up PrevLogIndex = %d, want 5: it should be built from the "+
			"nextIndex the first round's reply established, not from a stale one",
			msgs[1].PrevLogIndex)
	}
}
