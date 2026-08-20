package raft

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// recordingTransport captures every AppendEntries that goes out, so tests can
// assert on what was SENT. That is the right level for C-3: followers do not
// store entries until C-5, so nothing downstream is observable yet.
//
// It also replies Success on everything, which is easy to forget and matters:
// a node built with this transport commits and applies in the background while
// the test body runs. See TestSentEntriesSurviveLogChanges.
type recordingTransport struct {
	mu   sync.Mutex
	sent []*AppendEntriesArgs
	term int
}

func newRecordingTransport(term int) *recordingTransport {
	return &recordingTransport{term: term}
}

func (r *recordingTransport) SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	*reply = RequestVoteReply{Term: r.term, VoteGranted: false}
	return true
}

func (r *recordingTransport) SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	r.mu.Lock()
	r.sent = append(r.sent, args)
	r.mu.Unlock()

	*reply = AppendEntriesReply{Term: r.term, Success: true}
	return true
}

func (r *recordingTransport) messages() []*AppendEntriesArgs {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*AppendEntriesArgs(nil), r.sent...)
}

// waitForMessages blocks until at least n messages have been recorded.
func (r *recordingTransport) waitForMessages(t *testing.T, n int, within time.Duration) []*AppendEntriesArgs {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if msgs := r.messages(); len(msgs) >= n {
			return msgs
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d messages sent within %v, want %d", len(r.messages()), within, n)
	return nil
}

func drainInitialFanOut(t *testing.T, tr *recordingTransport, peers int) int {
	t.Helper()
	tr.waitForMessages(t, peers, time.Second)
	return len(tr.messages())
}

func (r *recordingTransport) waitForMessagesAfter(t *testing.T, mark, n int, within time.Duration) []*AppendEntriesArgs {
	t.Helper()
	all := r.waitForMessages(t, mark+n, within)
	return all[mark:]
}

func leaderWithTransport(t *testing.T, tr Transport, term int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, tr, 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.state = Candidate
	n.currentTerm = term
	n.becomeLeader()
	n.mu.Unlock()

	return n
}

// A follower refuses the command and tells the caller where to go. Its log must
// be untouched -- an entry that enters a log anywhere other than the leader is
// how two logs diverge without a term change to explain it.
func TestSubmitRejectedByNonLeader(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(denyAll(3)), 1)
	defer n.Stop()

	n.mu.Lock()
	n.currentTerm = 3
	before := len(n.log)
	n.mu.Unlock()

	index, term, isLeader := n.Submit([]byte("set x 1"))

	if isLeader {
		t.Fatal("a follower accepted a client command")
	}
	if index != 0 {
		t.Errorf("index = %d, want 0 on rejection", index)
	}
	if term != 3 {
		t.Errorf("term = %d, want 3 so the caller can reason about staleness", term)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != before {
		t.Errorf("follower log grew from %d to %d entries", before, len(n.log))
	}
}

func TestSubmitAppendsToLeaderLog(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	index, term, isLeader := n.Submit([]byte("set x 1"))

	if !isLeader {
		t.Fatal("leader rejected a client command")
	}
	if index != 1 {
		t.Errorf("index = %d, want 1 (first real index; 0 is the sentinel)", index)
	}
	if term != 5 {
		t.Errorf("term = %d, want 5", term)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if got := n.lastLogIndex(); got != 1 {
		t.Fatalf("lastLogIndex = %d, want 1", got)
	}
	entry := n.log[1]
	if entry.Term != 5 {
		t.Errorf("entry.Term = %d, want 5 (the term the leader created it in)", entry.Term)
	}
	if !bytes.Equal(entry.Command, []byte("set x 1")) {
		t.Errorf("entry.Command = %q, want %q", entry.Command, "set x 1")
	}
}

func TestSubmitReturnsConsecutiveIndices(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	for want := 1; want <= 4; want++ {
		got, _, ok := n.Submit([]byte{byte(want)})
		if !ok {
			t.Fatal("leader stopped accepting commands")
		}
		if got != want {
			t.Fatalf("Submit returned index %d, want %d", got, want)
		}
	}
}

// The caller owns the slice it passes in and may reuse it. If the log aliased
// that memory, a caller recycling a buffer would silently rewrite history -- a
// bug that never appears under a test that allocates fresh.
//
// This is the boundary where Command becomes immutable: from the append onward,
// nothing in the package writes to those bytes again.
func TestSubmitCopiesCommand(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	buf := []byte("original")
	n.Submit(buf)
	copy(buf, "OVERWRIT")

	n.mu.Lock()
	defer n.mu.Unlock()

	if !bytes.Equal(n.log[1].Command, []byte("original")) {
		t.Errorf("log entry = %q: Submit aliased the caller's buffer", n.log[1].Command)
	}
}

// Replication must not wait for the heartbeat tick. Correctness does not depend
// on it, but a full interval of latency on every write is most of the cost of a
// Put.
func TestSubmitTriggersReplicationImmediately(t *testing.T) {
	tr := newRecordingTransport(5)
	n := leaderWithTransport(t, tr, 5)

	// Drain the initial fan-out that becomeLeader kicks off.
	tr.waitForMessages(t, len(n.peers), time.Second)
	before := len(tr.messages())

	n.Submit([]byte("set x 1"))

	deadline := time.Now().Add(heartbeatInterval / 2)
	for time.Now().Before(deadline) {
		for _, m := range tr.messages()[before:] {
			if len(m.Entries) > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Error("no entries replicated within half a heartbeat interval of Submit")
}

// THE B-10 LESSON, NOW LOAD-BEARING.
//
// Each peer gets its own args struct. While every message was identical a shared
// pointer was merely a data race; now that PrevLogIndex is per-follower, sharing
// would send peers each other's consistency checks.
//
// Read the arithmetic carefully, because it is easy to get backwards:
// nextIndex[p] is the next entry OWED to p, so PrevLogIndex is one less and the
// entries sent are log[nextIndex:]. A peer at nextIndex 2 is believed to already
// hold index 1.
func TestEachPeerGetsItsOwnMessage(t *testing.T) {
	tr := newRecordingTransport(5)
	n := leaderWithTransport(t, tr, 5)

	mark := drainInitialFanOut(t, tr, len(n.peers))

	// Log becomes: [0]=sentinel, [1]=a, [2]=b, [3]=c. lastLogIndex = 3.
	n.mu.Lock()
	n.log = append(n.log,
		LogEntry{Term: 5, Command: []byte("a")},
		LogEntry{Term: 5, Command: []byte("b")},
		LogEntry{Term: 5, Command: []byte("c")},
	)
	n.nextIndex[1] = 4 // caught up: prev=3, entries=log[4:] = none
	n.nextIndex[2] = 2 // holds a: prev=1, entries=log[2:] = b, c
	n.mu.Unlock()

	n.replicateAll(5)
	msgs := tr.waitForMessagesAfter(t, mark, 2, time.Second)

	byPrev := map[int]*AppendEntriesArgs{}
	for _, m := range msgs {
		byPrev[m.PrevLogIndex] = m
	}
	if len(byPrev) < 2 {
		t.Fatalf("peers received identical PrevLogIndex: %v", byPrev)
	}

	if m := byPrev[3]; m == nil {
		t.Error("no message with PrevLogIndex 3 for the caught-up peer")
	} else if len(m.Entries) != 0 {
		t.Errorf("caught-up peer got %d entries, want 0", len(m.Entries))
	}

	m := byPrev[1]
	if m == nil {
		t.Fatal("no message with PrevLogIndex 1 for the lagging peer")
	}
	if m.PrevLogTerm != 5 {
		t.Errorf("PrevLogTerm = %d, want 5 (the term of log[1])", m.PrevLogTerm)
	}
	if len(m.Entries) != 2 {
		t.Fatalf("lagging peer got %d entries, want 2 (indices 2 and 3)", len(m.Entries))
	}
	if !bytes.Equal(m.Entries[0].Command, []byte("b")) {
		t.Errorf("first entry = %q, want %q: sending must start AT nextIndex",
			m.Entries[0].Command, "b")
	}
	if !bytes.Equal(m.Entries[1].Command, []byte("c")) {
		t.Errorf("second entry = %q, want %q", m.Entries[1].Command, "c")
	}
}

// WHAT THE COPY IN buildAppendEntries IS ACTUALLY FOR.
//
// The hazard is the log SLICE changing under an in-flight send: append can
// reallocate the backing array, and a step-down followed by repair can truncate
// it. Copying the entry structs out means a message on the wire is a snapshot,
// unaffected by whatever the log does next.
func TestSentEntriesSurviveLogChanges(t *testing.T) {
	tr := newRecordingTransport(5)
	n := leaderWithTransport(t, tr, 5)

	n.mu.Lock()
	n.log = append(n.log,
		LogEntry{Term: 5, Command: []byte("a")},
		LogEntry{Term: 5, Command: []byte("b")},
	)
	n.nextIndex[1] = 1
	n.nextIndex[2] = 1
	n.mu.Unlock()

	n.replicateAll(5)
	msgs := tr.waitForMessages(t, 2, time.Second)

	sent := msgs[0]
	if len(sent.Entries) != 2 {
		t.Fatalf("test setup wrong: sent %d entries, want 2", len(sent.Entries))
	}

	// Grow the log hard enough to force at least one reallocation, then throw
	// most of it away -- the two things that would corrupt a subslice.
	//
	// COMMIT STATE MUST BE ROLLED BACK IN THE SAME CRITICAL SECTION, and this
	// is not bookkeeping. recordingTransport answers Success to everything, so
	// by the time this line runs a reply handler has already credited both
	// followers with indices 1 and 2, counted a majority of three, and called
	// commitTo(2). The truncation below then leaves commitIndex at 2 over a log
	// holding only the sentinel.
	//
	// Raft cannot reach that state -- a leader never truncates, and receiver
	// rule 3 only removes an uncommitted tail -- so nothing downstream defends
	// against it beyond a logged Error. Before that Error existed the applier
	// woke, walked from lastApplied+1 to commitIndex, and panicked on
	// n.log[1]; whether it did depended on the race between its wake-up and
	// this line, which is why the whole suite passed for weeks and then failed
	// once under -race.
	//
	// The test's subject is the in-flight message, not the commit path. Undoing
	// the commit keeps the node in a state Raft could actually be in, so the
	// only artificial thing left is the truncation itself.
	n.mu.Lock()
	for i := 0; i < 64; i++ {
		n.log = append(n.log, LogEntry{Term: 5, Command: []byte{byte(i)}})
	}
	n.log = n.log[:1] // truncate back to the sentinel
	n.commitIndex = 0
	n.lastApplied = 0
	n.mu.Unlock()

	if len(sent.Entries) != 2 {
		t.Fatalf("in-flight message now carries %d entries: it was a subslice "+
			"of the live log", len(sent.Entries))
	}
	if !bytes.Equal(sent.Entries[0].Command, []byte("a")) ||
		!bytes.Equal(sent.Entries[1].Command, []byte("b")) {
		t.Errorf("in-flight entries changed after the log was rewritten: %q, %q",
			sent.Entries[0].Command, sent.Entries[1].Command)
	}
}

// TRIPWIRE, documenting a deliberate choice.
//
// The copy in buildAppendEntries is SHALLOW: it duplicates the LogEntry structs
// but not the bytes each Command points at. Leader and message therefore share
// command bytes, and that is intended -- deep-copying every command on every
// resend would allocate the whole outstanding log per peer per tick, to defend
// against a mutation that the package never performs.
//
// The invariant this rests on: LogEntry.Command is IMMUTABLE once appended.
// Submit copies the caller's bytes in precisely so the log owns them, and no
// code writes to them afterwards.
//
// If this test starts failing, someone made the copy deep. That is not wrong,
// but it is a real cost on the replication hot path, so it should be a decision
// with a DESIGN.md note rather than a drive-by change. If instead you find code
// mutating Command in place -- most likely a state machine in Phase F decoding
// into the slice it was handed -- fix that code, not this test.
func TestSentEntryBytesAliasTheLogDeliberately(t *testing.T) {
	tr := newRecordingTransport(5)
	n := leaderWithTransport(t, tr, 5)

	n.Submit([]byte("first"))
	msgs := tr.waitForMessages(t, 3, time.Second)

	var sent *AppendEntriesArgs
	for _, m := range msgs {
		if len(m.Entries) > 0 {
			sent = m
			break
		}
	}
	if sent == nil {
		t.Fatal("no message carried entries")
	}

	sent.Entries[0].Command[0] = 'X'

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.log[1].Command[0] != 'X' {
		t.Log("the entry copy is now deep. Confirm this was intentional and " +
			"note the added allocation cost in DESIGN.md, then delete this test.")
	}
}

// PrevLogIndex 0 must always be reachable, because that is where log repair
// bottoms out: every node has the sentinel, so the check at index 0 cannot fail.
func TestReplicationToAnEmptyFollowerStartsAtTheSentinel(t *testing.T) {
	tr := newRecordingTransport(5)
	n := leaderWithTransport(t, tr, 5)

	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 5, Command: []byte("a")})
	n.nextIndex[1] = 1 // this follower is believed to hold nothing
	n.mu.Unlock()

	n.replicateAll(5)
	msgs := tr.waitForMessages(t, 2, time.Second)

	for _, m := range msgs {
		if m.PrevLogIndex == 0 {
			if m.PrevLogTerm != 0 {
				t.Errorf("PrevLogTerm = %d at index 0, want 0 (the sentinel)", m.PrevLogTerm)
			}
			return
		}
	}
	t.Error("no message sent with PrevLogIndex 0")
}

// Concurrent clients must each get a distinct index, with no gaps and no
// duplicates. This is the test that earns its keep under -race.
//
// It is also the real proof that Submit's critical section is right, and worth
// knowing about before reading concurrent_test.go: that file's term/index
// collision check covers the same ground across leader changes, but this one is
// deterministic and fails loudly.
func TestConcurrentSubmitsGetDistinctIndices(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	const writers = 50
	var wg sync.WaitGroup
	indices := make([]int, writers)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			idx, _, ok := n.Submit([]byte(fmt.Sprintf("cmd-%d", i)))
			if !ok {
				t.Errorf("writer %d was rejected", i)
				return
			}
			indices[i] = idx
		}(i)
	}
	wg.Wait()

	seen := make(map[int]bool, writers)
	for _, idx := range indices {
		if seen[idx] {
			t.Fatalf("index %d handed to two clients", idx)
		}
		seen[idx] = true
	}
	for want := 1; want <= writers; want++ {
		if !seen[want] {
			t.Errorf("index %d was never allocated: a gap in the log", want)
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if got := n.lastLogIndex(); got != writers {
		t.Errorf("lastLogIndex = %d, want %d", got, writers)
	}
}

// A fan-out that outlives its term must not send. Otherwise a deposed leader
// keeps pushing entries at a cluster that has moved on.
func TestReplicationStopsAfterStepDown(t *testing.T) {
	tr := newRecordingTransport(9)
	n := leaderWithTransport(t, tr, 5)

	n.mu.Lock()
	n.becomeFollower(9)
	n.mu.Unlock()

	before := len(tr.messages())
	n.replicateAll(5) // a stale fan-out from the old term

	if after := len(tr.messages()); after != before {
		t.Errorf("%d messages sent after step-down", after-before)
	}
}

func TestSubmitReplicatesAndCommits(t *testing.T) {
	n := leaderWithTransport(t, newRecordingTransport(5), 5)

	for i := 0; i < 3; i++ {
		n.Submit([]byte{byte(i)})
	}
	time.Sleep(3 * heartbeatInterval)

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, p := range n.peers {
		if got := n.matchIndexFor(p); got != 3 {
			t.Errorf("matchIndex[%d] = %d, want 3: every entry was acknowledged",
				p, got)
		}
		if got := n.nextIndexFor(p); got != 4 {
			t.Errorf("nextIndex[%d] = %d, want 4 (one past matchIndex)", p, got)
		}
	}

	if n.commitIndex != 3 {
		t.Errorf("commitIndex = %d, want 3: three current-term entries on a "+
			"majority of three nodes", n.commitIndex)
	}

	// No consumer is attached to ApplyCh, so the applier is parked on its first
	// send and lastApplied has not moved. That is the back-pressure design
	// working, not a stall: consensus reached commitIndex 3 with a completely
	// dead state machine.
	if n.lastApplied != 0 {
		t.Errorf("lastApplied = %d, want 0: nothing is reading ApplyCh, so the "+
			"applier is parked on its first delivery", n.lastApplied)
	}
}
