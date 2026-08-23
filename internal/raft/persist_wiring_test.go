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
// persist_test.go proves the storage layer works. It says nothing about whether
// Raft uses it, and a node that grants votes without writing them passes every
// test in that file. These are the tests that fail when the markDirty and
// persistIfDirty calls are missing from the handlers.
//
// Each one asserts the same shape: perform the action, then read the storage
// DIRECTLY -- not the Node -- and check the promise is already there. Reading
// the storage rather than n.currentTerm is the whole point; the in-memory value
// is what a crash destroys.

// =============================================================================
// recordingStorage
// =============================================================================

// recordingStorage is a MemoryStorage that counts saves and can be made to
// block inside Save, which is how a test observes what happens BEFORE a write
// completes rather than after.
type recordingStorage struct {
	noSnapshots
	mu    sync.Mutex
	data  []byte
	saves int
	gate  chan struct{}
}

func newRecordingStorage() *recordingStorage { return &recordingStorage{} }

func (s *recordingStorage) Save(b []byte) error {
	s.mu.Lock()
	gate := s.gate
	s.mu.Unlock()

	if gate != nil {
		<-gate // a real fsync, stretched out to something a test can see
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append([]byte(nil), b...)
	s.saves++
	return nil
}

func (s *recordingStorage) Load() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		return nil, nil
	}
	return append([]byte(nil), s.data...), nil
}

func (s *recordingStorage) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

// hold makes the next Save block until release. Only one gate at a time.
func (s *recordingStorage) hold() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gate = make(chan struct{})
}

// release unblocks a held Save. Safe to call when nothing is held, and safe to
// call twice, so it works as a defer.
func (s *recordingStorage) release() {
	s.mu.Lock()
	g := s.gate
	s.gate = nil
	s.mu.Unlock()
	if g != nil {
		close(g)
	}
}

// durable reads what is actually on the storage right now.
func (s *recordingStorage) durable(t *testing.T) persistentState {
	t.Helper()
	ps, found, err := loadState(s)
	if err != nil {
		t.Fatalf("reading durable state: %v", err)
	}
	if !found {
		t.Fatal("nothing has been persisted at all")
	}
	return ps
}

// =============================================================================
// RequestVote
// =============================================================================

// The double-vote scenario, reduced to one assertion. If this fails, a node can
// grant a vote in term 1, crash, come back at term 0, and grant a second vote
// in term 1 to a different candidate -- two leaders in one term.
func TestGrantingAVoteIsDurableBeforeTheReply(t *testing.T) {
	store := newRecordingStorage()

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}, &reply)

	if !reply.VoteGranted {
		t.Fatalf("vote was not granted; the test is not exercising the grant path (reply %+v)", reply)
	}

	// The handler has returned, so the caller may already have counted this
	// vote. Everything it relied on must be on the storage by now.
	ps := store.durable(t)
	if ps.CurrentTerm != 1 {
		t.Errorf("durable currentTerm = %d, want 1", ps.CurrentTerm)
	}
	if ps.VotedFor != 1 {
		t.Errorf("durable votedFor = %d, want 1 -- the vote was granted but never written", ps.VotedFor)
	}
}

// A term bump with no vote is still a promise: the node has told the candidate
// it is in term 5, and a restart at term 4 would let it accept a leader it has
// already refused to follow.
func TestARefusedVoteStillPersistsTheTermBump(t *testing.T) {
	store := newRecordingStorage()

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	// Give the node a log the candidate cannot match, so §5.4.1 refuses.
	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 3, Command: []byte("x")})
	n.markDirty()
	n.persistIfDirty()
	n.mu.Unlock()

	var reply RequestVoteReply
	n.RequestVote(&RequestVoteArgs{
		Term:         5,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}, &reply)

	if reply.VoteGranted {
		t.Fatal("vote granted to a candidate with a shorter log; this test needs the refusal path")
	}

	ps := store.durable(t)
	if ps.CurrentTerm != 5 {
		t.Errorf("durable currentTerm = %d, want 5 -- the step-up was not written", ps.CurrentTerm)
	}
}

// =============================================================================
// AppendEntries
// =============================================================================

// The lost-committed-entry scenario. A follower that replies success has told
// the leader the entry is held; the leader may count it toward a majority and
// commit within a millisecond. If the entry was only ever in RAM, the commit is
// a lie.
func TestAppendedEntriesAreDurableBeforeTheReply(t *testing.T) {
	store := newRecordingStorage()

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 1, Command: []byte("put a 1")}},
		LeaderCommit: 0,
	}, &reply)

	if !reply.Success {
		t.Fatalf("append was rejected; the test is not exercising the append path (reply %+v)", reply)
	}

	ps := store.durable(t)
	if ps.CurrentTerm != 1 {
		t.Errorf("durable currentTerm = %d, want 1", ps.CurrentTerm)
	}
	if len(ps.Log) != 2 {
		t.Fatalf("durable log has %d entries, want 2 -- success was reported for an entry that was never written", len(ps.Log))
	}
	if got := string(ps.Log[1].Command); got != "put a 1" {
		t.Errorf("durable log[1].Command = %q, want %q", got, "put a 1")
	}
}

// A truncation is a mutation too. Overwriting a conflicting suffix and then
// crashing must not resurrect the entries that were removed.
func TestTruncationIsDurableBeforeTheReply(t *testing.T) {
	store := newRecordingStorage()

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	// A private tail from a leader that never committed it.
	n.mu.Lock()
	n.currentTerm = 2
	n.log = append(n.log, LogEntry{Term: 2, Command: []byte("doomed")}, LogEntry{Term: 2, Command: []byte("also doomed")})
	n.markDirty()
	n.persistIfDirty()
	n.mu.Unlock()

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 3, Command: []byte("survivor")}},
		LeaderCommit: 0,
	}, &reply)

	if !reply.Success {
		t.Fatalf("append was rejected; the test is not exercising the conflict path (reply %+v)", reply)
	}

	ps := store.durable(t)
	if len(ps.Log) != 2 {
		t.Fatalf("durable log has %d entries, want 2 -- the truncation was not written", len(ps.Log))
	}
	if got := string(ps.Log[1].Command); got != "survivor" {
		t.Errorf("durable log[1].Command = %q, want %q", got, "survivor")
	}
}

// A steady follower under heartbeats changes nothing persistent after the first
// message, and must therefore write nothing after the first message.
//
// THE BUG THIS EXISTS FOR. becomeFollower is called on every AppendEntries,
// including the same-term step-down that deliberately preserves votedFor. A
// markDirty at the top of that function rather than beside the assignments sets
// the flag on every heartbeat, and the node fsyncs at the heartbeat interval
// forever. Nothing is incorrect; the node is simply ten to a hundred times
// slower than it should be, on a path no correctness test observes.
func TestSteadyHeartbeatsDoNotWriteEveryTime(t *testing.T) {
	store := newRecordingStorage()

	n, err := OpenNode(0, []int{1, 2}, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	for i := 0; i < 10; i++ {
		var reply AppendEntriesReply
		n.AppendEntries(&AppendEntriesArgs{
			Term:         1,
			LeaderID:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}, &reply)
		if !reply.Success {
			t.Fatalf("heartbeat %d rejected (reply %+v)", i, reply)
		}
	}

	// One write: the step from term 0 to term 1 on the first heartbeat. The
	// other nine mutate nothing.
	if got := store.saveCount(); got != 1 {
		t.Errorf("saves = %d after ten identical heartbeats, want 1", got)
	}
}

// =============================================================================
// Campaigning
// =============================================================================

// This one inspects the storage from inside the send, which is the only way to
// prove the flush happened BEFORE the RPC left rather than shortly after.
//
// The failure it catches is subtle: a candidate crashes after its RequestVote
// is on the wire, restarts at the old term, and votes for somebody else in the
// term it already voted for itself in.
func TestCampaigningIsDurableBeforeAnyRequestVoteIsSent(t *testing.T) {
	store := newRecordingStorage()
	observed := make(chan persistentState, 8)

	stub := newStubTransport(func(to int, args *RequestVoteArgs) (RequestVoteReply, bool) {
		ps, found, err := loadState(store)
		if err != nil || !found {
			observed <- persistentState{CurrentTerm: -1, VotedFor: -1}
		} else {
			observed <- ps
		}
		return RequestVoteReply{Term: args.Term, VoteGranted: false}, true
	})

	n, err := OpenNode(0, []int{1, 2}, stub, 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}

	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	select {
	case ps := <-observed:
		if ps.CurrentTerm != 1 {
			t.Errorf("storage held term %d when RequestVote went out, want 1", ps.CurrentTerm)
		}
		if ps.VotedFor != 0 {
			t.Errorf("storage held votedFor %d when RequestVote went out, want 0 (self)", ps.VotedFor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no RequestVote was sent")
	}
}

// =============================================================================
// The leader's own append
// =============================================================================

// The single-node case, which is where "persist before responding" is easiest
// to get wrong: there is no peer to reply to, so the only thing gating the
// commit is the order of two statements inside appendAndReplicate.
//
// The storage is held open mid-Save. If the entry reaches the state machine
// while the write is still in flight, the node has applied something it could
// lose -- and told a client it succeeded.
func TestASoleNodeDoesNotApplyBeforeTheWriteCompletes(t *testing.T) {
	store := newRecordingStorage()
	defer store.release() // never leave a node wedged on a held gate

	n, err := OpenNode(0, nil, newStubTransport(nil), 1, store)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer n.Stop()

	n.Start()

	deadline := time.Now().Add(3 * time.Second)
	for {
		st, _, _ := n.snapshotState()
		if st == Leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a sole node never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Drain whatever the new leader has already produced -- a no-op barrier,
	// if becomeLeader appends one -- so the only thing left to arrive is the
	// entry this test submits. Deliberately not a hard requirement: whether an
	// election emits a barrier is not what this test is about.
drain:
	for {
		select {
		case <-n.ApplyCh():
		case <-time.After(300 * time.Millisecond):
			break drain
		}
	}

	before := store.saveCount()
	store.hold()

	go n.Submit([]byte("put a 1"))

	select {
	case <-n.ApplyCh():
		t.Fatal("the entry was applied while its write was still in flight")
	case <-time.After(250 * time.Millisecond):
		// Correct: blocked behind the write.
	}

	store.release()

	select {
	case <-n.ApplyCh():
	case <-time.After(2 * time.Second):
		t.Fatal("the entry never arrived after the write completed")
	}

	if store.saveCount() <= before {
		t.Errorf("saves went %d -> %d: the submitted entry was never written at all",
			before, store.saveCount())
	}
}
