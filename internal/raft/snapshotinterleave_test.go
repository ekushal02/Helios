package raft

import (
	"fmt"
	"testing"
	"time"
)

// =============================================================================
// Snapshot and AppendEntries, interleaved
// =============================================================================
//
// WHAT CAN ACTUALLY INTERLEAVE, which is narrower than it first looks.
//
// A leader keeps at most one round in flight per peer (DESIGN.md §10), so it
// never has an image and entries outstanding to the same follower at the same
// time. The orderings that survive that are:
//
//   - A round left over from a DEPOSED TERM. The in-flight slot is keyed by
//     term, so a new term's round starts while an old one is still on the wire.
//     Rule 1 rejects it on term, and that is the whole defence.
//   - A STALE MESSAGE FROM THE SAME LEADER that the network delayed past a
//     later one. Same term, so rule 1 does not catch it; the receiver rules
//     have to.
//   - A RESTARTED FOLLOWER whose own log disagrees with the image about the
//     same indices.
//   - THE APPLIER MID-BATCH. Entries copied out under the lock are still in
//     flight to the state machine while the log they came from is replaced.
//     This is the one the compaction work newly created.
//
// These are tested by handing the receiver each ordering directly rather than
// racing them, for the reason coalescing_test.go gives: a guard is better
// checked by what it does than by hoping a delay draw produced the case.
//
// A gRPC transport that pipelines would put the first two back on the ordinary
// path. That is an open question, not a hypothetical.

// installedFollower returns a follower whose log has been entirely replaced by
// an image at the given floor.
func installedFollower(t *testing.T, term, floor int) *Node {
	t.Helper()

	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = term
	n.mu.Unlock()

	var reply InstallSnapshotReply
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term:              term,
		LeaderID:          1,
		LastIncludedIndex: floor,
		LastIncludedTerm:  term,
		Data:              []byte("image"),
	}, &reply)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastIncludedIndex != floor {
		t.Fatalf("fixture: floor = %d, want %d", n.lastIncludedIndex, floor)
	}
	return n
}

// =============================================================================
// A stale AppendEntries landing after an image
// =============================================================================

// The message the leader sent before it compacted, arriving after the image it
// sent afterwards. Its PrevLogIndex is below the floor, so the entry the
// consistency check needs is gone.
//
// It must be rejected, and it must leave everything alone. Accepting it would
// append entries below the floor -- indices the image already accounts for --
// and rule 5 would then commit them.
func TestAStaleAppendEntriesFromBelowTheFloorChangesNothing(t *testing.T) {
	n := installedFollower(t, 4, 40)

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         4,
		LeaderID:     1,
		PrevLogIndex: 10,
		PrevLogTerm:  4,
		Entries:      []LogEntry{{Term: 4, Command: []byte("doomed")}},
		LeaderCommit: 40,
	}, &reply)

	if reply.Success {
		t.Fatal("accepted a message whose consistency check landed below the floor: " +
			"the entry it checked against was discarded and cannot have matched")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.lastIncludedIndex != 40 {
		t.Errorf("floor moved to %d", n.lastIncludedIndex)
	}
	if n.logLength() != 0 {
		t.Errorf("logLength = %d, want 0: entries from below the floor were appended", n.logLength())
	}
	if n.commitIndex != 40 {
		t.Errorf("commitIndex = %d, want 40", n.commitIndex)
	}

	// THE HINT MUST CONVERGE RATHER THAN LOOP. It cannot name a term -- those
	// entries are gone -- so it takes the "too short" shape, pointing at the end
	// of what this node does hold.
	if reply.ConflictIndex != n.lastLogIndex()+1 || reply.ConflictTerm != noConflictTerm {
		t.Errorf("hint = (%d, %d), want (%d, %d)",
			reply.ConflictIndex, reply.ConflictTerm, n.lastLogIndex()+1, noConflictTerm)
	}

	// And the leader reading that hint must end up at or below the floor, which
	// is the condition that makes it send an image instead of retrying entries.
	// Without this the two sides could reject each other indefinitely.
	leaderLog := []LogEntry{{Term: 0}}
	for i := 1; i <= 50; i++ {
		leaderLog = append(leaderLog, LogEntry{Term: 4})
	}
	next := nextIndexAfterConflict(leaderLog, 0, 11, reply.ConflictIndex, reply.ConflictTerm)
	if next > 40 {
		t.Errorf("the leader would resume at %d, above the follower's floor of 40: "+
			"it will keep sending entries the follower cannot check", next)
	}
}

// The floor itself IS checkable, and that is why lastIncludedTerm is stored. A
// message that lands exactly there is ordinary replication.
func TestAnAppendEntriesAtTheFloorIsAccepted(t *testing.T) {
	n := installedFollower(t, 4, 40)

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         4,
		LeaderID:     1,
		PrevLogIndex: 40,
		PrevLogTerm:  4,
		Entries: []LogEntry{
			{Term: 4, Command: []byte{41}},
			{Term: 4, Command: []byte{42}},
			{Term: 4, Command: []byte{43}},
		},
		LeaderCommit: 43,
	}, &reply)

	if !reply.Success {
		t.Fatalf("rejected a check at the floor, which lastIncludedTerm exists to "+
			"answer (reply %+v)", reply)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if got := n.lastLogIndex(); got != 43 {
		t.Errorf("lastLogIndex = %d, want 43", got)
	}
	if got := n.entryAt(41).Command[0]; got != 41 {
		t.Errorf("entryAt(41) = %d, want 41", got)
	}
	if n.commitIndex != 43 {
		t.Errorf("commitIndex = %d, want 43", n.commitIndex)
	}
}

// THE FIGURE 7 CAP, NOW AT A FLOOR. Rule 5 commits the smaller of LeaderCommit
// and what the message proves. A leader that has committed far past what this
// message carried must not drag the follower's commitIndex up with it.
func TestRuleFiveStillCapsAtWhatTheMessageProvesAboveAFloor(t *testing.T) {
	n := installedFollower(t, 4, 40)

	var reply AppendEntriesReply
	n.AppendEntries(&AppendEntriesArgs{
		Term:         4,
		LeaderID:     1,
		PrevLogIndex: 40,
		PrevLogTerm:  4,
		Entries:      []LogEntry{{Term: 4, Command: []byte{41}}, {Term: 4, Command: []byte{42}}},
		LeaderCommit: 90,
	}, &reply)

	if !reply.Success {
		t.Fatalf("append rejected (reply %+v)", reply)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.commitIndex != 42 {
		t.Errorf("commitIndex = %d, want 42: LeaderCommit is a fact about the "+
			"leader's log, not about the prefix this message proves", n.commitIndex)
	}
}

// The out-of-order guard in mergeEntries, exercised above a floor rather than
// above the sentinel. A short stale message must not truncate the longer tail a
// later message already installed.
func TestAShortStaleAppendEntriesDoesNotTruncateAboveTheFloor(t *testing.T) {
	n := installedFollower(t, 4, 40)

	long := &AppendEntriesArgs{
		Term: 4, LeaderID: 1, PrevLogIndex: 40, PrevLogTerm: 4,
		Entries: []LogEntry{
			{Term: 4, Command: []byte{41}},
			{Term: 4, Command: []byte{42}},
			{Term: 4, Command: []byte{43}},
		},
		LeaderCommit: 40,
	}
	var reply AppendEntriesReply
	n.AppendEntries(long, &reply)
	if !reply.Success {
		t.Fatalf("setup append rejected (reply %+v)", reply)
	}

	// The earlier, shorter message finally arrives.
	short := &AppendEntriesArgs{
		Term: 4, LeaderID: 1, PrevLogIndex: 40, PrevLogTerm: 4,
		Entries:      []LogEntry{{Term: 4, Command: []byte{41}}},
		LeaderCommit: 40,
	}
	n.AppendEntries(short, &reply)
	if !reply.Success {
		t.Fatalf("a duplicate message was rejected (reply %+v)", reply)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if got := n.lastLogIndex(); got != 43 {
		t.Errorf("lastLogIndex = %d, want 43: a stale message truncated entries the "+
			"leader may already have counted toward a majority", got)
	}
}

// =============================================================================
// An image landing over a log that disagrees
// =============================================================================

// A restarted follower holding its own history at the image's index. Rule 6's
// term check fails, so rule 7 applies and nothing survives.
func TestAnImageOverAConflictingPrefixDiscardsTheWholeLog(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 2
	for i := 1; i <= 8; i++ {
		n.log = append(n.log, LogEntry{Term: 2, Command: []byte{byte(i)}})
	}
	n.mu.Unlock()

	var reply InstallSnapshotReply
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term: 3, LeaderID: 1,
		LastIncludedIndex: 5,
		LastIncludedTerm:  3, // this node holds term 2 at index 5
		Data:              []byte("image"),
	}, &reply)

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.lastIncludedIndex != 5 {
		t.Fatalf("floor = %d, want 5", n.lastIncludedIndex)
	}
	if n.logLength() != 0 {
		t.Errorf("logLength = %d, want 0: entries from a history the image "+
			"contradicts were kept", n.logLength())
	}
	if n.commitIndex != 5 {
		t.Errorf("commitIndex = %d, want 5", n.commitIndex)
	}
	if n.pendingSnapshot == nil {
		t.Error("no image parked for the applier")
	}
}

// Election Safety broken. AppendEntries refuses the same situation (§4.2); an
// image is worse, because it would discard the whole log this node is still
// replicating from.
func TestALeaderRefusesAnImageAtItsOwnTerm(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 6
	n.state = Leader
	for i := 1; i <= 5; i++ {
		n.log = append(n.log, LogEntry{Term: 6, Command: []byte{byte(i)}})
	}
	n.initLeaderState()
	n.mu.Unlock()

	var reply InstallSnapshotReply
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term: 6, LeaderID: 1,
		LastIncludedIndex: 900,
		LastIncludedTerm:  6,
		Data:              []byte("image"),
	}, &reply)

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.lastIncludedIndex != 0 {
		t.Errorf("floor moved to %d: a leader installed a rival's image at its own term",
			n.lastIncludedIndex)
	}
	if n.state != Leader {
		t.Errorf("state = %v, want leader: nothing legitimate can depose a node at its own term", n.state)
	}
	if got := n.lastLogIndex(); got != 5 {
		t.Errorf("lastLogIndex = %d, want 5: the log was discarded", got)
	}
	// A HIGHER term is a different matter entirely, and must still work.
	n.mu.Unlock()
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term: 7, LeaderID: 1,
		LastIncludedIndex: 900, LastIncludedTerm: 7, Data: []byte("image"),
	}, &reply)
	n.mu.Lock()

	if n.lastIncludedIndex != 900 {
		t.Errorf("floor = %d after a higher-term image, want 900: the guard is "+
			"refusing legitimate leaders too", n.lastIncludedIndex)
	}
}

// =============================================================================
// The applier, mid-batch
// =============================================================================

// THE HAZARD COMPACTION CREATED, and the one nothing else covers.
//
// The applier copies a batch out under the lock and then delivers it with the
// lock released. An image arriving in that window replaces the log those
// entries came from. They must still arrive, still carry the data that was
// copied, and still be followed by the image rather than by anything rebuilt
// from a log that no longer holds them.
func TestABatchInFlightSurvivesAnImageInstalledBehindIt(t *testing.T) {
	n := NewNode(0, []int{1, 2}, newStubTransport(nil), 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	n.currentTerm = 2
	for i := 1; i <= 5; i++ {
		n.log = append(n.log, LogEntry{Term: 2, Command: []byte{byte(i)}})
	}
	n.mu.Unlock()

	commit(n, 5)

	// Take one. The applier now holds indices 2..5 in a batch it copied before
	// anything happened, parked on the send of index 2.
	if got := mustApply(t, n).CommandIndex; got != 1 {
		t.Fatalf("first delivery was index %d, want 1", got)
	}

	var reply InstallSnapshotReply
	n.InstallSnapshot(&InstallSnapshotArgs{
		Term: 2, LeaderID: 1,
		LastIncludedIndex: 40,
		LastIncludedTerm:  2,
		Data:              []byte("image"),
	}, &reply)

	n.mu.Lock()
	if n.logLength() != 0 || n.lastIncludedIndex != 40 {
		n.mu.Unlock()
		t.Fatalf("setup: floor %d, logLength %d, want 40 and 0",
			n.lastIncludedIndex, n.logLength())
	}
	n.mu.Unlock()

	// The rest of the batch still arrives, in order, carrying the data copied
	// under the lock -- not whatever a rebuild from the replaced log would find.
	for want := 2; want <= 5; want++ {
		msg := mustApply(t, n)
		if msg.CommandIndex != want {
			t.Fatalf("delivered index %d, want %d", msg.CommandIndex, want)
		}
		if !msg.CommandValid || len(msg.Command) != 1 || msg.Command[0] != byte(want) {
			t.Fatalf("index %d carried %v: the batch was rebuilt from a log that no "+
				"longer holds these entries", want, msg.Command)
		}
	}

	// Then the image, and nothing from the old history after it.
	img := mustApply(t, n)
	if !img.SnapshotValid {
		t.Fatalf("expected the image next, got index %d", img.CommandIndex)
	}
	if img.SnapshotIndex != 40 || img.SnapshotTerm != 2 {
		t.Errorf("image = (%d, %d), want (40, 2)", img.SnapshotIndex, img.SnapshotTerm)
	}

	mustNotApply(t, n, "the log holds nothing above the floor")

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastApplied != 40 {
		t.Errorf("lastApplied = %d, want 40", n.lastApplied)
	}
}

// =============================================================================
// The whole thing, under load
// =============================================================================

// A follower that has a REAL LOG of its own is crashed, the cluster compacts
// well past it, and it returns while writes are still flowing. Its log
// disagrees with the image about every index it holds, so rule 7 applies, and
// the tail arrives as ordinary AppendEntries immediately behind the image.
//
// This is the closest the sender can get to the interleaving: the image and the
// entries after it are consecutive rounds to the same peer, back to back, with
// new writes landing throughout.
func TestAFollowerWithItsOwnLogIsCaughtUpWhileWritesContinue(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster under load")
	}

	const (
		warmup    = 50
		bulk      = 1500
		threshold = 100
		seed      = 20260904
	)

	c := newCluster(t, 3, seed)

	machines := make([]*snapshotMachine, len(c.nodes))
	for i, n := range c.nodes {
		n.SetSnapshotThreshold(threshold)
		machines[i] = attachSnapshotMachine(t, n)
	}
	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}
	victim := c.othersThan(leader)[0]

	// The victim gets a real log first. Without this it comes back empty and
	// rule 7 is trivially satisfied -- which is the ten-thousand-entry test's
	// case, not this one.
	for i := 1; i <= warmup; i++ {
		if !c.submitToLeader([]byte(fmt.Sprintf("warm%04d=%d", i, i))) {
			t.Fatalf("no leader accepted warmup %d", i)
		}
	}
	if !machines[victim].waitForApplied(warmup, 20*time.Second) {
		_, applied, _, _, _ := machines[victim].snapshot()
		t.Fatalf("victim applied %d of %d warmup entries", applied, warmup)
	}

	c.crash(victim)

	for i := 1; i <= bulk; i++ {
		if !c.submitToLeader([]byte(fmt.Sprintf("bulk%05d=%d", i, i))) {
			t.Fatalf("no leader accepted bulk %d: %s", i, c.describe())
		}
	}
	if !machines[leader].waitForApplied(warmup+bulk, 60*time.Second) {
		t.Fatalf("leader stalled: %s", c.describe())
	}

	c.nodes[leader].mu.Lock()
	floor := c.nodes[leader].lastIncludedIndex
	c.nodes[leader].mu.Unlock()
	if floor <= warmup {
		t.Fatalf("leader floor is %d, not past the victim's %d entries: the victim "+
			"could be repaired by AppendEntries alone", floor, warmup)
	}

	snapshotsBefore := c.net.installSnapshots()

	// Writes keep flowing across the restart, so the image and the entries
	// after it are consecutive rounds rather than a quiet catch-up.
	stop := make(chan struct{})
	done := make(chan int)
	go func() {
		sent := 0
		for i := 1; ; i++ {
			select {
			case <-stop:
				done <- sent
				return
			default:
			}
			if c.submitToLeader([]byte(fmt.Sprintf("live%05d=%d", i, i))) {
				sent++
			}
			time.Sleep(time.Millisecond)
		}
	}()

	c.restart(victim)
	machines[victim] = attachSnapshotMachine(t, c.nodes[victim])

	// Let the writer run a while, then stop and let everything settle.
	time.Sleep(500 * time.Millisecond)
	close(stop)
	live := <-done

	target := warmup + bulk + live
	if !machines[leader].waitForApplied(target, 30*time.Second) {
		_, applied, _, _, _ := machines[leader].snapshot()
		t.Fatalf("leader applied %d of %d: %s", applied, target, c.describe())
	}
	if !machines[victim].waitForApplied(target, 60*time.Second) {
		_, applied, images, _, _ := machines[victim].snapshot()
		t.Fatalf("victim applied %d of %d after %d images: %s",
			applied, target, images, c.describe())
	}

	if got := c.net.installSnapshots() - snapshotsBefore; got == 0 {
		t.Error("the victim caught up without an image, so nothing about the " +
			"interleaving was exercised")
	} else {
		t.Logf("floor %d, %d images delivered, %d live writes during recovery",
			floor, got, live)
	}

	leaderKV, _, _, _, leaderFault := machines[leader].snapshot()
	victimKV, _, victimImages, _, victimFault := machines[victim].snapshot()

	if leaderFault != "" {
		t.Fatalf("leader state machine: %s", leaderFault)
	}
	if victimFault != "" {
		t.Fatalf("victim state machine: %s", victimFault)
	}
	if victimImages == 0 {
		t.Error("the victim's state machine was never handed an image")
	}
	if len(victimKV) != len(leaderKV) {
		t.Fatalf("victim holds %d keys, leader holds %d", len(victimKV), len(leaderKV))
	}
	for k, want := range leaderKV {
		if got := victimKV[k]; got != want {
			t.Fatalf("victim %s = %q, leader has %q", k, got, want)
		}
	}
}
