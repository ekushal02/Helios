package raft

import "time"

// =============================================================================
// SendInstallSnapshot for every transport in the suite
// =============================================================================
//
// Transport grew a third method, so every implementation needs one. They live
// in a dozen different test files, so the methods are gathered here instead:
// Go attaches a method to a type by package, not by file, and one place to look
// beats a dozen one-line edits scattered through the suite.
//
// ALL THE STUBS REFUSE. Returning false means "not delivered" -- the same answer
// the fake network gives for an unreachable peer -- so a leader releases the
// peer's replication slot and retries on its next round. The reply is left at
// its zero value, which cannot depose anyone: term 0 is never above a live
// node's currentTerm.
//
// That is the right default because none of these fixtures compacts. A stub
// that answered success would be claiming a follower had installed an image
// nobody sent, and would credit matchIndex for entries that never moved.
// Anything wanting real snapshot delivery uses the fake network below.

func (s *stubTransport) SendInstallSnapshot(to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return false
}

func (r *recordingTransport) SendInstallSnapshot(to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return false
}

func (g *gatedTransport) SendInstallSnapshot(to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return false
}

func (lf *liveFollower) SendInstallSnapshot(to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return false
}

// =============================================================================
// The fake network delivers them for real
// =============================================================================

// SendInstallSnapshot mirrors SendAppendEntries exactly: same routing, same
// delay, same reorder stamping, same round trip through gob in both directions
// so sender and receiver never share memory.
//
// The image is gob-encoded like any other payload, which is worth noticing --
// it means the ten-thousand-entry test really does serialise a whole state
// machine on every send, rather than passing a pointer and calling it a
// transfer.
func (e *endpoint) SendInstallSnapshot(to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	target, delay, seq, dup, ok := e.net.route(kindInstallSnapshot, e.from, to)
	if !ok {
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	e.net.arrive(e.from, to, seq)

	var argsCopy InstallSnapshotArgs
	mustRoundTrip(args, &argsCopy)

	if dup {
		// A duplicated InstallSnapshot exercises real, already-tested
		// receiver logic rather than a new failure mode of its own --
		// TestInstallSnapshotIgnoresAnImageAlreadyPassed already covers
		// a follower asked to install an image it has already passed.
		// See harness_test.go's SendRequestVote for exactly what "a
		// second time" means here and why the duplicate's own reply is
		// discarded.
		var dupArgs InstallSnapshotArgs
		mustRoundTrip(args, &dupArgs)
		e.net.countInstallSnapshot(len(dupArgs.Data))
		var discard InstallSnapshotReply
		target.InstallSnapshot(&dupArgs, &discard)
	}

	e.net.countInstallSnapshot(len(argsCopy.Data))

	var replyCopy InstallSnapshotReply
	target.InstallSnapshot(&argsCopy, &replyCopy)

	if !e.net.replyDeliverable(kindInstallSnapshot, e.from, to, seq) {
		return false
	}
	mustRoundTrip(&replyCopy, reply)
	return true
}

func (fn *fakeNetwork) countInstallSnapshot(bytes int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.installSnapshotRPCs++
	fn.snapshotBytes += bytes
}

// installSnapshots reports how many images were delivered. A test that expects
// a follower to be repaired by one and sees zero has not proved what it thinks:
// the follower caught up by ordinary replication, so nothing about compaction
// was exercised.
func (fn *fakeNetwork) installSnapshots() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.installSnapshotRPCs
}

// snapshotTraffic reports the total image bytes put on the wire, which is the
// number that says whether chunking is going to become necessary.
func (fn *fakeNetwork) snapshotTraffic() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.snapshotBytes
}