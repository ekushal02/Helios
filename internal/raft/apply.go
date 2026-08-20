package raft

// ApplyMsg is one committed entry handed up to the state machine.
//
// This is the only channel through which Raft talks upward. Everything below
// this line is agreement; everything above it is the key-value store. The layer
// above never reads n.log, n.commitIndex or n.lastApplied -- if it did, it would
// be re-deriving an order this file has already established, and the two
// derivations would eventually disagree.
//
// CommandValid exists so the message type can carry things that are not
// commands. Read barriers send a false one: the index must reach the state
// machine so a reader can observe it, but there is nothing to apply. Installed
// snapshots will be the second case (Phase F), and they must travel on THIS
// channel rather than a second one, because a snapshot and the entries after it
// are a single ordered sequence. Two channels would let the state machine see
// them interleaved.
//
// A consumer's obligation: on CommandValid false, advance your own index and
// apply nothing. Treating it as an error -- which is the natural first draft --
// makes every linearizable read look like a corrupt log.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	// TODO (F-*): SnapshotValid, Snapshot, SnapshotIndex, SnapshotTerm.
}

// -----------------------------------------------------------------------------
// Wiring
// -----------------------------------------------------------------------------

// initApplier builds the apply plumbing and starts the one goroutine allowed to
// send on it. Called once, from NewNode, before the node is handed to a caller
// who may start consuming immediately.
func (n *Node) initApplier() {
	// UNBUFFERED, deliberately. A buffer would let Raft run ahead of the state
	// machine and quietly hide a consumer that has stalled: commitIndex keeps
	// climbing, memory keeps growing, and the first symptom is an out-of-memory
	// kill rather than a slow apply. Unbuffered means back-pressure reaches the
	// applier immediately -- and because the applier holds no lock while
	// sending, that back-pressure never reaches consensus. Elections and
	// replication run at full speed with a completely dead state machine.
	n.applyCh = make(chan ApplyMsg)

	// CAPACITY 1, and the capacity is the design. This is a flag meaning
	// "commitIndex moved since you last looked", not a queue of events. A full
	// buffer means a wake-up is already pending, so dropping the send loses
	// nothing: the applier reads the live commitIndex when it wakes and picks up
	// everything, including whatever arrived while the token sat there.
	//
	// A sync.Cond would also work and was the first draft. The reason this is a
	// channel: a Cond cannot be woken by a channel close, so it needs its own
	// shutdown flag plus a broadcast under mu to avoid a lost wake-up during
	// Stop. A channel lets the applier wait on work and on shutdown in one
	// select, and stopCh already exists.
	n.applyNotify = make(chan struct{}, 1)

	n.applierDone = make(chan struct{})

	go n.applier()
}

// ApplyCh returns the channel of committed entries, in index order.
//
// EXACTLY ONE GOROUTINE MAY RECEIVE FROM THIS.
//
// The channel itself is ordered, so a pool of consumers looks safe. It is not.
// Two receivers take messages 4 and 5 in order and then apply them in whatever
// order the scheduler picks, so the state machine sees 5 before 4 and two
// replicas of the same log reach different states. What this file guarantees is
// DELIVERY order; only a single consumer turns that into APPLICATION order.
//
// The channel is closed when the applier exits, which happens once, on Stop. A
// consumer written as `for msg := range n.ApplyCh()` therefore terminates
// cleanly at shutdown with nothing else to wire up.
func (n *Node) ApplyCh() <-chan ApplyMsg {
	return n.applyCh
}

// -----------------------------------------------------------------------------
// The commit point
// -----------------------------------------------------------------------------

// commitTo raises commitIndex to idx and wakes the applier.
//
// Every advance of commitIndex goes through here: the leader counting a majority
// (C-10) and, once it lands, the follower obeying LeaderCommit (C-11). The point
// is that there is no way to advance commitIndex and forget to signal -- a bug
// that fails no test and simply delays every apply until some unrelated commit
// happens to wake the goroutine. That is the kind of fault that reads as "the
// system is a bit slow sometimes" for a year.
//
// Caller must hold mu.
func (n *Node) commitTo(idx int) {
	// Monotone. A lower idx is not an error: a stale LeaderCommit from a
	// reordered message legitimately arrives after a higher one, and the right
	// answer is to ignore it, not to un-commit. Nothing may ever move
	// commitIndex backwards -- an entry announced as committed has been
	// announced to a client.
	if idx <= n.commitIndex {
		return
	}

	// Believed impossible, hence Error. Committing past the end of the local log
	// means either the majority count read a matchIndex the log does not
	// support, or rule 5 dropped the min() against the last index the message
	// actually covered. Both are bugs in another file; this is where they
	// surface.
	if idx > n.lastLogIndex() {
		n.lg().Error("refusing to commit past the end of the log",
			"want", idx, "lastLogIndex", n.lastLogIndex())
		return
	}

	n.commitIndex = idx
	n.signalApplier()
}

// signalApplier drops a token in the notify channel, or does nothing if one is
// already there. Never blocks, so it is safe to call under mu.
func (n *Node) signalApplier() {
	select {
	case n.applyNotify <- struct{}{}:
	default:
	}
}

// -----------------------------------------------------------------------------
// The applier
// -----------------------------------------------------------------------------

// applier is the sole producer on applyCh, for the whole life of the node.
//
// WHY ONE GOROUTINE.
//
// The obvious alternative is to apply inline wherever commitIndex moves: the
// leader's reply handler notices a new majority and sends the entries itself.
// That breaks twice. First, the reply handler holds mu, and sending on an
// unbuffered channel while holding mu deadlocks the moment the state machine
// calls back into Raft -- which the key-value server will do on every read.
// Second, replies arrive on many goroutines, so two can be inside the apply path
// at once and the state machine sees entries out of order. Both problems
// dissolve if exactly one goroutine ever sends and it holds no lock while doing
// so.
//
// The shape is therefore: take a snapshot of the work under the lock, drop the
// lock, deliver, then sleep until there is more.
func (n *Node) applier() {
	// The applier is the only sender, which is what makes closing here safe.
	// Order matters: applyCh closes first, so a consumer ranging over it has
	// already seen the end of the stream by the time anything joins applierDone.
	defer close(n.applierDone)
	defer close(n.applyCh)

	for {
		// COPY THE ENTRIES OUT UNDER THE LOCK. This is the reason a batch exists
		// rather than reading n.log[i] just before each send.
		//
		// The instant mu is released, this node may receive an AppendEntries
		// that truncates the log (C-5) or reallocates its backing array by
		// appending. Reading n.log at send time would be a race in the plain
		// sense and a correctness bug in the interesting sense: the entry
		// delivered would not be the entry that was committed.
		//
		// Only the ApplyMsg structs are copied, so Command still aliases the
		// log's bytes. That is safe under the invariant in DESIGN.md, pinned by
		// TestSentEntryBytesAliasTheLogDeliberately: LogEntry.Command is
		// immutable once appended. Truncation drops an entry, it never edits one
		// in place.
		n.mu.Lock()
		first, last := n.lastApplied+1, n.commitIndex

		// THE LOG MUST COVER EVERYTHING COMMITTED, and if it does not, this
		// goroutine is where the violation becomes visible.
		//
		// Two invariants make this unreachable. commitTo refuses to raise
		// commitIndex past lastLogIndex, and no rule in Figure 2 truncates an
		// entry that some node has committed -- receiver rule 3 only ever
		// removes a conflicting tail, which by the Log Matching property is
		// uncommitted. So a shorter log than commitIndex means one of those two
		// is broken.
		//
		// Clamping rather than indexing anyway is the difference between a
		// logged fault on one node and a panic that takes down the process, in
		// a goroutine nobody owns and nobody can recover. The Error line is what
		// makes it a bug report rather than a silent swallow: search the log for
		// it, and the fault is in whoever moved commitIndex or shortened the
		// log, never in this file. Compare commitTo, which refuses the same
		// impossible state at the other end.
		if last > n.lastLogIndex() {
			n.lg().Error("commitIndex is past the end of the log",
				"commitIndex", last, "lastLogIndex", n.lastLogIndex(),
				"lastApplied", n.lastApplied)
			last = n.lastLogIndex()
		}

		batch := make([]ApplyMsg, 0, max(0, last-first+1))
		for i := first; i <= last; i++ {
			e := n.log[i]

			// A BARRIER IS DELIVERED, NOT SKIPPED. It carries no command, so
			// CommandValid is false and the state machine must not try to
			// interpret one -- but it must still arrive, because the reader is
			// waiting for the STATE MACHINE to reach this index, not for Raft
			// to. Filtering barriers out here would advance lastApplied while
			// leaving every consumer's own counter behind, and every read would
			// wait for an index that never shows up.
			batch = append(batch, ApplyMsg{
				CommandValid: !e.NoOp,
				Command:      e.Command,
				CommandIndex: i,
				CommandTerm:  e.Term,
			})
		}
		n.mu.Unlock()

		if len(batch) == 0 {
			// Nothing to do. Wait for a commit or for shutdown.
			//
			// A spurious wake is possible and harmless: the token may have been
			// left by a commit whose entries this goroutine already picked up on
			// the previous pass. The loop finds an empty batch and comes back
			// here.
			select {
			case <-n.applyNotify:
			case <-n.stopCh:
				return
			}
			continue
		}

		for _, msg := range batch {
			select {
			case n.applyCh <- msg:
			case <-n.stopCh:
				// Shutdown wins over delivery. The entries are still committed
				// and still in the log; a restarted node reapplies them from
				// index 1 (or from the snapshot, in Phase F). Blocking here for
				// a consumer that may never arrive would make Stop hang, and a
				// test suite that cannot stop a node cannot run the next test.
				return
			}

			// lastApplied advances only AFTER the message has been handed over.
			//
			// The cheaper alternative -- set lastApplied = last before dropping
			// the lock -- is a lie that surfaces two phases from now. lastApplied
			// is what a linearizable read consults to decide the state machine is
			// caught up (Phase D), and what a snapshot consults to decide which
			// entries may be discarded (Phase F). Both need the Figure 2 meaning:
			// applied, past tense. Reacquiring the lock per entry costs a mutex
			// hop against a channel send that already cost a scheduler round
			// trip.
			n.mu.Lock()
			if msg.CommandIndex != n.lastApplied+1 {
				// Believed impossible: this goroutine is the only writer of
				// lastApplied and it walks the batch in order. If this fires, a
				// second applier is running -- look for a duplicate initApplier.
				n.lg().Error("apply order broken",
					"delivered", msg.CommandIndex, "lastApplied", n.lastApplied)
			}
			n.lastApplied = msg.CommandIndex
			n.mu.Unlock()
		}
	}
}

// Submit hands back (index, term) and CommandTerm comes back out on this
// channel, so a client can already tell its own committed command from a peer's
// -- an index that reappears carrying a different term means the submission was
// overwritten. concurrent_test.go uses exactly that pair.
//
// TODO (D-*): a restarted node replays the log from index 1 through this same
// channel. The state machine must therefore apply the same prefix twice with the
// same result, or commands need identifiers so it can deduplicate. Whichever it
// is, decide it before the restart path exists. The same identifiers close the
// duplicate-command hole the concurrent test currently tolerates, which is why
// they are one decision and not two.