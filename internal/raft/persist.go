package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
)

// =============================================================================
// The record
// =============================================================================

// persistentState is exactly Figure 2's "Persistent state on all servers",
// and nothing else.
//
// commitIndex and lastApplied are absent on purpose. Figure 2 lists them as
// volatile, and reinitialising them to zero on restart is not a concession --
// it is required. commitIndex is recoverable from the leader's next
// AppendEntries; lastApplied must restart at zero because the state machine
// also restarts empty, and the applier replays the whole log to rebuild it.
// Persisting lastApplied without persisting the state machine alongside it
// would skip the replay and leave the node serving an empty map.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// LastIncludedIndex is the Raft index that Log[0] stands for.
	//
	// WITHOUT IT THE LOG IS AMBIGUOUS. Once compaction can shorten the log, the
	// slice is relative: position 0 is the floor, not index 0. A four-entry
	// record could be the full log of a node that has never snapshotted, or the
	// tail of one whose floor sits at 6, and nothing else on disk distinguishes
	// them. Reading the second as the first silently renumbers every entry.
	//
	// The floor's TERM needs no field of its own: Log[0] is the floor entry and
	// already carries it.
	//
	// A record written before this field existed decodes it as zero, which is
	// exactly "nothing discarded" -- the correct reading of an absolute log.
	// That is why the record version does not change.
	LastIncludedIndex int
}

const (
	stateMagic   = "HLRS" // Helios Raft State
	stateVersion = uint32(1)
	headerLen    = 4 + 4 + 4 + 4 // magic, version, payload length, payload CRC
)

// ErrCorruptState means a blob exists but is not a state record this build can
// trust.
//
// This must never be treated as "start fresh". A node that responds to
// unreadable state by resetting to term 0 with no vote has invented permission
// to vote twice in a term it already voted in -- the exact failure persistence
// exists to prevent. Refusing to start is the correct behaviour: an operator
// can then decide to wipe the directory deliberately, which is a different act
// from the process deciding it for them.
var ErrCorruptState = errors.New("raft: persistent state is corrupt")

// encodeState frames the record as:
//
//	magic[4] | version[4] | len(payload)[4] | crc32(payload)[4] | payload
//
// FileStorage's rename already rules out torn records, so the checksum is
// belt-and-braces: it catches bit rot, a truncating filesystem that does not
// honour the ordering it claims, and the much more likely case of someone
// pointing a node at the wrong directory.
func encodeState(ps persistentState) ([]byte, error) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(ps); err != nil {
		return nil, fmt.Errorf("raft: encoding persistent state: %w", err)
	}
	body := payload.Bytes()

	out := make([]byte, 0, headerLen+len(body))
	out = append(out, stateMagic...)
	out = binary.BigEndian.AppendUint32(out, stateVersion)
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
	out = append(out, body...)
	return out, nil
}

func decodeState(b []byte) (persistentState, error) {
	var ps persistentState

	if len(b) < headerLen {
		return ps, fmt.Errorf("%w: record is %d bytes, header is %d", ErrCorruptState, len(b), headerLen)
	}
	if string(b[0:4]) != stateMagic {
		return ps, fmt.Errorf("%w: bad magic %q", ErrCorruptState, b[0:4])
	}
	if v := binary.BigEndian.Uint32(b[4:8]); v != stateVersion {
		return ps, fmt.Errorf("%w: record version %d, this build reads %d", ErrCorruptState, v, stateVersion)
	}
	n := binary.BigEndian.Uint32(b[8:12])
	want := binary.BigEndian.Uint32(b[12:16])
	body := b[headerLen:]
	if uint32(len(body)) != n {
		return ps, fmt.Errorf("%w: payload is %d bytes, header claims %d", ErrCorruptState, len(body), n)
	}
	if got := crc32.ChecksumIEEE(body); got != want {
		return ps, fmt.Errorf("%w: checksum %08x, header claims %08x", ErrCorruptState, got, want)
	}

	// Decode into a zero value. gob omits zero fields on the wire, so the
	// target's prior contents would survive as phantom defaults.
	if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&ps); err != nil {
		return persistentState{}, fmt.Errorf("%w: %v", ErrCorruptState, err)
	}
	return ps, nil
}

// =============================================================================
// Node side
// =============================================================================

// markDirty records that currentTerm, votedFor or log has changed since the
// last durable write. Callers must hold n.mu.
//
// Every assignment to those three fields is followed by this call, and nothing
// else calls it. It belongs inside whatever guard decides that the value really
// changed: an unguarded markDirty in becomeFollower costs one fsync per
// heartbeat, because a same-term step-down reaches it without changing anything
// persistent.
func (n *Node) markDirty() {
	n.persistDirty = true
}

// persistIfDirty flushes state to stable storage if anything persistent has
// changed. Callers must hold n.mu.
//
// WHERE THIS GOES. In an RPC handler, as `defer n.persistIfDirty()` placed
// immediately after `defer n.mu.Unlock()`. Defers run last-in-first-out, so it
// executes while the lock is still held and before the handler returns -- which
// is before the caller can read *reply. One line covers every return path, and
// keeps covering the ones added later.
//
// Two places need an explicit call instead, because something happens mid-body
// that must not precede the flush:
//
//   - becomeCandidate, before `go n.runElection(...)`: a candidate that crashes
//     after its RequestVote goes out and returns at the old term will vote a
//     second time in the term it already voted in.
//   - appendAndReplicate, between the append and advanceCommitIndex: with no
//     peers the local append IS the majority, so the entry is committed and
//     applied before any fan-out happens.
//
// And one that has no reply to gate: stepDownIfStale, where the flag would
// otherwise survive the unlock and never be written if no further RPC arrives.
//
// The flag exists so that a handler which mutates twice -- becomeFollower for
// the term, then mergeEntries for the log -- costs one fsync rather than two.
//
// It runs under n.mu, so a slow disk stalls the whole node. That is the correct
// trade for now: the alternative is releasing the lock around the write, which
// admits a second mutation between the encode and the Save and puts a record on
// disk that no node ever held. Batching belongs with group commit, where the
// reply is deferred alongside the write.
func (n *Node) persistIfDirty() {
	if !n.persistDirty {
		return
	}
	n.persist()
	n.persistDirty = false
}

// persist writes the current persistent state unconditionally.
// Callers must hold n.mu.
func (n *Node) persist() {
	b, err := encodeState(persistentState{
		CurrentTerm:       n.currentTerm,
		VotedFor:          n.votedFor,
		Log:               n.log,
		LastIncludedIndex: n.lastIncludedIndex,
	})
	if err != nil {
		panic(fmt.Sprintf("raft: node %d cannot encode its own state: %v", n.id, err))
	}
	if err := n.storage.Save(b); err != nil {
		// A node that cannot make its state durable must stop participating
		// this instant. Logging and carrying on would let it grant a vote it
		// cannot remember, which is indistinguishable from having no
		// persistence at all -- except that the operator now believes it does.
		//
		// panic is the blunt version. Phase F should replace it with a halt
		// that keeps the process alive to serve a health endpoint reporting
		// why it stopped.
		panic(fmt.Sprintf("raft: node %d cannot persist state: %v", n.id, err))
	}
}

// =============================================================================
// Construction
// =============================================================================

// loadState reads and validates a storage's contents without touching a Node.
//
// found is false -- with a nil error -- when nothing has ever been saved. That
// is a genuinely fresh node, and the caller keeps NewNode's defaults.
//
// This takes a Storage rather than a *Node on purpose. NewNode starts the
// applier, so from the moment a Node exists there is no unsynchronised window
// in which to install state, and a Node built before validation would leak that
// goroutine on the error path with no handle left to stop it. Validating first
// makes both problems structurally impossible instead of merely locked around.
func loadState(s Storage) (persistentState, bool, error) {
	b, err := s.Load()
	if err != nil {
		return persistentState{}, false, fmt.Errorf("raft: reading persistent state: %w", err)
	}
	if b == nil {
		return persistentState{}, false, nil
	}
	ps, err := decodeState(b)
	if err != nil {
		return persistentState{}, false, err
	}
	return ps, true, nil
}

// adoptState installs a loaded record over a freshly constructed Node.
// Callers must hold n.mu.
func (n *Node) adoptState(ps persistentState) {
	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	n.log = ps.Log

	// THE FLOOR GOES IN BEFORE ANYTHING READS THE LOG BY INDEX. Every accessor
	// in log.go translates through lastIncludedIndex, so installing the log
	// while the floor still says 0 would renumber every entry for as long as
	// that window lasted.
	n.lastIncludedIndex = ps.LastIncludedIndex

	if len(n.log) == 0 {
		// Defensive: the floor entry should always have been saved, but
		// lastLogTerm indexes log[len-1] unguarded and an empty slice there is
		// a panic rather than a wrong answer. A log with no floor cannot be
		// positioned, so it is treated as a fresh one.
		n.log = []LogEntry{{Term: 0}}
		n.lastIncludedIndex = 0
	}

	// log[0] IS the floor entry, so its term is the floor's term. Deriving it
	// rather than storing it twice means the two can never disagree.
	n.lastIncludedTerm = n.log[0].Term

	n.persistDirty = false
}

// OpenNode builds a Node backed by storage, adopting whatever state that
// storage already holds.
//
// This is the constructor a durable server uses. NewNode stays as it was, so
// nothing that never restarts has to change.
//
// If the saved state cannot be read, OpenNode returns the error and no Node.
// See ErrCorruptState.
func OpenNode(id int, peers []int, transport Transport, seed int64, storage Storage) (*Node, error) {
	// Validate before constructing anything: see loadState.
	ps, found, err := loadState(storage)
	if err != nil {
		return nil, fmt.Errorf("raft: node %d: %w", id, err)
	}

	// The snapshot is read here too, so that an unreadable one fails before a
	// Node exists rather than after. Both records are validated in isolation
	// first; they are reconciled below, once the log is positioned.
	snap, hasSnap, err := loadSnapshot(storage)
	if err != nil {
		return nil, fmt.Errorf("raft: node %d: %w", id, err)
	}

	n := NewNode(id, peers, transport, seed)

	// NewNode has already started the applier, so every field write from here
	// is shared-memory access and takes the lock -- the storage handle
	// included, even though nothing reads it yet.
	n.mu.Lock()
	n.storage = storage
	if found {
		n.adoptState(ps)
	}
	if hasSnap {
		// The two records are written separately and carry their own floors.
		// Only their combination says whether this node's history is intact.
		if err := n.reconcileSnapshot(snap); err != nil {
			n.mu.Unlock()
			n.Stop()
			return nil, fmt.Errorf("raft: node %d: %w", id, err)
		}
		n.pendingSnapshot = &snap
	}

	// Everything at or below the floor is committed and applied by definition:
	// that is what taking a snapshot means. With no snapshot the floor is 0 and
	// this is the behaviour it always had.
	n.commitIndex = n.lastIncludedIndex
	n.lastApplied = n.lastIncludedIndex

	n.mu.Unlock()

	return n, nil
}
