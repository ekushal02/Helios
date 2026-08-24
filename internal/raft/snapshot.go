package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// =============================================================================
// The record
// =============================================================================

// Snapshot is an image of the replicated state machine, plus the two fields
// that say where in the log that image sits.
//
// WHY THE TWO FIELDS TRAVEL WITH THE BYTES. A state-machine image on its own is
// uninterpretable: a node holding it cannot say which entries it accounts for,
// so it cannot know what to ask a leader for, and a leader cannot know what to
// send. LastIncludedIndex answers that.
//
// LastIncludedTerm is the one people leave out, and it is not derivable from
// anything the node still has. A leader may send AppendEntries with
// PrevLogIndex equal to the floor, and the receiver has to answer the
// consistency check at that boundary -- but the entry whose term the check needs
// was discarded when the snapshot was taken. The header is the only surviving
// record of it. Without the field a follower would reject every check at the
// floor and could never be repaired past it.
//
// WHY THIS IS A SEPARATE RECORD FROM THE FIGURE 2 STATE. Section 5 of DESIGN.md
// argues that currentTerm, votedFor and the log belong in one opaque blob so
// they cannot become durable separately. The snapshot is the deliberate
// exception: that blob is rewritten on every term change and every append, and
// folding a multi-megabyte image into it would make granting a vote cost a full
// state-machine rewrite. The atomicity a single blob gave for free is replaced
// by the ordering rule on Storage.SaveSnapshot.
type Snapshot struct {
	// LastIncludedIndex is the index of the last entry the image accounts for.
	// Every entry at or below it has been applied and may be discarded from the
	// log. Zero means no snapshot: the floor is the sentinel, which is where
	// every node starts.
	LastIncludedIndex int

	// LastIncludedTerm is that entry's term. See above.
	LastIncludedTerm int

	// Data is whatever the state machine produced. Opaque here -- Raft never
	// interprets it, exactly as it never interprets LogEntry.Command.
	//
	// Empty is legal and is not the same as absent: a state machine whose keys
	// have all been deleted has an empty image at a real index.
	Data []byte
}

const (
	snapshotMagic   = "HLSN" // Helios Snapshot
	snapshotVersion = uint32(1)

	// magic, version, payload length, payload CRC.
	snapshotHeaderLen = 4 + 4 + 4 + 4

	// LastIncludedIndex and LastIncludedTerm, big-endian, ahead of the data.
	snapshotFieldsLen = 8 + 8
)

// ErrCorruptSnapshot means a snapshot exists but is not one this build can
// trust. Fatal, for the same reason as ErrCorruptState: a node that treats an
// unreadable snapshot as no snapshot has silently rewound its own state machine
// to empty while continuing to claim the identity that promised otherwise.
var ErrCorruptSnapshot = errors.New("raft: snapshot is corrupt")

// encodeSnapshot frames the record as
//
//	magic[4] | version[4] | len(payload)[4] | crc32(payload)[4] | payload
//	payload = lastIncludedIndex[8] | lastIncludedTerm[8] | data
//
// Framed by hand rather than through gob. The data is the whole state machine
// and may be large; gob would reflect over it and copy it a second time for no
// benefit, since the layout here is three fixed fields and a byte run.
//
// The checksum costs a pass over the image. That is per snapshot, not per write
// -- snapshots are taken on a log-length threshold, not on the hot path -- and
// it is the only thing standing between a bit flip in a large, long-lived file
// and a state machine restored from garbage.
func encodeSnapshot(s Snapshot) ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}

	payload := make([]byte, 0, snapshotFieldsLen+len(s.Data))
	payload = binary.BigEndian.AppendUint64(payload, uint64(s.LastIncludedIndex))
	payload = binary.BigEndian.AppendUint64(payload, uint64(s.LastIncludedTerm))
	payload = append(payload, s.Data...)

	out := make([]byte, 0, snapshotHeaderLen+len(payload))
	out = append(out, snapshotMagic...)
	out = binary.BigEndian.AppendUint32(out, snapshotVersion)
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)))
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(payload))
	out = append(out, payload...)
	return out, nil
}

func decodeSnapshot(b []byte) (Snapshot, error) {
	var s Snapshot

	if len(b) < snapshotHeaderLen {
		return s, fmt.Errorf("%w: record is %d bytes, header is %d",
			ErrCorruptSnapshot, len(b), snapshotHeaderLen)
	}
	if string(b[0:4]) != snapshotMagic {
		return s, fmt.Errorf("%w: bad magic %q", ErrCorruptSnapshot, b[0:4])
	}
	if v := binary.BigEndian.Uint32(b[4:8]); v != snapshotVersion {
		return s, fmt.Errorf("%w: record version %d, this build reads %d",
			ErrCorruptSnapshot, v, snapshotVersion)
	}

	n := binary.BigEndian.Uint32(b[8:12])
	want := binary.BigEndian.Uint32(b[12:16])
	payload := b[snapshotHeaderLen:]

	if uint32(len(payload)) != n {
		return s, fmt.Errorf("%w: payload is %d bytes, header claims %d",
			ErrCorruptSnapshot, len(payload), n)
	}
	if got := crc32.ChecksumIEEE(payload); got != want {
		return s, fmt.Errorf("%w: checksum %08x, header claims %08x",
			ErrCorruptSnapshot, got, want)
	}
	if len(payload) < snapshotFieldsLen {
		return s, fmt.Errorf("%w: payload is %d bytes, the two index fields are %d",
			ErrCorruptSnapshot, len(payload), snapshotFieldsLen)
	}

	s.LastIncludedIndex = int(binary.BigEndian.Uint64(payload[0:8]))
	s.LastIncludedTerm = int(binary.BigEndian.Uint64(payload[8:16]))

	// Copied, not aliased. The caller owns this and the buffer it came from may
	// be a reusable read buffer.
	s.Data = append([]byte(nil), payload[snapshotFieldsLen:]...)

	if err := s.validate(); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}

// validate rejects headers that no correct node could have written.
//
// The checksum catches damage in transit or at rest. This catches the other
// failure: a caller that built the record with a field unset. A zero index is
// not a snapshot of nothing, it is a snapshot nobody filled in, and adopting it
// would move a node's floor backwards to the sentinel.
func (s Snapshot) validate() error {
	if s.LastIncludedIndex < 1 {
		return fmt.Errorf("%w: LastIncludedIndex is %d, want at least 1 -- a "+
			"snapshot covering no entries is not a snapshot",
			ErrCorruptSnapshot, s.LastIncludedIndex)
	}
	// Only the sentinel has term 0, and the sentinel is at index 0, so a real
	// entry at index >= 1 always carries a term >= 1. A zero here means the
	// field was never set.
	if s.LastIncludedTerm < 1 {
		return fmt.Errorf("%w: LastIncludedTerm is %d at index %d, want at "+
			"least 1 -- only the sentinel has term 0",
			ErrCorruptSnapshot, s.LastIncludedTerm, s.LastIncludedIndex)
	}
	return nil
}

// covers reports whether this snapshot accounts for the given log index.
func (s Snapshot) covers(index int) bool { return index <= s.LastIncludedIndex }

// =============================================================================
// Node side
// =============================================================================

// snapshotFloor returns the index and term of the last entry discarded into a
// snapshot, or (0, 0) for a node that has never taken one.
//
// (0, 0) is not a special case dressed up as one: it is exactly the sentinel at
// log[0], whose index is 0 and whose term is 0. A node with no snapshot has its
// floor at the sentinel, which is why every existing consistency check at index
// 0 already does the right thing and why the index-offset work to come is a
// generalisation rather than a rewrite.
//
// Caller must hold mu.
func (n *Node) snapshotFloor() (index, term int) {
	return n.lastIncludedIndex, n.lastIncludedTerm
}

// hasSnapshot reports whether any entries have been discarded.
// Caller must hold mu.
func (n *Node) hasSnapshot() bool { return n.lastIncludedIndex > 0 }

// loadSnapshot reads and validates the stored snapshot without touching a Node.
//
// found is false, with a nil error, when none has been written. Every other
// failure is an error: see ErrCorruptSnapshot.
func loadSnapshot(s Storage) (Snapshot, bool, error) {
	b, err := s.LoadSnapshot()
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("raft: reading snapshot: %w", err)
	}
	if b == nil {
		return Snapshot{}, false, nil
	}
	snap, err := decodeSnapshot(b)
	if err != nil {
		return Snapshot{}, false, err
	}
	return snap, true, nil
}

// reconcileSnapshot brings the two durable records into agreement.
//
// Both carry a floor, written at different moments by different Saves, and only
// their combination says whether this node's history is intact. Caller must
// hold mu, and must have called adoptState first so the log is positioned.
//
// The three cases are not symmetric, because the write order is not symmetric:
// SaveSnapshot always completes before the Save that discards the entries it
// covers.
func (n *Node) reconcileSnapshot(snap Snapshot) error {
	switch {
	case snap.LastIncludedIndex < n.lastIncludedIndex:
		// The log was compacted PAST the snapshot meant to cover it. No correct
		// sequence of writes produces this: entries are missing and the only
		// thing that accounted for them is older than the truncation. Refusing
		// to start is the honest response.
		return fmt.Errorf("raft: log floor is %d but the newest snapshot only "+
			"covers through %d: entries were discarded with nothing to account "+
			"for them", n.lastIncludedIndex, snap.LastIncludedIndex)

	case snap.LastIncludedIndex == n.lastIncludedIndex:
		// A clean compaction: both records agree where the floor sits. The
		// terms are written independently -- one in the snapshot header, one as
		// log[0] of the state record -- so they are worth comparing.
		if n.lastIncludedTerm != snap.LastIncludedTerm {
			return fmt.Errorf("raft: floor at index %d has term %d in the log "+
				"and term %d in the snapshot: the two records come from "+
				"different histories",
				snap.LastIncludedIndex, n.lastIncludedTerm, snap.LastIncludedTerm)
		}
		return nil

	default:
		// The snapshot is AHEAD of the log's floor: the crash landed between
		// SaveSnapshot and the Save that would have shortened the log. The log
		// therefore still holds everything the image covers -- redundant, not
		// contradictory -- and recovery finishes the job that was interrupted.
		if n.lastLogIndex() < snap.LastIncludedIndex {
			return fmt.Errorf("raft: log ends at index %d but the snapshot "+
				"claims through %d: the log was truncated without a snapshot "+
				"to cover it", n.lastLogIndex(), snap.LastIncludedIndex)
		}
		// The entry is still present, so this compares two independent records
		// rather than a value against itself. It is the last moment it can.
		if got := n.termAt(snap.LastIncludedIndex); got != snap.LastIncludedTerm {
			return fmt.Errorf("raft: log[%d] has term %d, snapshot floor claims "+
				"term %d: the two records come from different histories",
				snap.LastIncludedIndex, got, snap.LastIncludedTerm)
		}

		n.compactTo(snap.LastIncludedIndex, snap.LastIncludedTerm)
		return nil
	}
}

// =============================================================================
// Taking one
// =============================================================================

// defaultSnapshotThreshold is how many entries may sit above the floor before
// the node asks the state machine for an image.
//
// Deliberately high. Every existing test in this package runs with logs far
// shorter than this, so turning compaction on changes nothing they assert --
// which matters more than usual right now, because a truncated log cannot yet
// repair a lagging follower (see buildAppendEntries). Tests that want
// compaction set their own threshold.
const defaultSnapshotThreshold = 2000

// SetSnapshotThreshold changes how many entries above the floor trigger the
// signal. Zero or negative disables compaction entirely.
func (n *Node) SetSnapshotThreshold(entries int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotThreshold = entries
}

// SnapshotNotify fires when the log has grown past the threshold and the state
// machine should hand back an image.
//
// ADVISORY, AND A SECOND UPWARD CHANNEL. DESIGN.md §2 says ApplyMsg is the only
// channel through which Raft talks upward, and the reason given is ordering: a
// snapshot and the entries after it are one sequence, and two channels would
// let the state machine see them interleaved. That argument is about DATA. This
// channel carries none -- it is a hint, it can be dropped, and dropping it
// costs a delayed compaction rather than a wrong answer. Capacity 1 with a
// non-blocking send, exactly like applyNotify: a token already sitting there
// means a request is pending, so a second one adds nothing.
//
// The state machine's obligation: take its own image at the index it has
// applied through, and call Snapshot(index, data). It may ignore this
// completely, in which case the log keeps growing and nothing breaks.
func (n *Node) SnapshotNotify() <-chan struct{} {
	return n.snapshotNotify
}

// maybeSignalSnapshot drops a token if enough entries have become discardable.
// Never blocks, so it is safe under mu. Caller must hold mu.
//
// Called from the applier after lastApplied advances, which is the only moment
// the answer can change in the direction that matters.
//
// IT MEASURES WHAT CAN BE DISCARDED, NOT WHAT IS HELD, and the distinction is
// the difference between compacting fifty times and compacting ten thousand.
//
// A snapshot may only cover entries the state machine has applied, so a
// compaction removes exactly lastApplied - lastIncludedIndex entries. Measuring
// the whole log instead -- everything above the floor, applied or not -- looks
// equivalent and thrashes: if the unapplied tail is by itself longer than the
// threshold, then after compacting to lastApplied the log is STILL over the
// line, the next applied entry signals again, and the node takes one complete
// image per entry for as long as the write load lasts. On a ten thousand entry
// run that was nine thousand four hundred images instead of fifty.
//
// The compactable count, by contrast, is driven to zero by every compaction, so
// the next signal is genuinely one threshold of progress away.
func (n *Node) maybeSignalSnapshot() {
	if n.snapshotThreshold <= 0 {
		return
	}
	if n.lastApplied-n.lastIncludedIndex < n.snapshotThreshold {
		return
	}

	select {
	case n.snapshotNotify <- struct{}{}:
	default:
	}
}

// Snapshot installs a state-machine image and discards the log behind it.
//
// index is the last log index the image accounts for; data is whatever the
// state machine produced. Raft never interprets data, exactly as it never
// interprets LogEntry.Command.
//
// THE ORDER IS THE CORRECTNESS ARGUMENT.
//
//  1. The image is made durable.
//  2. Only then is the log truncated in memory.
//  3. Only then is the shortened log written.
//
// A crash between 1 and 3 leaves a snapshot beside a log that still covers it:
// redundant, and recovery drops the overlap. The reverse order would leave
// entries discarded with nothing accounting for them, which is lost committed
// state with nothing left on disk that knows. See Storage.SaveSnapshot.
//
// A stale call is a no-op rather than an error. The state machine may be
// signalled twice, or may take its image at an index a later snapshot has
// already passed; neither is a fault, and neither may move the floor backwards.
func (n *Node) Snapshot(index int, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index <= n.lastIncludedIndex {
		return nil // already covered
	}

	// THE GUARD IS commitIndex, NOT lastApplied, AND THAT IS NOT A LOOSENING.
	//
	// What must never happen is an image covering entries that are not
	// committed: those can still be overwritten by a future leader, and an
	// image containing them would make the overwrite unrecoverable.
	//
	// lastApplied cannot serve as that guard, because it lags the caller by a
	// mutex acquisition. The applier hands a message to the state machine and
	// only then reacquires n.mu to record the delivery, so a machine that
	// snapshots the moment it applies is routinely ONE INDEX AHEAD of Raft's
	// own bookkeeping. Refusing on that basis rejects a correct caller using a
	// value known to be stale -- and every state machine written against this
	// API would hit it.
	//
	// The caller is authoritative about what it applied. It could only have
	// obtained entry `index` from this node, and only if the entry was
	// committed. So the check that survives is the one that matters.
	if index > n.commitIndex {
		return fmt.Errorf("raft: snapshot at index %d but only %d is committed",
			index, n.commitIndex)
	}
	if !n.hasEntryAt(index) {
		return fmt.Errorf("raft: snapshot at index %d, which the log does not hold "+
			"(floor %d, last %d)", index, n.lastIncludedIndex, n.lastLogIndex())
	}

	// Read the term BEFORE anything is discarded. After compactTo the entry is
	// gone and this is the only surviving record of it.
	term := n.termAt(index)

	blob, err := encodeSnapshot(Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              data,
	})
	if err != nil {
		return err
	}

	// Step 1. Durable before anything is thrown away.
	//
	// Under mu, so a large image stalls the node for the length of the write.
	// That is the same trade persistIfDirty makes and for the same reason:
	// releasing the lock admits a mutation between reading the log and
	// truncating it, which would put a floor on disk that does not match the
	// entries left behind. Compaction is rare enough that the stall is
	// affordable; if it stops being so, the fix is to build the image under the
	// lock and write it outside, not to loosen this.
	if err := n.storage.SaveSnapshot(blob); err != nil {
		return fmt.Errorf("raft: saving snapshot at index %d: %w", index, err)
	}

	discarded := index - n.lastIncludedIndex

	// Step 2, then step 3.
	n.compactTo(index, term)
	n.baseServers = append([]int(nil), n.servers...)

	// The caller has told us it applied through here, and it is the authority
	// on that. Raft's own record may still be one behind; leaving it there
	// would put lastApplied below the floor, and the applier would then try to
	// rebuild a batch from entries this call has just discarded.
	if index > n.lastApplied {
		n.lastApplied = index
	}

	n.markDirty()
	n.persistIfDirty()

	n.lg().Info("compacted log",
		"lastIncludedIndex", index, "lastIncludedTerm", term,
		"entriesDiscarded", discarded, "entriesRemaining", n.logLength(),
		"imageBytes", len(data))

	return nil
}
