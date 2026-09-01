package raft

// SnapshotFloor reports the index and term of the most recent snapshot
// this node has taken or installed -- 0, 0 if none exists yet. Exposed
// for the first time by F-7 (internal/kvstore's Watch mechanism), which
// needs to know the floor below which its own retained watch history
// can never be complete: any log entry at or below this index has been
// compacted away and will never be redelivered through ApplyCh again
// (no prefix is ever applied twice, §5/§6), so a Watch request for a
// start_revision at or below it cannot be answered completely no
// matter how large the retained ring buffer is allowed to grow.
//
// The identical "hint, not authority, read under the same lock every
// other field uses" character LeaderHint's own doc already states:
// this can advance the instant after it is read (a new snapshot taken
// concurrently), so a caller using it to decide "is this request
// answerable" is making the same best-effort call LeaderHint's own
// caller does, not relying on it for a stronger guarantee than the
// field itself can offer. A caller that needs the guarantee to hold
// exactly at some instant (Machine.take, Machine.installSnapshot) calls
// this at the moment its own floor-advancing operation completes, not
// speculatively beforehand.
func (n *Node) SnapshotFloor() (index, term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotFloor()
}
