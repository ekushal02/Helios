package raft

// NodeStatus is a point-in-time snapshot of this node's own Raft
// state -- everything an admin caller (internal/server's Status RPC,
// F-9) needs to report cluster health, taken under ONE lock
// acquisition so every field is mutually consistent as of one instant,
// not stitched together from several separately-locked reads that
// could each observe a different moment (a term that advanced between
// reading CommitIndex and reading LeaderID would be exactly the kind
// of internally-contradictory report this avoids).
type NodeStatus struct {
	ID    int
	State State
	Term  int

	// LeaderID is the peer this node currently believes leads, or
	// None (-1) if it has no belief -- the identical "hint, not
	// authority" meaning LeaderHint's own doc already gives this
	// field one layer up; Status reports the same underlying value,
	// just bundled here with everything else rather than fetched via
	// a second call.
	LeaderID int

	CommitIndex int
	LastApplied int

	// LogLength is len(n.log): how many entries this node currently
	// holds in memory, INCLUDING the one leading placeholder entry
	// every Node keeps (representing "the entry immediately before
	// the log's own first real index," present even on a node that
	// has never taken a snapshot -- see NewNode's own initialization).
	// LogLength - 1 is the count of real, submitted entries currently
	// retained; the raw value is reported here rather than adjusted,
	// since what an admin caller most likely wants from this field is
	// this node's actual in-memory footprint, which the placeholder is
	// genuinely part of.
	LogLength int

	// SnapshotIndex and SnapshotTerm are 0, 0 if this node has never
	// taken or installed a snapshot -- the identical zero-value
	// contract SnapshotFloor already has one layer down; Status
	// reports the same pair, bundled.
	SnapshotIndex int
	SnapshotTerm  int

	// Voters is the cluster membership this node currently believes
	// is in force -- every voting member's own ID, this node included
	// if it is one. A copy, not a reference to n's own internal slice:
	// safe for a caller to read after this method has returned and
	// n's own configuration has since changed.
	Voters []int
}

// Status returns a snapshot of this node's own state. Like LeaderHint
// and SnapshotFloor, this is a HINT about a moving system, not a
// promise that anything reported here is still true the instant after
// the call returns -- an admin caller polling several nodes is
// necessarily looking at several different, slightly-offset instants
// in time, not one coherent cluster-wide snapshot; that is inherent to
// asking distributed, independently-progressing nodes one at a time,
// not a limitation of this method specifically.
func (n *Node) Status() NodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	return NodeStatus{
		ID:            n.id,
		State:         n.state,
		Term:          n.currentTerm,
		LeaderID:      n.leaderID,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		LogLength:     len(n.log),
		SnapshotIndex: n.lastIncludedIndex,
		SnapshotTerm:  n.lastIncludedTerm,
		Voters:        append([]int(nil), n.servers...),
	}
}
