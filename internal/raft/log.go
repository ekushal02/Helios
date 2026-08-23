package raft

// =============================================================================
// Log indexing across a snapshot floor
// =============================================================================
//
// THE LAYOUT. n.log[0] is the FLOOR: the last entry discarded into a snapshot,
// retained for its term and nothing else. Real entries follow it. Slice
// position i holds Raft index lastIncludedIndex + i.
//
// This is not a new convention. Before any snapshot the floor is the sentinel
// at index 0 with term 0, and lastIncludedIndex is 0, so position equals index
// exactly as it always did. Compaction moves the floor off zero; it does not
// change the shape.
//
// THE SEAM. Every translation between a Raft index and a slice position happens
// in this file. Nothing else may index n.log directly, because a raw n.log[i]
// is silently wrong the moment the floor moves — and wrong in the worst
// direction, since a small positive offset still lands on a real entry and
// returns a plausible term. The invariant is greppable:
//
//	grep -n 'n\.log\[' internal/raft/*.go | grep -v _test.go
//
// Every hit must be inside this file. Appending (n.log = append(n.log, ...)) and
// taking the length are position operations and are fine anywhere.
//
// All of these require mu.

// firstLogIndex is the lowest index still held as a real entry.
//
// One past the floor, always. On a node with no snapshot that is 1, which is
// where the log has always started.
func (n *Node) firstLogIndex() int {
	return n.lastIncludedIndex + 1
}

// lastLogIndex is the index of the final entry, or the floor when every entry
// has been discarded.
func (n *Node) lastLogIndex() int {
	return n.lastIncludedIndex + len(n.log) - 1
}

// lastLogTerm is the term of the final entry, or the floor's term when the log
// holds nothing above it.
func (n *Node) lastLogTerm() int {
	return n.log[len(n.log)-1].Term
}

// logLength is how many real entries are held above the floor. This is what the
// compaction threshold measures — not lastLogIndex, which keeps rising after a
// snapshot has already thrown the entries away.
func (n *Node) logLength() int {
	return len(n.log) - 1
}

// hasEntryAt reports whether the entry for this index is still held.
//
// False for an index below the floor means "discarded, ask for a snapshot", not
// "never existed". Callers that can tell the difference must, because the two
// need opposite responses.
func (n *Node) hasEntryAt(index int) bool {
	return index >= n.firstLogIndex() && index <= n.lastLogIndex()
}

// isBelowFloor reports whether this index has been compacted away. The
// condition that means InstallSnapshot rather than a rejection.
func (n *Node) isBelowFloor(index int) bool {
	return index < n.lastIncludedIndex
}

// entryAt returns the entry at a Raft index.
//
// Believed-impossible out of range: every caller either checked hasEntryAt or
// derived the index from nextIndex, which is clamped. It logs rather than
// panicking, for the reason in DESIGN.md §8 — a panic here lands in a goroutine
// nobody owns.
func (n *Node) entryAt(index int) LogEntry {
	if !n.hasEntryAt(index) {
		n.lg().Error("log index out of range",
			"index", index, "floor", n.lastIncludedIndex, "last", n.lastLogIndex())
		return LogEntry{Term: -1}
	}
	return n.log[index-n.lastIncludedIndex]
}

// termAt returns the term of the entry at a Raft index, or of the floor.
//
// FAILS CLOSED. Out of range returns -1, which no real term can equal, so a
// consistency check run against a bug rejects and triggers repair instead of
// matching something it should not. Returning 0 would be far worse: 0 is the
// sentinel's term, so a bug would look like agreement at the bottom of the log.
func (n *Node) termAt(index int) int {
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm
	}
	if !n.hasEntryAt(index) {
		n.lg().Error("term requested for an index the log does not hold",
			"index", index, "floor", n.lastIncludedIndex, "last", n.lastLogIndex())
		return -1
	}
	return n.log[index-n.lastIncludedIndex].Term
}

// entriesFrom returns a COPY of every entry from index onward.
//
// A copy because the result goes to a network goroutine: a subslice would be a
// window into the live log, which a later append can reallocate and a
// truncation can rewrite. Shallow on purpose — Command bytes are immutable once
// appended. See DESIGN.md §10.
//
// An index at or below the floor returns nil; callers must check isBelowFloor
// first, because nil here is indistinguishable from "caught up".
func (n *Node) entriesFrom(index int) []LogEntry {
	if index <= n.lastIncludedIndex || index > n.lastLogIndex()+1 {
		return nil
	}
	return append([]LogEntry(nil), n.log[index-n.lastIncludedIndex:]...)
}

// truncateFrom discards the entry at index and everything after it.
//
// Rule 3 of the AppendEntries receiver. An index at or below the floor is
// refused: those entries are already in a snapshot, and a snapshot only ever
// covers committed entries, which no rule of Figure 2 may remove.
func (n *Node) truncateFrom(index int) {
	if index <= n.lastIncludedIndex {
		n.lg().Error("refusing to truncate into the snapshot floor",
			"index", index, "floor", n.lastIncludedIndex)
		return
	}
	if index > n.lastLogIndex() {
		return // nothing to remove
	}
	n.log = n.log[:index-n.lastIncludedIndex]
}

// compactTo drops every entry at or below index and installs a new floor.
//
// The slice is rebuilt rather than resliced. n.log = n.log[k:] would keep the
// whole original backing array alive behind the new header, so the memory the
// compaction was supposed to release would stay held until the log next grew
// past its old capacity — which is the entire point of doing this.
//
// Caller must have already made the snapshot durable. See Node.Snapshot.
func (n *Node) compactTo(index, term int) {
	tail := n.entriesFrom(index + 1)

	rebuilt := make([]LogEntry, 0, len(tail)+1)
	rebuilt = append(rebuilt, LogEntry{Term: term}) // the new floor
	rebuilt = append(rebuilt, tail...)

	n.log = rebuilt
	n.lastIncludedIndex = index
	n.lastIncludedTerm = term
}
