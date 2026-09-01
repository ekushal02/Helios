package raft

// LeaderHint reports this node's current belief about who leads --
// leaderID, exposed outside this package for the first time. submit.go's
// own doc has named this task since before internal/server existed:
// "the caller must find the real leader and try again; leaderID is the
// hint for that (F-2)." This accessor is the entire change needed to
// make the field usable from outside the package -- nothing about how
// leaderID itself is written changes (see appendentries.go,
// installsnapshot.go, election.go, config.go).
//
// A HINT, NOT AN AUTHORITATIVE ANSWER. It is ordinary volatile state
// (§6), read under the same lock every other field on Node uses, and
// can be stale the instant after it is read: the leader it names may
// already have stepped down, or a newer one may already have been
// elected without this node having heard yet. A caller that acts on it
// (redirecting a client, in F-2's case) is making a best-effort routing
// decision, not relying on it for correctness -- correctness comes from
// the redirected node's own leadership check when the retry lands
// there, exactly the same way a stale nextIndex guess costs an extra
// round trip rather than breaking anything (§10).
//
// Returns None (-1) if this node has no current belief: it has never
// heard from a leader, or the belief was reset on becoming a candidate
// or on stepping down.
func (n *Node) LeaderHint() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}
