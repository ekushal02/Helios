package raft

import (
	"errors"
	"fmt"
	"sort"
)

// =============================================================================
// Single-server membership change
// =============================================================================
//
// A DEVIATION FROM §6, AND THE ARGUMENT FOR IT. The paper changes membership
// through joint consensus: a transitional Cold,new configuration under which
// agreement needs separate majorities from both the old and new sets. Helios
// changes ONE SERVER AT A TIME instead, which is the simplification from
// Ongaro's dissertation §4.1, and which needs no transitional configuration at
// all.
//
// The reason it is safe: any two configurations differing by exactly one server
// have OVERLAPPING MAJORITIES. Going from three servers to four, a majority of
// the old is two and a majority of the new is three, and 2 + 3 > 4, so the two
// sets must share a member. That shared member votes at most once per term, so
// no two leaders can be elected under the two configurations at the same time.
// Figure 10's split is exactly what happens when the change is larger than one
// server and that overlap is lost; joint consensus exists to restore it. One
// server at a time has it for free.
//
// The cost is that a three-to-five change has to be done as two changes, and
// each must commit before the next begins. changeConfiguration enforces that.
//
// =============================================================================
// A CONFIGURATION TAKES EFFECT WHEN IT IS APPENDED, NOT WHEN IT COMMITS
// =============================================================================
//
// This is the rule that reads like a bug. §6: "a server always uses the latest
// configuration in its log, regardless of whether the entry is committed."
//
// Why it has to be that way. A leader replicating Cnew must count Cnew's
// majority while Cnew is still uncommitted -- that is what deciding whether it
// committed MEANS. If it counted Cold's majority instead, it could declare the
// change committed on the strength of a set that does not include the servers
// it is replicating to, and the two configurations would be making decisions
// independently, which is the thing the whole mechanism exists to prevent.
//
// Two consequences fall out, and both are handled here rather than assumed:
//
//   - TRUNCATION MUST ROLL THE CONFIGURATION BACK. Receiver rule 3 can delete a
//     config entry that was never committed, and the configuration in force
//     must go back to whatever preceded it. So the configuration is DERIVED
//     from the log, never remembered independently of it.
//   - THE CONFIGURATION BELOW THE SNAPSHOT FLOOR IS GONE. Deriving it from the
//     log only works as far back as the log reaches, so the state record
//     carries the configuration as of the floor -- exactly the problem
//     lastIncludedIndex already solves, with the same answer.

var (
	// ErrNotLeader means the change must be sent to whoever leads. leaderID is
	// the hint.
	ErrNotLeader = errors.New("raft: not the leader")

	// ErrConfigChangeInFlight means the previous change has not committed yet.
	// Allowing a second would put two configurations in play at once, and the
	// overlapping-majority argument above covers only one step at a time.
	ErrConfigChangeInFlight = errors.New("raft: a configuration change is already in flight")

	// ErrNoChange means the configuration already has the requested shape.
	ErrNoChange = errors.New("raft: the configuration already has that shape")
)

// isConfig reports whether this entry carries a configuration rather than a
// client command.
//
// Non-empty Servers is the marker, with no separate flag, because unlike
// LogEntry.Command there is no legitimate empty value to collide with: a
// cluster always has at least one server. Compare the reasoning on NoOp, where
// an empty command IS legitimate and a flag was therefore necessary.
func (e LogEntry) isConfig() bool { return len(e.Servers) > 0 }

// =============================================================================
// Adopting one
// =============================================================================

// setConfiguration installs a configuration and the index of the entry that
// carried it. Caller must hold mu.
//
// peers is derived here and nowhere else, so every existing reader of n.peers
// keeps working and cannot disagree with n.servers.
func (n *Node) setConfiguration(servers []int, index int) {
	sorted := append([]int(nil), servers...)
	sort.Ints(sorted)

	n.servers = sorted
	n.configIndex = index

	n.inConfig = false
	peers := make([]int, 0, len(sorted))
	for _, s := range sorted {
		if s == n.id {
			n.inConfig = true
			continue
		}
		peers = append(peers, s)
	}
	n.peers = peers

	if n.state != Leader {
		return
	}

	// A LEADER MUST FIX ITS BOOKKEEPING IMMEDIATELY. initLeaderState runs once
	// per term, so a server added mid-term has no nextIndex, and the zero value
	// a missing map key returns is 0 -- which buildAppendEntries reads as "at or
	// below the floor" and answers with a snapshot the leader may not have. The
	// new server would then never be contacted at all.
	for _, p := range peers {
		if _, known := n.nextIndex[p]; !known {
			n.nextIndex[p] = n.lastLogIndex() + 1
			n.matchIndex[p] = 0
		}
	}

	// And a departed server must stop being counted, or the majority is
	// computed against a set the cluster no longer has.
	for p := range n.nextIndex {
		if containsServer(sorted, p) {
			continue
		}
		delete(n.nextIndex, p)
		delete(n.matchIndex, p)
		delete(n.lastContact, p)
		delete(n.replicatingTerm, p)
		delete(n.replPending, p)
		delete(n.snapshotSentAt, p)
	}
}

// adoptConfigFromAppended installs the newest configuration among entries that
// were just appended, if any. firstIndex is the index of entries[0].
//
// The common path: an AppendEntries carrying ordinary commands touches nothing
// and costs one scan of the message. Caller must hold mu.
func (n *Node) adoptConfigFromAppended(firstIndex int, entries []LogEntry) {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].isConfig() {
			n.setConfiguration(entries[i].Servers, firstIndex+i)
			return
		}
	}
}

// refreshConfiguration recomputes the configuration from the log.
//
// Used after a TRUNCATION, which can delete the entry that put the current
// configuration in force. Walking back is the only correct answer: what
// replaces it is whatever config entry now sits highest, and below the floor
// that is baseServers.
//
// O(log length), and called only when receiver rule 3 actually removed
// something. Caller must hold mu.
func (n *Node) refreshConfiguration() {
	for i := n.lastLogIndex(); i >= n.firstLogIndex(); i-- {
		if e := n.entryAt(i); e.isConfig() {
			n.setConfiguration(e.Servers, i)
			return
		}
	}
	n.setConfiguration(n.baseServers, n.lastIncludedIndex)
}

// configCommitted reports whether the configuration in force is safe to build
// on: either it came from the floor, or the entry that carried it has committed.
//
// Caller must hold mu.
func (n *Node) configCommitted() bool { return n.commitIndex >= n.configIndex }

// Configuration returns the servers currently in the cluster, this node
// included if it is one of them.
func (n *Node) Configuration() []int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]int(nil), n.servers...)
}

// =============================================================================
// Making one
// =============================================================================

// AddServer brings a server into the cluster.
//
// The new server is a full voter from the moment this entry is APPENDED, which
// is the rule above. It starts with an empty log, so until the leader has
// repaired it the cluster is committing against a majority that includes a
// server which cannot yet acknowledge anything.
//
// SCOPE FENCE (catch-up phase). §6 avoids that window by admitting the server
// as a non-voting member first, replicating to it, and only then changing the
// configuration. Helios does not, so adding a server to a cluster that is
// already one failure from losing quorum will stall commits until the new
// server catches up. Safe -- nothing is lost, and progress resumes -- but it is
// an availability gap and it is not measured.
func (n *Node) AddServer(id int) (index int, term int, err error) {
	n.mu.Lock()
	current := append([]int(nil), n.servers...)
	n.mu.Unlock()

	if containsServer(current, id) {
		return 0, 0, fmt.Errorf("%w: server %d is already a member", ErrNoChange, id)
	}
	return n.changeConfiguration(append(current, id))
}

// RemoveServer takes a server out of the cluster.
//
// Removing the leader is legal and is handled: it goes on replicating the entry
// it is not a member of, and steps down once that entry commits. See
// stepDownIfRemoved.
//
// SCOPE FENCE (disruptive servers). §6's third issue: a removed server stops
// receiving heartbeats, times out, campaigns with a higher term, and deposes a
// healthy leader -- repeatedly. The fix is to disregard RequestVote received
// within the minimum election timeout of hearing from a current leader. Helios
// does not do that yet, so a removed server must actually be shut down rather
// than merely removed from the configuration.
func (n *Node) RemoveServer(id int) (index int, term int, err error) {
	n.mu.Lock()
	current := append([]int(nil), n.servers...)
	n.mu.Unlock()

	if !containsServer(current, id) {
		return 0, 0, fmt.Errorf("%w: server %d is not a member", ErrNoChange, id)
	}

	next := make([]int, 0, len(current)-1)
	for _, s := range current {
		if s != id {
			next = append(next, s)
		}
	}
	if len(next) == 0 {
		return 0, 0, fmt.Errorf("%w: a cluster cannot be empty", ErrNoChange)
	}
	return n.changeConfiguration(next)
}

// changeConfiguration appends a configuration entry, subject to the one-change-
// at-a-time rule.
//
// The precondition runs INSIDE the append, under the same lock that decides
// this node is still the leader. Checking it beforehand and appending
// afterwards would leave a window for a second change to slip between the two,
// which is precisely the state the overlapping-majority argument does not cover.
func (n *Node) changeConfiguration(servers []int) (index int, term int, err error) {
	entry := LogEntry{Servers: append([]int(nil), servers...)}

	index, term, isLeader, err := n.appendChecked(entry, func() error {
		if !n.configCommitted() {
			return fmt.Errorf("%w: the entry at index %d has not committed",
				ErrConfigChangeInFlight, n.configIndex)
		}
		return nil
	})

	if err != nil {
		return 0, term, err
	}
	if !isLeader {
		return 0, term, ErrNotLeader
	}
	return index, term, nil
}

// stepDownIfRemoved retires a leader that the committed configuration no longer
// contains.
//
// WHY IT WAITS FOR THE COMMIT when everything else about a configuration acts on
// the append. The leader has to keep replicating the entry that removes it --
// nobody else can, because nobody else is leader -- and the new configuration
// cannot operate on its own until that entry is committed. Stepping down at the
// append would abandon the change half-made, with no leader in either
// configuration and no entry committed to elect one from.
//
// Caller must hold mu.
func (n *Node) stepDownIfRemoved() {
	if n.state != Leader || n.inConfig {
		return
	}
	if !n.configCommitted() {
		return
	}

	n.lg().Info("stepping down: not a member of the committed configuration",
		"configIndex", n.configIndex, "servers", n.servers)

	n.state = Follower
	n.leaderID = None
	n.resetElectionTimer()
}

func containsServer(servers []int, id int) bool {
	for _, s := range servers {
		if s == id {
			return true
		}
	}
	return false
}
