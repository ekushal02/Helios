package raft

import "time"

// heartbeatLoop keeps this node's leadership alive for as long as it holds it.
func (n *Node) heartbeatLoop(term int) {
	n.sendHeartbeats(term)

	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.mu.Lock()
			stillLeading := n.state == Leader && n.currentTerm == term
			n.mu.Unlock()

			if !stillLeading {
				return
			}
			n.sendHeartbeats(term)
		}
	}
}

// sendHeartbeats fans an empty AppendEntries out to every peer.
func (n *Node) sendHeartbeats(term int) {
	n.mu.Lock()

	// Leadership may have ended between the ticker firing and this lock.
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	// TODO (C-3): PrevLogIndex/PrevLogTerm must become per-follower, derived
	// from nextIndex[peer], and Entries must carry whatever that follower is
	// missing. Then each peer needs its OWN args struct -- sharing one is only
	// safe while every peer gets identical content.
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: n.lastLogIndex(),
		PrevLogTerm:  n.lastLogTerm(),
		Entries:      nil, // empty: this is a heartbeat
		LeaderCommit: n.commitIndex,
	}
	peers := append([]int(nil), n.peers...)

	n.mu.Unlock() // never send RPCs holding the lock

	for _, peer := range peers {
		go func(to int) {
			var reply AppendEntriesReply
			if !n.transport.SendAppendEntries(to, args, &reply) {
				return // dropped, partitioned or dead: nothing to do
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Leader || n.currentTerm != term {
				return
			}

			n.stepDownIfStale(reply.Term)
		}(peer)
	}
}
