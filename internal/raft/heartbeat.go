package raft

import "time"

// heartbeatLoop keeps this node's leadership alive for as long as it holds it,
// and doubles as the retry engine for replication.
//
// term is the term this leadership belongs to. The loop exits the moment the
// node is no longer leader OF THAT TERM. Passing it in rather than reading
// n.currentTerm matters: a node can step down, win a later election, and start a
// second loop. Without the term check the old loop would keep running alongside
// the new one, both sending.
//
// C-3 turns this from a heartbeat loop into a general replication tick. It no
// longer sends empty messages -- it sends whatever each follower is missing,
// which is empty only when that follower is caught up. This is also what makes
// dropped AppendEntries self-healing: there is no retry queue, because the next
// tick recomputes and resends from nextIndex regardless of what was lost.
func (n *Node) heartbeatLoop(term int) {
	// Send one IMMEDIATELY. Waiting a full interval hands every follower a 50ms
	// head start on timing out, right when leadership is least established.
	n.replicateAll(term)

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
			n.replicateAll(term)
		}
	}
}
