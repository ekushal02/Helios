package kvstore

import "sync"

// WatchEvent is one applied write, as delivered to a watcher -- Put or
// Delete, decided by Tombstone the same way decodedCommand's own field
// already decides it, at the EXACT Raft log index (Revision) it
// committed at. Exported directly, not translated from a separate
// internal type: applyCommand builds one of these straight from the
// command it just applied, and Server.Watch consumes it directly, the
// same "same shape one layer up" pattern this project repeats at every
// boundary.
//
// UNLIKE GET/PUT/DELETE/SCAN'S OWN REVISION, THIS ONE IS EXACT, NOT
// APPROXIMATED. Server.getResponse's own doc names a real gap: a
// Get/Put/Delete/Scan response's Revision is Machine.AppliedIndex()
// taken after the call returns, a correct lower bound but not
// necessarily the exact index a specific key was last written at,
// because Machine's read/write methods never return the barrier or
// commit index they waited on. WatchEvent has no such gap -- it is
// built directly inside applyCommand, which already knows
// msg.CommandIndex is the exact index THIS event committed at.
type WatchEvent struct {
	Tombstone bool
	Key       []byte
	Value     []byte
	Revision  int
}

// watchState is one Machine's whole Watch subsystem: a bounded ring
// buffer of recently-applied events (so a new watcher requesting a
// past start_revision can replay it) plus live fan-out to every
// currently-subscribed stream.
type watchState struct {
	mu sync.Mutex

	events        []WatchEvent
	capacity      int
	oldestEvicted int // revision of the newest event ever evicted, or 0 if none evicted yet

	subs      map[int]chan WatchEvent
	nextSubID int
	subBuf    int // buffer size for every new subscriber channel
}

func newWatchState(historyCapacity, subscriberBufferSize int) *watchState {
	return &watchState{
		capacity: historyCapacity,
		subs:     make(map[int]chan WatchEvent),
		subBuf:   subscriberBufferSize,
	}
}

// notify records ev in the retained ring buffer and delivers it to
// every live subscriber. Called from applyCommand right after a
// successful, non-deduplicated write -- see that function's own doc
// for why this MUST NEVER BLOCK: applyCommand is the single, serial
// consumer of the whole apply path, and a slow watcher must never be
// able to stall every other write on the cluster. Delivery to each
// subscriber is therefore non-blocking (select/default): a watcher
// whose own buffered channel is already full is disconnected --
// closed, dropped from subs -- rather than allowed to backpressure the
// apply path or any other watcher. Server.Watch's own doc explains
// what a disconnected watcher's client sees.
func (w *watchState) notify(ev WatchEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.events = append(w.events, ev)
	if len(w.events) > w.capacity {
		w.oldestEvicted = w.events[0].Revision
		w.events = w.events[1:]
	}

	for id, ch := range w.subs {
		select {
		case ch <- ev:
		default:
			delete(w.subs, id)
			close(ch)
		}
	}
}

// subscribe registers a new live watcher and returns everything
// currently retained plus a channel for every subsequent event -- both
// captured under the SAME lock notify() also holds, so there is no gap
// between "read the retained history" and "start receiving live
// events" for an event to fall into: whatever notify() processes after
// this call returns either was already in the retained slice handed
// back here, or arrives on the returned channel, never both and never
// neither.
func (w *watchState) subscribe() (id int, retained []WatchEvent, live <-chan WatchEvent, oldestEvicted int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	id = w.nextSubID
	w.nextSubID++
	ch := make(chan WatchEvent, w.subBuf)
	w.subs[id] = ch

	return id, append([]WatchEvent(nil), w.events...), ch, w.oldestEvicted
}

func (w *watchState) unsubscribe(id int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ch, ok := w.subs[id]; ok {
		delete(w.subs, id)
		close(ch)
	}
}

// markFloor raises oldestEvicted to floor, if floor is higher than the
// current value -- called whenever this Machine's own Raft snapshot
// floor advances (NewMachine's own recovery, Machine.take,
// Machine.installSnapshot). THE FIX FOR A REAL GAP notify's own
// capacity-based eviction cannot catch alone: ordinary restart replay
// naturally repopulates the ring buffer for free (§18.5's identical
// argument for the dedup table -- redelivery through ApplyCh means
// notify() runs again for everything above the snapshot floor), but
// anything AT OR BELOW the floor is never redelivered at all -- it
// exists only inside the snapshot's own compacted image, which this
// Machine does not retain watch history for (DESIGN.md §12's own open
// question on this). Without this call, a start_revision at or below
// the floor could slip past the "already evicted" check on a node
// whose ring buffer capacity has simply never been exercised yet (a
// short replay after a fresh restart, for instance), silently
// returning an INCOMPLETE replay as if it were the whole story rather
// than reporting the gap the caller actually needs to know about.
func (w *watchState) markFloor(floor int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if floor > w.oldestEvicted {
		w.oldestEvicted = floor
	}
}

// closeAll disconnects every current subscriber -- called from Close,
// so an in-flight Watch RPC's own live channel closes cleanly (its
// range loop ends, chOk == false) rather than blocking forever on a
// Machine that has already shut down.
func (w *watchState) closeAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, ch := range w.subs {
		delete(w.subs, id)
		close(ch)
	}
}

// Watch registers a live watcher on this Machine's own apply stream --
// NOT filtered by key prefix here; prefix matching is Server.Watch's
// own concern, a presentation detail of the wire protocol, not
// something the storage layer needs to know about (Scan draws the
// identical line: scanLocked has no idea what a client's own bounds
// mean, it just walks a range it's told to walk).
//
// startRevision == 0 means live-only: no replay, subscribe and wait
// for whatever happens next. A positive startRevision requests replay
// from that point forward; if it has already fallen out of the
// retained window (at or below oldestEvicted), ok is false and the
// caller must tell its own client to resync (Server.Watch's own doc
// covers exactly how) rather than silently starting the watch with a
// gap in it.
//
// NOT LEADER-GATED, UNLIKE EVERY OTHER READ IN THIS PACKAGE -- A REAL,
// REASONED DEPARTURE FROM THE PATTERN, NOT AN OVERSIGHT. Get/Scan need
// linearizability: a single authoritative answer as of a point in
// time, which only the leader's own barrier can provide. Watch offers
// a different guarantee entirely -- "the events this node applies, in
// the order it applies them" -- which Raft's own state machine safety
// property already guarantees is identical, in order, on every node
// that has applied a given entry at all, leader or follower. A watch
// subscribed on a lagging follower delivers events LATE relative to
// when they committed on the leader, but never out of order and never
// duplicated (the same guarantee applyCommand's own dedup/apply-once
// property already gives the storage itself). Requiring leadership
// here would reject a request Raft's own safety guarantees make
// perfectly valid to serve.
//
// The caller MUST call the returned cancel function exactly once, when
// it stops reading events -- see Server.Watch's own doc for exactly
// when.
func (m *Machine) Watch(startRevision int) (replay []WatchEvent, live <-chan WatchEvent, cancel func(), ok bool) {
	id, retained, ch, oldestEvicted := m.watch.subscribe()

	if startRevision > 0 && startRevision <= oldestEvicted {
		m.watch.unsubscribe(id)
		return nil, nil, func() {}, false
	}

	if startRevision > 0 {
		for _, ev := range retained {
			if ev.Revision >= startRevision {
				replay = append(replay, ev)
			}
		}
	}

	return replay, ch, func() { m.watch.unsubscribe(id) }, true
}
