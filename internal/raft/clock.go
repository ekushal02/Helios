package raft

import "time"

// clock is Node's own source of "now," used ONLY on the lease-critical path:
// ReadLease and leaseExpiry (read.go), and the two call sites that stamp
// sentAt for noteContact (replication.go, installsnapshot.go). Everywhere
// else in this package -- election timers, heartbeat timers -- still calls
// time.Now() directly, deliberately unchanged. That is not an oversight;
// it is the boundary this task drew on purpose.
//
// WHY THIS EXISTS AT ALL, GIVEN §23'S OWN synctest INFRASTRUCTURE ALREADY
// VIRTUALIZES TIME FOR FREE. A synctest bubble gives every goroutine inside
// it the SAME virtual clock -- exactly what a test wanting DETERMINISTIC,
// FAST timing needs, and exactly why §23 built nothing further: Node's own
// real time.Now()/time.NewTicker calls already become virtual, jitter-free,
// and seed-replayable for any test that runs inside a bubble, with zero
// production code changes. What a bubble CANNOT do is give ONE node a
// DIFFERENT clock than its peers -- there is no notion, inside one bubble,
// of "this one goroutine's Now() answers differently from that one's." A
// clock SKEW fault -- one node's clock diverging from the others', including
// running backwards -- is a claim about exactly that divergence, so testing
// it needs a seam synctest structurally cannot provide.
//
// WHY ONLY THE LEASE PATH, NOT A NODE-WIDE REWRITE. Everywhere else in this
// package, a wrong answer from time.Now() is a LIVENESS bug at worst: an
// election timer that fires early or late still only affects how quickly
// the cluster converges, never whether an answer it gives is correct. The
// lease path is the one place this project has already, explicitly, named
// as different: DESIGN.md §9's own "bounded clock assumption" is a SAFETY
// argument -- ReadLease's whole justification for skipping a majority round
// is a claim that a SPECIFIC instant, measured on THIS node's own clock, has
// not yet arrived. A test that cannot make that specific claim false has no
// way to check whether the code checking it is actually correct, rather
// than merely plausible.
type clock interface {
	Now() time.Time
}

// realClock is the only clock a production Node ever uses -- a direct,
// zero-overhead wrapper around the standard library. NewNode sets it by
// default; nothing else in this package ever needs to construct one.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// now reads n.clock under n.mu and calls its Now() outside the lock.
//
// UNLIKE n.transport, WHICH NEVER CHANGES AFTER CONSTRUCTION AND SO NEEDS NO
// LOCK TO READ SAFELY, n.clock is deliberately swappable after construction
// -- that is the entire point of clock.go existing, and every clock-skew
// test in clockskew_test.go does exactly this, on an already-running node,
// to simulate a clock fault arriving mid-lifetime rather than only at
// startup. sendAppendEntries and sendInstallSnapshot both stamp sentAt
// BEFORE acquiring n.mu for the rest of their own bookkeeping, so a direct,
// unlocked n.clock.Now() there races a concurrent test (or, in principle, a
// future feature) that reassigns n.clock while a round is in flight. Reading
// the clock interface value itself under the lock is cheap -- copying a
// two-word interface, not the state behind it -- and then calling Now() on
// the copy outside the lock is safe regardless of which clock it is:
// realClock is stateless, and fakeClock (clockskew_test.go) protects its own
// state with its own mutex.
func (n *Node) now() time.Time {
	n.mu.Lock()
	clk := n.clock
	n.mu.Unlock()
	return clk.Now()
}
