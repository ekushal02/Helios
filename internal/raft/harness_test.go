package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNetwork, an in-memory switchboard between nodes.
//
// EVERY FAULT DECISION IT MAKES -- drop this request, duplicate it, delay it
// by how much, bias it toward arriving out of order, drop this reply -- is a
// pure function of (seed, kind, from, to, seq), computed by messageHash
// below. There used to be a single shared *rand.Rand, drawn from under fn.mu
// in whatever order goroutines happened to reach the lock in. That
// order is a property of the Go scheduler, not of the seed: election's own
// runElection fans SendRequestVote out to every peer AT ONCE (one goroutine
// each), so two runs of the identical seed could -- and, under -shuffle=on
// -count=3, eventually would -- hand message #1's drop roll to a different
// logical message each time. The seed still appeared in every log line
// (testlog_test.go), but pinning it and rerunning did not reproduce the
// failure it named. Hashing each decision from the message's own identity
// instead of a stream position removes the scheduler from the answer entirely:
// replaying seed S reproduces the exact same drops, duplicates, delays, and
// reorders, regardless of which goroutine got to fn.mu first. See
// simreplay_test.go for the test that checks exactly this property, and
// DESIGN.md §24 for why a per-pair seq is safe to use here despite being
// assigned under a lock shared across every pair.
//
// FOUR INDEPENDENTLY CONFIGURABLE FAULT TYPES, EACH WITH ITS OWN RATE (Phase
// G-4), NOT ONE KNOB TRYING TO STAND IN FOR ALL OF THEM. drop (dropRate,
// replyDropRate) discards a message outright -- the receiver never sees it.
// duplicate (duplicateRate) is drop's own opposite failure mode: the SAME
// request reaches the receiver TWICE, modeling a network that redelivers a
// packet rather than losing one, which is a materially different thing for a
// receiver's own idempotency to get right (RequestVote's one-vote-per-term
// rule, AppendEntries' index-and-term log matching) than either dropping or
// delaying ever exercises. delay (minDelay, maxDelay) is ordinary jitter.
// reorder (reorderRate) is a DELIBERATE bias, not delay's own incidental
// side effect: delay already produces some reordering for free whenever its
// own range is wide enough for two messages' draws to cross, but that rate
// is a derived property of whatever minDelay/maxDelay happen to be, not a
// dial of its own. reorderRate is a genuinely independent knob -- see
// rollReorderBoost's own doc for the mechanism -- that raises the ODDS a
// given message arrives out of order without changing what delay means for
// everything else, so a test can ask for "10% of messages reordered" without
// having to first reverse-engineer what delay range would incidentally
// produce that rate.
type fakeNetwork struct {
	mu sync.Mutex

	seed int64

	nodes map[int]*Node

	reachable map[int]map[int]bool //N x N matrix

	dropRate      float64       //chance a deliverable REQUEST is dropped anyway
	replyDropRate float64       //chance a REPLY is dropped after the handler ran
	duplicateRate float64       //chance a deliverable REQUEST also reaches the target a second time
	reorderRate   float64       //chance a message's own delay is deliberately biased late
	minDelay      time.Duration //lower bound on random per-message delay
	maxDelay      time.Duration //upper bound on random per-message delay

	rpcCount       int //for asserting things like "the minority got no votes"
	dropCount      int //requests plus replies actually discarded
	duplicateCount int //requests actually delivered a second time

	installSnapshotRPCs int
	preVoteRPCs         int
	snapshotBytes       int
	appendRPCs          int // AppendEntries messages actually delivered
	entriesShipped      int // entries carried across all of them

	nextSeq     map[[2]int]int //next stamp per directed pair, assigned at send
	lastArrived map[[2]int]int //highest stamp delivered per directed pair
	reordered   int            //messages that arrived behind a later-sent one

	// cutRefs counts, per directed pair, how many currently-active
	// isolateSubset calls are cutting it -- the reference-counted overlay
	// isolateSubset/restoreSubset (below) need to compose correctly, unlike
	// partition/heal's own wholesale-replace semantics: two independently
	// scheduled isolations of different, overlapping node subsets can
	// stay active across the same time window without one's own restore
	// call reopening a link the other still needs cut. partition, heal,
	// disconnect, and reconnect never touch this map -- they are, and stay,
	// direct, unconditional replacements of the whole reachable matrix,
	// exactly as every existing caller of them already assumes.
	cutRefs map[[2]int]int

	trace map[traceKey]decisionRecord //every decision made so far; see decisionTrace
}

func newFakeNetwork(seed int64) *fakeNetwork {
	return &fakeNetwork{
		seed:        seed,
		nodes:       make(map[int]*Node),
		reachable:   make(map[int]map[int]bool),
		nextSeq:     make(map[[2]int]int),
		lastArrived: make(map[[2]int]int),
		cutRefs:     make(map[[2]int]int),
	}
}

// =============================================================================
// Deterministic decisions: hashing instead of a shared RNG stream
// =============================================================================

// messageKind distinguishes the four RPCs sharing this network, so the same
// (from, to, seq) triple used for, say, a leader's third AppendEntries to a
// peer doesn't share a hash input with that peer's own third PreVote reply to
// someone else -- each kind gets its own hash space.
type messageKind uint8

const (
	kindRequestVote messageKind = iota
	kindPreVote
	kindAppendEntries
	kindInstallSnapshot
)

// traceKey identifies one logical message: the seq-th send of this kind from
// `from` to `to`. seq is assigned once, at route(), regardless of whether the
// message is ultimately dropped -- see route's own comment on why that
// assignment is safe to make under a lock shared across every directed pair,
// not just this one.
type traceKey struct {
	kind messageKind
	from int
	to   int
	seq  int
}

// decisionRecord is what the network decided for one message.
type decisionRecord struct {
	requestDropped bool
	replyDropped   bool
	delay          time.Duration
	duplicated     bool // this request was also delivered a second time
	reorderBoosted bool // this message's own delay was deliberately biased late
}

// splitmix64 is the same finalizer internal/storage/bloom's mix64 uses to
// decorrelate its two probe hashes (DESIGN.md §13.5), reused here for the
// identical reason: a small, fast, well-distributed integer hash that needs no
// external dependency and, critically, no shared mutable state -- calling it
// twice with the same input always gives the same output, from any goroutine,
// in any order.
func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	z := x
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// combineHash folds one more integer into a running hash, splitmix64-style.
func combineHash(h uint64, v int64) uint64 {
	return splitmix64(h ^ uint64(v))
}

// Salts distinguish the different QUESTIONS asked about the same message --
// "was the request dropped", "what was its delay", "was the reply dropped",
// "was it also duplicated", "was it biased toward reordering" -- so the
// answers vary independently even though every other input is shared
// between them.
const (
	saltRequestDrop uint64 = 1
	saltDelay       uint64 = 2
	saltReplyDrop   uint64 = 3
	saltDuplicate   uint64 = 4
	saltReorder     uint64 = 5
)

// messageHash is the entire source of randomness for one network decision.
// Nothing here reads real time, and nothing here reads or advances shared
// state -- it is a pure function of the network's own seed, which RPC this is,
// its direction, its place in that pair's own sequence, and which question is
// being asked. That purity is the whole point: see the fakeNetwork doc comment.
func (fn *fakeNetwork) messageHash(kind messageKind, from, to, seq int, salt uint64) uint64 {
	h := uint64(fn.seed)
	h = combineHash(h, int64(kind))
	h = combineHash(h, int64(from))
	h = combineHash(h, int64(to))
	h = combineHash(h, int64(seq))
	h = combineHash(h, int64(salt))
	return h
}

// hashUnitFloat maps a hash to [0, 1) using the standard 53-significant-bit
// technique -- the same one math/rand's own Float64 uses internally -- so
// dropRate and replyDropRate keep meaning exactly what they meant before.
func hashUnitFloat(h uint64) float64 {
	return float64(h>>11) / (1 << 53)
}

func (fn *fakeNetwork) rollDrop(kind messageKind, from, to, seq int) bool {
	if fn.dropRate <= 0 {
		return false
	}
	return hashUnitFloat(fn.messageHash(kind, from, to, seq, saltRequestDrop)) < fn.dropRate
}

func (fn *fakeNetwork) rollReplyDrop(kind messageKind, from, to, seq int) bool {
	if fn.replyDropRate <= 0 {
		return false
	}
	return hashUnitFloat(fn.messageHash(kind, from, to, seq, saltReplyDrop)) < fn.replyDropRate
}

func (fn *fakeNetwork) rollDelay(kind messageKind, from, to, seq int) time.Duration {
	if fn.maxDelay <= fn.minDelay {
		return fn.minDelay
	}
	span := uint64(fn.maxDelay - fn.minDelay)
	h := fn.messageHash(kind, from, to, seq, saltDelay)
	return fn.minDelay + time.Duration(h%span)
}

// rollDuplicate decides whether this request also reaches the target a
// second time -- independent of drop (a message can be BOTH dropped and,
// separately, never get the chance to duplicate, since route returns before
// either matters once dropped) and independent of delay (the duplicate rides
// along with the primary delivery's own timing rather than drawing a second
// one of its own; see the endpoint SendXxx methods for exactly where the
// second target.Xxx call happens).
func (fn *fakeNetwork) rollDuplicate(kind messageKind, from, to, seq int) bool {
	if fn.duplicateRate <= 0 {
		return false
	}
	return hashUnitFloat(fn.messageHash(kind, from, to, seq, saltDuplicate)) < fn.duplicateRate
}

// rollReorderBoost decides whether this message's own delay is deliberately
// pushed past maxDelay -- a genuine, independent fault, not delay's own
// incidental side effect. minDelay/maxDelay already produce SOME reordering
// for free whenever their own range is wide enough for two draws to cross,
// but that rate is a derived property of whatever the range happens to be,
// not something a caller can dial directly. Boosting a message's delay to
// strictly exceed maxDelay -- the highest an ordinary, non-boosted delay can
// ever be -- biases it toward losing a race against whatever this pair sends
// next, without touching what delay means for every other message. "Biases
// toward," not "guarantees": if nothing else is sent to this pair for a
// while, a boosted message still eventually arrives with nothing to have
// been overtaken by. See arrive's own reordered counter for how a caller
// confirms the bias actually produced measurable reordering, the same
// "measured, not just asserted" standard TestElectionTimeoutsAreRandomised
// already holds randomization claims to.
func (fn *fakeNetwork) rollReorderBoost(kind messageKind, from, to, seq int) bool {
	if fn.reorderRate <= 0 {
		return false
	}
	return hashUnitFloat(fn.messageHash(kind, from, to, seq, saltReorder)) < fn.reorderRate
}

// recordRequestLocked and recordReplyLocked build up fn.trace as decisions are
// made. Callers must hold fn.mu.
func (fn *fakeNetwork) recordRequestLocked(kind messageKind, from, to, seq int, dropped bool, delay time.Duration, duplicated, reorderBoosted bool) {
	if fn.trace == nil {
		fn.trace = make(map[traceKey]decisionRecord)
	}
	fn.trace[traceKey{kind, from, to, seq}] = decisionRecord{
		requestDropped: dropped,
		delay:          delay,
		duplicated:     duplicated,
		reorderBoosted: reorderBoosted,
	}
}

func (fn *fakeNetwork) recordReplyLocked(kind messageKind, from, to, seq int, dropped bool) {
	if fn.trace == nil {
		fn.trace = make(map[traceKey]decisionRecord)
	}
	rec := fn.trace[traceKey{kind, from, to, seq}]
	rec.replyDropped = dropped
	fn.trace[traceKey{kind, from, to, seq}] = rec
}

// decisionTrace returns a snapshot of every network decision made so far,
// keyed by the logical message it belongs to. Two runs of the identical
// scripted scenario at the identical seed produce identical traces -- deep
// equal, not just similarly shaped -- because every decision folded into it
// came from messageHash, never from a shared, order-dependent stream. See
// simreplay_test.go.
func (fn *fakeNetwork) decisionTrace() map[traceKey]decisionRecord {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	out := make(map[traceKey]decisionRecord, len(fn.trace))
	for k, v := range fn.trace {
		out[k] = v
	}
	return out
}

// register adds a node, fully connected to every node already present.
func (fn *fakeNetwork) register(n *Node) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	fn.nodes[n.id] = n
	if fn.reachable[n.id] == nil {
		fn.reachable[n.id] = make(map[int]bool)
	}
	for other := range fn.nodes {
		fn.reachable[n.id][other] = true
		fn.reachable[other][n.id] = true
	}
}

// disconnect isolates a node in both directions.
func (fn *fakeNetwork) disconnect(id int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for other := range fn.nodes {
		fn.reachable[id][other] = false
		fn.reachable[other][id] = false
	}
}

// reconnect restores a node's links in both directions.
func (fn *fakeNetwork) reconnect(id int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for other := range fn.nodes {
		fn.reachable[id][other] = true
		fn.reachable[other][id] = true
	}
}

// partition splits the cluster into groups: nodes talk iff they share a group.
func (fn *fakeNetwork) partition(groups ...[]int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	for from := range fn.nodes {
		for to := range fn.nodes {
			fn.reachable[from][to] = false
		}
	}
	for _, g := range groups {
		for _, from := range g {
			for _, to := range g {
				fn.reachable[from][to] = true
			}
		}
	}
}

// heal restores full connectivity.
func (fn *fakeNetwork) heal() {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for from := range fn.nodes {
		for to := range fn.nodes {
			fn.reachable[from][to] = true
		}
	}
}

// isolateSubset cuts every link between subset and the rest of the
// cluster, in both directions -- links WITHIN subset, and links within
// everyone else, are untouched. Unlike partition (which replaces the
// whole topology at once, given every group up front) or disconnect
// (which isolates exactly one node and has no notion of "while something
// else is also isolated"), isolateSubset is composable: it can be called
// for an arbitrary subset at an arbitrary moment, independently of
// whatever else is currently isolated, and its own restoreSubset call
// later reopens only what THIS call itself cut -- see cutRefs' own doc
// for why a plain "set reachable back to true" would be wrong the moment
// two isolations overlap on the same link. This is the primitive Phase
// G-3's faultInjector (faultinjector_test.go) is built on; it is also
// directly usable on its own, without a schedule, exactly as
// partition/heal already are.
func (fn *fakeNetwork) isolateSubset(subset []int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	in := make(map[int]bool, len(subset))
	for _, id := range subset {
		in[id] = true
	}
	for from := range fn.nodes {
		for to := range fn.nodes {
			if from == to || in[from] == in[to] {
				continue // both inside subset, both outside it, or the same node: not a crossing link
			}
			pair := [2]int{from, to}
			fn.cutRefs[pair]++
			fn.reachable[from][to] = false
		}
	}
}

// restoreSubset is isolateSubset's own inverse, for the identical subset
// argument: it decrements cutRefs for every link that call's own
// isolateSubset cut, reopening a link only once nothing else currently
// isolating it remains. Calling restoreSubset with a subset that was
// never isolated, or isolated by a call that has already been restored,
// is a safe no-op on any link whose count is already zero.
func (fn *fakeNetwork) restoreSubset(subset []int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	in := make(map[int]bool, len(subset))
	for _, id := range subset {
		in[id] = true
	}
	for from := range fn.nodes {
		for to := range fn.nodes {
			if from == to || in[from] == in[to] {
				continue
			}
			pair := [2]int{from, to}
			if fn.cutRefs[pair] > 0 {
				fn.cutRefs[pair]--
			}
			if fn.cutRefs[pair] == 0 {
				fn.reachable[from][to] = true
			}
		}
	}
}

func (fn *fakeNetwork) setDropRate(p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.dropRate = p
}

// setReplyDropRate loses the REPLY after the receiver has already acted on the
// request.
//
// THE CASE setDropRate CANNOT PRODUCE, and the more interesting of the two. A
// lost request means the follower never saw anything. A lost reply means the
// follower appended the entries and the leader does not know: nextIndex stays
// put and the next tick resends entries the follower already holds. That path
// is the only thing in the suite that puts mergeEntries in front of a duplicate
// append, and a receiver that truncated on entries matching its own log would
// pass every other test in this package.
func (fn *fakeNetwork) setReplyDropRate(p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.replyDropRate = p
}

// setDuplicateRate makes a deliverable REQUEST also reach its target a
// second time -- drop's own opposite failure mode (a message that shows up
// too often rather than not at all), and a materially different thing for a
// receiver to get right than dropping or delaying ever exercises: RequestVote
// must not grant a second vote in the same term just because it was asked
// twice, and AppendEntries' own index-and-term log matching must treat a
// repeated append as a no-op rather than corrupting the log by reapplying
// it. See rollDuplicate's own doc for exactly what "a second time" means
// mechanically.
func (fn *fakeNetwork) setDuplicateRate(p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.duplicateRate = p
}

// setReorderRate biases a message toward arriving out of order -- a
// deliberate, independently-dialable fault, not delay's own incidental side
// effect. See rollReorderBoost's own doc for the mechanism and for why
// "biases toward" rather than "guarantees" is the honest claim to make.
func (fn *fakeNetwork) setReorderRate(p float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.reorderRate = p
}

func (fn *fakeNetwork) setMaxDelay(d time.Duration) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.maxDelay = d
}

func (fn *fakeNetwork) setDelayRange(min, max time.Duration) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.minDelay = min
	fn.maxDelay = max
}

func (fn *fakeNetwork) rpcs() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.rpcCount
}

func (fn *fakeNetwork) drops() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.dropCount
}

// duplicates reports how many requests were actually delivered a second
// time so far -- the direct, observable count setDuplicateRate's own
// probability produces, the same relationship drops() already has to
// dropRate.
func (fn *fakeNetwork) duplicates() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.duplicateCount
}

func (fn *fakeNetwork) countAppend(entries int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.appendRPCs++
	fn.entriesShipped += entries
}

func (fn *fakeNetwork) appendStats() (msgs, entries int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.appendRPCs, fn.entriesShipped
}

func (fn *fakeNetwork) resetCounters() {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.rpcCount, fn.dropCount = 0, 0
	fn.appendRPCs, fn.entriesShipped = 0, 0
	fn.installSnapshotRPCs, fn.snapshotBytes = 0, 0
	fn.preVoteRPCs = 0
	fn.duplicateCount = 0
}

func (fn *fakeNetwork) reorderedCount() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.reordered
}

// endpoint binds the network to one node's perspective, so a Node never has to say who it is on every call.
type endpoint struct {
	net  *fakeNetwork
	from int
}

func (fn *fakeNetwork) endpoint(from int) Transport {
	return &endpoint{net: fn, from: from}
}

func (e *endpoint) SendRequestVote(to int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	target, delay, seq, dup, ok := e.net.route(kindRequestVote, e.from, to)
	if !ok {
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	e.net.arrive(e.from, to, seq)

	//Copy across the wire so sender and receiver never share memory.
	var argsCopy RequestVoteArgs
	mustRoundTrip(args, &argsCopy)

	if dup {
		// The network redelivers this exact request a second time -- a
		// fresh, independent decode, not the same argsCopy reused,
		// exactly as two genuinely separate packets carrying identical
		// bytes would each decode on their own. Its own reply is
		// discarded: Transport's synchronous, one-reply-per-call shape
		// has no second return path for it, which is itself a faithful
		// model of duplicate delivery without a duplicate
		// acknowledgment -- the shape a real retransmission takes.
		var dupArgs RequestVoteArgs
		mustRoundTrip(args, &dupArgs)
		var discard RequestVoteReply
		target.RequestVote(&dupArgs, &discard)
	}

	var replyCopy RequestVoteReply
	target.RequestVote(&argsCopy, &replyCopy)

	if !e.net.replyDeliverable(kindRequestVote, e.from, to, seq) {
		return false
	}
	mustRoundTrip(&replyCopy, reply)
	return true
}

func (e *endpoint) SendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	target, delay, seq, dup, ok := e.net.route(kindAppendEntries, e.from, to)
	if !ok {
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	e.net.arrive(e.from, to, seq)

	var argsCopy AppendEntriesArgs
	mustRoundTrip(args, &argsCopy)

	if dup {
		// See SendRequestVote's own doc on exactly what "a second time"
		// means here. countAppend runs for the duplicate too -- it is a
		// second, real delivery to the target, not a bookkeeping
		// no-op, and appendStats' own count should reflect that.
		var dupArgs AppendEntriesArgs
		mustRoundTrip(args, &dupArgs)
		e.net.countAppend(len(dupArgs.Entries))
		var discard AppendEntriesReply
		target.AppendEntries(&dupArgs, &discard)
	}

	e.net.countAppend(len(argsCopy.Entries))

	var replyCopy AppendEntriesReply
	target.AppendEntries(&argsCopy, &replyCopy)

	if !e.net.replyDeliverable(kindAppendEntries, e.from, to, seq) {
		return false
	}
	mustRoundTrip(&replyCopy, reply)
	return true
}

// route decides whether a message is delivered and returns the target, any
// delay to apply, a send-order stamp for the reorder accounting, and whether
// this request should ALSO be delivered a second time (see rollDuplicate's
// own doc, and the endpoint SendXxx methods for where the second delivery
// actually happens).
//
// seq is assigned HERE, unconditionally, before the drop roll -- so it names
// which attempted send this is between this pair (the seq-th send from `from`
// to `to`), not which one happened to survive. That assignment is safe to make
// under fn.mu, a lock shared across every pair in the cluster, DESPITE looking
// like the same race this file's own doc comment warns about, for a reason
// specific to this codebase rather than a general property of locks: Raft
// itself never has two goroutines sending to the same peer at once. A
// candidate asks each peer for exactly one vote per term (election.go's
// runElection fans out ONE goroutine per peer, never two to the same one), and
// a leader keeps at most one AppendEntries round in flight per peer by
// construction (replication.go's own coalescing rule, "AT MOST ONE ROUND IN
// FLIGHT PER PEER"). So although many DIFFERENT pairs' goroutines really do
// race for fn.mu here, any GIVEN pair's own seq values are assigned in the
// same causal order every replay produces regardless of who else was racing
// for the lock at the time -- which is exactly what makes it safe to feed into
// messageHash as if it were deterministic, because for a fixed pair it is.
// noCoalesce mode is the one path that doesn't hold this invariant: see
// DESIGN.md §24's open question on it.
//
// A message that is both dropped AND would otherwise have duplicated never
// gets the chance to: dropped means the target never sees it at all, so
// there is nothing left to duplicate -- dup is only ever meaningful, and
// only ever rolled, on the surviving path.
func (fn *fakeNetwork) route(kind messageKind, from, to int) (target *Node, delay time.Duration, seq int, dup bool, ok bool) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	fn.rpcCount++

	if !fn.reachable[from][to] {
		return nil, 0, 0, false, false
	}

	pair := [2]int{from, to}
	seq = fn.nextSeq[pair]
	fn.nextSeq[pair] = seq + 1

	if fn.rollDrop(kind, from, to, seq) {
		fn.dropCount++
		fn.recordRequestLocked(kind, from, to, seq, true, 0, false, false)
		return nil, 0, 0, false, false
	}

	delay = fn.rollDelay(kind, from, to, seq)
	boosted := fn.rollReorderBoost(kind, from, to, seq)
	if boosted {
		delay += fn.maxDelay + 1 // strictly past the highest an ordinary draw can ever be
	}
	dup = fn.rollDuplicate(kind, from, to, seq)
	if dup {
		fn.duplicateCount++
	}
	fn.recordRequestLocked(kind, from, to, seq, false, delay, dup, boosted)

	return fn.nodes[to], delay, seq, dup, true
}

// arrive records a delivery and counts it if an earlier-sent message to the
// same peer has already gone past.
//
// A dropped message still consumed a stamp at route() and never delivers,
// which leaves a permanent hole in the sequence. That is harmless: the hole
// never arrives, so it can neither cause nor mask a count.
func (fn *fakeNetwork) arrive(from, to, seq int) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	pair := [2]int{from, to}
	if high, seen := fn.lastArrived[pair]; seen && seq < high {
		fn.reordered++
		return
	}
	fn.lastArrived[pair] = seq
}

// deliverable re-checks the return path, since the network may have been cut while the handler was running.
func (fn *fakeNetwork) deliverable(from, to int) bool {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.reachable[from][to]
}

// replyDeliverable is deliverable plus the reply-loss roll, both decided for
// the SAME logical message (kind, from, to, seq) the request itself carried --
// deterministic for the identical reason route's own decisions are.
//
// The reply travels the OPPOSITE direction from the request: from `to` (the
// request's receiver) back to `from` (the request's original sender).
// Reachability is checked as (to, from) for that reason, even though the
// trace key stays (kind, from, to, seq) -- the request's own identity, which
// is what a replay needs to look this decision up by.
func (fn *fakeNetwork) replyDeliverable(kind messageKind, from, to, seq int) bool {
	reachable := fn.deliverable(to, from)

	fn.mu.Lock()
	defer fn.mu.Unlock()

	if !reachable {
		fn.recordReplyLocked(kind, from, to, seq, true)
		return false
	}

	if fn.rollReplyDrop(kind, from, to, seq) {
		fn.dropCount++
		fn.recordReplyLocked(kind, from, to, seq, true)
		return false
	}
	fn.recordReplyLocked(kind, from, to, seq, false)
	return true
}

// mustRoundTrip serializes and deserializes to sever pointer sharing between caller and callee, exactly as a real transport would.
func mustRoundTrip(src, dst any) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		panic(err)
	}
	if err := gob.NewDecoder(&buf).Decode(dst); err != nil {
		panic(err)
	}
}

// cluster, the thing tests actually construct.
type cluster struct {
	t     *testing.T
	net   *fakeNetwork
	nodes []*Node
	seed  int64

	// Storage belongs to the SLOT, not the Node: it is what survives a crash
	// and what a restarted Node is built over.
	storage    []*crashableStorage
	baseLogger *slog.Logger
	applyHook  func(node int, msg ApplyMsg)
	applyWG    sync.WaitGroup

	mu   sync.Mutex   //guards dead and nodes
	dead map[int]bool //ids killed via kill()
}

// newCluster builds n fully-connected in-memory nodes.
func newCluster(t *testing.T, n int, seed int64) *cluster {
	t.Helper()
	t.Logf("cluster: n=%d seed=%d", n, seed)

	net := newFakeNetwork(seed)
	c := &cluster{t: t, net: net, seed: seed, dead: make(map[int]bool)}

	t.Cleanup(c.stop)

	base := newTestLogger(t, seed)
	c.baseLogger = base

	for i := 0; i < n; i++ {
		var peers []int
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}

		store := newCrashableStorage()
		c.storage = append(c.storage, store)

		node, err := OpenNode(i, peers, net.endpoint(i), seed*1_000_003+int64(i), store)
		if err != nil {
			t.Fatalf("building node %d: %v", i, err)
		}
		node.SetLogger(base)
		net.register(node)
		c.nodes = append(c.nodes, node)
	}

	return c
}

func (c *cluster) start() {
	c.t.Helper()
	for _, n := range c.nodes {
		n.Start()
	}
	c.t.Cleanup(c.stop)
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

// ---------------------------------------------------------------------------
// Killing nodes
// ---------------------------------------------------------------------------

// kill simulates a crash. B-12 onward.
//
// BOTH halves are necessary, and the second is the one that is easy to miss.
// Stop only closes stopCh, which ends the election ticker and the heartbeat
// loop. It does NOT stop RequestVote and AppendEntries, because the fake
// network calls those handlers directly on the struct -- there is no process to
// exit and no socket to close. A node that is stopped but still reachable goes
// on granting votes forever, so a test that "kills" three of five nodes would
// still see five voters and elect a leader from a minority.
//
// Cutting the network is what makes the node actually absent. Contrast with
// B-14, where a node is partitioned but very much alive.
//
// heal() undoes the second half. Any test that heals after a kill must re-cut
// the dead; see healAroundTheDead in leaderfailure_test.go.

func (c *cluster) kill(id int) {
	c.t.Helper()

	c.nodes[id].Stop()
	c.net.disconnect(id)

	c.mu.Lock()
	c.dead[id] = true
	c.mu.Unlock()
}

func (c *cluster) isDead(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead[id]
}

// alive returns the ids still running.
func (c *cluster) alive() []int {
	var ids []int
	for _, n := range c.nodes {
		if !c.isDead(n.id) {
			ids = append(ids, n.id)
		}
	}
	return ids
}

// leadersByTerm groups every node that believes it leads by the term it thinks it leads in.
func (c *cluster) leadersByTerm() map[int][]int {
	byTerm := make(map[int][]int)
	for _, n := range c.nodes {
		if c.isDead(n.id) {
			continue
		}
		if state, term, _ := n.snapshotState(); state == Leader {
			byTerm[term] = append(byTerm[term], n.id)
		}
	}
	return byTerm
}

// candidatesByTerm groups every live node currently campaigning by the term it is campaigning in.
func (c *cluster) candidatesByTerm() map[int][]int {
	byTerm := make(map[int][]int)
	for _, n := range c.nodes {
		if c.isDead(n.id) {
			continue
		}
		if state, term, _ := n.snapshotState(); state == Candidate {
			byTerm[term] = append(byTerm[term], n.id)
		}
	}
	return byTerm
}

// maxTermAmong reports the highest term seen in a group.
func (c *cluster) maxTermAmong(ids []int) int {
	highest := -1
	for _, id := range ids {
		if _, term, _ := c.nodes[id].snapshotState(); term > highest {
			highest = term
		}
	}
	return highest
}

// waitForSplitVote polls until at least atLeast live nodes are campaigning in the SAME term, and returns that term with the ids involved.
func (c *cluster) waitForSplitVote(atLeast int, within time.Duration) (int, []int) {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for term, ids := range c.candidatesByTerm() {
			if len(ids) >= atLeast {
				return term, ids
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return None, nil
}

// checkSingleLeader returns the id of the leader in the newest term, or None if no node currently believes it leads.
func (c *cluster) checkSingleLeader() int {
	c.t.Helper()

	byTerm := c.leadersByTerm()

	newestTerm := -1
	leader := None

	for term, ids := range byTerm {
		if len(ids) > 1 {
			c.t.Fatalf("ELECTION SAFETY VIOLATED: term %d has %d leaders %v (seed %d)",
				term, len(ids), ids, c.seed)
		}
		if term > newestTerm {
			newestTerm = term
			leader = ids[0]
		}
	}

	return leader
}

// waitForSingleLeader polls until exactly one leader exists, or the bound expires.
func (c *cluster) waitForSingleLeader(within time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if leader := c.checkSingleLeader(); leader != None {
			return leader
		}
		time.Sleep(3 * time.Millisecond)
	}
	return None
}

// leadersAmong is leadersByTerm restricted to one side of a partition.
func (c *cluster) leadersAmong(ids []int) map[int][]int {
	byTerm := make(map[int][]int)
	for _, id := range ids {
		if c.isDead(id) {
			continue
		}
		if state, term, _ := c.nodes[id].snapshotState(); state == Leader {
			byTerm[term] = append(byTerm[term], id)
		}
	}
	return byTerm
}

// waitForStableCluster polls until exactly one leader exists AND every other node has settled into follower.
// Returns the leader's id, or None if the cluster did not settle in time.
func (c *cluster) waitForStableCluster(within time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		leader := c.checkSingleLeader()
		if leader != None && c.allOthersAreFollowers(leader) {
			return leader
		}
		time.Sleep(3 * time.Millisecond)
	}
	return None
}

// waitForNewLeader polls for a leader that is neither the old one nor leading in a term at or below the old one.
func (c *cluster) waitForNewLeader(oldLeader, oldTerm int, within time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if leader := c.checkSingleLeader(); leader != None && leader != oldLeader {
			if _, term, _ := c.nodes[leader].snapshotState(); term > oldTerm {
				return leader
			}
		}
		time.Sleep(3 * time.Millisecond)
	}
	return None
}

// allOthersAreFollowers reports whether every node except the leader has stood down.
func (c *cluster) allOthersAreFollowers(leader int) bool {
	for _, n := range c.nodes {
		if n.id == leader || c.isDead(n.id) {
			continue
		}
		if state, _, _ := n.snapshotState(); state != Follower {
			return false
		}
	}
	return true
}

// othersThan returns every live node id except the given one, in order.
func (c *cluster) othersThan(id int) []int {
	var ids []int
	for _, n := range c.nodes {
		if n.id != id && !c.isDead(n.id) {
			ids = append(ids, n.id)
		}
	}
	return ids
}

// describe renders every node's state, for failure messages.
func (c *cluster) describe() string {
	var b strings.Builder
	for i, n := range c.nodes {
		state, term, votedFor := n.snapshotState()
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "node %d: %v term=%d votedFor=%d", n.id, state, term, votedFor)
		if c.isDead(n.id) {
			b.WriteString(" (KILLED)")
		}
	}
	return b.String()
}

// alignElectionDeadlines forces the given nodes to time out at the same instant.
func (c *cluster) alignElectionDeadlines(at time.Time, ids ...int) {
	c.t.Helper()

	for _, id := range ids {
		n := c.nodes[id]
		n.mu.Lock()
		n.electionDeadline = at
		n.mu.Unlock()
	}
}

// waitForLeaderAmong polls until some node in ids leads in a term of at least minTerm.
func (c *cluster) waitForLeaderAmong(ids []int, minTerm int, within time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for term, leaders := range c.leadersAmong(ids) {
			if len(leaders) > 1 {
				c.t.Fatalf("ELECTION SAFETY VIOLATED: term %d has %d leaders %v (seed %d)",
					term, len(leaders), leaders, c.seed)
			}
			if term >= minTerm {
				return leaders[0]
			}
		}
		time.Sleep(3 * time.Millisecond)
	}
	return None
}

// assertNoLeaderAmong watches a group for the whole window and fails if any member ever leads in a term above aboveTerm.
func (c *cluster) assertNoLeaderAmong(ids []int, aboveTerm int, during time.Duration) {
	c.t.Helper()

	deadline := time.Now().Add(during)
	for time.Now().Before(deadline) {
		for term, leaders := range c.leadersAmong(ids) {
			if term > aboveTerm {
				c.t.Fatalf("MINORITY ELECTED A LEADER: nodes %v lead term %d, above the "+
					"pre-partition term %d, from a group of %d in a cluster of %d (seed %d): %s",
					leaders, term, aboveTerm, len(ids), len(c.nodes), c.seed, c.describe())
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// addNode builds a server that is not yet in anyone's configuration and
// registers it with the network. It starts with an empty configuration, so it
// will not campaign; it becomes a voter when the leader's configuration entry
// reaches it.
func (c *cluster) addNode() int {
	c.t.Helper()

	id := len(c.nodes)
	store := newCrashableStorage()
	c.storage = append(c.storage, store)

	node, err := OpenNode(id, nil, c.net.endpoint(id), c.seed*1_000_003+int64(id), store)
	if err != nil {
		c.t.Fatalf("building node %d: %v", id, err)
	}
	node.SetLogger(c.baseLogger)

	c.mu.Lock()
	c.nodes = append(c.nodes, node)
	c.mu.Unlock()

	c.net.register(node)
	if c.applyHook != nil {
		c.watchNode(node)
	}
	node.mu.Lock()
	node.setConfiguration(nil, 0)
	node.baseServers = nil
	node.mu.Unlock()

	node.Start()
	return id
}