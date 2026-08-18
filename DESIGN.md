# Helios — Design Document

A distributed, fault-tolerant key-value store built on the Raft consensus protocol.

**Status:** v1.2 — leader election and log replication implemented. Entries commit on
the leader; nothing is applied to a state machine yet.

---

## 1. Goals and non-goals

### Goals
- A replicated key-value store that never loses a committed write.
- Correctness under crashes, network partitions and message loss.
- A single Raft group, 3 or 5 nodes.

### Non-goals (v1)
- No sharding / multi-Raft. One Raft group, deliberately.
- No transactions across keys, no secondary indexes, no SQL layer.
- No Byzantine fault tolerance. Nodes may crash or be partitioned; they do not lie.

---

## 2. Architecture overview

Each node runs three layers:

```
   client (gRPC)
        |
   [ Raft module ]  <-- RPCs -->  peer nodes
        |
   apply channel
        |
   [ state machine ]
```

Writes go to the leader, are replicated to a majority, and only then are applied to
the state machine. The state machine never sees an uncommitted entry.

The Raft module reaches its peers through a Transport interface rather than a concrete
network client. Tests supply an in-memory switchboard whose connectivity can be cut
arbitrarily; production will supply gRPC.

---

## 3. Node states

Every node is in exactly one of three states at any time.

### Follower
Passive. Responds to RPCs, never initiates them.

- **Enters:** on startup; on seeing a higher term; on hearing from a legitimate leader.
- **Exits:** election timer expires without a valid AppendEntries → becomes candidate.
- **May do:** grant votes, append entries, redirect clients to the leader.

### Candidate
Actively campaigning for leadership.

- **Enters:** from follower on election timeout, or from candidate when its own election fails to resolve.
- **On entry:** increment `currentTerm`, vote for self, reset election timer, send
  RequestVote to all peers in parallel.
- **Exits (three ways):**
  1. Receives votes from a majority → leader.
  2. Receives AppendEntries from a node with term >= its own → follower.
  3. Election timer expires again (split vote) → new election at a higher term.

### Leader
The only node that accepts client writes.

- **Enters:** from candidate, on winning a majority.
- **On entry:** reinitialise `nextIndex[]` and `matchIndex[]` for all followers;
  begin sending heartbeats immediately.
- **Exits:** sees any RPC request or response carrying a term > `currentTerm` → follower.

### The rule that governs all three

> If any node, in any state, sees a term greater than its own, it immediately adopts that term and reverts to follower.
> If a leader from an older term becomes active again after being disconnected, it may still believe that it is the leader. However, another leader may already have been elected in a newer term. Therefore, when the old leader learns about the newer term, it must update its term and become a follower. This prevents an outdated leader from continuing to coordinate the cluster with stale information

---

## 4. RPCs

Two RPCs in v1. `InstallSnapshot` arrives in Phase D.

### 4.1 RequestVote

Sent by candidates to gather votes.

**Request**

| Field | Meaning |
|---|---|
| `term` | Candidate's term |
| `candidateId` | Who is asking |
| `lastLogIndex` | Index of candidate's last log entry |
| `lastLogTerm` | Term of candidate's last log entry |

**Response**

| Field | Meaning |
|---|---|
| `term` | Receiver's current term, so the candidate can update itself |
| `voteGranted` | True if the vote was given |

**Receiver rules**
1. Reply false if `term < currentTerm`.
2. If `votedFor` is null or equals `candidateId`, **and** the candidate's log is at
   least as up to date as the receiver's, grant the vote.

**Ordering matters:** the term-adoption rule from §3 runs before the votedFor check. A vote belongs to a term, so adopting a newer term must clear the old vote. Reversed, a node that voted once in term 4 would refuse every future election forever and the cluster would eventually be unable to elect anyone.

Only a **granted** vote resets the election timer. A rejection does not — a follower that reset its timer for every request it refused could be held off its own election indefinitely by one out-of-date node.

**The up-to-date check.** Compare `lastLogTerm` first; higher term wins. If equal,
the longer log wins.

>The up-to-date check exists to guarantee Leader Completeness: a new leader must already hold every entry that has been committed. Without it, a candidate missing committed entries could win, and would then force followers to delete entries the cluster had already acknowledged to a client.

>The check is sufficient because of majority intersection. A committed entry is, by definition, stored on a majority of servers. Winning an election requires votes from a majority of servers. Any two majorities of the same set must share at least one member, so every possible winner has at least one voter holding every committed entry. That voter compares logs before granting and refuses any candidate whose log is behind its own. A candidate missing a committed entry therefore cannot assemble a majority at all — this is a structural impossibility, not a low probability.

>The comparison is by last-entry term first, and by length only when the terms are equal. Length alone would be wrong: a leader may append entries to a single follower and fail before replicating them anywhere else, leaving that follower with a long log of entries that were never committed. A longer log is not necessarily a more current one. The term of the last entry records when the log last advanced, which is the property that matters.

>The term check and the log check are independent gates. A node partitioned alone can inflate its term arbitrarily by repeatedly campaigning. On rejoining, its high term forces others to step down and adopt it, but the log check still denies it the election

### 4.2 AppendEntries

Sent by the leader. Does **two** jobs: log replication, and — with an empty
`entries[]` — heartbeat that suppresses follower election timers.

**Request**

| Field | Meaning |
|---|---|
| `term` | Leader's term |
| `leaderId` | So followers can redirect clients |
| `prevLogIndex` | Index of the entry immediately preceding the new ones |
| `prevLogTerm` | Term of that entry |
| `entries[]` | Entries to store; empty for heartbeat |
| `leaderCommit` | Leader's `commitIndex` |

**Response**

| Field | Meaning |
|---|---|
| `term` | Receiver's current term |
| `success` | True if the follower contained an entry matching `prevLogIndex`/`prevLogTerm` |
| `conflictIndex`, `conflictTerm` | Fast-backup hint. **Not Figure 2** — see §8. Set only on a log rejection. |

**Receiver rules**
1. Reply false if `term < currentTerm`.
2. Reply false if the log has no entry at `prevLogIndex` with term `prevLogTerm`.
3. If an existing entry conflicts with a new one, delete it **and everything after it**.
4. Append any new entries not already present.
5. If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, index of last new entry)`.

>The consistency check ensures that the follower and leader agree on the log before the new entries are appended. If an existing follower entry conflicts with the leader's entry at the same index, the follower deletes that entry and all entries after it, then accepts the leader's entries. This is safe because conflicting entries are not committed entries. A follower may have persisted an entry that was received from an earlier leader but never reached a majority. Committed entries cannot be overwritten, so removing an uncommitted suffix does not lose a committed write.

**Implementation status.** Rules 1–4 are implemented. Rule 5 is not: followers ignore
`leaderCommit` for now, so only the leader's `commitIndex` ever moves.

---

## 5. Persistent state

Written to stable storage **before responding to any RPC**. This ordering is not
optional; it is the correctness requirement.

| Field | Why it must survive a crash |
|---|---|
| `currentTerm` | Terms must never move backwards. A node that forgets its term could accept a stale leader. |
| `votedFor` | Prevents casting two votes in the same term. |
| `log[]` | The entries themselves. Losing them loses committed writes. |

**On `votedFor`.** This records *who this node voted for*, not who won. A node often
does not know who won.

>A server may vote for only one candidate in a given term. For example, if C votes for A in term 5 and then crashes, C must persist votedFor = A. If C forgets this information after restarting, it could vote for B in the same term. With enough other votes, both A and B could obtain a majority and be elected leaders in term 5, violating Raft's Election Safety property. Therefore votedFor must be persisted before responding to the vote request.

**Not yet persisted.** All three currently live in memory only. Durability is Phase E;
until then a "restart" in tests means a fresh node, not a recovered one.

---

## 6. Volatile state

Lost on restart. Safe to lose, because each is either reconstructed from peers or
conservatively reinitialised.

### On all nodes

| Field | Meaning | Why losing it is safe |
|---|---|---|
| `commitIndex` | Highest entry known committed | Re-learned from the next AppendEntries |
| `lastApplied` | Highest entry applied to the state machine | Re-derived by replaying the log (or from a snapshot, Phase D) |
| `state` | follower / candidate / leader | Always restart as follower; worst case is one extra election |
| `electionDeadline` | When to start an election | Fresh deadline on start |

### Configuration and machinery

| Field | Meaning |
|---|---|
| `id`, `peers` | Cluster membership. `peers` excludes self, so cluster size is `len(peers)+1`. |
| `transport` | How this node reaches peers. Fake switchboard in tests, gRPC in production. |
| `rng` | Per-node seeded source for election timeouts. Never the global `rand`. |
| `mu` | Guards every field above. |

### On leaders only — reinitialised after every election

| Field | Meaning |
|---|---|
| `nextIndex[peer]` | Next log index to send. Optimistically initialised to leader's last index + 1. |
| `matchIndex[peer]` | Highest index known replicated on that peer. Initialised to 0. |

The optimistic/pessimistic split is deliberate: `nextIndex` guesses high and backs
off on rejection, while `matchIndex` claims nothing it has not proven. Initialising
`matchIndex` to the leader's last index for symmetry would let a fresh leader count a
majority immediately and commit entries held on one machine.

Both maps are keyed by peer id and **contain no entry for the leader itself**. There
is no meaningful "next entry to send to myself", and a self entry in `matchIndex`
would need updating on every local append — a step whose omission shows up only as an
off-by-one in the commit count. The majority count starts its tally at 1 instead.

Fresh maps are allocated on each election rather than the old ones cleared, so a
reply handler still in flight from a previous term cannot write into current state.
They are **not** cleared on step-down: clearing swaps one wrong answer (a stale index)
for another (zero, which reads as valid). The real guard is the
`state == Leader && currentTerm == term` check on every path that reads them.

---

## 7. Commit rules

An entry is committed once it is stored on a majority **and** is from the leader's
current term. Once an entry commits, every entry before it commits with it.

### Why the current-term restriction (§5.4.2, Figure 8)

A majority holding entry E stops a candidate that **lacks** E from winning: the two
majorities intersect, and the up-to-date check makes the shared voter refuse. It says
nothing about a candidate whose log is differently shaped but more up to date **by
term**.

Figure 8 is the counterexample. S1 leads term 2, gets index 2 onto itself and S2, and
crashes. S5 wins term 3 from S3 and S4 — legitimately, since neither saw index 2 —
writes its own index 2 in term 3, and crashes. S1 restarts, wins term 4, and finishes
replicating its **inherited** term-2 entry to S3. Index 2 is now on a majority. If
that counted as a commit, S5 could still win term 5 from S2, S3 and S4, because its
last-log term of 3 beats their 2 — and then overwrite the committed entry.

Waiting for a current-term entry closes it. Every node in that majority then has a
last-log term of at least `currentTerm`, so no candidate with an older tail can
out-rank them. The rule is not merely cautious: it manufactures the condition that
puts the entry beyond reach of any future leader.

Older entries commit **indirectly**, carried by Log Matching when something above them
commits directly.

### Consequence: inherited entries stall

A new leader holding uncommitted entries from previous terms cannot commit them until
it appends one of its own. On an idle cluster they sit indefinitely, and any client
waiting on them waits with them.

The standard remedy is a blank no-op entry appended on election (§8 of the paper).
Not implemented: it changes the indices clients observe and interacts with read-only
query handling, so it belongs to the task that needs it. Recorded here so the absence
is a decision, not an oversight.

---

## 8. Implementation decisions

Each decision paired with the alternative rejected and why.

### Log and entries

**Sentinel entry at `log[0]`.** Raft indexes from 1; Go slices index from 0. A dummy
entry (term 0, no command) makes slice position equal Raft index everywhere.
*Rejected:* an offset translated at every access — the translation leaks into every
handler and each site is an off-by-one waiting to happen. The sentinel also makes
`prevLogIndex = 0` work naturally for the first AppendEntries, and it is what
**guarantees log repair terminates**: a leader that has backed all the way off always
finds agreement at index 0.
*Cost:* Phase D truncation breaks position-equals-index. Revisit then.

**`LogEntry` has no `Index` field.** Position in the slice is the index; storing it too
creates two sources of truth that can disagree — and they would, the first time a
follower truncates. *Revisit at:* Phase D.

**`LogEntry.Command` is immutable once appended.** `Submit` copies the caller's bytes
in so the log owns them, and nothing writes to them afterwards. This is what lets
outgoing messages copy entries **shallowly** — the message gets its own view of *which*
entries are being sent, but shares the command bytes.
*Rejected:* deep-copying every command on every send, which costs an allocation and
memcpy of the whole outstanding log per peer per tick.
*Obligation:* consumers must treat `Command` as read-only. The pressure point is the
state machine in Phase F — a handler that decodes into the slice it was handed would
rewrite committed history.

**`votedFor` sentinel is `-1`, not a pointer.** *Rejected:* `*int` with `nil` meaning
"no vote". Nil dereference risk for no benefit.

### Timing and concurrency

**Election timer is a deadline, polled by a ticker.** An absolute `time.Time`, checked
every 10ms by one goroutine.
*Rejected:* `time.Timer` with `Reset` — resetting a timer that may have fired but not
been drained is a documented Go footgun, and this timer is reset from several
goroutines constantly. Polling makes a reset a plain field assignment.
*Cost:* up to 10ms imprecision, irrelevant against a 150–300ms timeout.

**Election timeouts from a per-node seeded RNG.** Never the global `rand`. Seed is
`clusterSeed + nodeID` so nodes differ while a whole run replays from one number.
*Caveat:* this is seeded **fault injection**, not deterministic simulation — goroutine
scheduling still varies. True replay needs a virtual clock (Phase G).

**Vote requests fan out in parallel.** One goroutine per peer, replies on a buffered
channel. *Rejected:* a sequential loop — one unreachable peer would consume the entire
election window. Every reply is re-checked against current state and term before being
counted; counting a reply from an election the node has left is the standard route to
two leaders in one term.

**Peers reached through a `Transport` interface.** *Rejected:* a gRPC client inside
`Node`. The interface is what makes partition and message-loss testing possible at all.

### Replication

**One send path for heartbeats and replication.** There is no separate heartbeat
message; `replicateAll` sends each follower whatever it is missing, which is nothing
when that follower is caught up. Consequence: the consistency check runs during idle
periods, so a divergent follower is detected without waiting for a client write.

**One args struct per peer, all built under one lock.** Peers sit at different points
in their logs, so a shared pointer would send them each other's consistency checks.
Building the whole fan-out at one instant also means the leader's picture of the
cluster is self-consistent.
*Rejected:* lock-per-peer inside the loop — peers would see the log at different
moments, and once replies mutate `nextIndex` concurrently the leader's view becomes
incoherent with itself.

**Entries are copied out of the log before sending.** A subslice would hand the network
goroutine a window into the live log, which a later append can reallocate and a
truncation can rewrite.

**`Submit` returns before replication, and its index is a prediction.** It appends
locally and returns `(index, term, isLeader)` immediately. If this leader is deposed
before the entry commits, a later leader may overwrite that position.
*Obligation on callers:* observe that index applied **with the returned term**. A
different term at that index means the submission was overwritten and must be reported
as failed, not retried — a blind retry risks double-applying if the original committed
after all.
*Known limit:* one goroutine per `Submit` means bursts produce redundant fan-outs. The
fix is per-follower replication goroutines woken by a condition variable; deferred
until there is a measurement.

**A rejected AppendEntries still resets the election timer.** The reset happens
**before** the log check. Rejection means the follower is behind or diverged — normal
after a partition heals, and exactly when the leader is repairing it. Resetting only
on acceptance would have a lagging follower time out mid-repair and campaign,
disrupting a healthy cluster at the worst moment. Legitimacy is decided by term
(receiver rule 1, the only rejection that withholds the reset); the log check decides
only whether *this message* applies.

**Truncation happens at the first conflicting term, not at `prevLogIndex`.** Rule 3 is
implemented literally: scan the incoming entries against the log and truncate where
the terms actually differ.
*Why it matters:* RPCs arrive out of order. A leader sends 1..5 then 1..8; the 1..8
message lands first; the delayed 1..5 then arrives with `prevLogIndex = 0`, which
passes the consistency check because the sentinel always matches. Blind truncation
would destroy entries 6–8, which the leader may already have committed.

**A repaired follower may be longer than the leader.** Entries at indices no message
has addressed stay put — they are uncommitted, and Log Matching constrains only
claimed indices. They are truncated when the leader sends something at those positions.
*Consequence for tests:* asserting "follower log == leader log" straight after repair
is wrong.

### Leader bookkeeping

**`matchIndex` is derived from what was sent.** `prevLogIndex + len(entries)`, never
`lastLogIndex()` at reply time. The log may have grown since the message left, and
crediting a follower with entries it never received is how a leader counts a majority
for an entry that exists on one machine.

**Reply handling is tied to the attempt that produced it.** `matchIndex` advances with
`max()`, so a late reply to an older, shorter message cannot un-prove agreement already
counted toward a commit. Backoff ignores any reply whose `prevLogIndex + 1` differs
from the current `nextIndex` — applying a superseded rejection would push `nextIndex`
back to where it started, a live-lock where repair keeps undoing itself.

**Fast-backup hint on the reply (deviation from Figure 2).** Figure 2's reply has two
fields; Helios adds `conflictIndex` and `conflictTerm` (§5.3 of the paper).

| Rejection | Follower reports | Leader does |
|---|---|---|
| Log too short | term 0, index = its `lastLogIndex + 1` | resume there |
| Term mismatch | the term it holds, and where that term's run begins | resume past its own last entry of that term, or discard the whole run if it has none |

*Safety rule:* the hint may only move `nextIndex` **backwards**. Moving it forward
would assume agreement never verified. `nextIndexAfterConflict` clamps to
`[1, current-1]` regardless of what the reply says, so a malformed hint costs round
trips rather than correctness.

*Measured:* across the six Figure 7 scenarios, 25 round trips → 12. Cases (a), (c) and
(d) are unchanged — they diverged by at most one index, so there was nothing to skip.
The entire benefit is in the badly diverged cases, which is why the paper treats it as
optional.

---

## 9. Testing conventions

**Scope fences.** A test may assert that a feature is *not yet* built, so that the task
implementing it gets a red build instead of silent scope creep. Fence failure messages
name the retiring task, making them greppable:

```
grep -rn "is C-[0-9]" internal/raft/*_test.go
```

Run it before starting a task. Convert rather than delete when the test still covers
something. Three have expired so far (C-5, C-6, C-10).

**Property checkers live in `properties_test.go`.** `assertLogMatching` and
`assertPrefixIntact` are stated over any set of logs, not one scenario, because every
later phase must re-establish them: Phase D that snapshotting preserves them, Phase E
that a restart from disk does, Phase H that a membership change does.

**Safety is monitored over runs, not asserted at moments.** `commitLedger` accumulates
every entry any node has committed and re-checks the set after each step. The property
is §5.4.3: once an entry is committed at an index, no node may hold a different entry
at that index **within its own committed prefix**. The qualifier is load-bearing —
nodes routinely hold divergent uncommitted entries, and Figure 8 turns on one surviving
on S5 for two terms.

**Safety tests are paired with a mutation that must break them.** A passing safety test
is worth nothing until it has been seen to fail for the right reason.
`TestFigure8DetectsAMissing5_4_2Check` replays the Figure 8 narrative with a
test-local commit rule that omits the term check and requires the monitor to catch it.
The mutation runs the real rule first and the unsafe one after — since the unsafe rule
can only advance `commitIndex` further, the result is exactly what a build without
§5.4.2 would produce, with no production code touched and no test-only knob added.
**A green result there is a failure to investigate, not a success.**

**Elections in scenario tests are real.** Figure 8 runs through the actual
`RequestVote` and up-to-date check, because S5's ability to overwrite a
majority-replicated entry depends on the election restriction *permitting* it.
Deciding elections by hand would assume away the mechanism under test.

**Timer assertions.** "Was reset" needs a property test — force the deadline to expire,
then assert remaining time is positive and within bounds. Never compare two randomly
drawn deadlines. "Was **not** reset" may use equality, since no second draw happened.

**Test files ship.** Go excludes `_test.go` from `go build`, so they add nothing to the
binary. The cost is CI time, addressed by tiering with `testing.Short()` rather than by
deletion. For a consensus implementation the tests are the only available evidence of
correctness.

---

## 10. Open questions

Deliberately unresolved. Each is answered by a later phase.

- **Election timer reset on stepping down.** `becomeFollower` resets the timer whenever
  a node adopts a newer term, including when it then refuses the vote on the up-to-date
  check. Figure 2 lists only two reset events, so this is a third.
  *Alternative rejected:* not resetting — a leader's deadline is never refreshed while
  it leads, so a leader stepping down would hold a deadline far in the past and
  immediately campaign against whoever deposed it, disrupting every failover.
  *The cost is real:* a node partitioned alone inflates its term by campaigning, and on
  rejoining forces the whole cluster to step down and reset despite a log too stale to
  win. **Prevote (D-12)** is the proper fix. Pinned by
  `TestHigherTermResetsTimerEvenWhenRefused`, to be inverted when Prevote lands.
- **No-op entry on election.** Would release entries inherited from previous terms
  immediately (§7), and is also the foundation for `ReadIndex`. Deferred because it puts
  an entry in the log no client submitted, which every index-reporting path must
  account for. Decide before Phase F rather than during.
- **Linearizable reads.** Reading local state on the leader is unsafe — a deposed
  leader does not know it. Plan: `ReadIndex` (no-op at start of term, then confirm
  leadership by heartbeat before serving), then evaluate lease-based reads.
- **fsync policy.** Per-write fsync is correct but slow. Measure the tradeoff (Phase E).
- **Batching.** Multiple client writes per AppendEntries, and coalescing the per-`Submit`
  fan-out. Deferred until there are baseline numbers.
- **Snapshots.** Log truncation and `InstallSnapshot` (Phase D). Breaks the
  sentinel/position-equals-index convention, the `lastIndexOfTerm` scan, and the
  assumption in `logMatchesAt` that index 0 is always checkable. A `prevLogIndex` below
  the snapshot floor must be answered with `InstallSnapshot`, not a rejection.
- **Membership changes.** Single-server add/remove (Phase D/H). Note that
  `logMatchesAt` is only correct because of one-leader-per-term, which is exactly what
  joint consensus exists to preserve.
- **Test suite runtime.** ~2 minutes under `-race` and growing, dominated by tests that
  sleep in real multiples of `electionTimeoutMax`. Candidate fix: pull the virtual clock
  (G-2) forward.

---

## 11. Revision log

| Version | Date | Change |
|---|---|---|
| v1 | | Initial design: states, RPCs, persistent and volatile state |
| v1.1 | | Through B-8. Majority-intersection argument for the up-to-date check; §8 implementation decisions; timer-reset deviation recorded as an open question; volatile state expanded to match the implementation |
| v1.2 | | Through Phase C. §7 commit rules written up with the Figure 8 argument; fast-backup hint recorded as a deviation with measured saving; §8 reorganised into log / timing / replication / bookkeeping; §9 testing conventions added; open questions updated |