# Helios — Design Document (v1)

A distributed, fault-tolerant key-value store built on the Raft consensus protocol.

**Status:** v1.1 — written before any Raft code exists. Covers the consensus layer only.

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

The Raft module reaches its peers through a Transport interface rather than a concrete network client. Tests supply an in-memory switchboard whose connectivity can be cut arbitrarily; production will supply gRPC.

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

**Receiver rules**
1. Reply false if `term < currentTerm`.
2. Reply false if the log has no entry at `prevLogIndex` with term `prevLogTerm`.
3. If an existing entry conflicts with a new one, delete it **and everything after it**.
4. Append any new entries not already present.
5. If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, index of last new entry)`.

>The consistency check ensures that the follower and leader agree on the log before the new entries are appended. If an existing follower entry conflicts with the leader's entry at the same index, the follower deletes that entry and all entries after it, then accepts the leader's entries. This is safe because conflicting entries are not committed entries. A follower may have persisted an entry that was received from an earlier leader but never reached a majority. Committed entries cannot be overwritten, so removing an uncommitted suffix does not lose a committed write.

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
off on rejection, while `matchIndex` claims nothing it has not proven.

---

## 7. Commit rules

- An entry is committed once it is stored on a majority **and** is from the leader's
  current term.
- Once committed, all prior entries are committed too.

<!-- TODO (yours): the current-term restriction is the subtlest rule in the paper
     (Figure 8). Come back and write this up when you reach task C-9. Leaving it
     unanswered for now is fine; pretending it is obvious is not. -->

---

## 8. Implementation decisions
 
Each decision paired with the alternative rejected and why.
 
### Log indexing: a sentinel entry at `log[0]`
 
Raft indexes from 1; Go slices index from 0. A dummy entry at position 0 (term 0,
no command) makes slice position equal Raft index everywhere.
 
*Rejected:* storing an offset and translating at every access. It works, but the
translation leaks into every RPC handler and every comparison, and each site is an
off-by-one waiting to happen. The sentinel also makes `prevLogIndex = 0` work
naturally for the very first AppendEntries instead of being a special case.
 
*Cost:* Phase D snapshotting truncates the log and breaks the position-equals-index
invariant. This will need revisiting then.
 
### `LogEntry` has no `Index` field
 
Position in the slice is the index. Storing it too creates two sources of truth
that can disagree.
 
*Revisit at:* Phase D, for the same truncation reason.
 
### `votedFor` sentinel is `-1`, not a pointer
 
*Rejected:* `*int` with `nil` meaning "no vote". Nil dereference risk for no
benefit; `-1` compares cheaply and serialises trivially.
 
### Election timer is a deadline, polled by a ticker
 
An absolute `time.Time`, checked every 10ms by one goroutine.
 
*Rejected:* `time.Timer` with `Reset`. Resetting a timer that may have already
fired but not been drained is a documented Go footgun, and this timer is reset from
several goroutines constantly. Polling makes a reset a plain field assignment: no
draining, no stale firings arriving after a reset.
 
*Cost:* up to one tick (10ms) of imprecision. Irrelevant against a 150–300ms
timeout.
 
### Election timeouts drawn from a per-node seeded RNG
 
Never the global `rand`. The seed is `clusterSeed + nodeID`, so nodes within one
cluster differ while the whole run replays from a single number.
 
*Why:* task G-2 requires that any failure reproduce exactly from a seed. Retrofitting
determinism is far harder than starting with it.
 
*Caveat:* this gives seeded **fault injection**, not deterministic simulation.
Goroutine scheduling still varies between runs. True replay needs a virtual clock
and a single-threaded event loop — a real rework, deferred to Phase G.
 
### Peers reached through a `Transport` interface
 
*Rejected:* a gRPC client inside `Node`. The interface lets tests substitute an
in-memory switchboard with controllable connectivity, which is what makes partition
and message-loss testing possible at all.
 
### Vote requests fan out in parallel
 
One goroutine per peer, replies collected on a buffered channel.
 
*Rejected:* a sequential loop. A single unreachable peer would consume the entire
150–300ms election window, so no election could ever complete while any node was
down — which defeats the purpose of the system.
 
Every reply is re-checked against the node's current state and term before being
counted. A reply from an election the node has already left must be discarded;
counting one is the standard route to two leaders in a single term.
 
### `becomeFollower` resets the election timer
 
See §9 — this is a known deviation from Figure 2, kept deliberately.
 
---
 
## 9. Open questions
 
Deliberately unresolved. Each is answered by a later phase.
 
- **Election timer reset on stepping down.** `becomeFollower` resets the timer
  whenever a node adopts a newer term, including when it then refuses the vote on
  the up-to-date check. Figure 2 lists only two reset events (a granted vote, and
  AppendEntries from the current leader), so this is a third.
  *Alternative rejected:* not resetting. A leader's deadline is never refreshed
  while it leads (the ticker skips leaders), so a leader stepping down would hold a
  deadline far in the past and would immediately campaign against whoever just
  deposed it — disrupting every failover. Resetting trades a certain disruption for
  a rarer one.
  *The rarer one is real:* a node partitioned alone inflates its term by
  campaigning, and on rejoining forces the whole cluster to step down and reset,
  despite having a log too stale to win. **Prevote (Phase D-12)** is the proper fix.
  Current behaviour is asserted explicitly by
  `TestHigherTermResetsTimerEvenWhenRefused`, which should be inverted when Prevote
  lands.
- **Linearizable reads.** Reading local state on the leader is unsafe. Plan: commit a
  no-op entry per read (Phase C), then evaluate lease-based reads.
- **fsync policy.** Per-write fsync is correct but slow. Measure the tradeoff (Phase D).
- **Batching.** Multiple client writes per AppendEntries. Deferred until there are
  baseline numbers to compare against.
- **Snapshots.** Log truncation and `InstallSnapshot` (Phase D). Will break the
  sentinel/position-equals-index convention from §8.
- **Membership changes.** Single-server add/remove (Phase D).
---

## 10. Revision log
 
| Version | Date | Change |
|---|---|---|
| v1 | | Initial design: states, RPCs, persistent and volatile state |
| v1.1 | | Through B-8. Majority-intersection argument for the up-to-date check; §8 implementation decisions; timer-reset deviation recorded as an open question; volatile state expanded to match the implementation |
 