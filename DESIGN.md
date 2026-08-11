# Helios — Design Document (v1)

A distributed, fault-tolerant key-value store built on the Raft consensus protocol.

**Status:** v1 — written before any Raft code exists. Covers the consensus layer only.

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

- **Enters:** from follower, on election timeout.
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

**The up-to-date check.** Compare `lastLogTerm` first; higher term wins. If equal,
the longer log wins.

>The up-to-date check prevents a candidate with a stale log from becoming leader when another server has more recent log entries. This helps preserve committed entries when leadership changes.

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
| election timer | Randomised countdown | Fresh timer on start |

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

## 8. Open questions

Deliberately unresolved in v1. Each is answered by a later phase.

- **Linearizable reads.** Reading local state on the leader is unsafe. Plan: commit a
  no-op entry per read (Phase C), then evaluate lease-based reads.
- **fsync policy.** Per-write fsync is correct but slow. Measure the tradeoff (Phase D).
- **Batching.** Multiple client writes per AppendEntries. Deferred until there are
  baseline numbers to compare against.
- **Snapshots.** Log truncation and `InstallSnapshot` (Phase D).
- **Membership changes.** Single-server add/remove (Phase D).

---

## 9. Revision log

| Version | Date | Change |
|---|---|---|
| v1 | | Initial design: states, RPCs, persistent and volatile state |