# Helios — Design Document

A distributed, fault-tolerant key-value store built on the Raft consensus protocol.

**Status:** v1.24 — leader election, log replication, the apply path, linearizable
reads, persistence and snapshotting are implemented. Entries commit on a majority, apply
in order on every node, survive a crash of the process or the machine, and can be read
back either through a barrier or, under a bounded-clock assumption, from a leader's
lease. The log is compacted behind a state-machine image, and a follower that falls
below the resulting floor is repaired with `InstallSnapshot` rather than entries.
Verified under crashes, restarts, network partitions, message loss, reordering, and a
node offline for ten thousand entries. The LSM storage engine — write-ahead log,
memtable, SSTable read/write with optional per-block compression, a Bloom filter
(measured, not yet wired into the SSTable read path), a shared LRU block cache, a
merged read/write path, and leveled compaction with a background runner and startup
recovery — is wired in as the actual Raft state machine (§14): `internal/kvstore.Machine`
consumes a real `raft.Node`'s `ApplyCh` end to end, implements both linearizable read
paths against the real storage engine, and implements Raft's snapshot contract (a
logical image of the full live key set). `cmd/helios` is a real, if single-node-only,
running program. A full-system test exists and has completed a real 300,000-key run
(§14.8) — reduced from the originally-planned one million after a real attempt at that
scale was watched for 19 hours and deliberately killed, its own trajectory being the
finding: restart currently reapplies the *entire* committed history rather than only
whatever wasn't yet durable (§14.9, high priority in §12); every applied write pays for
two separate, lock-held fsyncs, confirmed with unusual precision once restart-replay —
which pays for only one of the two — ran 21.85× faster than live writes on the same
303,000 entries (§14.10, §14.12); and the test's own first restart measurement was
silently measuring the wrong thing, found from a real run's reads timing out rather
than from review (§14.11). All findings correct on every value checked at every scale
tested; all costing (or previously hiding the cost of) more than a well-scoped system
should. The client-facing gRPC wire contract is now defined — `Get`, `Put`, `Delete`,
`Scan`, `Watch` (§15) — but nothing serves or calls it yet: no gRPC server, no client
library, no leader-hint or idempotency handling. That, a background flush goroutine,
and a handful of "asserted, not yet measured against a real workload" constants are
what remain, tracked explicitly in §12, §14.7, and §15.5.

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

The boundary is one-directional and strict. Everything below the apply channel is
agreement; everything above it is the key-value store. The layer above never reads
the log, the commit index or the applied index — if it did, it would be re-deriving
an order the consensus layer has already established, and the two derivations would
eventually disagree.

The Raft module reaches its peers through a Transport interface rather than a concrete
network client. Tests supply an in-memory switchboard whose connectivity can be cut
arbitrarily; production will supply gRPC.

Durable state sits behind a `Storage` interface on the same principle. Tests supply an
in-memory implementation whose writes can be made to stop reaching the medium partway
through a crash; production supplies a file. See §5.

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

Two RPCs in v1. `InstallSnapshot` arrives with snapshotting.

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
| `conflictIndex`, `conflictTerm` | Fast-backup hint. **Not Figure 2** — see §10. Set only on a log rejection. A `prevLogIndex` below the snapshot floor takes the "too short" shape, because the entries are gone and this node cannot say what term they carried. |

**Receiver rules**
1. Reply false if `term < currentTerm`.
2. Reply false if the log has no entry at `prevLogIndex` with term `prevLogTerm`.
3. If an existing entry conflicts with a new one, delete it **and everything after it**.
4. Append any new entries not already present.
5. If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, index of last new entry)`.

>The consistency check ensures that the follower and leader agree on the log before the new entries are appended. If an existing follower entry conflicts with the leader's entry at the same index, the follower deletes that entry and all entries after it, then accepts the leader's entries. This is safe because conflicting entries are not committed entries. A follower may have persisted an entry that was received from an earlier leader but never reached a majority. Committed entries cannot be overwritten, so removing an uncommitted suffix does not lose a committed write.

**Rule 5's `min` is the subtle one.** The second term is derived from the **message**
(`prevLogIndex + len(entries)`), never from the receiver's own last log index. The two
differ exactly in the Figure 7 cases where a follower holds a private tail the leader
never had. A heartbeat carrying no entries passes the check at `prevLogIndex` and
announces a high `leaderCommit`; committing to that bare value would commit the private
tail, the state machine would apply it, and the leader's next real message would
truncate it. An entry that was applied and then vanished is precisely what Raft exists
to prevent, and the client was told it succeeded.

Read it the safe way: the message proves agreement up to `prevLogIndex + len(entries)`
and says nothing whatsoever about the indices beyond. `leaderCommit` is a fact about the
leader's log; the message bound is the prefix of that log this node can vouch for.
Commit the smaller.

**A leader refuses a same-term AppendEntries.** Figure 2 does not say what a leader
should do with a message at exactly its own term, because Election Safety means the
situation cannot arise: one vote per node per term, and a majority is required to win.
Helios refuses it anyway and logs an error, on the same principle as the other
believed-impossible guards (§8). The damage from trusting the invariant here is silent
and asymmetric — a rival's message would truncate the log this node is still
replicating from, while its heartbeat loop carried on telling followers it leads.

### 4.3 InstallSnapshot

Sent by the leader when a follower's `nextIndex` has fallen to or below the leader's log
floor — the entries it is owed no longer exist, so there is nothing to replicate and the
image goes instead. See §5 for what the floor is and §10 for how the leader detects it.

**Request**

| Field | Meaning |
|---|---|
| `term` | Leader's term |
| `leaderId` | So followers can redirect clients |
| `lastIncludedIndex` | The last log index the image accounts for |
| `lastIncludedTerm` | That entry's term |
| `data` | The state-machine image, opaque to Raft |

**Response**

| Field | Meaning |
|---|---|
| `term` | Receiver's current term |

**No `success` field, deliberately.** A follower that rejects on term deposes the leader,
and the leader learns that from `term`. A follower that accepts has nothing to negotiate:
the image is not a proposal. Any other failure is the follower's own and a retry on the
next round is the whole remedy.

**Receiver rules**
1. Reply immediately if `term < currentTerm`.
2. Discard an image at or below `commitIndex` — this node has already passed it, and
   installing it would rewind the state machine. A leader can send one quite
   legitimately, having decided to send before a burst of AppendEntries it had already
   put on the wire arrived.
3. Write the image to stable storage **before** discarding anything.
4. If this node holds an entry at `lastIncludedIndex` with a matching term, keep
   everything after it; otherwise discard the whole log.
5. Hand the image to the state machine, and raise `commitIndex` to the floor.

**Rule 4 is an optimisation, and it matters more than it looks.** If the entry matches,
then by Log Matching everything before it agrees too, and the entries *after* it are as
valid as they were a moment ago. Discarding them would throw away a tail the leader must
then resend, turning a cheap repair into a full image plus a full retransmission.

**A DEVIATION FROM FIGURE 13: no `offset` or `done`.** The paper chunks the image so a
receiver can bound how much it buffers. Helios sends it whole, because the transport
already materialises a whole message and chunking would add a reassembly state machine —
with its own partial-transfer and leader-change-mid-transfer cases — to buy nothing at
the sizes this system currently reaches. It becomes necessary when an image stops
fitting comfortably in one RPC. That limit is real and is not measured.

**Delivery to the state machine is deferred to the applier.** The handler holds `mu`, and
sending on an unbuffered channel under the lock is the deadlock the single-applier design
exists to prevent (§8). It parks the image and signals; the applier picks it up, delivers
it, and only then advances `lastApplied`. That keeps one goroutine as the only sender on
the apply channel, which is what makes delivery ordering mean anything.

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

**Implemented.** All three reach stable storage before any reply or outgoing RPC that
depends on them. `fsync-policy.md` covers what "stable" is allowed to mean and what
each answer costs; this section covers the mechanism.

### The record

`Storage` is two methods over a single opaque blob — `Save(b []byte)` and `Load()
([]byte, error)` — rather than `Save(term, vote, log)`. The three fields have to become
durable together or not at all: a record holding a new term beside an old log is not a
state any correct node ever occupied, and a restart that adopted it would be worse than
losing the write outright. One blob makes that unrepresentable at this layer.

The blob is framed `magic[4] | version[4] | len(payload)[4] | crc32(payload)[4] |
payload`, the payload gob-encoded. `Load` returns `(nil, nil)` — and only that — when
nothing has ever been saved, which is the single case meaning "fresh node".

**The record also carries `lastIncludedIndex`, the Raft index that `Log[0]` stands for.**
Without it the log is ambiguous once compaction can shorten it: the slice is relative,
position 0 is the floor rather than index 0, and a four-entry record could equally be the
whole log of a node that has never snapshotted or the tail of one whose floor sits at
6,000. Reading the second as the first silently renumbers every entry. The floor's *term*
needs no field of its own — `Log[0]` is the floor entry and already carries it, and
deriving it means the two can never disagree. A record written before this field existed
decodes it as zero, which is exactly "nothing discarded", so the record version did not
need to change.

**A record that exists but does not decode is a fatal error, never a fresh start.** A
node that answers corruption by resetting to term 0 with no vote has invented permission
to vote twice in a term it already voted in — the exact failure this section exists to
prevent. `OpenNode` returns the error and no node, so an operator decides to wipe the
directory rather than the process deciding for them.

### Atomicity

`FileStorage.Save` writes a temp file, flushes it, renames it over the live file, and
flushes the directory. A write of more than one sector is not atomic, so overwriting in
place would leave a head from the new record and a tail from the old — a blob that may
still decode, since a shorter log with a higher term is structurally valid. `rename(2)`
is the cheapest primitive giving all-or-nothing replacement.

The directory flush is the step usually omitted. The rename is a metadata operation on
the directory, and flushing the file's contents does not flush the entry pointing at
them. A leftover temp file is never authoritative — it is the residue of a `Save` killed
before its rename, holding a record nobody was ever told about — and is removed when a
storage is opened.

### When the write happens

`markDirty` sits beside every assignment to the three fields and nowhere else;
`persistIfDirty` flushes at every point a mutation becomes visible to anyone else. The
flag exists so that a handler mutating twice — `becomeFollower` for the term, then
`mergeEntries` for the log — costs one write rather than two.

"Before responding" is necessary but not sufficient. There are three exits, not one:

| Exit | Site | If the write came after |
|---|---|---|
| RPC reply | `RequestVote`, `AppendEntries` | a double vote; a follower acknowledges an entry it then forgets |
| Outgoing RPC | `becomeCandidate` → `runElection` | crash once RequestVote is on the wire, restart at the old term, vote again in it |
| Commit accounting | `appendAndReplicate` → `advanceCommitIndex` | a single-node cluster commits and applies an entry that was never written |

In the handlers the idiom is `defer n.persistIfDirty()` immediately after `defer
n.mu.Unlock()`. Defers run last-in-first-out, so the flush happens under the lock and
before the caller can read the reply, and one line covers every return path including
ones added later. The other two exits need an explicit call mid-body, because the
mutation escapes before the function returns.

`becomeFollower` marks dirty only inside the branch where the term actually rises. The
same-term step-down runs on every heartbeat and changes nothing persistent; marking
unconditionally would cost a flush per heartbeat forever — a fault that is not a
correctness bug and that no correctness test would ever see.

The invariant is greppable, in the same style as the commit-index funnel:

```
grep -rn 'currentTerm = \|currentTerm++\|votedFor = \|n\.log = ' internal/raft/*.go | grep -v _test.go
```

Every hit must have a `markDirty` in the same locked region, and every path leaving that
region must pass a `persistIfDirty`.

### A write that fails stops the node

`persist` panics when `Save` returns an error. A node that cannot make its state durable
must stop participating that instant: carrying on would let it grant a vote it cannot
remember, which is indistinguishable from having no persistence at all except that the
operator now believes it has some. The panic is the blunt version; a halt that keeps the
process alive to report why it stopped belongs with the operational work.

### What the tests establish, and what they cannot

Atomicity is established by a child process SIGKILLed at random points across twelve
rounds sharing one directory: whatever survives must decode, be internally consistent,
and hold every term the process had already announced. That the handlers actually use
the storage is established separately, by reading the storage directly after a granted
vote, a refused vote, an append and a truncation. End-to-end recovery is established
across a hundred seeded rounds that crash a node under load and restart it.

**None of that tests fsync.** SIGKILL destroys a process, not a page cache, so every one
of those tests would pass with the flush calls deleted. The fsync policy rests on the
argument in `fsync-policy.md`, not on a green suite.

What a restart *costs* is measured separately, at image sizes up to a gigabyte — see
§10.

### The snapshot record

A second durable object, in the same directory and under the same `Storage` interface:

| Field | Meaning |
|---|---|
| `lastIncludedIndex` | The last log index the image accounts for |
| `lastIncludedTerm` | That entry's term |
| `data` | The state-machine image, opaque to Raft |

Framed like the state record — magic, version, length, CRC32 — but by hand rather than
through gob, because the payload is three fixed fields and a byte run that may be
megabytes, and gob would reflect over the image and copy it again for nothing.

**Why `lastIncludedTerm` is stored and not derived.** A leader may send AppendEntries
with `prevLogIndex` equal to the floor, and the receiver has to answer the consistency
check at that boundary — but the entry whose term the check needs was discarded when the
snapshot was taken. The header is the only surviving record of it. Without the field a
follower would reject every check at the floor and could never be repaired past it.

**Why it is a separate record when §5 argues so hard for one blob.** Size. The state
record is rewritten on every term change and every append; folding a multi-megabyte image
into it would make granting a vote cost a full state-machine rewrite. The atomicity that
one blob gave for free is replaced by an explicit ordering rule.

**A record that exists but does not decode is fatal here too.** A node that treats an
unreadable image as no image has silently rewound its own state machine to empty while
continuing to claim the identity that promised otherwise.

### The ordering rule

**The image must be durable before the truncated log is.** The two crash windows are not
symmetric:

| Crash point | Result |
|---|---|
| Image written, log not yet truncated | The log still holds everything the image covers. Redundant, not wrong; recovery drops the overlap. |
| Log truncated, image not yet written | Entries are gone and nothing accounts for them. Committed state lost, with nothing left on disk that knows. |

One is a wasted write and the other is data loss, so the write that *adds* information
goes first. `Node.Snapshot` and the `InstallSnapshot` receiver both follow it: image
durable, then truncate in memory, then write the shortened log.

### What a restart does with the two records

Both carry a floor, written at different moments, and only their combination says whether
the node's history is intact. `OpenNode` positions the log from the state record's floor,
then reconciles:

| Snapshot floor vs log floor | Meaning | Response |
|---|---|---|
| Behind | The log was compacted past the image meant to cover it | Impossible from correct writes — refuse to start |
| Equal | A clean compaction | Check the two independently written terms agree |
| Ahead | Crashed between the two writes | Finish the interrupted job: compact in memory |

The image is then parked for the applier, which delivers it before any entry. The state
machine restarted empty too, so it has to be rebuilt from the image and the tail — and
`lastApplied` stays where it was until the applier has actually handed it over, which is
the same past-tense rule the entry path follows (§8).

**This answers the replay question that §6 used to defer.** A restarted node does not
replay from index 1; it replays from the snapshot floor. No prefix is ever applied twice,
so the state machine needs no idempotence for *replay*. Duplicate commands from a client
retrying across a leader change remain a separate, open problem (§12).

---

## 6. Volatile state

Lost on restart. Safe to lose, because each is either reconstructed from peers or
conservatively reinitialised.

### On all nodes

| Field | Meaning | Why losing it is safe |
|---|---|---|
| `commitIndex` | Highest entry known committed | Re-learned from the next AppendEntries |
| `lastApplied` | Highest entry applied to the state machine | Re-derived by replaying the log from the snapshot floor |
| `state` | follower / candidate / leader | Always restart as follower; worst case is one extra election |
| `electionDeadline` | When to start an election | Fresh deadline on start |

**On restart both return to the snapshot floor — zero on a node that has never
compacted — and the log replays from there.** `commitIndex` goes straight to the floor,
because everything an image covers is committed by definition. `lastApplied` does not:
it waits until the applier has actually handed the image to the state machine, which
also restarted empty. Persisting `lastApplied` without persisting the state machine
beside it would skip the rebuild and leave the node serving nothing.

### Configuration and machinery

| Field | Meaning |
|---|---|
| `id`, `peers` | Cluster membership. `peers` excludes self, so cluster size is `len(peers)+1`. |
| `transport` | How this node reaches peers. Fake switchboard in tests, gRPC in production. |
| `rng` | Per-node seeded source for election timeouts. Never the global `rand`. |
| `applyCh`, `applyNotify`, `applierDone` | The apply plumbing. See §8. |
| `storage`, `persistDirty` | The durability boundary, and the flag that lets one handler's several mutations cost one write. See §5. |
| `lastIncludedIndex`, `lastIncludedTerm` | The snapshot floor. Persistent, not volatile — listed here because every accessor in the log seam reads them. `(0, 0)` is not a sentinel for "none": it is exactly `log[0]`'s index and term, so a node that has never compacted has its floor at the original sentinel. |
| `snapshotThreshold`, `snapshotNotify` | How many discardable entries trigger a compaction request, and the capacity-1 channel that carries it. See §10. |
| `pendingSnapshot` | An image received from a leader, or read from disk on restart, waiting for the applier to hand it up. |
| `mu` | Guards every field above except the apply channels. |

### On leaders only — reinitialised after every election

| Field | Meaning |
|---|---|
| `nextIndex[peer]` | Next log index to send. Optimistically initialised to leader's last index + 1. |
| `matchIndex[peer]` | Highest index known replicated on that peer. Initialised to 0. |
| `replicatingTerm[peer]` | Term of the AppendEntries round in flight to that peer, 0 when idle. A term rather than a bool, so a round left over from a deposed leadership cannot free a slot a newer term's round is holding. |
| `snapshotSentAt[peer]` | When an image was last *attempted* for that peer. Throttles rebuilds to one per heartbeat interval. See §10. |
| `replPending[peer]` | A replication trigger arrived while a round was out. This is what collapses a burst of client writes into one follow-up message. |
| `lastContact[peer]` | Send time of the most recent message that peer answered. The raw material for the read lease (§9). Empty on election, so a lease is never inherited from a term this node no longer holds. |

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
Not implemented: it shifts every index by one, and every path that reports an index to
a client would have to account for it. Recorded here so the absence is a decision
rather than an oversight.

What exists instead is the read barrier (§9), which appends a current-term entry on
demand. A read therefore releases the backlog as a side effect, and any caller that
needs a fresh leader to catch up can force it with one. That is why the read path and
the no-op are one decision rather than two.

---

## 8. The apply path

Committing an entry and applying it are different events, separated by a goroutine.

`commitTo` is the single funnel through which `commitIndex` ever moves — the leader
counting a majority, and the follower obeying `leaderCommit`. It refuses to move
backwards, refuses to move past the end of the local log, and signals the applier.
There is no way to advance `commitIndex` and forget to signal, which would otherwise be
a bug that fails no test and merely delays every apply until some unrelated commit
happens to wake the goroutine — the kind of fault that reads as "the system is a bit
slow sometimes" for a year. The invariant is greppable:

```
grep -rn "commitIndex = " internal/raft/*.go | grep -v '_test.go'
```

One hit, in `commitTo`. Two deliberate exceptions live in test code and are documented
in §11.

**One applier goroutine per node, and exactly one consumer of the apply channel.** The
channel itself is ordered, so a pool of consumers looks safe and is not: two receivers
take messages 4 and 5 in order and then apply them in whatever order the scheduler
picks. The channel guarantees *delivery* order; only a single consumer turns that into
*application* order.

**Applying inline, wherever `commitIndex` moves, was rejected twice over.** The reply
handler holds the lock, and sending on an unbuffered channel under it deadlocks the
moment the state machine calls back into Raft — which a read does, on every request.
And replies land on many goroutines, so two could be inside the apply path at once.
Both problems dissolve if exactly one goroutine sends and holds no lock while sending.

**The apply channel is unbuffered; the notify channel has capacity 1.** A buffer on the
apply channel would let Raft run ahead of a stalled state machine, and the first symptom
would be an out-of-memory kill rather than a slow apply. Unbuffered means back-pressure
reaches the applier immediately — and because the applier holds no lock while sending,
that back-pressure never reaches consensus: elections and replication run at full speed
with a completely dead state machine. The notify channel is a flag meaning "`commitIndex`
moved", not a queue of events, so a full buffer means a wake-up is already pending and
dropping the send loses nothing.
*Rejected:* a condition variable, which cannot be woken by a channel close and so needs
its own shutdown machinery; a channel lets the applier wait on work and on shutdown in
one `select`, and the stop channel already exists.

**Entries are copied out under the lock before delivery.** The instant the lock is
released, an incoming AppendEntries can truncate the log or reallocate its backing
array. Reading the log at send time would deliver an entry other than the one that was
committed.

**`lastApplied` advances after delivery, not after copying.** Figure 2's meaning is
applied, past tense. Two features read it that way — a linearizable read decides the
state machine is caught up, and a snapshot decides which entries may be discarded — so
the cheaper version is a lie that surfaces later.

**The applier clamps rather than panics.** If `commitIndex` ever exceeds the log, the
applier logs an error and clamps. Two invariants make that unreachable — `commitTo`
refuses to raise `commitIndex` past the last index, and receiver rule 3 only removes an
uncommitted tail — so reaching it means one of them is broken. A panic in a goroutine
nobody owns takes down the process instead of producing a bug report.

**Shutdown wins over delivery.** The applier's send selects on the stop channel as well.
Blocking for a consumer that may never arrive would make `Stop` hang, and a test suite
that cannot stop a node cannot run the next test. The entries remain committed and in
the log; a restarted node reapplies them.

### Believed-impossible conditions are guarded, not assumed

Four states in the implementation cannot occur if the rest of the implementation is
correct. All four are checked anyway, and all four log an error rather than failing
silently or crashing:

| Where | Condition | Response |
|---|---|---|
| `commitTo` | Commit past the end of the log | Refuse |
| `applier` | Committed index exceeds the log | Log and clamp |
| `mergeEntries` | Truncation at or below `commitIndex` | Log and proceed |
| `AppendEntries` | Message at own term while leading | Refuse |

The reasoning is the same in each case. A correct cluster never reaches the line, so the
check costs nothing; an incorrect one produces a searchable error naming the file that
is actually at fault, instead of a corrupted log or a process-killing panic in a
goroutine nobody can recover.

---

## 9. Linearizable reads

**A read is a barrier plus a wait.** `ReadIndex` appends a no-op entry in the current
term and returns its index. The caller reads its own state machine only after observing
that index applied **carrying the returned term** — the same claim ticket `Submit`
issues, used to detect that this node was deposed mid-read.

```
idx, term, isLeader := n.ReadIndex()
if !isLeader           -> redirect
wait for the state machine to reach idx
if term at idx != term -> deposed; start over
read local state
```

Raft returns an index and nothing else, because Raft does not hold the data. A read that
returned a value would mean the consensus layer re-deriving state the applier has
already established, and the two derivations would eventually disagree.

### Why reading local state is wrong

The tempting implementation is `if state == Leader { return data[key] }`. It is one
comparison, needs no network, and is wrong three separate ways.

**1. Leadership is not a local fact.** A leader partitioned into a minority does not
know it. It keeps `state == Leader` until something tells it otherwise, and nothing can:
the majority has elected a successor and is committing writes on the far side of a cut
it cannot observe. Its own log and commit index remain perfectly self-consistent. It
serves confident, stale answers for as long as the partition lasts.

No local check detects this, and the reason is worth stating plainly: **every local fact
is a statement about the past.** "I am in state Leader" means "I was leader when I last
heard from someone". "I received a heartbeat 12ms ago" means a majority agreed with me
12ms ago. Neither is evidence about now. Only a fresh majority round is, and committing
the barrier *is* that round — a deposed leader cannot get a majority to accept an entry
in its term, so the read never completes rather than completing wrongly.

This is the failure mode with no cheaper remedy, and the reason the whole mechanism
exists. The corresponding test asserts both halves: that the stale leader still reports
`Leader` and still holds the old value — so a local read would have been served, and
would have been wrong — and then that the barrier refuses to complete.

**2. A leader can be behind its own commit index.** `commitIndex` moves in a reply
handler; `lastApplied` moves when the applier hands the entry over. Between those two the
node holds a write it has already acknowledged and has not yet run. Reading the data
there omits a write the client was told succeeded. Waiting for the barrier to **apply**,
not merely commit, closes this.

**3. A fresh leader is missing writes its predecessor acknowledged.** §5.4.2 forbids
committing an inherited entry by counting replicas, so a new leader holds entries that
were committed by the leader before it and are not committed here yet — and its state
machine does not have them. The barrier fixes this as a side effect: it is a current-term
entry, so committing it commits the whole prefix beneath it by Log Matching.

**And on followers:** a follower's state machine trails the leader's by construction, not
by accident. It applies what `leaderCommit` told it, and `leaderCommit` arrives on the
message *after* the one that carried the entry. Write to the leader, get an
acknowledgement, read from a follower, see the old value.

**A read must not run on the apply goroutine.** Step two waits for the state machine to
reach an index, and the state machine only reaches indices because something is draining
the apply channel. If that is the same goroutine, it waits for a delivery it is itself
responsible for making; the applier parks on an unbuffered send and the stall is silent —
no error, no timeout, nothing in the log. The server layer therefore keeps one goroutine
that does nothing but apply, and serves reads from others. That is the same separation the
apply channel already requires for a different reason (exactly one consumer, so delivery
order becomes application order), which is why it costs nothing to honour.

### Lease reads

A leader that completed a round trip with a majority at time *T* knows every node in that
majority reset its election timer at or after *T*. None can campaign for a full election
timeout, an election needs a majority, and any two majorities intersect — so no other
leader can exist, and this node's state is authoritative without any network round trip
at all.

`ReadLease` grants that permission and returns the instant it expires. It is a pure
optimisation: every reason it can refuse is answered by the barrier, so the client
protocol is lease-first with the barrier as fallback.

**It removes one of the three objections above, not three.** The other two are structural
and are checked explicitly. The caller still waits for the state machine to reach the
returned index, because the applied index trails the commit index. And the lease is
refused outright until this leader has committed an entry in its own term — without that,
§5.4.2 means its commit index does not yet cover the entries its predecessor committed,
and its state machine is missing acknowledged writes. That second gate is the same
dependency the paper's cheaper `ReadIndex` has on the no-op at election; because Helios
has no such no-op, a write-idle cluster simply has no lease and every read pays for a
barrier.

**The lease dates from the send, not from the reply.** A follower resets its timer when it
*receives* a message, which is at or after the send, so its deadline is at or after
`sentAt + electionTimeoutMin`. Dating the contact from the send understates the lease by
one one-way latency, which is the only direction it is safe to be wrong in. It also means
a log *rejection* counts as contact: a failed consistency check resets the timer before
running the check, so a rejection proves exactly what the lease needs. The only rejection
that withholds the reset is the stale-term case, which deposes this node before the
question can be asked.

**The lease runs from the *k*th most recent contact.** One peer answering is not a
majority. With five nodes a leader needs two peers besides itself, so the lease dates from
the older of the two that make up the quorum.

**The safety assumption, stated plainly: clock rates are bounded.** Nothing compares
timestamps across machines, so absolute offset is irrelevant — what matters is that no two
nodes' clocks run at rates differing by more than the drift allowance. The lease is
discounted accordingly. If the leader's clock is slow by *d* a lease of *L* takes
*L*/(1−*d*) real time; if a follower's is fast by *d* a timeout of *E* takes *E*/(1+*d*).
Safety requires *L* < *E*(1−*d*)/(1+*d*), which at 10% gives a 123ms lease against a 150ms
floor.

**The assumption is not only about clocks.** A descheduled process — a stop-the-world
pause, a throttled container, a suspended VM — advances wall-clock time without advancing
its own progress, and is indistinguishable from a slow clock. The read protocol re-checks
the lease after waiting for the state machine, which closes the window between the check
and the read. It cannot close the window between that check and the value reaching the
client, and nothing local can. That residue is the cost, not a defect to be fixed.

**When not to use it.** Any caller that cannot accept a hardware assumption in its
correctness argument calls the barrier directly. The barrier's guarantee is structural — a
deposed leader *cannot* obtain a majority — and that is a different kind of claim from
*this machine's clock is behaving*.

### Cost, measured

Five nodes, fifty reads down each path, the race detector off. The link setting is charged
once per RPC rather than per direction.

| link | RPC | barrier p50 | barrier p95 | lease p50 | lease p95 |
|---|---|---|---|---|---|
| loopback | 0 | 1.13ms | 1.18ms | 2.8µs | 4.9µs |
| LAN | 5ms | 5.56ms | 5.66ms | 1.3µs | 3.6µs |
| cross-zone | 25ms | 25.78ms | 26.35ms | 4.8µs | 7.8µs |

Two things the shape shows.

**The lease line is flat.** Its cost does not track the link, which is the evidence that it
does no network work at all — the variation between the three rows is scheduling noise, not
latency. The comparison asserts this rather than leaving it to be read off a chart: a lease
p95 above one one-way delay fails the measurement.

**A barrier read is the RPC plus about a millisecond.** Subtracting the link leaves 1.13 /
0.56 / 0.78ms of local work, most of which is the 1ms polling granularity of the test's
wait rather than anything in the implementation.

That second observation settles a question the paper leaves open. The cheaper `ReadIndex`
variant skips the log write and confirms leadership with a heartbeat round instead — but it
still pays the round trip, so against these numbers it would save under a millisecond out of
25.8ms across zones. **The append is not what a barrier read costs; the RPC is.** The
argument for the no-op on election therefore rests on log growth and throughput, not on
latency: a read-heavy workload appending an entry per read grows the log at write rates.
That cost is real and is not measured here.

The measurement is reproducible with `-measure`, and writes a CSV and per-link histograms
alongside the election-time data.

---

## 10. Implementation decisions

Each decision paired with the alternative rejected and why.

### Log and entries

**Sentinel entry at `log[0]`.** Raft indexes from 1; Go slices index from 0. A dummy
entry (term 0, no command) makes slice position equal Raft index everywhere.
*Rejected:* an offset translated at every access — the translation leaks into every
handler and each site is an off-by-one waiting to happen. The sentinel also makes
`prevLogIndex = 0` work naturally for the first AppendEntries, and it is what
**guarantees log repair terminates**: a leader that has backed all the way off always
finds agreement at index 0.
*Cost:* truncation breaks position-equals-index. Revisit when snapshotting lands.

**`LogEntry` has no `Index` field.** Position in the slice is the index; storing it too
creates two sources of truth that can disagree — and they would, the first time a
follower truncates. *Revisit at:* snapshotting.

**`LogEntry.Command` is immutable once appended.** `Submit` copies the caller's bytes
in so the log owns them, and nothing writes to them afterwards. This is what lets
outgoing messages copy entries **shallowly** — the message gets its own view of *which*
entries are being sent, but shares the command bytes.
*Rejected:* deep-copying every command on every send, which costs an allocation and
memcpy of the whole outstanding log per peer per tick.
*Obligation:* consumers must treat `Command` as read-only. The pressure point is the
state machine — a handler that decoded into the slice it was handed would rewrite
committed history.

**`LogEntry` carries a `NoOp` flag — a documented departure from Figure 2.** Figure 2's
log holds commands; the paper's no-op is described without saying how a node recognises
one.
*Rejected:* a nil `Command` as the marker, which needs no new field and matches the
sentinel's existing convention — but gob collapses nil and empty, so an application
could never legitimately commit an empty command again. Reserving part of the client's
value space to encode an internal fact is invisible until someone hits it.
*Cost:* none on the wire. gob omits `false`, so a normal entry encodes exactly as
before. The barrier is a consensus-level concept — a leader must be able to append one
before it knows anything about the state machine above it — so the consensus layer owns
the representation.

**A barrier is delivered, not filtered.** It reaches the apply channel with
`CommandValid` false and no command. Skipping it inside the applier would advance
`lastApplied` while leaving every consumer's own index behind, and every read would then
wait for an index that never arrives.
*Obligation on consumers:* on `CommandValid` false, advance your index and apply nothing.
Treating it as an error — the natural first draft — makes every linearizable read look
like a corrupt log.

**One append path.** `Submit` and `ReadIndex` both go through a single internal function
that stamps the term under the same lock acquisition that appends.
*Rejected:* duplicating it, because the non-obvious line is the commit-rule evaluation
that a single-node cluster depends on — with no peers there is no reply handler to
trigger it, so the log would grow with the commit index at zero forever. A second copy
would omit it.

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
scheduling still varies. True replay needs a virtual clock.

**Vote requests fan out in parallel.** One goroutine per peer, replies on a buffered
channel. *Rejected:* a sequential loop — one unreachable peer would consume the entire
election window. Every reply is re-checked against current state and term before being
counted; counting a reply from an election the node has left is the standard route to
two leaders in one term.

**Peers reached through a `Transport` interface.** *Rejected:* a gRPC client inside
`Node`. The interface is what makes partition and message-loss testing possible at all.

### Replication

**One send path for heartbeats and replication.** There is no separate heartbeat
message; the fan-out sends each follower whatever it is missing, which is nothing
when that follower is caught up. Consequence: the consistency check runs during idle
periods, so a divergent follower is detected without waiting for a client write.

**The first fan-out is immediate, not one tick later.** A new leader that waited a full
heartbeat interval before asserting itself would hand every follower a head start on
timing out, at the moment leadership is least established.
*Consequence for tests:* a node is sending before the test body runs. See §11.

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
*Former limit, now closed:* one fan-out per `Submit` meant bursts produced redundant
messages. See the next entry.

**At most one AppendEntries round in flight per peer.** A trigger arriving while a round
is outstanding sets a flag instead of starting a second round; when the round returns it
builds one message covering everything that accumulated. The batching window is the
round trip itself, so there is nothing to tune and an idle cluster is unaffected.
*Why:* `Submit` returns as soon as the entry is appended and persisted, so a thousand
concurrent clients fill the log in the time one message takes to reach a follower and
come back. One fan-out per `Submit` meant a thousand messages, each rebuilding
`log[nextIndex:]` from a `nextIndex` that had barely moved — quadratic bytes for linear
progress.
*Measured*, three nodes, 1000 commands from 64 clients, 1–3ms link, race detector off:

| | commands/s | AppendEntries sent | entries shipped |
|---|---|---|---|
| one fan-out per `Submit` | 7,800 | 2,003 | 129,825 |
| one round per peer | 20,400 | 16 | 1,996 |

2.6× the throughput, 125× fewer messages, 65× fewer entries on the wire. 1,996 is the
floor — two followers × 1000 entries, each crossing once — so the remaining traffic is
irreducible.
*Consequence:* a peer that stops answering stops receiving until its send returns. That
is correct, since piling messages on an unresponsive follower helps nobody, but it makes
a deadline on `Transport.SendAppendEntries` a requirement rather than a nicety. The fake
network returns immediately when a peer is unreachable; gRPC must be configured to.
*Second consequence:* same-peer reordering is no longer produced by the system. See §11.
*Cost:* a lone write on an idle cluster may now wait out an in-flight round rather than
getting its own immediate fan-out — the same latency-for-throughput trade as group
commit. At low load the window is near zero, but p99 write latency under light load has
not been measured.

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

### Snapshots and compaction

**The log is a slice whose element 0 is the floor, and every index-to-position
translation lives in one file.** `log.go` owns `firstLogIndex`, `lastLogIndex`,
`entryAt`, `termAt`, `entriesFrom`, `truncateFrom` and `compactTo`; nothing else may
index the slice directly. A raw `n.log[i]` is silently wrong the moment the floor moves,
and wrong in the worst direction — a small positive offset still lands on a real entry
and returns a plausible term. The invariant is greppable:

```
grep -n 'n\.log\[' internal/raft/*.go | grep -v _test.go
```

Every hit must be inside `log.go`, with one documented exception: `adoptState` reads
`n.log[0].Term` to derive the floor's term, which is a position operation by definition
rather than an index translation. Appending and taking the length are position operations
too and are fine anywhere.

**`termAt` fails closed.** Out of range it returns −1, which no real term can equal, so a
consistency check run against a bug rejects and triggers repair rather than matching
something it should not. Returning 0 would be far worse: 0 is the original sentinel's
term, so a bug would read as agreement at the bottom of the log.

**`compactTo` rebuilds the slice rather than reslicing it.** `n.log = n.log[k:]` keeps the
whole original backing array alive behind the new header, so the memory the compaction
was taken to release stays held until the log next grows past its old capacity — which is
the entire point of doing it.

**The compaction trigger measures what compaction can remove, not what the log holds.**
`lastApplied - lastIncludedIndex`, never `len(log)`. A snapshot may only cover applied
entries, so a compaction removes exactly that many; measuring the whole log instead looks
equivalent and thrashes. If the unapplied tail is by itself longer than the threshold,
then after compacting to `lastApplied` the log is *still* over the line, the next applied
entry signals again, and the node takes one complete image per entry for as long as the
write load lasts. *Measured*, on ten thousand entries at a threshold of 200: 59 images
with the right metric, 9,453 with the wrong one, and 1.2 seconds against 69. Neither
correctness testing nor a short-log test can see this; only a long run can.

**`Snapshot`'s guard is `commitIndex`, not `lastApplied`.** What must never happen is an
image covering uncommitted entries — those can still be overwritten, and an image
containing one makes the overwrite unrecoverable. `lastApplied` cannot serve as that
guard because it lags the caller by a mutex acquisition: the applier hands a message to
the state machine and only *then* reacquires `mu` to record the delivery, so a machine
that snapshots the moment it applies is routinely one index ahead of Raft's own
bookkeeping. Refusing there rejects a correct caller on stale information, and every state
machine written against this API would hit it. The caller is authoritative about what it
applied — it could only have obtained the entry from this node — so `Snapshot` accepts and
drags `lastApplied` up to the floor. The applier's order check tolerates the equality that
results, and complains only about a gap or a step backwards.

**Building an image is throttled to one attempt per heartbeat per peer.**
`buildInstallSnapshot` reads the whole image back from storage and checksums it, under
`mu`. `replicateAll` runs on every client write and not just on the tick, so a follower
that is down while the cluster is under load would have a full image rebuilt for it once
per submitted command. The throttle is on the *attempt* rather than on success, because an
unreachable peer fails instantly and it is precisely that instant retry which makes the
storm.

**The leader keeps no copy of the image in memory.** `Snapshot` hands the bytes to the
storage layer and lets go; holding one would double the memory the compaction was taken
to release. The cost is a read and a decode on the send path, bounded by the throttle
above. If that stops being affordable the fix is a cache beside the floor, not a read
outside the lock — the floor can move between the read and the send.

### Recovery, measured

A restart is three phases, and only the first is bounded by the image. `commitIndex`
comes back at the floor, so the tail above it is loaded but **not** replayed until a
leader re-establishes it; the figures below simulate that first message.

Apple M3, APFS on the internal NVMe, Go 1.26.5. Warm page cache — the records are written
moments before they are read, so a genuinely cold restart adds a disk read of the same
size. Tail held at 2,000 entries except in the first row.

| Durable state | Take: encode | Take: save | Alloc to take | `OpenNode` | Alloc to open | Tail replay |
|---|---|---|---|---|---|---|
| no image, 200k-entry tail | — | — | — | 19 ms | 34 MB | 40 ms |
| 1 MB image | 0.16 ms | 8 ms | 2 MB | 0.5 ms | 2.4 MB | 1 ms |
| 100 MB image | 28 ms | 41 ms | 200 MB | 34 ms | 200 MB | 1 ms |
| 1 GB image | 497 ms | 524 ms | 2,048 MB | 491 ms | 2,048 MB | 1 ms |

**Time is not the constraint.** A gigabyte of state is back to a usable node in 491 ms,
about 2 GB/s through `OpenNode`. Handing the image to the state machine costs 1 ms at
every size, because it is a pointer through a channel — the machine's own rebuild is on
top of that and is entirely the application's. The log tail is cheap in both directions:
200,000 entries gob-decode in 19 ms and replay at roughly five million entries a second.

**Memory is.** Allocation is exactly twice the image on both paths, to the megabyte.
Taking one costs a payload buffer and an output buffer, and the caller's own copy is
still live while both exist — so snapshotting 1 GB of state needs about **3 GB
resident**. Recovering it needs 2 GB: the `ReadFile` buffer plus `decodeSnapshot`'s copy.

That is a consequence of `Storage.Save` taking a `[]byte`: the encode has to materialise
the whole blob before anything can be written. An `io.WriterTo`-shaped API would remove
two of the three copies on the write side, and handing ownership of the read buffer to
`decodeSnapshot` would remove one of the two on the read side — the buffer `os.ReadFile`
returns is fresh and nobody else holds it, so the copy is defending against a caller that
does not exist. Neither is urgent at the sizes measured. Both become urgent the moment a
node is sized to hold as much state as it has memory.

### Leader bookkeeping

**`matchIndex` is derived from what was sent.** `prevLogIndex + len(entries)`, never
the leader's last index at reply time. The log may have grown since the message left,
and crediting a follower with entries it never received is how a leader counts a
majority for an entry that exists on one machine.

**Reply handling is tied to the attempt that produced it.** `matchIndex` advances with
`max()`, so a late reply to an older, shorter message cannot un-prove agreement already
counted toward a commit. Backoff ignores any reply whose `prevLogIndex + 1` differs
from the current `nextIndex` — applying a superseded rejection would push `nextIndex`
back to where it started, a live-lock where repair keeps undoing itself.

**Fast-backup hint on the reply (deviation from Figure 2).** Figure 2's reply has two
fields; Helios adds `conflictIndex` and `conflictTerm` (§5.3 of the paper).

| Rejection | Follower reports | Leader does |
|---|---|---|
| Log too short | term 0, index = its last index + 1 | resume there |
| Term mismatch | the term it holds, and where that term's run begins | resume past its own last entry of that term, or discard the whole run if it has none |

*Safety rule:* the hint may only move `nextIndex` **backwards, or hold it at the
sentinel floor of 1**. Moving it forward would assume agreement never verified. The
computation clamps to `[1, current-1]` regardless of what the reply says, so a
malformed hint costs round trips rather than correctness. At `current == 1` the floor
and the clamp coincide and the value holds — unreachable from a correct follower, since
the consistency check at the sentinel cannot fail, and there is nowhere below 1 to go.

*Measured:* across the six Figure 7 scenarios, 25 round trips → 12. Cases (a), (c) and
(d) are unchanged — they diverged by at most one index, so there was nothing to skip.
The entire benefit is in the badly diverged cases, which is why the paper treats it as
optional.

---

## 11. Testing conventions

**Scope fences.** A test may assert that a feature is *not yet* built, so that the work
implementing it gets a red build instead of silent scope creep. Fence comments are
marked so they are greppable, and their failure messages name the change that retires
them:

```
grep -rn "SCOPE FENCE" internal/raft/*_test.go
```

Run it before starting new work. Convert rather than delete when the test still covers
something.

**Property checkers are stated over any set of logs, not one scenario**, because every
later change has a reason to re-assert them: that snapshotting preserves them, that a
restart from disk does, that a membership change does.

**Safety is monitored over runs, not asserted at moments.** A commit ledger accumulates
every entry any node has committed and re-checks the set after each step. The property
is §5.4.3: once an entry is committed at an index, no node may hold a different entry
at that index **within its own committed prefix**. The qualifier is load-bearing —
nodes routinely hold divergent uncommitted entries, and Figure 8 turns on one surviving
on S5 for two terms.

The ledger watches the commit index rather than the applied index, and that is a choice
rather than a limitation. The commit index is the *decision* that releases an entry, it
moves on every node including followers with no consumer attached, and watching it
catches a violation one step earlier than the state machine could.

**Safety tests are paired with a mutation that must break them.** A passing safety test
is worth nothing until it has been seen to fail for the right reason. The Figure 8
scenario is replayed with a test-local commit rule that omits the current-term check and
requires the monitor to catch it. The mutation runs the real rule first and the unsafe
one after — since the unsafe rule can only advance the commit index further, the result
is exactly what a build without the check would produce, with no production code touched
and no test-only knob added.
**A green result there is a failure to investigate, not a success.**

**Elections in scenario tests are real.** The Figure 8 narrative runs through the actual
RequestVote and up-to-date check, because S5's ability to overwrite a
majority-replicated entry depends on the election restriction *permitting* it. Deciding
elections by hand would assume away the mechanism under test.

**Timer assertions.** "Was reset" needs a property test — force the deadline to expire,
then assert remaining time is positive and within bounds. Never compare two randomly
drawn deadlines. "Was **not** reset" may use equality, since no second draw happened.

### Fixtures must be inert, or their output must be marked

A node built with an agreeable transport starts making progress the moment it becomes
leader: the fan-out is immediate, and it happens with `nextIndex` reflecting a log that
is still just the sentinel. Three separate bugs came from this — a truncation test that
committed behind itself, a family of commit tests that passed only by winning a very
short race, and a fan-out test that inspected messages built before its own setup ran.

Two remedies, depending on whether the test needs the loop at all:

- **Decision logic** installs leader state directly and starts nothing. A guard test
  asserts the fixture is still inert after several heartbeat intervals of doing nothing.
- **Tests that need real sending** drain the initial round and count from a mark.
  Counting messages from zero is always wrong.

**Every node must be stopped.** Constructing a node starts its applier goroutine, and one
that reaches leadership also starts a replication loop that keeps calling its transport.
Neither fails anything, and together they were roughly ninety leaked goroutines per suite
run. Nodes are built through a helper that registers teardown; `Stop` is idempotent, so
adding it alongside an existing `defer` costs nothing.

### Fault injection

Faults are injected at the network, not the node. The switchboard models four:

| Fault | What it models |
|---|---|
| Dropped request | Rolled before the handler runs. The receiver never saw anything. |
| Dropped reply | Rolled after the handler runs. **The receiver acted and the sender does not know.** |
| Reachability cuts | Partitions, in either or both directions. |
| Per-message random delay | Latency, and — as a consequence — reordering. |

The dropped reply is the one that earns its keep: `nextIndex` stays put and the next
tick resends entries the follower already holds, which is the only exercise the merge
path gets against a duplicate append. A receiver that truncated on entries matching its
own log would pass everything else in the suite.

**Reordering is no longer emergent, and is no longer asserted.** It used to be: one
goroutine per message plus one random delay per goroutine meant a message overtook an
earlier one to the same peer whenever it drew a shorter delay than the gap between their
sends. The network stamped send order, recorded arrival order, and tests failed if the
count came back zero. Coalescing ended it — a second message to a peer is not built
until the first has returned — so inversions now require a round from a deposed term to
still be outstanding when the new term starts one. The counter is still recorded and
reported; it is no longer a pass condition.

The guards it stood in for — the monotonic `matchIndex`, and backoff ignoring a reply to
a superseded attempt — are tested directly instead, by handing the stale reply to the
guard rather than racing it in. That is deterministic, and it checks what the guard
*does* rather than merely that inversions happened somewhere in a run that also passed.

**`kill` and `crash` are different faults and both are needed.** `kill` stops a node's
goroutines and cuts its network but leaves its memory intact — an unreachable node,
which is what a failover test wants. `crash` models power loss: the network is cut
first, *then* writes stop reaching the medium, *then* the goroutines stop. The order is
the whole design. Reversed, the node would persist nothing, reply success, and the
leader would count a follower that is about to forget — a lost committed entry
manufactured by the harness rather than found in the code. Only a `crash` can be
followed by a `restart`, which builds a new node over the storage the old one left.

**Chaos, quiesce, assert.** Raft does not promise progress on a lossy network, so no test
asserts it. Tests run under fault injection, repair the network, and then check
convergence. Election Safety is the exception and is watched for the whole run, because
it must hold at every instant — a cluster that briefly had two leaders and then settled
would pass every end-state check.

**A leader change needs a current-term write before anything commits.** Required
boilerplate for any test that changes leaders, for the reason in §7. A read barrier is
the honest way to issue one.

### Working with goroutines in tests

**`t.Fatalf` only from the test goroutine.** Elsewhere it calls `runtime.Goexit` on the
wrong stack: the test body keeps running and reports something unrelated. Background
goroutines record a fault string that the test body reports after joining them. Nothing
may call `t.Log` after the test completes, which is why state machine goroutines never
touch `t` and the log writer has a closed flag.

**Killing a node means stopping it *and* cutting the network.** A stopped node still
answers RPCs, because the fake network calls handlers directly on the struct. Healing a
partition restores every pair and therefore undoes the second half, quietly returning a
dead node to the voting population — heal helpers re-cut them.

**The suite is run shuffled.** Test order within a package is otherwise fixed, and two
of the three fixture bugs above were order- and timing-sensitive. `-shuffle=on` found
the third on its first invocation.

**Test files ship.** Go excludes them from the binary, so they add nothing. The cost is
CI time, addressed by tiering with `testing.Short()` rather than by deletion. For a
consensus implementation the tests are the only available evidence of correctness.

### Documented exceptions to the commit-index funnel

Two test files assign the commit index directly rather than through `commitTo`, and both
are deliberate:

- The Figure 8 mutation, which bypasses the funnel on purpose so the safety monitor can
  be shown to fire.
- A truncation test that rolls the commit index *backwards*, which `commitTo` correctly
  refuses.

The invariant grep in §8 excludes test files for this reason.

---

## 12. Open questions

Deliberately unresolved. Each is answered by later work.

- **Election timer reset on stepping down.** A node resets its timer whenever it adopts a
  newer term, including when it then refuses the vote on the up-to-date check. Figure 2
  lists only two reset events, so this is a third.
  *Alternative rejected:* not resetting — a leader's deadline is never refreshed while it
  leads, so a leader stepping down would hold a deadline far in the past and immediately
  campaign against whoever deposed it, disrupting every failover.
  *The cost is real:* a node partitioned alone inflates its term by campaigning, and on
  rejoining forces the whole cluster to step down and reset despite a log too stale to
  win. **Pre-vote** is the proper fix: a candidate first asks whether it *could* win,
  without incrementing anyone's term. Current behaviour is pinned by a test that should be
  inverted when pre-vote lands.
- **No-op entry on election.** Would release inherited entries immediately (§7) and would
  let a leader serve lease reads from the moment it wins rather than from its first client
  write. Measurement has weakened the other argument for it: the heartbeat-only variant it
  enables is barely cheaper than the barrier in latency terms (§9), because the cost is the
  round trip and not the append. Still deferred because it shifts every index by one and
  every path that reports an index to a client must account for it. Decide before the client
  session layer, not during.
- **Read throughput, as distinct from read latency.** Latency is measured and answered by
  the lease (§9). What is not measured is that every *barrier* read appends an entry, so a
  read-heavy workload on a cluster with no usable lease grows the log at write rates. This
  is now the strongest argument for the no-op on election, and it needs a number before it
  is an argument at all.
- **Duplicate commands.** A client that retries after a leader change can land the same
  command twice, since the retry is issued without knowing whether the original committed.
  Both copies apply — invisible for an idempotent write, wrong for anything else. Closed by
  client identifiers plus a dedup table, which is the same decision as making log replay on
  restart idempotent. Applied-count assertions currently use `>=` and carry a scope fence.
- **`persistIfDirty` runs under `n.mu`.** So a node has at most one write outstanding no
  matter how many clients are calling `Submit`, and group commit has nothing to
  coalesce: the batched storage measures 1.00 flushes per write on the real write path,
  against 0.04 at the storage layer with 64 concurrent writers. That gap — 147 against
  3,679 writes/s — is what the critical section costs, and it is the largest single
  number in `fsync-policy.md`. Moving the write out is the highest-value
  performance work left in the storage path. It is blocked on one question: coalescing
  drops superseded records, which is sound only because the records form a total order
  over states the node actually occupied. Released from the lock, a record that shortens
  the log no longer subsumes the promise made by the record that lengthened it. Settle
  that before wiring the batcher in — it is written and tested, and would be actively
  wrong today. **§14.10 confirms this cost at real, full-system scale**: it is one of two
  stacked, lock-held fsyncs on every applied write (the other is the storage engine's
  own WAL fsync, `wal.SyncAlways`), and the two together are what made this project's
  own million-key test take far longer than originally estimated.
- **A write's two lock-held fsyncs (Raft's own `persistIfDirty` and the storage
  engine's own WAL) may not both be load-bearing, and fixing them together could be
  more valuable than fixing either alone.** §14.10's own argument, connected
  explicitly to §14.9's: if restart is fixed to durably track which Raft index the
  storage engine's recovered state already reflects, Raft's persisted log becomes
  arguably *already* sufficient durability for crash recovery, which would make the
  storage engine's own per-write fsync redundant for that purpose specifically (its
  role in staging an efficient, flushable memtable would remain). Neither this nor
  the `persistIfDirty` batching question above was attempted under the time pressure
  of the task that found the connection between them.
- **Whole-log rewrite on every persist.** Each write re-encodes the entire log.
  Measured, this is not the bottleneck: extrapolating from an unflushed 7,862-byte
  record at 0.284ms, the record would have to reach roughly 190 KB — order six thousand
  entries — before it cost as much as one flush. Compaction is needed for restart time
  and memory, not for write throughput. Revisit with snapshots.
- **A pipelining transport brings reordering back.** Coalescing removed same-peer
  inversions from the wire, so nothing exercises the out-of-order reply guards end to
  end any more. If the gRPC transport pipelines, the hazard returns to production with
  only unit coverage behind it.
- **Chunking `InstallSnapshot`.** The image is sent whole, so it must fit in one RPC.
  Figure 13's `offset`/`done` exist to bound what a receiver buffers, and chunking would
  add a reassembly state machine with its own partial-transfer and
  leader-change-mid-transfer cases. The recovery measurement (§10) gives the shape of the
  limit: a gigabyte moves through `OpenNode` in half a second, so latency is not the
  reason to chunk — the receiver holding two copies of it is.
- **Snapshot encode and decode each allocate a second copy of the image.** Measured at
  exactly 2× on both paths, so taking a snapshot of 1 GB of state costs about 3 GB
  resident and recovering it costs 2 GB (§10). `Storage.Save` taking a `[]byte` is what
  forces the encode to materialise the whole blob; an `io.WriterTo` shape would remove two
  of the three. `decodeSnapshot`'s copy defends against a caller reusing its read buffer,
  which `os.ReadFile` never does. Not urgent at the sizes measured, and the first thing to
  fix if a node is ever sized to hold as much state as it has memory.
- **A snapshot arriving while entries are in flight.** The leader decides to send an image
  and, separately, has AppendEntries already on the wire. The receiver's ordering rules
  cover each message individually, but the interleaving is only argued, not tested — the
  ten-thousand-entry run happens to deliver exactly one image to an idle follower. This is
  the next task.
- **`kvMachine` ignores `SnapshotValid`.** The fixture used by every cluster test handles
  commands and barriers but not images, so it would silently ignore one rather than
  complaining. Unreachable today because those tests never compact, but it is a trap armed
  for whoever raises a threshold. Give it a `fault` line.
- **Membership changes.**- **Membership changes.** Single-server add and remove. Note that the consistency check is
  only correct because of one-leader-per-term, which is exactly what joint consensus exists
  to preserve.
- **Log Matching checkers ignore the no-op flag.** Two entries differing only in that flag
  compare equal, so the property checkers would not notice a barrier substituted for a
  command at the same index and term. Unreachable today, since nothing produces that state;
  fix it when the no-op on election lands and barriers become common.
- **Test suite runtime.** Roughly four minutes under `-race` and still growing — 232s at
  v1.6, 215s at v1.5, 155s at v1.4. Persistence added a write to every
  cluster test, and the crash-recovery rounds and the two measurement tests added the
  rest. It is still dominated by tests that sleep in real multiples of the election
  timeout. A virtual clock is the fix, and this remains the open question with the most
  evidence behind it: the suite is now long enough that the temptation to skip
  `-race -shuffle=on` on a change one feels sure about is a real risk rather than a
  hypothetical one. Worth noting the counter-example — the ten-thousand-entry snapshot
  test runs in 1.2 seconds, because it is bounded by work rather than by sleeping.
- **Timing constants are scattered.** Each test file defines its own bounds. Consolidating
  them would make the suite's time budget visible in one place; worth doing alongside the
  virtual clock.
- **The storage engine's `SyncBatch` policy has no driver yet.** `wal.WAL.Sync` is the
  primitive a ticker or write-batcher would call to coalesce many concurrent appenders
  behind one fsync (§13.1), on the same principle `persistIfDirty` already applies to
  Raft's own persistent state — but nothing in the engine calls it on a schedule yet.
  Wiring one is memtable work, not WAL work, so it waits on the memtable existing.
- **SSTable block, index, and footer are designed but not built.** ~~§13.2 fixes the byte
  layout; the block writer, index builder, and reader that turn a flushed memtable into
  a file on disk are the next task.~~ **Closed in v1.9** — see §13.2.
- **Memtable flush trigger is not implemented.** ~~`Len()` reports a distinct-key count,
  not an approximate byte size, and nothing yet decides when a memtable is full and
  should be switched out and flushed. The Raft log's own compaction trigger (§10)
  already argues for measuring the right quantity rather than a proxy for it; the same
  argument applies here; a size estimate, not an entry count, is almost certainly the
  right trigger, and is not yet measured.~~ **Closed in v1.9** — see §13.3.
- **No multi-memtable or multi-SSTable read path yet.** ~~`FlushIfFull` (§13.3) blocks the
  caller for the duration of one flush and does not swap a fresh memtable into the write
  path first, so as written a flush would stall writes rather than run underneath them.
  §13.4's read-path sketch ("checks the active memtable first, then any memtables
  mid-flush, then SSTables oldest-to-newest") describes the target shape; nothing yet
  merges more than one source, and nothing yet decides which SSTable, if several exist,
  is newest. This is the next task the flush trigger sets up.~~ **Closed in v1.11** — see
  §13.6, with one correction: the target shape was written down backwards. SSTables must
  be checked newest-to-oldest, not oldest-to-newest, or a newer tombstone or overwrite
  would never shadow an older value sitting in a "more recent" SSTable read first. §13.4
  and this open question both said it wrong; both are fixed as of v1.11.
- **The flush trigger still does not swap in a fresh memtable or run in the background.**
  ~~v1.11 closed the READ side of having more than one memtable and SSTable to search —
  given an already-assembled, already-ordered list of sources, `engine.Reader` merges
  them correctly. v1.12 added `engine.Writer`, so a live `Put`/`Delete` now durably
  reaches a single active memtable through the WAL. What still doesn't exist is the
  orchestration connecting the two: nothing calls `FlushIfFull`, decides a memtable is
  "full," swaps a fresh `Writer` target into place, and hands the frozen memtable to a
  background flush while keeping it visible to `Reader` as an immutable memtable in the
  meantime. `Reader` and `Writer` are both built to be handed that state once it exists;
  nothing yet produces it, and nothing yet unifies `Reader` and `Writer` into one type a
  caller would actually hold.~~ **Closed in v1.19** — see §14.2's
  `kvstore.Machine.freezeAndFlushLocked`, with one part still open: the swap runs
  synchronously in the apply path, not in the background the way `compaction.Background`
  itself runs (§13.9). A background flush goroutine remains its own, separate open
  question, listed below.
- **Flushing runs synchronously in the apply path, not on a background goroutine.**
  §14.2's `freezeAndFlushLocked` closes the swap-the-memtable question, but while a
  flush is in progress no further command can apply, and a concurrent `Get` blocks
  behind the same mutex. Mirroring `compaction.Background`'s own shape (§13.9) for
  flushing specifically is real, deferred work — attempting it in the same task that
  closed the swap question itself would have been two large changes at once.
- **HIGH PRIORITY — restart reapplies the entire committed history a second time,
  rather than only whatever wasn't yet durable.** Found in §14.9, not assumed: a
  five-thousand-key sanity run showed on-disk size roughly doubling across a restart
  in which nothing new was written. `lastApplied` is volatile Raft state and resets
  to zero on every restart; nothing currently tells a freshly-reopened `raft.Node`
  "the state machine already durably reflects everything up to index N," so
  `ApplyCh` redelivers the full history on top of what `compaction.Recover` and
  `RecoverMemtable` already reconstructed. Not a correctness bug — Put and Delete
  are idempotent under replay in the same order, checked directly at both the small
  and (pending) one-million-key scale — but restart cost currently scales with total
  applied history rather than with the unflushed tail since the last checkpoint,
  which is what it should scale with. The fix needs the storage engine to durably
  track which Raft index its own recovered state corresponds to, without adding a
  second fsync to the write path that already pays for one — not a small change, and
  deliberately not attempted under the time pressure of the task that found it.
- **A read reloads the manifest on every call rather than reacting to a change
  notification.** §14.3's `orderedSSTReadersLocked` has to reload because
  `compaction.Background` can change the manifest at any moment, independent of
  `Machine`'s own apply loop, and there is no cheaper moment to notice than checking on
  every read. Unmeasured against a real read-heavy workload.
- **Snapshots are logical (the full live key set, re-encoded), not physical (a
  reference to the SSTable files that already hold the data).** §14.4 argues this
  explicitly: a physical snapshot, the way RocksDB's own checkpoint mechanism works,
  needs `InstallSnapshot`'s RPC to carry file references or chunked bytes rather than
  one opaque blob — which needs the *next* open question on this list solved first.
- **SSTable file naming and manifest are not designed.** ~~`sstable.Write` and
  `FlushIfFull` both take a caller-supplied path and do nothing to track which SSTables
  exist, in what order, or which have been superseded by compaction. `Info.Path`/`Info.Bytes`
  give a caller enough to build that bookkeeping, but nothing here does yet — needed
  before more than one SSTable can coexist meaningfully.~~ **Closed in v1.13** — see
  §13.8's `manifest` package. File naming is a plain incrementing sequence number
  (`nextSequence`, derived from the manifest's own contents rather than a separate
  counter) — deliberately not a richer scheme (no key-range or level encoded in the
  name itself), since the manifest, not the filename, is the source of truth for which
  level a file belongs to.
- **Nothing yet decides when to run a compaction, or how often.** ~~§13.8's `Run`
  performs one complete pick-merge-write-swap cycle and returns; nothing calls it on a
  schedule, after a flush, or in the background, the same still-open gap `FlushIfFull`
  has always had (previous bullet). A single `Run` call also does not cascade —
  compacting L0 into L1 can itself push L1 over its own threshold, and nothing
  currently notices and re-triggers a second compaction in response.~~ **Closed in
  v1.14** — see §13.9's `compaction.Background`, with one correction found while
  building it: the cascading concern above does not actually arise under the current
  design. `CompactLevel` always replaces the level below with exactly one file, so
  every level above L0 can only ever hold zero or one files, and a level holding at
  most one file can never exceed any threshold of 1 or more — genuine cascading is
  impossible with a sane configuration, not merely untested. What *is* still open is
  `Background`'s connection to a live flush: nothing yet adds a newly flushed SSTable
  to the manifest's L0 automatically ("The flush trigger still does not swap in a
  fresh memtable or run in the background," above, still open), so `Background` has
  nothing to react to until something else populates L0 for it to notice.
- **Compaction merges a whole level with the whole level below it, not just the
  overlapping key range.** True leveled compaction (LevelDB, RocksDB) only merges the
  files at level L+1 whose key ranges overlap the specific L files being compacted,
  bounding how much of L+1 gets rewritten per compaction. §13.8's `CompactLevel`
  merges everything in both levels every time — simpler to get correct, and correct
  regardless of key distribution, but write-amplifies more than necessary once a level
  holds more data than a single compaction needs to touch.
- **Compaction triggers on file count, not byte size**, and produces exactly one
  output file per run rather than several size-bounded ones. Both are argued in
  §13.8 as deliberate, correctness-first simplifications; both are natural next steps
  once `targetBlockSize`'s own still-open measurement question (below) has an answer to
  build on.
- **Space amplification, measured at full convergence, cannot distinguish compaction
  configurations from each other under the current whole-level-merge design.**
  §13.11 found this directly: every `MaxFilesPerLevel` setting converges to the same
  fully-deduplicated steady state, since `CompactLevel` always performs a full merge
  rather than a partial, overlapping-range one. The moment partial-range merging
  (already an open question, above) is built, this stops being true — a partial merge
  can leave stale data behind in ranges it didn't touch, and final space amplification
  would start actually depending on how aggressively a configuration compacts. Peak
  space amplification (measured during the run, not after) is the number that
  differentiates configurations under the current design, and is what §13.11's chart
  plots for exactly this reason.
- **`Options.MaxFilesPerLevel: 0` is caught at runtime, not rejected upfront.**
  `Background`'s `maxDrainCycles` safety cap (§13.9) turns the degenerate
  zero-threshold case into a bounded, reported error after 64 wasted compaction
  cycles, rather than `StartBackground` or `Run` validating the configuration and
  refusing it immediately. Cheaper to build and still safe, but a caller finds out
  about the mistake late and somewhat obliquely (an error message) instead of
  immediately and clearly (a rejected argument). **This nearly bit a second time**:
  §13.11's own amplification measurement first tried draining at `maxFilesPerLevel: 0`
  in a loop to force full convergence, reproducing the identical cascade-forever
  hazard in a different function (`drainAndTally`, which has no cap of its own) before
  it was caught and replaced with a narrower, bounded fix. One runtime guard in one
  place did not prevent the same mistake from being written twice — a real argument
  for validating this upfront, centrally, rather than trusting every future drain loop
  to remember the hazard on its own.
- **Orphaned files from a crash between the manifest swap and cleanup are not detected
  or removed on a later startup.** ~~§13.8's `Run` argues this window is safe (the
  manifest is already correct, the leftover files are just wasted disk space), but
  nothing currently scans a directory for files the manifest doesn't reference and
  removes them — a real, if low-severity, gap.~~ **Closed in v1.15** — see §13.10's
  `compaction.Recover`, which also validates every referenced file actually opens as
  a well-formed SSTable, catching corruption at startup rather than at the first
  unlucky `Get`. Still not automatic: `Recover` must be called once, explicitly,
  before `Run` or `Background` ever starts against the same directory — nothing
  enforces that ordering beyond the doc comment saying so.
- **SSTable block reads scan linearly rather than binary search.** §13.2's original
  design already anticipated this ("once an index exists, a lookup inside a block can
  binary search rather than scan"); the reader implemented in v1.9 scans a block's
  entries in order instead, bounded by `targetBlockSize` but not yet optimised. Revisit
  once block reads show up in a measurement, the same way the WAL's sync policy was
  measured before being tuned rather than guessed at up front.
- **`targetBlockSize` (4KB) is asserted, not measured.** Chosen as a conservative,
  page-sized default with no workload behind the number, unlike the compaction trigger
  and the fsync policy, which were both picked after measuring. The right value trades
  off index size (smaller blocks, bigger index) against read amplification (bigger
  blocks, more wasted bytes per point lookup); nothing here has measured either side yet.
- **Bloom filter is not wired into the SSTable write or read paths.** §13.5 implements
  and measures the filter itself; `Write` does not build one per file, `Open` does not
  load one, and `Get` does not consult one before its block read. This is the whole
  reason the structure exists — skipping a disk read for a key that turns out to be
  absent — and none of that payoff is realized yet.
- **No filter serialization format.** A Bloom filter built at flush time has to survive
  being written to and read back from disk to be useful across a process restart, the
  same way the index and footer do (§13.2). Nothing here has designed where a filter's
  bytes would live in the file layout (a fourth section before the footer, most likely)
  or how a reader would know its size without a length prefix somewhere. Blocked on the
  wiring question above — no point designing a format for a filter nothing yet builds.
- **`bitsPerKey` is not chosen for this engine, only measured in the abstract.** §13.5's
  table shows three settings' actual behavior but does not pick one as Helios's default,
  the way the fsync policy (`fsync-policy.md`) and `targetBlockSize` (§13.2) still
  haven't been picked from measurement either. Revisit once real workload numbers exist
  to trade read-amplification savings against the per-key memory cost.
- **Block cache size is not chosen for this engine either, for the identical reason.**
  §13.12's three sizes (10%, 50%, 150% of one test file's cacheable bytes) show the
  shape of the trade-off, not a recommendation — a real deployment's cache should be
  sized against its own working set and available memory, neither of which this
  measurement has any way to know. The same still-open list this bullet joins
  (`bitsPerKey`, `targetBlockSize`, the fsync policy) is exactly the list of "measured,
  not yet picked" defaults a real workload would eventually resolve together.
- **`blockcache.LRU` holds one mutex for its entire Get or Put, not a sharded or
  lock-free design.** Correct and simple, but every concurrent caller serializes
  through the same lock regardless of which keys they're touching — a real bottleneck
  only under heavy concurrent access this engine hasn't been measured at yet.
  Deliberately not solved here, on the same "correctness first" priority §13.2's
  original linear block scan took over an unbuilt binary search.
- **The SSTable footer has no format-version field.** §13.13's block-level
  compression is a breaking change to the on-disk block layout, made safely only
  because nothing in this actively-developed project depends on an old file
  surviving a format change — every test rebuilds its own files fresh. A real
  deployment migrating live data across a format boundary would need `Open` to tell
  old-format and new-format files apart before it can even find the footer's own
  fields reliably, which needs a version marker this footer has never carried.
  Deferred until this project actually needs to open a file across a format
  boundary, which it has not yet had to.
- **Flate's compression level is asserted at the standard library's default, not
  measured or tuned.** §13.13 picks `flate.DefaultCompression` and stops there — the
  same "asserted, not yet chosen from a real workload" status `targetBlockSize`,
  `bitsPerKey`, and block cache size are already recorded under. A faster level would
  trade some ratio for lower CPU cost on both the write and (more importantly, since
  it happens far more often) the read path; nothing here has measured where that
  trade is worth making yet.
- **The skip list has no `Seek`.** `NewIterator` only walks from the beginning. A range
  scan from an arbitrary start key — which an SSTable read path merging several sources
  will eventually want — needs a seek that reuses `search`'s predecessor-finding walk
  rather than a full scan from the head. Deferred until something needs it.
- **No custom key comparator.** Keys are compared with `bytes.Compare` throughout, which
  is exactly right for the byte-string keys this engine has today. A comparator seam
  would only earn its complexity if a future key encoding (composite keys, integer keys
  needing numeric rather than lexicographic order) needed it, and nothing does yet.

---

## 13. On-disk formats (storage engine)

The state machine that sits above the apply channel (§2) is going to be an LSM engine
with its own write-ahead log and its own SSTables, not a consumer of Raft's `Storage`
interface (§5). The two are separate durability islands answering different questions.
Raft's persistent state exists so that an agreement, once reached, is never
re-litigated — it is what keeps `currentTerm` and `votedFor` from moving backwards.
The engine's write-ahead log exists so that a command the state machine has already
accepted is not forgotten by the process that accepted it, before that command has
made it into an SSTable. Committing an entry answers "did the cluster agree to this;"
the engine's WAL answers "did the layer above consensus actually write this down."
Treating the first as sufficient evidence of the second — for instance, deciding a
crashed and rebuilt state machine is fine because Raft's log still has the entry — would
be the same mistake §2 warns against from the other direction: the state machine
re-deriving a guarantee that belongs to a layer it must not read.

### 13.1 Write-ahead log record — designed and implemented

```
+-----------+-------------+-----------+------------------------+
| CRC32(4B) | Length(4B)  | Type(1B)  | Payload (Length bytes) |
+-----------+-------------+-----------+------------------------+
```

`CRC32` covers `Type` and `Payload` only — never `Length`, and never itself. A reader
does not need `Length` to be independently verified: a corrupted `Length` either reads
too few or too many payload bytes, and either way the bytes that land in `Type`+`Payload`
will not match the checksum. Covering `Length` as well would catch nothing that covering
`Type`+`Payload` doesn't already catch, at the cost of a second checksum pass.

`Length` is the payload length alone, not payload-plus-type, so a reader sizes its
allocation before it has even looked at `Type`.

Payload for a **Put**:

```
+-------------+------------+---------------+--------------+
| KeyLen(4B)  | Key(...)   | ValueLen(4B)  | Value(...)   |
+-------------+------------+---------------+--------------+
```

Payload for a **Delete**:

```
+-------------+------------+
| KeyLen(4B)  | Key(...)   |
+-------------+------------+
```

Both lengths are explicit rather than relying on a separator byte, on the same
reasoning as the persistent-state blob in §5: a key or value is opaque as far as the
WAL is concerned, and scanning it for a delimiter would break the moment a legitimate
key or value happened to contain that byte.

**A DEVIATION FROM LEVELDB'S BLOCK-FRAGMENTED WAL.** LevelDB frames records inside
fixed 32KB blocks, splitting a record that crosses a block boundary into
FIRST/MIDDLE/LAST fragments so a reader can bound how much it buffers per block and
skip a corrupt block without losing the whole file. Helios's WAL records are single
key/value pairs, not the multi-hundred-KB blocks that scheme exists to bound, and
Replay already stops rather than skips past a bad record (see below) — so
fragmentation would add a reassembly state machine, with its own partial-record and
mid-write-crash cases, to buy nothing at the sizes this system currently produces.
Revisit if a record ever needs to hold something too large to buffer comfortably in
one piece; nothing here bounds a single record's size today.

**Replay stops at the first torn or corrupt record, on purpose.** A WAL is written by
exactly one appender in strictly increasing order, so once a record fails to validate —
whether the file simply ends mid-record (a crash between two writes) or a record-sized
span of bytes fails its CRC (corruption within the file) — everything after that point
is either unwritten or the residue of an interrupted write. Nothing past it can be
trusted regardless of whether it happens to decode cleanly, so `Replay` treats both
cases identically: stop, and report how much of the file was valid. Distinguishing
"torn" from "corrupt" is a diagnostic question, not a recovery one, and is left to an
operator inspecting the file past the returned offset rather than encoded into the
replay path itself.

**Sync policy is the same fork `fsync-policy.md` documents for Raft's persistent
state, one layer up.** `SyncAlways` fsyncs every record and is the correct default for
anything the caller intends to keep; `SyncNever` flushes to the OS buffer but never
fsyncs, for tests and throughput measurement only; `SyncBatch` flushes on every append
and leaves the fsync to an explicit `Sync` call, so a driver external to this package
can coalesce many concurrent writers behind one flush the way `persistIfDirty`
coalesces Raft's own writes. This package provides `Sync` as the primitive; wiring a
ticker or write-batcher that calls it on a schedule is separate work, tracked in §12.

**Implemented** at `internal/storage/wal/`. Round-trip, corrupt-record, and torn-tail
recovery are exercised directly; the three sync policies are asserted to produce
byte-identical replayed data, since policy governs only *when* an fsync happens, never
what gets written or read back.

**Startup calls `Recover`, not `Replay` directly, and `Recover` truncates.** `Replay`
by itself is read-only and leaves a torn or corrupt tail physically in place — correct
for a pass that only wants to read, since the bad bytes are inert to a reader that stops
before them. A boot sequence is not read-only, though: it replays and then resumes
writing, and resuming into a file that still has the bad tail sitting in the middle
creates a standing hazard rather than a one-time one. New records would land *after*
the old corruption, so the file would read good records, then the bad tail, then the
new records. The next restart's `Replay` stops at the same old corruption it stopped at
the time before — nothing about those bytes changed — and never reaches anything
written since. Every record appended after a recovery that did not truncate is lost on
every restart from then on, silently, because the failure looks identical to a clean
recovery until a second restart happens to be examined closely.

`Recover` closes this by truncating the file to the valid prefix before handing back an
open `WAL`, so a resumed writer's new tail begins exactly where the last good record
ended and nothing appended afterward can ever be shadowed by corruption that record
already argued past. The truncation is safe to interrupt: `Replay` is a deterministic
function of the bytes on disk, so a crash between measuring the valid length and
finishing the truncate just means the next `Recover` call measures the same length
again, and truncating to a length the file is already at or under is a no-op.

**Proven, not just argued.** A test writes five good records and a sixth that is then
corrupted on disk by flipping a byte in its payload — the shape of a bad sector or a
torn write that happened to leave a checksum failure rather than a clean short read.
Recovery is asserted to (1) return without error, (2) recover exactly the five good
records and nothing of the sixth, and (3) truncate the file to the offset the sixth
record started at. A further append and a second, independent recovery pass then
confirm the file replays *all six* records — the original five plus the new one — which
is the assertion a truncation-free `Recover` would fail even while passing the first
two: it would still stop cleanly at the same old corruption on the second pass and never
reach the record written after the first recovery. A sibling test repeats the proof
against a torn tail (a truncated file, standing in for a crash between two writes)
rather than a bit flip, since a node cannot tell which shape of bad tail it is looking
at without trying, and `Recover` must handle both identically.

### 13.2 SSTable block, footer, and index — implemented

Fixed on paper alongside the WAL, because a memtable flush turns WAL-shaped records
into SSTable entries and the two payload layouts are deliberately close in
shape for that reason.

**Data block** — a run of sorted entries followed by a per-block checksum:

```
+---------+-------------+------------+---------------+--------------+
| Type(1B)| KeyLen(4B)  | Key(...)   | ValueLen(4B)  | Value(...)   |    (Put)
+---------+-------------+------------+---------------+--------------+
+---------+-------------+------------+
| Type(1B)| KeyLen(4B)  | Key(...)   |                                   (Delete)
+---------+-------------+------------+
| ... further entries, sorted by key ...                              |
+-----------------------------------------------------------------------+
| BlockCRC32 (4B)                                                       |
+-----------------------------------------------------------------------+
```

Entries are sorted so that, once an index exists, a lookup inside a block can binary
search rather than scan. No restart-point prefix compression yet — that is an open
question (§12): it saves bytes on disk at the cost of a slightly more involved block
reader, and nothing here needs the space back yet. The current reader also does not
binary-search *within* a block once it has located the right one — see §12's note on
this being a linear scan for now, bounded by `targetBlockSize` but not yet optimised.

**THE TYPE BYTE IS A DEVIATION FROM THE ORIGINAL v1.7 DRAFT OF THIS SECTION**, which
had no per-entry discriminator and simply mirrored the WAL's Put payload. That was an
oversight, not a considered simplification: the WAL tells a Put and a Delete apart
using its own *record-level* Type field (§13.1), because a WAL is a sequence of
independently framed records each with its own header. A data block has no equivalent
outer framing once the WAL's record boundary is gone — its entries share one
`BlockCRC32` and one physical run — so without a discriminator, a tombstone flushed
out of the memtable would be indistinguishable on disk from a legitimate empty-string
value. That is exactly the read bug §13.4's three-outcome `Get` contract exists to
prevent one layer up, reintroduced one layer down. A sentinel encoding (`ValueLen =
0xFFFFFFFF`, say, in place of a type byte) was considered and rejected on the same
reasoning the WAL already applied to keys and values themselves: a value is opaque as
far as this format is concerned, and a sentinel is only safe until something
legitimately produces the bytes it claimed no one would. One byte per entry, always
present, rules the ambiguity out structurally instead of by convention. A Delete entry
carries no value field at all — not a zero-length one — for the same reason the WAL's
own Delete payload doesn't (§13.1): the two payload shapes were meant to stay close,
and this keeps them close in the one place the earlier draft had let them drift apart.

**Index block** — one entry per data block, keyed by that block's last key, so a
lookup binary-searches the index in memory and then reads exactly one data block off
disk:

```
+-------------+---------------+-----------------+------------------+
| KeyLen(4B)  | LastKey(...)  | BlockOffset(8B) | BlockLength(4B)  |
+-------------+---------------+-----------------+------------------+
| ... one entry per data block, in block order ...                 |
+---------------------------------------------------------------------+
```

`BlockLength` is the block's full on-disk size, `BlockCRC32` included — a reader that
has read one index entry can read exactly `BlockLength` bytes at `BlockOffset` and
hand the whole thing straight to CRC verification without a second seek.

**Footer** — fixed size, at the very end of the file:

```
+--------------------+--------------------+-----------+
| IndexOffset (8B)    | IndexLength (8B)   | Magic(8B) |
+--------------------+--------------------+-----------+
```

A reader opens the file, seeks to `len(file) − FooterSize`, and finds everything else
from there without needing a preceding table of contents. `Magic` plays the same role
`magic[4]` plays in the persistent-state record (§5): a reader that opens the wrong
file, or a truncated one, learns that immediately from a magic mismatch instead of
decoding garbage as a plausible-looking footer. The eight bytes chosen are the ASCII
string `HELIOSST`, for no reason beyond being memorable in a hex dump.

*Rejected: a footer or table of contents at the front of the file.* An SSTable is
written once, sequentially, one data block after another; the index and footer can
only be assembled once every block's final offset is known, which means they are
necessarily the last things written. Putting them at the front would mean either
reserving a fixed size for them before the key count is known — wrong the moment it
varies — or a second pass to backfill placeholder bytes, which a trailing footer
avoids by construction.

**Target block size** is 4KB (`targetBlockSize`), bounding how much a reader buffers
to satisfy one `Get` — the same reason LevelDB fragments its own log into 32KB blocks
(§13.1), applied here to reads rather than writes. A block is allowed to exceed the
target by exactly one entry: the writer only refuses to *start* a new entry in an
already-over-target block, never closes an empty one to satisfy the budget. Closing an
empty block on this check would leave any single entry larger than `targetBlockSize`
unwritable — the same "always make forward progress on the record in hand" argument
the WAL's own no-fragmentation note already makes (§13.1). The number itself is a
conservative starting point, not yet measured against this engine's workload; see §12.

**Atomicity.** A complete SSTable — every data block, the index, and the footer — is
built under a temporary filename and published with the same write-temp / fsync /
rename / fsync-directory sequence `FileStorage` uses for Raft's own persistent state
(§5), for the identical reason: a write of more than one sector is not atomic, and a
crash mid-write must never leave a reader able to open a file at the final path whose
footer, index, or trailing blocks don't actually exist. This is a sharper requirement
than the WAL's, and deliberately handled differently. The WAL is append-only and a
reader already tolerates a torn tail by design (§13.1); an SSTable has no equivalent
tolerance, because its footer is the only way in and is meaningless until every block
and the index behind it exist. Publishing an already-complete file with one `rename`
is exactly the case that primitive exists for. `Write` also refuses outright to
overwrite a file already at the destination path — an SSTable is immutable once
published, and silently clobbering one a manifest or another reader might still be
holding open would violate that guarantee rather than merely bend it.

**Ordering is asserted, not just assumed.** `Write` rejects a source that produces a
key out of order (or repeated) relative to the one before it, the same
believed-impossible-conditions-are-guarded-not-assumed posture §8 takes toward Raft's
apply path. `*memtable.Iterator` can never itself violate this — it walks the skip
list's own ordered level-0 lane — so in production the guard should never fire; it
exists for the same reason the memtable's own writer mutex exists for a hypothetical
second writer (§13.4): correctness for a caller that shouldn't occur, at negligible
cost to provide.

**Reading.** `Open` reads the footer, then the index, into memory — nothing else —
and reads exactly one data block per `Get`, found by binary-searching the in-memory
index for the first block whose `LastKey` is `>=` the key being looked up. `Get`
carries the same three-outcome contract `(*memtable.Memtable).Get` does: never found,
found-and-tombstoned, or found-and-live, for the same reason (§13.4) — an SSTable
sitting underneath one or more memtables in a future read path must be able to tell
"absent" from "deleted here" without falling through to whatever it holds underneath.

**Implemented** at `internal/storage/sstable/`, across `block.go` (entry framing and
per-block CRC), `index.go` (the index and footer), `writer.go` (`Write`, the streaming
block builder, and the atomic publish sequence), and `reader.go` (`Open` and `Get`).
Correctness is checked by a round-trip test against a 5,000-entry reference map with
randomly sized values (forcing several data blocks), a dedicated tombstone-preservation
test, the missing-key contract, block-size bounds, index-sort order, and CRC-corruption
detection at both the block and footer level — all under `-race -shuffle=on -count=3`.

### 13.3 Memtable flush trigger — implemented

Closes the other half of the open question §13.2 used to end on: something has to
decide when a memtable is full and act on it. Two things were missing, addressed
separately because they belong to different packages.

**What "full" means, reported by the memtable itself.** `(*Memtable).ApproxSize()`
sums the key and value bytes every `Put` and `Delete` has ever added or changed —
tombstones contribute their key bytes only, matching what a flush actually writes for
one (§13.2's Delete entry has no value field). `Len()` already existed and was never
the right signal: it counts distinct keys, so a memtable holding one 10MB value and one
holding ten thousand one-byte values would look identical to it, but wildly different
to a flush. This is the same lesson the Raft log's own compaction trigger already
paid for (§10) — measuring `logLength()` in place of the discardable-entry count it
actually needed produced roughly 160× more snapshots than necessary — applied a second
time to a second layer's sizing decision. `ApproxSize` is approximate in one direction
only: it excludes the skip list's own per-node overhead (pointers, the `entryValue`
box), which is real but roughly constant per key regardless of value size, so it
shifts where the true resident size sits without changing where a threshold should.

**Deciding "full enough," left to the caller, same as always.** The memtable's own
type doc has said since §13.4 was written that switching to a fresh memtable and
flushing the old one is the caller's job, not this package's. That boundary is kept:
`ApproxSize` only answers "how big," never "is that big enough." `sstable.FlushIfFull`
is the thin trigger that makes the decision and acts on it — compare `ApproxSize`
against a caller-supplied threshold, and if it's reached, call `Flush` (§13.2's
`Write`, fed from the memtable's own `Iterator`). It is deliberately minimal: it does
not choose a file path or a sequence number (the caller does, the same way `wal.Open`
takes a full path rather than inventing a naming scheme), does not swap a fresh
memtable into the write path, does not clear or reset the memtable it just flushed —
`Memtable` has no such operation, by design (§13.4) — and does not run in the
background. All of that is orchestration belonging to a layer above both packages that
does not exist yet; see §12.

**Implemented** at `internal/storage/memtable/memtable.go` (`ApproxSize`, and the size
bookkeeping added to `upsert`) and `internal/storage/sstable/flush.go` (`Flush`,
`FlushIfFull`). Tested: `ApproxSize` against inserts, overwrites in both directions,
deletes of both new and existing keys, and delete-then-put; `FlushIfFull` below and at
threshold, and confirmed — with a test, not just the doc comment above — to leave the
source memtable's `ApproxSize` and contents completely unchanged after flushing it.

### 13.4 Memtable — a skip list, implemented

The memtable is the sorted, in-memory structure every write lands in before it is
durable in an SSTable: the WAL (§13.1) makes a write crash-safe, and the memtable is
what makes it *queryable* while it is still only in the WAL and not yet flushed. A read
path checks the active memtable first, then any memtables mid-flush, then SSTables
newest-to-oldest, stopping at the first tombstone or value it finds — the memtable is
the first and cheapest of those stops, and the one every write passes through.

**Why a skip list.** Three structures were on the table.

*A red-black or AVL tree* gives the same O(log n) operations and sorted iteration, but
rebalancing on insert rewrites parent and child pointers as a unit — there is no way to
publish a rotation as a single atomic pointer swap the way a skip list publishes a new
node. A concurrent reader mid-traversal during a rotation can be sent down a pointer
that the rotation is in the middle of replacing. Making that safe means either a lock
readers must take too, or a far more intricate concurrent-rotation scheme than this
component's job justifies.

*A plain sorted slice, or a `map` sorted at flush time*, needs no concurrency scheme at
all for reads, since nothing is ever rearranged mid-read — but every insert into a
slice at the wrong position is an O(n) shift, and a `map` has no order to give an
iterator without an O(n log n) sort on every flush. Both are the right choice once a
structure is only ever built once and read many times (an SSTable's index, for
instance); a memtable is inserted into constantly and must stay sorted the entire time.

*A skip list* (Pugh, 1990) gives up the tree's worst-case guarantees for a
probabilistic O(log n) — but its insert is a splice of independent forward pointers,
one per level, each one a single pointer write. Publishing a new node is exactly as
atomic as a skip list's structure allows it to be, without extra machinery to make it
so. That property is the entire reason it was chosen over the other two.

**The concurrency contract, stated precisely.**

- `Get` and iteration take no lock. Any number of goroutines may call either, at any
  time, concurrently with each other.
- `Put` and `Delete` are serialized by an internal mutex. Production has exactly one
  writer in practice — the same single-applier goroutine that already owns the apply
  path (§8) — so the mutex is never contended there; it exists so a second writer,
  should one ever call in by mistake, gets correctness rather than corruption. This is
  the same "guarded, not assumed" posture DESIGN.md §8 takes toward Raft's own
  believed-impossible states, applied to a different layer: `TestConcurrentWritersAreGuardedNotAssumed`
  exercises sixteen concurrent writers on disjoint keys and asserts every one lands
  correctly, precisely because that usage should never occur and is cheap to guarantee
  anyway.

**Why the splice is safe without a reader-side lock.** A new node's own forward
pointers are set before the node is reachable from anywhere — nothing else can observe
it yet, so no atomics are needed for that half. Publishing it is a single atomic store
of the node pointer into its predecessor's forward slot at each level. A concurrent
reader loading that slot observes either the old successor or the new node, and if it
observes the new node, that node's own forward pointers were already set one line
earlier in program order — there is no interleaving in which a reader can reach a node
that is only half-linked. Go's memory model treats `sync/atomic` operations as
synchronization points precisely so that a store observed by a load carries everything
sequenced before the store with it; that guarantee is what this argument leans on.

**Why an update is safe without a reader-side lock either.** A key's value and its
tombstone bit are boxed together in one struct and swapped as a single atomic pointer,
never written to separately. A reader that loads a node's current entry gets the whole
old struct or the whole new one — never a value that has moved on while its tombstone
bit has not, or the reverse, and never a value with some old bytes and some new ones
spliced together. `TestConcurrentReadsDuringUpdateDetectTornValue` checks this directly
rather than by argument alone: values are filled with a single repeated byte, so a torn
read would show up as a value whose bytes disagree with each other, and 20,000
concurrent overwrites against 16 concurrent readers never produce one.

**What a `Get` result means.** Three outcomes, not two — `ok=false` (never written to
this memtable; keep searching an older level), `ok=true, tombstone=true` (deleted here;
stop searching older levels, the value has been superseded), and `ok=true,
tombstone=false, value=...` (found). Collapsing the tombstone case into "not found"
would be the classic LSM read bug: a delete followed by a search that falls through to
a stale value still sitting in an older SSTable.

**Iteration makes no snapshot-isolation promise, deliberately.** An iterator walks the
same lock-free level-0 chain a `Get` does, so it may run concurrently with a writer —
`TestConcurrentIteratorDuringWrites` runs eight iterators against one writer and asserts
only that keys never arrive out of order, not that any particular set of keys was seen.
That is the honest contract for what an iterator is for today: draining a memtable that
has already been switched out of the write path ahead of a flush, where nothing is
writing to it any more and the ordering guarantee is all a block writer needs. It would
be the wrong contract for a hypothetical transaction or range-scan feature that needed
a consistent point-in-time view of a memtable still being written to; nothing here
claims to provide that.

**`rng` is per-memtable and seeded, never the global `rand`.** The same rule Raft's
election timer follows (§10), for the same reason: a seeded, per-instance source makes
one memtable's sequence of level choices reproducible from one seed regardless of what
else is running, and keeps two memtables from sharing state that would let one's insert
order leak into another's level distribution.

**`Memtable` exposes `ApplyPut`/`ApplyDelete` without importing package `wal`.**
`ApplyPut` and `ApplyDelete` are the two operations a WAL replay needs to rebuild a
Memtable's state, kept as plain methods rather than behind a formal interface this
package would have to import `wal` to name. Not importing `wal` keeps the dependency
one-way: the WAL knows nothing about memtables, and the memtable package knows nothing
about the WAL either. **This paragraph itself was wrong from v1.8 until v1.12** — it
used to claim `ApplyPut`/`ApplyDelete` "satisfy the `wal.Sink` interface structurally"
and that a node's startup path calls `wal.RecoverAndOpen(path, policy, memtable)`.
Neither ever existed: package `wal` has never defined a `Sink` interface, and its
actual recovery function, `Recover` (§13.1), takes a plain `func(wal.Entry) error`
callback, not a `Memtable` or any interface at all. The design was right — this is
exactly the shape the wiring needed to take — but the code implementing it did not
exist for four revisions after the prose describing it was written. `engine.RecoverMemtable`
(§13.7) is that wiring, finally built: the `func(wal.Entry) error` closure this
paragraph always implied, living in package `engine` rather than here or in `wal`,
because `engine` is the one package allowed to depend on both.

**Implemented** at `internal/storage/memtable/`, across `skiplist.go` (the node type,
the lock-free `search`, and level selection), `memtable.go` (the public `Put` / `Delete`
/ `Get` / `Len` / `ApproxSize` surface and the `ApplyPut`/`ApplyDelete` recovery
methods), and `iterator.go`.
Correctness is checked against a reference map built alongside 5,000 randomly ordered
inserts, including duplicate keys and tombstones; concurrency is checked by four
dedicated tests — concurrent reads alone, concurrent reads against a single writer
inserting new keys, concurrent reads against a single writer overwriting one hot key
(the torn-value check above), and concurrent iteration against a writer — all run under
`-race -shuffle=on -count=3`. `ApproxSize`, added in v1.9, has its own dedicated tests
covering inserts, overwrites in both directions, deletes of both new and existing keys,
and delete-then-put — see §13.3.

### 13.5 Bloom filter — implemented, measured

Built as a standalone data structure, deliberately not yet wired into the SSTable
write or read paths (§13.2). A point lookup for a key that turns out to be absent from
a given SSTable currently costs one block read to discover that (§13.2's `Get`); the
whole reason an LSM engine grows a Bloom filter is to answer "definitely not here"
for most of those lookups from a small in-memory structure instead, without touching
disk. Wiring one into `Write` (built once, over the same sorted keys a flush already
iterates) and `Open`/`Get` (checked before the index search) is real work belonging to
a later task; this one is the filter itself, built and its own core claim — the
false-positive rate follows the standard formula — checked against real numbers before
anything depends on it.

**The structure.** A fixed-size bit array, `Add` and `Test` each touching `k` bits
derived from a key by Kirsch-Mitzenmacher double hashing: `h_i = h1 + i·h2 (mod
numBits)`. `k`, the number of probes, is chosen once at construction time from
`bitsPerKey` — `k = round(bitsPerKey · ln 2)`, clamped to `[1, 30]` — the standard
result for the `k` that minimizes false-positive rate at a given bits-per-key budget.
`Test` never produces a false negative: a key's bits, once set by `Add`, are never
cleared by anything, and there is no remove operation.

**THE FIRST HASH CONSTRUCTION TRIED DID NOT WORK, AND THE MEASUREMENT IS WHAT CAUGHT
IT.** `h1` and `h2` were originally FNV-1 and FNV-1a — the same algorithm with its
multiply and XOR steps swapped — on the reasoning that Kirsch-Mitzenmacher double
hashing only needs `h2` to disagree with `h1` across most inputs, not to be
cryptographically independent of it. Measuring against the theoretical curve showed
that reasoning was wrong in practice: observed false-positive rates ran 2×–10× the
theoretical value, and — the detail that pointed at the actual cause rather than
generic noise — the excess grew with `k`, worse at higher bits-per-key settings, not
uniform across all three. FNV-1 and FNV-1a are close enough as transformations of the
same bytes that their outputs correlate more than the double-hashing proof's
independence assumption tolerates, and that correlation compounds as more linear
combinations of `h1` and `h2` get probed. The fix was to stop hashing the key a second
time and instead derive `h2` from `h1` through `mix64`, the splitmix64 finalizer (also
used as MurmurHash3's 64-bit finalizer): three xorshift/multiply rounds giving full
avalanche, so every input bit influences every output bit. This is the same shape
LevelDB's own Bloom filter takes — one real hash, every other probe derived from it —
rather than a coincidence. Re-measured after the fix, every setting landed inside its
tolerance band; see the table below. **The lesson generalizes past this one
structure**: two hashes being "different algorithms" is not the same claim as two
hashes being independent enough for a construction that assumes independence, and the
gap between those two claims is exactly what a passing-looking implementation can hide
until it's measured against a number with a formula behind it.

**The formula, evaluated at what `New` actually builds, not an idealization of it.**
`TheoreticalFalsePositiveRate(numKeys, bitsPerKey)` computes `p = (1 − e^(−kn/m))^k`
at the same `n`, `m`, and `k` a `Filter` for those parameters would actually use —
`m` rounded up to a whole number of bytes and `k` rounded to an integer, both by the
same `sizeFor` helper `New` itself calls — rather than the simplified asymptotic
`p ≈ 0.6185^bitsPerKey` some references quote, which assumes both quantities are
real-valued. The two roundings are small at the sizes this engine measures at, but
computing the exact formula costs nothing extra and removes a source of "why doesn't
my number match the reference" that has nothing to do with the implementation.

**Measured**, 50,000 keys added, 200,000 disjoint keys tested against, three widely
spread bits-per-key settings — 6 (space-favoring), 10 (LevelDB's and RocksDB's shared
default), and 14 (accuracy-favoring):

| bits/key | k | bits (array size) | theoretical FPR | observed FPR | ratio |
|---|---|---|---|---|---|
| 6  | 4 | 300,000 | 0.05606 | 0.05591 | 0.997 |
| 10 | 6 | 500,000 | 0.00844 | 0.00842 | 0.998 |
| 14 | 9 | 700,000 | 0.00121 | 0.00125 | 1.027 |

Every observed rate falls within a tolerance band derived from the sampling
distribution itself — the count of false positives among 200,000 independent trials
at the theoretical rate `p` is `Binomial(200000, p)`, so the band is six standard
deviations (`6·√(p(1−p)/200000)`) plus a 10% relative margin, wide enough to absorb
FNV-1a-and-`mix64` not being a literal uniform-random oracle without hiding a real
implementation bug the way the pre-fix numbers were not hidden by it.

**Implemented** at `internal/storage/bloom/`, across `bloom.go` (`New`, `Add`, `Test`,
`OptimalK`, `TheoreticalFalsePositiveRate`, and the hashing internals above),
`bloom_test.go` (the never-a-false-negative invariant, checked exhaustively rather
than sampled; an empty filter matching nothing; sizing always rounding to a whole
number of bytes; degenerate non-positive inputs clamped rather than rejected; and
determinism — two identically built filters agreeing on every `Test` call, which is
what makes the measurement below reproducible run to run), and `measure_test.go` (the
false-positive measurement above, plus a coarse monotonicity check that the rate never
gets worse as bits-per-key rises, independent of the exact numbers). All under `-race
-shuffle=on -count=3`.

### 13.6 Read path across memtables and SSTables — implemented

Every layer under §13 has been built and tested in isolation — the memtable's own
`Get` (§13.4), an SSTable's own `Get` (§13.2) — but a real read has to check more than
one of them, in the right order, and stop at the first one with an answer. This is
that merge, and nothing more: `engine.Reader` does not decide when a memtable becomes
immutable, does not trigger a flush, does not track which SSTable is newest, and does
not persist or discover which files exist. It is handed its sources already in the
right order and its only job is to walk them.

**The order, corrected.** Active memtable first, then immutable memtables (frozen,
waiting to be flushed) newest-frozen-first, then SSTables newest-flushed-first —
**not** the oldest-to-newest order §13.4's own read-path sketch and one of §12's open
questions both stated until this section corrected them. The reasoning is the same
reasoning behind every tombstone-shadowing argument elsewhere in §13: a key's most
recent write — whichever tier it landed in — has to be the first one found, or an
older tier's stale value (or a since-superseded live value sitting under a newer
tombstone) would win instead. Checking SSTables oldest-first would have been exactly
that bug, just one layer further out than the ones §13.2 and §13.4 already guard
against. Nothing in code ever shipped with the wrong order — it was only ever wrong in
the prose describing the target shape — but it was wrong for two revisions before this
task's own title stated the correct order plainly enough to catch it.

**The three-outcome discipline finally pays for itself.** `(*memtable.Memtable).Get`
and `(*sstable.Reader).Get` each distinguish "never written here" from "deleted here"
from "found here, live" for exactly this moment: `Reader.Get` stops at the first tier
reporting either a live value or a tombstone, and only a tier reporting the key as
never-seen lets the search continue to an older one. A two-outcome `Get` at any layer
below this one — collapsing "deleted" into "not found" — would have made this method
impossible to implement correctly, no matter how carefully the merge logic above it
was written. `Reader.Get` itself returns two outcomes, not three: a caller outside
this package has no use for "never written" versus "written, then deleted," and
passing the distinction further out would only relocate the same collapsing risk
instead of removing it.

**An SSTable read error halts the search rather than falling through.** A corrupt or
unreadable block in the newest SSTable being checked might be hiding the actual answer
— a tombstone, or an overwrite — for the key in question. Treating that error as "this
tier had nothing to say" and moving on to an older SSTable could silently return stale
data in place of surfacing the failure. `TestSSTableReadErrorHaltsSearchAndDoesNotFallThrough`
checks this directly: a failing newer SSTable with a real, matching answer sitting in
an older one still produces an error, never the older tier's value.

**Bloom filter integration is deliberately absent.** §13.5 built and measured a Bloom
filter; `Reader.Get` does not consult one before calling an SSTable's `Get`, which is
the entire performance case a filter exists to make. Wiring it in needs a filter
serialization format that doesn't exist yet (§12) and a place in `Open` to load one —
real work, left for the task that closes those open questions, not attempted here.

**No orchestration, still.** `Reader` accepts an already-ordered list of immutable
memtables and SSTables; nothing yet produces that list. `sstable.FlushIfFull` (§13.3)
still blocks its caller for the duration of a flush, still does not swap a fresh
memtable into the write path first, and nothing calls it on a schedule. Closing the
read-path open question did not close the write-path one sitting next to it in §12 —
seeing that clearly is exactly why the two are recorded as separate questions instead
of one.

**Implemented** at `internal/storage/engine/`, in `reader.go`. Two narrow interfaces —
`memtableSource` and `sstableSource`, matching `(*memtable.Memtable).Get`'s and
`(*sstable.Reader).Get`'s exact signatures — let the merge logic be tested against
hand-built fakes for the cases that are awkward to provoke through real files on every
run (tombstone shadowing at each tier, a simulated corrupt-block error), on the same
narrow-interface precedent `sstable.Source` set in `writer.go`. `reader_test.go` also
runs the same scenarios end to end through real `*memtable.Memtable` and
`*sstable.Reader` values, confirming `NewReader`'s adapter wiring and not just the
interface-driven logic. 100% statement coverage, all under `-race -shuffle=on
-count=3`.

### 13.7 Tombstones and why a delete is a write — implemented

Every layer in §13 already carries a tombstone somewhere — a `RecordDelete` in the
WAL (§13.1), a `tombstone` bit in a memtable's `entryValue` (§13.4), a `Delete` entry
with no value field in an SSTable's data block (§13.2), the shadowing logic in
`Reader.Get` (§13.6) — but none of those sections, on its own, states the single
principle all four exist to serve, or proves it by construction rather than by
scattered argument. This section is that principle, stated once, plus the piece of
code that was still missing to make it checkable end to end: `engine.Writer`, the live
write path, which did not exist before this task even though every read-side and
on-disk piece around it did.

**The principle.** In a log-structured, immutable-file storage engine, deleting a key
cannot mean removing information — it can only mean recording a new fact that
supersedes an old one. This is forced by two things this design already committed to
elsewhere, not a new constraint invented for deletes specifically:

1. **Durability (§13.1).** A write is not acknowledged until it is on stable storage.
   If a delete were handled purely in memory — remove the key from the active
   memtable, append nothing to the WAL — a crash between that removal and the next
   flush would lose all record that the delete ever happened. On restart, WAL replay
   would rebuild the memtable from whatever Puts it still has on file, and the
   "deleted" key would silently reappear with its last value. A delete has to cross
   the exact same durability boundary a Put does, for the exact same reason.
2. **Immutability once flushed (§13.2, §13.6).** Even setting crash recovery aside,
   the moment more than one memtable or SSTable can exist for the same key range —
   which §13.6 is what made real — an older, already-durable copy of a key may sit in
   a tier the operation currently running cannot see or touch. Nothing can reach back
   and un-write that copy; SSTables are flushed once and never modified again (§13.2's
   whole `ErrFileExists` argument). The only way to make that old copy stop being the
   answer is to write something NEWER that a read, walking tiers newest-first, finds
   first — a tombstone is that newer fact, not an erasure of the old one.

Both arguments land on the same conclusion from different directions: a delete is a
**write** — same durability contract, same shape, same place in every tier's ordering
— whose payload happens to mean "nothing is here anymore" instead of carrying a value.
Nowhere in this codebase does a delete take a different code path than a put for
reasons of being a delete; the only differences are which WAL record type gets
appended and which bit gets set.

**What was actually still missing.** Every tier already stored a tombstone correctly.
What had never been built was the thing a client's `Delete` call would actually go
through: `engine.Writer`, pairing one `*wal.WAL` and one `*memtable.Memtable`, with
`Put` and `Delete` both following the identical two-step sequence — append to the WAL,
check the error, only then apply to the memtable — differing solely in which WAL
method and which Memtable method get called. `Delete`'s doc states directly why it
cannot short-circuit when the memtable doesn't currently hold the key: it has no way
of knowing whether an older, invisible-to-it tier holds a live copy, so skipping the
append is indistinguishable, after a crash, from the delete having never been
requested at all — `TestDeleteOfAKeyNeverPutStillWritesATombstone` checks this
directly.

**Proved, not just argued.** `TestDeleteSurvivesACrashBetweenAppendAndApply` provokes
the exact crash window the durability argument above describes: a `Put` and a
`Delete` both go through a `Writer`, and then the in-memory `*memtable.Memtable` that
applied them is discarded outright — standing in for a process that died with
unflushed memory state — with no flush, no snapshot, nothing. `engine.RecoverMemtable`
rebuilds a completely fresh memtable from the WAL alone, and the tombstone is there.
If `Delete`'s `AppendDelete` call were ever skipped, reordered after the memtable
update, or replaced with an in-memory-only removal, this is the test that would catch
it — the same role `TestStartupRecoveryStopsCleanlyAtTornTail` (§13.1) plays for WAL
truncation, proving the property by rebuilding from bytes on disk rather than trusting
the in-memory state that produced them. `TestPutAndDeleteBothFailIdenticallyWhenTheWALIsUnwritable`
checks the failure side of the same symmetry: closing the WAL out from under a
`Writer` makes both `Put` and `Delete` fail before ever touching the memtable, not one
of them silently degrading to an in-memory-only operation while the other correctly
fails.

**`engine.RecoverMemtable` also closes a four-revision-old documentation bug.**
§13.4's memtable section has claimed, since v1.8, that `ApplyPut`/`ApplyDelete`
"satisfy a `wal.Sink` interface" and that a startup path calls
`wal.RecoverAndOpen(path, policy, memtable)`. Neither type nor function has ever
existed in package `wal` — `Recover` (§13.1) takes a plain `func(wal.Entry) error`
callback. The design was correct; the code implementing it was not written until this
task. `RecoverMemtable` is that callback, built once in package `engine` (the one
package allowed to depend on both `wal` and `memtable`) instead of hand-rolled by
every future caller, and §13.4's text is corrected to describe what actually exists.

**Implemented** at `internal/storage/engine/writer.go` (`Writer`, `NewWriter`,
`RecoverMemtable`) and `writer_test.go`. Alongside the crash-window and failure-
symmetry tests above: immediate visibility of both a `Put` and a `Delete` through the
paired memtable; a delete of a key the memtable never held still producing a
tombstone; and `TestReadPathSeesADeleteThroughTheFullStack`, running a `Put` then a
`Delete` through a real `Writer` and confirming `Reader.Get` (§13.6) — not a direct
`Memtable.Get` — reports the key gone, tying the write path and the read path
together through the same data for the first time. All under `-race -shuffle=on
-count=3`.

### 13.8 Leveled compaction — implemented

This section closes two open questions at once: "SSTable file naming and manifest
are not designed" (recorded since v1.9) and the actual reason a manifest was needed
in the first place — nothing had ever reclaimed the space a superseded value or a
no-longer-needed tombstone was still taking up. Getting here also required two
smaller, real pieces that turned out not to exist yet: a way to read an entire
SSTable's contents in order (`Get`, §13.2, only ever answered one key at a time), and
a way to detect a source failing mid-read instead of silently looking like it
finished cleanly. Both are covered below before the compaction algorithm itself,
because the algorithm is built directly on top of them.

**`Source` gained `Err() error`, and this is a real interface change, not an
addition made for free.** Every `Source` before this task was `*memtable.Iterator` —
pure in-memory, structurally unable to fail — so `Write`'s original loop,
`for src.Next() { ... }`, never checked for a failure because nothing could produce
one. `sstable.Iterator` (below) reads blocks off disk mid-scan, which very much can
fail, and `Next()` returning `false` is the same observable outcome whether iteration
finished cleanly or broke partway through a corrupt block. Without `Err`, a
compaction merging several SSTables could silently write a truncated output file and
report success — exactly the kind of bug this codebase has repeatedly built explicit
guards against rather than trusted itself to avoid by care alone (§13.1's CRC-checked
`Recover`, §13.2's block CRCs, §13.6's SSTable-read-error-halts-the-search rule).
`Write` now checks `src.Err()` immediately after its main loop, before the final
block, index, and footer are ever written, and refuses to publish a file if it is
non-nil — `TestWriteRefusesToPublishWhenTheSourceFails` provokes exactly this with a
source that yields several good entries and then fails, and confirms no file, not
even a partial one, is left at the destination path. `*memtable.Iterator.Err` trivially
returns `nil` always, at zero real cost, rather than being exempted from an interface
it was the sole implementation of.

**`sstable.Iterator` is `Get`'s counterpart for reading everything instead of one
key**, walking every data block in file order and decoding its entries — the same
`verifyAndSplitBlock`/`decodeBlockEntries` machinery `Get` already used, just iterated
across every block instead of binary-searched to one. It satisfies `Source`, which is
the entire reason it exists as this type rather than some SSTable-specific scan API:
a compaction reads several SSTables, merges them, and hands the merged result
straight back to `Write` to produce a new file, and nothing in that pipeline needs to
know an SSTable-backed `Source` looks any different from a memtable-backed one.
`TestIteratorReportsCorruptionThroughErrRatherThanSilence` flips a byte inside a real
flushed file's data block and confirms the iterator stops with a non-nil `Err` rather
than quietly reporting fewer entries than the file actually has.

**`sstable.Merge` combines several `Source`s, already sorted, into one sorted
`Source`** — the primitive both the write side (Reader's tier-priority rule, §13.6)
and compaction's write-out step are built on. Sources are required newest-first, the
identical convention `engine.Reader` already uses and for the identical reason: on a
key collision, the lowest-indexed source wins, and every source holding that key is
still advanced past it so the key is never repeated in the output — a repeated key
would trip `Write`'s own `ErrOutOfOrder` guard, so this is load-bearing, not
cosmetic. **A real bug was caught and fixed while building this, not left to a
future bug report**: the first version checked every source's `Err()` in a loop that
skipped sources already marked "done" — which meant a source that failed (and was
therefore marked done in the same instant) would never actually have its error
noticed, because the very flag that should have flagged it for inspection was also
the flag being used to skip it. Fixed by checking `Err()` at the exact moment a
source transitions to done, inside `advance`, rather than in a separate later pass —
`TestMergeStopsAndPropagatesWhenASourceFails` is the regression test.

If `dropTombstones` is true, a winning tombstone is discarded rather than emitted —
see `CompactLevel`'s own doc, below, for exactly when that is safe.
`TestMergeDroppingATombstoneDoesNotResurrectAnOlderValue` pins the property that makes
this safe to do at all: the winner is resolved from every source holding the key
*before* the drop decision is made, so dropping a tombstone can never fall through to
an older source's stale copy of the same key — the tombstone and the stale value are
never both candidates for the output at once.

**The manifest** (`internal/storage/manifest/`) is deliberately the smallest thing
that can answer "what exists right now": `Levels [][]string`, one slice of SSTable
filenames per level, nothing else — no byte sizes, no key ranges, no sequence
numbers, no record of a compaction in progress. `Save` persists it with the identical
write-temp/fsync/rename/fsync-directory sequence `FileStorage` uses for Raft's own
state (§5) and `sstable.Write` uses for a flushed file (§13.2) — a third repetition of
the same primitive, for the same reason: a reader must never be able to open the
manifest partway through a write and see a state that names a file that does not
exist. `Load` on a missing file returns an empty manifest rather than an error, the
same "nothing on disk yet is a valid starting state" posture `Recover` (§13.1) takes
toward a missing WAL.

**Leveled compaction** (`internal/storage/compaction/`) answers the four questions
its own task description asks, in that order:

1. **Pick the level** — `PickLevel` returns the lowest level whose file count exceeds
   a threshold (`Options.MaxFilesPerLevel`, default 4, mirroring LevelDB's own L0
   trigger). The *lowest* over-threshold level, not the most over-threshold one, is
   chosen: L0 files overlap in key range and are checked by every read regardless of
   whether they hold the key being looked for (§13.6), so a backlog at the shallowest
   level costs every read, not just ones for keys living in whichever level happens to
   be most backlogged.
2. **Merge** — `sstable.Merge`, above, fed one `sstable.Iterator` per file in the
   chosen level and the level below it, level's files listed first (newer).
3. **Write out** — the merged `Source` is handed straight to the existing, unmodified
   `sstable.Write` (§13.2). Compaction adds no new file-writing code of its own; it
   only ever produces the `Source` `Write` already knew how to consume.
4. **Atomically swap the manifest** — `manifest.Save`, once, after the new file is
   already durably on disk.

**The tombstone rule.** A tombstone surviving the merge into level `L+1` is dropped
— actually reclaiming the space, which is the entire point of a compaction reaching a
delete at all — only if `L+1` will be the lowest level holding any data once this
compaction finishes (`isBottomAfterCompaction`: every level deeper than `L+1` is
currently empty). If a deeper level still holds files, this compaction has no way of
knowing whether one of them still holds an older, live copy of the key the tombstone
is shadowing; dropping it anyway would let that older copy resurface the next time
that deeper level is searched — exactly the bug §13.7's whole delete-is-a-write
argument exists to prevent, one level further down.
`TestCompactLevelKeepsTombstoneWhenDeeperLevelsHaveData` and
`TestCompactLevelDropsTombstoneWhenNoDeeperLevelHasData` check both sides of this
directly, the second by confirming the key is genuinely gone from the output, not
merely tombstoned.

**The crash-safety ordering, stated once and enforced by code order.** `Run` writes
the merged file first (relying on `sstable.Write`'s own atomicity, not adding to it),
saves the new manifest second, and deletes the superseded files last. A crash before
the manifest save leaves the OLD manifest naming the OLD files, all still on disk —
compaction simply never happened. A crash after the save but before cleanup leaves
harmless orphaned files the manifest no longer references — disk usage, not a
correctness problem, and not yet garbage-collected automatically (open question,
below). The one ordering never used is deleting old files before or during the swap:
that crash window would leave the manifest naming a file that is already gone, with
no way to recover from it on the next `Load`.
`TestRunNeverDeletesOldFilesIfTheManifestSwapFails` checks this by forcing the
manifest save to fail (an unwritable path) and confirming every source file is still
exactly where it was.

**Deliberate simplifications, recorded as open questions rather than solved here:**
file-count triggers rather than byte size (§12); exactly one output file per
compaction rather than several size-bounded ones; a compaction always merges a level's
files with *all* of the level below's files rather than only the overlapping subset a
true leveled scheme would target, which write-amplifies more than necessary but is
far simpler to get correct; and orphaned files left behind by a crash between the
manifest swap and cleanup are not yet detected or removed on a later startup.

**Implemented** at `internal/storage/sstable/iterator.go` (`Iterator`),
`internal/storage/sstable/merge.go` (`Merge`), `internal/storage/manifest/` (`Manifest`,
`Load`, `Save`), and `internal/storage/compaction/` (`PickLevel`, `CompactLevel`,
`Run`). Tested end to end: a full `Run` cycle against real flushed SSTables, confirmed
against the manifest on disk, the merged file's actual contents, and that superseded
files are gone; the manifest-swap-failure crash-safety test above; and every merge and
tombstone-rule case argued above, checked directly rather than only in prose. All
under `-race -shuffle=on -count=3`.

### 13.9 Background compaction — implemented, measured

`compaction.Run` (§13.8) does one complete pick-merge-write-swap cycle and returns —
nothing before this section ever called it more than once, synchronously, from a
test. This closes that: `Background` runs cycles on a ticker in their own goroutine,
so nothing calling `engine.Writer.Put` or `.Delete` (§13.7) ever has to trigger or
wait on a compaction.

**Why this needed no new locking, and why that claim is checked rather than trusted.**
`engine.Writer`'s only durable state is its own `*wal.WAL` and `*memtable.Memtable`
(§13.7); `Run` only ever touches SSTable files already on disk plus the manifest
(§13.8). The two share no lock, no field, nothing — a live write path was never going
to contend with a background compaction for anything in-process, by construction
rather than by any synchronization added here. That argument is worth exactly as much
as a test that runs both at once under `-race` and finds nothing, which
`TestConcurrentWritesAndBackgroundCompactionProduceNoRaceOrCorruption` does: a write
workload and `Background` compacting a seeded backlog, concurrently, in the same
directory, clean under the race detector, with the compacted output still correct
once both finish.

**What running concurrently CAN still cost: the same disk.** No in-process lock is
contended, but `Background`'s own I/O — reading several SSTables, writing a merged
one, then `manifest.Save`'s fsync/rename/fsync-directory sequence — competes with a
concurrent `Writer`'s WAL fsyncs for the same physical disk's bandwidth and, on a
`SyncAlways` policy, the same fsync queue. This is the actual question worth
measuring, and the one a "no shared lock" argument alone cannot answer.

**Measured**, `TestWriteLatencyWithAndWithoutBackgroundCompaction`:

| scenario | mean | p50 | p99 | max |
|---|---|---|---|---|
| baseline (no compaction) | 2.876ms | 2.985ms | 4.011ms | 10.184ms |
| concurrent (background compaction) | 2.762ms | 2.982ms | 3.607ms | 10.080ms |

One background compaction cycle completed during the concurrent run. Mean, p50, and
max are within measurement noise of each other between the two scenarios — p99 is
actually slightly *better* concurrent (3.607ms vs. 4.011ms), which is consistent
with ordinary run-to-run variance at this sample size rather than a real effect in
either direction. **The result supports the "no shared lock" argument directly**:
if `Background`'s own disk I/O were meaningfully starving the concurrent `Writer`'s
WAL fsyncs, the concurrent row would show consistently higher latency at every
percentile, not numbers this close together with p99 favoring the concurrent case.

**THIS MEASUREMENT DELIBERATELY DOES NOT ASSERT A HARD STATISTICAL THRESHOLD IN
CODE**, unlike the Bloom filter's false-positive rate (§13.5), which a formula
predicts exactly and which the test therefore checks against a derived tolerance
band. Write latency under concurrent disk I/O has no such formula — it is a property
of the actual machine and disk it runs on, and a hard-coded millisecond bound would
be flaky on a slower CI runner or a faster laptop without meaning anything more true
on either. The honest way to report it is the numbers themselves, above, from a real
run — not asserted, measured, the same distinction the fsync policy write-up
(`fsync-policy.md`) already draws. The test's only hard assertion is a generous
smoke-test ceiling — no single write may take longer than five seconds — which exists
to catch an actual hang or deadlock, a real bug this test should still fail loudly
on, not to make a claim about ordinary contention.

**A hazard found while designing `Background`, fixed before it shipped rather than
after a bug report.** `Run` compacting level `L` always replaces `L+1`'s files with
exactly one new file (§13.8's documented simplification), which means every level
above L0 can only ever hold zero or one files — genuine cascading (one compaction
pushing the level below it over its own threshold) cannot happen with any sane
`MaxFilesPerLevel` of 1 or more, since a level holding at most one file can never
exceed a threshold that isn't 0. But nothing rejected `Options{MaxFilesPerLevel: 0}`,
and under that value, `PickLevel` considers a level with even one file "over
threshold" forever: a single file would get compacted one level deeper, endlessly,
the manifest growing one level longer each cycle with no convergence. This was found
by reasoning through the design, not by hitting it — `maxDrainCycles` bounds
`drainOneCycle` to 64 `Run` calls before it gives up and reports an error instead of
spinning, and `TestDrainOneCycleStopsAtTheSafetyCapRatherThanSpinningForever` provokes
the degenerate configuration directly and confirms the loop terminates in well under
a second rather than hanging.

**`Stop` waits, deliberately unlike Raft's own ticker shutdown.** `Node.Stop`
(`election.go`) is fire-and-forget: it closes a channel and returns, trusting the
ticker goroutine to notice eventually. `Background.Stop` closes its own stop channel
and then blocks until the loop has actually exited, because a caller — tests,
especially — needs the guarantee that no further `Run` call will touch the manifest
or an SSTable file once `Stop` returns, not just that a stop was requested. The
difference is not a style preference: `TestBackgroundCompactsAnAlreadyOverThresholdManifestOnStart`
and `TestBackgroundDoesNothingWhenNothingNeedsCompacting` both inspect on-disk state
immediately after `Stop` returns, and depend on this guarantee to be deterministic at
all — a fire-and-forget `Stop` would make both tests flaky, not merely slower.

**Implemented** at `internal/storage/compaction/background.go` (`Background`,
`StartBackground`, `Stop`, `Err`, `Cycles`) and `background_test.go` /
`stall_test.go`. Tested: compacting an already-over-threshold manifest immediately on
start (not waiting a full interval); repeated compaction across several ticks as new
backlog arrives; a compaction error surfacing through `Err` without hanging the loop
or leaving it unstoppable; `Stop`'s idempotency; the safety-cap termination above;
the concurrent race/correctness test above; and the write-latency measurement above.
All under `-race -shuffle=on -count=3`.

### 13.10 Manifest recovery — implemented

§13.8 already made a crash mid-compaction *safe*: `Run`'s ordering (write the merged
file, then swap the manifest, then delete superseded files) means a crash at any
point leaves either the old, complete state or the new one, never something naming a
file that doesn't exist. What it left open — recorded explicitly at the time —
was making that same crash *recoverable* in the operational sense: nothing detected
or cleaned up the orphaned files a crash between the swap and cleanup leaves behind,
and nothing validated, at startup, that the manifest's claims about what exists on
disk are actually true. `Recover` is that missing step.

**Two kinds of mismatch, two very different responses.** A file on disk the manifest
doesn't reference is an orphan — exactly what `Run`'s own crash window produces — and
`Recover` deletes it, no error. A `.tmp` file is deleted unconditionally, without even
checking the manifest: `sstable.Write` and `manifest.Save` each only ever leave one
behind by being interrupted mid-write (a hard kill or power loss, not a returned
error, since a returned error already triggers their own defer-based cleanup), so a
`.tmp` file existing at all means it was abandoned — there is no legitimate reading
of the manifest under which one would ever be a live, needed file. The other
direction is treated oppositely: a file the manifest *references* but disk is
missing, or which fails to open as a valid SSTable, is reported as an error, not
silently tolerated. Given `sstable.Write`'s and `manifest.Save`'s own atomicity
guarantees, a manifest is only ever saved after the file it names is already
durably, completely on disk — so this can only mean a bug in this package, external
interference with the data directory, or real disk corruption, none of which
`Recover` can safely paper over by pretending the file was never named. This is the
same "believed impossible is guarded, not assumed" posture §8 takes toward Raft's
own invariants, applied here to this package's own claims about itself. Every
referenced file is opened, not merely stat'd — `sstable.Open` verifies the footer and
index, catching a present-but-corrupt file at startup instead of the first time some
later `Get` happens to read the wrong block.

**`Recover` must run before any compaction starts, and cannot enforce that itself.**
It deletes files a live compaction may have legitimately created but not yet
referenced — its own in-progress merged output, its own in-progress `.tmp` file —
so running it concurrently with `Run` or `Background` would delete out from under
them. There is no way to tell "a stale orphan from a past crash" apart from "a file
a compaction running right now is about to reference" by looking at the file alone;
this is a precondition stated in `Recover`'s own doc, the same way `engine.RecoverMemtable`
(§13.7) is understood to run once, at startup, before anything live depends on the
state it rebuilds — not enforced by a type system, enforced by being the first thing
a caller does.

**Proved against the exact crash window `Run`'s own documentation describes, not a
different or easier one.** `TestRecoverClosesTheExactCrashWindowRunDocuments`
reproduces it by hand: call `CompactLevel` and `manifest.Save` directly — the first
two of `Run`'s three steps — and deliberately stop before the third (deleting the
superseded files), the precise point `Run`'s own doc names as the one safe crash
window. `Recover` is then called against that state and confirmed to finish exactly
the cleanup the simulated crash interrupted, with the compacted data itself intact
and correct.

**Implemented** at `internal/storage/compaction/recover.go` (`Recover`) and
`recover_test.go`. Tested: a healthy directory left untouched; an orphaned SSTable
removed; a leftover `.tmp` file removed unconditionally; a missing referenced file
reported as an error; a corrupt referenced file caught by `sstable.Open` and
reported as an error; a fresh, manifest-free directory handled the same way `Load`
already does (§13.8); files this package has no business touching (anything that
isn't `.sst` or `.tmp`) left alone; and the crash-window reproduction above. All
under `-race -shuffle=on -count=3`.

### 13.11 Write and space amplification, measured across three configurations

Every prior section of §13 built one piece of the compaction pipeline in isolation;
this is the first place two of that pipeline's own numbers are put next to each
other on purpose. Write amplification and space amplification are the two standard
LSM terms (any RocksDB or LevelDB tuning guide covers both) for the same underlying
trade-off:

	write amplification = total physical bytes written / total logical bytes written
	space amplification = on-disk bytes for the current live data / the minimal bytes that data actually needs

More frequent compaction should lower space amplification (stale copies and spent
tombstones get reclaimed sooner) at the cost of higher write amplification (more
rewriting overall) — the classic tension, and the reason `MaxFilesPerLevel` (§13.8)
is a knob at all rather than a constant. `compaction.MeasureAmplification` checks
that expectation against real numbers rather than trusting it as self-evident.

**A finding, not a flaw: final space amplification does not differentiate the three
configurations at all.** `CompactLevel` always performs a *full* merge of both levels
it touches (§13.8's documented simplification, not a partial-range one), so every
configuration, once drained to full convergence, arrives at the exact same
fully-deduplicated steady state — there is only one canonical "everything merged,
nothing stale left" representation of a given live key set, and how many
intermediate compactions ran on the way there cannot change the destination. The
first version of this measurement tried to show the trade-off using this final,
converged snapshot and found no difference across configurations — correctly, as it
turns out, once the reason was traced down rather than assumed to be a bug. Two real
mistakes were caught and fixed in the process of tracing it:

1. **A workload-boundary artifact, not a real result.** With a fixed number of flush
   cycles, a larger `MaxFilesPerLevel` needs more accumulated L0 files before
   compaction ever triggers, so the workload could end with a small, never-yet-merged
   L0 backlog sitting there simply because the run stopped before enough new flushes
   arrived to cross the threshold again. In an early run, that leftover backlog was
   large enough to make the loosest configuration's measured space amplification come
   out *identical* to the tightest one's, for a reason that had nothing to do with
   either policy. Fixed by forcing any leftover L0 backlog into L1 once, explicitly,
   after the workload ends, so every configuration's final snapshot reflects true
   convergence rather than wherever the fixed op count happened to leave it.
2. **The first fix attempted for (1) reproduced a hazard already found and fixed
   once, in a different file.** Draining with `PickLevel`/`maxFilesPerLevel: 0` in a
   loop was tried first — and is exactly the degenerate configuration
   `Background.maxDrainCycles` (§13.9) already exists to guard against: a level with
   even one file is "over threshold" forever at that setting, cascading one level
   deeper every cycle with no convergence, because a compaction always leaves exactly
   one file at the level it just wrote into. `drainAndTally` (this section's own
   drain loop) has no such cap, so the same hazard reappeared here, in new code,
   because the fix for (1) was reached for without first checking whether it reintroduced
   something already on record. The actual fix needed was much narrower: only level 0
   can ever legitimately hold a leftover backlog at the end of this workload (every
   level below it is always left at 0 or 1 files by construction, the same invariant
   §13.9 already argues from), so the real fix is one explicit, bounded `CompactLevel`
   call on level 0 — never a loop, and never at threshold 0.

**The number that does differentiate the three configurations is peak, not final,
space amplification** — the highest total on-disk footprint observed at any point
*during* the run, sampled after each flush and before that cycle's compaction (if
any) has had a chance to reclaim anything. This is the number a real deployment
actually has to provision disk headroom for: not "how small does it get once
everything settles," but "how large does the backlog get before compaction catches
up." A looser `MaxFilesPerLevel` lets more un-merged data accumulate before
reclaiming it, which is exactly what peak space amplification is expected to show
rising with, and does.

**Deterministic, not measured-with-tolerance like write latency (§13.9).** Every
byte count here follows from a fixed, seeded workload and this package's own
deterministic merge and compaction logic — there is no real-time disk latency
anywhere in this measurement's own loop, unlike §13.9's. `TestMeasureAmplificationIsFullyDeterministic`
checks this directly (two runs at the same configuration compared field by field,
not just eyeballed), and it is what lets
`TestWriteAndSpaceAmplificationAcrossThreeConfigurations` assert the trade-off's
direction as a hard pass/fail requirement rather than only log it.

**Measured**, confirmed identical on the project owner's own machine — as expected,
since every input here is deterministic (`TestMeasureAmplificationIsFullyDeterministic`
above is exactly this guarantee, checked, not just claimed):

| maxFilesPerLevel | write amplification | peak space amplification | final space amplification |
|---|---|---|---|
| 2 | 2.750 | 2.417 | 1.090 |
| 4 | 2.446 | 3.287 | 1.090 |
| 8 | 2.347 | 5.024 | 1.090 |

**Plotted** at `amplification.svg` (repository root, alongside `fsync-policy.md` —
this project's other measured artifact that lives outside DESIGN.md itself),
generated by the same command: two bar-chart panels sharing a `maxFilesPerLevel`
x-axis, write amplification on top and peak space amplification below, with a
dashed reference line marking the constant final space amplification value. No
third-party charting dependency was added — this project's `go.mod` has never had
one, and three bars per panel is well within what a hand-written SVG can render
plainly, on the same "use the stdlib, build the structure by hand" precedent
`hash/fnv` and `hash/crc32` already set elsewhere in this engine (§13.1, §13.5).

**Implemented** at `internal/storage/compaction/amplification.go`
(`AmplificationResult`, `MeasureAmplification`, `drainAndTally`, `totalOnDiskBytes`),
`amplification_test.go`, and `cmd/ampplot/main.go` (the standalone tool that prints
a copy-pasteable table and writes the SVG). All under `-race -shuffle=on -count=3`.

### 13.12 Block cache with LRU eviction, measured

Every `Get` before this section paid for a full disk read, CRC verification, and
entry decode on every single call, even for a block it had already read a moment
ago (§13.2). A block cache is the standard answer (any RocksDB or LevelDB tuning
guide has one): keep recently-used blocks' decoded entries in memory, so a repeat
lookup skips all three costs — not just the disk read, which the OS's own page
cache may already be absorbing, but the CPU cost of re-verifying and re-parsing a
block it has already verified and parsed once.

**A cache keyed by (path, block index) can never go stale — there is only an
eviction problem to solve, not an invalidation one.** An SSTable is written once
and never modified again after `Write` publishes it (§13.2's whole `ErrFileExists`
argument), and compaction (§13.8) never edits a file in place — it always produces
an entirely new one and leaves the old for deletion, never mutation. The bytes at
a given (path, block index) today are the bytes that will be there for as long as
that file exists at all. This is what let the cache itself stay simple: no version
numbers, no generation counters, no "has this changed since I cached it" check
anywhere in `blockcache.LRU` — the invalidation problem every general-purpose cache
has to solve does not exist for this engine's data, by construction.

**`blockcache.LRU[K, V]`** (`internal/storage/blockcache/`) is a generic,
byte-size-bounded LRU built entirely from stdlib — `container/list` for the recency
ordering, a map for O(1) lookup, no third-party dependency, on the same precedent
`cmd/ampplot`'s hand-written SVG already set (§13.11). It knows nothing about
blocks, SSTables, or this engine specifically; `sstable.BlockCache` is a type alias
instantiating it with an unexported cache key (`path`, block index) and value
(`[]blockEntry`, the already-decoded entries), plus `NewBlockCache(maxBytes)`, which
sizes each cached block by its summed key-and-value bytes — the same "count key and
value bytes, not structural overhead" convention `(*Memtable).ApproxSize` already
established (§13.3), applied here to a cached block instead of a whole memtable.

**A real edge case caught while designing the eviction rule, not after.** The
straightforward eviction loop — evict from the back until under budget — has a
failure mode: if a single entry is larger than the entire budget, that loop would
evict everything, *including the entry `Put` was just asked to insert*, making
`Put` silently a no-op for anything bigger than the configured size. This is not
hypothetical: an SSTable data block is usually near `targetBlockSize` but is
explicitly allowed to exceed it by one entry (§13.2), so a cache sized smaller than
the largest block that can occur would otherwise never successfully cache anything
at all. Fixed by never evicting the last remaining entry — an oversized single
entry is kept, alone, exceeding the nominal budget, rather than refused.
**A second, related bug surfaced immediately after fixing the first**: that same
leniency meant a budget of `0` (meant as "cache nothing") still left exactly one
entry cached, since `Put` always inserts before the eviction loop runs and that
loop refuses to remove a lone survivor. Fixed by handling `maxBytes <= 0` as its
own case, checked first, before there is anything to evict —
`TestBudgetOfZeroCachesNothing` and `TestSingleEntryLargerThanBudgetIsStillCached`
both pin the two cases directly so neither regresses into the other again.

**`Reader` gains an optional cache, with zero blast radius on the existing API.**
`Open` is unchanged — every existing caller, across every package that already uses
`sstable.Open`, keeps working exactly as before, with no cache at all. A new
`OpenWithCache(path, cache)` attaches a shared `*BlockCache`, which may already hold
blocks from other files: `TestCacheIsSharedAcrossMultipleFiles` confirms two
different SSTables' blocks don't collide or shadow each other in one shared cache.
`TestCacheHitAvoidsTouchingDiskAtAll` is the test that actually proves a hit skips
disk entirely, rather than trusting the code path by inspection: read a key once
to populate the cache, truncate the underlying file to zero bytes, and confirm a
second `Get` for the same key still returns the right value — which is only
possible if that second call never touched the file at all.

**`Iterator` deliberately does not use the cache.** A full sequential scan — the
only thing `Iterator` is for, used by compaction's merges (§13.8) — reads every
block exactly once no matter what, so there is nothing a cache could save it from
re-reading during that same scan; populating the cache with blocks a merge happens
to pass through would only evict genuinely-reusable point-lookup entries for other
keys that were never going to be re-read by that merge anyway.

**Measured**, `TestReadLatencyWithAndWithoutBlockCacheAtThreeSizes`: a single
5,000-key SSTable, read `20,000` times under a fixed, seeded Zipfian access pattern
(skewed, not uniform — a cache's entire benefit disappears under uniform access to
a working set larger than itself, since every access would be a first touch
regardless of size), once with no cache and once each at three cache sizes (10%,
50%, and 150% of the exact cacheable byte total, the last one comfortably enough to
hold every block at once):

| configuration | mean latency | p50 | p99 | hit rate |
|---|---|---|---|---|
| no cache | 1.530µs | 1.292µs | 4.334µs | — |
| 10% | 600ns | 125ns | 2.292µs | 0.701 |
| 50% | 282ns | 125ns | 2.000µs | 0.908 |
| 150% | 142ns | 125ns | 1.500µs | 0.986 |

**Measured on the project owner's own machine**, above. Latency is logged, not
asserted against a hard threshold, for the identical reason §13.9's write-stall
measurement isn't: real time depends on the machine it runs on, and these numbers
differ noticeably from an earlier sandbox run (1.530µs vs. 2.567µs mean at no
cache) while showing the identical shape — a clean staircase, no cache slowest,
150% fastest — for exactly that reason: the shape is what the design predicts, the
absolute numbers are whatever the machine happens to produce. Hit rate is asserted
directly, because it depends only on the fixed Zipfian seed and this package's own
deterministic `LRU` — a larger cache must never hit less often than a smaller one
against the identical access pattern, and the 150% cache (sized to hold everything)
must reach a miss count nowhere near the total read count. The hit-rate column
above — 0.701 / 0.908 / 0.986 — matches the sandbox run exactly, confirming that
determinism directly rather than by argument alone.

**Implemented** at `internal/storage/blockcache/lru.go` (`LRU`, `New`),
`lru_test.go`; `internal/storage/sstable/reader.go` (`BlockCache`, `NewBlockCache`,
`OpenWithCache`, the cache-aware `loadBlock`), `cache_test.go`, and
`cache_latency_test.go`. All under `-race -shuffle=on -count=3`.

### 13.13 Per-block compression, measured against its CPU cost

Every data block until this section stored its entries exactly as written — no
smaller than the sum of every key and value in it, redundancy and all. Real records
(structured rows, log lines, serialized objects) usually carry far more repeated
literal text across entries than an incompressible workload ever would, and
DEFLATE-family compression is very good at removing exactly that kind of
redundancy. This section adds it, per block, and measures what it actually costs
against what it actually saves — the two numbers a compression decision is
worthless without both of.

**The block layout gained a one-byte marker, not a version field.** A data block is
now `CompressionType(1B) + entries (raw or compressed) + BlockCRC32(4B)`, where the
CRC covers the type byte and whatever follows it together. **This is a breaking
change to the on-disk format**, the same category of change the original per-entry
type byte was (§13.2) — but unlike that one, made after real files already existed
in this project's own test suite, not before. It is safe here for the same reason
any breaking format change is safe in an actively-developed, not-yet-deployed
engine: nothing depends on an old file surviving a format change, and every test
that builds one rebuilds it fresh. A real deployment migrating live data across
format versions would need a version field in the footer to tell old and new files
apart at `Open` time — recorded as an open question rather than built here, since
nothing in this project has ever needed to open a file across a format boundary yet.

**flate, not gzip or zlib — the reason is this format's own CRC.** Both gzip and
zlib wrap DEFLATE in their own container, complete with their own header and (gzip's
case) their own CRC32. Redundant here: every block already carries a `BlockCRC32`
over its full on-disk bytes, compressed or not, so a self-checksumming compression
container would mean paying for two checksums covering overlapping bytes.
`compress/flate` is the raw DEFLATE stream with no such wrapper — the same "use the
stdlib primitive, build only the structure around it" precedent `hash/crc32` and
`hash/fnv` already set (§13.1, §13.5), extended here to picking the leanest stdlib
variant of a whole family rather than the most convenient one.

**CRC verification happens on the on-disk bytes, before decompression is ever
attempted — never the other way around.** Corruption happens to whatever bytes are
physically on disk, which are the compressed ones once compression is in play;
checking those first, and only decompressing once they're known-good, means this
package never hands an unverified byte stream to a decompressor. A decompressor fed
adversarial or corrupted input is exactly the kind of component a format like this
should never trust blindly, which is also why `decompressFlate` enforces
`maxDecompressedBlockSize` (64× `targetBlockSize`) — a defensive bound against a
CRC-valid-but-adversarial stream claiming to expand to something unbounded, the
classic "decompression bomb" shape, not a limit any block this package's own
`Write` produces would ever approach.

**Compression is skipped, even when requested, if it doesn't actually help.**
`finalizeBlock` compresses first, compares sizes, and falls back to
`CompressionNone` whenever the compressed form isn't strictly smaller — a block of
already-compressed or near-random data can come out of flate *larger* than it went
in, and decompression has a real CPU cost a reader would otherwise pay on every
future `Get` for no benefit at all. `TestFinalizeBlockFallsBackToUncompressedWhenItDoesNotHelp`
checks this directly against genuinely random input.

**`Write` is completely unchanged; `WriteCompressed` is new — the identical "leave
the simple existing path alone" precedent `OpenWithCache` set over `Open` (§13.12),
now applied to the write side.** Every existing caller of `Write`, across every
package that already uses it, keeps writing the same bytes it always has. Reading a
compressed file needs no new API at all: `Open`, `OpenWithCache`, `Get`, and
`Iterator` all handle a compressed block exactly like an uncompressed one, because
`verifyAndSplitBlock` absorbs the decompression transparently before
`decodeBlockEntries` ever sees the bytes — `TestOpenReadsCompressedFilesWithoutAnySpecialAPI`
checks this against a full sequential scan and a cached `Get` both, not just a plain
point lookup.

**Measured**, `TestCompressionSpaceSavingAgainstCPUCost`: 5,000 keys, each with a
moderately redundant, JSON-shaped value (repeated field names and boilerplate text,
varying only a few fields per record — the shape of a real structured log line or
serialized row, not padding or random bytes), flushed once uncompressed and once
with flate, then read back 20,000 times against each (no block cache, so
decompression's cost is paid on every single `Get` rather than diluted after the
first touch of each block):

| | uncompressed | compressed (flate) |
|---|---|---|
| file size | 810,514 B | 63,597 B |
| space saved | — | 92.2% |
| write (compress) duration | 10.816ms | 29.313ms |
| read latency, mean | 1.878µs | 7.072µs |
| read latency, p99 | 12.542µs | 41.625µs |

Measured on the project owner's own machine. File size and space-saved percentage
matched my own sandbox run exactly, as expected (deterministic — see below). Write
duration was roughly 2.7x slower compressed; read latency roughly 3.8x slower on
mean and 3.3x slower on p99 — a real, measurable CPU cost on both paths, smaller in
absolute terms than my own sandbox's ratios (which ran closer to 4-5x on both) but
the same direction and the same order of magnitude, consistent with ordinary
machine-to-machine variance in a real-time measurement rather than a discrepancy
worth chasing.

File size and space-saved percentage are deterministic (the same "no timing
involved" argument §13.11's amplification measurement and §13.12's hit-rate numbers
already make) and are asserted directly in the test — at least 20% saved on this
workload, or the test fails outright. Write and read *durations* are logged only,
not asserted against a hard threshold, for the identical reason every other latency
measurement in this project draws that line: real time depends on the machine
running it.

**Implemented** at `internal/storage/sstable/compress.go` (`CompressionType`,
`compressFlate`, `decompressFlate`, `maxDecompressedBlockSize`), the compression-
aware `finalizeBlock`/`verifyAndSplitBlock` in `block.go`, `WriteCompressed` and the
shared `write` helper in `writer.go`, `FlushCompressed` in `flush.go`,
`compress_test.go`, `compression_test.go`, and `compression_measure_test.go`. All
under `-race -shuffle=on -count=3`.

---

## 14. The state machine

Everything in §2 through §12 is agreement; everything in §13 is a storage engine that
has, until now, only ever been exercised by tests calling it directly. Nothing has
ever connected the two. apply.go's own doc comment has said what belongs here since
before §13 existed at all: *"Everything below this line is agreement; everything
above it is the key-value store."* This section is that layer, finally built:
`internal/kvstore`, a real state machine attached to a real `raft.Node`'s `ApplyCh`,
backed by the storage engine end to end rather than a map.

**Not a hypothetical gap — a literal one, named in the code.** `e2e_test.go`'s own
`kvMachine`, present since this project's very first Raft phase, carries this exact
comment on its command encoding: *"A throwaway encoding, deliberately. Phase H
brings a real one; giving this test a serious codec now would mean writing it twice
and inviting the second one to be a copy of the first."* This is Phase H.
`internal/kvstore` never imports `internal/raft`'s test files and `internal/raft`
never imports `internal/kvstore` at all — the new package is a consumer of the
public `raft.Node` API (`Submit`, `ApplyCh`, `ReadIndex`, `ReadLease`,
`SnapshotNotify`, `Snapshot`), exactly as any other caller of that API would be,
with no special access whatsoever. Everything this section describes was built by
reading that public surface and `e2e_test.go` / `installsnapshot_test.go`'s own
existing test harnesses for precedent, not by reaching into Raft's internals.

### 14.1 A real command codec, replacing the throwaway one

`codec.go` gives `Put` and `Delete` a real wire encoding —

```
Put:    [opType(1B)][keyLen(4B)][key][valueLen(4B)][value]
Delete: [opType(1B)][keyLen(4B)][key]
```

— the same length-prefixed, opaque-payload shape the WAL (§13.1) and SSTable block
entries (§13.2) already use, for the identical reason both give: a key or value is
opaque as far as this format is concerned, so a length prefix is the only delimiter
that cannot collide with content. A Delete carries no value field at all, not a
zero-length one, matching the same distinction §13.2's data blocks draw between "no
value" and "empty value" — an empty string is a legitimate value a client might
actually `Put`, and conflating it with "this is a delete" is exactly the read bug
every layer below this one was already built to avoid.

### 14.2 The memtable-swap open question, finally closed

Since §13.3, every subsequent §13 section's own open questions have repeated some
version of the same sentence: *"nothing yet swaps a full memtable out from under
live writes."* `Machine.freezeAndFlushLocked`, called from the apply path once
`(*Memtable).ApproxSize` crosses a configured threshold, is that sentence closed —
close the active WAL, flush the frozen memtable to a new *compressed* SSTable
(§13.13, on by default: this is the first place every piece built across §13 runs
together as one real pipeline, and shipping it with compression on is the honest way
to exercise that pipeline as it actually runs, not a stripped-down demo of it),
record the new file in the manifest's L0 (§13.8, newest-first — a fresh flush is
always the newest thing that exists), delete the now-redundant WAL, and start a
fresh memtable, WAL, and `Writer` as the new active target. `TestFlushTriggersOnceTheActiveMemtableExceedsThreshold`
checks this directly: a low threshold, a hundred writes, at least one file
materializes in L0, and every value — written before or after the flush — still
reads back correctly through it.

**This runs synchronously, in the apply path — a real, named simplification, not an
oversight.** While a flush is in progress, no further command can be applied, and a
concurrent `Get` blocks behind the same mutex the flush holds. A background flush
goroutine, mirroring `compaction.Background`'s own shape (§13.9), is real future
work, recorded as an open question rather than attempted in the same task that
closes the swap-the-memtable question itself — two large changes at once was the
wrong scope for either one.

`compaction.Background` (§13.9) runs alongside, started by `NewMachine` against the
same manifest and directory, draining whatever `freezeAndFlushLocked` produces
independently of the apply loop. `TestBackgroundCompactionDrainsL0AfterEnoughFlushes`
is the test that proves these two independently-built pieces actually interoperate,
not just coexist: enough flushes to cross `MaxFilesPerLevel` leaves L1 holding a
real compacted file, with every current value still correct once the read path is
serving data that has been rewritten underneath it.

### 14.3 Reads: both linearizable paths, against the real read path

`Machine.Get` is the safe path — `n.ReadIndex()`, a wait for that barrier to apply
at the term it was issued under, then a local read via a freshly-constructed
`engine.Reader` (§13.6) over the current active memtable and every open SSTable
reader, reloaded from the manifest on every call so a compaction that finished a
moment ago on a *different* goroutine is never missed. `Machine.GetLeaseRead` is the
lease path (§9) — no round trip, correct only under the bounded-clock-drift
assumption that whole section already argues for. Neither method ever runs on the
goroutine that consumes `ApplyCh` — the one way to deadlock this, per `read.go`'s own
warning, and the reason `Get`/`GetLeaseRead` are ordinary exported methods any
caller's own goroutine can call, never the apply loop's.

**Reloading the manifest on every read is a real, unmeasured cost, recorded rather
than optimized away.** `compaction.Background` can change the manifest at any
moment, independent of this Machine's own apply loop, so there is no cheaper moment
to notice a change than checking on every read. A change-notification from
`Background`, rather than a reload on every call, is the natural next step and is
left for later.

**Applied-term bookkeeping is windowed by index, not pruned at snapshot time — a
design that was tried, found wrong, and replaced before shipping.** A caller waiting
on a specific `(index, term)` pair (the claim-ticket protocol `Submit` and
`ReadIndex` both use) needs to know the term a *specific* index carried, not just the
most recent one. Pruning that map down to "entries above the current snapshot floor"
the moment a snapshot is taken sounds airtight — no future barrier can land at or
below a covered index — but is not: a barrier issued a moment before a snapshot,
still waiting when that snapshot's own pruning runs, would find its own index's term
already deleted, even though it truly was applied at a real, correct term. Not a
wrong answer (a missing entry is treated as "unknown, retry," never fabricated) but a
spurious failure a genuinely successful read had no reason to suffer.
`maxAppliedTermsWindow` (10,000 entries, trimmed by index on every apply) sidesteps
the timing question entirely: it has nothing to do with when a snapshot happens, only
with how far behind the current applied index a lookup is, and a barrier checked
promptly — the only kind `ReadIndex`'s own protocol describes — always finds its
entry.

### 14.4 Snapshots: a logical image, not a physical one — a choice, not a default

`take` (answering `n.SnapshotNotify()`) and `installSnapshot` (answering a
`SnapshotValid` `ApplyMsg`) implement Raft's existing snapshot contract exactly as
`installsnapshot_test.go`'s own `snapshotMachine` already does for its map — encode
the whole state, hand it to `n.Snapshot(index, data)`; decode a received image and
*replace* local state with it, never merge, per Rule 8. What differs is what "the
whole state" means once it is not a map.

A snapshot image is the full live key set at the applied index it covers, as a flat
sequence of length-prefixed key/value pairs, produced by `sstable.Merge` (§13.8) over
the active memtable's iterator and every open SSTable reader's iterator, **with
tombstones dropped**: a snapshot is ground truth as of its index, so a deleted key is
correctly just absent from it, never present-as-a-tombstone. This is a **logical**
snapshot, not a **physical** one, and that was a choice made explicitly, not the
only design available. A real production LSM engine typically checkpoints by
referencing its own on-disk SSTable files directly — RocksDB's own checkpoint
mechanism hard-links the current manifest's files rather than re-serializing their
contents, far cheaper for a large, mostly-immutable dataset. That approach needs
`InstallSnapshot`'s own RPC to carry file references or chunked file bytes rather
than one opaque blob, which this project's Raft layer does not do yet — "chunking
`InstallSnapshot`" has been an open question since before this package existed.
Building that now would have meant redesigning the snapshot RPC in the same task
that wires in a state machine for the first time — two large changes at once. The
logical form built here works within Raft's existing, completely unmodified
contract; the physical form is real, deferred work, recorded as an open question
rather than solved by default.

**Installation replays through the ordinary write path — not a second, snapshot-only
mechanism.** `installSnapshot` wipes exactly the files this `Machine` itself
tracked (never a blanket directory removal, so anything else sharing the same disk
is never at risk), then decodes the image by calling a fresh `Writer.Put` once per
entry — the identical WAL-then-memtable durability sequence a live client write
takes (§13.7's whole delete-is-a-write argument, extended here to
"install-is-a-write," for the same underlying reason: nothing about *how* data
arrived changes what durability it needs). `TestSnapshotTakeAndInstallRoundTrips`
builds a real image from one `Machine`'s live state and installs it into a
completely separate one, confirming every key reads back correctly — checked by
reading the reconstructed storage state directly, deliberately bypassing `Get`'s own
Raft read-barrier protocol, because the two `Machine`s in that test are attached to
two independently-running, unrelated `raft.Node`s whose own index spaces were never
going to line up, and what the test is actually checking (did installation correctly
rebuild the storage engine) has nothing to do with Raft's read protocol at all.

### 14.5 Restart, and the first time every recovery path in this project runs together

`NewMachine`'s startup sequence is `compaction.Recover` (§13.10, cleaning up any
orphaned files from a crash mid-compaction and validating every SSTable the manifest
still claims exists), then `engine.RecoverMemtable` (§13.7, replaying whatever the
active WAL still holds into a fresh memtable) — composed with `raft.OpenNode`'s own,
completely separate recovery of Raft's own log and persistent state (§5). Three
independently-built recovery mechanisms, none of which had ever been exercised
alongside the other two before `TestRestartRecoversAllAppliedState`: stop a
`Machine` and its `Node` mid-run, after enough writes to have triggered at least one
flush, open fresh ones against the exact same on-disk directories, and confirm every
value — including a delete issued before the stop — is exactly as it was.

### 14.6 A real, if minimal, running program

`cmd/helios/main.go` is no longer the one-line stub it was: a real `raft.Node`,
opened against real `FileStorage`, with a real `Machine` attached, running until a
shutdown signal arrives. **Single-node only, deliberately, as a limit of the
project's current scope, not of this command.** Raft's own `Transport` interface has
never had a real network implementation — every multi-node test in package `raft`
runs against an in-memory fake transport built for testing, and a gRPC API,
multi-node deployment, and chaos testing are explicitly later, unreached phases of
this project. A no-op `Transport` is therefore the only honest choice available for
a cluster of exactly one node, which never has a peer to reach — not a shortcut
standing in for a real transport this command chose not to build.

Run and stopped cleanly by hand, twice in a row against the same data directory,
confirming what the tests already prove more rigorously: the second run picks up
exactly where the first left off, on disk, for real, not simulated.

### 14.7 What this section deliberately leaves open

Recorded here rather than left implicit: no client-facing network API exists yet
(no gRPC, matching the project's own stated roadmap); the flush that closes §14.2's
open question runs synchronously rather than in the background; a read reloads the
manifest on every call rather than reacting to a change notification; snapshots are
logical, not physical, and `InstallSnapshot` still has no chunking; and a Machine's
own configured constants (`FlushThresholdBytes`, `CompactionInterval`, the 64MB
block cache `cmd/helios` picks) join `targetBlockSize`, `bitsPerKey`, and the rest of
§12's list of "asserted, not yet measured against a real workload" defaults. One more
was found only after this section shipped, at real scale — restart redoing more work
than it needs to, §14.9's own finding, marked HIGH PRIORITY in §12 rather than folded
in quietly alongside the rest.

**Implemented** at `internal/kvstore/` — `codec.go` (`encodePut`, `encodeDelete`,
`decodeCommand`), `snapshot.go` (`encodeSnapshotImage`, `decodeSnapshotImage`),
`machine.go` (`Machine`, `NewMachine`, the apply loop, `freezeAndFlushLocked`),
`read_snapshot.go` (`Get`, `GetLeaseRead`, `Put`, `Delete`, `take`,
`installSnapshot`) — and `cmd/helios/main.go`. Tested: the codec's round trip and
every malformed-input case; a real single-node `raft.Node` (built from scratch for
these tests, using nothing but the public `Transport` and `OpenNode`/`FileStorage`
API, since Raft's own multi-node test harness is unexported and internal to package
`raft`) driving `Put`/`Delete`/`Get`/`GetLeaseRead` through 300 real operations
including overwrites and deletes; the flush trigger; background compaction actually
interoperating with it; snapshot take-and-install; and restart recovery. All under
`-race -shuffle=on -count=3`.

### 14.8 The full-system test: one million keys

Every test in §14.1–14.7 proves the pieces work — at a scale small enough to run in
seconds, as part of the ordinary `-race -shuffle=on -count=3` gate. None of them
prove the system holds together at a scale where every mechanism actually has to do
real work: enough writes to flush dozens of times, enough data for compaction to
matter, enough total bytes that a bug hiding in "this only shows up once you're past
the first few files" has somewhere to hide. `TestFullSystemOneMillionKeys`
(`internal/kvstore/fullsystem_test.go`) is that test — one million distinct keys,
written through a real single-node `raft.Node` and the full storage engine beneath
it, 1% of them deleted, all of them verified, the whole system stopped and reopened
from disk, and verified again.

**Gated behind `testing.Short()`**, the identical convention this project's own
testing philosophy has followed since the Raft phase: *"the right response to suite
growth is `testing.Short()` tiering, not deletion."* It never runs as part of the
ordinary gate; run it explicitly:

```
go test ./internal/kvstore/... -run TestFullSystemOneMillionKeys -v -timeout 2h
```

**A generous timeout is not paranoia.** `DefaultOptions` uses `wal.SyncAlways`
(§13.1) — every applied write fsyncs before `Machine.Put` returns, and application
happens on exactly one goroutine (the apply loop, §14), so the write phase's total
time is fundamentally bounded by one million sequential fsyncs *regardless* of how
many goroutines are pipelining `Submit` calls into it (64, in this test — real
concurrency, since `Submit` itself returns as soon as an entry reaches the log,
`submit.go`'s own doc, well before it applies). That is not a limitation this test
works around; it is the honest cost of the durability guarantee `DefaultOptions`
makes, and a weaker sync policy here would mean the test was no longer actually
checking what it claims to.

**Raft's own snapshotting is disabled for this test** (`SetSnapshotThreshold(0)`),
deliberately, not as an oversight. §14.4 already documents why a snapshot here is
*logical*, not physical: `take` re-encodes the entire current live key set into one
blob on every cycle. At this scale, that cost grows with the live set across the
run — left enabled at Raft's own default threshold (2,000 entries), it would fire
roughly 500 times over the course of one million applies, each one progressively
more expensive as the live set grows toward it. What this test is actually
exercising is the storage engine's *own* restart path — `compaction.Recover`
(§13.10) and WAL replay (§13.7's `RecoverMemtable`) — which needs no Raft snapshot
to have ever been taken at all; `TestSnapshotTakeAndInstallRoundTrips` already
checks the Raft-snapshot path in isolation, at a scale that doesn't confound it with
this cost.

**Verification uses the lease read path, not the safe one, for all one million
checks — with a small sample of the safe path checked separately.** `Get`'s
barrier-based protocol (§14.3) appends a real log entry per call; doing that a
million times would double the log for no benefit the correctness claim actually
needs. `GetLeaseRead` exercises the same underlying storage read path at full scale
with no log growth; `fullSystemSpotCheckSafeReads` checks 200 keys, evenly spread
across the key space, through `Get` specifically, confirming the two paths still
agree without paying for a million barrier entries to prove it everywhere.

**Measured, on the project owner's own machine — at 300,000 keys, not the full one
million, and that reduction is itself part of the finding.** A first attempt at the
full one million keys was run, watched, and deliberately killed after roughly 19
hours — not because anything was broken (every signal available, real syscalls, a
coherent manifest, growing on-disk state, confirmed it was doing real work the whole
time) but because the trajectory made clear that completing it would take multiple
days, driven by the compounding compaction cost §14.10 already names. That attempt's
own findings are recorded in full in §14.12, including a striking, precise
confirmation of the two-stacked-fsync diagnosis that only showed up once a real
restart at real scale actually ran. `HELIOS_FULLSYSTEM_KEYS=300000` — the same test,
the same code path, `-timeout 0`, run to completion rather than killed — is what
produced the table below.

| phase | duration | rate |
|---|---|---|
| write (300,000 puts) | 4h15m54s | 19.54 puts/sec (final instantaneous; started at 102/s, decayed continuously — see §14.12) |
| delete (3,000 deletes) | 4m42s | 10.62 deletes/sec |
| read-back, pre-restart | 4.86s | 61,723 gets/sec (lease read path) |
| restart — setup | 449ms | — |
| restart — full catch-up (ApplyCh redelivery) | 11m49.7s | 426.96 entries/sec (303,000 entries) |
| read-back, post-restart | 4.77s | 62,896 gets/sec (lease read path) |
| on-disk size, before restart | 26.3 MB (6 files) | — |
| on-disk size, after restart | 41.5 MB (5 files) | — |
| total test wall time | 4h33m | — |

Correct on every one of 300,000 keys, both before and after restart, including the
3,000 deleted keys correctly reporting absent both times. `Fault()` empty throughout.

### 14.9 A real finding from building this test: restart redoes more work than it needs to

**This is not a correctness bug — checked directly, not assumed.** Every value read
back after a restart, at every scale this project has tested, including the small
sanity run that surfaced this, is exactly correct. What was found instead is that
restart currently costs more than it should, in a way invisible at the scale
§14.1–14.7's own tests run at and unmissable at this section's.

`lastApplied` is volatile Raft state (§5's persistent/volatile distinction) and
resets to zero on every restart. Nothing currently tells a freshly-reopened
`raft.Node` "the state machine already durably reflects everything up to index N" —
so once a restarted node re-establishes leadership (committing its own current-term
entry, which by Log Matching transitively commits everything beneath it — the exact
mechanism `read.go`'s own barrier argument already describes for a different
purpose), `ApplyCh` redelivers *every* committed entry from the beginning, on top of
whatever `compaction.Recover` and `RecoverMemtable` already reconstructed from disk.
Put and Delete are idempotent under replay in the same order, which is exactly why
this produces the *correct final value* every time — and exactly why
`TestRestartRecoversAllAppliedState`, run at a scale where "twice as much work" is
still fast, never caught it. A small sanity run of this section's own full-system
test, at 5,000 keys rather than one million, made it directly visible: on-disk size
roughly doubled across a restart in which nothing new was written, because the
entire applied history had been durably re-written to the fresh WAL a second time.

**The fix needs the storage engine to durably know which Raft index its own
recovered state already corresponds to** — and the cheapest *correct* way to record
that, without adding a second fsync to every single write (which would undo the
entire point: a write path already paying for one fsync per apply has no room to pay
for two), is not a small change. It was not attempted here, under time pressure, in
the same task that found it — deliberately, on the same "a correct earlier design
should not be reversed by pressure to ship a fix quickly" discipline this project has
applied to every other significant finding. Recorded as a new, high-priority open
question (§12) rather than patched provisionally.

**Practical consequence for §14.8's own numbers**: expect the restart phase to take
roughly as long as the original write phase, not the near-instant recovery a
correctly-scoped restart would achieve. This is stated plainly in the test's own
output (`fullsystem_test.go`'s own `t.Logf` note alongside the restart duration) so
whoever runs it is not left wondering whether the number they are looking at is a
mistake.

### 14.10 A second real finding: every write crosses two fsync boundaries, and
concurrency doesn't help the way this section originally assumed

**The first real attempt at §14.8's full one-million-key run, on real hardware,
hit its own two-hour timeout at roughly 19% complete.** Not a deadlock — the
`go test` timeout's own goroutine dump is the proof, not an assumption: dozens of
writer goroutines queued on two ordinary mutexes (this package's own `Machine.mu`,
and `raft.Node`'s own `n.mu`), with the log index each one was waiting on climbing
steadily across the dump rather than frozen at a single value. That is what
contention behind one serialized bottleneck looks like; a genuine deadlock would
show every index frozen and at least one cycle of locks each waiting on the other,
neither of which appeared.

**The real cause: every applied write pays for two separate, sequential, lock-held
fsyncs, not the one this section's original write-up accounted for.**
`raft.Node.Submit`'s own `appendChecked` holds `n.mu` for the full duration of
`persistIfDirty`'s synchronous fsync of Raft's persistent log (`submit.go`, §5) —
already a known, pre-existing open question in this project (§12: `persistIfDirty`
under `n.mu` blocking group commit, measured a 25× potential gain from batching,
long before this task). Separately, `Machine.applyCommand` holds its own `Machine.mu`
for the full duration of the storage engine's own WAL fsync (`wal.SyncAlways`,
§13.1). Two fully independent locks, two fully independent fsyncs, both held
across the actual disk write, both on the path of every single Put — and neither
one is small on real hardware. A first attempt at estimating this section's own
expected runtime accounted for one fsync, not two, and the true per-write cost
turned out to be far higher than that estimate.

**This is also why the original 64-goroutine writer pool in §14.8 was itself a real
mistake, corrected before this section shipped rather than left in.**
`fullSystemWritePhase`'s original doc argued that concurrent `Submit` calls would
pipeline real throughput, since `Submit` "returns as soon as the entry is in this
node's log" (`submit.go`'s own doc) rather than waiting for replication or
application. That argument is true as far as it goes and still misses the actual
bottleneck: `Submit` is not cheap to call concurrently, because `appendChecked`
holds `n.mu` for its own synchronous fsync before ever returning — so every
concurrent caller queues behind that one lock regardless of how many are trying at
once. Measured directly, not just reasoned about: a sanity run at 8 writers reached
the identical throughput (409 puts/sec) a first run at 64 writers had reached
(405 puts/sec) — concurrency purchased nothing. The pool was reduced from 64 to 8,
not for correctness (both were already correct) but because a pool of 64 produces
a nearly illegible goroutine dump on any future timeout for zero benefit, while
8 stays small enough to read and still isn't purely sequential.

**A path to a real fix exists, and is directly connected to §14.9's own finding,
but neither was attempted here.** If restart is eventually fixed to durably track
which Raft index the storage engine's own recovered state already reflects (§14.9),
that finding's own logic runs in reverse too: Raft's persisted log is arguably
*already* sufficient durability for every applied command, which would make the
storage engine's own per-write WAL fsync redundant rather than load-bearing, at
least for crash-recovery purposes specifically (its role in building an efficient,
flushable in-memory memtable stage would remain). Fixing both together could
plausibly remove one of the two fsync boundaries entirely rather than just
batching each independently. Recorded as a further open question (§12), explicitly
connected to §14.9's rather than a new, unrelated one — not attempted under the
time pressure of the task that found it, for the identical reason §14.9's own fix
wasn't.

**`HELIOS_FULLSYSTEM_KEYS` was added to §14.8's own test as a direct result of this
finding**, not part of its original design: an environment-variable override (not a
test flag, not a second exported constant — loud enough not to be forgotten) that
runs the exact same code path at a caller-chosen scale, letting a real throughput
number be measured on whatever machine the full run will happen on before
committing hours to it, rather than trusting an estimate a second time.

**Reducing the writer pool from 64 to 8, measured directly rather than just argued
for, turned out to help more than the argument alone predicted.** §14.10 reasoned
that concurrency couldn't purchase real throughput here, since every `Submit` queues
behind the same lock-held fsync regardless of how many callers are trying at once —
true, but incomplete. A real run at reduced scale (20,000 keys) with the corrected
8-writer pool reached 77.19 puts/sec, against the original 64-writer run's own
observed 26.34 ops/sec (extrapolated from its timeout's own goroutine dump) — nearly
3x faster, not merely unchanged. Fewer goroutines contending for the same serialized
resource reduced real scheduling and lock-acquisition overhead on top of removing
the throughput benefit that was never actually there; the original 64-writer design
was not just failing to help, it was actively costing something.

### 14.11 A third real finding: the test's own restart measurement was wrong, not
just the system it measured

**§14.9 documents a real system finding — restart reapplies the entire committed
history. §14.11 is a separate, second finding, in this test's own code, not the
system it tests: the test was not correctly MEASURING that cost, and a real
20,000-key run failing is what surfaced it, not review.**

`NewMachine` starts the apply loop with `go machine.run()` and returns immediately;
it does not wait for Raft's redelivered backlog to finish reapplying. §14.8's first
version measured "restart" as the time until `NewMachine` returned — which only ever
captured the fast, synchronous half of a restart (`compaction.Recover`, WAL replay)
while the slow half (the apply loop working through everything §14.9 says Raft
redelivers) ran invisibly, overlapping with whatever the test did next: its own
post-restart verification. At real scale, that produced exactly the failure a
premature verification phase would: 144 of the first `GetLeaseRead` calls timed out
waiting for their own read barrier, because the apply loop was still minutes behind
and `defaultReadTimeout` (5 seconds, `read_snapshot.go`) was nowhere near long
enough to wait it out. **Every one of those reads was correctly refusing to answer
from state that hadn't caught up — not a correctness bug. A timing bug in the test's
own phase ordering, not in `Machine`.**

Fixed by capturing `AppliedIndex()` from the first `Machine` immediately before
closing it, then polling the second one after restart until it reaches that same
value — with a budget derived from the write and delete phases the same run already
measured (3x their combined duration, §14.9's own claim that catch-up costs roughly
what produced the backlog, checked directly rather than assumed) — before
verification is allowed to start at all. That wait is also what "restart" now
actually reports: confirmed at 20,000 keys, `setup: 316ms, full catch-up: 1m15s,
total: 1m16s` — a real, substantial cost, correctly measured and correctly waited
for, rather than a misleadingly fast number that was silently measuring the wrong
thing.

**Practical consequence for planning a full run**: extrapolate from the write phase
*and* the restart phase together, not the write phase alone — the earlier guidance
to budget "10-15 hours" based on write-phase extrapolation alone understated the
total, since the restart phase's real cost was invisible until this fix existed to
measure it.

### 14.12 The killed one-million-key attempt, and what a completed 300,000-key run
confirmed about all three prior findings

**A real attempt at the full one million keys was made, watched to completion of
its 19th hour, and deliberately killed — not because anything was wrong, but
because the trajectory made the true cost visible for the first time.** Every
external signal checked during that run (a process sample showing genuine syscalls
— `write`, `fcntl`, `rename` — not pure blocking; a coherent, valid `MANIFEST`; a
steadily growing on-disk footprint; roughly 23% average CPU utilization over 18h48m
of wall-clock time, consistent with a process genuinely waiting on serialized disk
I/O rather than stuck) confirmed the system was doing real work the entire time, not
hung. It was killed anyway, because at the observed rate, completing it would have
taken multiple additional days — a cost this task's own scope does not extend to
absorbing.

**A real gap in this test's own instrumentation was found and fixed as a direct
result of trying to observe that 19-hour run.** `t.Logf` output is buffered by the
testing framework and only flushes when the test *completes* — a test that runs for
19 hours and is then killed had produced, up to that point, precisely nothing
quotable, regardless of how much real progress it had made. `fullSystemProgress`
(`fullsystem_test.go`) was added to fix this: a periodic reporter using plain,
unbuffered `fmt.Fprintf` to stdout, mirrored to a fixed external log path outside
`t.TempDir()` (so it survives regardless of how the test process ends), reporting
completed count, elapsed time, instantaneous-since-start rate, and a naive linear
ETA every two minutes. Confirmed working immediately: a fresh attempt at
`HELIOS_FULLSYSTEM_KEYS=300000`, this time watched live rather than reconstructed
after the fact from `lsof` and manifest archaeology, is what produced §14.8's real
numbers.

**The write phase's own rate, watched continuously for the first time, decayed
smoothly and continuously across the entire run — not to a stable floor, contrary
to a real-time read taken partway through.** During the 300,000-key run, the
reported cumulative rate fell from 102.18/s in the first two minutes to 19.54/s at
completion 4h16m later. A live reading taken around the 40-46 minute mark, when the
*interval-to-interval* rate briefly held near 23-24/s for several consecutive
samples, was read in conversation as possible evidence of stabilization. **The full
curve shows this was not a stabilization — it was a brief flattening within a
longer decay that continued smoothly onward to 19.54/s by the end.** Recorded
plainly as a correction, not smoothed over: a short window of real data supported a
read that the complete run did not bear out, and the honest thing is to say so
rather than let the earlier, more optimistic reading stand uncorrected.

**The clearest and most precise confirmation yet of §14.10's two-stacked-fsync
diagnosis came from the restart, not the write phase.** Replaying 303,000 entries
(300,000 puts plus 3,000 deletes) via `ApplyCh` redelivery during restart took 709.7
seconds — 426.96 entries/sec, **21.85× faster** than the write phase's own final
rate of 19.54 puts/sec. This is exactly the shape §14.10's own argument predicts:
replay never calls `Submit`, so it never pays for `persistIfDirty`'s fsync — every
entry arrives already committed, straight onto `ApplyCh` — while it still pays the
storage engine's own WAL fsync in full, since `Machine.applyCommand` runs unchanged
either way. Removing one of the two fsync boundaries and keeping the other produced
a >20× speedup on the exact same kind of work. This is consistent with, not
definitive proof of, the two-fsync diagnosis specifically — replay also skips the
worker-pool scheduling and retry-loop overhead the write phase's own client-side
code carries, which a fully controlled comparison would need to isolate before
calling this conclusive. It is nonetheless the single most striking piece of
evidence gathered so far for why §12's connected open question (removing one fsync
boundary entirely, not just batching each) is worth pursuing.

**On-disk footprint grew across the restart despite the entry count staying fixed**
(26.3MB across 6 files before, to 41.5MB across 5 files after) — consistent with
compaction running again during replay, on the same live key set, and is not itself
surprising: a fresh `Machine` replaying through `Put` rebuilds its own SSTables from
scratch via the identical flush/compaction pipeline live traffic uses, so some
reshuffling of the physical bytes representing the same logical data is expected.

---

## 15. The gRPC API surface

### 15.1 The wire contract, and why it's five RPCs and nothing more yet

`api/proto/helios/v1/helios.proto` defines `helios.v1.Helios`: `Get`, `Put`,
`Delete`, `Scan` (unary), and `Watch` (server-streaming). Generated Go lives at
`api/helios/v1/` — deliberately **not** under `internal/`, the one place in this
project's layout where that matters: every other package can afford to be
unimportable from outside the module, but the client library a later task builds
has to import these generated types from outside `internal/`'s boundary, so the
public/internal line had to be drawn here before any server or client code
existed to get it wrong. This section records the schema; nothing serves or calls
it yet. `internal/kvstore.Machine` (§14) is untouched — this RPC layer will sit in
front of it, not replace anything about how it works.

### 15.2 Revision is Raft's own log index, not a new counter

Every response that names a value also carries a `revision`. It is not a new
piece of state: it is `commitIndex`/`lastApplied` (§6, §8) — already the
authoritative answer to "when did this write become durable and visible" —
exposed at the API boundary under a client-facing name. A second, independently
maintained MVCC version counter was the alternative not taken, for the same
reason §14.2's memtable-swap fix reused the apply path instead of adding a
parallel one: two mechanisms claiming to answer the same question is two things
that can disagree, and Raft's log index was already correct and already tested
before this task started.

### 15.3 Consistency is a per-request field, mapped onto reads already built and measured

`GetRequest` and `ScanRequest` each carry a `Consistency` enum —
`CONSISTENCY_LINEARIZABLE` or `CONSISTENCY_STALE` — rather than a connection- or
cluster-wide mode. This is not a new read path; it is a selector over the two
that §9 already implements and measured: the committed-index barrier read and the
lease read bounded by a documented clock-skew assumption. `CONSISTENCY_UNSPECIFIED`
(proto3's required zero value) resolves to linearizable once a server exists to
resolve it (§15.5) — the unset case defaults to the expensive, safe path rather
than the cheap one, so a client that forgets to set the field gets correctness,
not a silent staleness bug. A cluster-wide setting was rejected because it would
force every caller sharing one client connection to accept the same tradeoff; a
dashboard reader and a read-your-writes caller have no reason to.

### 15.4 Scan is unary with an opaque continuation token, not a stream

`ScanRequest` takes `start_key` (inclusive), `end_key` (exclusive), `limit`, and
`page_token`; `ScanResponse` returns a page of `KeyValue` plus `next_page_token`,
empty when the scan is exhausted. This task only commits to that shape — the
token's actual encoding is unimplemented, an explicit scope fence for a later
task ("Scan with pagination over the sorted key space") rather than a guess made
now and possibly thrown away. Server-streaming was the alternative, rejected
because a client-driven token gives the client control over page timing and a
clean, idempotent retry point for a single dropped page; a stream that dies
mid-scan has no such point without inventing the same token concept anyway, just
later and under worse conditions.

### 15.5 Watch: one prefix per stream, explicitly not etcd's multiplexed protocol

`WatchRequest` takes a `key_prefix` and a `start_revision` — zero means "start
from now," non-zero means replay committed changes from that revision forward
before switching to live delivery, which is what lets a reconnecting client
resume a watch without a gap. Cancellation is closing the stream; there is no
explicit create/cancel message. `WatchEvent`s arrive batched inside
`WatchResponse` rather than one per frame, the same amortization argument
already measured for AppendEntries coalescing (§10, replication). etcd's own
watch protocol multiplexes many watches over one bidirectional stream with
explicit watch IDs — a real design, deliberately not this one. That protocol
earns its cost at a scale of concurrent watches per connection this project has
never named as a target (it is not one of the measured numbers in §1's own
goals), and it is a second protocol surface that would need proving correct
under the fault injection this project has not yet reached (Phase G). Revisit if
a later phase's benchmark plan actually calls for it — not before.

### 15.6 What this task deliberately leaves for later tasks, not forgotten

**Idempotency.** No `client_id` or `sequence_number` field exists yet on `Put` or
`Delete`. Adding one later is a non-breaking, additive wire change, and this
project's own open-questions list (§12, "Duplicate commands") already names the
dedup-table design as unsettled — guessing its shape here, before that task
specs it, would mean redoing it. **The `NotLeader` error.** Not a field on any
response message. It will be carried as gRPC status detail metadata once a
server exists (the next task), which keeps every success-path message free of a
field that only means something on failure, and applies uniformly across all
five RPCs instead of five separately-invented `leader_hint` fields.
**`DeleteResponse`'s missing previous value.** It returns `found` (a bool,
mirroring `GetResponse.found`'s comma-ok shape) but not the value that was
deleted. etcd's `DeleteRange` supports this as an optional `prev_kv`; deferred
here the same way, as a same-shape additive field, until a real caller's
ergonomics actually need it.

**Implemented** at `api/proto/helios/v1/helios.proto` (schema) and generated
into `api/helios/v1/` via `buf` (config: `buf.yaml`, `buf.gen.yaml`) driving
local `protoc-gen-go` and `protoc-gen-go-grpc` — not `buf.build`'s remote
plugins, so generation has no dependency on network access beyond the two
`go install`s. Tested at `api/helios/v1/smoke_test.go`, hand-written and never
touched by regeneration: a marshal/unmarshal round trip for every message,
`GetResponse`'s zero value read as absent-not-empty, `Consistency`'s zero value
read as unspecified-not-a-real-choice, and a compile-time check
(`var _ HeliosServer = (*fakeServer)(nil)`) that `Watch` generated as
server-streaming rather than silently becoming unary if the `.proto`'s `stream`
keyword were ever dropped in a future edit.

## 16. Revision log


| Version | Change |

|---|---|
| v1 | Initial design: states, RPCs, persistent and volatile state |
| v1.1 | Majority-intersection argument for the up-to-date check; implementation decisions; the election-timer reset deviation recorded as an open question; volatile state expanded to match the implementation |
| v1.2 | Commit rules and the Figure 8 argument; leader bookkeeping; the fast-backup hint recorded as a deviation with measured round-trip savings; implementation decisions reorganised into log / timing / replication / bookkeeping; testing conventions added |
| v1.3 | The apply path documented for the first time; linearizable reads and the argument against local reads; the no-op entry flag recorded as a departure from Figure 2; AppendEntries rule 5 and the same-term refusal written up; believed-impossible guards collected as a single principle; fault injection, fixture-inertness and test-goroutine conventions added; duplicate commands and read-barrier cost promoted to open questions |
| v1.4 | Lease-based reads, with the clock-rate assumption and the process-pause residue documented; the send-time and quorum-ordering rules for deriving a lease; read latency measured across three link speeds, and the argument for the no-op on election revised in light of it from latency to log growth |
| v1.5 | Persistence implemented: the record format and its checksum, the write-temp / fsync / rename / fsync-dir sequence, the dirty-flag funnel and the three exits it guards, and the refusal to treat a corrupt record as a fresh node; the limits of SIGKILL as evidence stated explicitly; fsync policy measured across always / batch / never and written up in `docs/fsync-policy.md`, with the node's write path shown to be entirely fsync-bound and the compaction-first assumption withdrawn; replication coalesced to one round in flight per peer, measured at 2.6× throughput and 125× fewer messages; `kill` and `crash` distinguished as separate faults; reordering demoted from an asserted property to a reported one, with the guards it had been covering tested directly instead |
| v1.6 | Snapshotting and log compaction: the snapshot record and why `lastIncludedTerm` cannot be derived; the ordering rule between image and truncated log, and the asymmetry of the two crash windows; the state record carrying its own floor so a compacted log is not ambiguous; three-way reconciliation of the two floors on restart; `InstallSnapshot` written up as §4.3 with the no-chunking deviation; the index seam in `log.go` with its greppable invariant and fail-closed `termAt`; the compaction trigger measuring discardable entries rather than log length, with the 59-versus-9,453 measurement behind it; `Snapshot` guarded on `commitIndex` because `lastApplied` lags the caller by a mutex acquisition; image rebuilds throttled per peer; the restart-replay question closed and chunking, snapshot-during-replication and the `kvMachine` gap opened; the snapshot/AppendEntries interleavings pinned deterministically, with a same-term leader now refusing an image as it already refuses entries; recovery measured at image sizes up to a gigabyte, showing time is not the constraint and that both the encode and decode paths allocate a second copy of the image |
| v1.7 | On-disk formats for the LSM storage engine fixed on paper: the write-ahead log record framing and its CRC coverage, with the departure from LevelDB's block-fragmented WAL recorded and reasoned through; the SSTable data block, index block, and trailing footer laid out, with the footer-at-the-end decision argued from the engine's own write order; the boundary between Raft's persistent state (§5) and the engine's write-ahead log stated explicitly, since the two are separate durability islands answering different questions. The write-ahead log itself implemented at `internal/storage/wal/`, with the three sync policies, corrupt-record handling, and torn-tail recovery tested directly. Startup recovery added as `Recover`, distinct from a bare `Replay`, with the truncate-the-stale-tail step it performs and the reason skipping it would permanently hide every record written after a recovery that didn't truncate; proven with a test that deliberately corrupts a record's payload on disk, asserts recovery stops there cleanly, and then confirms a second, independent recovery pass sees a record appended after the first one — the assertion a truncation-free recovery would fail. SSTable encoding left as designed-but-not-yet-built. |
| v1.8 | The memtable implemented as a skip list (§13.4): lock-free `Get` and iteration against a single mutex-serialized writer, an atomically-swapped value-plus-tombstone pair per key so neither a splice nor an update can ever be observed half-done, and a per-instance seeded RNG for level selection on the same rule as Raft's election timer. Three structures — red-black tree, sorted slice, skip list — compared and the choice argued from which one lets a new node's publication be a single atomic pointer write. `Memtable` satisfies `wal.Sink` structurally, wiring the write-ahead log's recovery path to the memtable without either package importing the other. Concurrency argued from the memory model and then checked directly: concurrent reads alone, concurrent reads against a single inserting writer, concurrent reads against a single updating writer with byte-level torn-value detection, and concurrent iteration against a writer, all under `-race -shuffle=on -count=3`; a fifth test drives sixteen concurrent writers on disjoint keys through the same internal mutex production never contends, on the same "guarded, not assumed" reasoning §8 applies to Raft's own believed-impossible states. |
| v1.9 | The memtable flush path implemented end to end (§13.2, §13.3). SSTable data blocks, index, and footer built out at `internal/storage/sstable/`, closing the open question v1.7 left behind — including a per-entry type byte the original paper design was missing, added and argued for so a flushed tombstone can't be mistaken for a legitimate empty value once the WAL's own record-level discriminator is no longer available to lean on. `Write` streams a sorted source into 4KB-target data blocks and publishes the finished file with the same write-temp/fsync/rename/fsync-directory sequence `FileStorage` already uses for Raft's own state, refusing outright to overwrite an existing path. `Open`/`Get` binary-search the in-memory index and verify each block's CRC before trusting it, with the binary-search step later factored into its own `findBlock` and given dedicated boundary tests (exact `LastKey` matches, the gap between blocks, before-first and past-last). `(*Memtable).ApproxSize` closes the other half of the open question — key-and-value byte tracking added to `upsert`, replacing `Len`'s distinct-key count as the quantity a flush decision should be based on, on the same measure-the-real-quantity argument the Raft compaction trigger's own fix already established (§10). `sstable.FlushIfFull` is the thin trigger tying the two together: compares `ApproxSize` against a caller-supplied threshold and flushes if reached, deliberately stopping short of choosing a file path, swapping in a fresh memtable, or running in the background — all recorded as still-open work, alongside the block reader's linear in-block scan and the unmeasured 4KB block-size default. |
| v1.10 | A Bloom filter implemented and measured at `internal/storage/bloom/` (§13.5), standalone and not yet wired into the SSTable write or read paths. Kirsch-Mitzenmacher double hashing derives `k` probe positions from two base hashes; `k` itself chosen from `bitsPerKey` by the standard `k = bitsPerKey · ln 2` result, clamped to `[1, 30]`. The first hash construction tried — FNV-1 and FNV-1a as the two base hashes — measured 2x-10x the theoretical false-positive rate, worse at higher `k`, which is what pointed at correlated hashes rather than generic noise as the cause; fixed by deriving the second hash from the first through `mix64` (the splitmix64/MurmurHash3 finalizer) instead of hashing the key a second time with a related algorithm, the same one-real-hash shape LevelDB's own filter takes. Re-measured after the fix: observed false-positive rate within 3% of the theoretical formula — evaluated at the actual, rounded `n`, `m`, and `k` a filter is built with, not the simplified asymptotic approximation — at three widely spread bits-per-key settings (6, 10, and 14, the last two bracketing LevelDB's and RocksDB's shared default). Wiring the filter into a flush and a lookup, and designing where its bytes would live in the SSTable file format, both recorded as open questions rather than attempted here. |
| v1.11 | The read path across memtables and SSTables implemented at `internal/storage/engine/` (§13.6): `Reader.Get` checks the active memtable, then immutable memtables newest-frozen-first, then SSTables newest-flushed-first, stopping at the first tier reporting a live value or a tombstone. Two narrow interfaces mirroring `(*memtable.Memtable).Get`'s and `(*sstable.Reader).Get`'s exact signatures let the merge logic be tested against hand-built fakes for tombstone-shadowing at every tier and a simulated corrupt-SSTable error, plus an end-to-end test through real memtables and SSTables confirming the adapter wiring itself. A real bug was caught and fixed in the writing, not the code: two prior mentions of the target read-path order -- §13.4's sketch and one of §12's open questions -- said SSTables should be checked oldest-to-newest, backwards from correct; both corrected here, alongside an unrelated orphaned-bullet formatting break in §12 introduced while adding the Bloom filter section in v1.10. Bloom filter integration remains explicitly out of scope, recorded as still open. The read side of "more than one memtable and SSTable to search" is closed; the write side -- deciding when to freeze a memtable, swapping in a fresh one, and running a flush in the background -- is not, and is recorded as its own separate open question rather than assumed closed alongside this one. || v1.11 | The read path across memtables and SSTables implemented at `internal/storage/engine/` (§13.6): `Reader.Get` checks the active memtable, then immutable memtables newest-frozen-first, then SSTables newest-flushed-first, stopping at the first tier reporting a live value or a tombstone. Two narrow interfaces mirroring `(*memtable.Memtable).Get`'s and `(*sstable.Reader).Get`'s exact signatures let the merge logic be tested against hand-built fakes for tombstone-shadowing at every tier and a simulated corrupt-SSTable error, plus an end-to-end test through real memtables and SSTables confirming the adapter wiring itself. A real bug was caught and fixed in the writing, not the code: two prior mentions of the target read-path order — §13.4's sketch and one of §12's open questions — said SSTables should be checked oldest-to-newest, backwards from correct; both corrected here, alongside an unrelated orphaned-bullet formatting break in §12 introduced while adding the Bloom filter section in v1.10. Bloom filter integration remains explicitly out of scope, recorded as still open. The read side of "more than one memtable and SSTable to search" is closed; the write side — deciding when to freeze a memtable, swapping in a fresh one, and running a flush in the background — is not, and is recorded as its own separate open question rather than assumed closed alongside this one. |

| v1.12 | Tombstones and the argument for why a delete is a write consolidated at `internal/storage/engine/` (§13.7), alongside the one piece of code that was still missing to make the argument checkable end to end: `Writer`, the live write path. `Put` and `Delete` both append to the WAL before touching the memtable, in that order, every time -- the durability boundary a delete has to cross for the identical reason a put does, argued from two things already committed to elsewhere (the WAL's own durability contract, and SSTable/read-path immutability once more than one tier can exist) rather than as a new rule invented for deletes. Proved rather than only argued: a `Put` and `Delete` run through a `Writer`, the in-memory memtable that applied them is discarded outright to simulate a crash, and `RecoverMemtable` rebuilds the tombstone from the WAL alone. `RecoverMemtable` also closes a real, four-revision-old documentation bug -- §13.4 had claimed since v1.8 that a `wal.Sink` interface and a `wal.RecoverAndOpen` function existed and wired a Memtable to WAL replay; neither ever did, and both are corrected. Bloom filter integration and the write-side orchestration that would swap a full memtable out from under live writes remain explicitly open, tracked separately in §12. |
| v1.13 | Leveled compaction implemented end to end (§13.8): a manifest (`internal/storage/manifest/`) tracking which SSTable belongs to which level, persisted with the same write-temp/fsync/rename/fsync-dir sequence used everywhere else in this engine; `PickLevel` choosing the lowest over-threshold level; `sstable.Merge`, a k-way merge over several sorted `Source`s with a newest-first tie-break and an explicit, argued rule for when a surviving tombstone can finally be dropped instead of carried forward; `sstable.Iterator`, the sequential full-table scan `Get` was never built to do, needed to feed a compaction at all; and `compaction.Run`, tying all of it together with a crash-safety ordering (write the merged file, then swap the manifest, then delete superseded files -- checked directly by forcing the swap to fail and confirming nothing is deleted). Two real bugs were caught and fixed while building this, not left for later: `sstable.Source` gained an `Err() error` method after realizing `Write`'s original loop had no way to detect a source failing mid-scan, a gap invisible until an SSTable-backed (and therefore genuinely fallible) `Source` existed for the first time; and `Merge`'s own first draft checked a source's `Err` only for sources not yet marked done, which is precisely the sources whose failure it needed to catch, fixed by checking at the exact moment a source transitions to done rather than in a later pass. File-count compaction triggers, single-output-file compactions, whole-level (rather than overlapping-range) merges, and orphaned post-crash file cleanup are all recorded as open questions rather than solved here. |
| v1.14 | Background compaction implemented and measured (§13.9): `compaction.Background` runs pick-merge-write-swap cycles on a ticker, in their own goroutine, so a live `engine.Writer` never triggers or waits on one. No new locking was needed -- Writer only touches its own WAL and memtable, Run only touches SSTable files and the manifest, and `TestConcurrentWritesAndBackgroundCompactionProduceNoRaceOrCorruption` confirms this holds under `-race`, not just on paper. What running concurrently can still cost is the same physical disk, which `TestWriteLatencyWithAndWithoutBackgroundCompaction` measures directly rather than assumes away -- numbers pending a run on the user's own machine, the same discipline the Bloom filter's false-positive measurement (v1.10) already established, since write latency under real disk contention has no formula to check it against the way false-positive rate does. A real hazard was found and fixed before it could ship: `CompactLevel`'s one-output-file design means every level above L0 can only ever hold zero or one files, so genuine cascading is impossible with a sane threshold -- but `Options{MaxFilesPerLevel: 0}` was never rejected, and under it a single leftover file would get compacted one level deeper forever. `maxDrainCycles` bounds this to a reported error instead of an infinite spin, caught by reasoning through the design rather than by hitting it in production. `Background.Stop` deliberately waits for its goroutine to fully exit, unlike Raft's own fire-and-forget ticker shutdown, because callers -- tests, especially -- need the guarantee that no further compaction touches disk once Stop returns. |
| v1.15 | Manifest recovery implemented (§13.10): `compaction.Recover` closes the open question v1.13's manifest design and v1.14's background runner both left explicitly unsolved -- a crash between the manifest swap and Run's own cleanup step (§13.8) was already safe, never recoverable in the sense of a node actually reclaiming the wasted disk space or noticing something was wrong. Recover reconciles the manifest against what a directory actually holds: any unreferenced `.sst` file is an orphan and is deleted; any leftover `.tmp` file is deleted unconditionally, since sstable.Write's and manifest.Save's own atomicity means one can only exist if a process died mid-write, never as a legitimate artifact; a file the manifest references but disk is missing, or which fails to open as a valid SSTable, is reported as an error rather than silently tolerated, on the same believed-impossible-is-guarded-not-assumed posture §8 applies to Raft's own invariants. Proved against the exact crash window Run's own documentation names, not an easier one: CompactLevel and manifest.Save are called directly, stopping deliberately before the cleanup step a crash would have interrupted, and Recover is shown to finish exactly that interrupted work. Recover must be called once, explicitly, before Run or Background ever starts against the same directory -- a precondition stated in its own doc and not enforced by the type system, on the same explicit-recovery-step precedent engine.RecoverMemtable already set rather than a change to StartBackground's signature. |
| v1.16 | Write and space amplification measured across three compaction configurations and plotted (§13.11): `compaction.MeasureAmplification` runs a fixed, seeded 20,000-operation workload at MaxFilesPerLevel settings of 2, 4, and 8, and `cmd/ampplot` renders the result as a hand-written SVG bar chart (no third-party charting dependency added -- this project's go.mod has never had one). The measurement is fully deterministic, unlike write latency (v1.14), so the trade-off's direction is asserted as a hard test requirement rather than only logged. A genuine finding, not a flaw: final (fully-converged) space amplification came out identical across all three configurations, because CompactLevel's whole-level merge (§13.8) always reaches the same deduplicated steady state regardless of path; peak space amplification, sampled mid-run before compaction catches up, is what actually differentiates the configurations, and is what gets plotted. Two real mistakes were caught and fixed while building this: a workload-boundary artifact where a fixed operation count left the loosest configuration with an unmerged L0 leftover that skewed its final snapshot to falsely match the tightest configuration's; and the first attempted fix for that reproduced, in a new function, the exact MaxFilesPerLevel-0 cascade-forever hazard v1.14's Background.maxDrainCycles was built to guard against elsewhere -- caught before shipping, and replaced with a narrower, bounded single-compaction fix rather than a capped loop. Recorded as a new open question: one runtime guard in one place did not stop the same mistake from being written twice, an argument for validating this upfront rather than per-caller. Also fixed in passing: a stale `docs/fsync-policy.md` path in five live prose references (the file lives at the repository root); the v1.5 revision entry's own wording is left untouched as a historical record. |
| v1.17 | A block cache with LRU eviction implemented and measured (§13.12): `blockcache.LRU[K, V]`, a generic, byte-size-bounded cache built entirely from stdlib (`container/list`), wired into `sstable.Reader` via a new `OpenWithCache` that leaves the existing `Open` and every one of its callers completely unchanged. Keyed by (path, block index), which needs no invalidation logic at all -- an SSTable is immutable once published (§13.2) and compaction (§13.8) never edits one in place, so a cached block can never go stale, only become evictable. Two real bugs caught while designing the eviction rule, not after: a naive evict-until-under-budget loop would silently make Put a no-op for any single entry larger than the configured budget (a real case, since a data block is allowed to exceed targetBlockSize by one entry), fixed by never evicting the last remaining entry; and that same fix then left a `maxBytes: 0` cache ("cache nothing") holding exactly one entry regardless, fixed by handling a non-positive budget as its own case checked first. `TestCacheHitAvoidsTouchingDiskAtAll` is the test that actually proves a cache hit skips disk entirely rather than trusting the code path by inspection -- read a key once, truncate the file to zero bytes, confirm a second read for the same key still succeeds. Read latency measured with no cache and at three cache sizes (10%, 50%, 150% of one SSTable's cacheable bytes) against a fixed, seeded Zipfian access pattern -- numbers pending a run on the user's own machine, the same discipline every prior latency measurement in this project has followed, since real time depends on the machine it runs on; hit rate, which depends only on the deterministic seed and cache logic, is asserted directly rather than only logged. Two new open questions recorded: the cache's single coarse mutex, and its size being measured in the abstract rather than chosen for a real workload, joining `bitsPerKey` and `targetBlockSize` on the same still-open list. |
| v1.18 | Per-block compression implemented and measured against its CPU cost (§13.13): the data block layout gains a one-byte CompressionType marker ahead of its entries, CRC-verified on the on-disk (possibly compressed) bytes before decompression is ever attempted -- a real, breaking change to the on-disk format, made safely only because nothing in this actively-developed project depends on an old file surviving a format change. flate, not gzip or zlib, chosen specifically because this format already carries its own BlockCRC32 and a self-checksumming compression container would mean paying for two overlapping checksums. finalizeBlock compresses first and falls back to CompressionNone whenever the compressed form isn't strictly smaller, so a block of already-compressed or near-random data never pays a decompression cost for zero benefit. decompressFlate enforces a 64x-targetBlockSize decompression-bomb guard, defending against a CRC-valid-but-adversarial stream rather than anything this package's own Write could ever legitimately produce. Write is completely unchanged; WriteCompressed is new, the identical "leave the simple existing path alone" precedent OpenWithCache set over Open (v1.17) now applied to the write side -- and unlike that precedent, reading a compressed file needs no new API at all, since verifyAndSplitBlock absorbs decompression transparently before any caller (Get, Iterator, a cached Reader) ever sees the difference. Measured on a moderately redundant, JSON-shaped 5,000-key workload: space saved and file size are deterministic and asserted directly (at least 20% or the test fails); write (compress) and read (decompress) duration are logged only, pending a run on the user's own machine, the same discipline every prior latency measurement in this project has followed. Two new open questions recorded: the SSTable footer has no format-version field for a real cross-version migration, and flate's compression level is asserted at the standard library default rather than measured, joining targetBlockSize, bitsPerKey, and block cache size on the same still-open list. |
| v1.19 | The storage engine wired in as Raft's real state machine (new §14), replacing e2e_test.go's own kvMachine and its explicitly-named "throwaway encoding" -- that file's own comment has said "Phase H brings a real one" since before §13 existed. internal/kvstore is a pure consumer of raft.Node's public API (Submit, ApplyCh, ReadIndex, ReadLease, SnapshotNotify, Snapshot); neither package imports the other's internals. A real Put/Delete wire codec replaces the throwaway one. Machine.freezeAndFlushLocked closes the "nothing swaps a full memtable out from under live writes" gap every §13 section since v1.10 has repeated -- synchronously, in the apply path, a named simplification rather than an oversight, with a background flush goroutine recorded as its own separate open question. Both linearizable read paths (barrier and lease) run against a freshly-constructed engine.Reader, reloaded from the manifest on every call since compaction.Background can change it independently of the apply loop. Applied-term bookkeeping is windowed by index rather than pruned at snapshot time -- the first design tried (prune at the snapshot floor) was found to produce spurious read failures for a barrier issued just before a snapshot, and was replaced before shipping. Snapshots are a logical image (the full live key set via sstable.Merge, tombstones dropped) rather than a physical one referencing SSTable files directly -- a real, explicit design choice, not a default, argued in full in §14.4 and left as an open question rather than attempted alongside everything else in this task. Startup composes three independently-built recovery mechanisms (compaction.Recover, engine.RecoverMemtable, raft.OpenNode's own log recovery) for the first time. cmd/helios is a real, running, single-node program -- started, stopped, and restarted by hand against the same data directory, not just exercised by tests -- honestly single-node-only, since Raft's own Transport has never had a real network implementation. Tested end to end with a real single-node raft.Node built from scratch for these tests (Raft's own multi-node test harness is unexported and internal to package raft): 300 real Put/Delete/overwrite operations, the flush trigger, background compaction actually interoperating with it, snapshot take-and-install, and restart recovery, all under -race -shuffle=on -count=3. Five new open questions recorded in §12, closing one that has recurred since v1.10. |
| v1.20 | The full-system test built (new §14.8): TestFullSystemOneMillionKeys writes 1,000,000 distinct keys through a real single-node raft.Node and the full storage engine, deletes 1% of them, verifies all of them, stops and reopens the whole system from disk, and verifies again -- gated behind testing.Short() and run explicitly, not part of the ordinary gate, the same convention this project's testing philosophy has followed since the Raft phase. Verification uses the lease read path at full scale (no per-check log growth) with a 200-key spot-check through the safe barrier path. Raft's own snapshotting is disabled for the test, deliberately -- isolating the storage engine's own restart path (compaction.Recover, RecoverMemtable) from the growing cost of §14.4's already-documented logical-snapshot re-encoding. Deliberately not run in this sandbox -- the number belongs to whoever actually depends on it, the same discipline every other real-time measurement in this project has followed, applied with more force at this scale. A genuine finding surfaced while sanity-checking the test at reduced scale before delivering it, not left for the person running the real one to discover alone (new §14.9, marked HIGH PRIORITY in §12): restart currently reapplies the ENTIRE committed history a second time, on top of what WAL/SSTable recovery already reconstructs, because lastApplied is volatile Raft state that resets on every restart with nothing yet telling a freshly-reopened Node "the state machine already durably reflects everything up to index N." Checked directly, not assumed, to be correct on every value rather than merely wasteful in a way that also happens to be safe: Put and Delete are idempotent under replay in the same order, which is exactly why this was invisible at every smaller scale this project had tested restart at until now. The real fix -- durably tracking the applied-index high-water mark without adding a second fsync to a write path that already pays for one -- was not attempted under the time pressure of the task that found it. |
| v1.21 | A second real finding from §14.8's full-system test, surfaced by its actual first attempt at real scale (new §14.10): every applied write pays for two separate, sequential, lock-held fsyncs, not the one the test's original design accounted for -- raft.Node.Submit's own appendChecked holds n.mu through persistIfDirty's synchronous fsync (a pre-existing, already-documented open question, now confirmed at real-system scale rather than only in isolation), and Machine.applyCommand separately holds its own Machine.mu through the storage engine's own WAL fsync. Confirmed via the go test timeout's own goroutine dump, not assumed: dozens of writer goroutines queued on two ordinary mutexes with climbing (not frozen) indices -- the signature of contention behind one serialized bottleneck, not a deadlock. A connected design mistake corrected before the test shipped its second version: the original 64-goroutine writer pool assumed concurrent Submit calls would pipeline real throughput, which is false here -- Submit is not cheap to call concurrently, since appendChecked holds n.mu for its own fsync before returning. Measured directly: 8 writers and 64 writers reached materially identical throughput (409 vs 405 puts/sec), so the pool was reduced to 8 -- legibility on a future timeout's dump, not a throughput fix, since none is available without touching Raft's own persistIfDirty. HELIOS_FULLSYSTEM_KEYS added to the test as a direct result: an environment-variable override letting a real throughput number be measured, on whatever machine the full run will happen on, at a scale that finishes in minutes, before committing hours to the full one. Two new open questions recorded in §12: persistIfDirty's cost confirmed at full-system scale (connected to the pre-existing entry on it), and a further one connecting this finding to v1.20's own restart-replay finding -- fixing both together could plausibly remove one of the two fsync boundaries entirely, rather than only batching each independently; neither attempted under the time pressure of the task that found the connection. |
| v1.22 | A third real finding from getting §14.8's full-system test to run cleanly (new §14.11): the test's own restart measurement was wrong, not just the system it measures. NewMachine starts the apply loop with go machine.run() and returns immediately, so the first version's restartDuration only ever captured the fast synchronous half of a restart (compaction.Recover, WAL replay) while the slow half -- the apply loop's full catch-up through everything §14.9 says Raft redelivers -- ran invisibly, overlapping with the test's own post-restart verification. Surfaced by a real 20,000-key run failing, not by review: 144 of the first GetLeaseRead calls timed out waiting for their own read barrier, because the apply loop was still minutes behind and the 5-second default read timeout was nowhere near enough to wait it out. Every one of those reads was correctly refusing to answer from state that hadn't caught up -- confirmed to be a timing bug in the test's own phase ordering, not a correctness bug in Machine. Fixed by capturing AppliedIndex() before closing the first Machine and polling the second one until it reaches that value, budgeted at 3x the same run's own measured write-plus-delete duration, before verification is allowed to start. Confirmed working at 20,000 keys: a clean pass, with the real restart cost now correctly reported (setup 316ms, full catch-up 1m15s). Also confirmed at this run: reducing the write-phase worker pool from 64 to 8 (§14.10) was not merely neutral as reasoned -- measured directly, it achieved 77.19 puts/sec against the original 64-writer run's own extrapolated 26.34 ops/sec, nearly 3x faster, since fewer goroutines meant less real contention overhead on top of removing a throughput benefit that was never actually there. Extrapolating this run's real rates to the full one-million-key scale revised the total time estimate down substantially, from the earlier 10-15 hour guidance (based on write-phase extrapolation alone, before the restart phase was being measured at all) to roughly 4h40m (write ~3h36m, delete ~3m, restart catch-up ~1h3m) -- pending confirmation from the actual full run, not yet executed. §14.8's own results table remains pending that run. |
| v1.23 | §14.8's full-system test completed for real, at 300,000 keys rather than the originally-planned one million (new §14.12): a first attempt at the full one million was run, watched, and deliberately killed after roughly 19 hours -- confirmed to be doing genuine work the whole time (real syscalls, a coherent manifest, growing on-disk state, ~23% average CPU utilization consistent with disk-I/O-bound waiting, not a hang), killed anyway because the trajectory made clear it would take multiple more days. That attempt directly surfaced a real gap in the test's own instrumentation, fixed as a result: t.Logf output only flushes when a test completes, so a killed 19-hour run had produced nothing quotable regardless of real progress made. fullSystemProgress added: a periodic, unbuffered fmt.Fprintf reporter mirrored to a fixed external log path, immediately validated by a fresh, watched 300,000-key run that completed cleanly in 4h33m, correct on every key before and after restart. Two further findings from that completed run: the write rate's mid-run appearance of stabilizing (an interval-to-interval read taken around the 40-46 minute mark) was corrected against the full curve, which showed continuous decay to completion rather than a genuine plateau -- recorded as a correction, not smoothed over. And restart's own catch-up (which never calls Submit and so never pays for persistIfDirty's fsync) ran 303,000 entries at 426.96/s, 21.85x faster than the write phase's own final rate of 19.54/s on the same data -- the most precise confirmation yet of §14.10's two-stacked-fsync diagnosis, though explicitly not claimed as definitive proof, since replay also skips client-side scheduling overhead the write phase's own worker pool carries. §14.8's results table filled in with real numbers throughout. |
| v1.24 | The gRPC wire contract defined (new §15), the first task of Phase F: `helios.v1.Helios` -- `Get`, `Put`, `Delete`, `Scan` (unary), `Watch` (server-streaming) -- in `api/proto/helios/v1/helios.proto`, generated into public `api/helios/v1/` via `buf` driving local `protoc-gen-go`/`protoc-gen-go-grpc`. Schema only; no server or client exists yet. Every response's `revision` field is Raft's own commitIndex/lastApplied (§6, §8) exposed at the boundary, not a second counter. `Consistency` (`LINEARIZABLE`/`STALE`) is a per-request field on `Get`/`Scan` selecting between the two read paths §9 already implements and measured, defaulting to linearizable when unset. `Scan` is unary plus an opaque continuation token rather than a stream, committing only to the shape -- the token's real encoding is explicit deferred scope for a later task. `Watch` is single-prefix server-streaming, deliberately not etcd's multiplexed bidirectional protocol, which was rejected as unjustified complexity against this project's own stated goals and benchmark plan (§1). Idempotency fields and the `NotLeader` error's wire shape are named as deferred, not designed here, matching §12's own "Duplicate commands" open question. Tested at `api/helios/v1/smoke_test.go`: a marshal/unmarshal round trip per message, the zero-value contracts for `GetResponse.found` and `Consistency`, and a compile-time check that `Watch` generated as streaming rather than unary. |