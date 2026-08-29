# Helios — Design Document

A distributed, fault-tolerant key-value store built on the Raft consensus protocol.

**Status:** v1.8 — leader election, log replication, the apply path, linearizable
reads, persistence and snapshotting are implemented. Entries commit on a majority, apply
in order on every node, survive a crash of the process or the machine, and can be read
back either through a barrier or, under a bounded-clock assumption, from a leader's
lease. The log is compacted behind a state-machine image, and a follower that falls
below the resulting floor is repaired with `InstallSnapshot` rather than entries.
Verified under crashes, restarts, network partitions, message loss, reordering, and a
node offline for ten thousand entries. The on-disk formats for the LSM storage engine
that will back the state machine above the apply channel — write-ahead log record,
SSTable data block, footer, and index — are fixed on paper. The write-ahead log and the
memtable (a skip list) are both implemented and tested, including concurrent reads
against a single writer.

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
depends on them. `docs/fsync-policy.md` covers what "stable" is allowed to mean and what
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
argument in `docs/fsync-policy.md`, not on a green suite.

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
  number in `docs/fsync-policy.md`. Moving the write out is the highest-value
  performance work left in the storage path. It is blocked on one question: coalescing
  drops superseded records, which is sound only because the records form a total order
  over states the node actually occupied. Released from the lock, a record that shortens
  the log no longer subsumes the promise made by the record that lengthened it. Settle
  that before wiring the batcher in — it is written and tested, and would be actively
  wrong today.
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
- **SSTable block, index, and footer are designed but not built.** §13.2 fixes the byte
  layout; the block writer, index builder, and reader that turn a flushed memtable into
  a file on disk are the next task.
- **Memtable flush trigger is not implemented.** `Len()` reports a distinct-key count,
  not an approximate byte size, and nothing yet decides when a memtable is full and
  should be switched out and flushed. The Raft log's own compaction trigger (§10)
  already argues for measuring the right quantity rather than a proxy for it; the same
  argument applies here; a size estimate, not an entry count, is almost certainly the
  right trigger, and is not yet measured.
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

**Sync policy is the same fork `docs/fsync-policy.md` documents for Raft's persistent
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

### 13.2 SSTable block, footer, and index — designed, not yet implemented

Fixed on paper now, alongside the WAL, because a memtable flush turns WAL-shaped
records into SSTable entries and the two payload layouts are deliberately close in
shape for that reason.

**Data block** — a run of sorted entries followed by a per-block checksum:

```
+-------------+------------+---------------+--------------+
| KeyLen(4B)  | Key(...)   | ValueLen(4B)  | Value(...)   |
+-------------+------------+---------------+--------------+
| ... further entries, sorted by key ...                   |
+------------------------------------------------------------+
| BlockCRC32 (4B)                                             |
+------------------------------------------------------------+
```

Entries are sorted so that, once an index exists, a lookup inside a block can binary
search rather than scan. No restart-point prefix compression yet — that is an open
question (§12): it saves bytes on disk at the cost of a slightly more involved block
reader, and nothing here needs the space back yet.

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
decoding garbage as a plausible-looking footer.

*Rejected: a footer or table of contents at the front of the file.* An SSTable is
written once, sequentially, one data block after another; the index and footer can
only be assembled once every block's final offset is known, which means they are
necessarily the last things written. Putting them at the front would mean either
reserving a fixed size for them before the key count is known — wrong the moment it
varies — or a second pass to backfill placeholder bytes, which a trailing footer
avoids by construction.

**Not yet implemented.** This is the byte layout, decided so the WAL and the SSTable
agree on how a key/value pair is framed; the block writer, the index builder, and the
reader that turns this layout into working code are separate, later work.

### 13.3 Memtable — a skip list, implemented

The memtable is the sorted, in-memory structure every write lands in before it is
durable in an SSTable: the WAL (§13.1) makes a write crash-safe, and the memtable is
what makes it *queryable* while it is still only in the WAL and not yet flushed. A read
path checks the active memtable first, then any memtables mid-flush, then SSTables
oldest-to-newest, stopping at the first tombstone or value it finds — the memtable is
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

**`Memtable` satisfies `wal.Sink` structurally.** `ApplyPut` and `ApplyDelete` give a
`Memtable` the two methods `wal.RecoverAndOpen` (§13.1) expects of a recovery sink,
without this package importing package `wal` at all — Go's structural interfaces make
that possible, and not importing it keeps the dependency one-way: the WAL knows nothing
about memtables, and the memtable package knows nothing about the WAL either. A node's
startup path is what wires the two together, by calling
`wal.RecoverAndOpen(path, policy, memtable)`.

**Implemented** at `internal/storage/memtable/`, across `skiplist.go` (the node type,
the lock-free `search`, and level selection), `memtable.go` (the public `Put` / `Delete`
/ `Get` / `Len` surface and the `wal.Sink` methods), and `iterator.go`. Correctness is
checked against a reference map built alongside 5,000 randomly ordered inserts,
including duplicate keys and tombstones; concurrency is checked by four dedicated
tests — concurrent reads alone, concurrent reads against a single writer inserting new
keys, concurrent reads against a single writer overwriting one hot key (the torn-value
check above), and concurrent iteration against a writer — all run under `-race
-shuffle=on -count=3`.

---

## 14. Revision log

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
| v1.8 | The memtable implemented as a skip list (§13.3): lock-free `Get` and iteration against a single mutex-serialized writer, an atomically-swapped value-plus-tombstone pair per key so neither a splice nor an update can ever be observed half-done, and a per-instance seeded RNG for level selection on the same rule as Raft's election timer. Three structures — red-black tree, sorted slice, skip list — compared and the choice argued from which one lets a new node's publication be a single atomic pointer write. `Memtable` satisfies `wal.Sink` structurally, wiring the write-ahead log's recovery path to the memtable without either package importing the other. Concurrency argued from the memory model and then checked directly: concurrent reads alone, concurrent reads against a single inserting writer, concurrent reads against a single updating writer with byte-level torn-value detection, and concurrent iteration against a writer, all under `-race -shuffle=on -count=3`; a fifth test drives sixteen concurrent writers on disjoint keys through the same internal mutex production never contends, on the same "guarded, not assumed" reasoning §8 applies to Raft's own believed-impossible states. |