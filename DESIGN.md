# Helios — Design Document

A distributed, fault-tolerant key-value store built on the Raft consensus protocol.

**Status:** v1.4 — leader election, log replication, the apply path and linearizable
reads are implemented. Entries commit on a majority, apply in order on every node, and
can be read back either through a barrier or, under a bounded-clock assumption, from a
leader's lease. Verified under crashes, network partitions, message loss and reordering.
Nothing is persisted to stable storage yet.

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
| `conflictIndex`, `conflictTerm` | Fast-backup hint. **Not Figure 2** — see §10. Set only on a log rejection. |

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

**Not yet persisted.** All three currently live in memory only. Durability is future
work; until then a "restart" in tests means a fresh node, not a recovered one. The
call sites are marked in `election.go` and `requestvote.go`.

---

## 6. Volatile state

Lost on restart. Safe to lose, because each is either reconstructed from peers or
conservatively reinitialised.

### On all nodes

| Field | Meaning | Why losing it is safe |
|---|---|---|
| `commitIndex` | Highest entry known committed | Re-learned from the next AppendEntries |
| `lastApplied` | Highest entry applied to the state machine | Re-derived by replaying the log, or from a snapshot once snapshotting lands |
| `state` | follower / candidate / leader | Always restart as follower; worst case is one extra election |
| `electionDeadline` | When to start an election | Fresh deadline on start |

### Configuration and machinery

| Field | Meaning |
|---|---|
| `id`, `peers` | Cluster membership. `peers` excludes self, so cluster size is `len(peers)+1`. |
| `transport` | How this node reaches peers. Fake switchboard in tests, gRPC in production. |
| `rng` | Per-node seeded source for election timeouts. Never the global `rand`. |
| `applyCh`, `applyNotify`, `applierDone` | The apply plumbing. See §8. |
| `mu` | Guards every field above except the apply channels. |

### On leaders only — reinitialised after every election

| Field | Meaning |
|---|---|
| `nextIndex[peer]` | Next log index to send. Optimistically initialised to leader's last index + 1. |
| `matchIndex[peer]` | Highest index known replicated on that peer. Initialised to 0. |
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

**Reordering is measured, not configured.** It is emergent — one goroutine per message
plus one random delay per goroutine — so a message overtakes an earlier one to the same
peer whenever it draws a shorter delay than the gap between their sends. Under client
load that gap is microseconds and inversions are constant; between heartbeats it is
50ms and a 10ms jitter range makes them impossible. There is no rate to dial, so the
network stamps send order, records arrival order, and tests assert the observed count is
nonzero rather than claiming a percentage.

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
- **fsync policy.** Per-write fsync is correct but slow. Measure the tradeoff alongside
  durability.
- **Batching.** Multiple client writes per AppendEntries, and coalescing the per-`Submit`
  fan-out. Deferred until there are baseline numbers.
- **Snapshots.** Log truncation and `InstallSnapshot`. Breaks the
  sentinel/position-equals-index convention, the term-run scan used by the fast-backup
  hint, and the assumption that index 0 is always checkable. A `prevLogIndex` below the
  snapshot floor must be answered with `InstallSnapshot`, not a rejection.
- **Membership changes.** Single-server add and remove. Note that the consistency check is
  only correct because of one-leader-per-term, which is exactly what joint consensus exists
  to preserve.
- **Log Matching checkers ignore the no-op flag.** Two entries differing only in that flag
  compare equal, so the property checkers would not notice a barrier substituted for a
  command at the same index and term. Unreachable today, since nothing produces that state;
  fix it when the no-op on election lands and barriers become common.
- **Test suite runtime.** Roughly two and a half minutes under `-race` and growing,
  dominated by tests that sleep in real multiples of the election timeout. A virtual clock
  is the fix, and this is now the open question with the most evidence behind it.
- **Timing constants are scattered.** Each test file defines its own bounds. Consolidating
  them would make the suite's time budget visible in one place; worth doing alongside the
  virtual clock.

---

## 13. Revision log

| Version | Change |
|---|---|
| v1 | Initial design: states, RPCs, persistent and volatile state |
| v1.1 | Majority-intersection argument for the up-to-date check; implementation decisions; the election-timer reset deviation recorded as an open question; volatile state expanded to match the implementation |
| v1.2 | Commit rules and the Figure 8 argument; leader bookkeeping; the fast-backup hint recorded as a deviation with measured round-trip savings; implementation decisions reorganised into log / timing / replication / bookkeeping; testing conventions added |
| v1.3 | The apply path documented for the first time; linearizable reads and the argument against local reads; the no-op entry flag recorded as a departure from Figure 2; AppendEntries rule 5 and the same-term refusal written up; believed-impossible guards collected as a single principle; fault injection, fixture-inertness and test-goroutine conventions added; duplicate commands and read-barrier cost promoted to open questions |
| v1.4 | Lease-based reads, with the clock-rate assumption and the process-pause residue documented; the send-time and quorum-ordering rules for deriving a lease; read latency measured across three link speeds, and the argument for the no-op on election revised in light of it from latency to log growth |