# fsync policy

Helios persists three fields — `currentTerm`, `votedFor` and the log — before
responding to any RPC. This note covers the remaining question: what "persists"
is allowed to mean, what each answer costs, and which one Helios ships.

## The three policies

**Always.** Every record is written to a temp file, flushed to the device,
renamed over the live file, and the directory flushed, before `Save` returns.

**Batch (group commit).** Concurrent callers share one write and one flush.
Every caller still blocks until that flush completes. Records superseded while a
flush is in progress are dropped without ever being written, which is sound here
because each record is a complete snapshot of a state the node genuinely
occupied, and the records form a total order — they are all built under the node
mutex.

**Never.** The record is written and renamed, but nothing is flushed. `Save`
returns as soon as the kernel has the bytes.

All three are equally *atomic*. The temp-file-and-rename sequence is unchanged
across policies, so no policy can leave a torn record or a half-applied update.
What varies is which failures the bytes survive.

## What each policy survives

| Failure | Always | Batch | Never |
|---|---|---|---|
| Process killed (SIGKILL, panic, OOM) | survives | survives | **survives** |
| Kernel panic | survives | survives | lost |
| Power loss | survives | survives | lost |
| Device ignores the flush | lost | lost | lost |

The third column's first row is the one that matters most, because it is the one
that makes this decision hard to test. The page cache belongs to the kernel, not
the process, so bytes handed to `write(2)` outlive the process that wrote them
regardless of any flush. `crash_test.go` SIGKILLs a child process at random
points across twelve rounds and would pass identically with every `Sync` call
deleted. **The fsync policy cannot be justified by any test currently in this
repository.** It has to be justified by argument, which is what this note is.

Catching it would take fault injection below the kernel — a device-mapper
`log-writes` target replaying the write stream up to an arbitrary point, or a
machine somebody unplugs. That belongs with the fault-injection work.

## What losing it costs in Raft terms

A node that comes back having forgotten its persistent state is not a node that
failed. It is a node that lies.

Raft tolerates *f* crashed nodes out of 2*f*+1, and the tolerance depends on a
crashed node either staying down or returning with its memory of what it
promised. A node that returns with the same identity and no memory can:

- **Vote twice in one term.** It granted a vote to A in term 7, replied, lost
  power, and came back at term 0. B campaigns in term 7 and it votes again.
  Two leaders in term 7, and Election Safety — the property asserted on every
  poll in `cluster_test.go` — is gone.
- **Erase an acknowledged write.** It was one of the bare majority holding entry
  12 when the leader committed. It returns without entry 12, wins the next
  election on a log that never had it, and overwrites the index. A client was
  told that write succeeded.

Neither failure needs a majority to lose state. **One node is enough**, which is
why "we have three replicas, so a lost write on one of them is fine" is the
wrong intuition: replication protects against nodes *stopping*, not against
nodes *forgetting*.

## The case for Never anyway

It is not an unserious position, and two well-known systems take opposite sides
of it. Kafka defaults to no fsync on the broker log and relies on replication
across brokers; etcd fsyncs its WAL on every append. The difference is what each
is willing to assume about failure independence: replication substitutes for
durability only when the replicas fail independently, and a rack losing power, a
hypervisor host dying, or an availability zone going dark takes out the
correlated majority that the argument assumed away.

A defensible Never deployment therefore needs, at minimum: replicas in
independent power and failure domains, an operational rule that a node which
loses power is wiped and rejoins with a **new identity** rather than restarting
in place, and an explicit acceptance that a correlated outage can lose
acknowledged writes. Helios makes none of those guarantees today, and a
key-value store whose selling point is linearizability should not quietly
weaken the property in its storage layer.

## The case for Batch

Batch is not a durability tradeoff at all. It makes the same promise as Always
and differs only in scheduling: the first caller in a batch waits slightly
longer so that everyone behind them shares one trip to the device. The guarantee
at the point of reply is identical.

There is one place to be careful. Coalescing drops superseded records, and the
argument for why that is safe rests on the records being a total order over
states the node actually occupied — which holds because every record is built
under the node mutex. Moving `persistIfDirty` out from under that lock, which is
precisely what would make batching useful, would break that argument for
truncating writes: a record that shortens the log does not subsume the promise
made by the record that lengthened it. Establish the ordering another way before
wiring the batcher in.

## Measurements

Machine: Apple M3, macOS, APFS on the internal NVMe, 8 logical CPUs
Go: 1.26.5, darwin/arm64
Record: 256-entry log, 7862 bytes, fixed size

Single run each. Repeated runs vary by roughly ±10% on the flushing policies —
enough that any difference under that threshold below should be read as noise.

### Storage layer, concurrency supplied by the benchmark

`go test ./internal/raft/ -run '^$' -bench BenchmarkStorageSave -benchtime 200x`

| Policy | writers=1 | writers=8 | writers=64 | flush/write @64 |
|---|---|---|---|---|
| always | 136 w/s (7.37 ms) | 151 w/s (6.63 ms) | 147 w/s (6.82 ms) | 1.00 |
| batch | 143 w/s (6.99 ms) | 633 w/s (1.58 ms) | **3679 w/s (0.27 ms)** | 0.04 |
| never | 3518 w/s (0.28 ms) | 4996 w/s (0.20 ms) | 4763 w/s (0.21 ms) | — |

### Write path, a real single-node leader

`go test ./internal/raft/ -run TestFsyncPolicyOnTheWritePath -v`
1000 commands, 64 concurrent clients, log growing from 0 to 1000 entries.

| Policy | commands/s | ms/command | flush/write |
|---|---|---|---|
| always | 147 | 6.80 | 1.00 |
| batch | 142 | 7.06 | **1.00** |
| never | 4782 | 0.21 | — |

### Reading the two tables together

**One fsync costs 6.8 ms on this machine.** Go's `Sync` on macOS issues
`F_FULLFSYNC`, a genuine device cache flush, and the resulting figure is closer
to a spinning disk than to the NVMe it is running on. Every other number here is
a consequence of that one.

**Always does not scale.** 136 → 151 → 147 writes/s from 1 to 64 writers: flat
inside the noise. `FileStorage.mu` serializes, there is one device, and adding
callers only adds queueing. Never plateaus for the same reason at a higher
level, bounded by the write and rename syscalls rather than by the flush.

**Batch is the only policy where concurrency reduces work.** `flush/write` falls
1.00 → 0.25 → 0.04 as callers pile up, and throughput rises with it. At 64
writers group commit reaches **25× Always and 77% of Never, while giving up no
durability at all.** The apparent choice between "safe" and "fast" is mostly an
artifact of measuring one writer at a time.

Batch at one writer (143 w/s) matches Always (136 w/s) inside the noise band —
one flush per write in both cases, plus a goroutine handoff. The batcher is not
free, but at this flush cost the handoff is unmeasurable.

**The node's write path is entirely fsync-bound.** Always on the node reaches
147 commands/s; Always at the storage layer with a fixed record reaches 146.6.
Those agree to within 1%, which means everything Raft does per command — the
append, the commit check, the applier handoff, and the whole-log re-encode of a
log growing to 1000 entries — costs under 2% of a write. The disk is not
competing with the implementation; it is the implementation's entire cost.

**Batch buys nothing on the write path, exactly as predicted.** 1001 saves
served by 1001 flushes: a ratio of 1.00 with no rounding. `persistIfDirty` runs
under `n.mu`, so a node has at most one `Save` outstanding no matter how many
clients are calling `Submit`. The 3% it loses to Always is the goroutine hop
with nothing to show for it. **The gap between 147 and 3679 writes/s is what the
critical section is currently costing**, and it is the largest single number in
this document.

**Correction to an earlier assumption.** An earlier draft of this note claimed
log compaction was the larger win and should land before any fsync work. The
measurement refutes that. Extrapolating from Never at one writer — 0.284 ms for
a 7862-byte record — the record would have to reach roughly 190 KB, on the order
of six thousand entries at this command size, before the whole-log rewrite cost
as much as a single flush. Compaction is still needed, for restart time and
memory, but it is not the write-throughput bottleneck at any log size this
system currently reaches.

**Priority that follows.** The highest-value performance work in the storage
path is moving `persistIfDirty` out of the critical section so that group commit
has something to coalesce. That is blocked on the truncation-ordering argument
in "The case for Batch" above, which has to be settled first: the batcher is
already written and tested, and would be actively wrong under a released lock.

### Platform caveat

Go's `File.Sync` on macOS issues `F_FULLFSYNC`, which asks the device to flush
its own write cache. Linux's `fsync` on consumer SSDs frequently does not, and
returns as soon as the data reaches the drive's volatile cache. The 6.8 ms
measured here would likely be a few hundred microseconds on a Linux box with
comparable hardware — and that box would be offering less durability than it
appears to. Do not compare these figures across platforms, and do not read a
fast fsync on Linux as evidence that the flush was honoured.

## Decision

Helios ships **SyncAlways** as the default and the only policy the node uses.

`SyncNever` exists on `FileStorage` for benchmarking and for tests that do not
care. It is not exposed as a deployment option, because a durability setting
that can be flipped in a config file will eventually be flipped by somebody who
has not read this note.

`batchedStorage` exists and passes its contract tests but is not wired into the
node. There is nothing for it to coalesce until the write leaves the critical
section, and it would be unsound the moment it does, until the ordering question
is answered. Revisit both together — the payoff, on the numbers above, is 25×.