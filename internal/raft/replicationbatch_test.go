package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Counting what reached the state machines
// =============================================================================

// appliedHighWater tracks the highest index each node has applied, so the
// measurement can wait for COMMITS rather than for Submit to return.
//
// Timing Submit would measure nothing useful: it returns as soon as the entry
// is in the local log, before a single byte has left the leader. Replication
// throughput is only visible at the far end of the apply channel.
type appliedHighWater struct {
	mu   sync.Mutex
	high map[int]int
}

func newAppliedHighWater() *appliedHighWater {
	return &appliedHighWater{high: make(map[int]int)}
}

func (a *appliedHighWater) record(node int, msg ApplyMsg) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if msg.CommandIndex > a.high[node] {
		a.high[node] = msg.CommandIndex
	}
}

// best reports the furthest any node has got.
func (a *appliedHighWater) best() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	highest := 0
	for _, h := range a.high {
		if h > highest {
			highest = h
		}
	}
	return highest
}

func (a *appliedHighWater) waitFor(idx int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if a.best() >= idx {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// =============================================================================
// The measurement
// =============================================================================

// TestReplicationBatchingThroughput compares one fan-out per client write
// against one round in flight per peer.
//
// A Test rather than a Benchmark, for the same reason as the fsync measurement:
// the unbatched arm does work quadratic in the number of commands, so letting
// the framework scale b.N upward produces a run nobody intended.
//
// THE NETWORK HAS LATENCY ON PURPOSE. With instant delivery, replies come back
// before the next Submit lands, nextIndex keeps pace, and every message carries
// one entry -- there is nothing for batching to collapse and the comparison
// shows nothing. One to three milliseconds is a plausible LAN round trip and is
// what makes the in-flight window wide enough for writes to accumulate in it,
// which is the situation batching exists for.
//
// Storage stays in memory. The fsync policy is measured separately, and at
// 6.8 ms a flush it would swamp everything on this page.
func TestReplicationBatchingThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement, not an assertion; runs in the full suite")
	}

	for _, tc := range []struct {
		name     string
		coalesce bool
	}{
		{"per-submit-fanout", false},
		{"coalesced-rounds", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			measureReplication(t, tc.coalesce)
		})
	}
}

func measureReplication(t *testing.T, coalesce bool) {
	const (
		commands = 1000
		clients  = 64
		seed     = 20260901
	)

	c := newCluster(t, 3, seed)

	if !coalesce {
		for _, n := range c.nodes {
			n.mu.Lock()
			n.noCoalesce = true
			n.mu.Unlock()
		}
	}

	applied := newAppliedHighWater()
	c.watchApplies(applied.record)
	c.start()

	leader := c.waitForStableCluster(5 * time.Second)
	if leader == None {
		t.Fatalf("no leader: %s", c.describe())
	}

	// Latency only from here, so the election's own traffic is not in the
	// numbers. The delay applies to every message from this point.
	c.net.setDelayRange(time.Millisecond, 3*time.Millisecond)
	c.net.resetCounters()

	start := time.Now()

	var wg sync.WaitGroup
	for k := 0; k < clients; k++ {
		share := commands / clients
		if k < commands%clients {
			share++
		}
		wg.Add(1)
		go func(count, id int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				c.submitToLeader([]byte(fmt.Sprintf("client%02d-cmd%04d", id, i)))
			}
		}(share, k)
	}
	wg.Wait()
	submitted := time.Since(start)

	if !applied.waitFor(commands, 60*time.Second) {
		t.Fatalf("only %d of %d commands committed in 60s: %s",
			applied.best(), commands, c.describe())
	}
	elapsed := time.Since(start)

	msgs, entries := c.net.appendStats()

	perMessage := 0.0
	if msgs > 0 {
		perMessage = float64(entries) / float64(msgs)
	}

	t.Logf("%d commands from %d clients, all submitted in %v, all committed in %v",
		commands, clients, submitted.Round(time.Millisecond), elapsed.Round(time.Millisecond))
	t.Logf("  throughput   %.0f commands/s", float64(commands)/elapsed.Seconds())
	t.Logf("  messages     %d AppendEntries (%.2f per command)",
		msgs, float64(msgs)/float64(commands))
	t.Logf("  entries      %d shipped (%.1f per message, %.1fx the %d that exist)",
		entries, perMessage, float64(entries)/float64(commands), commands)
}
