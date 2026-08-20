package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// Read latency: barrier versus lease
// =============================================================================
//
// Opt-in, and it reuses the measurement flags and the reporting helpers that
// already exist:
//
//	go test ./internal/raft/ -run TestReadLatencyDistribution -measure -v
//
// Run it WITHOUT -race. The detector adds enough overhead to swamp the
// microsecond end and make the lease path look worse than it is.
//
// WHAT IS BEING MEASURED, AND WHY AT SEVERAL LATENCIES.
//
// A barrier read costs an append, a round trip to a majority, and an apply. A
// lease read costs a mutex acquisition and a map lookup. The difference is
// therefore not a fixed number -- it is ONE ROUND TRIP -- and a single figure
// taken on an in-memory network would be an argument about allocation costs
// rather than about the optimisation.
//
// So the same comparison runs at three link speeds, chosen to bracket where a
// real cluster sits.
//
// HOW THE HARNESS MODELS LATENCY, because the first version of this file got it
// wrong and asserted against the wrong quantity.
//
// The fake network sleeps ONCE PER RPC, on the request leg; the reply path has
// no delay. So setDelayRange(D, D) does not model a one-way hop of D -- it
// models an RPC whose whole round trip costs D. The names below are RPC
// latencies for that reason. A real network with a one-way delay of d has an
// RPC latency of roughly 2d, so the wan figure here corresponds to a link with
// about 12ms each way.
//
// A SECOND ARTEFACT, in the barrier's disfavour. kvMachine.waitForIndex polls
// at 1ms, so a barrier read is charged up to a millisecond of polling
// granularity that a condition variable would not cost. That is essentially the
// whole loopback figure, and it means the loopback saving below is overstated.
// The 5ms and 25ms figures are dominated by the network and are not materially
// affected.
//
// The prediction the numbers should confirm: barrier tracks the RPC latency,
// lease stays flat and near zero. If the lease line rises with the link speed,
// something in that path is doing network work it should not be.

func TestReadLatencyDistribution(t *testing.T) {
	if !*measure {
		t.Skip("measurement is opt-in: go test -run TestReadLatencyDistribution -measure -v")
	}

	links := []struct {
		name string
		rpc  time.Duration // whole-RPC latency, not one-way. See the note above.
	}{
		{"loopback", 0},
		{"lan-5ms", 5 * time.Millisecond},
		{"wan-25ms", 25 * time.Millisecond},
	}

	if err := os.MkdirAll(*measureOut, 0o755); err != nil {
		t.Fatalf("creating %s: %v", *measureOut, err)
	}
	csvPath := filepath.Join(*measureOut, "read-latency.csv")
	csv, err := os.Create(csvPath)
	if err != nil {
		t.Fatalf("creating %s: %v", csvPath, err)
	}
	defer csv.Close()
	fmt.Fprintln(csv, "link,path,trial,milliseconds")

	type row struct {
		link          string
		rpc           time.Duration
		barrier       []time.Duration
		lease         []time.Duration
		leaseRefused  int
		barrierFailed int
	}
	var rows []row

	// Fewer trials than the election measurement: each barrier read at 25ms
	// costs a round trip, so a hundred of them is several seconds of waiting.
	trials := *measureTrials
	if trials > 50 {
		trials = 50
	}

	for _, link := range links {
		barrier, lease, refused, failed := readLatencyTrial(t, link.rpc, trials)

		for i, d := range barrier {
			fmt.Fprintf(csv, "%s,barrier,%d,%.4f\n", link.name, i, milliseconds(d))
		}
		for i, d := range lease {
			fmt.Fprintf(csv, "%s,lease,%d,%.4f\n", link.name, i, milliseconds(d))
		}

		sort.Slice(barrier, func(a, b int) bool { return barrier[a] < barrier[b] })
		sort.Slice(lease, func(a, b int) bool { return lease[a] < lease[b] })

		rows = append(rows, row{link.name, link.rpc, barrier, lease, refused, failed})

		t.Logf("\n%s", report(link.name+" barrier", barrier))
		t.Logf("\n%s", report(link.name+" lease", lease))

		for _, s := range []struct {
			suffix  string
			samples []time.Duration
		}{{"barrier", barrier}, {"lease", lease}} {
			p := filepath.Join(*measureOut, "read-"+link.name+"-"+s.suffix+".svg")
			if err := writeHistogramSVG(p, link.name+" "+s.suffix, s.samples, bucketFor(s.samples)); err != nil {
				t.Errorf("writing %s: %v", p, err)
			}
		}
	}

	// --- the comparison, which is the actual deliverable -------------------

	var b strings.Builder
	fmt.Fprintf(&b, "\nread latency, %d trials per path\n\n", trials)
	fmt.Fprintf(&b, "  %-10s %-9s %13s %13s %13s %13s %13s\n",
		"link", "rpc", "barrier p50", "barrier p95", "lease p50", "lease p95", "saved p50")

	for _, r := range rows {
		saved := percentile(r.barrier, 0.50) - percentile(r.lease, 0.50)
		fmt.Fprintf(&b, "  %-10s %-9v %13v %13v %13v %13v %13v\n",
			r.link, r.rpc,
			percentile(r.barrier, 0.50), percentile(r.barrier, 0.95),
			percentile(r.lease, 0.50), percentile(r.lease, 0.95),
			saved)
	}

	fmt.Fprintf(&b, "\n  A barrier read is an append, one RPC to a majority, and an apply.\n")
	fmt.Fprintf(&b, "  A lease read is a mutex acquisition and a map lookup.\n")
	fmt.Fprintf(&b, "  The saving is the RPC. It is worth nothing on loopback and\n")
	fmt.Fprintf(&b, "  almost everything across zones.\n")
	fmt.Fprintf(&b, "\n  Caveat: waitForIndex polls at 1ms, so the barrier is charged up\n")
	fmt.Fprintf(&b, "  to a millisecond it would not pay with a condition variable. That\n")
	fmt.Fprintf(&b, "  is most of the loopback row and none of the wan row.\n")
	t.Logf("%s", b.String())

	for _, r := range rows {
		if r.leaseRefused > 0 {
			t.Logf("%s: the lease was unavailable on %d of %d reads",
				r.link, r.leaseRefused, trials)
		}
		if r.barrierFailed > 0 {
			t.Errorf("%s: %d barrier reads failed outright", r.link, r.barrierFailed)
		}
		if r.rpc == 0 {
			continue // no network term to compare against
		}

		// THE PREDICTION, asserted rather than eyeballed.
		//
		// A lease read does no network work, so its cost must not track the
		// link speed. A barrier read makes exactly one RPC, so its cost must be
		// at least the RPC latency. Both bounds are generous: the point is to
		// catch a lease path that started waiting on something, or a barrier
		// path that stopped reaching a majority, not to police jitter.
		if p95 := percentile(r.lease, 0.95); p95 > r.rpc {
			t.Errorf("%s: lease p95 is %v against an RPC latency of %v -- the "+
				"lease path is doing network work", r.link, p95, r.rpc)
		}
		if p50 := percentile(r.barrier, 0.50); p50 < r.rpc {
			t.Errorf("%s: barrier p50 is %v, below one RPC (%v) -- the barrier "+
				"is not reaching a majority", r.link, p50, r.rpc)
		}
	}

	t.Logf("wrote %s and one SVG per link and path to %s/", csvPath, *measureOut)
}

// readLatencyTrial builds one cluster at the given RPC latency and times an
// equal number of reads down each path.
//
// The two paths are interleaved rather than run in separate phases, so that a
// drifting cluster -- a leader change, a slow patch of scheduling -- lands on
// both series rather than on whichever ran second.
func readLatencyTrial(t *testing.T, rpc time.Duration, trials int) (barrier, lease []time.Duration, refused, failed int) {
	t.Helper()

	c := newCluster(t, 5, 12_000+int64(rpc))
	c.net.setDelayRange(rpc, rpc)
	c.start()

	machines := make([]*kvMachine, len(c.nodes))
	for i, n := range c.nodes {
		machines[i] = attachMachine(n)
	}

	leader := c.waitForStableCluster(10 * time.Second)
	if leader == None {
		t.Fatalf("rpc %v: no leader within 10s: %s", rpc, c.describe())
	}

	// WARM-UP, and it is required rather than tidy. A lease read is refused
	// until this term has committed something of its own, so without a write
	// first every lease sample would be a refusal and the comparison would
	// measure the fallback.
	idx, _, isLeader := c.nodes[leader].Submit(encodePut("k", "warm"))
	if !isLeader {
		t.Fatalf("rpc %v: the leader stopped leading before the warm-up", rpc)
	}
	if !machines[leader].waitForIndex(idx, 10*time.Second) {
		t.Fatalf("rpc %v: the warm-up write never applied", rpc)
	}

	n, m := c.nodes[leader], machines[leader]

	for i := 0; i < trials; i++ {
		start := time.Now()
		_, _, err := readLeased(n, m, "k", 10*time.Second)
		took := time.Since(start)
		if err != nil {
			refused++
		} else {
			lease = append(lease, took)
		}

		start = time.Now()
		_, _, err = readThrough(n, m, "k", 10*time.Second)
		took = time.Since(start)
		if err != nil {
			failed++
		} else {
			barrier = append(barrier, took)
		}
	}

	if len(barrier) == 0 || len(lease) == 0 {
		t.Fatalf("rpc %v: no usable samples (barrier %d, lease %d, refused %d, failed %d)",
			rpc, len(barrier), len(lease), refused, failed)
	}
	return barrier, lease, refused, failed
}

func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// bucketFor picks a histogram bucket wide enough to be readable at whatever
// scale the samples landed on. Lease reads are microseconds and barrier reads at
// 25ms are tens of milliseconds; one fixed bucket cannot serve both, and the
// election measurement's 10ms bucket collapses every lease series into a single
// bar.
func bucketFor(sorted []time.Duration) time.Duration {
	if len(sorted) == 0 {
		return time.Millisecond
	}
	span := sorted[len(sorted)-1] - sorted[0]
	if span <= 0 {
		return time.Microsecond
	}
	bucket := span / 20
	if bucket < time.Microsecond {
		bucket = time.Microsecond
	}
	return bucket
}
