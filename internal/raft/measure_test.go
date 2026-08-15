package raft

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var (
	measure       = flag.Bool("measure", false, "run the B-15 election-time measurement")
	measureTrials = flag.Int("measure.trials", 100, "trials per scenario")
	measureOut    = flag.String("measure.out", "testdata", "directory for the CSV and SVG output")
)

// samplePoll is how often the measurement checks for a leader.
const samplePoll = time.Millisecond

func TestElectionTimeDistribution(t *testing.T) {
	if !*measure {
		t.Skip("measurement is opt-in: go test -run TestElectionTimeDistribution -measure -v")
	}

	n := *measureTrials
	scenarios := []struct {
		name    string
		samples []time.Duration
	}{
		{"cold-start-3", coldStartSamples(t, 3, n)},
		{"cold-start-5", coldStartSamples(t, 5, n)},
		{"failover-5", failoverSamples(t, 5, n)},
	}

	if err := os.MkdirAll(*measureOut, 0o755); err != nil {
		t.Fatalf("creating %s: %v", *measureOut, err)
	}

	csvPath := filepath.Join(*measureOut, "election-times.csv")
	csv, err := os.Create(csvPath)
	if err != nil {
		t.Fatalf("creating %s: %v", csvPath, err)
	}
	defer csv.Close()
	fmt.Fprintln(csv, "scenario,trial,milliseconds")

	for _, s := range scenarios {
		sort.Slice(s.samples, func(a, b int) bool { return s.samples[a] < s.samples[b] })

		for i, d := range s.samples {
			fmt.Fprintf(csv, "%s,%d,%.3f\n", s.name, i, float64(d)/float64(time.Millisecond))
		}

		if s.samples[0] < electionTimeoutMin {
			t.Errorf("%s: fastest election was %v, below electionTimeoutMin %v — "+
				"something campaigned without waiting out its timer",
				s.name, s.samples[0], electionTimeoutMin)
		}

		t.Logf("\n%s", report(s.name, s.samples))

		svgPath := filepath.Join(*measureOut, s.name+".svg")
		if err := writeHistogramSVG(svgPath, s.name, s.samples, 10*time.Millisecond); err != nil {
			t.Errorf("writing %s: %v", svgPath, err)
		}
	}

	// The prediction, stated after the fact so the numbers above stand on their own.
	for _, size := range []int{3, 5} {
		t.Logf("theory, %d nodes: mean of min-of-%d draws = %v, median = %v",
			size, size, expectedMinOfN(size), medianMinOfN(size))
	}

	t.Logf("wrote %s and one SVG per scenario to %s/", csvPath, *measureOut)
}

// coldStartSamples measures time from starting the cluster to a leader existing.
func coldStartSamples(t *testing.T, nodes, trials int) []time.Duration {
	t.Helper()

	samples := make([]time.Duration, 0, trials)
	for i := 0; i < trials; i++ {
		func() {
			c := newCluster(t, nodes, int64(9000+i))
			defer c.stop()

			start := time.Now()
			c.start()

			d, leader := timeToLeader(c, start, electionBound)
			if leader == None {
				t.Fatalf("cold-start-%d trial %d (seed %d): no leader within %v: %s",
					nodes, i, c.seed, electionBound, c.describe())
			}
			samples = append(samples, d)
		}()
	}
	return samples
}

// failoverSamples measures time from killing the leader to a replacement leading in a later term.
func failoverSamples(t *testing.T, nodes, trials int) []time.Duration {
	t.Helper()

	samples := make([]time.Duration, 0, trials)
	for i := 0; i < trials; i++ {
		func() {
			c := newCluster(t, nodes, int64(9500+i))
			defer c.stop()
			c.start()

			old := c.waitForStableCluster(electionBound)
			if old == None {
				t.Fatalf("failover-%d trial %d (seed %d): no initial leader: %s",
					nodes, i, c.seed, c.describe())
			}
			_, oldTerm, _ := c.nodes[old].snapshotState()

			start := time.Now()
			c.kill(old)

			deadline := start.Add(failoverBound)
			var elapsed time.Duration
			for time.Now().Before(deadline) {
				if l := c.checkSingleLeader(); l != None && l != old {
					if _, term, _ := c.nodes[l].snapshotState(); term > oldTerm {
						elapsed = time.Since(start)
						break
					}
				}
				time.Sleep(samplePoll)
			}
			if elapsed == 0 {
				t.Fatalf("failover-%d trial %d (seed %d): no replacement within %v: %s",
					nodes, i, c.seed, failoverBound, c.describe())
			}
			samples = append(samples, elapsed)
		}()
	}
	return samples
}

func timeToLeader(c *cluster, start time.Time, within time.Duration) (time.Duration, int) {
	deadline := start.Add(within)
	for time.Now().Before(deadline) {
		if leader := c.checkSingleLeader(); leader != None {
			return time.Since(start), leader
		}
		time.Sleep(samplePoll)
	}
	return 0, None
}

// ---------------------------------------------------------------------------
// Statistics and plotting
// ---------------------------------------------------------------------------

// percentile uses the nearest-rank method on an already sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func mean(samples []time.Duration) time.Duration {
	var total time.Duration
	for _, d := range samples {
		total += d
	}
	return total / time.Duration(len(samples))
}

// expectedMinOfN is the mean of the smallest of n uniform draws from
// [electionTimeoutMin, electionTimeoutMax): a + (b-a)/(n+1).
func expectedMinOfN(n int) time.Duration {
	spread := float64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(spread/float64(n+1))
}

// medianMinOfN is the median of the same quantity: a + (b-a)(1 - 2^(-1/n)).
func medianMinOfN(n int) time.Duration {
	spread := float64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(spread*(1-math.Pow(0.5, 1/float64(n))))
}

// report renders percentiles and an ASCII histogram, so the shape is visible in
// the test log without opening anything.
func report(name string, sorted []time.Duration) string {
	var b strings.Builder

	fmt.Fprintf(&b, "%s over %d trials\n", name, len(sorted))
	fmt.Fprintf(&b, "  min %v  p50 %v  p90 %v  p95 %v  max %v  mean %v\n",
		sorted[0], percentile(sorted, 0.50), percentile(sorted, 0.90),
		percentile(sorted, 0.95), sorted[len(sorted)-1], mean(sorted))

	const bucket = 10 * time.Millisecond
	counts, first := histogram(sorted, bucket)

	widest := 0
	for _, c := range counts {
		if c > widest {
			widest = c
		}
	}
	for i, c := range counts {
		lo := first + time.Duration(i)*bucket
		bar := 0
		if widest > 0 {
			bar = c * 40 / widest
		}
		fmt.Fprintf(&b, "  %4d-%4dms |%s %d\n",
			lo/time.Millisecond, (lo+bucket)/time.Millisecond,
			strings.Repeat("#", bar), c)
	}
	return b.String()
}

// histogram buckets a sorted slice and returns the counts plus the lower edge of
// the first bucket.
func histogram(sorted []time.Duration, bucket time.Duration) ([]int, time.Duration) {
	first := sorted[0].Truncate(bucket)
	last := sorted[len(sorted)-1].Truncate(bucket)
	counts := make([]int, int((last-first)/bucket)+1)
	for _, d := range sorted {
		counts[int((d.Truncate(bucket)-first)/bucket)]++
	}
	return counts, first
}

// writeHistogramSVG draws the distribution with no third-party dependencies, so
// the plot is a committable artifact rather than something that only exists on
// the machine that had matplotlib installed.
func writeHistogramSVG(path, title string, sorted []time.Duration, bucket time.Duration) error {
	const (
		w, h            = 820.0, 380.0
		left, right     = 60.0, 20.0
		top, bottomEdge = 50.0, 60.0
	)
	plotW := w - left - right
	plotH := h - top - bottomEdge

	counts, first := histogram(sorted, bucket)
	widest := 1
	for _, c := range counts {
		if c > widest {
			widest = c
		}
	}
	barW := plotW / float64(len(counts))

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" font-family="monospace" font-size="11">`, w, h, w, h)
	fmt.Fprintf(&b, `<rect width="%.0f" height="%.0f" fill="#ffffff"/>`, w, h)
	fmt.Fprintf(&b, `<text x="%.1f" y="26" font-size="14">%s — %d trials</text>`, left, title, len(sorted))
	fmt.Fprintf(&b, `<text x="%.1f" y="42" fill="#666">p50 %v   p95 %v   max %v</text>`,
		left, percentile(sorted, 0.50), percentile(sorted, 0.95), sorted[len(sorted)-1])

	// Axes.
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#333"/>`,
		left, top+plotH, left+plotW, top+plotH)
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#333"/>`,
		left, top, left, top+plotH)
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="end" fill="#666">%d</text>`,
		left-6, top+10, widest)

	for i, c := range counts {
		bh := float64(c) / float64(widest) * plotH
		x := left + float64(i)*barW
		fmt.Fprintf(&b, `<rect x="%.2f" y="%.2f" width="%.2f" height="%.2f" fill="#4a6fa5"/>`,
			x, top+plotH-bh, barW-1, bh)

		// Label every fifth bucket, or the axis becomes unreadable.
		if i%5 == 0 {
			lo := (first + time.Duration(i)*bucket) / time.Millisecond
			fmt.Fprintf(&b, `<text x="%.2f" y="%.1f" text-anchor="middle" fill="#666">%d</text>`,
				x+barW/2, top+plotH+16, lo)
		}
	}
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#666">milliseconds</text>`,
		left+plotW/2, h-14)
	b.WriteString(`</svg>`)

	return os.WriteFile(path, []byte(b.String()), 0o644)
}
