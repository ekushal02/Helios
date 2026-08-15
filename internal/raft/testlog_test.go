package raft

import (
	"bytes"
	"flag"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// raftLogLevel controls how much the nodes say during tests.
//
// Off by default, and that default is not laziness. A five-node cluster at debug
// emits well over a hundred lines a second, and TestSingleLeaderElected runs a
// hundred clusters. Logs you cannot switch off are logs you learn to ignore.
//
//	go test ./internal/raft/ -run TestForcedSplitVoteResolves -raftlog=info -v
var raftLogLevel = flag.String("raftlog", "off", "node log level during tests: off, info, debug")

// newTestLogger builds a logger that writes into t.Log, so its output is
// attached to the test that produced it and is shown only when that test fails
// or -v is set. One writer per cluster; SetLogger adds the per-node id.
func newTestLogger(t *testing.T, seed int64) *slog.Logger {
	level, on := parseLogLevel(*raftLogLevel)
	if !on {
		return discardLogger
	}

	w := &testWriter{t: t}
	t.Cleanup(w.close)

	start := time.Now()
	h := slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: level,
		// t.Logf reports the location of the Write call, which is this file --
		// useless. slog's own source attribute points at the line that actually
		// logged, which is the one you want to open.
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Wall-clock timestamps are the wrong unit here. Elections take
			// single-digit milliseconds and the interesting question is always
			// "how long after the last thing", so the time attribute becomes an
			// offset from when the cluster was built.
			if len(groups) == 0 && a.Key == slog.TimeKey {
				ms := float64(time.Since(start)) / float64(time.Millisecond)
				return slog.String("t", fmt.Sprintf("+%.3fms", ms))
			}
			// Full paths push the useful attributes off the right-hand edge.
			if len(groups) == 0 && a.Key == slog.SourceKey {
				if src, ok := a.Value.Any().(*slog.Source); ok {
					return slog.String("at", fmt.Sprintf("%s:%d", filepath.Base(src.File), src.Line))
				}
			}
			return a
		},
	})

	// The seed rides on every line. When trial 47 of 100 fails, the reproduction
	// is sitting in the log rather than in a mental count of loop iterations.
	return slog.New(h).With("seed", seed)
}

// testWriter forwards log lines to t.Log, and stops doing so once the test is
// over.
//
// THE CLOSED FLAG IS THE ENTIRE POINT OF THIS TYPE. Calling t.Log after its test
// has finished panics with "Log in goroutine after Test has completed", and Raft
// nodes are full of goroutines that outlive the test body by a few milliseconds:
// an in-flight RequestVote, a heartbeat mid-send, a ticker that has not yet
// noticed stopCh. Without the flag the suite fails intermittently, in a
// different test each run, with a panic that points at the logging and not at
// the code that was actually still running.
type testWriter struct {
	t testing.TB

	mu     sync.Mutex
	closed bool
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return len(p), nil // swallowed on purpose, see the type comment
	}
	w.t.Logf("%s", bytes.TrimRight(p, "\n"))
	return len(p), nil
}

func (w *testWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
}

func parseLogLevel(s string) (slog.Level, bool) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, true
	case "info":
		return slog.LevelInfo, true
	case "warn":
		return slog.LevelWarn, true
	case "off", "", "none":
		return 0, false
	default:
		return slog.LevelInfo, true
	}
}
