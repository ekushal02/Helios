package raft

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// parseLogLines decodes every non-empty JSON line in buf, in order --
// the same shape a real log aggregator would see reading this
// project's own structured output (internal/logging) line by line.
func parseLogLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not valid JSON: %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records
}

func waitForLeaderLogging(t *testing.T, n *Node, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, _, isLeader := n.Submit([]byte("probe")); isLeader {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("node did not become leader within %v", within)
}

// TestStateTransitionsAreLoggedAtDebugLevel is the actual point of this
// task: a single-node cluster's own follower->candidate->leader
// transitions, at a Debug-level logger, must appear as "state
// transition" log lines with accurate from/to/reason fields -- not
// just SOME log output, the specific mechanism setState (logging.go)
// adds.
func TestStateTransitionsAreLoggedAtDebugLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := OpenNode(7, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	n.SetLogger(logger)
	n.Start()
	defer n.Stop()

	waitForLeaderLogging(t, n, 3*time.Second)

	var transitions []map[string]any
	for _, rec := range parseLogLines(t, &buf) {
		if rec["msg"] == "state transition" {
			transitions = append(transitions, rec)
		}
	}

	if len(transitions) < 2 {
		t.Fatalf("got %d \"state transition\" line(s), want at least 2 (follower->candidate, candidate->leader); all lines: %+v",
			len(transitions), parseLogLines(t, &buf))
	}

	first, second := transitions[0], transitions[1]
	if first["from"] != "follower" || first["to"] != "candidate" {
		t.Errorf("transitions[0] = %+v, want from=follower to=candidate", first)
	}
	if first["reason"] != "election timeout" {
		t.Errorf("transitions[0].reason = %v, want \"election timeout\"", first["reason"])
	}
	if second["from"] != "candidate" || second["to"] != "leader" {
		t.Errorf("transitions[1] = %+v, want from=candidate to=leader", second)
	}
	if second["reason"] != "won election" {
		t.Errorf("transitions[1].reason = %v, want \"won election\"", second["reason"])
	}

	for _, tr := range transitions {
		if tr["level"] != "DEBUG" {
			t.Errorf("transition %+v: level = %v, want \"DEBUG\"", tr, tr["level"])
		}
		if tr["node"] != float64(7) { // json.Unmarshal decodes numbers as float64
			t.Errorf("transition %+v: node = %v, want 7", tr, tr["node"])
		}
	}
}

// TestStateTransitionsDoNotAppearAboveDebugLevel is the other half of
// "a DEBUG MODE that dumps" -- the same election, at an Info-level
// logger, must produce zero "state transition" lines. Debug-gating is
// the entire mechanism (internal/logging's own doc); this proves it
// actually gates, not merely that Debug-level lines look right when
// Debug happens to be on.
func TestStateTransitionsDoNotAppearAboveDebugLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := OpenNode(7, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	n.SetLogger(logger)
	n.Start()
	defer n.Stop()

	waitForLeaderLogging(t, n, 3*time.Second)

	for _, rec := range parseLogLines(t, &buf) {
		if rec["msg"] == "state transition" {
			t.Fatalf("a \"state transition\" line appeared at Info level: %+v -- these must only appear when Debug is enabled", rec)
		}
	}
}

// TestRepeatedFollowerAssignmentIsNotLoggedAsATransition is setState's
// own no-op guard, checked directly: installsnapshot.go's own call
// site sets Follower even when the node is already Follower (the
// Candidate case having already converted via an inner becomeFollower
// call moments earlier) -- that must not produce a second, misleading
// "state transition" line claiming follower->follower.
func TestRepeatedFollowerAssignmentIsNotLoggedAsATransition(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	storage, err := NewFileStorage(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := OpenNode(1, nil, leaderHintNoopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	n.SetLogger(logger)
	defer n.Stop()

	// Before Start: n is Follower already (NewNode/OpenNode's own
	// initial value, not a transition). Calling setState directly with
	// the SAME state it already holds must produce no line at all --
	// exercised here without needing to drive the node through a real
	// InstallSnapshot exchange just to reach that one call site.
	n.mu.Lock()
	n.setState(Follower, "test: repeated assignment")
	n.mu.Unlock()

	for _, rec := range parseLogLines(t, &buf) {
		if rec["msg"] == "state transition" {
			t.Fatalf("setState(Follower, ...) on an already-Follower node logged a transition: %+v, want none", rec)
		}
	}
}
