package history

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/ekushal02/helios/client"
	"github.com/ekushal02/helios/internal/kvstore"
	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/server"
	"github.com/ekushal02/helios/internal/storage/sstable"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
)

// noopTransport, startRealServer: the identical real-single-node-server
// pattern client/client_test.go already uses, duplicated here rather than
// exported from that package -- every other package in this project keeps
// its own test harness self-contained rather than sharing test-only code
// across package boundaries (internal/raft's own harness_test.go is the
// clearest example), and this package's own tests are few enough that the
// duplication costs less than a shared, cross-package test-helper surface
// would.
type noopTransport struct{}

func (noopTransport) SendRequestVote(int, *raft.RequestVoteArgs, *raft.RequestVoteReply) bool {
	return false
}
func (noopTransport) SendPreVote(int, *raft.PreVoteArgs, *raft.PreVoteReply) bool { return false }
func (noopTransport) SendAppendEntries(int, *raft.AppendEntriesArgs, *raft.AppendEntriesReply) bool {
	return false
}
func (noopTransport) SendInstallSnapshot(int, *raft.InstallSnapshotArgs, *raft.InstallSnapshotReply) bool {
	return false
}

func startRealServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	storage, err := raft.NewFileStorage(filepath.Join(dir, "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := raft.OpenNode(1, nil, noopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	n.Start()
	t.Cleanup(n.Stop)

	cache := sstable.NewBlockCache(1 << 20)
	m, err := kvstore.NewMachine(n, filepath.Join(dir, "kv"), cache, kvstore.DefaultOptions)
	if err != nil {
		t.Fatalf("NewMachine: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	// Waited out HERE, before any client ever dials in -- deliberately
	// different from client/client_test.go's own startRealServer, which
	// this was otherwise duplicated from unchanged. That package has its
	// own dedicated test (TestPutSucceedsAfterRetryingThroughPreElectionNotLeader)
	// for the specific claim that a client's own retry loop can carry a
	// call through a still-electing cluster, and deliberately does NOT
	// wait here so that test has a real pre-election window to exercise.
	// None of THIS package's own tests are testing that; they all just
	// want a ready server as their starting point. Relying on the retry
	// budget for that instead surfaced as a real, if intermittent, flake:
	// "exhausted retry attempts... not the leader" on a run where several
	// other tests in this file had already spun up their own real
	// clusters moments before, under -race -count=3, on a real machine --
	// enough accumulated load that one single-node election occasionally
	// took longer than the client's own default 8-attempt budget allows.
	// Waiting here removes that dependency entirely for every test that
	// doesn't need it.
	waitForLeader(t, m, 3*time.Second)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	gs := grpc.NewServer()
	heliosv1.RegisterHeliosServer(gs, server.New(n, m))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	return lis.Addr().String()
}

// waitForLeader polls Machine.Put with a harmless probe write until this
// node reports itself leader -- Put returns ErrNotLeader immediately,
// without blocking, when it isn't (kvstore/read_snapshot.go's own write:
// "idx, term, isLeader := m.n.Submit(cmd); if !isLeader { return
// ErrNotLeader }"), so this is a bounded poll, not a real retry loop. A
// single-node cluster is always eventually its own majority.
func waitForLeader(t *testing.T, m *kvstore.Machine, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if err := m.Put([]byte("__probe__"), nil); err == nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("machine did not report a leader within %v", within)
}

func newRealClient(t *testing.T) *client.Client {
	t.Helper()
	addr := startRealServer(t)
	c, err := client.New(client.Config{Peers: map[int]string{1: addr}})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// =============================================================================
// Every operation kind, logged
// =============================================================================

// TestRecordingClientLogsInvokeAndReturnForEveryOperation drives one of
// each recorded operation against a real single-node server and checks
// that every one produced exactly two events -- an invoke and a matching
// return, sharing an OpID, in that order -- with Input/Output content that
// actually reflects what was called and what came back. This is the
// literal task: log every client operation's invoke and return, with
// timestamps.
func TestRecordingClientLogsInvokeAndReturnForEveryOperation(t *testing.T) {
	c := newRealClient(t)
	rec := NewRecorder()
	rc := Wrap(c, rec, 0)
	ctx := context.Background()

	if _, err := rc.Put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, _, err := rc.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, _, _, err := rc.GetStale(ctx, []byte("a")); err != nil {
		t.Fatalf("GetStale: %v", err)
	}
	if _, _, err := rc.Scan(ctx, []byte("a"), nil, 10, nil); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if _, _, err := rc.ScanStale(ctx, []byte("a"), nil, 10, nil); err != nil {
		t.Fatalf("ScanStale: %v", err)
	}
	if _, err := rc.ScanAll(ctx, []byte("a"), nil, 10); err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	if _, err := rc.Delete(ctx, []byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	wantKinds := []Kind{KindPut, KindGet, KindGetStale, KindScan, KindScanStale, KindScanAll, KindDelete}
	events := rec.Events()
	if len(events) != 2*len(wantKinds) {
		t.Fatalf("recorded %d events for %d operations, want %d (one invoke, one return, each)",
			len(events), len(wantKinds), 2*len(wantKinds))
	}

	// Events arrive in invoke/return INTERLEAVING order (Recorder.Events'
	// own doc), which for this test's own strictly sequential calls is
	// exactly: invoke(op0), return(op0), invoke(op1), return(op1), ...
	for i, kind := range wantKinds {
		inv, ret := events[2*i], events[2*i+1]
		if !inv.IsInvoke || ret.IsInvoke {
			t.Fatalf("operation %d (%s): event order = (IsInvoke=%v, IsInvoke=%v), want (true, false)",
				i, kind, inv.IsInvoke, ret.IsInvoke)
		}
		if inv.Kind != kind || ret.Kind != kind {
			t.Errorf("operation %d: Kind = (%s, %s), want (%s, %s)", i, inv.Kind, ret.Kind, kind, kind)
		}
		if inv.OpID != ret.OpID {
			t.Errorf("operation %d (%s): invoke OpID %d != return OpID %d", i, kind, inv.OpID, ret.OpID)
		}
		if inv.ClientID != 0 || ret.ClientID != 0 {
			t.Errorf("operation %d (%s): ClientID = (%d, %d), want (0, 0)", i, kind, inv.ClientID, ret.ClientID)
		}
		if ret.Timestamp.Before(inv.Timestamp) {
			t.Errorf("operation %d (%s): return timestamp %v is before its own invoke timestamp %v",
				i, kind, ret.Timestamp, inv.Timestamp)
		}
	}

	// Content spot-checks: the FIRST invoke (Put) actually carries the
	// key/value it was called with, and the SECOND return (Get) actually
	// carries the value the server sent back -- not just the right shape,
	// the right data.
	putIn, ok := events[0].Input.(PutInput)
	if !ok {
		t.Fatalf("Put invoke Input has type %T, want PutInput", events[0].Input)
	}
	if string(putIn.Key) != "a" || string(putIn.Value) != "1" {
		t.Errorf("Put invoke Input = %+v, want Key=a Value=1", putIn)
	}
	getOut, ok := events[3].Output.(GetOutput)
	if !ok {
		t.Fatalf("Get return Output has type %T, want GetOutput", events[3].Output)
	}
	if !getOut.Ok || string(getOut.Value) != "1" {
		t.Errorf("Get return Output = %+v, want Ok=true Value=1", getOut)
	}
}

// TestRecordedInputIsNotAliasedToCallerMemory confirms copyBytes is doing
// its job: mutating the caller's own key slice AFTER the call returns must
// not change what was recorded.
func TestRecordedInputIsNotAliasedToCallerMemory(t *testing.T) {
	c := newRealClient(t)
	rec := NewRecorder()
	rc := Wrap(c, rec, 0)
	ctx := context.Background()

	key := []byte("mutate-me")
	if _, err := rc.Put(ctx, key, []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	copy(key, "AAAAAAAAA")

	events := rec.Events()
	in, ok := events[0].Input.(PutInput)
	if !ok {
		t.Fatalf("Input has type %T, want PutInput", events[0].Input)
	}
	if string(in.Key) != "mutate-me" {
		t.Errorf("recorded key = %q after the caller's own buffer was mutated, want %q "+
			"(the recorded Event must hold its own copy)", in.Key, "mutate-me")
	}
}

// =============================================================================
// Concurrency
// =============================================================================

// TestConcurrentClientsGetDistinctOpIDsAndCompleteCounts drives several
// RecordingClients, each with its own clientID, sharing ONE Recorder,
// concurrently -- the exact shape a chaos scenario or a benchmark actually
// uses this package under. No two events may share an OpID unless one is
// the other's own matching return, and the final count must be exactly
// two per operation actually issued.
func TestConcurrentClientsGetDistinctOpIDsAndCompleteCounts(t *testing.T) {
	c := newRealClient(t)
	rec := NewRecorder()

	const actors = 5
	const opsPerActor = 20
	var wg sync.WaitGroup
	for a := 0; a < actors; a++ {
		wg.Add(1)
		go func(actorID int) {
			defer wg.Done()
			rc := Wrap(c, rec, actorID)
			ctx := context.Background()
			for i := 0; i < opsPerActor; i++ {
				key := []byte(fmt.Sprintf("actor-%d-key-%d", actorID, i))
				if _, err := rc.Put(ctx, key, []byte("v")); err != nil {
					t.Errorf("actor %d put %d: %v", actorID, i, err)
					return
				}
			}
		}(a)
	}
	wg.Wait()

	events := rec.Events()
	wantEvents := 2 * actors * opsPerActor
	if len(events) != wantEvents {
		t.Fatalf("recorded %d events for %d actors x %d ops, want %d", len(events), actors, opsPerActor, wantEvents)
	}

	invokes := make(map[int64]bool)
	returns := make(map[int64]bool)
	for _, e := range events {
		if e.IsInvoke {
			if invokes[e.OpID] {
				t.Errorf("OpID %d invoked twice", e.OpID)
			}
			invokes[e.OpID] = true
		} else {
			if returns[e.OpID] {
				t.Errorf("OpID %d returned twice", e.OpID)
			}
			returns[e.OpID] = true
		}
	}
	if len(invokes) != actors*opsPerActor || len(returns) != actors*opsPerActor {
		t.Errorf("distinct invoked OpIDs = %d, distinct returned OpIDs = %d, want %d each",
			len(invokes), len(returns), actors*opsPerActor)
	}
	for id := range invokes {
		if !returns[id] {
			t.Errorf("OpID %d was invoked but never returned", id)
		}
	}
}

// =============================================================================
// JSON Lines persistence
// =============================================================================

// TestWriteJSONLThenReadJSONLRoundTrips writes a real recorded history to a
// buffer, reads it back, and confirms every field -- including Input/Output,
// decoded via DecodeInput/DecodeOutput back into the SAME concrete types
// they were recorded with -- survives unchanged.
func TestWriteJSONLThenReadJSONLRoundTrips(t *testing.T) {
	c := newRealClient(t)
	rec := NewRecorder()
	rc := Wrap(c, rec, 7)
	ctx := context.Background()

	if _, err := rc.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, _, err := rc.Get(ctx, []byte("k")); err != nil {
		t.Fatalf("Get: %v", err)
	}

	original := rec.Events()

	var buf bytes.Buffer
	if err := WriteJSONL(&buf, original); err != nil {
		t.Fatalf("WriteJSONL: %v", err)
	}

	decoded, err := ReadJSONL(&buf)
	if err != nil {
		t.Fatalf("ReadJSONL: %v", err)
	}
	if len(decoded) != len(original) {
		t.Fatalf("ReadJSONL returned %d events, want %d", len(decoded), len(original))
	}

	for i, orig := range original {
		got := decoded[i]
		if got.ClientID != orig.ClientID || got.OpID != orig.OpID || got.Kind != orig.Kind || got.IsInvoke != orig.IsInvoke {
			t.Errorf("event %d: decoded (client=%d op=%d kind=%s invoke=%v) != original (client=%d op=%d kind=%s invoke=%v)",
				i, got.ClientID, got.OpID, got.Kind, got.IsInvoke, orig.ClientID, orig.OpID, orig.Kind, orig.IsInvoke)
		}
		if !got.Timestamp.Equal(orig.Timestamp) {
			t.Errorf("event %d: decoded timestamp %v != original %v", i, got.Timestamp, orig.Timestamp)
		}
	}

	// Kind-specific decode: the Put invoke's own Input, and the Get
	// return's own Output, round-tripped back into their exact original Go
	// types via DecodeInput/DecodeOutput.
	var putIn PutInput
	if err := decoded[0].DecodeInput(&putIn); err != nil {
		t.Fatalf("DecodeInput: %v", err)
	}
	if string(putIn.Key) != "k" || string(putIn.Value) != "v" {
		t.Errorf("decoded PutInput = %+v, want Key=k Value=v", putIn)
	}

	var getOut GetOutput
	if err := decoded[3].DecodeOutput(&getOut); err != nil {
		t.Fatalf("DecodeOutput: %v", err)
	}
	if !getOut.Ok || string(getOut.Value) != "v" {
		t.Errorf("decoded GetOutput = %+v, want Ok=true Value=v", getOut)
	}
}

// TestReadJSONLSkipsBlankLinesAndRejectsMalformedOnes covers both halves of
// ReadJSONL's own tolerance: a blank line (the seam between two
// concatenated files, say) is skipped silently; anything else that fails
// to parse is a hard, line-numbered error, not a silently dropped record.
func TestReadJSONLSkipsBlankLinesAndRejectsMalformedOnes(t *testing.T) {
	good := `{"client_id":0,"op_id":0,"kind":"Get","is_invoke":true,"timestamp":"2026-01-01T00:00:00Z","input":{"key":"a2E="}}`

	t.Run("blank line skipped", func(t *testing.T) {
		input := good + "\n\n" + good + "\n"
		decoded, err := ReadJSONL(bytes.NewBufferString(input))
		if err != nil {
			t.Fatalf("ReadJSONL: %v", err)
		}
		if len(decoded) != 2 {
			t.Fatalf("got %d events, want 2 (the blank line must not produce a third)", len(decoded))
		}
	})

	t.Run("malformed line rejected", func(t *testing.T) {
		input := good + "\n" + "{not valid json" + "\n"
		_, err := ReadJSONL(bytes.NewBufferString(input))
		if err == nil {
			t.Fatal("ReadJSONL on a malformed line: err = nil, want an error naming the line")
		}
	})
}

// TestKindJSONRoundTripsByName confirms Kind's own MarshalJSON/UnmarshalJSON
// pair, directly: every defined Kind survives a round trip, and an unknown
// name is rejected rather than silently mapped to some default.
func TestKindJSONRoundTripsByName(t *testing.T) {
	kinds := []Kind{KindGet, KindGetStale, KindPut, KindDelete, KindScan, KindScanStale, KindScanAll}
	for _, k := range kinds {
		b, err := k.MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON(%v): %v", k, err)
		}
		var got Kind
		if err := got.UnmarshalJSON(b); err != nil {
			t.Fatalf("UnmarshalJSON(%s): %v", b, err)
		}
		if got != k {
			t.Errorf("round trip: %v -> %s -> %v, want %v", k, b, got, k)
		}
	}

	var k Kind
	if err := k.UnmarshalJSON([]byte(`"NotARealKind"`)); err == nil {
		t.Error("UnmarshalJSON on an unrecognized name: err = nil, want an error")
	}
}

// =============================================================================
// Timestamps
// =============================================================================

// TestReturnTimestampNeverPrecedesItsOwnInvoke is the literal claim
// timestamps exist to support: for any operation actually recorded, its
// return happened at or after its own invoke, measured against the real
// wall clock, not just internally consistent ordering in the events slice.
func TestReturnTimestampNeverPrecedesItsOwnInvoke(t *testing.T) {
	c := newRealClient(t)
	rec := NewRecorder()
	rc := Wrap(c, rec, 0)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		before := time.Now()
		if _, err := rc.Put(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		after := time.Now()

		events := rec.Events()
		inv, ret := events[len(events)-2], events[len(events)-1]
		if inv.Timestamp.Before(before) || inv.Timestamp.After(after) {
			t.Errorf("op %d: invoke timestamp %v outside the call's own [%v, %v] window", i, inv.Timestamp, before, after)
		}
		if ret.Timestamp.Before(inv.Timestamp) {
			t.Errorf("op %d: return timestamp %v before its own invoke timestamp %v", i, ret.Timestamp, inv.Timestamp)
		}
		if ret.Timestamp.After(after) {
			t.Errorf("op %d: return timestamp %v after the call was observed to have already returned (%v)", i, ret.Timestamp, after)
		}
	}
}