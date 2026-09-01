package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
	"github.com/ekushal02/helios/internal/kvstore"
	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/server"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// =============================================================================
// A scripted fake server -- deterministic control over exactly the
// failure sequence a given test wants, the identical reason the Raft
// phase itself was built and tested against a fake in-memory transport
// before any real one existed (internal/raft's own harness). Most of
// what this task needs tested is CLIENT-side retry/backoff/redirect
// logic in response to specific server behaviors -- best tested against
// a controllable double, not only against the real (but currently
// single-node-only) system.
// =============================================================================

// fakeServer answers Get with a scripted sequence of responses, one per
// call, in order. Calling it more times than scripted is itself a test
// failure signal (codes.Internal), not a panic -- a scripted-out-of-
// responses call means the retry loop did something the test did not
// expect, which is exactly the kind of thing a test should FAIL on
// loudly rather than hang or crash on.
type fakeServer struct {
	heliosv1.UnimplementedHeliosServer
	mu        sync.Mutex
	responses []func() (*heliosv1.GetResponse, error)
	calls     int
}

func (f *fakeServer) Get(ctx context.Context, req *heliosv1.GetRequest) (*heliosv1.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls >= len(f.responses) {
		return nil, status.Errorf(codes.Internal, "fakeServer: call %d exceeds %d scripted responses", f.calls+1, len(f.responses))
	}
	fn := f.responses[f.calls]
	f.calls++
	return fn()
}

func (f *fakeServer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeScanServer scripts Scan responses the same way fakeServer scripts
// Get -- kept as its own small type rather than folded into fakeServer,
// since a scripted Scan response has a different shape than a scripted
// Get one and forcing them to share one responses slice would need an
// awkward union type for no real benefit given how few tests need it.
type fakeScanServer struct {
	heliosv1.UnimplementedHeliosServer
	mu        sync.Mutex
	responses []func() (*heliosv1.ScanResponse, error)
	calls     int
}

func (f *fakeScanServer) Scan(ctx context.Context, req *heliosv1.ScanRequest) (*heliosv1.ScanResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls >= len(f.responses) {
		return nil, status.Errorf(codes.Internal, "fakeScanServer: call %d exceeds %d scripted responses", f.calls+1, len(f.responses))
	}
	fn := f.responses[f.calls]
	f.calls++
	return fn()
}

// startFakeServer serves srv on a real, local, randomly-assigned TCP
// port and returns its address. A real listener and a real grpc.Server,
// not an in-process fake transport -- this Client dials over real gRPC,
// so its tests need a real server to dial, even a scripted one.
func startFakeServer(t *testing.T, srv heliosv1.HeliosServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	gs := grpc.NewServer()
	heliosv1.RegisterHeliosServer(gs, srv)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func okResponse(value string, revision int64) func() (*heliosv1.GetResponse, error) {
	return func() (*heliosv1.GetResponse, error) {
		return &heliosv1.GetResponse{Value: []byte(value), Found: true, Revision: revision}, nil
	}
}

func notLeaderResponse(leaderID int64) func() (*heliosv1.GetResponse, error) {
	return func() (*heliosv1.GetResponse, error) {
		st := status.New(codes.Unavailable, "not the leader")
		withDetails, err := st.WithDetails(&heliosv1.NotLeaderDetail{LeaderId: leaderID})
		if err != nil {
			return nil, st.Err()
		}
		return nil, withDetails.Err()
	}
}

func deadlineExceededResponse() func() (*heliosv1.GetResponse, error) {
	return func() (*heliosv1.GetResponse, error) {
		return nil, status.Error(codes.DeadlineExceeded, "read barrier did not apply in time")
	}
}

// fastRetryConfig keeps every retry-exercising test's own real wall-clock
// backoff tiny -- these tests are checking BRANCH BEHAVIOR (which peer
// got called, how many times, with or without a delay), not backoff
// timing itself, so there is no reason to make the test suite slow to
// prove it.
var fastRetryConfig = RetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}

// =============================================================================
// Branch 3: a known leader hint is followed immediately
// =============================================================================

func TestGetFollowsKnownLeaderHintImmediately(t *testing.T) {
	leaderAddr := startFakeServer(t, &fakeServer{responses: []func() (*heliosv1.GetResponse, error){
		okResponse("v1", 42),
	}})
	follower := &fakeServer{responses: []func() (*heliosv1.GetResponse, error){
		notLeaderResponse(2), // "2" is the leader's own peer ID, assigned below
	}}
	followerAddr := startFakeServer(t, follower)

	c, err := New(Config{
		Peers:        map[int]string{1: followerAddr, 2: leaderAddr},
		InitialGuess: 1, // deliberately start at the non-leader
		Retry:        fastRetryConfig,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	value, ok, revision, err := c.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "v1" || revision != 42 {
		t.Fatalf("Get = (%q, ok=%v, rev=%d), want (\"v1\", true, 42)", value, ok, revision)
	}
	if follower.callCount() != 1 {
		t.Errorf("follower called %d times, want exactly 1 -- a real hint should not be retried against the peer that gave it", follower.callCount())
	}
}

// =============================================================================
// Branch 4: an unknown hinted peer falls back to cycling
// =============================================================================

func TestGetFallsBackToCyclingOnUnknownHintedPeer(t *testing.T) {
	badGuess := startFakeServer(t, &fakeServer{responses: []func() (*heliosv1.GetResponse, error){
		notLeaderResponse(99), // 99 is not in this Client's Peers map at all
	}})
	realLeader := startFakeServer(t, &fakeServer{responses: []func() (*heliosv1.GetResponse, error){
		okResponse("v2", 7),
	}})

	c, err := New(Config{
		Peers:        map[int]string{1: badGuess, 2: realLeader},
		InitialGuess: 1,
		Retry:        fastRetryConfig,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	value, ok, revision, err := c.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "v2" || revision != 7 {
		t.Fatalf("Get = (%q, ok=%v, rev=%d), want (\"v2\", true, 7)", value, ok, revision)
	}
}

// =============================================================================
// Branch 5: DeadlineExceeded retries the SAME peer
// =============================================================================

func TestGetRetriesSamePeerOnDeadlineExceeded(t *testing.T) {
	srv := &fakeServer{responses: []func() (*heliosv1.GetResponse, error){
		deadlineExceededResponse(),
		deadlineExceededResponse(),
		okResponse("v3", 9),
	}}
	addr := startFakeServer(t, srv)

	c, err := New(Config{Peers: map[int]string{1: addr}, Retry: fastRetryConfig})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	value, ok, revision, err := c.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "v3" || revision != 9 {
		t.Fatalf("Get = (%q, ok=%v, rev=%d), want (\"v3\", true, 9)", value, ok, revision)
	}
	if srv.callCount() != 3 {
		t.Errorf("server called %d times, want exactly 3 (2 timeouts then success)", srv.callCount())
	}
}

// =============================================================================
// Branch 4 via a real dial failure, not a scripted one
// =============================================================================

func TestGetCyclesAwayFromAnUnreachablePeer(t *testing.T) {
	// A real listener, opened and immediately closed -- the port is
	// real and was briefly bound, so a connection attempt fails the
	// way an actually-down peer's would, rather than however an
	// obviously-never-valid address might fail differently.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	deadAddr := lis.Addr().String()
	lis.Close()

	aliveAddr := startFakeServer(t, &fakeServer{responses: []func() (*heliosv1.GetResponse, error){
		okResponse("v4", 3),
	}})

	c, err := New(Config{
		Peers:        map[int]string{1: deadAddr, 2: aliveAddr},
		InitialGuess: 1,
		Retry:        fastRetryConfig,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	value, ok, _, err := c.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "v4" {
		t.Fatalf("Get = (%q, ok=%v), want (\"v4\", true)", value, ok)
	}
}

// =============================================================================
// Exhausting every attempt returns ErrRetriesExhausted
// =============================================================================

func TestGetFailsAfterExhaustingRetries(t *testing.T) {
	responses := make([]func() (*heliosv1.GetResponse, error), fastRetryConfig.MaxAttempts)
	for i := range responses {
		responses[i] = notLeaderResponse(1) // hints itself -- an unhelpful but "known" peer, so it retries the same one every time without ever succeeding
	}
	addr := startFakeServer(t, &fakeServer{responses: responses})

	c, err := New(Config{Peers: map[int]string{1: addr}, Retry: fastRetryConfig})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	_, _, _, err = c.Get(context.Background(), []byte("k"))
	if err == nil {
		t.Fatal("Get: err = nil, want ErrRetriesExhausted")
	}
	if !errors.Is(err, ErrRetriesExhausted) {
		t.Errorf("Get: err = %v, want it to wrap ErrRetriesExhausted", err)
	}
}

// =============================================================================
// Context cancellation is respected, not overridden by retry budget
// =============================================================================

func TestGetRespectsContextCancellation(t *testing.T) {
	responses := make([]func() (*heliosv1.GetResponse, error), 100) // far more than the retry budget would ever consume on its own
	for i := range responses {
		responses[i] = deadlineExceededResponse()
	}
	addr := startFakeServer(t, &fakeServer{responses: responses})

	// A generous retry budget with real (not fast-test) backoff, so the
	// only thing that can plausibly stop this before the retry budget
	// itself would is the context.
	c, err := New(Config{
		Peers: map[int]string{1: addr},
		Retry: RetryConfig{MaxAttempts: 50, BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, _, err = c.Get(ctx, []byte("k"))
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Get: err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Get took %v to respect a 20ms context deadline -- retry budget overrode cancellation", elapsed)
	}
}

// =============================================================================
// Real end-to-end: an actual node, Machine, and Server over real gRPC
// =============================================================================

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

// startRealServer builds a real single-node raft.Node and kvstore.Machine
// (the identical shape internal/server/server_test.go's own newTestNode
// and newTestServer use) and serves them over a real gRPC listener,
// returning the address a Client can dial. Deliberately does NOT wait
// for the node to win its own election first -- see
// TestPutSucceedsAfterRetryingThroughPreElectionNotLeader, the whole
// point of this task, which needs that pre-election window still open.
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

func TestPutGetDeleteRoundTripAgainstARealSingleNodeServer(t *testing.T) {
	addr := startRealServer(t)
	c, err := New(Config{Peers: map[int]string{1: addr}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if _, err := c.Put(context.Background(), []byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, ok, revision, err := c.Get(context.Background(), []byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "1" || revision <= 0 {
		t.Fatalf("Get(a) = (%q, ok=%v, rev=%d), want (\"1\", true, >0)", value, ok, revision)
	}

	if _, err := c.Delete(context.Background(), []byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _, err = c.Get(context.Background(), []byte("a"))
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if ok {
		t.Error("Get(a) after Delete: ok = true, want false")
	}

	value, ok, _, err = c.GetStale(context.Background(), []byte("a"))
	if err != nil {
		t.Fatalf("GetStale after Delete: %v", err)
	}
	if ok {
		t.Errorf("GetStale(a) after Delete = (%q, true), want (_, false)", value)
	}
}

// TestPutSucceedsAfterRetryingThroughPreElectionNotLeader is the point
// of this whole task, proven end to end: call Put on a real,
// just-started, real single-node cluster BEFORE it has elected itself
// -- every attempt should hit real ErrNotLeader (internal/server's own
// translateErr, over real gRPC), and the Client's retry loop should
// carry it through to a real, eventual success once the node's own
// election completes -- with no special-casing anywhere in this test
// or in the client for "this is the pre-election case." It is just an
// ordinary NotLeader retry that happens to resolve via a real election
// rather than a real redirect to a different node, which is exactly
// the case F-2's own leaderhint_test.go covers at the raft.Node layer
// and this test covers at the full-stack layer.
func TestPutSucceedsAfterRetryingThroughPreElectionNotLeader(t *testing.T) {
	addr := startRealServer(t) // no waitForLeader anywhere -- the retry loop IS the wait
	c, err := New(Config{
		Peers: map[int]string{1: addr},
		Retry: RetryConfig{MaxAttempts: 30, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v (retry loop never carried the client through to a real election)", err)
	}

	value, ok, _, err := c.Get(context.Background(), []byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "1" {
		t.Fatalf("Get(a) = (%q, ok=%v), want (\"1\", true)", value, ok)
	}
}

// TestScanFollowsKnownLeaderHintImmediately proves do[] works correctly
// instantiated with ScanRequest/ScanResponse -- Scan is the first RPC
// besides Get to actually exercise the generic retry loop; this is the
// same hint-following branch TestGetFollowsKnownLeaderHintImmediately
// already proves for Get, checked here for Scan specifically rather
// than assumed to carry over untested.
func TestScanFollowsKnownLeaderHintImmediately(t *testing.T) {
	leaderAddr := startFakeServer(t, &fakeScanServer{responses: []func() (*heliosv1.ScanResponse, error){
		func() (*heliosv1.ScanResponse, error) {
			return &heliosv1.ScanResponse{Pairs: []*heliosv1.KeyValue{
				{Key: []byte("a"), Value: []byte("1"), Revision: 1},
			}}, nil
		},
	}})
	follower := &fakeScanServer{responses: []func() (*heliosv1.ScanResponse, error){
		func() (*heliosv1.ScanResponse, error) {
			st := status.New(codes.Unavailable, "not the leader")
			withDetails, err := st.WithDetails(&heliosv1.NotLeaderDetail{LeaderId: 2})
			if err != nil {
				return nil, st.Err()
			}
			return nil, withDetails.Err()
		},
	}}
	followerAddr := startFakeServer(t, follower)

	c, err := New(Config{
		Peers:        map[int]string{1: followerAddr, 2: leaderAddr},
		InitialGuess: 1,
		Retry:        fastRetryConfig,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	pairs, next, err := c.Scan(context.Background(), nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(next) != 0 {
		t.Errorf("nextPageToken = %q, want empty", next)
	}
	if len(pairs) != 1 || string(pairs[0].Key) != "a" || string(pairs[0].Value) != "1" {
		t.Fatalf("Scan = %+v, want one pair (a, 1)", pairs)
	}
}

// TestScanAndScanAllAgainstARealSingleNodeServer is the real end-to-end
// counterpart to internal/kvstore's and internal/server's own Scan
// tests, proven through the actual client a real caller uses: Scan
// returning one bounded page, and ScanAll aggregating every page into
// the full set.
func TestScanAndScanAllAgainstARealSingleNodeServer(t *testing.T) {
	addr := startRealServer(t)
	c, err := New(Config{Peers: map[int]string{1: addr}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	const count = 17
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		if _, err := c.Put(context.Background(), key, []byte("v")); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	pairs, next, err := c.Scan(context.Background(), nil, nil, 5, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(pairs) != 5 {
		t.Fatalf("Scan(limit=5) returned %d pairs, want 5", len(pairs))
	}
	if len(next) == 0 {
		t.Fatal("Scan(limit=5) nextPageToken is empty, want a continuation (17 keys exist)")
	}

	all, err := c.ScanAll(context.Background(), nil, nil, 5)
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	if len(all) != count {
		t.Fatalf("ScanAll returned %d pairs, want %d", len(all), count)
	}
	for i, kv := range all {
		want := fmt.Sprintf("key-%02d", i)
		if string(kv.Key) != want {
			t.Errorf("all[%d].Key = %q, want %q", i, kv.Key, want)
		}
	}
}

// TestNewRejectsEmptyPeers and TestNewRejectsUnknownInitialGuess lock in
// Config validation directly, rather than only via whatever error a
// misconfigured Client would eventually produce three layers down.
func TestNewRejectsEmptyPeers(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New(Config{}): err = nil, want an error for an empty Peers map")
	}
}

func TestNewRejectsUnknownInitialGuess(t *testing.T) {
	_, err := New(Config{Peers: map[int]string{1: "irrelevant:1"}, InitialGuess: 99})
	if err == nil {
		t.Fatal("New with InitialGuess not in Peers: err = nil, want an error")
	}
}