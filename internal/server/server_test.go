package server

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
	"github.com/ekushal02/helios/internal/kvstore"
	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// noopTransport never sends anything -- a single-node cluster (peers ==
// nil) never has anyone to send to. The identical shape
// kvstore/machine_test.go's own noopTransport already uses; duplicated
// here rather than shared because it is unexported in that package and
// this package's tests have no other reason to import kvstore's test
// files.
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

func newTestNode(t *testing.T, dir string) *raft.Node {
	t.Helper()
	storage, err := raft.NewFileStorage(filepath.Join(dir, "raft"))
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	n, err := raft.OpenNode(1, nil, noopTransport{}, 1, storage)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	t.Cleanup(n.Stop)
	return n
}

func newTestServer(t *testing.T, dir string, n *raft.Node) *Server {
	t.Helper()
	cache := sstable.NewBlockCache(1 << 20)
	m, err := kvstore.NewMachine(n, filepath.Join(dir, "kv"), cache, kvstore.DefaultOptions)
	if err != nil {
		t.Fatalf("NewMachine: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return New(n, m)
}

// waitForLeader polls Put with a harmless probe key until the server
// reports success -- dogfooding the RPC handler under test itself,
// rather than reaching around it into raft.Node.Submit or kvstore's own
// unexported codec, which this package cannot reach anyway.
func waitForLeader(t *testing.T, s *Server, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		_, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte("__probe__")})
		if err == nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no leader within %v", within)
}

func TestPutThenGetRoundTripsThroughTheServer(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := s.Get(context.Background(), &heliosv1.GetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "1" {
		t.Fatalf("Get(a) = (found=%v, value=%q), want (true, \"1\")", resp.GetFound(), resp.GetValue())
	}
	if resp.GetRevision() <= 0 {
		t.Errorf("Get(a).Revision = %d, want > 0 after a real committed write", resp.GetRevision())
	}
}

func TestGetOnAMissingKeyReportsNotFound(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	resp, err := s.Get(context.Background(), &heliosv1.GetRequest{Key: []byte("never-written")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetFound() {
		t.Errorf("Get(never-written).Found = true, want false")
	}
	if len(resp.GetValue()) != 0 {
		t.Errorf("Get(never-written).Value = %q, want empty", resp.GetValue())
	}
}

func TestDeleteThenGetReportsAbsent(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	delResp, err := s.Delete(context.Background(), &heliosv1.DeleteRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Locks in the documented gap: Found is always false today. This
	// assertion should be inverted the day Delete's Found is actually
	// implemented (see the doc comment on Server.Delete).
	if delResp.GetFound() {
		t.Errorf("Delete(a).Found = true, want false (unimplemented -- see Server.Delete's doc)")
	}

	getResp, err := s.Get(context.Background(), &heliosv1.GetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if getResp.GetFound() {
		t.Errorf("Get(a) after Delete: Found = true, want false")
	}
}

func TestGetStaleConsistencyUsesLeaseRead(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := s.Get(context.Background(), &heliosv1.GetRequest{
		Key:         []byte("a"),
		Consistency: heliosv1.Consistency_CONSISTENCY_STALE,
	})
	if err != nil {
		t.Fatalf("Get(CONSISTENCY_STALE): %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "1" {
		t.Fatalf("Get(a, STALE) = (found=%v, value=%q), want (true, \"1\") -- "+
			"a fresh single-node leader should have a valid lease", resp.GetFound(), resp.GetValue())
	}
}

// TestGetBeforeAnyElectionReportsNotLeaderWithHint is the point of this
// whole task: before a single-node cluster has won its own first
// election, every RPC must answer NotLeader, and the leader hint
// attached to that status must be checkable by a caller (a future
// client library, F-3) rather than only human-readable in an error
// string.
func TestGetBeforeAnyElectionReportsNotLeaderWithHint(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start() // deliberately no waitForLeader -- this test needs the pre-election window
	s := newTestServer(t, dir, n)

	_, err := s.Get(context.Background(), &heliosv1.GetRequest{Key: []byte("a")})
	if err == nil {
		t.Fatal("Get before any election: err = nil, want NotLeader")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Get before any election: err = %v, want a gRPC status error", err)
	}
	if st.Code() != codes.Unavailable {
		t.Errorf("Get before any election: code = %v, want %v", st.Code(), codes.Unavailable)
	}

	var hint *heliosv1.NotLeaderDetail
	for _, d := range st.Details() {
		if nl, ok := d.(*heliosv1.NotLeaderDetail); ok {
			hint = nl
		}
	}
	if hint == nil {
		t.Fatalf("Get before any election: status has no NotLeaderDetail among %d detail(s)", len(st.Details()))
	}
	if hint.GetLeaderId() != int64(raft.None) {
		t.Errorf("NotLeaderDetail.LeaderId = %d, want %d (raft.None -- no election has completed)",
			hint.GetLeaderId(), raft.None)
	}
}

// TestPutBeforeAnyElectionReportsNotLeader is the write-path half of the
// above -- Put and Delete go through Machine.write, a different call
// path from Get's Machine.Get/GetLeaseRead, and both need the same
// translation covered independently rather than assumed from Get's own
// passing test.
func TestPutBeforeAnyElectionReportsNotLeader(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)

	_, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte("a"), Value: []byte("1")})
	if err == nil {
		t.Fatal("Put before any election: err = nil, want NotLeader")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("Put before any election: err = %v, want a codes.Unavailable status", err)
	}
}

// fakeWatchStream implements heliosv1.Helios_WatchServer (a
// grpc.ServerStreamingServer[WatchResponse]) directly -- the identical
// "direct method call, not a real gRPC connection" testing convention
// every other test in this file already uses for Get/Put/Delete/Scan.
// Only Context() and Send() are ever actually exercised by
// Server.Watch's own implementation; the rest of grpc.ServerStream's
// interface is satisfied with no-ops purely to compile.
type fakeWatchStream struct {
	ctx context.Context

	mu  sync.Mutex
	got []*heliosv1.WatchResponse
}

func (f *fakeWatchStream) Send(resp *heliosv1.WatchResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, resp)
	return nil
}
func (f *fakeWatchStream) Context() context.Context   { return f.ctx }
func (f *fakeWatchStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeWatchStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeWatchStream) SetTrailer(metadata.MD)       {}
func (f *fakeWatchStream) SendMsg(m interface{}) error  { return nil }
func (f *fakeWatchStream) RecvMsg(m interface{}) error  { return nil }

func (f *fakeWatchStream) events() []*heliosv1.WatchEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []*heliosv1.WatchEvent
	for _, r := range f.got {
		all = append(all, r.GetEvents()...)
	}
	return all
}

// TestWatchStreamsOnlyMatchingPrefixLive proves the actual point of
// this task at the RPC boundary: a live Watch delivers a Put matching
// its own KeyPrefix and does NOT deliver one that doesn't, with the
// correct Type/Key/Value on the wire.
func TestWatchStreamsOnlyMatchingPrefixLive(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &fakeWatchStream{ctx: ctx}

	watchDone := make(chan error, 1)
	go func() {
		watchDone <- s.Watch(&heliosv1.WatchRequest{KeyPrefix: []byte("user/")}, fs)
	}()

	// A short, generous pause for the goroutine above to actually reach
	// Machine.Watch's own subscribe call before either Put below runs
	// -- Watch(0) is live-only, no replay, so a write that lands before
	// subscription completes would never be seen at all. The same
	// bounded-sleep-before-a-background-goroutine pattern
	// integration_test.go's own snapshot tests already use.
	time.Sleep(150 * time.Millisecond)

	if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte("user/1"), Value: []byte("alice")}); err != nil {
		t.Fatalf("Put(user/1): %v", err)
	}
	if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte("other/1"), Value: []byte("nope")}); err != nil {
		t.Fatalf("Put(other/1): %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for len(fs.events()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after its own context was canceled")
	}

	events := fs.events()
	if len(events) != 1 {
		t.Fatalf("got %d event(s), want exactly 1 (only the \"user/\"-prefixed Put)", len(events))
	}
	if string(events[0].GetKey()) != "user/1" || string(events[0].GetValue()) != "alice" {
		t.Errorf("event = (%q, %q), want (\"user/1\", \"alice\")", events[0].GetKey(), events[0].GetValue())
	}
	if events[0].GetType() != heliosv1.WatchEvent_PUT {
		t.Errorf("event.Type = %v, want PUT", events[0].GetType())
	}
	if events[0].GetRevision() <= 0 {
		t.Errorf("event.Revision = %d, want > 0", events[0].GetRevision())
	}
}

// TestWatchReportsOutOfRangeForAnEvictedStartRevision is
// Machine.Watch's own ok=false path (internal/kvstore/watch_test.go's
// TestWatchReportsGapWhenStartRevisionHasBeenEvicted), checked at the
// RPC boundary: it must surface as codes.OutOfRange, not a generic
// error or a silently-degraded watch.
func TestWatchReportsOutOfRangeForAnEvictedStartRevision(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()

	cache := sstable.NewBlockCache(1 << 20)
	opts := kvstore.DefaultOptions
	opts.WatchHistoryCapacity = 5
	m, err := kvstore.NewMachine(n, filepath.Join(dir, "kv"), cache, opts)
	if err != nil {
		t.Fatalf("NewMachine: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	s := New(n, m)
	waitForLeader(t, s, 3*time.Second)

	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: key, Value: []byte("v")}); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	fs := &fakeWatchStream{ctx: context.Background()}
	err = s.Watch(&heliosv1.WatchRequest{StartRevision: 1}, fs)
	if err == nil {
		t.Fatal("Watch(start_revision=1): err = nil, want codes.OutOfRange")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.OutOfRange {
		t.Errorf("Watch(start_revision=1): err = %v, want codes.OutOfRange", err)
	}
}

// TestScanReturnsPairsInRange is Scan's own basic round trip at the RPC
// boundary -- the kvstore-level mechanism is already thoroughly tested
// in internal/kvstore/scan_test.go; this and the tests below exist to
// prove the translation (wire types, defaults, pagination cursor
// handoff) is correct, not to re-prove the merge itself.
func TestScanReturnsPairsInRange(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	for _, k := range []string{"a", "b", "c"} {
		if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte(k), Value: []byte("v-" + k)}); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	// StartKey: "a" -- not the ScanRequest zero value, which is
	// unbounded from the very beginning of the key space. waitForLeader
	// (above) itself Puts a real, durable "__probe__" key to detect
	// leader election, and "_" (0x5F) sorts before "a" (0x61), so an
	// unbounded scan would silently pick it up as a fourth pair.
	resp, err := s.Scan(context.Background(), &heliosv1.ScanRequest{StartKey: []byte("a")})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(resp.GetNextPageToken()) != 0 {
		t.Errorf("NextPageToken = %q, want empty (everything fit in one page)", resp.GetNextPageToken())
	}
	pairs := resp.GetPairs()
	if len(pairs) != 3 {
		t.Fatalf("Scan returned %d pairs, want 3", len(pairs))
	}
	for i, want := range []string{"a", "b", "c"} {
		if string(pairs[i].GetKey()) != want || string(pairs[i].GetValue()) != "v-"+want {
			t.Errorf("pairs[%d] = (%q, %q), want (%q, %q)", i, pairs[i].GetKey(), pairs[i].GetValue(), want, "v-"+want)
		}
		if pairs[i].GetRevision() <= 0 {
			t.Errorf("pairs[%d].Revision = %d, want > 0", i, pairs[i].GetRevision())
		}
	}
}

// TestScanPaginatesUsingNextPageTokenAsTheNextStartKey is F-6's own
// pagination contract, checked at the wire boundary: repeatedly calling
// Scan with each response's own NextPageToken fed back as the next
// request's PageToken must converge to the full set, matching a single
// unlimited Scan exactly.
func TestScanPaginatesUsingNextPageTokenAsTheNextStartKey(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	const count = 23
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte(key), Value: []byte("v")}); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	var got []string
	var pageToken []byte
	pages := 0
	for {
		pages++
		if pages > count {
			t.Fatalf("did not converge after %d pages -- pagination is looping", pages)
		}
		// StartKey: "a" matters only for the first page (PageToken is
		// empty then); every later page's PageToken overrides it
		// anyway. Without it, page one would pick up waitForLeader's
		// own "__probe__" key -- see TestScanReturnsPairsInRange's own
		// comment on why.
		resp, err := s.Scan(context.Background(), &heliosv1.ScanRequest{StartKey: []byte("a"), Limit: 4, PageToken: pageToken})
		if err != nil {
			t.Fatalf("Scan (page %d): %v", pages, err)
		}
		for _, p := range resp.GetPairs() {
			got = append(got, string(p.GetKey()))
		}
		pageToken = resp.GetNextPageToken()
		if len(pageToken) == 0 {
			break
		}
	}

	if len(got) != count {
		t.Fatalf("paginated Scan returned %d keys across %d pages, want %d", len(got), pages, count)
	}
	for i, key := range got {
		want := fmt.Sprintf("key-%03d", i)
		if key != want {
			t.Errorf("got[%d] = %q, want %q", i, key, want)
		}
	}
}

// TestScanBeforeAnyElectionReportsNotLeader mirrors the identical
// pre-election test already established for Get and Put
// (TestGetBeforeAnyElectionReportsNotLeaderWithHint,
// TestPutBeforeAnyElectionReportsNotLeader) -- Scan goes through the
// same ReadIndex barrier Get does, so it must fail the same way before
// any leader exists.
func TestScanBeforeAnyElectionReportsNotLeader(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)

	_, err := s.Scan(context.Background(), &heliosv1.ScanRequest{})
	if err == nil {
		t.Fatal("Scan before any election: err = nil, want NotLeader")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("Scan before any election: err = %v, want a codes.Unavailable status", err)
	}
}

// TestPutIsIdempotentAcrossARetriedRPC (F-4) is server_test.go's own
// bridge between internal/kvstore's mechanism-level tests
// (idempotency_test.go) and what a real client.Client retry actually
// produces on the wire: two SEPARATE Put RPCs, same ClientId and
// SequenceNumber, different Value -- exactly the shape client.go's do[]
// loop builds when the first attempt's response is lost and it retries.
// The second call must succeed (not error) and must NOT have changed
// the stored value.
func TestPutIsIdempotentAcrossARetriedRPC(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	const clientID, seq = 99, 1
	if _, err := s.Put(context.Background(), &heliosv1.PutRequest{
		Key: []byte("a"), Value: []byte("v1"), ClientId: clientID, SequenceNumber: seq,
	}); err != nil {
		t.Fatalf("original Put: %v", err)
	}

	// The "retry": identical ClientId/SequenceNumber, a different Value
	// -- if this were mistakenly applied again, Get would come back
	// "v2", not "v1".
	if _, err := s.Put(context.Background(), &heliosv1.PutRequest{
		Key: []byte("a"), Value: []byte("v2"), ClientId: clientID, SequenceNumber: seq,
	}); err != nil {
		t.Fatalf("retried Put: %v (a deduplicated write must still return success)", err)
	}

	resp, err := s.Get(context.Background(), &heliosv1.GetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "v1" {
		t.Fatalf("Get(a) = (%q, found=%v), want (\"v1\", true) -- the retried Put must not have overwritten the original", resp.GetValue(), resp.GetFound())
	}
}

// TestPutRetryStormAppliesExactlyOnceAtTheRPCBoundary is
// TestRetryStormAppliesExactlyOnce (internal/kvstore/idempotency_test.go)
// proven one layer up, at the boundary a real storm of retries actually
// crosses: many concurrent Put RPCs sharing one ClientId/SequenceNumber,
// each targeting a distinct key so "exactly once" is a direct count of
// keys that exist afterward, not something inferred from which value
// happened to win an overwrite race (see the kvstore-level test's own
// doc for the full argument on why distinct keys, not distinct values
// at one shared key, is what makes this assertion airtight).
func TestPutRetryStormAppliesExactlyOnceAtTheRPCBoundary(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	const (
		clientID = 777
		seq      = 1
		storm    = 64
	)

	ready := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, storm)
	for i := 0; i < storm; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-ready
			_, err := s.Put(context.Background(), &heliosv1.PutRequest{
				Key:            []byte(fmt.Sprintf("storm-key-%02d", i)),
				Value:          []byte(fmt.Sprintf("storm-value-%02d", i)),
				ClientId:       clientID,
				SequenceNumber: seq,
			})
			errs[i] = err
		}(i)
	}
	close(ready)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("attempt %d: Put returned an error: %v -- every attempt in the storm must report success", i, err)
		}
	}

	applied := 0
	var appliedKeys []string
	for i := 0; i < storm; i++ {
		key := fmt.Sprintf("storm-key-%02d", i)
		resp, err := s.Get(context.Background(), &heliosv1.GetRequest{Key: []byte(key)})
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if resp.GetFound() {
			applied++
			appliedKeys = append(appliedKeys, key)
		}
	}
	if applied != 1 {
		t.Fatalf("a retry storm of %d concurrent Put RPCs sharing (ClientId=%d, SequenceNumber=%d) "+
			"resulted in %d key(s) actually applied (%v), want exactly 1", storm, clientID, seq, applied, appliedKeys)
	}
}

// TestStatusBeforeAnyElection is the RPC-boundary counterpart to
// internal/raft/status_test.go's own TestStatusBeforeAnyElection --
// the real point of Status not being leader-gated (unlike every other
// RPC's own pre-election test in this file): it must succeed and
// report real, if early, values, not a NotLeader error.
func TestStatusBeforeAnyElection(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n) // deliberately no waitForLeader

	resp, err := s.Status(context.Background(), &heliosv1.StatusRequest{})
	if err != nil {
		t.Fatalf("Status before any election: %v, want success (Status is not leader-gated)", err)
	}
	if resp.GetRaftState() != "follower" {
		t.Errorf("RaftState = %q, want \"follower\"", resp.GetRaftState())
	}
	if resp.GetLeaderId() != int64(raft.None) {
		t.Errorf("LeaderId = %d, want %d (raft.None)", resp.GetLeaderId(), raft.None)
	}
	if resp.GetFault() != "" {
		t.Errorf("Fault = %q, want empty on a healthy, freshly-opened Machine", resp.GetFault())
	}
}

// TestStatusAfterWinningAnElectionAndWriting proves every field that
// SHOULD change with real activity actually does, at the RPC boundary
// -- the same properties internal/raft/status_test.go already checks
// directly on Node.Status, checked here through the wire translation
// instead.
func TestStatusAfterWinningAnElectionAndWriting(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	if _, err := s.Put(context.Background(), &heliosv1.PutRequest{Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := s.Status(context.Background(), &heliosv1.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if resp.GetRaftState() != "leader" {
		t.Errorf("RaftState = %q, want \"leader\"", resp.GetRaftState())
	}
	if resp.GetTerm() < 1 {
		t.Errorf("Term = %d, want >= 1", resp.GetTerm())
	}
	if resp.GetCommitIndex() <= 0 || resp.GetLastApplied() <= 0 {
		t.Errorf("CommitIndex/LastApplied = %d/%d, want both > 0 after a real committed write", resp.GetCommitIndex(), resp.GetLastApplied())
	}
	if resp.GetLogLength() < 2 { // the leading placeholder plus at least the one real Put
		t.Errorf("LogLength = %d, want >= 2", resp.GetLogLength())
	}
	if len(resp.GetVoters()) != 1 || resp.GetVoters()[0] != 1 {
		t.Errorf("Voters = %v, want [1] (this single-node cluster's own id)", resp.GetVoters())
	}
	if resp.GetMachineAppliedIndex() <= 0 {
		t.Errorf("MachineAppliedIndex = %d, want > 0", resp.GetMachineAppliedIndex())
	}
	if resp.GetFault() != "" {
		t.Errorf("Fault = %q, want empty", resp.GetFault())
	}
}