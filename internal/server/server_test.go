package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
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

// TestScanAndWatchAreUnimplemented locks in the embedding-based scope
// fence itself: Server never overrides Scan or Watch, so both must fall
// through to heliosv1.UnimplementedHeliosServer's own codes.Unimplemented
// answer until F-6 and F-7 implement them for real.
func TestScanAndWatchAreUnimplemented(t *testing.T) {
	dir := t.TempDir()
	n := newTestNode(t, dir)
	n.Start()
	s := newTestServer(t, dir, n)
	waitForLeader(t, s, 3*time.Second)

	_, err := s.Scan(context.Background(), &heliosv1.ScanRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Errorf("Scan: err = %v, want codes.Unimplemented", err)
	}

	err = s.Watch(&heliosv1.WatchRequest{}, nil)
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Errorf("Watch: err = %v, want codes.Unimplemented", err)
	}
}
