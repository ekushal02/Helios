package main

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
	"github.com/ekushal02/helios/internal/kvstore"
	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/server"
	"github.com/ekushal02/helios/internal/storage/sstable"
)

// noopTransport never sends anything -- a single-node cluster (peers ==
// nil) never has anyone to send to. Duplicated here rather than shared
// across packages, the identical shape every other test file in this
// project that needs one already takes (server_test.go, client_test.go
// each carry their own copy).
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

// startRealServer builds a real single-node raft.Node, kvstore.Machine,
// and internal/server.Server, serves them over a real gRPC listener,
// and waits for the node to win its own election before returning.
// Unlike client_test.go's own identically-shaped helper, which
// deliberately does NOT wait (proving the client library's own retry
// loop handles the pre-election window -- already covered there), this
// package's own tests are about the CLI's argument handling and output
// formatting, not about re-proving that mechanism, so waiting here
// keeps every assertion below focused on what heliosctl itself does.
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
	s := server.New(n, m)
	gs := grpc.NewServer()
	heliosv1.RegisterHeliosServer(gs, s)
	// F-9: status.go now calls the admin service directly (no longer
	// the old Get-probe workaround), so the test server needs to
	// register it too, exactly as cmd/helios/main.go now does.
	heliosv1.RegisterHeliosAdminServer(gs, s)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := s.Put(ctx, &heliosv1.PutRequest{Key: []byte("__probe__"), Value: nil})
		cancel()
		if err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	return lis.Addr().String()
}

func TestEndToEndAgainstARealSingleNodeServer(t *testing.T) {
	addr := startRealServer(t)
	peersFlag := "--peers=1=" + addr

	var stdout, stderr bytes.Buffer

	// put
	code := run([]string{"put", peersFlag, "user/1", "alice"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("put: exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("put stdout = %q, want OK", stdout.String())
	}

	// get (found)
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"get", peersFlag, "user/1"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get: exit code = %d, stderr = %q", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "alice" {
		t.Errorf("get stdout = %q, want \"alice\"", got)
	}
	if !strings.Contains(stderr.String(), "revision:") {
		t.Errorf("get stderr = %q, want a revision line", stderr.String())
	}

	// get (missing key)
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"get", peersFlag, "user/missing"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("get on a missing key: exit code = 0, want non-zero")
	}
	if !strings.Contains(stdout.String(), "key not found") {
		t.Errorf("get stdout = %q, want \"(key not found)\"", stdout.String())
	}

	// a second key so scan has a real range to walk
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"put", peersFlag, "user/2", "bob"}, &stdout, &stderr); code != 0 {
		t.Fatalf("put user/2: exit code = %d, stderr = %q", code, stderr.String())
	}

	// scan --all
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"scan", peersFlag, "--all"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("scan: exit code = %d, stderr = %q", code, stderr.String())
	}
	scanOut := stdout.String()
	if !strings.Contains(scanOut, "user/1\talice") {
		t.Errorf("scan stdout = %q, want a \"user/1\\talice\" line", scanOut)
	}
	if !strings.Contains(scanOut, "user/2\tbob") {
		t.Errorf("scan stdout = %q, want a \"user/2\\tbob\" line", scanOut)
	}

	// del
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"del", peersFlag, "user/1"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("del: exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("del stdout = %q, want OK", stdout.String())
	}

	// get after del confirms it is actually gone, not just that del
	// itself reported success
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"get", peersFlag, "user/1"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("get after del: exit code = 0, want non-zero -- the key should be gone")
	}

	// status
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"status", peersFlag}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("status: exit code = %d, stderr = %q", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("status stdout = %q, want a header line plus at least one peer line", stdout.String())
	}
	// PEER ADDRESS REACHABLE STATE TERM COMMIT APPLIED LOG_LEN SNAPSHOT FAULT
	fields := strings.Fields(lines[1])
	if len(fields) < 10 {
		t.Fatalf("status peer line = %q, want 10 fields", lines[1])
	}
	if fields[2] != "yes" {
		t.Errorf("REACHABLE = %q, want \"yes\"", fields[2])
	}
	if fields[3] != "leader" {
		t.Errorf("STATE = %q, want \"leader\" (the only node in a single-node cluster)", fields[3])
	}
	if fields[9] != "-" {
		t.Errorf("FAULT = %q, want \"-\" (healthy)", fields[9])
	}
}

func TestStatusReportsUnreachableForADeadPeer(t *testing.T) {
	// A real listener, opened and immediately closed -- the port is
	// real and was briefly bound, so a connection attempt fails the
	// way an actually-down peer's would. The identical technique
	// client_test.go's own TestGetCyclesAwayFromAnUnreachablePeer uses.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	deadAddr := lis.Addr().String()
	lis.Close()

	var stdout, stderr bytes.Buffer
	code := run([]string{"status", "--peers=1=" + deadAddr, "--timeout=500ms"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("status against a dead peer: exit code = 0, want non-zero")
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("status stdout = %q, want a header line plus one peer line", stdout.String())
	}
	// tabwriter replaces tabs with padded spaces once flushed, so the
	// peer line's own columns are parsed by field, not by searching for
	// a literal tab-joined substring.
	fields := strings.Fields(lines[1])
	if len(fields) < 4 {
		t.Fatalf("status peer line = %q, want at least 4 fields (peer, address, reachable, state)", lines[1])
	}
	if fields[0] != "1" {
		t.Errorf("peer id = %q, want \"1\"", fields[0])
	}
	if fields[1] != deadAddr {
		t.Errorf("address = %q, want %q", fields[1], deadAddr)
	}
	if fields[2] != "no" {
		t.Errorf("reachable column = %q, want \"no\"", fields[2])
	}
	if fields[3] != "-" {
		t.Errorf("state column = %q, want \"-\" (unreachable peers have no state to report)", fields[3])
	}
}