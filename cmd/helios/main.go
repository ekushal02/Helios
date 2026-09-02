// Command helios starts a single Helios node: a real raft.Node, backed
// by real on-disk persistence, with the full storage engine (§13)
// wired in as its state machine via internal/kvstore, and now (§16) a
// real gRPC server in front of it -- the first time every piece built
// across this project runs together as one program, not just as tests
// exercising it.
//
// SINGLE-NODE RAFT, DELIBERATELY, NOT AS A LIMITATION OF THIS COMMAND
// BUT OF THE PROJECT'S CURRENT SCOPE. Raft's own Transport interface
// (internal/raft/rpc.go) has never had a real network implementation --
// every multi-node test in package raft runs against an in-memory fake
// transport built for testing, and multi-node deployment and chaos
// testing are explicitly later phases this project has not reached
// yet. A no-op Transport is therefore not a shortcut taken here; it is
// the only transport that honestly exists to use, for a cluster of one
// node that never has a Raft peer to reach.
//
// THE GRPC SERVER IS REAL, EVEN THOUGH RAFT ITSELF IS STILL SINGLE-NODE.
// internal/server's NotLeader handling and leader-hint plumbing do not
// depend on multi-node Raft existing -- they depend only on Node's own
// leadership state, which is real and correct on one node today (see
// internal/raft/leaderhint_test.go's own pre-election-window test for
// the case that actually exercises it on a single node).
//
// This binary only ever starts a node; it has no client-facing
// command-line tool of its own. cmd/heliosctl is that tool -- a
// separate binary, matching etcd/etcdctl's own established server/CLI
// split (see cmd/heliosctl/main.go's own doc for the full reasoning).
package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
	"github.com/ekushal02/helios/internal/kvstore"
	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/server"
	"github.com/ekushal02/helios/internal/storage/sstable"
	"google.golang.org/grpc"
)

// noopTransport never sends anything. See the package doc for why this
// is the honest choice, not a placeholder standing in for a real one
// that was simply left unbuilt for this command specifically.
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

func main() {
	dataDir := "./helios-data"
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}
	grpcAddr := ":50051"
	if len(os.Args) > 2 {
		grpcAddr = os.Args[2]
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	fmt.Printf("HELIOS starting -- data directory: %s, gRPC address: %s\n", dataDir, grpcAddr)

	raftDir := filepath.Join(dataDir, "raft")
	kvDir := filepath.Join(dataDir, "kv")

	storage, err := raft.NewFileStorage(raftDir)
	if err != nil {
		logger.Error("open raft storage", "err", err)
		os.Exit(1)
	}

	n, err := raft.OpenNode(1, nil, noopTransport{}, time.Now().UnixNano(), storage)
	if err != nil {
		logger.Error("open raft node", "err", err)
		os.Exit(1)
	}
	n.SetLogger(logger)
	n.Start()

	// A single, node-wide block cache (§13.12), shared by every SSTable
	// this Machine opens across its whole lifetime -- the same
	// intended usage §13.12's own doc describes, one cache per
	// deployment, not one per file.
	cache := sstable.NewBlockCache(64 << 20) // 64MB, unmeasured against a real workload -- see DESIGN.md §12

	m, err := kvstore.NewMachine(n, kvDir, cache, kvstore.DefaultOptions)
	if err != nil {
		logger.Error("open state machine", "err", err)
		n.Stop()
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("listen for gRPC", "addr", grpcAddr, "err", err)
		m.Close()
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	heliosSrv := server.New(n, m)
	heliosv1.RegisterHeliosServer(grpcServer, heliosSrv)
	// The identical Server value, registered a second time against the
	// admin service (F-9) -- Server implements both interfaces (see
	// its own package doc), and both need the same n/m pair, so there
	// is no reason for cmd/helios to construct two separate objects
	// just to hand each service its own.
	heliosv1.RegisterHeliosAdminServer(grpcServer, heliosSrv)

	// Serve on its own goroutine -- Serve blocks until Stop/GracefulStop,
	// which only happens from the shutdown-signal handling below, the
	// same "one goroutine runs the thing, main blocks on a stop signal"
	// shape n.Start()'s own background loops already use.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(lis)
	}()

	fmt.Printf("HELIOS ready (single-node Raft, real gRPC on %s). Ctrl+C to stop.\n", grpcAddr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case <-stop:
		fmt.Println("\nHELIOS stopping...")
	case err := <-serveErr:
		// The gRPC server exited on its own (a real bind/accept failure
		// after startup, not the ordinary shutdown path) -- worth
		// logging distinctly from an operator-requested stop, though
		// the shutdown sequence below is identical either way.
		logger.Error("gRPC server stopped unexpectedly", "err", err)
	}

	// GracefulStop before m.Close(): let in-flight RPCs finish against a
	// still-open Machine rather than racing a request against Close()
	// tearing down the storage engine underneath it.
	grpcServer.GracefulStop()

	if err := m.Close(); err != nil {
		logger.Error("close state machine", "err", err)
	}
	fmt.Println("HELIOS stopped.")
}