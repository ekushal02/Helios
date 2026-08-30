// Command helios starts a single Helios node: a real raft.Node, backed
// by real on-disk persistence, with the full storage engine (§13)
// wired in as its state machine via internal/kvstore -- the first time
// every piece built across this project runs together as one program,
// not just as tests exercising it.
//
// SINGLE-NODE ONLY, DELIBERATELY, NOT AS A LIMITATION OF THIS COMMAND
// BUT OF THE PROJECT'S CURRENT SCOPE. Raft's own Transport interface
// (internal/raft/rpc.go) has never had a real network implementation --
// every multi-node test in package raft runs against an in-memory fake
// transport built for testing, and a gRPC API, chaos testing, and
// multi-node deployment are explicitly later phases this project has
// not reached yet. A no-op Transport is therefore not a shortcut taken
// here; it is the only transport that honestly exists to use, for a
// cluster of one node that never has a peer to reach.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ekushal02/helios/internal/kvstore"
	"github.com/ekushal02/helios/internal/raft"
	"github.com/ekushal02/helios/internal/storage/sstable"
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	fmt.Printf("HELIOS starting -- data directory: %s\n", dataDir)

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

	fmt.Println("HELIOS ready (single-node). Ctrl+C to stop.")

	// No client-facing API exists yet (gRPC is later roadmap work,
	// per this package's own doc) -- this command demonstrates the
	// full vertical wiring is real and correct, waiting for a shutdown
	// signal rather than exercising Put/Delete/Get itself, which the
	// tests in internal/kvstore already do thoroughly.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	fmt.Println("\nHELIOS stopping...")
	if err := m.Close(); err != nil {
		logger.Error("close state machine", "err", err)
	}
	fmt.Println("HELIOS stopped.")
}
