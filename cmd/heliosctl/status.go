package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
)

func runStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { printCommandHelp(stderr, "status") }
	cf := addCommonFlags(fs)

	if err := fs.Parse(args); err != nil {
		return 1
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "heliosctl status: unexpected argument(s) %v -- status takes no positional arguments, only flags\n\n", fs.Args())
		printCommandHelp(stderr, "status")
		return 1
	}

	peers, err := parsePeers(cf.peers)
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl status: %v\n", err)
		return 1
	}

	ids := make([]int, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PEER\tADDRESS\tREACHABLE\tSTATE\tTERM\tCOMMIT\tAPPLIED\tLOG_LEN\tSNAPSHOT\tFAULT")

	allReachable := true
	for _, id := range ids {
		addr := peers[id]
		row, err := probePeer(cf.timeout, addr)
		if err != nil {
			allReachable = false
			fmt.Fprintf(tw, "%d\t%s\tno\t-\t-\t-\t-\t-\t-\t%s\n", id, addr, err)
			continue
		}
		fmt.Fprintf(tw, "%d\t%s\tyes\t%s\t%d\t%d\t%d\t%d\t%s\t%s\n",
			id, addr, row.state, row.term, row.commit, row.applied, row.logLen, row.snapshot, row.fault)
	}
	tw.Flush()

	if !allReachable {
		return 1
	}
	return 0
}

type statusRow struct {
	state    string
	term     int64
	commit   int64
	applied  int64
	logLen   int64
	snapshot string
	fault    string
}

// probePeer talks to exactly ONE peer, over its own raw connection --
// bypassing client.Client entirely, not just its multi-peer redirect
// logic (which the old, pre-F-9 version of this function used at
// MaxAttempts=1 for the identical reason). client.Client only speaks
// heliosv1.HeliosClient (the data-plane service); Status lives on the
// separate heliosv1.HeliosAdminClient (admin.proto, F-9), which
// client.Client has no reason to wrap, since admin introspection is
// not the resilient, cluster-hiding kind of call that package exists
// for.
//
// Deliberately NOT leader-gated on the server side (Server.Status's own
// doc), so unlike the old Get-based probe this replaces, a follower's
// own answer is never confused with "unreachable" -- reachability and
// leadership are two independent, directly-reported facts now, not one
// inferred from the other via a gRPC status code.
func probePeer(timeout time.Duration, addr string) (statusRow, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return statusRow{}, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := heliosv1.NewHeliosAdminClient(conn).Status(ctx, &heliosv1.StatusRequest{})
	if err != nil {
		return statusRow{}, fmt.Errorf("unreachable: %w", err)
	}

	snapshot := "-"
	if resp.GetSnapshotIndex() > 0 {
		snapshot = fmt.Sprintf("%d/%d", resp.GetSnapshotIndex(), resp.GetSnapshotTerm())
	}
	fault := resp.GetFault()
	if fault == "" {
		fault = "-"
	}

	return statusRow{
		state:    resp.GetRaftState(),
		term:     resp.GetTerm(),
		commit:   resp.GetCommitIndex(),
		applied:  resp.GetLastApplied(),
		logLen:   resp.GetLogLength(),
		snapshot: snapshot,
		fault:    fault,
	}, nil
}