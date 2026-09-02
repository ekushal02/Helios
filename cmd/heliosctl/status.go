package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
	"github.com/ekushal02/helios/client"
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
	fmt.Fprintln(tw, "PEER\tADDRESS\tREACHABLE\tLEADER\tNOTE")

	allReachable := true
	for _, id := range ids {
		addr := peers[id]
		reachable, leader, note := probePeer(cf.timeout, id, addr)
		if !reachable {
			allReachable = false
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", id, addr, yesNo(reachable), leaderColumn(reachable, leader), note)
	}
	tw.Flush()

	if !allReachable {
		return 1
	}
	return 0
}

// probePeer talks to exactly ONE peer, bypassing client.Client's own
// multi-peer redirect logic entirely. That resilience is exactly what
// get/put/del/scan want and what status does not: status exists to
// show what THIS specific node itself answers, not wherever a
// resilient client would end up after following a hint elsewhere. A
// single-peer Config with MaxAttempts=1 gives exactly one real RPC
// against exactly the peer being asked about, no retry, no redirect --
// this status IS the whole answer for that peer, not a partial one
// still waiting on more attempts.
func probePeer(timeout time.Duration, id int, addr string) (reachable, leader bool, note string) {
	c, err := client.New(client.Config{
		Peers: map[int]string{id: addr},
		Retry: client.RetryConfig{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
	if err != nil {
		return false, false, err.Error()
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// The key itself is arbitrary and never expected to exist --
	// this call is a reachability/leadership probe, not a real read.
	// A successful response (found or not) proves this peer answered
	// as leader; the interesting case below is what its FAILURE says.
	_, _, _, err = c.Get(ctx, []byte("__heliosctl_status_probe__"))
	if err == nil {
		return true, true, "-"
	}

	st, isStatus := status.FromError(err)
	if !isStatus {
		return false, false, "unreachable: " + err.Error()
	}
	if st.Code() != codes.Unavailable {
		return false, false, st.Message()
	}

	// codes.Unavailable covers BOTH a structured "I'm just not the
	// leader" answer AND a genuine connection failure (Server.Get's
	// own doc: a dial/network failure surfaces as this identical
	// code) -- indistinguishable by code alone. Only a NotLeaderDetail
	// actually proves this peer was reachable and responded.
	for _, d := range st.Details() {
		nl, isNotLeader := d.(*heliosv1.NotLeaderDetail)
		if !isNotLeader {
			continue
		}
		if nl.GetLeaderId() < 0 {
			return true, false, "reachable, not the leader (no leader elected yet)"
		}
		return true, false, fmt.Sprintf("reachable, not the leader (believes leader is peer %d)", nl.GetLeaderId())
	}
	return false, false, "unreachable: " + st.Message()
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func leaderColumn(reachable, leader bool) string {
	if !reachable {
		return "-"
	}
	return yesNo(leader)
}
