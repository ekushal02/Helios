package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/ekushal02/helios/client"
)

func runScan(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { printCommandHelp(stderr, "scan") }
	cf := addCommonFlags(fs)
	start := fs.String("start", "", "inclusive lower bound (empty = from the beginning)")
	end := fs.String("end", "", "exclusive upper bound (empty = no upper bound)")
	limit := fs.Int("limit", 0, "max pairs per page (0 = server default)")
	all := fs.Bool("all", false, "fetch every page automatically and print everything")
	stale := fs.Bool("stale", false, "read from this node's local lease instead of a full linearizable barrier (ignored with --all)")

	if err := fs.Parse(args); err != nil {
		return 1
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "heliosctl scan: unexpected argument(s) %v -- scan takes no positional arguments, only flags\n\n", fs.Args())
		printCommandHelp(stderr, "scan")
		return 1
	}

	c, err := newClient(cf.peers)
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl scan: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cf.timeout)
	defer cancel()

	var startKey, endKey []byte
	if *start != "" {
		startKey = []byte(*start)
	}
	if *end != "" {
		endKey = []byte(*end)
	}

	if *all {
		// ScanAll (client.go) always uses a linearizable Scan
		// internally -- it has no consistency parameter of its own --
		// so --stale has nothing to affect here. scanHelp says this
		// plainly rather than silently ignoring the flag with no
		// explanation.
		pairs, err := c.ScanAll(ctx, startKey, endKey, *limit)
		if err != nil {
			fmt.Fprintf(stderr, "heliosctl scan: %v\n", err)
			return 1
		}
		printPairs(stdout, pairs)
		fmt.Fprintf(stderr, "%d pair(s)\n", len(pairs))
		return 0
	}

	var pairs []client.KeyValue
	var next []byte
	if *stale {
		pairs, next, err = c.ScanStale(ctx, startKey, endKey, *limit, nil)
	} else {
		pairs, next, err = c.Scan(ctx, startKey, endKey, *limit, nil)
	}
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl scan: %v\n", err)
		return 1
	}
	printPairs(stdout, pairs)
	fmt.Fprintf(stderr, "%d pair(s)", len(pairs))
	if len(next) > 0 {
		fmt.Fprintf(stderr, "; more available -- pass --start=%q to continue, or use --all to fetch everything", next)
	}
	fmt.Fprintln(stderr)
	return 0
}

func printPairs(w io.Writer, pairs []client.KeyValue) {
	for _, kv := range pairs {
		fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
	}
}
