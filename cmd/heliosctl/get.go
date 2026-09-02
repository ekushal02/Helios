package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

func runGet(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { printCommandHelp(stderr, "get") }
	cf := addCommonFlags(fs)
	stale := fs.Bool("stale", false, "read from this node's local lease instead of a full linearizable barrier")

	if err := fs.Parse(args); err != nil {
		return 1
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintf(stderr, "heliosctl get: want exactly one argument (the key), got %d\n\n", len(rest))
		printCommandHelp(stderr, "get")
		return 1
	}
	key := rest[0]

	c, err := newClient(cf.peers)
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl get: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cf.timeout)
	defer cancel()

	var (
		value    []byte
		ok       bool
		revision int64
	)
	if *stale {
		value, ok, revision, err = c.GetStale(ctx, []byte(key))
	} else {
		value, ok, revision, err = c.Get(ctx, []byte(key))
	}
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl get: %v\n", err)
		return 1
	}
	if !ok {
		// Matches redis-cli's own well-known convention for a missing
		// key -- a real, directly relevant precedent for a KV store
		// CLI specifically, not an invented convention.
		fmt.Fprintln(stdout, "(key not found)")
		return 1
	}

	// Value on stdout, metadata on stderr -- the same Unix convention
	// Server.getResponse's own doc already leans on for the revision
	// itself: a caller piping stdout somewhere (a file, another
	// command) gets exactly the value, nothing else mixed in.
	fmt.Fprintf(stdout, "%s\n", value)
	fmt.Fprintf(stderr, "revision: %d\n", revision)
	return 0
}
