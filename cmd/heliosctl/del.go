package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

func runDel(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("del", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { printCommandHelp(stderr, "del") }
	cf := addCommonFlags(fs)

	if err := fs.Parse(args); err != nil {
		return 1
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintf(stderr, "heliosctl del: want exactly one argument (the key), got %d\n\n", len(rest))
		printCommandHelp(stderr, "del")
		return 1
	}
	key := rest[0]

	c, err := newClient(cf.peers)
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl del: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cf.timeout)
	defer cancel()

	// DeleteResponse.Found is always false server-side today (a real,
	// documented gap -- DESIGN.md §16.5, Server.Delete's own doc) --
	// this command does not pretend to know whether the key existed,
	// only that the delete itself committed. See delHelp's own text.
	revision, err := c.Delete(ctx, []byte(key))
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl del: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "OK")
	fmt.Fprintf(stderr, "revision: %d\n", revision)
	return 0
}
