package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

func runPut(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { printCommandHelp(stderr, "put") }
	cf := addCommonFlags(fs)

	if err := fs.Parse(args); err != nil {
		return 1
	}
	rest := fs.Args()
	if len(rest) != 2 {
		fmt.Fprintf(stderr, "heliosctl put: want exactly two arguments (key and value), got %d\n\n", len(rest))
		printCommandHelp(stderr, "put")
		return 1
	}
	key, value := rest[0], rest[1]

	c, err := newClient(cf.peers)
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl put: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cf.timeout)
	defer cancel()

	revision, err := c.Put(ctx, []byte(key), []byte(value))
	if err != nil {
		fmt.Fprintf(stderr, "heliosctl put: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "OK")
	fmt.Fprintf(stderr, "revision: %d\n", revision)
	return 0
}
