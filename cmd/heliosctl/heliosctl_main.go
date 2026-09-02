// Command heliosctl is the command-line client for a Helios cluster:
// get/put/del/scan a running cluster, and check peer status, from a
// terminal.
//
// A SEPARATE BINARY FROM cmd/helios, DELIBERATELY -- MATCHING A
// WELL-KNOWN REAL PRECEDENT (etcd/etcdctl, Kubernetes' own
// apiserver/kubectl): a server daemon and its own command-line client
// are conventionally two different programs, not two modes of one
// program selected by a subcommand. cmd/helios already exists, already
// named "helios," and already takes positional arguments (a data
// directory, then a gRPC address); changing its own invocation shape to
// something like "helios serve <dir> <addr>" just to make room for
// "helios get ..." would be a breaking change to an already-established
// command, for a benefit (one shared binary name) real prior art does
// not treat as worth that cost.
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's own testable core -- every test in this package calls
// this directly, never main() itself, so a test never needs to fork a
// real process or intercept os.Exit to check behavior.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printTopLevelHelp(stderr)
		return 1 // no command at all is a usage error -- distinct from a deliberate "heliosctl help" below, which exits 0
	}

	cmd, rest := args[0], args[1:]

	switch cmd {
	case "help", "-h", "--help":
		if len(rest) > 0 {
			printCommandHelp(stdout, rest[0])
			return 0
		}
		printTopLevelHelp(stdout)
		return 0
	case "get":
		return runGet(rest, stdout, stderr)
	case "put":
		return runPut(rest, stdout, stderr)
	case "del":
		return runDel(rest, stdout, stderr)
	case "scan":
		return runScan(rest, stdout, stderr)
	case "status":
		return runStatus(rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "heliosctl: unknown command %q\n\n", cmd)
		printTopLevelHelp(stderr)
		return 1
	}
}
