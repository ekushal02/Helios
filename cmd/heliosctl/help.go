package main

import (
	"fmt"
	"io"
)

const topLevelHelp = `heliosctl is the command-line client for a Helios cluster.

Usage:
  heliosctl <command> [flags] [arguments]

Commands:
  get       Get the value for a key
  put       Set a key to a value
  del       Delete a key
  scan      List key-value pairs in a range
  status    Check whether configured peers are reachable and who leads
  help      Show this help, or "heliosctl help <command>" for one command

Every command accepts:
  --peers=1=host:port,2=host:port   cluster peers (default: 1=localhost:50051)
  --timeout=5s                      how long to wait before giving up

Examples:
  heliosctl put user/1 alice
  heliosctl get user/1
  heliosctl scan --start=user/ --end=user0 --all
  heliosctl del user/1
  heliosctl status

Run "heliosctl help <command>" for details and more examples on a
specific command, e.g. "heliosctl help scan".
`

const getHelp = `heliosctl get <key>

Get the value for a key. Prints the value to stdout, and the revision
it was last written at to stderr (so "heliosctl get foo > out.txt"
captures just the value, the way a Unix tool is expected to). Exits
with a non-zero status and prints "(key not found)" if the key does
not exist.

Flags:
  --stale              read from this node's local lease instead of a
                       full linearizable barrier (faster; the answer
                       may be a few writes behind on a busy cluster --
                       today this is still leader-only under the hood,
                       see DESIGN.md §16.5)
  --peers, --timeout   see "heliosctl help"

Examples:
  heliosctl get user/1
  heliosctl get --stale user/1
  heliosctl get user/1 > alice.txt
`

const putHelp = `heliosctl put <key> <value>

Set key to value. Prints OK on success, and the revision it committed
at to stderr.

Flags:
  --peers, --timeout   see "heliosctl help"

Examples:
  heliosctl put user/1 alice
  heliosctl put "key with spaces" "value with spaces"
`

const delHelp = `heliosctl del <key>

Delete a key. Prints OK once the delete itself has committed.

Note: the server does not currently report whether the key existed
before the delete (a known, documented gap -- see DESIGN.md §16.5), so
"OK" here confirms the delete request succeeded, not that there was
anything there to delete.

Flags:
  --peers, --timeout   see "heliosctl help"

Examples:
  heliosctl del user/1
`

const scanHelp = `heliosctl scan [flags]

List key-value pairs in [--start, --end), sorted. Takes no positional
arguments -- every bound is a flag.

Flags:
  --start=KEY         inclusive lower bound (default: from the beginning)
  --end=KEY           exclusive upper bound (default: no upper bound)
  --limit=N           max pairs per page (default: server default, currently 100)
  --all               fetch every page automatically and print everything
                      (may be slow on a very large range; always uses a
                      linearizable read -- --stale is ignored when
                      combined with --all)
  --stale             read from this node's local lease instead of a full
                      linearizable barrier (ignored when --all is set)
  --peers, --timeout  see "heliosctl help"

To scan every key with a given prefix, set --end to the same prefix
with its last byte incremented -- e.g. --start=user/ --end=user0
matches every key starting with "user/", since "0" sorts just after
"/".

Examples:
  heliosctl scan --all
  heliosctl scan --start=user/ --end=user0 --all
  heliosctl scan --limit=10
`

const statusHelp = `heliosctl status [flags]

For each configured peer, report whether it's reachable and whether it
believes itself to be the leader.

Talks to each peer INDIVIDUALLY -- unlike get/put/del/scan, which use
the cluster resiliently via client.Client and don't care which specific
node ends up answering, status exists specifically to show per-node
detail, so it deliberately does not retry or redirect across peers the
way every other command does.

Flags:
  --peers, --timeout   see "heliosctl help"

Examples:
  heliosctl status
  heliosctl status --peers=1=localhost:50051,2=localhost:50052,3=localhost:50053
`

func printTopLevelHelp(w io.Writer) {
	fmt.Fprint(w, topLevelHelp)
}

func printCommandHelp(w io.Writer, cmd string) {
	switch cmd {
	case "get":
		fmt.Fprint(w, getHelp)
	case "put":
		fmt.Fprint(w, putHelp)
	case "del":
		fmt.Fprint(w, delHelp)
	case "scan":
		fmt.Fprint(w, scanHelp)
	case "status":
		fmt.Fprint(w, statusHelp)
	default:
		fmt.Fprintf(w, "heliosctl: no help for %q -- known commands: get, put, del, scan, status\n\n", cmd)
		printTopLevelHelp(w)
	}
}
