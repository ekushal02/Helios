package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ekushal02/helios/client"
)

// defaultPeers matches cmd/helios's own default gRPC address for a
// single-node deployment (":50051") -- so someone who just started
// `helios` with no arguments and then runs `heliosctl get foo` with no
// flags either gets exactly what they'd expect, no --peers needed. The
// first thing "a stranger could follow" actually needs is not having
// to learn --peers before their first command works at all.
const defaultPeers = "1=localhost:50051"

// commonFlags are the flags every subcommand accepts, defined once
// here rather than five times.
type commonFlags struct {
	peers   string
	timeout time.Duration
}

func addCommonFlags(fs *flag.FlagSet) *commonFlags {
	cf := &commonFlags{}
	fs.StringVar(&cf.peers, "peers", defaultPeers,
		"cluster peers as id=address pairs, comma-separated (e.g. 1=host1:50051,2=host2:50051)")
	fs.DurationVar(&cf.timeout, "timeout", 5*time.Second, "how long to wait before giving up")
	return cf
}

// parsePeers turns "1=host:port,2=host:port" into the map
// client.Config.Peers itself wants -- the identical id=address shape
// that field documents, spelled as a flag string because a CLI has no
// other way to hand over a map.
func parsePeers(s string) (map[int]string, error) {
	peers := make(map[int]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, found := strings.Cut(part, "=")
		if !found {
			return nil, fmt.Errorf("invalid --peers entry %q: want id=address (e.g. 1=localhost:50051)", part)
		}
		id, err := strconv.Atoi(strings.TrimSpace(idStr))
		if err != nil {
			return nil, fmt.Errorf("invalid peer id %q in %q: want a number", strings.TrimSpace(idStr), part)
		}
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return nil, fmt.Errorf("invalid --peers entry %q: address is empty", part)
		}
		peers[id] = addr
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("--peers is empty -- want at least one id=address pair (e.g. 1=localhost:50051)")
	}
	return peers, nil
}

// newClient builds a resilient, multi-peer client.Client from a
// --peers flag value -- the ordinary way every subcommand except
// status talks to the cluster (status deliberately bypasses this; see
// status.go's own doc on why).
func newClient(peersFlag string) (*client.Client, error) {
	peers, err := parsePeers(peersFlag)
	if err != nil {
		return nil, err
	}
	c, err := client.New(client.Config{Peers: peers})
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return c, nil
}
