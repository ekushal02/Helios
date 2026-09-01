// Package client is the Go client library for Helios (Phase F-3): connect
// to a cluster, discover the current leader, and retry with backoff on
// NotLeader (F-2's own error, defined in internal/server/server.go and
// api/proto/helios/v1/errors.proto).
//
// THE ADDRESS BOOK LIVES HERE, EXACTLY WHERE F-2 SAID IT WOULD.
// NotLeaderDetail carries only a peer ID, deliberately not a network
// address -- F-2's own doc: "resolving a peer ID to a dialable network
// address needs a cluster address book, which exists nowhere in this
// project yet... that is the client library, not this node." Config.Peers
// is that book.
//
// API SHAPE MIRRORS kvstore.Machine, NOT THE RAW PROTO MESSAGES -- the
// same "same shape one layer up" pattern this project has used at every
// boundary since codec.go first framed a Command the same way the WAL and
// SSTable already framed their own payloads. Get/GetStale/Put/Delete
// return plain Go values (value, ok, revision, err), the identical shape
// Machine.Get/GetLeaseRead/Put/Delete already have -- a caller who has
// used Machine directly (every existing test in internal/kvstore) already
// knows this API. The generated protobuf types stay an internal
// implementation detail of this package, never part of its public
// surface.
//
// Scan and Watch have no client methods yet -- F-6 and F-7's jobs,
// matching the identical scope fence internal/server.Server already
// applies on the server side (embedding UnimplementedHeliosServer rather
// than stubbing either RPC out).
package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
)

// ErrRetriesExhausted means every configured attempt failed. Check with
// errors.Is; the wrapped underlying error (the last attempt's own
// failure) is available via errors.Unwrap or by inspecting the message.
var ErrRetriesExhausted = errors.New("client: exhausted retry attempts")

// RetryConfig controls how Client retries a request after a failure.
//
// TWO DIFFERENT RETRIES, NOT ONE, AND ONLY ONE OF THEM PAYS THIS COST.
// Following an explicit NotLeaderDetail hint retries IMMEDIATELY, no
// delay -- the server just told this client exactly where to go, and
// waiting on real information is pure wasted latency. Backoff exists for
// the other case: no usable hint, a peer that will not even accept a
// connection, or a read barrier that has not applied yet -- genuine
// guesses, where hammering the same guess as fast as possible would only
// add load to a struggling or partitioned node. See do's own doc for the
// exact branch each case takes.
type RetryConfig struct {
	// MaxAttempts bounds total tries across both kinds of retry. Default 8.
	MaxAttempts int

	// BaseDelay is the first backoff, before exponential growth. Default 20ms.
	BaseDelay time.Duration

	// MaxDelay caps how large a single backoff can grow to. Default 500ms.
	MaxDelay time.Duration
}

// defaultRetryConfig is UNMEASURED against a real workload or a real
// multi-node failover -- see DESIGN.md §12/§18's own open question on
// this. Sized by reasoning from a real, already-measured number rather
// than picked blind: §10 measured election timeouts in the 150-300ms
// range, so MaxAttempts=8 with this growth curve (7 backoff waits fit
// between 8 attempts: 20, 40, 80, 160, 320, 500-capped, 500ms, summing
// to about 1.6s of possible backoff time -- before counting any
// hint-following retries along the way, which cost nothing) gives a
// client several real election timeouts' worth of patience before
// giving up, not an arbitrary round number.
var defaultRetryConfig = RetryConfig{
	MaxAttempts: 8,
	BaseDelay:   20 * time.Millisecond,
	MaxDelay:    500 * time.Millisecond,
}

// Config configures a Client.
type Config struct {
	// Peers maps every known peer's Raft ID to a dialable gRPC address
	// ("host:port"). Required, and must have at least one entry.
	Peers map[int]string

	// InitialGuess is which peer to try first, before this Client has
	// learned anything real. Zero (the default) means "the smallest
	// peer ID in Peers" -- an arbitrary but DETERMINISTIC choice: Go
	// map iteration order is randomized, and a fixed starting point is
	// what makes two runs against the same cluster behave the same way,
	// which matters for tests and for reasoning about behavior at all.
	InitialGuess int

	// DialOptions are passed to every grpc.NewClient call this Client
	// makes. Defaults to insecure.NewCredentials() when left nil --
	// this project has no TLS story yet (a real, named open question,
	// not an oversight), and requiring every caller and every test to
	// spell that out explicitly would be friction with no matching
	// safety benefit at this project's current stage. Supplying any
	// DialOptions here replaces the default entirely -- the caller
	// taking control is responsible for including transport
	// credentials themselves.
	DialOptions []grpc.DialOption

	// Retry controls retry/backoff behavior. Zero value means
	// defaultRetryConfig.
	Retry RetryConfig

	// Seed sets this Client's random source: backoff jitter (§17's own
	// full-jitter doc) AND, as of F-4, this Client's own clientID.
	// Zero (the default) derives a seed from time.Now().UnixNano().
	// Set explicitly for a deterministic, reproducible test -- the
	// identical reason every seeded rand.Rand throughout internal/raft
	// is never the global math/rand source (see election.go's own doc
	// on this). Setting Seed does NOT make clientID predictable across
	// two Clients built with the same Seed collide with each other on
	// purpose -- it makes ONE Client's own behavior reproducible run to
	// run, which is what a test needs; two Clients sharing a Seed would
	// also share a clientID, which is a test-authoring hazard, not a
	// feature, so tests that need multiple distinct clientIDs use
	// distinct Seeds (or leave Seed unset and let each Client draw its
	// own from real entropy).
	Seed int64
}

// Client is a connection to a Helios cluster: one gRPC connection per
// peer, dialed lazily and cached, plus a shared, atomically-updated
// current best guess at who leads.
type Client struct {
	peers     map[int]string
	sortedIDs []int // fixed at construction; the deterministic cycling ring nextPeer walks
	dialOpts  []grpc.DialOption
	retry     RetryConfig

	connsMu sync.Mutex
	conns   map[int]*grpc.ClientConn

	leaderGuess atomic.Int64 // current best-guess leader peer ID; a hint, not an authority -- see LeaderHint's own doc in internal/raft for the identical idea one layer down

	rngMu sync.Mutex
	rng   *rand.Rand

	// clientID identifies this Client's own write session for
	// duplicate suppression (F-4) -- a random, non-zero uint64 drawn
	// once at construction, kept only in memory for this Client's
	// process lifetime. It does not need to survive a process restart:
	// idempotency here protects a retry within a single Client
	// instance's own retry loop (do's own doc), not a resumed session
	// after a crash, so there is nothing a persisted identity would
	// buy that a fresh random one on the next process doesn't already
	// give (a fresh clientID simply starts its own independent dedup
	// history with the server, at seq 1 again).
	clientID uint64

	// seqCounter is one shared, monotonic sequence space for every
	// write this Client issues -- Put and Delete both draw from it,
	// not two separate spaces per RPC kind, since uniqueness across
	// this clientID's own history is all the dedup table on the server
	// side actually needs (machine.go's own applyCommand doc).
	seqCounter atomic.Uint64

	// writeMu serializes Put and Delete -- the WHOLE call, including
	// every retry attempt inside do's own loop, not just the seq number
	// assignment. THIS IS WHAT MAKES "sequence numbers commit in the
	// order they were assigned" AN ACTUAL INVARIANT RATHER THAN AN
	// ASSUMPTION. Without it, two goroutines calling Put concurrently on
	// the same Client could draw seq=5 and seq=6 independently and have
	// their underlying Raft entries commit in either order -- and the
	// dedup check in machine.go's applyCommand ("at or below the
	// highest sequence number already applied") is only correct under
	// in-order delivery per clientID; out-of-order arrival would let a
	// higher, already-applied seq silently suppress a lower, genuinely
	// distinct write that simply committed later. Holding writeMu for
	// each write's full duration -- submission through every retry to
	// final success or failure -- means seq N+1 is never even
	// SUBMITTED until seq N's call has completely finished, so they can
	// never commit out of order. The cost is real: writes from one
	// Client are now fully serialized, not just ordered. A caller
	// wanting concurrent write throughput uses multiple Client
	// instances, each with its own independent clientID and therefore
	// its own independent dedup history -- the documented way to get
	// concurrency back, not a workaround for a bug.
	writeMu sync.Mutex
}

// New validates cfg and returns a Client. It does not connect to
// anything yet -- grpc.NewClient itself is lazy, and this Client goes
// further, not even calling grpc.NewClient for any peer until that
// peer is actually addressed by a request (see clientFor).
func New(cfg Config) (*Client, error) {
	if len(cfg.Peers) == 0 {
		return nil, errors.New("client: Config.Peers must have at least one entry")
	}

	sortedIDs := make([]int, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Ints(sortedIDs)

	initial := cfg.InitialGuess
	if initial == 0 {
		initial = sortedIDs[0]
	} else if _, ok := cfg.Peers[initial]; !ok {
		return nil, fmt.Errorf("client: InitialGuess %d is not a key in Config.Peers", initial)
	}

	dialOpts := cfg.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	retry := cfg.Retry
	if retry.MaxAttempts <= 0 {
		retry = defaultRetryConfig
	}

	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	peers := make(map[int]string, len(cfg.Peers))
	for id, addr := range cfg.Peers {
		peers[id] = addr
	}

	rng := rand.New(rand.NewSource(seed))

	// clientID must be non-zero -- 0 is the server's own "no session,
	// do not deduplicate" sentinel (internal/server.Server.Put's own
	// doc). rng.Uint64() landing on exactly 0 is a 1-in-2^64 event, but
	// checked rather than trusted, the same "believed impossible is
	// checked, not assumed" standard this project has held its own
	// Raft invariants to since §8.
	clientID := rng.Uint64()
	for clientID == 0 {
		clientID = rng.Uint64()
	}

	c := &Client{
		peers:     peers,
		sortedIDs: sortedIDs,
		dialOpts:  dialOpts,
		retry:     retry,
		conns:     make(map[int]*grpc.ClientConn),
		rng:       rng,
		clientID:  clientID,
	}
	c.leaderGuess.Store(int64(initial))
	return c, nil
}

// Close closes every gRPC connection this Client has opened.
func (c *Client) Close() error {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()
	var firstErr error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// -----------------------------------------------------------------------
// Public API -- mirrors kvstore.Machine's own shape (see package doc)
// -----------------------------------------------------------------------

// Get is a linearizable read (CONSISTENCY_LINEARIZABLE) -- the
// network-facing mirror of Machine.Get.
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, ok bool, revision int64, err error) {
	req := &heliosv1.GetRequest{Key: key, Consistency: heliosv1.Consistency_CONSISTENCY_LINEARIZABLE}
	resp, err := do[heliosv1.GetRequest, heliosv1.GetResponse](ctx, c, req, heliosv1.HeliosClient.Get)
	if err != nil {
		return nil, false, 0, err
	}
	return resp.GetValue(), resp.GetFound(), resp.GetRevision(), nil
}

// GetStale is CONSISTENCY_STALE -- the network-facing mirror of
// Machine.GetLeaseRead. Named GetStale rather than a consistency
// parameter on Get, matching Machine's own split into two distinctly
// named methods rather than one method with a flag -- see DESIGN.md §16's
// own note that CONSISTENCY_STALE is, today, still a leader-only,
// still-linearizable read (GetLeaseRead), not the follower-servable
// stale read the name suggests. This client does not paper over that
// gap either: it is exactly as stale (i.e., not very) as the server it
// is talking to actually is.
func (c *Client) GetStale(ctx context.Context, key []byte) (value []byte, ok bool, revision int64, err error) {
	req := &heliosv1.GetRequest{Key: key, Consistency: heliosv1.Consistency_CONSISTENCY_STALE}
	resp, err := do[heliosv1.GetRequest, heliosv1.GetResponse](ctx, c, req, heliosv1.HeliosClient.Get)
	if err != nil {
		return nil, false, 0, err
	}
	return resp.GetValue(), resp.GetFound(), resp.GetRevision(), nil
}

// Put is the network-facing mirror of Machine.Put, with duplicate
// suppression (F-4) built in automatically -- every retry do's own loop
// issues for this call carries the identical ClientId and
// SequenceNumber, since req is built once, here, before do ever runs,
// and the same *PutRequest is reused across every attempt.
//
// writeMu is held for this call's ENTIRE duration, not just while
// assigning seq -- see the Client struct's own doc on writeMu for why
// that is what makes "this Client's sequence numbers commit in the
// order they were assigned" a real guarantee rather than a hope.
func (c *Client) Put(ctx context.Context, key, value []byte) (revision int64, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	seq := c.seqCounter.Add(1)
	req := &heliosv1.PutRequest{Key: key, Value: value, ClientId: c.clientID, SequenceNumber: seq}
	resp, err := do[heliosv1.PutRequest, heliosv1.PutResponse](ctx, c, req, heliosv1.HeliosClient.Put)
	if err != nil {
		return 0, err
	}
	return resp.GetRevision(), nil
}

// Delete is the network-facing mirror of Machine.Delete, with the
// identical duplicate-suppression contract Put's own doc describes.
// Its own Found field is unimplemented server-side (always false -- see
// internal/server/server.go's own doc on Server.Delete); this client
// does not invent a different answer, it returns exactly what the wire
// carries.
func (c *Client) Delete(ctx context.Context, key []byte) (revision int64, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	seq := c.seqCounter.Add(1)
	req := &heliosv1.DeleteRequest{Key: key, ClientId: c.clientID, SequenceNumber: seq}
	resp, err := do[heliosv1.DeleteRequest, heliosv1.DeleteResponse](ctx, c, req, heliosv1.HeliosClient.Delete)
	if err != nil {
		return 0, err
	}
	return resp.GetRevision(), nil
}

// -----------------------------------------------------------------------
// The retry loop
// -----------------------------------------------------------------------

// do sends req to the current best-guess leader via method (a gRPC
// client method expression, e.g. heliosv1.HeliosClient.Get), retrying
// on failure per RetryConfig until success, a non-retryable error, or
// ctx's own cancellation.
//
// Branches, in the order checked, and why each one does what it does:
//
//  1. ctx already done -- return its error immediately. No point
//     starting an attempt the caller has already given up on.
//  2. Success -- record this peer as the leader guess (it just proved
//     it, by answering) and return.
//  3. codes.Unavailable WITH a NotLeaderDetail naming a peer this
//     Client has an address for -- switch the guess to that peer and
//     retry IMMEDIATELY, no backoff. Real information, not a guess.
//  4. codes.Unavailable with no usable hint (absent, or naming a peer
//     ID this Client has no address for -- including the -1 sentinel a
//     server reports before its own first election has completed) --
//     cycle to the next peer in the deterministic ring and back off.
//     A guess, so it waits like one.
//  5. codes.DeadlineExceeded -- the read barrier this call was waiting
//     on did not land in time. Retry the SAME peer (it may well still
//     be leader, just briefly behind) after backing off.
//  6. Not a gRPC status at all -- most plausibly a transport-level
//     failure (a peer refusing the connection outright). Cycle and
//     back off, the same as an unhinted Unavailable.
//  7. Anything else (codes.Internal, codes.InvalidArgument, ...) --
//     not retryable. Return it as-is; retrying a real bug does not fix
//     the bug.
func do[Req any, Resp any](
	ctx context.Context,
	c *Client,
	req *Req,
	method func(heliosv1.HeliosClient, context.Context, *Req, ...grpc.CallOption) (*Resp, error),
) (*Resp, error) {
	peer := int(c.leaderGuess.Load())
	var lastErr error

	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		cli, connErr := c.clientFor(peer)
		if connErr != nil {
			lastErr = connErr
			peer = c.nextPeer(peer)
			c.backoff(ctx, attempt)
			continue
		}

		resp, err := method(cli, ctx, req)
		if err == nil {
			c.leaderGuess.Store(int64(peer))
			return resp, nil
		}
		lastErr = err

		st, isStatus := status.FromError(err)
		switch {
		case isStatus && st.Code() == codes.Unavailable:
			if hintID, ok := notLeaderHint(st); ok {
				if _, known := c.peers[hintID]; known {
					peer = hintID
					c.leaderGuess.Store(int64(peer))
					continue // real information -- no backoff
				}
			}
			peer = c.nextPeer(peer)
			c.backoff(ctx, attempt)
		case isStatus && st.Code() == codes.DeadlineExceeded:
			c.backoff(ctx, attempt) // same peer -- it may just be catching up
		case !isStatus:
			peer = c.nextPeer(peer)
			c.backoff(ctx, attempt)
		default:
			return nil, err
		}
	}

	return nil, fmt.Errorf("%w after %d attempts (last tried peer %d): %w",
		ErrRetriesExhausted, c.retry.MaxAttempts, peer, lastErr)
}

// notLeaderHint extracts a peer ID from a NotLeaderDetail among st's
// details, if one is present. Does not itself check whether that ID is
// one this Client has an address for -- do's own caller does that,
// since "known" depends on this Client's Peers, not on anything the
// status carries.
func notLeaderHint(st *status.Status) (peerID int, ok bool) {
	for _, d := range st.Details() {
		if nl, isNotLeader := d.(*heliosv1.NotLeaderDetail); isNotLeader {
			return int(nl.GetLeaderId()), true
		}
	}
	return 0, false
}

// clientFor returns a heliosv1.HeliosClient for peerID, dialing and
// caching a *grpc.ClientConn on first use. grpc.NewClient itself is
// lazy -- it does not attempt a real network connection until the
// first RPC -- so a connErr here is almost always a malformed target
// string, not a reachability problem; a peer that is simply down or
// unreachable is instead discovered as an RPC-time codes.Unavailable,
// handled by do's own case for that.
func (c *Client) clientFor(peerID int) (heliosv1.HeliosClient, error) {
	addr, ok := c.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("client: no known address for peer %d", peerID)
	}

	c.connsMu.Lock()
	defer c.connsMu.Unlock()
	if conn, ok := c.conns[peerID]; ok {
		return heliosv1.NewHeliosClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, c.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("client: dial peer %d (%s): %w", peerID, addr, err)
	}
	c.conns[peerID] = conn
	return heliosv1.NewHeliosClient(conn), nil
}

// nextPeer returns the peer after current in a fixed, sorted ring --
// deterministic so two runs against the same cluster cycle through
// peers in the same order, the same reasoning InitialGuess's own doc
// gives for picking the smallest ID by default. If current is not a
// known peer at all (should not happen -- every caller passes either
// the initial guess or a value already checked against c.peers), starts
// the ring from its own beginning.
func (c *Client) nextPeer(current int) int {
	for i, id := range c.sortedIDs {
		if id == current {
			return c.sortedIDs[(i+1)%len(c.sortedIDs)]
		}
	}
	return c.sortedIDs[0]
}

// backoff waits an exponentially-growing, jittered delay before
// returning, or returns early if ctx is done first. Full jitter (a
// uniform random draw between 0 and the capped exponential delay,
// rather than the delay itself): AWS's own well-known backoff
// architecture write-up is the standard citation for why -- it spreads
// retries out over the whole window instead of every client waiting
// the identical amount and re-colliding on the same instant, which
// plain exponential backoff with no jitter at all is prone to under
// concurrent load.
func (c *Client) backoff(ctx context.Context, attempt int) {
	delay := c.retry.BaseDelay
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= c.retry.MaxDelay {
			delay = c.retry.MaxDelay
			break
		}
	}
	if delay <= 0 {
		return
	}

	c.rngMu.Lock()
	jittered := time.Duration(c.rng.Int63n(int64(delay) + 1))
	c.rngMu.Unlock()

	t := time.NewTimer(jittered)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
