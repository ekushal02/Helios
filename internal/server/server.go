// Package server wires heliosv1.HeliosServer (api/proto/helios/v1/helios.proto,
// Phase F-1) AND heliosv1.HeliosAdminServer (admin.proto, F-9) to a
// single node's *kvstore.Machine and the *raft.Node it's attached to.
// One Server per node, implementing both services -- cmd/helios
// registers the same value against both, since an admin endpoint and
// the data-plane API it inspects both need the identical n/m pair, and
// nothing about splitting them into two Go types would change what
// either actually does.
//
// This is a translation layer and nothing more: it does not reimplement
// any read or write logic. Every data-plane RPC calls straight through
// to an existing Machine method (Get, GetLeaseRead, Put, Delete, Scan,
// ScanLeaseRead, Watch -- all built and tested in Phase H, F-6, and F-7)
// and translates the Go return values into wire types and gRPC status
// codes. Status (F-9) is the one exception to "translates a Machine
// call" -- it reads raft.Node.Status() directly, since cluster/Raft
// introspection is exactly what that method exists to report, not
// something Machine has any reason to know about.
package server

import (
	"bytes"
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
	"github.com/ekushal02/helios/internal/kvstore"
	"github.com/ekushal02/helios/internal/raft"
)

// Server implements both heliosv1.HeliosServer and
// heliosv1.HeliosAdminServer.
type Server struct {
	// Embedded only to satisfy each service interface's own forward-
	// compatibility requirement (every implementation must embed its
	// own Unimplemented* type, so a FUTURE new RPC added to either
	// service doesn't break existing implementations at compile time).
	// Every RPC both services define has a real implementation as of
	// F-9; nothing currently falls through to either embed.
	heliosv1.UnimplementedHeliosServer
	heliosv1.UnimplementedHeliosAdminServer

	n *raft.Node
	m *kvstore.Machine
}

var _ heliosv1.HeliosServer = (*Server)(nil)
var _ heliosv1.HeliosAdminServer = (*Server)(nil)

// New wraps an already-open Node and Machine. It does not start or stop
// either -- their lifecycle belongs to the caller (cmd/helios), the
// identical ownership rule Machine itself already states about the Node
// it's attached to ("It does not stop n -- the Node this Machine is
// attached to is the caller's, not this package's, to own the lifecycle
// of").
func New(n *raft.Node, m *kvstore.Machine) *Server {
	return &Server{n: n, m: m}
}

// Get serves GetRequest.Consistency's own two paths (F-1, §15.3):
// CONSISTENCY_LINEARIZABLE (and the CONSISTENCY_UNSPECIFIED default)
// through Machine.Get, the log-barrier path; CONSISTENCY_STALE through
// Machine.GetLeaseRead, the cheaper lease path.
//
// A NAMED GAP, NOT AN OVERSIGHT: F-1's own doc describes
// CONSISTENCY_STALE as "served from local state without a leader round
// trip... may not reflect the most recent committed writes" -- a read
// servable by a follower. Machine has no such method. GetLeaseRead is
// the closest available cheaper path, but it is still a LEADER-ONLY,
// still-linearizable read (correct under §9's bounded-clock assumption,
// not "may be stale"); it just skips the round trip a barrier pays for.
// Wiring CONSISTENCY_STALE to it now is the honest best-available
// choice, not a silent redefinition of what F-1 promised -- recorded
// as its own open question in DESIGN.md rather than left implicit.
func (s *Server) Get(ctx context.Context, req *heliosv1.GetRequest) (*heliosv1.GetResponse, error) {
	if req.GetConsistency() == heliosv1.Consistency_CONSISTENCY_STALE {
		value, ok, leaseValid, err := s.m.GetLeaseRead(req.GetKey())
		if err != nil {
			return nil, s.translateErr(err)
		}
		if !leaseValid {
			// Not the same claim as ErrNotLeader: this node might still
			// be leader, just without a lease fresh enough to answer
			// locally right now (a fresh leader, or one whose lease
			// window lapsed). Its own status, not folded into the
			// leader-hint path, which would send a client chasing a
			// "real leader" that may already be this node.
			return nil, status.Error(codes.Unavailable,
				"lease read unavailable right now; retry, or request CONSISTENCY_LINEARIZABLE")
		}
		return s.getResponse(value, ok), nil
	}

	value, ok, err := s.m.Get(req.GetKey())
	if err != nil {
		return nil, s.translateErr(err)
	}
	return s.getResponse(value, ok), nil
}

// getResponse fills Revision from Machine.AppliedIndex(), taken
// immediately after the read that produced value/ok has already
// returned.
//
// AN APPROXIMATION, RECORDED AS ONE, NOT THE EXACT COMMIT INDEX F-1's
// OWN DOC DESCRIBES ("the revision this value was written at"). Machine
// has no per-key write-index tracking -- Get and GetLeaseRead return
// only (value, ok, err); the exact barrier index each call waited on
// never leaves Machine. AppliedIndex() taken right after is a real,
// correct value with a real guarantee (it is >= whatever index this
// read's own barrier waited on, since that barrier already applied
// before Get returned) -- just not necessarily the index the specific
// key was LAST written at, if other writes committed concurrently.
// Threading the exact per-operation index through means widening
// Machine's public API, which has call sites across three existing test
// files (machine_test.go, integration_test.go, fullsystem_test.go) --
// out of scope for wiring the server itself; recorded in DESIGN.md
// rather than done as a drive-by change here.
func (s *Server) getResponse(value []byte, ok bool) *heliosv1.GetResponse {
	resp := &heliosv1.GetResponse{Found: ok}
	if ok {
		resp.Value = value
	}
	resp.Revision = int64(s.m.AppliedIndex())
	return resp
}

// Put writes through Machine.Put (ClientId == 0) or Machine.PutIdempotent
// (F-4): a client_id of 0 means the caller carries no session -- direct
// grpcurl, a future admin tool, anything not going through
// client.Client -- and gets exactly the pre-F-4 behavior, never
// deduplicated. Revision carries the same AppliedIndex()-after-the-call
// approximation Get's doc explains above, applied here to a write's own
// commit index instead of a read's barrier index -- the identical gap,
// not a second one, and unaffected by which of the two paths below ran:
// a deduplicated write still advances AppliedIndex() by way of its own
// (skipped-but-still-recorded) log entry, see machine.go's applyCommand.
func (s *Server) Put(ctx context.Context, req *heliosv1.PutRequest) (*heliosv1.PutResponse, error) {
	var err error
	if req.GetClientId() == 0 {
		err = s.m.Put(req.GetKey(), req.GetValue())
	} else {
		err = s.m.PutIdempotent(req.GetKey(), req.GetValue(), req.GetClientId(), req.GetSequenceNumber())
	}
	if err != nil {
		return nil, s.translateErr(err)
	}
	return &heliosv1.PutResponse{Revision: int64(s.m.AppliedIndex())}, nil
}

// Delete writes through Machine.Delete.
//
// Found is UNIMPLEMENTED here -- always false, not a best-effort guess.
// Machine.Delete reports only success or failure, never whether the key
// existed beforehand; the one way to find out would be a Get
// immediately before the Delete, which is not atomic with it and would
// report a real but MISLEADING answer under a concurrent writer racing
// between the two calls (a wrong-looking-right answer is worse than an
// honestly-blank field the DESIGN.md open question already names
// plainly). Made correct only by a real engine-level change --
// engine.Writer.Delete itself reporting prior existence -- deferred
// rather than approximated with a race here.
func (s *Server) Delete(ctx context.Context, req *heliosv1.DeleteRequest) (*heliosv1.DeleteResponse, error) {
	var err error
	if req.GetClientId() == 0 {
		err = s.m.Delete(req.GetKey())
	} else {
		err = s.m.DeleteIdempotent(req.GetKey(), req.GetClientId(), req.GetSequenceNumber())
	}
	if err != nil {
		return nil, s.translateErr(err)
	}
	return &heliosv1.DeleteResponse{Found: false, Revision: int64(s.m.AppliedIndex())}, nil
}

// Scan serves ScanRequest's own pagination contract (F-1, §15.4; F-6
// implements it): PageToken, when set, takes over as the effective
// StartKey -- it IS the next page's own inclusive lower bound (see
// kvstore.Machine.scanLocked's own doc for exactly why a plain
// "continue from here" key, not an offset or an opaque cursor in the
// usual sense, is what this storage engine's iterators can actually
// support without a Seek method neither has). Limit <= 0 (including
// the ScanRequest zero value) resolves to defaultScanLimit, mirroring
// Machine.Scan's own identical default so the two layers never
// disagree about what "unset" means. Consistency selects Machine.Scan
// (linearizable) or Machine.ScanLeaseRead (lease) exactly the way Get
// already does -- see Server.Get's own doc for the full argument,
// unchanged here.
//
// Every KeyValue.Revision in the response is the SAME
// AppliedIndex()-after-the-call approximation Get's Revision already
// uses (Server.getResponse's own doc) -- one value for the whole page,
// not tracked per key, the identical named gap.
func (s *Server) Scan(ctx context.Context, req *heliosv1.ScanRequest) (*heliosv1.ScanResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultScanLimit
	}
	startKey := req.GetStartKey()
	if len(req.GetPageToken()) > 0 {
		startKey = req.GetPageToken()
	}

	var (
		pairs      []kvstore.KeyValue
		nextCursor []byte
		err        error
	)
	if req.GetConsistency() == heliosv1.Consistency_CONSISTENCY_STALE {
		var leaseValid bool
		pairs, nextCursor, leaseValid, err = s.m.ScanLeaseRead(startKey, req.GetEndKey(), limit)
		if err == nil && !leaseValid {
			// Identical reasoning to Server.Get's own STALE branch: this
			// node might still be leader, just without a lease fresh
			// enough to answer locally right now.
			return nil, status.Error(codes.Unavailable,
				"lease read unavailable right now; retry, or request CONSISTENCY_LINEARIZABLE")
		}
	} else {
		pairs, nextCursor, err = s.m.Scan(startKey, req.GetEndKey(), limit)
	}
	if err != nil {
		return nil, s.translateErr(err)
	}

	revision := int64(s.m.AppliedIndex())
	wire := make([]*heliosv1.KeyValue, len(pairs))
	for i, kv := range pairs {
		wire[i] = &heliosv1.KeyValue{Key: kv.Key, Value: kv.Value, Revision: revision}
	}
	return &heliosv1.ScanResponse{Pairs: wire, NextPageToken: nextCursor}, nil
}

// defaultScanLimit mirrors kvstore.defaultScanPageSize -- kept as its
// own constant rather than importing the unexported one, since a gRPC
// server choosing a wire-level default and a Machine method choosing
// its own Go-level default are two separate decisions that happen to
// agree, not one shared piece of state; see Server.Scan's own doc.
// Equally UNMEASURED against a real workload (DESIGN.md §12).
const defaultScanLimit = 100

// watchBatchSize bounds how many events Server.Watch groups into one
// WatchResponse -- both for replaying retained history and for
// draining a burst of live events before sending. UNMEASURED, same as
// every other constant on this list (DESIGN.md §12). Matches
// WatchResponse's own doc: batching amortizes stream frame overhead
// under bursty writes, the same argument already measured for
// AppendEntries coalescing (§10).
const watchBatchSize = 100

// Watch streams every Put/Delete whose key has req's own KeyPrefix,
// starting from StartRevision (0 = live only, matching WatchRequest's
// own documented zero value).
//
// NOT LEADER-GATED -- Machine.Watch's own doc gives the full argument:
// Watch does not need linearizability, only the in-order, exactly-once
// delivery Raft's own state machine safety property already guarantees
// on every node that applies a given entry, leader or follower.
//
// A REJECTED StartRevision (already evicted from Machine's retained
// history) is codes.OutOfRange, not codes.Unavailable/NotLeader or a
// generic error -- gRPC's own status code for exactly this shape of
// problem: the request itself is fine, but the specific range it asks
// for is no longer servable. The message tells the client what to do
// about it (resync via Scan, then retry Watch from a current
// revision), since silently starting the watch with an undetectable
// gap in it would be worse than refusing outright.
func (s *Server) Watch(req *heliosv1.WatchRequest, stream heliosv1.Helios_WatchServer) error {
	prefix := req.GetKeyPrefix()
	startRevision := int(req.GetStartRevision())

	replay, live, cancel, ok := s.m.Watch(startRevision)
	defer cancel()
	if !ok {
		return status.Errorf(codes.OutOfRange,
			"start_revision %d has already been compacted out of this node's retained watch history; "+
				"resync via Scan and retry Watch with a current revision", startRevision)
	}

	send := func(events []*heliosv1.WatchEvent) error {
		if len(events) == 0 {
			return nil
		}
		return stream.Send(&heliosv1.WatchResponse{Events: events})
	}

	// Replay first, batched in groups of watchBatchSize -- a historical
	// backlog delivered as a handful of frames, not one per event.
	var batch []*heliosv1.WatchEvent
	for _, ev := range replay {
		if !bytes.HasPrefix(ev.Key, prefix) {
			continue
		}
		batch = append(batch, toWireWatchEvent(ev))
		if len(batch) >= watchBatchSize {
			if err := send(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := send(batch); err != nil {
		return err
	}

	// Then live, one event at a time as they arrive, with a quick
	// non-blocking drain of whatever else is already buffered before
	// each send -- the same batching-under-bursts intent, applied to
	// the live path instead of the replay backlog. stream.Context()
	// is what ends this loop under ordinary operation: it is canceled
	// the moment the client disconnects or its own RPC deadline
	// expires, exactly the same context gRPC already threads through
	// every unary handler, just read directly here since a streaming
	// handler has no ctx parameter of its own to use instead. live
	// closing (chOk == false) means THIS Machine shut down
	// (Machine.Close, watch.go's own closeAll) -- ending the stream
	// cleanly rather than blocking on a subsystem that no longer
	// exists to deliver anything.
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev, chOk := <-live:
			if !chOk {
				return nil
			}
			batch = batch[:0]
			if bytes.HasPrefix(ev.Key, prefix) {
				batch = append(batch, toWireWatchEvent(ev))
			}
		drain:
			for len(batch) < watchBatchSize {
				select {
				case ev2, chOk2 := <-live:
					if !chOk2 {
						break drain
					}
					if bytes.HasPrefix(ev2.Key, prefix) {
						batch = append(batch, toWireWatchEvent(ev2))
					}
				default:
					break drain
				}
			}
			if err := send(batch); err != nil {
				return err
			}
		}
	}
}

func toWireWatchEvent(ev kvstore.WatchEvent) *heliosv1.WatchEvent {
	we := &heliosv1.WatchEvent{Key: ev.Key, Revision: int64(ev.Revision)}
	if ev.Tombstone {
		we.Type = heliosv1.WatchEvent_DELETE
	} else {
		we.Type = heliosv1.WatchEvent_PUT
		we.Value = ev.Value
	}
	return we
}

// Status reports this node's own point-in-time view of its Raft and
// storage state (admin.proto, F-9).
//
// DELIBERATELY NOT LEADER-GATED -- THE ONLY OTHER RPC IN THIS PACKAGE
// THAT ISN'T IS Watch (F-7), FOR A RELATED BUT DISTINCT REASON. Watch
// doesn't need a leader because Raft's own state machine safety makes
// its guarantee (in-order, exactly-once delivery) valid on any node.
// Status doesn't need one because it isn't reporting a CLUSTER fact at
// all -- it reports what THIS node itself currently believes, which is
// exactly as true on a follower as on a leader. A follower correctly
// answering "I am a follower, term 3, leader is peer 2" is the
// expected, useful response to an admin caller inspecting that
// specific node -- not a NotLeader error redirecting them somewhere
// else, which would make it impossible to ever ask a follower "what do
// YOU currently think is going on."
func (s *Server) Status(ctx context.Context, req *heliosv1.StatusRequest) (*heliosv1.StatusResponse, error) {
	st := s.n.Status()

	voters := make([]int64, len(st.Voters))
	for i, v := range st.Voters {
		voters[i] = int64(v)
	}

	return &heliosv1.StatusResponse{
		NodeId:              int64(st.ID),
		RaftState:           st.State.String(),
		Term:                int64(st.Term),
		LeaderId:            int64(st.LeaderID),
		CommitIndex:         int64(st.CommitIndex),
		LastApplied:         int64(st.LastApplied),
		LogLength:           int64(st.LogLength),
		SnapshotIndex:       int64(st.SnapshotIndex),
		SnapshotTerm:        int64(st.SnapshotTerm),
		Voters:              voters,
		MachineAppliedIndex: int64(s.m.AppliedIndex()),
		Fault:               s.m.Fault(),
	}, nil
}

// translateErr maps a kvstore error to a gRPC status.
//
// codes.Unavailable FOR ErrNotLeader, NOT codes.FailedPrecondition --
// checked against gRPC's own status-code litmus test, not picked by
// feel. The gRPC documentation's own guidance: use UNAVAILABLE when
// "the client can retry just the failing call" (elsewhere, against a
// different node); use FAILED_PRECONDITION when the client should NOT
// retry until the system state is explicitly fixed. F-3's own stated
// job -- "retry with backoff on NotLeader" -- is exactly the first
// case: the failing call itself, redirected, is expected to succeed
// with no other state needing to change first.
func (s *Server) translateErr(err error) error {
	switch {
	case errors.Is(err, kvstore.ErrNotLeader):
		st := status.New(codes.Unavailable, "not the leader")
		hint := &heliosv1.NotLeaderDetail{LeaderId: int64(s.n.LeaderHint())}
		if withDetails, dErr := st.WithDetails(hint); dErr == nil {
			return withDetails.Err()
		}
		// WithDetails failing at all is itself unusual (a marshal
		// failure on a two-field message); fall back to the plain
		// status rather than losing the original error entirely.
		return st.Err()
	case errors.Is(err, kvstore.ErrReadTimedOut):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Errorf(codes.Internal, "kvstore: %v", err)
	}
}