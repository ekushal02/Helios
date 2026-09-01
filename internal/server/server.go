// Package server wires heliosv1.HeliosServer (api/proto/helios/v1/helios.proto,
// Phase F-1) to a single node's *kvstore.Machine and the *raft.Node it's
// attached to. One Server per node -- cmd/helios constructs exactly one,
// the same per-node-singleton shape as its own Machine and Node.
//
// This is a translation layer and nothing more: it does not reimplement
// any read or write logic. Every RPC calls straight through to an
// existing Machine method (Get, GetLeaseRead, Put, Delete, Scan,
// ScanLeaseRead -- all built and tested in Phase H and F-6) and
// translates the Go return values into wire types and gRPC status
// codes. Watch is Phase F-7's job; embedding
// heliosv1.UnimplementedHeliosServer means that one RPC correctly
// answers codes.Unimplemented until then, without this package needing
// a stub of its own for it.
package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	heliosv1 "github.com/ekushal02/helios/api/helios/v1"
	"github.com/ekushal02/helios/internal/kvstore"
	"github.com/ekushal02/helios/internal/raft"
)

// Server implements heliosv1.HeliosServer.
type Server struct {
	// Watch (F-7) falls through to this until implemented here -- see
	// the package doc. Scan no longer does, as of F-6.
	heliosv1.UnimplementedHeliosServer

	n *raft.Node
	m *kvstore.Machine
}

var _ heliosv1.HeliosServer = (*Server)(nil)

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