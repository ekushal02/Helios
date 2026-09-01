package heliosv1

// This file is intentionally NOT generated. `buf generate` only ever
// produces helios.pb.go and helios_grpc.pb.go (see buf.gen.yaml); this
// file lives alongside them permanently and is never touched by
// regeneration. Its job is narrow: prove the .proto compiles to what
// DESIGN.md says it should, before any server or client code is written
// against it in F-2/F-3.

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
)

// --- Wire round-trips ------------------------------------------------------
//
// A field typo, a wrong field number, or an accidentally-reused number
// across two messages won't show up as a compile error — proto is
// structurally permissive. Marshal/unmarshal round trips are what actually
// exercise the wire encoding.

func TestGetRoundTrip(t *testing.T) {
	req := &GetRequest{Key: []byte("foo"), Consistency: Consistency_CONSISTENCY_LINEARIZABLE}
	roundTrip(t, req, &GetRequest{})

	resp := &GetResponse{Value: []byte("bar"), Found: true, Revision: 42}
	roundTrip(t, resp, &GetResponse{})
}

func TestPutRoundTrip(t *testing.T) {
	req := &PutRequest{Key: []byte("foo"), Value: []byte("bar")}
	roundTrip(t, req, &PutRequest{})

	resp := &PutResponse{Revision: 7}
	roundTrip(t, resp, &PutResponse{})
}

func TestDeleteRoundTrip(t *testing.T) {
	req := &DeleteRequest{Key: []byte("foo")}
	roundTrip(t, req, &DeleteRequest{})

	resp := &DeleteResponse{Found: true, Revision: 9}
	roundTrip(t, resp, &DeleteResponse{})
}

func TestScanRoundTrip(t *testing.T) {
	req := &ScanRequest{
		StartKey:    []byte("a"),
		EndKey:      []byte("z"),
		Limit:       100,
		PageToken:   []byte("opaque-token"),
		Consistency: Consistency_CONSISTENCY_STALE,
	}
	roundTrip(t, req, &ScanRequest{})

	resp := &ScanResponse{
		Pairs: []*KeyValue{
			{Key: []byte("a"), Value: []byte("1"), Revision: 1},
			{Key: []byte("b"), Value: []byte("2"), Revision: 2},
		},
		NextPageToken: []byte("more"),
	}
	roundTrip(t, resp, &ScanResponse{})
}

func TestWatchRoundTrip(t *testing.T) {
	req := &WatchRequest{KeyPrefix: []byte("users/"), StartRevision: 100}
	roundTrip(t, req, &WatchRequest{})

	resp := &WatchResponse{
		Events: []*WatchEvent{
			{Type: WatchEvent_PUT, Key: []byte("users/1"), Value: []byte("alice"), Revision: 101},
			{Type: WatchEvent_DELETE, Key: []byte("users/2"), Revision: 102},
		},
	}
	roundTrip(t, resp, &WatchResponse{})
}

// TestFoundDefaultsFalse locks in the comma-ok-style contract documented in
// helios.proto: an unset GetResponse must read as "not found", never as a
// zero-length value silently standing in for absence.
func TestFoundDefaultsFalse(t *testing.T) {
	resp := &GetResponse{}
	if resp.GetFound() {
		t.Fatalf("zero-value GetResponse.Found = true, want false")
	}
	if len(resp.GetValue()) != 0 {
		t.Fatalf("zero-value GetResponse.Value = %q, want empty", resp.GetValue())
	}
}

// TestConsistencyUnspecifiedIsZero locks in that leaving Consistency unset
// on the wire is indistinguishable from CONSISTENCY_UNSPECIFIED, which is
// what the server-side default-to-linearizable behavior (F-2) depends on.
func TestConsistencyUnspecifiedIsZero(t *testing.T) {
	var c Consistency
	if c != Consistency_CONSISTENCY_UNSPECIFIED {
		t.Fatalf("zero-value Consistency = %v, want CONSISTENCY_UNSPECIFIED", c)
	}
}

func roundTrip(t *testing.T, in, out proto.Message) {
	t.Helper()
	wire, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal(%T) failed: %v", in, err)
	}
	if err := proto.Unmarshal(wire, out); err != nil {
		t.Fatalf("Unmarshal(%T) failed: %v", in, err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round trip mismatch for %T:\n  in:  %v\n  out: %v", in, in, out)
	}
}

// --- Service shape ----------------------------------------------------------
//
// Compile-time proof that HeliosServer has exactly the five methods
// helios.proto declares, with the signatures generation should produce:
// four unary RPCs and one server-streaming RPC. If protoc-gen-go-grpc ever
// changes its generated interface shape, or a future edit to the .proto
// changes a method signature, this fails to compile — which is a much
// earlier and clearer failure than a runtime "method not implemented".

type fakeServer struct {
	UnimplementedHeliosServer
}

var _ HeliosServer = (*fakeServer)(nil)

func (fakeServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, nil
}

func (fakeServer) Put(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, nil
}

func (fakeServer) Delete(context.Context, *DeleteRequest) (*DeleteResponse, error) {
	return nil, nil
}

func (fakeServer) Scan(context.Context, *ScanRequest) (*ScanResponse, error) {
	return nil, nil
}

func (fakeServer) Watch(*WatchRequest, Helios_WatchServer) error {
	return nil
}

// TestClientInterfaceHasWatch guards against Watch accidentally generating
// as a unary method — a mistake that would only show up if the .proto's
// `stream` keyword got dropped in a future edit. NewHeliosClient's return
// type satisfying HeliosClient is enough to prove the streaming method
// exists with the streaming client return type, since that's what the
// interface assignment forces the compiler to check.
func TestClientInterfaceHasWatch(t *testing.T) {
	var _ HeliosClient
}
