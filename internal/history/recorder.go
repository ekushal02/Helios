package history

import (
	"context"
	"sync"
	"time"

	"github.com/ekushal02/helios/client"
)

// Recorder is the shared, ordered history log. One Recorder is meant to be
// wrapped by several RecordingClients at once -- one per concurrent actor
// in whatever benchmark or chaos scenario is driving the workload, each
// carrying its own clientID -- so that OpIDs stay globally unique and every
// actor's own operations land in a single, shared timeline rather than
// several disjoint per-client logs a checker would have to merge itself
// later.
//
// Safe for concurrent use: every RecordingClient method that touches a
// Recorder does so through Invoke/Return below, both of which hold the
// same mutex for their entire body.
type Recorder struct {
	mu       sync.Mutex
	nextOpID int64
	events   []Event
}

// NewRecorder returns an empty Recorder, ready to be shared across however
// many RecordingClients a caller wraps around it.
func NewRecorder() *Recorder {
	return &Recorder{}
}

// Invoke records that an operation of the given kind is starting, and
// returns the OpID its matching Return call must be given. Exported so a
// caller wrapping an operation this package does not already cover
// (Watch, or a future RPC) can still write directly into a shared
// Recorder without this package having to know about it.
func (r *Recorder) Invoke(clientID int, kind Kind, input any) (opID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	opID = r.nextOpID
	r.nextOpID++
	r.events = append(r.events, Event{
		ClientID:  clientID,
		OpID:      opID,
		Kind:      kind,
		IsInvoke:  true,
		Timestamp: time.Now(),
		Input:     input,
	})
	return opID
}

// Return records that the operation opID identifies has completed.
func (r *Recorder) Return(clientID int, opID int64, kind Kind, output any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, Event{
		ClientID:  clientID,
		OpID:      opID,
		Kind:      kind,
		IsInvoke:  false,
		Timestamp: time.Now(),
		Output:    output,
	})
}

// Events returns a copy of every event recorded so far, in the order they
// were recorded -- which is invoke/return INTERLEAVING order, not OpID
// order: a slow operation's own Return can be appended well after several
// later operations' own Invoke calls, exactly the overlap a linearizability
// checker needs to see to do its own job. Callers that want events grouped
// or sorted do that themselves; this package does not reorder anything.
func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// Len reports how many events (invokes plus returns) have been recorded so
// far -- always even once every in-flight operation has returned, since
// every Invoke this package itself issues (via RecordingClient) is matched
// by exactly one Return before the wrapped call returns.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// RecordingClient wraps a *client.Client, logging one Invoke and one
// Return, both timestamped, for every operation it forwards -- everything
// client.Client exports except Watch (see Kind's own doc for why). Every
// other exported method of the embedded *client.Client (Close, and any
// future addition this package has not been taught about) remains reachable
// unchanged through Go's own method promotion; only the methods redefined
// below are actually recorded.
type RecordingClient struct {
	*client.Client
	rec      *Recorder
	clientID int
}

// Wrap returns a RecordingClient that forwards every recorded operation to
// c and logs it into rec under clientID. clientID identifies this ACTOR in
// the recorded history -- which concurrent caller issued a given operation
// -- and is entirely this package's own concern: it does not need to, and
// generally should not, match client.Client's own internal write-dedup
// identity (a different concept for a different purpose -- see that
// field's own doc in client.go). Callers driving N concurrent workers
// against one or more underlying Clients typically assign 0..N-1.
func Wrap(c *client.Client, rec *Recorder, clientID int) *RecordingClient {
	return &RecordingClient{Client: c, rec: rec, clientID: clientID}
}

func (rc *RecordingClient) Get(ctx context.Context, key []byte) (value []byte, ok bool, revision int64, err error) {
	opID := rc.rec.Invoke(rc.clientID, KindGet, GetInput{Key: copyBytes(key)})
	value, ok, revision, err = rc.Client.Get(ctx, key)
	rc.rec.Return(rc.clientID, opID, KindGet, GetOutput{
		Value: copyBytes(value), Ok: ok, Revision: revision, Err: errString(err),
	})
	return value, ok, revision, err
}

func (rc *RecordingClient) GetStale(ctx context.Context, key []byte) (value []byte, ok bool, revision int64, err error) {
	opID := rc.rec.Invoke(rc.clientID, KindGetStale, GetInput{Key: copyBytes(key)})
	value, ok, revision, err = rc.Client.GetStale(ctx, key)
	rc.rec.Return(rc.clientID, opID, KindGetStale, GetOutput{
		Value: copyBytes(value), Ok: ok, Revision: revision, Err: errString(err),
	})
	return value, ok, revision, err
}

func (rc *RecordingClient) Put(ctx context.Context, key, value []byte) (revision int64, err error) {
	opID := rc.rec.Invoke(rc.clientID, KindPut, PutInput{Key: copyBytes(key), Value: copyBytes(value)})
	revision, err = rc.Client.Put(ctx, key, value)
	rc.rec.Return(rc.clientID, opID, KindPut, PutOutput{Revision: revision, Err: errString(err)})
	return revision, err
}

func (rc *RecordingClient) Delete(ctx context.Context, key []byte) (revision int64, err error) {
	opID := rc.rec.Invoke(rc.clientID, KindDelete, DeleteInput{Key: copyBytes(key)})
	revision, err = rc.Client.Delete(ctx, key)
	rc.rec.Return(rc.clientID, opID, KindDelete, DeleteOutput{Revision: revision, Err: errString(err)})
	return revision, err
}

func (rc *RecordingClient) Scan(ctx context.Context, startKey, endKey []byte, limit int, pageToken []byte) (pairs []client.KeyValue, nextPageToken []byte, err error) {
	opID := rc.rec.Invoke(rc.clientID, KindScan, ScanInput{
		StartKey: copyBytes(startKey), EndKey: copyBytes(endKey), Limit: limit, PageToken: copyBytes(pageToken),
	})
	pairs, nextPageToken, err = rc.Client.Scan(ctx, startKey, endKey, limit, pageToken)
	rc.rec.Return(rc.clientID, opID, KindScan, ScanOutput{
		Pairs: copyPairs(pairs), NextPageToken: copyBytes(nextPageToken), Err: errString(err),
	})
	return pairs, nextPageToken, err
}

func (rc *RecordingClient) ScanStale(ctx context.Context, startKey, endKey []byte, limit int, pageToken []byte) (pairs []client.KeyValue, nextPageToken []byte, err error) {
	opID := rc.rec.Invoke(rc.clientID, KindScanStale, ScanInput{
		StartKey: copyBytes(startKey), EndKey: copyBytes(endKey), Limit: limit, PageToken: copyBytes(pageToken),
	})
	pairs, nextPageToken, err = rc.Client.ScanStale(ctx, startKey, endKey, limit, pageToken)
	rc.rec.Return(rc.clientID, opID, KindScanStale, ScanOutput{
		Pairs: copyPairs(pairs), NextPageToken: copyBytes(nextPageToken), Err: errString(err),
	})
	return pairs, nextPageToken, err
}

func (rc *RecordingClient) ScanAll(ctx context.Context, startKey, endKey []byte, limit int) (pairs []client.KeyValue, err error) {
	opID := rc.rec.Invoke(rc.clientID, KindScanAll, ScanAllInput{
		StartKey: copyBytes(startKey), EndKey: copyBytes(endKey), Limit: limit,
	})
	pairs, err = rc.Client.ScanAll(ctx, startKey, endKey, limit)
	rc.rec.Return(rc.clientID, opID, KindScanAll, ScanAllOutput{Pairs: copyPairs(pairs), Err: errString(err)})
	return pairs, err
}

// copyPairs deep-copies a []client.KeyValue -- same reasoning as copyBytes,
// one level up: Scan/ScanAll's own returned slice, and the byte slices
// inside each pair, must not be aliased into a recorded Event that a
// caller might read back long after the original slice's own backing
// array could have been reused or mutated.
func copyPairs(pairs []client.KeyValue) []client.KeyValue {
	if pairs == nil {
		return nil
	}
	out := make([]client.KeyValue, len(pairs))
	for i, p := range pairs {
		out[i] = client.KeyValue{Key: copyBytes(p.Key), Value: copyBytes(p.Value), Revision: p.Revision}
	}
	return out
}
