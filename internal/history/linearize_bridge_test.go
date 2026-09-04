package history

import (
	"context"
	"testing"
	"time"

	"github.com/ekushal02/helios/internal/linearize"
)

func gev(clientID int, opID int64, kind Kind, isInvoke bool, t time.Time, input, output any) Event {
	return Event{ClientID: clientID, OpID: opID, Kind: kind, IsInvoke: isInvoke, Timestamp: t, Input: input, Output: output}
}

// TestToLinearizeOperationsPairsInvokeAndReturn is the direct, hand-built
// check: a Put and a Get, each a clean invoke/return pair, convert into
// exactly two linearize.Operations with the right Start/End/Input/Output.
func TestToLinearizeOperationsPairsInvokeAndReturn(t *testing.T) {
	t0 := time.Now()
	events := []Event{
		gev(0, 0, KindPut, true, t0, PutInput{Key: []byte("a"), Value: []byte("1")}, nil),
		gev(0, 0, KindPut, false, t0.Add(10*time.Millisecond), nil, PutOutput{Revision: 5}),
		gev(0, 1, KindGet, true, t0.Add(20*time.Millisecond), GetInput{Key: []byte("a")}, nil),
		gev(0, 1, KindGet, false, t0.Add(30*time.Millisecond), nil, GetOutput{Value: []byte("1"), Ok: true, Revision: 5}),
	}

	ops, err := ToLinearizeOperations(events)
	if err != nil {
		t.Fatalf("ToLinearizeOperations: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("got %d operations, want 2", len(ops))
	}

	put := ops[0]
	if !put.Start.Equal(t0) || !put.End.Equal(t0.Add(10*time.Millisecond)) {
		t.Errorf("put: Start/End = %v/%v, want %v/%v", put.Start, put.End, t0, t0.Add(10*time.Millisecond))
	}
	in, ok := put.Input.(linearize.KVInput)
	if !ok || in.Op != linearize.KVPut || in.Key != "a" || in.Value != "1" {
		t.Errorf("put: Input = %+v (ok=%v), want KVInput{Op:Put, Key:a, Value:1}", put.Input, ok)
	}

	get := ops[1]
	out, ok := get.Output.(linearize.KVOutput)
	if !ok || !out.Found || out.Value != "1" {
		t.Errorf("get: Output = %+v (ok=%v), want KVOutput{Value:1, Found:true}", get.Output, ok)
	}
}

// TestToLinearizeOperationsExcludesErroredOperations confirms an operation
// whose Output carries a non-empty Err never reaches the returned slice --
// its outcome is ambiguous, not a violation to check against.
func TestToLinearizeOperationsExcludesErroredOperations(t *testing.T) {
	t0 := time.Now()
	events := []Event{
		gev(0, 0, KindPut, true, t0, PutInput{Key: []byte("a"), Value: []byte("1")}, nil),
		gev(0, 0, KindPut, false, t0.Add(time.Millisecond), nil, PutOutput{Err: "deadline exceeded"}),
	}
	ops, err := ToLinearizeOperations(events)
	if err != nil {
		t.Fatalf("ToLinearizeOperations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d operations for an errored Put, want 0 (it should be excluded, not erroring)", len(ops))
	}
}

// TestToLinearizeOperationsExcludesIncompleteOperations confirms an Invoke
// with no matching Return is excluded rather than causing the whole
// conversion to fail -- the realistic shape of a history recorded mid-flight.
func TestToLinearizeOperationsExcludesIncompleteOperations(t *testing.T) {
	events := []Event{
		gev(0, 0, KindGet, true, time.Now(), GetInput{Key: []byte("a")}, nil),
		// no matching return
	}
	ops, err := ToLinearizeOperations(events)
	if err != nil {
		t.Fatalf("ToLinearizeOperations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d operations for an invoke with no return, want 0", len(ops))
	}
}

// TestToLinearizeOperationsRejectsScanOperations locks in the documented
// hard boundary: a Scan-family event makes the whole conversion fail,
// rather than silently dropping it the way an errored or incomplete
// operation is dropped -- a real Scan-related violation must never slip
// through unchecked.
func TestToLinearizeOperationsRejectsScanOperations(t *testing.T) {
	events := []Event{
		gev(0, 0, KindScan, true, time.Now(), ScanInput{StartKey: []byte("a")}, nil),
		gev(0, 0, KindScan, false, time.Now(), nil, ScanOutput{}),
	}
	if _, err := ToLinearizeOperations(events); err == nil {
		t.Fatal("ToLinearizeOperations with a Scan event: err = nil, want an error")
	}
}

// TestToLinearizeOperationsEndToEndAgainstARealServer ties the whole
// pipeline together against a real, unfaulted, single-node server: record
// a real workload through RecordingClient, convert the result, and check
// it -- a healthy server talking to well-behaved clients must always
// produce a linearizable history. This is the test that actually proves
// internal/history and internal/linearize compose correctly, not just that
// each independently does what its own unit tests say it does.
func TestToLinearizeOperationsEndToEndAgainstARealServer(t *testing.T) {
	c := newRealClient(t)
	rec := NewRecorder()
	rc := Wrap(c, rec, 0)
	ctx := context.Background()

	if _, err := rc.Put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, _, err := rc.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := rc.Put(ctx, []byte("a"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, _, err := rc.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := rc.Delete(ctx, []byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, _, err := rc.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("Get: %v", err)
	}

	ops, err := ToLinearizeOperations(rec.Events())
	if err != nil {
		t.Fatalf("ToLinearizeOperations: %v", err)
	}
	if len(ops) != 6 {
		t.Fatalf("got %d operations, want 6", len(ops))
	}

	res, err := linearize.CheckLinearizability(ops, linearize.KVModel{})
	if err != nil {
		t.Fatalf("CheckLinearizability: %v", err)
	}
	if !res.Linearizable {
		t.Fatalf("a real, unfaulted single-node server produced a non-linearizable history: witness=%+v", res.Witness)
	}
}

// TestToLinearizeOperationsEndToEndConcurrentWorkload is the same
// end-to-end shape under real concurrency -- several actors hammering the
// same small key space at once, exactly what a chaos scenario's own
// workload looks like, still against a healthy, unfaulted server, still
// expected to linearize.
func TestToLinearizeOperationsEndToEndConcurrentWorkload(t *testing.T) {
	c := newRealClient(t)
	rec := NewRecorder()

	const actors = 4
	const opsPerActor = 15
	done := make(chan struct{}, actors)
	for a := 0; a < actors; a++ {
		go func(actorID int) {
			defer func() { done <- struct{}{} }()
			rc := Wrap(c, rec, actorID)
			ctx := context.Background()
			keys := [][]byte{[]byte("x"), []byte("y")}
			for i := 0; i < opsPerActor; i++ {
				key := keys[i%len(keys)]
				if i%3 == 0 {
					rc.Get(ctx, key)
				} else {
					rc.Put(ctx, key, []byte{byte('0' + i%10)})
				}
			}
		}(a)
	}
	for a := 0; a < actors; a++ {
		<-done
	}

	ops, err := ToLinearizeOperations(rec.Events())
	if err != nil {
		t.Fatalf("ToLinearizeOperations: %v", err)
	}
	if len(ops) == 0 {
		t.Fatal("0 operations converted from a real concurrent workload -- the test drove no successful operations at all")
	}

	res, err := linearize.CheckLinearizability(ops, linearize.KVModel{})
	if err != nil {
		t.Fatalf("CheckLinearizability: %v", err)
	}
	if !res.Linearizable {
		t.Fatalf("a real, unfaulted server under concurrent load produced a non-linearizable history "+
			"(%d operations checked): witness=%+v", len(ops), res.Witness)
	}
}
