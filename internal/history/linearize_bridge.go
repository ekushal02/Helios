package history

import (
	"fmt"
	"sort"

	"github.com/ekushal02/helios/internal/linearize"
)

// ToLinearizeOperations converts a recorded Event stream -- the output of
// a Recorder's own Events() method -- into the []linearize.Operation
// CheckLinearizability actually needs, pairing each operation's own Invoke
// and Return by OpID.
//
// THIS LIVES IN package history, NOT package linearize, DELIBERATELY. The
// checker's own core (internal/linearize) has no dependency on this
// package, or on anything that imports gRPC, at all -- it is a general,
// reusable linearizability checker, buildable and testable with no network
// access whatsoever. This function is the one piece of code that needs
// both this package's own Event shape and linearize's own Operation shape
// at once, and putting it here rather than there is what keeps that
// dependency pointing the one direction it can without creating a cycle:
// history already depends on client (and therefore gRPC); linearize
// depends on nothing beyond the standard library. Reversing this --
// putting the bridge in package linearize and importing history from
// there -- would make linearize.go and kvmodel.go's own tests unbuildable
// in any environment without gRPC available, exactly the limitation this
// split exists to avoid.
//
// Returned operations are sorted by OpID, purely for reproducible
// diagnostics (a linearize.Violation's own OpIndex means the same thing
// across repeated runs of the identical input) -- CheckLinearizability's
// own answer does not depend on slice order at all.
//
// TWO CATEGORIES OF EVENT ARE DELIBERATELY EXCLUDED, NOT ERRORED ON.
//
// An operation whose own recorded Output carries a non-empty Err is
// AMBIGUOUS: client/client.go's own retry loop means a client-visible
// error does not necessarily mean the operation never took effect (a
// commit that succeeded on the server but whose acknowledgment was lost is
// exactly the scenario internal/raft's own reply-drop tests exist to
// exercise) -- correctly modeling "this may or may not have happened" needs
// trying BOTH possibilities in the search, real future work this first
// version does not attempt. An operation invoked but never returned (its
// own Recorder was read mid-flight, or the process ended before the
// matching Return was ever recorded) is the identical ambiguity for the
// identical reason. Both are silently dropped from the returned slice
// rather than causing this function to fail outright -- a realistic
// recorded history from a live chaos scenario will often contain a few of
// either, and refusing to check the rest of an otherwise good history over
// that would defeat the point of recording one at all.
//
// SCAN, SCANSTALE, AND SCANALL ARE A HARD ERROR, NOT A SILENT EXCLUSION --
// see linearize.KVModel's own doc for why they are out of scope, and why
// silently ignoring them here (rather than refusing the whole conversion)
// would let a real Scan-related violation slip through unchecked instead
// of being surfaced as "cannot check this history at all."
func ToLinearizeOperations(events []Event) ([]linearize.Operation, error) {
	type pair struct {
		invoke *Event
		ret    *Event
	}
	byOp := make(map[int64]*pair)

	for i := range events {
		e := &events[i]
		switch e.Kind {
		case KindScan, KindScanStale, KindScanAll:
			return nil, fmt.Errorf("history: event (op %d, client %d, %s) is a Scan-family operation, "+
				"which the linearizability checker does not model -- see linearize.KVModel's own doc",
				e.OpID, e.ClientID, e.Kind)
		}

		p := byOp[e.OpID]
		if p == nil {
			p = &pair{}
			byOp[e.OpID] = p
		}
		if e.IsInvoke {
			p.invoke = e
		} else {
			p.ret = e
		}
	}

	opIDs := make([]int64, 0, len(byOp))
	for id := range byOp {
		opIDs = append(opIDs, id)
	}
	sort.Slice(opIDs, func(i, j int) bool { return opIDs[i] < opIDs[j] })

	var ops []linearize.Operation
	for _, id := range opIDs {
		p := byOp[id]
		if p.invoke == nil || p.ret == nil {
			continue // an incomplete operation -- see this function's own doc
		}
		input, output, err := kvOperation(p.invoke, p.ret)
		if err != nil {
			return nil, err
		}
		if output == nil {
			continue // an errored operation -- see this function's own doc
		}
		ops = append(ops, linearize.Operation{
			ClientID: p.invoke.ClientID,
			Start:    p.invoke.Timestamp,
			End:      p.ret.Timestamp,
			Input:    input,
			Output:   output,
		})
	}
	return ops, nil
}

// kvOperation converts one matched invoke/return pair into a
// linearize.KVInput/linearize.KVOutput pair. output == nil (with err ==
// nil) signals an operation that returned a client-visible error and
// should be excluded, per ToLinearizeOperations' own doc. err != nil
// signals a genuine, unexpected shape -- a Kind this function does not
// recognize, or a type assertion that should have succeeded but didn't.
// RecordingClient's own methods (recorder.go) only ever store the exact
// struct types this package defines, so this branch firing at all points
// at a real bug in one of the two, not at anything a real recorded history
// could produce on its own.
func kvOperation(invoke, ret *Event) (input, output any, err error) {
	switch invoke.Kind {
	case KindGet, KindGetStale:
		in, ok := invoke.Input.(GetInput)
		if !ok {
			return nil, nil, fmt.Errorf("history: op %d: Get invoke Input has type %T, want GetInput", invoke.OpID, invoke.Input)
		}
		out, ok := ret.Output.(GetOutput)
		if !ok {
			return nil, nil, fmt.Errorf("history: op %d: Get return Output has type %T, want GetOutput", invoke.OpID, ret.Output)
		}
		if out.Err != "" {
			return nil, nil, nil
		}
		return linearize.KVInput{Op: linearize.KVGet, Key: string(in.Key)},
			linearize.KVOutput{Value: string(out.Value), Found: out.Ok}, nil

	case KindPut:
		in, ok := invoke.Input.(PutInput)
		if !ok {
			return nil, nil, fmt.Errorf("history: op %d: Put invoke Input has type %T, want PutInput", invoke.OpID, invoke.Input)
		}
		out, ok := ret.Output.(PutOutput)
		if !ok {
			return nil, nil, fmt.Errorf("history: op %d: Put return Output has type %T, want PutOutput", invoke.OpID, ret.Output)
		}
		if out.Err != "" {
			return nil, nil, nil
		}
		return linearize.KVInput{Op: linearize.KVPut, Key: string(in.Key), Value: string(in.Value)},
			linearize.KVOutput{}, nil

	case KindDelete:
		in, ok := invoke.Input.(DeleteInput)
		if !ok {
			return nil, nil, fmt.Errorf("history: op %d: Delete invoke Input has type %T, want DeleteInput", invoke.OpID, invoke.Input)
		}
		out, ok := ret.Output.(DeleteOutput)
		if !ok {
			return nil, nil, fmt.Errorf("history: op %d: Delete return Output has type %T, want DeleteOutput", invoke.OpID, ret.Output)
		}
		if out.Err != "" {
			return nil, nil, nil
		}
		return linearize.KVInput{Op: linearize.KVDelete, Key: string(in.Key)}, linearize.KVOutput{}, nil

	default:
		return nil, nil, fmt.Errorf("history: op %d: unrecognized Kind %s", invoke.OpID, invoke.Kind)
	}
}
