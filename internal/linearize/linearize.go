// Package linearize checks whether a recorded history of concurrent
// operations is LINEARIZABLE: whether there exists some total order of the
// operations, consistent with the real-time order their invocations and
// responses actually happened in, under which each operation's own
// recorded result is exactly what a correct SEQUENTIAL execution of the
// object would have produced.
//
// This is Wing and Gong's own 1988 algorithm ("Testing and Verifying
// Concurrent Objects"), implemented directly rather than wired in from an
// existing checker (Porcupine and similar tools implement a related but
// distinct, more heavily optimized algorithm) -- chosen specifically so
// this project understands, rather than merely invokes, what checking a
// history for linearizability actually requires, matching how every other
// major component here (Raft itself, the LSM engine, the gRPC transport)
// was built from its own first principles rather than assembled from a
// library that does the same job.
//
// =============================================================================
// THE ALGORITHM, IN FULL
// =============================================================================
//
// A HISTORY is a set of OPERATIONS, each with a real-time INVOCATION
// instant and a real-time RESPONSE instant (Operation.Start and
// Operation.End below), an INPUT (what the operation was called with), and
// an OUTPUT (what it actually returned).
//
// Two operations are CONCURRENT if their [Start, End] intervals overlap;
// otherwise one happened strictly before the other. A LINEARIZATION is a
// total order of every operation in the history such that:
//
//  1. real-time order is respected: if op A's End is at or before op B's
//     Start, A must come before B in the order (concurrent operations may
//     be ordered either way -- that flexibility is the entire reason
//     checking this is hard);
//  2. the order is legal for the object's own SEQUENTIAL SPECIFICATION
//     (Model, below): applying every operation's own Input to the model,
//     one at a time, in this order, starting from the model's own initial
//     state, produces exactly the Output each operation actually recorded,
//     at every step.
//
// A history is linearizable if AT LEAST ONE such order exists.
//
// THE SEARCH. CheckLinearizability performs a recursive backtracking
// search directly over "which operation could legally be linearized
// next," exactly as Wing and Gong's own paper describes it. At any point
// in the search, the operations already placed in the order are tracked as
// a BITSET (linearized, a uint64 -- see the 64-operation limit this
// implies, documented on CheckLinearizability itself). An operation i, not
// yet in that bitset, is ELIGIBLE to be placed NEXT if and only if no
// OTHER not-yet-linearized operation j necessarily precedes it in real
// time (ops[j].End at or before ops[i].Start) -- eligible, below, is
// exactly this check. Choosing an eligible operation means: apply its
// Input to the model's current state, and if the model's own resulting
// Output matches what that operation ACTUALLY recorded (Model.Equal),
// recurse with that operation added to the bitset and the model advanced
// to its new state. If every eligible choice's own recursive attempt
// fails, this point in the search fails and the caller backtracks to try
// a different eligible operation instead.
//
// MEMOIZATION. The search space is, in the worst case, exponential
// (a well-known, unavoidable property of linearizability checking in
// general, not a shortcoming of this particular implementation -- Wing and
// Gong's own paper proves the general problem is NP-complete). The one
// optimization applied here, matching the one Wing and Gong's own paper
// describes, is memoizing FAILURE: once a given (bitset of operations
// already linearized, resulting model state) pair has been explored and
// found to have no valid continuation, that exact pair is never explored
// again. This requires the model state itself to be comparable across
// different orderings of the SAME set of operations -- two different
// orderings of an identical operation set can legitimately reach two
// DIFFERENT states (a KV store's own Put(k, "a") then Put(k, "b") does not
// commute), so memoizing on the bitset ALONE would be unsound: it would
// conflate two genuinely different reachable states as if they were one.
// Model.StateKey exists specifically to give the search a canonical,
// comparable representation of a state for exactly this purpose, distinct
// from the state value itself (which is free to be any Go value the
// model's own Apply finds convenient, including one that is not directly
// comparable, like a map).
package linearize

import (
	"fmt"
	"math/bits"
	"time"
)

// Operation is one client operation, already paired into a single
// invoke/response record -- the shape this checker's own search actually
// needs, independent of wherever the recording came from. See
// internal/linearize's own history bridge (a separate file, deliberately:
// it is the one piece of this package that depends on internal/history,
// and therefore transitively on gRPC -- see that file's own doc) for
// converting a recorded internal/history.Event stream into a slice of
// these.
type Operation struct {
	// ClientID identifies which concurrent actor issued this operation --
	// carried through for a caller's own diagnostics (which client's
	// operations were involved in a failing history) but not used by the
	// algorithm itself, which only ever reasons about real-time ordering
	// and the model's own sequential behavior.
	ClientID int

	// Start and End are this operation's own invocation and response
	// instants. Start must not be after End; CheckLinearizability rejects
	// any Operation where it is, rather than silently producing a
	// meaningless answer from malformed input.
	Start time.Time
	End   time.Time

	// Input is passed to Model.Apply exactly as given. Output is what this
	// operation ACTUALLY recorded as having happened, compared against
	// Model.Apply's own returned output via Model.Equal at every step of
	// the search.
	Input  any
	Output any
}

// Model is a sequential specification for some object: given the object's
// current state and one operation's own Input, what state does the object
// move to, and what output does a correct, single-threaded implementation
// return? CheckLinearizability never calls these concurrently with
// themselves; a Model implementation does not need its own locking.
type Model interface {
	// Init returns the object's own starting state -- an empty map for a
	// KV store, for instance.
	Init() any

	// Apply computes the new state and the output a sequential execution
	// of input against state would produce. Must be a pure function of its
	// two arguments: the search calls this along many different candidate
	// orderings, including ones it later abandons, and Apply must not
	// mutate state in place or otherwise carry side effects between calls
	// -- see KVModel's own doc for the concrete discipline (copy on
	// write) this implies for any model backed by a Go map or slice.
	Apply(state any, input any) (newState any, output any)

	// Equal reports whether two outputs represent the SAME result, for
	// this model's own purposes -- what CheckLinearizability compares
	// Apply's own returned output against an operation's actual, recorded
	// Output. Deliberately separate from Go's own == or reflect.DeepEqual:
	// a model is free to ignore fields that are not part of the object's
	// own logical specification (KVModel's own doc explains why it ignores
	// Revision, for instance) without CheckLinearizability having to know
	// anything about which fields those are.
	Equal(a, b any) bool

	// StateKey returns a canonical, comparable (usable as a Go map key)
	// representation of state, for the search's own failure memoization --
	// see this package's own doc for why memoizing on the bitset of
	// linearized operations alone would be unsound, and why this is
	// therefore a distinct method from Equal rather than reusing it.
	StateKey(state any) string
}

// maxOperations is the hard limit CheckLinearizability's own uint64
// bitset imposes -- not a tuning knob, a structural fact about the
// representation chosen. A history larger than this needs a bitset type
// with more bits, or (given the search is exponential regardless) is
// probably past the point an exhaustive checker is the right tool at all;
// neither is attempted here. See DESIGN.md's own note on this checker's
// intended scale: chaos-scenario histories of a few dozen operations, not
// a production audit log.
const maxOperations = 64

// CheckLinearizability reports whether ops admits some linearization
// consistent with model, and (when it does not) one witness explaining
// why: WHICH operation, at what point in the search, had no candidate
// ordering whose model-predicted output matched what it actually recorded.
// That witness is diagnostic only -- there is no single "the" violation in
// general, since a history can fail to linearize for more than one
// independent reason -- but naming ONE concrete, checkable reason is far
// more useful for debugging a failing chaos scenario than "false" on its
// own would be.
func CheckLinearizability(ops []Operation, model Model) (Result, error) {
	n := len(ops)
	if n > maxOperations {
		return Result{}, fmt.Errorf("linearize: %d operations exceeds this checker's %d-operation limit", n, maxOperations)
	}
	for i, op := range ops {
		if op.End.Before(op.Start) {
			return Result{}, fmt.Errorf("linearize: operation %d (client %d): End %v is before Start %v",
				i, op.ClientID, op.End, op.Start)
		}
	}

	c := &checker{ops: ops, model: model, failed: make(map[string]bool)}
	ok, witness := c.search(0, model.Init())
	return Result{Linearizable: ok, Witness: witness}, nil
}

// Result is CheckLinearizability's own answer.
type Result struct {
	Linearizable bool
	// Witness explains a failure -- zero value when Linearizable is true.
	Witness Violation
}

// Violation names one concrete point where the search found no legal
// continuation: at OpIndex (an index into the ops slice originally passed
// to CheckLinearizability), with EligibleAt operations already linearized
// (also indices into that same slice), every remaining eligible candidate,
// including this one, failed to match the model's own predicted output at
// least once along the deepest path the search actually explored.
type Violation struct {
	OpIndex      int
	ClientID     int
	Linearized   []int
	ModelOutput  any
	ActualOutput any
}

type checker struct {
	ops    []Operation
	model  Model
	failed map[string]bool // memoized (bitset, stateKey) pairs already known to have no valid continuation
}

// search is the recursive core described in this package's own doc
// comment. linearized is the bitset of ops already placed; state is the
// model's own current state after applying them, in whatever order the
// search chose along the path that got here.
func (c *checker) search(linearized uint64, state any) (bool, Violation) {
	if bits.OnesCount64(linearized) == len(c.ops) {
		return true, Violation{}
	}

	key := memoKey(linearized, c.model.StateKey(state))
	if c.failed[key] {
		return false, Violation{}
	}

	var deepest Violation
	for i := range c.ops {
		bit := uint64(1) << uint(i)
		if linearized&bit != 0 {
			continue // already linearized
		}
		if !c.eligible(linearized, i) {
			continue // some remaining operation must precede this one in real time
		}

		newState, output := c.model.Apply(state, c.ops[i].Input)
		if !c.model.Equal(output, c.ops[i].Output) {
			// This ordering is real-time-legal but not model-legal: applying
			// op i here does not produce what op i actually recorded. Record
			// it as the current best witness (deepest by linearized-count,
			// so a caller sees the failure closest to a complete order rather
			// than the very first candidate tried) and try the next eligible
			// operation instead.
			if bits.OnesCount64(linearized) >= len(deepest.Linearized) {
				deepest = Violation{
					OpIndex:      i,
					ClientID:     c.ops[i].ClientID,
					Linearized:   bitsetToIndices(linearized, len(c.ops)),
					ModelOutput:  output,
					ActualOutput: c.ops[i].Output,
				}
			}
			continue
		}

		if ok, v := c.search(linearized|bit, newState); ok {
			return true, Violation{}
		} else if len(v.Linearized) >= len(deepest.Linearized) {
			deepest = v
		}
	}

	c.failed[key] = true
	return false, deepest
}

// eligible reports whether ops[i] could legally be the next operation
// linearized, given that every operation named in linearized has already
// been placed: true exactly when no OTHER not-yet-linearized operation
// necessarily happened before ops[i] in real time.
func (c *checker) eligible(linearized uint64, i int) bool {
	for j := range c.ops {
		if j == i {
			continue
		}
		bit := uint64(1) << uint(j)
		if linearized&bit != 0 {
			continue // j already linearized -- no longer a constraint on what comes "next"
		}
		if !c.ops[j].End.After(c.ops[i].Start) {
			// ops[j].End <= ops[i].Start: j happened-before i in real time,
			// and j has not been placed yet, so i cannot go next.
			return false
		}
	}
	return true
}

func memoKey(linearized uint64, stateKey string) string {
	return fmt.Sprintf("%d|%s", linearized, stateKey)
}

func bitsetToIndices(bitset uint64, n int) []int {
	var out []int
	for i := 0; i < n; i++ {
		if bitset&(uint64(1)<<uint(i)) != 0 {
			out = append(out, i)
		}
	}
	return out
}
