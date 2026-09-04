// Package history records the invoke and return of every client operation,
// timestamped, into a single shared, ordered log -- the raw material a
// linearizability checker (Porcupine and similar tools all expect this
// shape: one Invoke event and one Return event per operation, correlated by
// an operation id, each carrying that operation's own input or output) needs
// to later decide whether an observed history is consistent with some valid
// linearization of the operations it contains.
//
// This package does NOT check anything. It is the recording half only --
// "log every client operation's invoke and return with timestamps," and
// nothing more. Checking whether a recorded history is actually
// linearizable is real, separate, future work: this package's whole job is
// to produce a history complete and precise enough for that work to consume
// later without having to change how histories are captured.
package history

import (
	"fmt"
	"time"

	"github.com/ekushal02/helios/client"
)

// Kind identifies which client operation an Event belongs to.
//
// Watch is deliberately absent. Every other operation here is a single
// request-response pair with a clean invoke-then-return shape; Watch
// returns a channel that goes on delivering events indefinitely, which
// does not fit the invoke/return model at all -- there is no single
// instant to date a "return" from, and a subscription's own correctness
// property (every applied write delivered, in order, exactly once) is a
// different question from linearizability in the first place. Recording
// Watch subscriptions is real, separate future work if it is ever needed,
// not a gap in this one.
type Kind int

const (
	KindGet Kind = iota
	KindGetStale
	KindPut
	KindDelete
	KindScan
	KindScanStale
	KindScanAll
)

func (k Kind) String() string {
	switch k {
	case KindGet:
		return "Get"
	case KindGetStale:
		return "GetStale"
	case KindPut:
		return "Put"
	case KindDelete:
		return "Delete"
	case KindScan:
		return "Scan"
	case KindScanStale:
		return "ScanStale"
	case KindScanAll:
		return "ScanAll"
	default:
		return fmt.Sprintf("Kind(%d)", int(k))
	}
}

// Event is one invoke or one return in a recorded history -- never both.
// Two Events with the same OpID and different IsInvoke values are the two
// halves of a single operation.
//
// Input is populated on an invoke Event and nil on the matching return;
// Output is the reverse. Both hold this package's own per-operation
// structs (GetInput, PutOutput, and so on) -- concrete Go types for a
// producer's own convenience, not the more general shape a checker
// consuming a WRITTEN history sees again after ReadJSONL (see
// DecodedEvent's own doc for why those two shapes are deliberately
// different).
type Event struct {
	ClientID  int       `json:"client_id"`
	OpID      int64     `json:"op_id"`
	Kind      Kind      `json:"kind"`
	IsInvoke  bool      `json:"is_invoke"`
	Timestamp time.Time `json:"timestamp"`
	Input     any       `json:"input,omitempty"`
	Output    any       `json:"output,omitempty"`
}

// Per-operation Input/Output shapes. Byte slices are always copies, never
// aliases into caller-owned memory -- see RecordingClient's own methods for
// why that matters: a recorded history is a durable, inspectable record,
// and must not silently change underneath a reader because the original
// caller happened to reuse or mutate a buffer after the call returned.

type GetInput struct {
	Key []byte `json:"key"`
}

type GetOutput struct {
	Value    []byte `json:"value,omitempty"`
	Ok       bool   `json:"ok"`
	Revision int64  `json:"revision"`
	Err      string `json:"err,omitempty"`
}

type PutInput struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type PutOutput struct {
	Revision int64  `json:"revision"`
	Err      string `json:"err,omitempty"`
}

type DeleteInput struct {
	Key []byte `json:"key"`
}

type DeleteOutput struct {
	Revision int64  `json:"revision"`
	Err      string `json:"err,omitempty"`
}

type ScanInput struct {
	StartKey  []byte `json:"start_key,omitempty"`
	EndKey    []byte `json:"end_key,omitempty"`
	Limit     int    `json:"limit"`
	PageToken []byte `json:"page_token,omitempty"`
}

type ScanOutput struct {
	Pairs         []client.KeyValue `json:"pairs,omitempty"`
	NextPageToken []byte            `json:"next_page_token,omitempty"`
	Err           string            `json:"err,omitempty"`
}

type ScanAllInput struct {
	StartKey []byte `json:"start_key,omitempty"`
	EndKey   []byte `json:"end_key,omitempty"`
	Limit    int    `json:"limit"`
}

type ScanAllOutput struct {
	Pairs []client.KeyValue `json:"pairs,omitempty"`
	Err   string            `json:"err,omitempty"`
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
