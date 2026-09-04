package history

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// MarshalJSON renders a Kind as its own name ("Get", "Put", ...) rather
// than the bare integer backing it -- a recorded history is meant to be
// read by a human debugging a chaos scenario as often as it is meant to be
// fed to a checker, and "kind":"Put" needs no cross-reference back to this
// file the way "kind":2 would.
func (k Kind) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

// UnmarshalJSON reverses MarshalJSON. An unrecognized name is an error,
// not silently mapped to some default Kind -- a history a checker
// misreads is worse than one it refuses to read at all.
func (k *Kind) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("history: Kind: %w", err)
	}
	switch s {
	case "Get":
		*k = KindGet
	case "GetStale":
		*k = KindGetStale
	case "Put":
		*k = KindPut
	case "Delete":
		*k = KindDelete
	case "Scan":
		*k = KindScan
	case "ScanStale":
		*k = KindScanStale
	case "ScanAll":
		*k = KindScanAll
	default:
		return fmt.Errorf("history: unknown Kind %q", s)
	}
	return nil
}

// WriteJSONL writes events to w, one JSON object per line, in the order
// given -- the standard JSON Lines shape: streamable (a consumer can start
// processing before the writer finishes), appendable (a new event is one
// more line, not a rewrite of the whole file), and grep/jq-able directly
// without a JSON-aware tool. events is written in the order passed in, not
// re-sorted by OpID or timestamp -- Recorder.Events' own doc explains why
// that order (invoke/return interleaving, not OpID order) is itself
// meaningful and should survive a round trip through this function
// unchanged.
func WriteJSONL(w io.Writer, events []Event) error {
	enc := json.NewEncoder(w)
	for i, e := range events {
		if err := enc.Encode(e); err != nil {
			return fmt.Errorf("history: write event %d (op %d): %w", i, e.OpID, err)
		}
	}
	return nil
}

// DecodedEvent is what ReadJSONL returns. Input and Output are kept as raw,
// undecoded JSON rather than this package's own per-operation structs
// (GetInput, PutOutput, and so on) -- deliberately a different shape from
// Event's own, producer-side one. A reader of a previously-written history
// already has Kind to tell it which shape to expect, and can decode Input
// or Output into exactly that shape itself (DecodeInput/DecodeOutput below
// do this for every Kind this package defines) -- keeping this package from
// having to be the one place responsible for reconstructing a producer's
// own concrete Go types on every read, which a future checker consuming
// these histories may not even want: Porcupine's own model, for instance,
// takes Input/Output as bare `any` and never needs this package's own
// struct definitions at all.
type DecodedEvent struct {
	ClientID  int
	OpID      int64
	Kind      Kind
	IsInvoke  bool
	Timestamp time.Time
	Input     json.RawMessage
	Output    json.RawMessage
}

type wireEvent struct {
	ClientID  int             `json:"client_id"`
	OpID      int64           `json:"op_id"`
	Kind      Kind            `json:"kind"`
	IsInvoke  bool            `json:"is_invoke"`
	Timestamp time.Time       `json:"timestamp"`
	Input     json.RawMessage `json:"input,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
}

// ReadJSONL reads a history previously written by WriteJSONL, one event per
// line, in file order. A blank line is skipped (WriteJSONL never produces
// one, but a history someone hand-edited or concatenated from two files
// might have one at the seam); any other malformed line is a hard error
// naming its own line number, not a silently-skipped record -- a checker
// working from a history with a hole in it and no indication one exists is
// worse off than one that gets told outright.
func ReadJSONL(r io.Reader) ([]DecodedEvent, error) {
	var out []DecodedEvent
	sc := bufio.NewScanner(r)
	// Default bufio.Scanner line limit (64KiB) is comfortably exceeded by a
	// single Scan return carrying a full page of KeyValue pairs; sized up
	// here rather than leaving a real history to fail with "token too long"
	// on exactly the operations most worth recording.
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	lineNum := 0
	for sc.Scan() {
		lineNum++
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var we wireEvent
		if err := json.Unmarshal(line, &we); err != nil {
			return nil, fmt.Errorf("history: line %d: %w", lineNum, err)
		}
		out = append(out, DecodedEvent{
			ClientID:  we.ClientID,
			OpID:      we.OpID,
			Kind:      we.Kind,
			IsInvoke:  we.IsInvoke,
			Timestamp: we.Timestamp,
			Input:     we.Input,
			Output:    we.Output,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("history: %w", err)
	}
	return out, nil
}

// DecodeInput unmarshals e.Input into the concrete Go type e.Kind's own
// producer-side Event.Input would have held (GetInput for KindGet and
// KindGetStale, PutInput for KindPut, and so on) -- the read-side
// counterpart to the specific struct a RecordingClient method populated
// when this event was first recorded. Returns an error for KindGet/
// KindGetStale mismatches or any Kind this package does not define; e.Input
// being empty (a return event, which never carries one) also errors, since
// there is nothing meaningful to decode.
func (e DecodedEvent) DecodeInput(v any) error {
	if len(e.Input) == 0 {
		return fmt.Errorf("history: event (op %d, %s) has no Input to decode", e.OpID, e.Kind)
	}
	return json.Unmarshal(e.Input, v)
}

// DecodeOutput is DecodeInput's own counterpart for e.Output.
func (e DecodedEvent) DecodeOutput(v any) error {
	if len(e.Output) == 0 {
		return fmt.Errorf("history: event (op %d, %s) has no Output to decode", e.OpID, e.Kind)
	}
	return json.Unmarshal(e.Output, v)
}
