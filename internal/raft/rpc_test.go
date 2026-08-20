package raft

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"
)

// These tests do not exercise behaviour. They pin the SHAPE of the wire format
// so that a field added, removed or retyped in a later task fails loudly here
// and forces a conscious decision plus a DESIGN.md note, rather than sliding in
// as a silent protocol change.

// fieldSpec is one expected struct field: name and type, in declaration order.
type fieldSpec struct {
	name string
	typ  string
}

func assertFields(t *testing.T, v interface{}, want []fieldSpec) {
	t.Helper()

	rt := reflect.TypeOf(v)
	if rt.NumField() != len(want) {
		t.Fatalf("%s has %d fields, Figure 2 defines %d: %v",
			rt.Name(), rt.NumField(), len(want), fieldNames(rt))
	}
	for i, w := range want {
		f := rt.Field(i)
		if f.Name != w.name {
			t.Errorf("%s field %d is %q, want %q", rt.Name(), i, f.Name, w.name)
		}
		if got := f.Type.String(); got != w.typ {
			t.Errorf("%s.%s is %s, want %s", rt.Name(), f.Name, got, w.typ)
		}
		// Unexported fields are invisible to gob and would be silently dropped
		// in transit -- a bug that only shows up as a peer disagreeing about
		// state it never received.
		if f.PkgPath != "" {
			t.Errorf("%s.%s is unexported and will not survive encoding",
				rt.Name(), f.Name)
		}
	}
}

func fieldNames(rt reflect.Type) []string {
	names := make([]string, rt.NumField())
	for i := range names {
		names[i] = rt.Field(i).Name
	}
	return names
}

// THIS TEST DID ITS JOB. It failed when NoOp was added, which forced the
// decision to be written down rather than slid in: see LogEntry's own comment
// for why an explicit flag beat reusing a nil Command, and DESIGN.md §8 for the
// consequences on the wire.
//
// NoOp is a documented departure from Figure 2, listed after the two fields the
// paper defines, in the same way AppendEntriesReply carries the §5.3 fast-backup
// hint after its Figure 2 fields.
func TestLogEntryMatchesFigure2(t *testing.T) {
	assertFields(t, LogEntry{}, []fieldSpec{
		{"Term", "int"},
		{"Command", "[]uint8"},
		{"NoOp", "bool"},
	})

	// The absence of an Index field is a design decision, not an oversight:
	// position in n.log IS the index. If this ever fails because someone added
	// Index, the question to answer first is which of the two is authoritative
	// after a truncation.
	if _, ok := reflect.TypeOf(LogEntry{}).FieldByName("Index"); ok {
		t.Error("LogEntry has an Index field: position in the log is the index")
	}
}

func TestAppendEntriesArgsMatchesFigure2(t *testing.T) {
	assertFields(t, AppendEntriesArgs{}, []fieldSpec{
		{"Term", "int"},
		{"LeaderID", "int"},
		{"PrevLogIndex", "int"},
		{"PrevLogTerm", "int"},
		{"Entries", "[]raft.LogEntry"},
		{"LeaderCommit", "int"},
	})
}

func TestAppendEntriesReplyMatchesFigure2(t *testing.T) {
	// Figure 2 fields FIRST and unchanged, then the documented §5.3 extension.
	// Ordering matters for readability, not for gob, which is name-keyed.
	assertFields(t, AppendEntriesReply{}, []fieldSpec{
		{"Term", "int"},
		{"Success", "bool"},
		{"ConflictIndex", "int"},
		{"ConflictTerm", "int"},
	})
}

func TestRequestVoteMatchesFigure2(t *testing.T) {
	assertFields(t, RequestVoteArgs{}, []fieldSpec{
		{"Term", "int"},
		{"CandidateID", "int"},
		{"LastLogIndex", "int"},
		{"LastLogTerm", "int"},
	})
	assertFields(t, RequestVoteReply{}, []fieldSpec{
		{"Term", "int"},
		{"VoteGranted", "bool"},
	})
}

// Entries must survive encoding with its contents intact and its backing array
// separate. If the decode shared memory with the original, a follower
// truncating its log in C-5 could reach back and corrupt the leader's.
func TestAppendEntriesArgsRoundTrips(t *testing.T) {
	orig := AppendEntriesArgs{
		Term:         7,
		LeaderID:     2,
		PrevLogIndex: 3,
		PrevLogTerm:  5,
		Entries: []LogEntry{
			{Term: 5, Command: []byte("set x 1")},
			{Term: 7, Command: []byte("set y 2")},
		},
		LeaderCommit: 3,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&orig); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got AppendEntriesArgs
	if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !reflect.DeepEqual(orig, got) {
		t.Fatalf("round trip changed the message:\n got %+v\nwant %+v", got, orig)
	}

	// Mutating the decoded copy must not touch the original.
	got.Entries[0].Command[0] = 'X'
	if orig.Entries[0].Command[0] == 'X' {
		t.Error("decoded Entries shares backing memory with the original")
	}
}

// A barrier must survive the wire, and a normal entry must not grow because
// barriers exist.
//
// gob omits zero values, so NoOp false encodes to nothing at all: adding the
// field cost every existing entry exactly zero bytes. That is the whole reason
// an explicit flag was affordable where a nil-Command convention would have
// been free but ambiguous.
func TestNoOpEntriesRoundTrip(t *testing.T) {
	orig := AppendEntriesArgs{
		Term:     9,
		LeaderID: 1,
		Entries: []LogEntry{
			{Term: 9, Command: []byte("set x 1")},
			{Term: 9, NoOp: true},
		},
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&orig); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got AppendEntriesArgs
	if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Entries) != 2 {
		t.Fatalf("decoded %d entries, want 2", len(got.Entries))
	}
	if got.Entries[0].NoOp {
		t.Error("a client command decoded as a barrier")
	}
	if !got.Entries[1].NoOp {
		t.Error("a barrier decoded as a client command: the follower would " +
			"hand it to the state machine, which cannot parse it")
	}
	if got.Entries[1].Command != nil {
		t.Errorf("barrier carries a command %q, want none", got.Entries[1].Command)
	}
}

// THE GOB TRAP WORTH KNOWING ABOUT.
//
// gob omits zero values, so an all-zero message encodes to almost nothing and
// decoding it leaves the destination UNCHANGED rather than zeroing it. Any code
// that reuses a reply struct across calls will read stale values from the
// previous call and think they came off the wire.
//
// The rule this pins: always decode into a FRESH struct, never a reused one.
func TestZeroReplyDoesNotClearDestination(t *testing.T) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&AppendEntriesReply{}); err != nil {
		t.Fatalf("encode: %v", err)
	}

	dirty := AppendEntriesReply{Term: 50, Success: true}
	if err := gob.NewDecoder(&buf).Decode(&dirty); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if dirty.Term != 50 {
		t.Skip("gob now zeroes the destination; the reuse hazard is gone")
	}
	t.Logf("confirmed: decoding a zero reply left Term=%d in place. "+
		"Reply structs must be freshly declared per call.", dirty.Term)
}

// A heartbeat is an AppendEntries with no entries. nil and empty are the same
// thing after a gob round trip, so no code may distinguish them.
func TestHeartbeatEntriesNormaliseToNil(t *testing.T) {
	for _, entries := range [][]LogEntry{nil, {}} {
		var buf bytes.Buffer
		args := AppendEntriesArgs{Term: 1, LeaderID: 0, Entries: entries}
		if err := gob.NewEncoder(&buf).Encode(&args); err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got AppendEntriesArgs
		if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(got.Entries) != 0 {
			t.Errorf("Entries = %v, want empty", got.Entries)
		}
	}
}

// The sentinel convention made explicit. PrevLogIndex 0 with PrevLogTerm 0 must
// describe a log holding nothing but the sentinel -- that is what a leader
// sends to a follower it believes is empty.
//
// Phase D breaks this: after a snapshot, index 0 is no longer present and the
// baseline moves to lastIncludedIndex. When that lands, this test is the marker
// for every place the assumption is baked in.
func TestFreshLogBaselineIsIndexZero(t *testing.T) {
	n := NewNode(0, []int{1, 2}, nil, 1)
	t.Cleanup(n.Stop)

	n.mu.Lock()
	defer n.mu.Unlock()

	if got := n.lastLogIndex(); got != 0 {
		t.Errorf("lastLogIndex on a fresh node = %d, want 0 (sentinel only)", got)
	}
	if got := n.lastLogTerm(); got != 0 {
		t.Errorf("lastLogTerm on a fresh node = %d, want 0", got)
	}
	if len(n.log) != 1 {
		t.Fatalf("fresh log holds %d entries, want 1 sentinel", len(n.log))
	}
	if n.log[0].Command != nil {
		t.Error("the sentinel carries a command: it must never be applied")
	}
}