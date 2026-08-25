package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tempWALPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func TestAppendAndReplayRoundTrip(t *testing.T) {
	path := tempWALPath(t)

	w, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	type write struct {
		del   bool
		key   string
		value string
	}
	writes := []write{
		{key: "a", value: "1"},
		{key: "b", value: "2"},
		{del: true, key: "a"},
		{key: "c", value: ""}, // empty value must round-trip, not be confused with a delete
	}

	for _, wr := range writes {
		var err error
		if wr.del {
			_, err = w.AppendDelete([]byte(wr.key))
		} else {
			_, err = w.Append([]byte(wr.key), []byte(wr.value))
		}
		if err != nil {
			t.Fatalf("append %+v: %v", wr, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got []Entry
	validUpTo, err := Replay(path, func(e Entry) error {
		// Copy out; Replay reuses no buffers across calls but be
		// defensive against future changes.
		key := append([]byte(nil), e.Key...)
		var val []byte
		if e.Value != nil {
			val = append([]byte(nil), e.Value...)
		}
		got = append(got, Entry{Offset: e.Offset, Type: e.Type, Key: key, Value: val})
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatalf("Stat: %v", statErr)
	}
	if validUpTo != info.Size() {
		t.Fatalf("validUpTo = %d, want full file size %d", validUpTo, info.Size())
	}

	if len(got) != len(writes) {
		t.Fatalf("got %d entries, want %d", len(got), len(writes))
	}
	for i, wr := range writes {
		e := got[i]
		if wr.del {
			if e.Type != RecordDelete {
				t.Errorf("entry %d: type = %v, want RecordDelete", i, e.Type)
			}
			if string(e.Key) != wr.key {
				t.Errorf("entry %d: key = %q, want %q", i, e.Key, wr.key)
			}
			if e.Value != nil {
				t.Errorf("entry %d: value = %q, want nil", i, e.Value)
			}
			continue
		}
		if e.Type != RecordPut {
			t.Errorf("entry %d: type = %v, want RecordPut", i, e.Type)
		}
		if string(e.Key) != wr.key {
			t.Errorf("entry %d: key = %q, want %q", i, e.Key, wr.key)
		}
		if string(e.Value) != wr.value {
			t.Errorf("entry %d: value = %q, want %q", i, e.Value, wr.value)
		}
	}
}

func TestReplayEmptyFile(t *testing.T) {
	path := tempWALPath(t)
	// No file exists yet at all.
	validUpTo, err := Replay(path, func(Entry) error {
		t.Fatal("fn called on a nonexistent file")
		return nil
	})
	if err != nil {
		t.Fatalf("Replay on missing file: %v", err)
	}
	if validUpTo != 0 {
		t.Fatalf("validUpTo = %d, want 0", validUpTo)
	}
}

func TestReplayStopsAtCorruptRecord(t *testing.T) {
	path := tempWALPath(t)

	w, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	off1, err := w.Append([]byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("append 1: %v", err)
	}
	off2, err := w.Append([]byte("k2"), []byte("v2"))
	if err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if _, err := w.Append([]byte("k3"), []byte("v3")); err != nil {
		t.Fatalf("append 3: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a byte inside the second record's payload -- past its header,
	// so the corruption is in the data the CRC actually covers.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	corruptAt := off2 + int64(headerSize) + 1 // one byte into record 2's payload
	raw[corruptAt] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var got []Entry
	validUpTo, err := Replay(path, func(e Entry) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1 (only the record before the corruption)", len(got))
	}
	if got[0].Offset != off1 {
		t.Fatalf("surviving entry at offset %d, want %d", got[0].Offset, off1)
	}
	if validUpTo != off2 {
		t.Fatalf("validUpTo = %d, want %d (start of the corrupt record)", validUpTo, off2)
	}
}

func TestReplayStopsAtTornTail(t *testing.T) {
	path := tempWALPath(t)

	w, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	off1, err := w.Append([]byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("append 1: %v", err)
	}
	off2, err := w.Append([]byte("k2"), []byte("v2"))
	if err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Truncate mid-way through the second record, simulating a crash
	// between two writes.
	tornLen := off2 + int64(headerSize) + 1
	if err := os.Truncate(path, tornLen); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var got []Entry
	validUpTo, err := Replay(path, func(e Entry) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 || got[0].Offset != off1 {
		t.Fatalf("got %v, want exactly the first record at offset %d", got, off1)
	}
	if validUpTo != off2 {
		t.Fatalf("validUpTo = %d, want %d (start of the torn record)", validUpTo, off2)
	}
}

func TestOpenAppendsRatherThanTruncates(t *testing.T) {
	path := tempWALPath(t)

	w1, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	if _, err := w1.Append([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	w2, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	if _, err := w2.Append([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}

	var keys []string
	if _, err := Replay(path, func(e Entry) error {
		keys = append(keys, string(e.Key))
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(keys) != 2 || keys[0] != "k1" || keys[1] != "k2" {
		t.Fatalf("keys = %v, want [k1 k2]; Open must append, not truncate", keys)
	}
}

func TestSyncPoliciesProduceIdenticalData(t *testing.T) {
	// The three policies trade off durability and cost, never correctness:
	// whatever survives to be replayed must be byte-identical regardless
	// of policy, because policy only governs when fsync happens, never
	// what gets written or read back.
	for _, policy := range []SyncPolicy{SyncAlways, SyncNever, SyncBatch} {
		policy := policy
		t.Run(policyName(policy), func(t *testing.T) {
			path := tempWALPath(t)
			w, err := Open(path, policy)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			for i := 0; i < 50; i++ {
				if _, err := w.Append([]byte{byte(i)}, []byte{byte(i), byte(i)}); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
			}
			// SyncBatch needs an explicit Sync to guarantee the
			// buffered writer's contents are flushed before Close
			// re-flushes anyway; call it for every policy since
			// Sync must always be safe to call regardless.
			if err := w.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			var count int
			if _, err := Replay(path, func(e Entry) error {
				if len(e.Key) != 1 || e.Key[0] != byte(count) {
					t.Fatalf("entry %d: key = %v, want [%d]", count, e.Key, count)
				}
				if !bytes.Equal(e.Value, []byte{byte(count), byte(count)}) {
					t.Fatalf("entry %d: value = %v, want [%d %d]", count, e.Value, count, count)
				}
				count++
				return nil
			}); err != nil {
				t.Fatalf("Replay: %v", err)
			}
			if count != 50 {
				t.Fatalf("replayed %d entries, want 50", count)
			}
		})
	}
}

func policyName(p SyncPolicy) string {
	switch p {
	case SyncAlways:
		return "SyncAlways"
	case SyncNever:
		return "SyncNever"
	case SyncBatch:
		return "SyncBatch"
	default:
		return "unknown"
	}
}

func TestReplayFnErrorStopsAndPropagates(t *testing.T) {
	path := tempWALPath(t)
	w, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := w.Append([]byte{byte(i)}, nil); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wantErr := errStop
	var seen int
	_, err = Replay(path, func(e Entry) error {
		seen++
		if seen == 2 {
			return wantErr
		}
		return nil
	})
	if err != wantErr {
		t.Fatalf("Replay err = %v, want %v", err, wantErr)
	}
	if seen != 2 {
		t.Fatalf("fn called %d times, want 2 (Replay must stop on the caller's error)", seen)
	}
}

var errStop = errStopType{}

type errStopType struct{}

func (errStopType) Error() string { return "stop" }
