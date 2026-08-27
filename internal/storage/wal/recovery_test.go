package wal

import (
	"os"
	"testing"
)

// TestStartupRecoveryStopsCleanlyAtCorruptTail simulates a node's boot
// sequence against a WAL file whose tail was deliberately corrupted --
// standing in for a torn write or a bad sector left behind by whatever
// process wrote it last -- and proves three things a real startup depends
// on:
//
//  1. Recover does not error, panic, or hang on the corrupt bytes. It
//     returns exactly the well-formed prefix and stops there.
//  2. The recovered records are byte-for-byte the ones written before the
//     corruption, in order, and nothing from the corrupt record leaks
//     through partially decoded.
//  3. The corrupt tail does not come back to haunt a later restart: once
//     Recover has run, new records appended after it are not shadowed by
//     the old corruption on the *next* restart. This is the failure mode
//     Recover's truncation step exists to close (see wal.go); a version of
//     Recover that replayed without truncating would fail exactly this
//     assertion while still passing (1) and (2).
func TestStartupRecoveryStopsCleanlyAtCorruptTail(t *testing.T) {
	path := tempWALPath(t)

	// --- "process 1": write five good records, then a sixth that will
	// end up corrupted on disk. ---
	good := []struct{ key, value string }{
		{"k0", "v0"},
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
	}

	w, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, kv := range good {
		if _, err := w.Append([]byte(kv.key), []byte(kv.value)); err != nil {
			t.Fatalf("append %q: %v", kv.key, err)
		}
	}
	corruptOffset, err := w.Append([]byte("k5"), []byte("v5-doomed"))
	if err != nil {
		t.Fatalf("append doomed record: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Deliberately corrupt the sixth record: flip a byte inside its
	// payload, past its header, so the bytes the CRC actually covers are
	// what's wrong -- the shape of a bad sector or a torn write that
	// happened to leave a checksum-detectable mess rather than a clean
	// short read.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	flipAt := corruptOffset + int64(headerSize) + 1
	if flipAt >= int64(len(raw)) {
		t.Fatalf("test setup bug: flip offset %d out of range (file is %d bytes)", flipAt, len(raw))
	}
	raw[flipAt] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	corruptedFileSize := int64(len(raw))

	// --- "startup": the boot sequence a node actually runs. ---
	var recovered []Entry
	w2, err := Recover(path, SyncAlways, func(e Entry) error {
		recovered = append(recovered, Entry{
			Offset: e.Offset,
			Type:   e.Type,
			Key:    append([]byte(nil), e.Key...),
			Value:  append([]byte(nil), e.Value...),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Recover: %v (startup must not fail on a corrupt tail)", err)
	}
	defer w2.Close()

	// (1) & (2): exactly the good records, in order, nothing corrupt
	// leaking through.
	if len(recovered) != len(good) {
		t.Fatalf("recovered %d records, want %d -- replay did not stop cleanly at the corruption",
			len(recovered), len(good))
	}
	for i, kv := range good {
		if string(recovered[i].Key) != kv.key || string(recovered[i].Value) != kv.value {
			t.Errorf("record %d = (%q, %q), want (%q, %q)",
				i, recovered[i].Key, recovered[i].Value, kv.key, kv.value)
		}
	}

	// The corrupt record and anything at or after it must be gone from
	// disk, not merely skipped in memory -- Recover truncates to the
	// valid prefix so a resumed writer does not append behind a landmine.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat after recovery: %v", err)
	}
	if info.Size() != corruptOffset {
		t.Fatalf("file size after recovery = %d, want %d (truncated to the last good record); "+
			"corrupted size was %d", info.Size(), corruptOffset, corruptedFileSize)
	}

	// --- resume writing, as the now-recovered node would. ---
	if _, err := w2.Append([]byte("k5"), []byte("v5-real")); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close after recovery: %v", err)
	}

	// (3): a SECOND restart must see all six records, not just the five
	// from before recovery. If Recover had left the corrupt bytes in
	// place instead of truncating them, this replay would stop at the
	// same old corruption and never reach "k5"/"v5-real" -- silently
	// losing every record written since the first recovery, forever.
	var secondRecovered []Entry
	w3, err := Recover(path, SyncAlways, func(e Entry) error {
		secondRecovered = append(secondRecovered, Entry{
			Key:   append([]byte(nil), e.Key...),
			Value: append([]byte(nil), e.Value...),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	defer w3.Close()

	wantSecond := append(append([]struct{ key, value string }{}, good...),
		struct{ key, value string }{"k5", "v5-real"})
	if len(secondRecovered) != len(wantSecond) {
		t.Fatalf("second restart recovered %d records, want %d -- "+
			"the old corruption is still shadowing records written after recovery",
			len(secondRecovered), len(wantSecond))
	}
	for i, kv := range wantSecond {
		if string(secondRecovered[i].Key) != kv.key || string(secondRecovered[i].Value) != kv.value {
			t.Errorf("second restart record %d = (%q, %q), want (%q, %q)",
				i, secondRecovered[i].Key, secondRecovered[i].Value, kv.key, kv.value)
		}
	}
}

// TestStartupRecoveryStopsCleanlyAtTornTail is the crash-shaped sibling of
// the corruption test above: the file simply ends mid-record, as it would
// if the process died between two writes, rather than containing a
// record's worth of bytes that fail their checksum. Recover must handle
// both the same way -- cleanly, with no error -- since a node cannot tell
// which one it is looking at without trying, and both mean the same thing
// operationally: "the last thing on disk is not trustworthy."
func TestStartupRecoveryStopsCleanlyAtTornTail(t *testing.T) {
	path := tempWALPath(t)

	w, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = w.Append([]byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("append 1: %v", err)
	}
	off2, err := w.Append([]byte("b"), []byte("2"))
	if err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Truncate partway through the second record -- a torn write, not a
	// bit flip.
	tornLen := off2 + int64(headerSize) + 1
	if err := os.Truncate(path, tornLen); err != nil {
		t.Fatalf("Truncate (simulating a crash): %v", err)
	}

	var recovered []Entry
	w2, err := Recover(path, SyncAlways, func(e Entry) error {
		recovered = append(recovered, e)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	defer w2.Close()

	if len(recovered) != 1 || string(recovered[0].Key) != "a" {
		t.Fatalf("recovered %v, want exactly the record before the torn tail", recovered)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != off2 {
		t.Fatalf("file size after recovery = %d, want %d (truncated to the last good record)",
			info.Size(), off2)
	}

	if _, err := w2.Append([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var final []Entry
	w3, err := Recover(path, SyncAlways, func(e Entry) error {
		final = append(final, e)
		return nil
	})
	if err != nil {
		t.Fatalf("final Recover: %v", err)
	}
	defer w3.Close()
	if len(final) != 2 {
		t.Fatalf("final replay found %d records, want 2 (%q then %q)", len(final), "a", "c")
	}
	if string(final[0].Key) != "a" || string(final[1].Key) != "c" {
		t.Fatalf("final replay = %v, want [a c]", final)
	}
}

// TestRecoverOnMissingFileStartsFresh confirms Recover is a valid first
// call on a path nothing has ever written to -- the common case of a
// brand-new node -- and does not require a special first-boot path.
func TestRecoverOnMissingFileStartsFresh(t *testing.T) {
	path := tempWALPath(t)
	calls := 0
	w, err := Recover(path, SyncAlways, func(Entry) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Recover on missing file: %v", err)
	}
	defer w.Close()
	if calls != 0 {
		t.Fatalf("fn called %d times on a fresh file, want 0", calls)
	}
	if _, err := w.Append([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("append to freshly recovered WAL: %v", err)
	}
}