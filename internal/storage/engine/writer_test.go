package engine

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ekushal02/helios/internal/storage/memtable"
	"github.com/ekushal02/helios/internal/storage/wal"
)

func openWriter(t *testing.T, path string) (*Writer, *memtable.Memtable) {
	t.Helper()
	m := memtable.NewWithSeed(1)
	w, err := RecoverMemtable(path, wal.SyncAlways, m)
	if err != nil {
		t.Fatalf("RecoverMemtable: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return NewWriter(w, m), m
}

func TestPutIsVisibleImmediately(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	writer, m := openWriter(t, path)

	if err := writer.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, tombstone, ok := m.Get([]byte("k"))
	if !ok || tombstone || string(value) != "v" {
		t.Fatalf("Get(k) after Put = (%q, tombstone=%v, ok=%v), want (\"v\", false, true)", value, tombstone, ok)
	}
}

// TestDeleteIsVisibleImmediately is Put's test, run against Delete, to
// make the symmetry claim concrete rather than only argued in comments:
// the same shape of test, with the same shape of result, modulo which
// operation ran.
func TestDeleteIsVisibleImmediately(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	writer, m := openWriter(t, path)

	if err := writer.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := writer.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	value, tombstone, ok := m.Get([]byte("k"))
	if !ok || !tombstone || value != nil {
		t.Fatalf("Get(k) after Delete = (%q, tombstone=%v, ok=%v), want (nil, true, true)", value, tombstone, ok)
	}
}

// TestDeleteOfAKeyNeverPutStillWritesATombstone is the test that most
// directly exercises the type doc's argument: Delete must not treat "the
// memtable doesn't currently hold this key" as a reason to skip the
// write, because a Writer never knows whether an older, invisible-to-it
// tier holds a live copy underneath.
func TestDeleteOfAKeyNeverPutStillWritesATombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	writer, m := openWriter(t, path)

	if err := writer.Delete([]byte("never-put")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	value, tombstone, ok := m.Get([]byte("never-put"))
	if !ok || !tombstone || value != nil {
		t.Fatalf("Get(never-put) after Delete = (%q, tombstone=%v, ok=%v), want (nil, true, true) -- "+
			"a tombstone must be written even for a key this memtable never saw a Put for", value, tombstone, ok)
	}
}

// TestDeleteSurvivesACrashBetweenAppendAndApply is the test that makes
// "a delete is a write" a checked fact rather than an assertion: it
// simulates the exact crash window the type doc calls out -- durable in
// the WAL, but the in-memory memtable that would have applied it is
// simply thrown away, standing in for a process that died right there --
// and confirms RecoverMemtable rebuilds the tombstone from the WAL alone,
// with no help from the memtable that was "supposed to" have it. If
// Delete's AppendDelete call were ever skipped or reordered after the
// memtable update, this is exactly the test that would catch it, the
// same way TestStartupRecoveryStopsCleanlyAtTornTail (§13.1) catches a
// broken truncation by proving a second, independent recovery pass still
// sees what the first one wrote.
func TestDeleteSurvivesACrashBetweenAppendAndApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	writer, _ := openWriter(t, path)
	if err := writer.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := writer.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Simulate a crash: the WAL has both records durably; the memtable
	// that applied them is discarded without ever being flushed anywhere,
	// standing in for a process that died with unflushed memory state.

	fresh := memtable.NewWithSeed(2)
	w2, err := RecoverMemtable(path, wal.SyncAlways, fresh)
	if err != nil {
		t.Fatalf("RecoverMemtable after simulated crash: %v", err)
	}
	defer w2.Close()

	value, tombstone, ok := fresh.Get([]byte("k"))
	if !ok || !tombstone || value != nil {
		t.Fatalf("Get(k) on a memtable rebuilt purely from the WAL = (%q, tombstone=%v, ok=%v), want (nil, true, true) -- "+
			"the delete's durability must come entirely from the WAL record, not from the original memtable", value, tombstone, ok)
	}
}

// TestPutAndDeleteBothFailIdenticallyWhenTheWALIsUnwritable is the other
// half of the symmetry claim: whatever happens to Put when the WAL append
// fails must happen to Delete too, not a special case for one or the
// other.
func TestPutAndDeleteBothFailIdenticallyWhenTheWALIsUnwritable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	writer, m := openWriter(t, path)

	// Close the WAL out from under the Writer to force every subsequent
	// Append/AppendDelete to fail -- the WAL package's own contract is
	// that a closed WAL refuses further writes, which recovery_test.go
	// already relies on elsewhere.
	if err := writer.wal.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	if err := writer.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("Put against a closed WAL: err = nil, want an error")
	}
	if _, _, ok := m.Get([]byte("k")); ok {
		t.Fatal("Put against a closed WAL still reached the memtable -- the memtable must not be touched when the WAL append fails")
	}

	if err := writer.Delete([]byte("k")); err == nil {
		t.Fatal("Delete against a closed WAL: err = nil, want an error")
	}
	if _, tombstone, ok := m.Get([]byte("k")); ok && tombstone {
		t.Fatal("Delete against a closed WAL still reached the memtable -- must fail exactly as Put does, not partially succeed")
	}
}

// TestPutAndDeleteFailuresWrapTheUnderlyingWALError checks that Put and
// Delete's errors are not generic replacements for the WAL's own failure
// -- the underlying cause ("file already closed," here) has to still be
// visible in the returned error, and the message has to identify which
// operation failed.
func TestPutAndDeleteFailuresWrapTheUnderlyingWALError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	writer, _ := openWriter(t, path)
	if err := writer.wal.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	putErr := writer.Put([]byte("k"), []byte("v"))
	if putErr == nil {
		t.Fatal("Put against a closed WAL: err = nil, want an error")
	}
	if !strings.Contains(putErr.Error(), "closed") {
		t.Errorf("Put's error %q should still show the underlying cause (a closed WAL)", putErr)
	}
	if !strings.Contains(putErr.Error(), "put") {
		t.Errorf("Put's error %q should identify which operation failed", putErr)
	}

	deleteErr := writer.Delete([]byte("k"))
	if deleteErr == nil {
		t.Fatal("Delete against a closed WAL: err = nil, want an error")
	}
	if !strings.Contains(deleteErr.Error(), "closed") {
		t.Errorf("Delete's error %q should still show the underlying cause (a closed WAL)", deleteErr)
	}
	if !strings.Contains(deleteErr.Error(), "delete") {
		t.Errorf("Delete's error %q should identify which operation failed", deleteErr)
	}
}

// TestReadPathSeesADeleteThroughTheFullStack ties Writer (this file) to
// Reader (§13.6) the way a real caller would use both: write a value,
// confirm it is visible, delete it, and confirm Reader -- not just a
// direct Memtable.Get -- reports it as gone. This is the end-to-end
// version of the read-path tombstone tests in reader_test.go, run
// against data that arrived through the actual write path instead of a
// hand-built fake.
func TestReadPathSeesADeleteThroughTheFullStack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	writer, m := openWriter(t, path)
	reader := NewReader(m, nil, nil)

	if err := writer.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if value, ok, err := reader.Get([]byte("k")); err != nil || !ok || string(value) != "v" {
		t.Fatalf("Reader.Get(k) after Put = (%q, ok=%v, err=%v), want (\"v\", true, nil)", value, ok, err)
	}

	if err := writer.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if value, ok, err := reader.Get([]byte("k")); err != nil || ok || value != nil {
		t.Fatalf("Reader.Get(k) after Delete = (%q, ok=%v, err=%v), want (nil, false, nil)", value, ok, err)
	}
}
