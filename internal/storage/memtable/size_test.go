package memtable

import "testing"

func TestApproxSizeStartsAtZero(t *testing.T) {
	m := NewWithSeed(1)
	if got := m.ApproxSize(); got != 0 {
		t.Fatalf("ApproxSize on an empty Memtable = %d, want 0", got)
	}
}

func TestApproxSizeCountsKeyAndValueBytes(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("abc"), []byte("defgh")) // 3 + 5
	if got, want := m.ApproxSize(), int64(8); got != want {
		t.Fatalf("ApproxSize after one Put = %d, want %d", got, want)
	}
	m.Put([]byte("xy"), []byte("z")) // +2 +1
	if got, want := m.ApproxSize(), int64(11); got != want {
		t.Fatalf("ApproxSize after two Puts = %d, want %d", got, want)
	}
}

func TestApproxSizeOnOverwriteChargesOnlyTheDelta(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("k"), []byte("short")) // 1 + 5 = 6
	m.Put([]byte("k"), []byte("a much longer value"))
	// Same key: the key's byte cost is charged once, not twice.
	want := int64(1 + len("a much longer value"))
	if got := m.ApproxSize(); got != want {
		t.Fatalf("ApproxSize after overwriting with a longer value = %d, want %d", got, want)
	}

	m.Put([]byte("k"), []byte("x"))
	want = int64(1 + 1)
	if got := m.ApproxSize(); got != want {
		t.Fatalf("ApproxSize after overwriting with a shorter value = %d, want %d", got, want)
	}
}

func TestApproxSizeOnDeleteOfANeverWrittenKeyChargesTheKeyOnly(t *testing.T) {
	m := NewWithSeed(1)
	m.Delete([]byte("gone"))
	if got, want := m.ApproxSize(), int64(len("gone")); got != want {
		t.Fatalf("ApproxSize after Delete of a new key = %d, want %d (key bytes only, no value)", got, want)
	}
}

func TestApproxSizeOnDeleteOfAnExistingKeyDropsTheValueBytes(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("k"), []byte("value-bytes"))
	m.Delete([]byte("k"))
	if got, want := m.ApproxSize(), int64(1); got != want {
		t.Fatalf("ApproxSize after deleting an existing key = %d, want %d (key bytes only)", got, want)
	}
}

func TestApproxSizeAfterDeleteThenPutRestoresValueBytes(t *testing.T) {
	m := NewWithSeed(1)
	m.Put([]byte("k"), []byte("value-bytes")) // 1 + 11
	m.Delete([]byte("k"))                     // 1
	m.Put([]byte("k"), []byte("new"))         // 1 + 3
	if got, want := m.ApproxSize(), int64(1+3); got != want {
		t.Fatalf("ApproxSize after delete-then-put = %d, want %d", got, want)
	}
}
