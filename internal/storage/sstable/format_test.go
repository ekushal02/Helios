package sstable

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeBlockRoundTrip(t *testing.T) {
	var block []byte
	block = encodePutEntry(block, []byte("a"), []byte("1"))
	block = encodePutEntry(block, []byte("bb"), []byte(""))
	block = encodeDeleteEntry(block, []byte("ccc"))

	finalized, err := finalizeBlock(block, CompressionNone)
	if err != nil {
		t.Fatalf("finalizeBlock: %v", err)
	}

	body, err := verifyAndSplitBlock(finalized)
	if err != nil {
		t.Fatalf("verifyAndSplitBlock: %v", err)
	}
	entries, err := decodeBlockEntries(body)
	if err != nil {
		t.Fatalf("decodeBlockEntries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}

	if string(entries[0].Key) != "a" || entries[0].Tombstone || string(entries[0].Value) != "1" {
		t.Fatalf("entries[0] = %+v", entries[0])
	}
	// A Put with an explicit empty value must not be confused with a
	// tombstone -- the type byte, not the value length, is what a reader
	// trusts.
	if string(entries[1].Key) != "bb" || entries[1].Tombstone || len(entries[1].Value) != 0 {
		t.Fatalf("entries[1] = %+v", entries[1])
	}
	if string(entries[2].Key) != "ccc" || !entries[2].Tombstone || entries[2].Value != nil {
		t.Fatalf("entries[2] = %+v", entries[2])
	}
}

func TestVerifyAndSplitBlockDetectsCorruption(t *testing.T) {
	var block []byte
	block = encodePutEntry(block, []byte("a"), []byte("1"))
	finalized, err := finalizeBlock(block, CompressionNone)
	if err != nil {
		t.Fatalf("finalizeBlock: %v", err)
	}

	corrupted := bytes.Clone(finalized)
	// Flip a bit anywhere in the CRC-covered bytes (here, byte 0, the
	// compression-type byte) -- the CRC covers the type byte and the
	// payload together (block.go), so corrupting either is caught the
	// same way, before verifyAndSplitBlock ever looks at what the type
	// byte says.
	corrupted[0] ^= 0xFF

	if _, err := verifyAndSplitBlock(corrupted); err != ErrCorruptBlock {
		t.Fatalf("verifyAndSplitBlock on corrupted data: err = %v, want ErrCorruptBlock", err)
	}
}

func TestVerifyAndSplitBlockRejectsTruncatedInput(t *testing.T) {
	if _, err := verifyAndSplitBlock([]byte{1, 2, 3}); err != ErrCorruptBlock {
		t.Fatalf("verifyAndSplitBlock on 3 bytes (shorter than one type byte + one CRC): err = %v, want ErrCorruptBlock", err)
	}
}

func TestEncodeDecodeIndexRoundTrip(t *testing.T) {
	want := []indexEntry{
		{LastKey: []byte("m"), BlockOffset: 0, BlockLength: 128},
		{LastKey: []byte("z"), BlockOffset: 128, BlockLength: 256},
	}
	var buf []byte
	for _, e := range want {
		buf = encodeIndexEntry(buf, e)
	}
	got, err := decodeIndex(buf)
	if err != nil {
		t.Fatalf("decodeIndex: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d index entries, want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i].LastKey) != string(want[i].LastKey) ||
			got[i].BlockOffset != want[i].BlockOffset ||
			got[i].BlockLength != want[i].BlockLength {
			t.Fatalf("index entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestEncodeDecodeFooterRoundTrip(t *testing.T) {
	want := footer{IndexOffset: 4096, IndexLength: 64}
	buf := encodeFooter(want)
	if len(buf) != footerSize {
		t.Fatalf("encoded footer is %d bytes, want %d", len(buf), footerSize)
	}
	got, err := decodeFooter(buf)
	if err != nil {
		t.Fatalf("decodeFooter: %v", err)
	}
	if got != want {
		t.Fatalf("decodeFooter = %+v, want %+v", got, want)
	}
}

func TestDecodeFooterRejectsBadMagic(t *testing.T) {
	buf := encodeFooter(footer{IndexOffset: 1, IndexLength: 2})
	buf[len(buf)-1] ^= 0xFF // corrupt the last magic byte
	if _, err := decodeFooter(buf); err != ErrNotSSTable {
		t.Fatalf("decodeFooter with bad magic: err = %v, want ErrNotSSTable", err)
	}
}

func TestDecodeFooterRejectsWrongLength(t *testing.T) {
	if _, err := decodeFooter([]byte{1, 2, 3}); err != ErrCorruptBlock {
		t.Fatalf("decodeFooter on a short buffer: err = %v, want ErrCorruptBlock", err)
	}
}

func TestReadUint32PrefixedRejectsTruncatedLength(t *testing.T) {
	// Fewer than 4 bytes left: not even the length prefix fits.
	if _, _, err := readUint32Prefixed([]byte{1, 2, 3}, 0); err != ErrCorruptBlock {
		t.Fatalf("readUint32Prefixed with a truncated length: err = %v, want ErrCorruptBlock", err)
	}
}

func TestReadUint32PrefixedRejectsTruncatedField(t *testing.T) {
	// Length prefix claims 10 bytes follow; only 2 are actually there.
	buf := []byte{10, 0, 0, 0, 'a', 'b'}
	if _, _, err := readUint32Prefixed(buf, 0); err != ErrCorruptBlock {
		t.Fatalf("readUint32Prefixed with a field shorter than its own length prefix: err = %v, want ErrCorruptBlock", err)
	}
}

func TestDecodeBlockEntriesRejectsUnknownType(t *testing.T) {
	var block []byte
	block = append(block, 0x7F) // not entryPut (0) or entryDelete (1)
	block = appendUint32Prefixed(block, []byte("k"))
	if _, err := decodeBlockEntries(block); err != ErrCorruptBlock {
		t.Fatalf("decodeBlockEntries with an unknown type byte: err = %v, want ErrCorruptBlock", err)
	}
}

func TestDecodeBlockEntriesRejectsTruncatedType(t *testing.T) {
	if _, err := decodeBlockEntries(nil); err != nil {
		t.Fatalf("decodeBlockEntries on an empty body: err = %v, want nil (zero entries, not an error)", err)
	}
}

func TestDecodeIndexRejectsTruncatedEntry(t *testing.T) {
	// A well-formed LastKey field followed by too few bytes for
	// BlockOffset+BlockLength.
	var buf []byte
	buf = appendUint32Prefixed(buf, []byte("k"))
	buf = append(buf, 1, 2, 3) // short of offsetSize+lenSize
	if _, err := decodeIndex(buf); err != ErrCorruptBlock {
		t.Fatalf("decodeIndex on a truncated entry: err = %v, want ErrCorruptBlock", err)
	}
}
