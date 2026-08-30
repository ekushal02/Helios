package sstable

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"
)

func TestCompressFlateThenDecompressRoundTrips(t *testing.T) {
	original := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog ", 50))
	compressed, err := compressFlate(original)
	if err != nil {
		t.Fatalf("compressFlate: %v", err)
	}
	if len(compressed) >= len(original) {
		t.Fatalf("compressed size %d >= original size %d, want meaningfully smaller for repetitive input", len(compressed), len(original))
	}

	decompressed, err := decompressFlate(compressed)
	if err != nil {
		t.Fatalf("decompressFlate: %v", err)
	}
	if !bytes.Equal(decompressed, original) {
		t.Fatalf("round trip mismatch: got %d bytes, want %d bytes matching the original", len(decompressed), len(original))
	}
}

func TestCompressFlateOnEmptyInput(t *testing.T) {
	compressed, err := compressFlate(nil)
	if err != nil {
		t.Fatalf("compressFlate(nil): %v", err)
	}
	decompressed, err := decompressFlate(compressed)
	if err != nil {
		t.Fatalf("decompressFlate: %v", err)
	}
	if len(decompressed) != 0 {
		t.Fatalf("decompressed %d bytes from empty input, want 0", len(decompressed))
	}
}

func TestDecompressFlateRejectsGarbageInput(t *testing.T) {
	garbage := make([]byte, 64)
	if _, err := rand.Read(garbage); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if _, err := decompressFlate(garbage); err == nil {
		t.Fatal("decompressFlate on random garbage: err = nil, want a decode error")
	}
}

func TestDecompressFlateEnforcesTheSizeLimit(t *testing.T) {
	// A highly compressible input that decompresses to well past
	// maxDecompressedBlockSize -- not remotely realistic for a block
	// this package's own Write ever produces (blocks are built up to
	// targetBlockSize before compression), but exactly the shape a
	// decompression-bomb guard has to refuse regardless of how it
	// arrived.
	huge := bytes.Repeat([]byte{'a'}, maxDecompressedBlockSize*2)
	compressed, err := compressFlate(huge)
	if err != nil {
		t.Fatalf("compressFlate: %v", err)
	}
	if _, err := decompressFlate(compressed); err == nil {
		t.Fatal("decompressFlate on an oversized stream: err = nil, want the size-limit error")
	}
}

func TestFinalizeBlockFallsBackToUncompressedWhenItDoesNotHelp(t *testing.T) {
	// Random bytes are, by construction, essentially incompressible --
	// flate should produce output no smaller than the input (likely
	// slightly larger, from its own framing), so finalizeBlock must
	// choose CompressionNone rather than pay decompression cost for
	// nothing.
	random := make([]byte, 256)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	finalized, err := finalizeBlock(random, CompressionFlate)
	if err != nil {
		t.Fatalf("finalizeBlock: %v", err)
	}
	if CompressionType(finalized[0]) != CompressionNone {
		t.Fatalf("finalizeBlock on incompressible data chose type %d, want CompressionNone (%d)", finalized[0], CompressionNone)
	}

	body, err := verifyAndSplitBlock(finalized)
	if err != nil {
		t.Fatalf("verifyAndSplitBlock: %v", err)
	}
	if !bytes.Equal(body, random) {
		t.Fatal("round trip through finalizeBlock/verifyAndSplitBlock changed the bytes")
	}
}

func TestFinalizeBlockUsesCompressionWhenItHelps(t *testing.T) {
	compressible := []byte(strings.Repeat("aaaaaaaaaa", 200))
	finalized, err := finalizeBlock(compressible, CompressionFlate)
	if err != nil {
		t.Fatalf("finalizeBlock: %v", err)
	}
	if CompressionType(finalized[0]) != CompressionFlate {
		t.Fatalf("finalizeBlock on highly compressible data chose type %d, want CompressionFlate (%d)", finalized[0], CompressionFlate)
	}
	if len(finalized) >= len(compressible) {
		t.Fatalf("finalized block (%d bytes) is not smaller than the original entry bytes (%d bytes)", len(finalized), len(compressible))
	}

	body, err := verifyAndSplitBlock(finalized)
	if err != nil {
		t.Fatalf("verifyAndSplitBlock: %v", err)
	}
	if !bytes.Equal(body, compressible) {
		t.Fatal("round trip through finalizeBlock/verifyAndSplitBlock changed the bytes")
	}
}

func TestFinalizeBlockWithCompressionNoneNeverCompresses(t *testing.T) {
	compressible := []byte(strings.Repeat("aaaaaaaaaa", 200))
	finalized, err := finalizeBlock(compressible, CompressionNone)
	if err != nil {
		t.Fatalf("finalizeBlock: %v", err)
	}
	if CompressionType(finalized[0]) != CompressionNone {
		t.Fatalf("finalizeBlock(CompressionNone) chose type %d, want CompressionNone (%d) regardless of compressibility", finalized[0], CompressionNone)
	}
}

// TestVerifyAndSplitBlockRejectsAnUnknownCompressionType constructs a
// block by hand -- an unknown type byte, a valid CRC over it -- so the
// "unknown compression type" branch in verifyAndSplitBlock is reached
// directly, rather than only reachable in principle behind a CRC
// mismatch that would otherwise short-circuit the same test case.
func TestVerifyAndSplitBlockRejectsAnUnknownCompressionType(t *testing.T) {
	payload := []byte("hello")
	raw := append([]byte{99}, payload...) // 99: not CompressionNone or CompressionFlate
	crc := crc32.ChecksumIEEE(raw)
	var crcBuf [crcSize]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	raw = append(raw, crcBuf[:]...)

	if _, err := verifyAndSplitBlock(raw); err != ErrCorruptBlock {
		t.Fatalf("verifyAndSplitBlock with an unknown but CRC-valid compression type: err = %v, want ErrCorruptBlock", err)
	}
}
