package sstable

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
)

// CompressionType identifies how one data block's entry bytes are stored
// on disk -- raw, or compressed by a specific algorithm. It is a
// per-BLOCK choice, not a per-file one: two blocks in the same SSTable
// can carry different values, because the writer (finalizeBlock) decides
// per block whether compressing actually helped (see CompressionFlate's
// doc) and falls back to CompressionNone when it didn't.
type CompressionType uint8

const (
	// CompressionNone means the block's entry bytes are stored exactly
	// as decodeBlockEntries expects them, unmodified -- the only
	// compression type that has ever existed in this format before this
	// task, and still the default for every existing caller of Write.
	CompressionNone CompressionType = 0

	// CompressionFlate means the block's entry bytes were passed through
	// compress/flate (raw DEFLATE) before being written.
	//
	// FLATE, NOT GZIP OR ZLIB, AND THE REASON IS THIS FORMAT'S OWN CRC.
	// Both gzip and zlib wrap DEFLATE in their own container, which adds
	// its own header and (in gzip's case) its own CRC32 -- entirely
	// redundant here, since every data block already carries a BlockCRC32
	// over its full on-disk bytes (block.go), compressed or not. flate is
	// the raw DEFLATE stream with no such wrapper, so choosing it costs
	// nothing beyond what this format was already paying for verification,
	// rather than paying twice for two different checksums covering
	// overlapping bytes.
	CompressionFlate CompressionType = 1
)

// maxDecompressedBlockSize bounds how large a single block is ever
// allowed to decompress to -- a defensive limit, not a normal-operation
// one. A block written by this package's own Write is built from entries
// accumulated up to targetBlockSize (4KB) before compression, so its
// decompressed size is never remotely close to this bound in practice;
// the limit exists for the case a block's CRC matches (ruling out
// ordinary bit-rot) but its claimed compressed content still decompresses
// to something enormous -- the classic "decompression bomb" a compressed
// format has to defend against by NOT trusting the compressed stream to
// describe its own size honestly. 64x targetBlockSize is generous enough
// to never clip a legitimate block from this writer while still refusing
// to let a single block consume unbounded memory on read.
const maxDecompressedBlockSize = 64 * targetBlockSize

// compressFlate compresses body with raw DEFLATE at the standard
// library's default compression level -- a balance between ratio and
// CPU cost this package has not tuned further; see DESIGN.md §12 for
// that as an open question, the same "asserted, not yet measured
// against a real workload" status targetBlockSize and bitsPerKey are
// already recorded under.
func compressFlate(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, fmt.Errorf("sstable: compress: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return nil, fmt.Errorf("sstable: compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("sstable: compress: %w", err)
	}
	return buf.Bytes(), nil
}

// decompressFlate reverses compressFlate, refusing to produce more than
// maxDecompressedBlockSize bytes -- see that constant's doc for why this
// limit exists at all. io.LimitReader silently truncates rather than
// erroring on its own once the limit is hit, so the limit is checked
// explicitly here by reading one byte past it and treating that extra
// byte's presence as the signal the stream would have kept going.
func decompressFlate(compressed []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(compressed))
	defer r.Close()

	limited := io.LimitReader(r, maxDecompressedBlockSize+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("sstable: decompress: %w: %w", ErrCorruptBlock, err)
	}
	if len(out) > maxDecompressedBlockSize {
		return nil, fmt.Errorf("sstable: decompress: exceeds %d-byte limit: %w", maxDecompressedBlockSize, ErrCorruptBlock)
	}
	return out, nil
}
