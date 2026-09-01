package kvstore

import (
	"bytes"
	"testing"
)

func TestEncodeDecodePutRoundTrips(t *testing.T) {
	cmd := encodePut([]byte("key"), []byte("value"), 0, 0)
	dec, err := decodeCommand(cmd)
	if err != nil {
		t.Fatalf("decodeCommand: %v", err)
	}
	if dec.Tombstone {
		t.Fatal("decoded a Put as a tombstone")
	}
	if !bytes.Equal(dec.Key, []byte("key")) || !bytes.Equal(dec.Value, []byte("value")) {
		t.Fatalf("decoded = %+v, want key=key value=value", dec)
	}
}

func TestEncodeDecodeDeleteRoundTrips(t *testing.T) {
	cmd := encodeDelete([]byte("key"), 0, 0)
	dec, err := decodeCommand(cmd)
	if err != nil {
		t.Fatalf("decodeCommand: %v", err)
	}
	if !dec.Tombstone {
		t.Fatal("decoded a Delete without Tombstone set")
	}
	if !bytes.Equal(dec.Key, []byte("key")) {
		t.Fatalf("decoded key = %q, want %q", dec.Key, "key")
	}
	if dec.Value != nil {
		t.Fatalf("decoded Delete has a non-nil Value: %q", dec.Value)
	}
}

// TestPutWithAnEmptyValueIsNotConfusedWithADelete is the exact read bug
// the codec's own doc calls out: an empty-string value is a legitimate
// Put, not a signal to treat the entry as deleted.
func TestPutWithAnEmptyValueIsNotConfusedWithADelete(t *testing.T) {
	cmd := encodePut([]byte("key"), []byte(""), 0, 0)
	dec, err := decodeCommand(cmd)
	if err != nil {
		t.Fatalf("decodeCommand: %v", err)
	}
	if dec.Tombstone {
		t.Fatal("a Put with an empty value decoded as a tombstone")
	}
	if len(dec.Value) != 0 {
		t.Fatalf("decoded Value = %q, want empty", dec.Value)
	}
}

// TestEncodeDecodePutCarriesClientIDAndSequenceNumber and its Delete
// counterpart are F-4's own round trip: the fields machine.go's apply
// path actually keys its dedup check on must survive the wire
// unchanged, the identical property every other field in this codec
// has always needed and had tested (above).
func TestEncodeDecodePutCarriesClientIDAndSequenceNumber(t *testing.T) {
	cmd := encodePut([]byte("key"), []byte("value"), 42, 7)
	dec, err := decodeCommand(cmd)
	if err != nil {
		t.Fatalf("decodeCommand: %v", err)
	}
	if dec.ClientID != 42 || dec.SequenceNumber != 7 {
		t.Fatalf("decoded ClientID=%d SequenceNumber=%d, want 42, 7", dec.ClientID, dec.SequenceNumber)
	}
}

func TestEncodeDecodeDeleteCarriesClientIDAndSequenceNumber(t *testing.T) {
	cmd := encodeDelete([]byte("key"), 42, 7)
	dec, err := decodeCommand(cmd)
	if err != nil {
		t.Fatalf("decodeCommand: %v", err)
	}
	if dec.ClientID != 42 || dec.SequenceNumber != 7 {
		t.Fatalf("decoded ClientID=%d SequenceNumber=%d, want 42, 7", dec.ClientID, dec.SequenceNumber)
	}
}

func TestDecodeCommandRejectsEmptyInput(t *testing.T) {
	if _, err := decodeCommand(nil); err == nil {
		t.Fatal("decodeCommand(nil): err = nil, want an error")
	}
}

func TestDecodeCommandRejectsAnUnknownOpType(t *testing.T) {
	cmd := append([]byte{99}, encodePut([]byte("k"), []byte("v"), 0, 0)[1:]...)
	if _, err := decodeCommand(cmd); err == nil {
		t.Fatal("decodeCommand with an unknown op type: err = nil, want an error")
	}
}

// TestDecodeCommandRejectsATruncatedClientIDAndSequenceNumber is F-4's
// own new truncation case: fewer than 16 bytes after opType, the fixed
// width client_id+sequence_number now always occupies.
func TestDecodeCommandRejectsATruncatedClientIDAndSequenceNumber(t *testing.T) {
	if _, err := decodeCommand([]byte{byte(opPut), 1, 2, 3}); err == nil {
		t.Fatal("decodeCommand with a truncated client_id/sequence_number: err = nil, want an error")
	}
}

func TestDecodeCommandRejectsATruncatedLengthPrefix(t *testing.T) {
	// 16 well-formed bytes for client_id+sequence_number, then a
	// truncated key-length prefix -- padded so this test still reaches
	// and exercises readLenPrefixed's own truncation check, the thing
	// it is actually named for, rather than tripping the newer
	// client_id/sequence_number check first by accident.
	cmd := append([]byte{byte(opPut)}, make([]byte, 16)...)
	cmd = append(cmd, 1, 2)
	if _, err := decodeCommand(cmd); err == nil {
		t.Fatal("decodeCommand with a truncated length prefix: err = nil, want an error")
	}
}

func TestDecodeCommandRejectsALengthPrefixExceedingTheRemainingBytes(t *testing.T) {
	// 16 well-formed bytes for client_id+sequence_number, then a key
	// length prefix that claims 100 bytes but supplies none.
	cmd := append([]byte{byte(opPut)}, make([]byte, 16)...)
	cmd = append(cmd, 100, 0, 0, 0)
	if _, err := decodeCommand(cmd); err == nil {
		t.Fatal("decodeCommand with an over-long length prefix: err = nil, want an error")
	}
}

func TestDecodeCommandRejectsATruncatedPutMissingItsValue(t *testing.T) {
	// A well-formed client_id/sequence_number and key, then nothing for
	// the value length prefix.
	var cmd []byte
	cmd = append(cmd, byte(opPut))
	cmd = appendUint64(cmd, 0)
	cmd = appendUint64(cmd, 0)
	cmd = appendLenPrefixed(cmd, []byte("key"))
	if _, err := decodeCommand(cmd); err == nil {
		t.Fatal("decodeCommand with a Put missing its value: err = nil, want an error")
	}
}
