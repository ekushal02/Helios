package kvstore

import (
	"bytes"
	"testing"
)

func TestEncodeDecodePutRoundTrips(t *testing.T) {
	cmd := encodePut([]byte("key"), []byte("value"))
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
	cmd := encodeDelete([]byte("key"))
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
	cmd := encodePut([]byte("key"), []byte(""))
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

func TestDecodeCommandRejectsEmptyInput(t *testing.T) {
	if _, err := decodeCommand(nil); err == nil {
		t.Fatal("decodeCommand(nil): err = nil, want an error")
	}
}

func TestDecodeCommandRejectsAnUnknownOpType(t *testing.T) {
	cmd := append([]byte{99}, encodePut([]byte("k"), []byte("v"))[1:]...)
	if _, err := decodeCommand(cmd); err == nil {
		t.Fatal("decodeCommand with an unknown op type: err = nil, want an error")
	}
}

func TestDecodeCommandRejectsATruncatedLengthPrefix(t *testing.T) {
	if _, err := decodeCommand([]byte{byte(opPut), 1, 2}); err == nil {
		t.Fatal("decodeCommand with a truncated length prefix: err = nil, want an error")
	}
}

func TestDecodeCommandRejectsALengthPrefixExceedingTheRemainingBytes(t *testing.T) {
	// Claims a 100-byte key but supplies none.
	cmd := []byte{byte(opPut), 100, 0, 0, 0}
	if _, err := decodeCommand(cmd); err == nil {
		t.Fatal("decodeCommand with an over-long length prefix: err = nil, want an error")
	}
}

func TestDecodeCommandRejectsATruncatedPutMissingItsValue(t *testing.T) {
	// A well-formed key, then nothing for the value length prefix.
	var cmd []byte
	cmd = append(cmd, byte(opPut))
	cmd = appendLenPrefixed(cmd, []byte("key"))
	if _, err := decodeCommand(cmd); err == nil {
		t.Fatal("decodeCommand with a Put missing its value: err = nil, want an error")
	}
}
