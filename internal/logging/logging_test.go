package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"  debug  ", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"WARN", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
		{"", slog.LevelInfo},
		{"bogus", slog.LevelInfo},
		{"trace", slog.LevelInfo}, // not a level this project defines -- falls back, does not panic or error
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := ParseLevel(tt.in); got != tt.want {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// TestNewProducesValidJSONWithTheRequestedLevel checks the actual
// output, not just that New returns something -- a real log line,
// parsed back as JSON, with the fields a structured-logging consumer
// would actually depend on.
func TestNewProducesValidJSONWithTheRequestedLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, slog.LevelInfo)
	logger.Info("hello", "key", "value")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not valid JSON: %q: %v", buf.String(), err)
	}
	if rec["msg"] != "hello" {
		t.Errorf("msg = %v, want \"hello\"", rec["msg"])
	}
	if rec["key"] != "value" {
		t.Errorf("key = %v, want \"value\"", rec["key"])
	}
	if rec["level"] != "INFO" {
		t.Errorf("level = %v, want \"INFO\"", rec["level"])
	}
}

// TestNewFiltersBelowItsOwnLevel is the other half of "with levels" --
// a logger built at Warn must not emit Info or Debug lines at all, not
// merely mark them somehow for a reader to filter out later.
func TestNewFiltersBelowItsOwnLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, slog.LevelWarn)
	logger.Debug("should not appear")
	logger.Info("should not appear either")
	logger.Warn("should appear")

	out := buf.String()
	if buf.Len() == 0 {
		t.Fatal("no output at all -- want the Warn line at least")
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(out), &rec); err != nil {
		t.Fatalf("output is not valid JSON: %q: %v", out, err)
	}
	if rec["msg"] != "should appear" {
		t.Errorf("only line emitted has msg = %v, want \"should appear\" -- Debug/Info lines were not filtered", rec["msg"])
	}
}