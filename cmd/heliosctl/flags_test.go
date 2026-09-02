package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    map[int]string
		wantErr bool
	}{
		{"single", "1=localhost:50051", map[int]string{1: "localhost:50051"}, false},
		{"multiple", "1=host1:50051,2=host2:50052", map[int]string{1: "host1:50051", 2: "host2:50052"}, false},
		{"whitespace tolerated", " 1 = localhost:50051 , 2=host2:1 ", map[int]string{1: "localhost:50051", 2: "host2:1"}, false},
		{"empty string", "", nil, true},
		{"missing equals", "1", nil, true},
		{"non-numeric id", "x=localhost:50051", nil, true},
		{"empty address", "1=", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePeers(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parsePeers(%q): err = nil, want an error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePeers(%q): %v", tt.in, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parsePeers(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for id, addr := range tt.want {
				if got[id] != addr {
					t.Errorf("parsePeers(%q)[%d] = %q, want %q", tt.in, id, got[id], addr)
				}
			}
		})
	}
}

// TestSubcommandsRejectWrongArgCount checks every subcommand's own
// argument validation fails fast, WITHOUT attempting any network I/O --
// each of these calls with deliberately-wrong arguments and expects a
// prompt, network-free non-zero exit, not a connection timeout.
func TestSubcommandsRejectWrongArgCount(t *testing.T) {
	tests := []struct {
		name string
		fn   func(args []string, stdout, stderr io.Writer) int
		args []string
	}{
		{"get with no args", runGet, nil},
		{"get with two args", runGet, []string{"a", "b"}},
		{"put with one arg", runPut, []string{"a"}},
		{"put with three args", runPut, []string{"a", "b", "c"}},
		{"del with no args", runDel, nil},
		{"del with two args", runDel, []string{"a", "b"}},
		{"scan with a positional arg", runScan, []string{"unexpected"}},
		{"status with a positional arg", runStatus, []string{"unexpected"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := tt.fn(tt.args, &stdout, &stderr)
			if code == 0 {
				t.Fatalf("exit code = 0, want non-zero for invalid arguments")
			}
			if stderr.Len() == 0 {
				t.Fatal("stderr is empty, want an explanation of the argument error")
			}
		})
	}
}

func TestSubcommandsRejectAnUnknownFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runGet([]string{"--bogus", "key"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("get --bogus: exit code = 0, want non-zero for an unknown flag")
	}
}

func TestRunWithNoArgsPrintsHelpAndFails(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero when no command is given")
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Errorf("stderr = %q, want it to contain top-level usage", stderr.String())
	}
}

func TestRunHelpSucceeds(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "Usage:") {
		t.Errorf("stdout = %q, want top-level usage", stdout.String())
	}
}

func TestRunHelpForEachKnownCommand(t *testing.T) {
	for _, cmd := range []string{"get", "put", "del", "scan", "status"} {
		t.Run(cmd, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run([]string{"help", cmd}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("exit code = %d, want 0", code)
			}
			if !strings.Contains(stdout.String(), "heliosctl "+cmd) {
				t.Errorf("stdout = %q, want it to contain %q's own usage line", stdout.String(), cmd)
			}
			if !strings.Contains(stdout.String(), "Examples:") {
				t.Errorf("%s's help has no Examples section -- the whole point of this task is help a stranger can follow", cmd)
			}
		})
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"bogus"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero for an unknown command")
	}
	if !strings.Contains(stderr.String(), `unknown command "bogus"`) {
		t.Errorf("stderr = %q, want it to name the unknown command", stderr.String())
	}
}
