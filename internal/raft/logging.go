package raft

import (
	"io"
	"log/slog"
)

// discardLogger is what a Node uses until something gives it a real one.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
	Level: slog.LevelError + 1,
}))

// LogValue makes State render as its name rather than its integer.
func (s State) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

// SetLogger attaches a logger and permanently binds this node's id to it.
func (n *Node) SetLogger(l *slog.Logger) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if l == nil {
		l = discardLogger
	}
	n.logger = l.With("node", n.id)
}

func (n *Node) lg() *slog.Logger {
	if n.logger == nil {
		return discardLogger
	}
	return n.logger.With("term", n.currentTerm, "state", n.state)
}

// setState assigns n.state and, if this is an actual transition (not a
// same-to-same no-op -- see installsnapshot.go's own call site for why
// that guard matters), logs it at Debug level: "state transition" is a
// debug mode message, ALWAYS, deliberately not something that also
// happens to appear at a louder level for particular transitions
// (compare config.go's stepDownIfRemoved, which logs its own Info-level
// line for WHY it's stepping down, independent of and in addition to
// the mechanical transition line this method emits). Turning on debug
// logging (internal/logging.ParseLevel("debug"), cmd/helios's own
// HELIOS_LOG_LEVEL) is what "dumps Raft state transitions" -- there is
// no separate flag for it, because there is nothing else debug-level
// logging would need to mean here.
//
// This is the ONLY place n.state is ever assigned outside NewNode's own
// initial struct literal (Follower, the state every node starts in,
// which is not a transition from anything and logs nothing) -- every
// call site that used to write n.state = X directly now calls this
// instead, the same "one choke point, not several places doing the
// identical assignment independently" reasoning §8 gives for
// commitIndex's own commitTo funnel.
//
// Caller must hold mu, matching lg() and every other state-mutating
// method in this package.
func (n *Node) setState(newState State, reason string) {
	oldState := n.state
	n.state = newState
	if oldState == newState {
		return
	}
	n.lg().Debug("state transition", "from", oldState, "to", newState, "reason", reason)
}