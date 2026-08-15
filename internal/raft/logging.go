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
