// Package logging builds this project's own structured logger: JSON,
// always, with a configurable minimum level. One mechanism, not a
// convention each binary is trusted to repeat correctly -- cmd/helios
// (and any future binary that logs at all) calls New here rather than
// constructing its own slog.NewJSONHandler and re-deriving the same
// level-parsing logic independently.
//
// "A DEBUG MODE THAT DUMPS RAFT STATE TRANSITIONS" IS NOT A SEPARATE
// FLAG -- IT IS THIS PACKAGE'S OWN DEBUG LEVEL. internal/raft's own
// setState (logging.go) logs every state transition at exactly
// slog.LevelDebug, unconditionally -- there is no second switch
// anywhere that turns transition-dumping on or off independently of
// the log level itself. Setting the level this package builds a
// logger at to Debug is the whole mechanism; nothing else is needed
// or exists.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// ParseLevel turns a level name ("debug", "info", "warn"/"warning",
// "error", case-insensitive, surrounding whitespace ignored) into its
// slog.Level. An empty or unrecognized string returns slog.LevelInfo
// -- the same "default to the safe, ordinary choice on an unset or
// unrecognized value" convention Server.Get's own Consistency handling
// already follows for CONSISTENCY_UNSPECIFIED (§16.5): a caller that
// mistypes HELIOS_LOG_LEVEL gets ordinary logging, not a startup
// failure over a cosmetic setting, and not silently more or less
// verbose than they'd reasonably expect from a name they didn't
// actually ask for.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New builds a structured JSON logger at the given level, writing to w.
func New(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}