package linearize

import (
	"sort"
	"strings"
)

// KVModel is the sequential specification for a Helios key-value store's
// own Get, GetStale, Put, and Delete operations: exactly what a single,
// unshared, in-memory map would do if every operation ran one at a time,
// with no concurrency at all.
//
// Get AND GetStale SHARE ONE SPECIFICATION, DELIBERATELY. Helios's own
// GetStale is, today, still a leader-only, still-linearizable lease read
// (client/client.go's own doc on this, and DESIGN.md §16) -- not the
// follower-servable, genuinely-stale read its name suggests. A history
// containing both kinds of read is checked as if every read, of either
// kind, must return whatever a fully linearizable Get would have -- which
// is exactly the property this project currently claims for GetStale, and
// exactly what would show up as a violation the day that claim stops being
// true (a follower starts answering GetStale directly, say) without this
// model having to change to notice it.
//
// SCAN, SCANSTALE, AND SCANALL ARE OUT OF SCOPE, A NAMED GAP, NOT AN
// OVERSIGHT. Modeling a single-key read or write sequentially is
// straightforward: one key, one value, one before-and-after state. A
// range scan's own sequential specification is real but substantially more
// involved -- particularly with pagination, where a single logical Scan
// can span several separate RPCs (and, in a recorded history, several
// separate Operations) each carrying a continuation token whose own
// validity depends on the exact state ANOTHER operation might have
// mutated in between pages. Getting that right is a real, separate task,
// not a small addition to this one; a history containing Scan/ScanStale/
// ScanAll operations is rejected outright by the history bridge (see that
// file's own doc) rather than silently ignoring them, which would let a
// real Scan-related violation slip through unchecked.
type KVModel struct{}

// KVOp names which single-key operation a KVInput represents.
type KVOp int

const (
	KVGet KVOp = iota
	KVPut
	KVDelete
)

func (op KVOp) String() string {
	switch op {
	case KVGet:
		return "Get"
	case KVPut:
		return "Put"
	case KVDelete:
		return "Delete"
	default:
		return "KVOp(?)"
	}
}

// KVInput is what KVModel.Apply consumes. Value is only meaningful for
// KVPut.
type KVInput struct {
	Op    KVOp
	Key   string
	Value string
}

// KVOutput is what KVModel.Apply produces, and what a recorded Operation's
// own Output must equal (per KVModel.Equal) for a given ordering to be
// legal at that point in the search.
//
// PUT AND DELETE BOTH ALWAYS SUCCEED IN THIS MODEL -- there is no Ok or Err
// field a caller needs to populate for them, matching the real system's own
// contract (client/client.go's Put and Delete return only a revision and an
// error; a nil error IS the success signal, and the history bridge only
// ever converts operations that returned a nil error into an Operation at
// all -- see that file's own doc on why an operation whose own outcome is
// ambiguous, a real error, is excluded rather than guessed at).
type KVOutput struct {
	// Value and Found are meaningful for KVGet only. Found mirrors
	// client.Get's own `ok` return: whether the key currently has a value
	// at all, distinct from Value being the empty string as a legitimate
	// value some client actually put there.
	Value string
	Found bool
}

func (KVModel) Init() any {
	return map[string]string{}
}

// Apply is a pure function of its own two arguments, by construction: it
// never mutates state in place, always returning a fresh map for any
// operation that could have changed one (Put, Delete), and the identical,
// unmodified state value back for one that could not (Get). The search in
// linearize.go tries many different candidate orderings, including ones it
// abandons after this call returns, so a state one candidate produced must
// never be visible to, or mutated by, a sibling candidate that shares the
// same parent state.
func (KVModel) Apply(state any, input any) (any, any) {
	m := state.(map[string]string)
	in := input.(KVInput)

	switch in.Op {
	case KVGet:
		value, found := m[in.Key]
		return m, KVOutput{Value: value, Found: found}

	case KVPut:
		next := copyMap(m)
		next[in.Key] = in.Value
		return next, KVOutput{}

	case KVDelete:
		next := copyMap(m)
		delete(next, in.Key)
		return next, KVOutput{}

	default:
		panic("linearize: KVModel.Apply: unknown KVOp")
	}
}

func (KVModel) Equal(a, b any) bool {
	return a.(KVOutput) == b.(KVOutput)
}

// StateKey renders a map[string]string as a canonical, sorted string --
// the search's own memoization needs THIS, not Equal, specifically because
// two different orderings of the identical set of operations can produce
// two genuinely different maps (Put(k, "a") then Put(k, "b") does not
// commute), and conflating them would be unsound. See linearize.go's own
// package doc for the fuller argument.
func (KVModel) StateKey(state any) string {
	m := state.(map[string]string)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('\x00') // NUL as a field separator -- not a byte any real key/value in this test model needs to contain
		sb.WriteString(m[k])
		sb.WriteByte('\x1e') // record separator between entries
	}
	return sb.String()
}

func copyMap(m map[string]string) map[string]string {
	next := make(map[string]string, len(m))
	for k, v := range m {
		next[k] = v
	}
	return next
}
