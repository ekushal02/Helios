package sstable

import "bytes"

// Merge combines several Sources, already each individually in ascending
// key order, into one Source in ascending key order -- the primitive
// compaction (§13.8) is built on: read every file at the level being
// compacted plus the level below it, merge them, hand the result
// straight to Write to produce the replacement file.
//
// sources MUST be given newest-first -- the same convention
// engine.Reader already uses for its own tiers (§13.6), and for the
// identical reason: when the same key appears in more than one source,
// the earliest (lowest-indexed) source that has it wins, whether that
// entry is a live value or a tombstone. Every source that also has the
// key is still advanced past it -- consumed, not re-emitted -- so a key
// present in three sources appears exactly once in the merged output,
// never three times, which Write's own ErrOutOfOrder guard would
// otherwise reject as a repeated (non-strictly-increasing) key.
//
// If dropTombstones is true, a winning entry that is a tombstone is
// discarded rather than emitted at all -- the actual point of a
// compaction that has reached the bottom of the tiers a key could be
// shadowing: once nothing older exists anywhere, a delete marker has
// finished its job and can finally stop taking up space. See §13.8 for
// the exact rule Compact uses to decide when this is safe to pass as
// true; Merge itself does not know or care why, it only does what it is
// told.
func Merge(sources []Source, dropTombstones bool) Source {
	m := &merger{sources: sources, dropTombstones: dropTombstones}
	m.primeAll()
	return m
}

// merger's per-source lookahead: each wrapped source keeps its current
// entry buffered so Merge can compare "the next key from every source"
// without consuming any of them until a winner is chosen.
type mergerSource struct {
	src  Source
	done bool // true once src.Next() has returned false
}

type merger struct {
	sources        []Source
	states         []mergerSource
	dropTombstones bool

	cur blockEntry
	err error
}

func (m *merger) primeAll() {
	m.states = make([]mergerSource, len(m.sources))
	for i, s := range m.sources {
		m.states[i] = mergerSource{src: s}
		m.advance(i)
	}
}

// advance calls Next on states[i]'s source and, the moment it returns
// false, checks Err right then -- not later, and not from some separate
// pass over sources already marked done. A source transitions to done
// and (possibly) failed in the exact same instant; checking Err at any
// other time risks exactly the bug this function exists to avoid: a
// later scan that skips sources already marked done would never notice
// one of them failed rather than finished cleanly.
func (m *merger) advance(i int) {
	s := &m.states[i]
	if s.done {
		return
	}
	if !s.src.Next() {
		s.done = true
		if err := s.src.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}
}

// Next finds the smallest key currently buffered across every
// not-yet-exhausted source, resolves it against the newest-first
// priority rule, advances every source that held that key (whether or
// not it won), and either returns true with the winner buffered in cur,
// or loops again if the winner was a dropped tombstone.
func (m *merger) Next() bool {
	if m.err != nil {
		return false
	}
	for {
		minIdx := -1
		for i := range m.states {
			if m.states[i].done {
				continue
			}
			if minIdx == -1 || bytes.Compare(m.states[i].src.Key(), m.states[minIdx].src.Key()) < 0 {
				minIdx = i
			}
		}
		if minIdx == -1 {
			return false // every source exhausted cleanly
		}
		minKey := m.states[minIdx].src.Key()

		// The lowest-indexed source among every source currently AT
		// minKey wins -- "lowest index" is "newest" by the caller's own
		// ordering contract. Every source at minKey is advanced,
		// regardless of whether it won, so minKey is never seen again on
		// a later call.
		winner := -1
		for i := range m.states {
			if m.states[i].done {
				continue
			}
			if bytes.Equal(m.states[i].src.Key(), minKey) {
				if winner == -1 {
					winner = i
				}
			}
		}

		var win blockEntry
		win.Key = append([]byte(nil), minKey...)
		if !m.states[winner].src.Tombstone() {
			win.Value = append([]byte(nil), m.states[winner].src.Value()...)
		} else {
			win.Tombstone = true
		}

		for i := range m.states {
			if !m.states[i].done && bytes.Equal(m.states[i].src.Key(), minKey) {
				m.advance(i)
			}
		}
		if m.err != nil {
			// One of the sources just advanced past minKey failed doing
			// so. The entry chosen above was read successfully, but
			// publishing it while a sibling source is broken would still
			// produce a file whose remaining, unread keys are silently
			// missing -- so this failure takes priority over the entry
			// in hand.
			return false
		}

		if m.dropTombstones && win.Tombstone {
			continue // this key's story ends here; nothing older to shadow
		}
		m.cur = win
		return true
	}
}

func (m *merger) Key() []byte     { return m.cur.Key }
func (m *merger) Value() []byte   { return m.cur.Value }
func (m *merger) Tombstone() bool { return m.cur.Tombstone }
func (m *merger) Err() error      { return m.err }
