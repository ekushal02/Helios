package sstable

import "github.com/ekushal02/helios/internal/storage/memtable"

// Flush drains m, in its entirety, into a new immutable SSTable at path.
// It is Write with memtable.Memtable's own Iterator plugged in as the
// Source, which is the only reason this file imports package memtable at
// all -- see the doc on the Source interface in writer.go.
//
// Flush does not clear, reset, or otherwise touch m. That is deliberate:
// a Memtable is not resized or reorganized once created (see its own type
// doc), so there is no in-place "empty it back out" operation to call
// even if this package wanted to. The caller is expected to have already
// switched writes over to a fresh Memtable before calling Flush on the
// old one -- exactly the sequencing the memtable package's own doc
// describes and leaves to its caller.
func Flush(m *memtable.Memtable, path string) (*Info, error) {
	return Write(m.NewIterator(), path)
}

// FlushCompressed is Flush, but every data block is compressed -- see
// WriteCompressed. Added alongside Flush rather than replacing it, on
// the same "leave the existing simple path alone" precedent Write and
// WriteCompressed themselves follow.
func FlushCompressed(m *memtable.Memtable, path string, compression CompressionType) (*Info, error) {
	return WriteCompressed(m.NewIterator(), path, compression)
}

// FlushIfFull is the size-threshold trigger this task exists to add: if
// m.ApproxSize() has reached thresholdBytes, flush it to path and report
// flushed=true; otherwise do nothing and report flushed=false with a nil
// error, which is not a failure -- most calls against a memtable that
// isn't full yet are expected to look exactly like this.
//
// THIS IS DELIBERATELY THIN, AND STOPS SHORT OF BEING AN ENGINE. It makes
// exactly one decision -- big enough, or not -- and if so, one blocking
// write. It does not choose path or a sequence number (the caller
// controls both, the same way wal.Open takes a full path rather than
// inventing a naming scheme of its own), does not swap a fresh Memtable
// into the write path, does not make the flushed memtable's data visible
// to reads against the new one (see DESIGN.md §13.4's note on memtables
// "mid-flush"), and does not run in the background -- the caller blocks
// for the duration of the write, same as Flush. All of that is real work
// belonging to whatever orchestrates the write path above both this
// package and memtable, and none of it exists yet; wiring it is future
// work, tracked in DESIGN.md §12.
func FlushIfFull(m *memtable.Memtable, thresholdBytes int64, path string) (flushed bool, info *Info, err error) {
	if m.ApproxSize() < thresholdBytes {
		return false, nil, nil
	}
	info, err = Flush(m, path)
	if err != nil {
		return false, nil, err
	}
	return true, info, nil
}
