package raft

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// Storage is the durability boundary for a Node.
//
// The interface is deliberately a single opaque blob rather than
// Save(term, vote, log). Everything Figure 2 calls persistent has to become
// durable together or not at all: a record holding a new term next to an old
// log is not a state any correct node ever occupied, and a restart that
// adopted it would be worse than losing the write entirely. One blob makes
// that impossible to get wrong at this layer.
type Storage interface {
	// Save makes b durable, replacing whatever was there before.
	//
	// Save must be atomic with respect to process death: a concurrent crash
	// leaves either the complete previous blob or the complete new one, never
	// a mixture and never nothing.
	//
	// Save returns only after the bytes are on stable storage.
	Save(b []byte) error

	// Load returns the most recently saved blob.
	//
	// Load returns (nil, nil) — and only that — when nothing has ever been
	// saved. That is a fresh node. Every other failure is an error, including
	// a blob that exists but cannot be read: see the note on decodeState.
	Load() ([]byte, error)
}

// =============================================================================
// MemoryStorage
// =============================================================================

// MemoryStorage keeps state in RAM. It satisfies the atomicity contract
// trivially, and it is what NewNode installs by default so that every existing
// test keeps working unchanged.
//
// It is not durable, obviously. A node backed by MemoryStorage is exactly as
// safe as a node with no persistence at all — which is why the harness must
// hand out FileStorage, or a MemoryStorage explicitly carried across the
// restart, in any test that claims to restart anything.
type MemoryStorage struct {
	mu   sync.Mutex
	data []byte
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) Save(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Copy. The caller's buffer is not ours to alias, and encodeState
	// currently returns a fresh slice only by accident of implementation.
	s.data = append([]byte(nil), b...)
	return nil
}

func (s *MemoryStorage) Load() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		return nil, nil
	}
	return append([]byte(nil), s.data...), nil
}

// =============================================================================
// FileStorage
// =============================================================================

const (
	stateFileName = "state.raft"
	tempFileName  = "state.raft.tmp"
)

// FileStorage writes state to a single file in a directory, using the
// write-temp / fsync / rename / fsync-directory sequence.
//
// WHY NOT JUST WRITE THE FILE IN PLACE. Because a write of more than one
// sector is not atomic. A crash midway through overwriting state.raft leaves a
// file whose head is the new record and whose tail is the old one. That blob
// may still decode — a shorter log with a higher term is structurally valid —
// and a node that adopted it would have silently lost entries it had
// acknowledged. rename(2) is the only cheap primitive that gives all-or-nothing
// replacement.
//
// WHY THE DIRECTORY FSYNC. The rename is a metadata operation on the directory.
// Syncing the file's contents does not sync the directory entry that points at
// them, so a crash after rename but before the directory is flushed can leave
// the old name in place with the new file orphaned. This is the step that gets
// skipped in nine out of ten implementations of this function.
type FileStorage struct {
	mu   sync.Mutex
	dir  string
	path string
	tmp  string
}

// NewFileStorage prepares dir to hold one node's state, creating it if needed.
//
// It removes any leftover temp file. A temp file is never authoritative — if
// one exists it is the residue of a Save that was killed before its rename, and
// the record it holds was by definition never acknowledged to anyone.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{
		dir:  dir,
		path: filepath.Join(dir, stateFileName),
		tmp:  filepath.Join(dir, tempFileName),
	}
	if err := os.Remove(s.tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	return s, nil
}

// Dir reports the directory this storage owns. Tests use it; production does
// not need it.
func (s *FileStorage) Dir() string { return s.dir }

func (s *FileStorage) Save(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	// Contents durable before the name is switched. Doing these in the other
	// order would let a crash expose an empty state.raft.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(s.tmp, s.path); err != nil {
		return err
	}
	return syncDir(s.dir)
}

func (s *FileStorage) Load() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil // never saved: a genuinely fresh node
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

// syncDir flushes a directory's metadata so that a rename into it survives.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		// Windows cannot fsync a directory handle. Helios targets unix, so
		// everywhere else this is a real failure and must not be swallowed.
		if runtime.GOOS == "windows" {
			return nil
		}
		return err
	}
	return nil
}
