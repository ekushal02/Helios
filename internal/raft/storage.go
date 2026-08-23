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
	// Save returns only after the bytes are on stable storage. SyncNever
	// weakens exactly this sentence and nothing else -- see SyncPolicy.
	Save(b []byte) error

	// Load returns the most recently saved blob.
	//
	// Load returns (nil, nil) -- and only that -- when nothing has ever been
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
// safe as a node with no persistence at all -- which is why the harness must
// hand out FileStorage, or a storage explicitly carried across the restart, in
// any test that claims to restart anything.
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
// Sync policy
// =============================================================================

// SyncPolicy decides whether FileStorage waits for the device before returning.
//
// It does NOT decide whether the write is atomic. Both policies write to a temp
// file and rename, so both leave either the whole old record or the whole new
// one after a process death. The difference is narrower and more serious: which
// failures the bytes survive. See docs/fsync-policy.md.
type SyncPolicy int

const (
	// SyncAlways flushes the file and the directory before Save returns. This
	// is the only policy under which Figure 2's "updated on stable storage
	// before responding to RPCs" is literally true.
	SyncAlways SyncPolicy = iota

	// SyncNever hands the bytes to the kernel and returns.
	//
	// The record still survives a process crash -- the page cache belongs to
	// the kernel, not the process -- which is exactly why the SIGKILL test
	// cannot tell the two policies apart. It does not survive a power cut or a
	// kernel panic, and a Raft node that comes back having forgotten its term
	// and vote can grant a second vote in a term it already voted in.
	SyncNever
)

func (p SyncPolicy) String() string {
	switch p {
	case SyncAlways:
		return "always"
	case SyncNever:
		return "never"
	default:
		return "unknown"
	}
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
// may still decode -- a shorter log with a higher term is structurally valid --
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
	mu     sync.Mutex
	dir    string
	path   string
	tmp    string
	policy SyncPolicy
}

// NewFileStorage prepares dir to hold one node's state, creating it if needed.
// The default policy is SyncAlways; anything else has to be asked for by name.
func NewFileStorage(dir string) (*FileStorage, error) {
	return NewFileStorageWithPolicy(dir, SyncAlways)
}

// NewFileStorageWithPolicy is NewFileStorage with the durability guarantee
// stated explicitly at the call site, which is where a reader will look for it.
//
// It removes any leftover temp file. A temp file is never authoritative -- if
// one exists it is the residue of a Save that was killed before its rename, and
// the record it holds was by definition never acknowledged to anyone.
func NewFileStorageWithPolicy(dir string, policy SyncPolicy) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{
		dir:    dir,
		path:   filepath.Join(dir, stateFileName),
		tmp:    filepath.Join(dir, tempFileName),
		policy: policy,
	}
	if err := os.Remove(s.tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	return s, nil
}

// Dir reports the directory this storage owns. Tests use it; production does
// not need it.
func (s *FileStorage) Dir() string { return s.dir }

// Policy reports the durability guarantee this storage is making.
func (s *FileStorage) Policy() SyncPolicy { return s.policy }

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
	if s.policy == SyncAlways {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	// The rename happens under every policy. Atomicity is not the thing being
	// traded away here; durability is.
	if err := os.Rename(s.tmp, s.path); err != nil {
		return err
	}
	if s.policy == SyncAlways {
		return syncDir(s.dir)
	}
	return nil
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

// =============================================================================
// Group commit
// =============================================================================

// batchedStorage coalesces concurrent Saves into a single write and a single
// flush, and returns to every caller only once that flush has completed.
//
// IT GIVES UP NO DURABILITY. This is the point people get wrong about group
// commit: it is not "fsync less often and hope". Every Save still blocks until
// the bytes are on the medium. What is shared is the flush, not the promise.
// The cost is latency for whoever arrives first in a batch -- they wait out the
// stragglers who join behind them -- in exchange for throughput proportional to
// how many callers are in flight at once.
//
// WHY COALESCING IS SOUND HERE. Every record is a complete snapshot of the
// three persistent fields, and every record is built under n.mu, so the blobs
// form a totally ordered sequence of states the node genuinely occupied. A
// caller waiting on generation G is therefore satisfied by any durable
// generation >= G: the later record describes a state the node has since
// legitimately reached. Superseded records are dropped without ever being
// written, which is where the saving comes from.
//
// That argument leans on the total order. If persistIfDirty is ever moved out
// from under n.mu -- which is exactly what would make this wrapper useful --
// two records could be built from interleaved states and the "later record
// subsumes the earlier promise" step stops holding for truncations. Establish
// the ordering some other way before wiring this in.
//
// NOT CURRENTLY WIRED INTO Node, and the benchmark explains why: with
// persistIfDirty under n.mu a node has at most one Save in flight, so there is
// never anything to coalesce.
type batchedStorage struct {
	base Storage

	mu      sync.Mutex
	cond    *sync.Cond
	pending []byte
	pendGen uint64 // newest record handed in
	doneGen uint64 // newest record on the medium
	err     error
	closed  bool

	wake chan struct{}
	quit chan struct{}
	done chan struct{}

	saves   int // calls to Save
	flushes int // calls that reached the disk
}

func newBatchedStorage(base Storage) *batchedStorage {
	s := &batchedStorage{
		base: base,
		wake: make(chan struct{}, 1),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.flusher()
	return s
}

func (s *batchedStorage) Save(b []byte) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("raft: storage is closed")
	}
	s.saves++
	s.pendGen++
	gen := s.pendGen
	s.pending = append([]byte(nil), b...)
	s.mu.Unlock()

	// Capacity 1, and a dropped token is not a lost wake-up: a token already
	// sitting there guarantees the flusher will loop again, and when it does it
	// reads the NEWEST pending record rather than the one that queued the token.
	select {
	case s.wake <- struct{}{}:
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for s.doneGen < gen && s.err == nil && !s.closed {
		s.cond.Wait()
	}
	if s.err != nil {
		return s.err
	}
	if s.doneGen < gen {
		return errors.New("raft: storage closed before the write completed")
	}
	return nil
}

func (s *batchedStorage) Load() ([]byte, error) { return s.base.Load() }

// flusher is the only goroutine that touches the underlying storage.
//
// There is no linger timer, deliberately. The batching window is the previous
// flush: everyone who arrives while a write is in progress piles into the next
// one. That needs no tuning and cannot make an idle node slower, which a fixed
// delay would.
func (s *batchedStorage) flusher() {
	defer close(s.done)

	for {
		select {
		case <-s.wake:
		case <-s.quit:
			return
		}

		s.mu.Lock()
		blob, gen := s.pending, s.pendGen
		if gen <= s.doneGen || s.err != nil {
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()

		err := s.base.Save(blob)

		s.mu.Lock()
		s.flushes++
		if err != nil {
			s.err = err
		} else if gen > s.doneGen {
			s.doneGen = gen
		}
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

// Close stops the flusher and releases anyone still waiting. Records that had
// not reached the disk are lost, which is correct: their Saves return an error
// and never reported success to anybody.
func (s *batchedStorage) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()

	close(s.quit)
	<-s.done
}

// stats reports how many Saves were served by how many trips to the disk. A
// ratio near 1 means nothing coalesced.
func (s *batchedStorage) stats() (saves, flushes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves, s.flushes
}
