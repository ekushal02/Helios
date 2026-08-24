package raft

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// State is the role a node currently occupies
type State int

const (
	Follower State = iota //starts here
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown" //invalid state
	}
}

const None = -1 //no vote yet

// Node is a single RAFT server
type Node struct {
	mu sync.Mutex //guards every field below

	id           int          // Node's ID
	peers        []int        //ids of all the other nodes in the cluster
	transport    Transport    //how this node reaches peers
	logger       *slog.Logger //nil until SetLogger; lg() falls back to discard
	currentTerm  int          //current election term
	votedFor     int          //current vote
	log          []LogEntry   //replicated commands
	storage      Storage      // where currentTerm, votedFor and log survive a restart
	persistDirty bool         // set by markDirty, cleared by persistIfDirty

	lastIncludedIndex int
	lastIncludedTerm  int

	snapshotThreshold int           // entries above the floor before a signal
	snapshotNotify    chan struct{} // capacity 1: "the log has outgrown the threshold"

	commitIndex int   //last commited
	lastApplied int   //last executed
	state       State //current role

	leaderID   int
	nextIndex  map[int]int // guess: where to send next. Optimistic.
	matchIndex map[int]int // proven: replicated up to here. Pessimistic.

	replicatingTerm map[int]int       // peer -> term of the round in flight, 0 when idle
	replPending     map[int]bool      // peer -> a trigger arrived while a round was out
	snapshotSentAt  map[int]time.Time // peer -> when an image was last attempted
	noCoalesce      bool              // measurement only: one fan-out per Submit, as it was

	lastContact map[int]time.Time // lastContact records, per peer, the SEND time of the most recent message that peer answered.

	electionDeadline time.Time     //election starting timer
	rng              *rand.Rand    //randomised election timeout
	stopCh           chan struct{} //shut ticker down

	applyCh     chan ApplyMsg // committed entries, in index order, one consumer
	applyNotify chan struct{} // capacity 1: "commitIndex moved"
	applierDone chan struct{} // closed when applier() returns

	servers     []int // every voter, this node included if it is one
	inConfig    bool  // whether this node is a member of servers
	configIndex int   // index of the entry that put servers in force, or the floor
	baseServers []int // the configuration as of the snapshot floor

	pendingSnapshot *Snapshot

	stopOnce sync.Once //stopOnce makes Stop safe to call more than once.
}

// New node returns a node in the state every RAFT server starts in: follower
func NewNode(id int, peers []int, transport Transport, seed int64) *Node {
	n := &Node{
		id:                id,
		peers:             peers,
		transport:         transport,
		logger:            discardLogger,
		currentTerm:       0,
		votedFor:          None,
		leaderID:          None,
		log:               []LogEntry{{Term: 0}},
		storage:           NewMemoryStorage(),
		replicatingTerm:   make(map[int]int),
		replPending:       make(map[int]bool),
		snapshotSentAt:    make(map[int]time.Time), // measurement only: one fan-out per Submit, as it was
		commitIndex:       0,
		lastApplied:       0,
		state:             Follower,
		rng:               rand.New(rand.NewSource(seed)),
		stopCh:            make(chan struct{}),
		snapshotThreshold: defaultSnapshotThreshold,
		snapshotNotify:    make(chan struct{}, 1),
	}

	n.setConfiguration(append(append([]int(nil), peers...), id), 0)
	n.baseServers = append([]int(nil), n.servers...)
	n.initApplier()

	return n
}
