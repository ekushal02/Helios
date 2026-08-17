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

	id          int          // Node's ID
	peers       []int        //ids of all the other nodes in the cluster
	transport   Transport    //how this node reaches peers
	logger      *slog.Logger //nil until SetLogger; lg() falls back to discard
	currentTerm int          //current election term
	votedFor    int          //current vote
	log         []LogEntry   //replicated commands

	commitIndex int   //last commited
	lastApplied int   //last executed
	state       State //current role

	leaderID   int
	nextIndex  map[int]int // guess: where to send next. Optimistic.
	matchIndex map[int]int // proven: replicated up to here. Pessimistic.

	electionDeadline time.Time     //election starting timer
	rng              *rand.Rand    //randomised election timeout
	stopCh           chan struct{} //shut ticker down

	stopOnce sync.Once //stopOnce makes Stop safe to call more than once.
}

// New node returns a node in the state every RAFT server starts in: follower
func NewNode(id int, peers []int, transport Transport, seed int64) *Node {
	return &Node{
		id:          id,
		peers:       peers,
		transport:   transport,
		logger:      discardLogger,
		currentTerm: 0,
		votedFor:    None,
		leaderID:    None,
		log:         []LogEntry{{Term: 0}},
		commitIndex: 0,
		lastApplied: 0,
		state:       Follower,
		rng:         rand.New(rand.NewSource(seed)),
		stopCh:      make(chan struct{}),
	}
}

// lastLogIndex returns the index of the final entry
func (n *Node) lastLogIndex() int {
	return len(n.log) - 1
}

// lastLogTerm return the term of the final entry
func (n *Node) lastLogTerm() int {
	return n.log[n.lastLogIndex()].Term
}
