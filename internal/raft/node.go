package raft

import "sync"

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

// Log entry is one command in the replicated log
type LogEntry struct {
	Term    int    //creation term
	Command []byte //Client Command
}

// Node is a single RAFT server
type Node struct {
	mu sync.Mutex //guards every field below

	id          int        // Node's ID
	peers       []int      //ids of all the other nodes in the cluster
	transport   Transport  //how this node reaches peers
	currentTerm int        //current election term
	votedFor    int        //current vote
	log         []LogEntry //replicated commands

	commitIndex int   //last commited
	lastApplied int   //last executed
	state       State //current role
}

// New node returns a node in the state every RAFT server starts in: follower
func NewNode(id int, peers []int, transport Transport) *Node {
	return &Node{
		id:          id,
		peers:       peers,
		transport:   transport,
		currentTerm: 0,
		votedFor:    None,
		log:         []LogEntry{{Term: 0}},
		commitIndex: 0,
		lastApplied: 0,
		state:       Follower,
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
