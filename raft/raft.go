package raft

import (
	"cs345/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

// ApplyMsg is used to send committed log entries to the state machine
// the tester uses this
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry holds a single entry in the log
type LogEntry struct {
	Command interface{}
	Term    int
}

// these are the 3 possible states a raft server can be in
// 0 = follower, 1 = candidate, 2 = leader
const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

// Raft is the main struct that holds all the state for a single raft peer
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // channel to send committed entries

	// persistent state (would need to be saved to disk in project 4)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// only used by leader
	nextIndex  []int
	matchIndex []int

	// my added fields for election stuff
	currentRole     int       // 0=follower 1=candidate 2=leader
	lastHeartbeat   time.Time // tracks when we last heard from the leader
	electionTimeout time.Duration
	dead            int32 // set to 1 by Kill()

	voteMu sync.Mutex // separate lock just for counting votes
}

// GetState returns the current term and whether this server thinks it is the leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int = rf.currentTerm
	var isLeader bool = false

	if rf.currentRole == LEADER {
		isLeader = true
	}

	return term, isLeader
}

// save Raft's persistent state to stable storage
// not needed for project 3
func (rf *Raft) persist() {
}

// restore previously persisted state
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
}

// RequestVoteArgs holds the arguments for a RequestVote RPC
// field names must start with capital letters for RPC to work!!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply holds the reply for a RequestVote RPC
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote is the RPC handler that decides whether to give a vote to a candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// default is to not grant the vote
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// reject if their term is old
	if args.Term < rf.currentTerm {
		return
	} else {
		// if we see a higher term we need to update ourselves and become follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.currentRole = FOLLOWER
			rf.votedFor = -1
		}

		reply.Term = rf.currentTerm

		// only vote if we haven't voted yet or we already voted for this person
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true

			// reset timer since we granted a vote
			rf.lastHeartbeat = time.Now()
			rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
		}
	}
}

// AppendEntriesArgs are the arguments for AppendEntries RPC
// we only use Term and LeaderID for now (no log entries in project 3)
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

// AppendEntriesReply is the reply struct for AppendEntries
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries handles heartbeats from the leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// if leader is from old term ignore them
	if args.Term < rf.currentTerm {
		return
	} else {
		// valid heartbeat so reset the election timer
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond

		// update term if we're behind
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		// become follower
		rf.currentRole = FOLLOWER

		reply.Term = rf.currentTerm
		reply.Success = true
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries sends an AppendEntries RPC (heartbeat) to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// startElection is called when the election timer fires
// this server becomes a candidate and asks everyone for their vote
func (rf *Raft) startElection() {
	// become a candidate and increment term
	rf.currentRole = CANDIDATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me

	// reset the timer
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond

	// save the term we're running in so goroutines can check later
	termWeStartedIn := rf.currentTerm
	myID := rf.me
	totalPeers := len(rf.peers)
	majorityNeeded := totalPeers/2 + 1

	// build the args struct to send to everyone
	voteArgs := &RequestVoteArgs{
		Term:        termWeStartedIn,
		CandidateID: myID,
	}

	// start with 1 vote
	numberOfVotesReceived := 1

	// send RequestVote to every other server
	for i := 0; i < totalPeers; i++ {
		if i == myID {
			// skip ourselves
			continue
		}

		// send each RPC in its own goroutine so we don't block
		go func(serverIndex int) {
			voteReply := &RequestVoteReply{}
			rpcSucceeded := rf.sendRequestVote(serverIndex, voteArgs, voteReply)

			if rpcSucceeded == false {
				// network failed or server down, just ignore
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// if someone has a higher term we need to step down
			if voteReply.Term > rf.currentTerm {
				rf.currentTerm = voteReply.Term
				rf.currentRole = FOLLOWER
				rf.votedFor = -1
				rf.lastHeartbeat = time.Now()
				rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
				return
			}

			// make sure we're still in the same election (term might have changed)
			if rf.currentRole != CANDIDATE || rf.currentTerm != termWeStartedIn {
				return
			}

			// count the vote if they said yes
			if voteReply.VoteGranted == true {
				numberOfVotesReceived = numberOfVotesReceived + 1

				// check if we have enough votes to win
				if numberOfVotesReceived >= majorityNeeded && rf.currentRole == CANDIDATE {
					// we won! become the leader
					rf.currentRole = LEADER
					// start sending heartbeats to everyone
					go rf.sendHeartbeats()
				}
			}
		}(i)
	}
}

// sendHeartbeats is run by the leader to keep sending heartbeats so followers
// don't start elections. runs in a loop until we're no longer leader
func (rf *Raft) sendHeartbeats() {
	for {
		// stop if killed
		if atomic.LoadInt32(&rf.dead) == 1 {
			return
		}

		rf.mu.Lock()

		// stop if we're not leader anymore
		if rf.currentRole != LEADER {
			rf.mu.Unlock()
			return
		}

		// grab what we need while holding the lock
		currentTerm := rf.currentTerm
		leaderID := rf.me
		numPeers := len(rf.peers)

		rf.mu.Unlock()

		// send a heartbeat to each follower
		for i := 0; i < numPeers; i++ {
			if i == leaderID {
				continue
			}

			go func(server int) {
				heartbeatArgs := &AppendEntriesArgs{
					Term:     currentTerm,
					LeaderID: leaderID,
				}
				heartbeatReply := &AppendEntriesReply{}

				worked := rf.sendAppendEntries(server, heartbeatArgs, heartbeatReply)
				if worked == false {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// step down if we see a higher term
				if heartbeatReply.Term > rf.currentTerm {
					rf.currentTerm = heartbeatReply.Term
					rf.currentRole = FOLLOWER
					rf.votedFor = -1
					rf.lastHeartbeat = time.Now()
					rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
				}
			}(i)
		}

		// wait 100ms before next heartbeat since we cant do more than 10 per second
		time.Sleep(100 * time.Millisecond)
	}
}

// electionTimer runs in the background and triggers elections when
// we haven't heard from the leader for too long
func (rf *Raft) electionTimer() {
	for {
		// check every 10ms
		time.Sleep(10 * time.Millisecond)

		// stop looping if killed
		if atomic.LoadInt32(&rf.dead) == 1 {
			return
		}

		rf.mu.Lock()

		// only start an election if we're not the leader
		if rf.currentRole != LEADER {
			timeSinceLastHeartbeat := time.Since(rf.lastHeartbeat)

			if timeSinceLastHeartbeat >= rf.electionTimeout {
				// timeout! start an election
				rf.startElection()
			}
		}

		rf.mu.Unlock()
	}
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Start is called to add a new command to the log
// returns false if this server is not the leader
// (log replication is implemented in project 4)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var index int = -1
	var term int = rf.currentTerm
	var isLeader bool = false

	if rf.currentRole == LEADER {
		isLeader = true
	}

	return index, term, isLeader
}

// Make creates a new Raft server and starts it
// this is called by the tester to set up a new peer
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// initialize all the state
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 means hasn't voted for anyone yet
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentRole = FOLLOWER // start as follower

	// set a random election timeout so all servers don't fire at once
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond

	// load any saved state from before a crash (project 4)
	rf.readPersist(persister.ReadRaftState())

	// start the background goroutine that watches for election timeouts
	go rf.electionTimer()

	return rf
}
