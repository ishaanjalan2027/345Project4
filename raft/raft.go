package raft

import (
	"bytes"
	"cs345/labgob"
	"cs345/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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
	io_writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(io_writer)

	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)

	rf.persister.SaveRaftState(io_writer.Bytes())
}

// restore previously persisted state
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	io_reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(io_reader)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	decoder.Decode(&currentTerm)
	decoder.Decode(&votedFor)
	decoder.Decode(&log)

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
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
			rf.persist()
		}

		reply.Term = rf.currentTerm

		myLastLogIndex := len(rf.log) - 1
		myLastLogTerm := rf.log[myLastLogIndex].Term
		candidateLogOK := false
		if args.LastLogTerm > myLastLogTerm {
			candidateLogOK = true
		} else if args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex {
			candidateLogOK = true
		}

		// only vote if we haven't voted yet or we already voted for this person
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && candidateLogOK {
			rf.votedFor = args.CandidateID
			rf.persist()
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
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the reply struct for AppendEntries
// Updated with extra fields per section 5.3 because 4BChurn is too slow
type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictTerm int
	FirstIndex   int
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
		// valid leader term so reset the election timer
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond

		// update term if we're behind
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}

		// become follower
		rf.currentRole = FOLLOWER

		if args.PrevLogIndex >= len(rf.log) {
			reply.Term = rf.currentTerm
			reply.FirstIndex = len(rf.log)
			reply.ConflictTerm = -1
			return
		}

		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

			conflictIndex := args.PrevLogIndex
			for conflictIndex > 0 && rf.log[conflictIndex-1].Term != reply.ConflictTerm {
				conflictIndex = conflictIndex - 1
			}

			reply.FirstIndex = conflictIndex
			return
		}

		for i := 0; i < len(args.Entries); i++ {
			logIndex := args.PrevLogIndex + 1 + i

			if logIndex < len(rf.log) {
				if rf.log[logIndex].Term != args.Entries[i].Term {
					rf.log = rf.log[:logIndex]
					rf.log = append(rf.log, args.Entries[i:]...)
					rf.persist()
					break
				}
			} else {
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		}

		if args.LeaderCommit > rf.commitIndex {
			lastLogIndex := len(rf.log) - 1
			if args.LeaderCommit < lastLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastLogIndex
			}
			rf.sendCommittedEntries()
		}

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
	rf.persist()

	// reset the timer
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond

	// save the term we're running in so goroutines can check later
	termWeStartedIn := rf.currentTerm
	myID := rf.me
	totalPeers := len(rf.peers)
	majorityNeeded := totalPeers/2 + 1

	// build the args struct to send to everyone
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	voteArgs := &RequestVoteArgs{
		Term:         termWeStartedIn,
		CandidateID:  myID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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
				rf.persist()
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
					lastLogIndex := len(rf.log) - 1
					for j := 0; j < len(rf.peers); j++ {
						rf.nextIndex[j] = lastLogIndex + 1
						rf.matchIndex[j] = 0
					}
					rf.matchIndex[rf.me] = lastLogIndex
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
		leaderID := rf.me
		numPeers := len(rf.peers)

		rf.mu.Unlock()

		// send a heartbeat to each follower
		for i := 0; i < numPeers; i++ {
			if i == leaderID {
				continue
			}

			go func(server int) {
				rf.syncFollower(server)
			}(i)
		}

		// wait 100ms before next heartbeat since we cant do more than 10 per second
		time.Sleep(100 * time.Millisecond)
	}
}

// syncFollower sends the leader's log entries to one follower
func (rf *Raft) syncFollower(server int) {
	rf.mu.Lock()

	if rf.currentRole != LEADER {
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	leaderID := rf.me
	nextIndex := rf.nextIndex[server]
	if nextIndex < 1 {
		nextIndex = 1
	}
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := make([]LogEntry, len(rf.log[nextIndex:]))
	copy(entries, rf.log[nextIndex:])

	argsToSend := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	rpcReply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, argsToSend, rpcReply)
	if ok == false {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// step down if we see a higher term
	if rpcReply.Term > rf.currentTerm {
		rf.currentTerm = rpcReply.Term
		rf.currentRole = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
		return
	}

	if rf.currentRole != LEADER || rf.currentTerm != currentTerm {
		return
	}

	if rpcReply.Success == true {
		rf.matchIndex[server] = argsToSend.PrevLogIndex + len(argsToSend.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		for index := len(rf.log) - 1; index > rf.commitIndex; index-- {
			if rf.log[index].Term != rf.currentTerm {
				continue
			}

			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= index {
					count = count + 1
				}
			}

			if count >= len(rf.peers)/2+1 {
				rf.commitIndex = index
				rf.sendCommittedEntries()
				break
			}
		}
	} else {
		if rpcReply.ConflictTerm == -1 {
			rf.nextIndex[server] = rpcReply.FirstIndex
		} else {
			lastIndex := -1
			for i := len(rf.log) - 1; i > 0; i-- {
				if rf.log[i].Term == rpcReply.ConflictTerm {
					lastIndex = i
					break
				}
			}

			if lastIndex >= 0 {
				rf.nextIndex[server] = lastIndex + 1
			} else {
				rf.nextIndex[server] = rpcReply.FirstIndex
			}
		}

		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
}

func (rf *Raft) sendCommittedEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied = rf.lastApplied + 1
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
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
	atomic.StoreInt32(&rf.dead, 1)
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
		newEntry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		}
		rf.log = append(rf.log, newEntry)
		rf.persist()
		index = len(rf.log) - 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1

		for logIndex := len(rf.log) - 1; logIndex > rf.commitIndex; logIndex-- {
			if rf.log[logIndex].Term != rf.currentTerm {
				continue
			}

			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= logIndex {
					count = count + 1
				}
			}

			if count >= len(rf.peers)/2+1 {
				rf.commitIndex = logIndex
				rf.sendCommittedEntries()
				break
			}
		}

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.syncFollower(i)
			}
		}
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
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
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
