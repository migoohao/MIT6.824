package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

type Tick int
type Role int

const ElectionTimeoutMin int64 = 250
const ElectionTimeoutMax int64 = 300
const HeartbeatsInterval int64 = 120
const VoteNon = -1

const (
	TIMEOUT Tick = iota
	OK
)

const (
	WORKER Role = iota
	LEADER
	CANDIDATE
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	tick        Tick
	role        Role

	log         []LogEntry
	commitIndex int
	//lastApplied int

	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("term:%v %v receive vote request from %v Term %v", rf.currentTerm, rf.me, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("term:%v: %v refuse to vote for %v, Term %v", rf.currentTerm, rf.me, args.CandidateId, args.Term)
		return
	}
	lastLogIndex := len(rf.log) - 1
	if args.Term > rf.currentTerm || rf.voteFor == VoteNon || rf.voteFor == args.CandidateId ||
		args.LastLogTerm > rf.log[lastLogIndex].Term || (args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex > lastLogIndex) {
		rf.role = WORKER
		rf.tick = OK
		rf.voteFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		DPrintf("term:%v: %v vote for %v, Term %v", rf.currentTerm, rf.me, args.CandidateId, args.Term)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logLen := len(rf.log)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = WORKER
		rf.tick = OK
		rf.voteFor = VoteNon
		reply.Term = rf.currentTerm
		reply.Success = rf.updateLog(args)
		DPrintf("term:%v %v receive heartbeats request from %v Term %v leaderCommit %v pLogIndex %v plogTerm %v entries %v success %v",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), reply.Success)
		return
	}
	if rf.role == LEADER || args.Term < rf.currentTerm || logLen <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("term:%v %v receive heartbeats request from %v Term %v leaderCommit %v pLogIndex %v plogTerm %v entries %v success %v",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), reply.Success)
		return
	}
	rf.tick = OK
	rf.role = WORKER
	reply.Term = rf.currentTerm
	reply.Success = rf.updateLog(args)
	DPrintf("term:%v %v receive heartbeats request from %v Term %v leaderCommit %v pLogIndex %v plogTerm %v entries %v success %v",
		rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), reply.Success)
}

func (rf *Raft) updateLog(args *AppendEntriesArgs) bool {
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return false
	}
	rf.log = rf.log[:args.PrevLogIndex+1]
	for _, log := range args.Entries {
		rf.log = append(rf.log, log)
	}
	oldCommit := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log)-1 < args.LeaderCommit {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	for i := oldCommit + 1; i <= rf.commitIndex; i++ {
		DPrintf("term:%v %v commit command %v", rf.currentTerm, rf.me, rf.log[i].Command)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
	}
	return true
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		isLeader = false
		return index, term, isLeader
	}
	term = rf.currentTerm
	log := LogEntry{Command: command, Term: term}
	rf.log = append(rf.log, log)
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	index = len(rf.log) - 1
	DPrintf("term:%v %v return Start true, command %v index %v", term, rf.me, command, index)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTimeout() {
	for {
		ms := ElectionTimeoutMin + rand.Int63()%(ElectionTimeoutMax-ElectionTimeoutMin)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.tick == OK {
			rf.tick = TIMEOUT
			rf.mu.Unlock()
			continue
		}
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.role = CANDIDATE
		lastLogIndex := len(rf.log) - 1
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.log[lastLogIndex].Term,
		}
		rf.mu.Unlock()
		rf.sendElection(args)
	}
}

func (rf *Raft) turnToWorker() {
	DPrintf("term:%v %v turn to worker", rf.currentTerm, rf.me)
	rf.role = WORKER
	rf.voteFor = VoteNon
	rf.tick = OK
}

func (rf *Raft) sendElection(args RequestVoteArgs) {
	DPrintf("term:%v %v want to be leader, start election", rf.currentTerm, rf.me)
	var vote int
	vch := make(chan int, len(rf.peers)-1)
	for i := range rf.peers {
		if i != rf.me {
			go rf.asyncRequestVote(i, args, &vote, vch)
		}
	}
	// must have one reply at least
	for i := 0; i < len(rf.peers)/2; i++ {
		<-vch
	}
}

func (rf *Raft) sendHeartbeats() {
	for {
		time.Sleep(time.Duration(HeartbeatsInterval) * time.Millisecond)
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			continue
		}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		rf.tick = OK
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.PrevLogTerm = 0
				args.PrevLogIndex = 0
				args.Entries = rf.log[rf.nextIndex[i]:]
				args.LeaderCommit = rf.commitIndex
				preLogIndex := rf.nextIndex[i] - 1
				if preLogIndex > 0 {
					args.PrevLogIndex = preLogIndex
					args.PrevLogTerm = rf.log[preLogIndex].Term
				}
				rf.mu.Unlock()
				go rf.asyncHeartbeats(i, args)
			}
		}
	}
}

func (rf *Raft) asyncHeartbeats(i int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.turnToWorker()
			return
		}
		if reply.Success {
			rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[i] = rf.matchIndex[i] + 1
		} else {
			term := rf.log[rf.nextIndex[i]-1].Term
			for k := rf.nextIndex[i] - 1; k > 0; k-- {
				if rf.log[k].Term != term {
					rf.nextIndex[i] = k + 1
					break
				}
			}
		}
		rf.updateCommit()
	}
}

func (rf *Raft) updateCommit() {
	for i := len(rf.log) - 1; i > 0 && rf.commitIndex < i; i-- {
		nextCommit := i
		count := 0
		for _, match := range rf.matchIndex {
			if nextCommit <= match {
				count++
			}
		}
		if count > len(rf.matchIndex)/2 {
			for commit := rf.commitIndex + 1; commit <= nextCommit; commit++ {
				DPrintf("term:%v %v commit command %v", rf.currentTerm, rf.me, rf.log[commit].Command)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[commit].Command,
					CommandIndex: commit,
				}
			}
			rf.commitIndex = nextCommit
		}
	}
}

func (rf *Raft) asyncRequestVote(i int, args RequestVoteArgs, vote *int, vch chan int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(i, &args, &reply)
	vch <- 1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.turnToWorker()
			return
		}
		if reply.VoteGranted && rf.role == CANDIDATE {
			*vote++
			if *vote >= len(rf.peers)/2 {
				rf.tick = OK
				rf.role = LEADER
				for i := range rf.nextIndex {
					rf.nextIndex[i] = 1
					rf.matchIndex[i] = 0
					if i == rf.me {
						rf.matchIndex[i] = len(rf.log) - 1
						rf.nextIndex[i] = rf.matchIndex[i] + 1
					}
				}
				DPrintf("term:%v: %v win, claim to be leader", rf.currentTerm, rf.me)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially hol//log start from index 1, so index 0 is uselessds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.voteFor = VoteNon
	rf.tick = TIMEOUT
	rf.role = WORKER
	//log start from index 1, so index 0 is useless
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.electionTimeout()
	go rf.sendHeartbeats()

	return rf
}
