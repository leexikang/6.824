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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// import "bytes"
// import "../labgob"

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

//State is the raft current state
type State string

const (
	Leader    = "Leader"
	Candidate = "candidate"
	Follower  = "follower"
)

// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	isLeader      bool
	state         State
	currentTerm   int
	voteFor       int
	lastReceiveAt time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
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
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = rf.currentTerm
	rf.convertToFollower(args.Term)
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
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
		if rf.killed() {
			return
		}
		min := 1000
		max := 2000
		rand.Seed(time.Now().UnixNano())
		timeout := rand.Intn(max-(min+1)) + min
		startTime := time.Now()
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		rf.mu.Lock()
		DPrintf("[%d] started election after %d at term %d", rf.me, timeout, rf.currentTerm)
		if rf.lastReceiveAt.Before(startTime) {
			if rf.state != Leader {
				DPrintf("[%d] %v - %v", rf.me, startTime, rf.lastReceiveAt)
				go rf.startElection()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	DPrintf("[%d] starting the election at term %d", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.convertToCandidate()
	rf.mu.Unlock()
	done := false
	DPrintf("[%d] started the term %d", rf.me, rf.currentTerm)
	vote := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			if rf.killed() {
				return
			}
			reply := RequestVoteReply{}
			rf.mu.Lock()
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
			rf.mu.Unlock()
			DPrintf("[%d] send RequestVoteRPC  to %d at term %d", rf.me, server, rf.currentTerm)
			ok := rf.sendRequestVote(server, &args, &reply)
			DPrintf("[%d] send RequestVoteRPC to %d success, result - %t", rf.me, server, ok)
			if !ok {
				return
			}
			if !reply.VoteGranted {
				DPrintf("[%d] got reject from %d", rf.me, server)
				rf.mu.Lock()
				rf.convertToFollower(rf.currentTerm)
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			DPrintf("[%d] got vote from %d", rf.me, server)
			vote++
			DPrintf("[%d] got %d votes", rf.me, vote)
			rf.mu.Unlock()

			rf.mu.Lock()
			if !done && rf.gotMajority(vote) {
				if rf.currentTerm > reply.Term {
					DPrintf("[%d] currentTerm %d reply term %d", rf.me, rf.currentTerm, reply.Term)
					rf.convertToLeader()
					done = true
					go rf.sendHeartBeats()
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

type Entry struct {
	Term  int
	Value interface{}
}

type AppendEntriesArgs struct {
	Term int
	/* LeaderId     int*/
	//PrevLogIndex int
	//PrevLogTerm  int
	//Entries      []Entry
	/*LeaderCommit int*/
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	}
	rf.lastReceiveAt = time.Now()
}

func (rf *Raft) gotMajority(vote int) bool {
	return vote >= int(math.Round(float64(len(rf.peers))/2.0))
}

func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	DPrintf("[%d] convertToFollower at term %d", rf.me, rf.currentTerm)
	rf.voteFor = -1
	rf.lastReceiveAt = time.Now()
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	DPrintf("[%d] convertToCandidate after term [%d]", rf.me, rf.currentTerm)
	rf.currentTerm++
	DPrintf("[%d] become candidate at term[%d]", rf.me, rf.currentTerm)
	rf.lastReceiveAt = time.Now()
	rf.voteFor = rf.me
}
func (rf *Raft) convertToLeader() {
	rf.state = Leader
	DPrintf("[%d] becomes leader", rf.me)
	rf.lastReceiveAt = time.Now()
}

func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			go func(server int) {
				if server == rf.me {
					return
				}
				args := AppendEntriesArgs{Term: rf.currentTerm}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				if ok {
					DPrintf("[%d] heartbeat sent to %d", rf.me, server)
				}
			}(i)
		}
		time.Sleep(800 * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
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
	rf.isLeader = false
	rf.convertToFollower(0)
	// Your initialization code here (2A, 2B, 2C).
	go rf.electionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
