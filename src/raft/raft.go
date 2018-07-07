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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"../labrpc"
)

// import "bytes"
// import "labgob"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	timeout     int
	lastReceive int // 记录上次收到消息的时间
	n           int
	voteNum     int
	state       int // 0 follower, 1 candidate, 2 leader
}

// a struct to hold information about each log entry.
type Entry struct {
	LogIndex int
	Conmand  string
	Term     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm

	//if rf.state == 2 {
	//	isleader = true
	//} else {
	//	isleader = false
	//}

	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}

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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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
	reply.Term = rf.currentTerm

	if rf.currentTerm < args.Term {

		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.lastReceive = now()

		//如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
		rf.state = 0
		rf.currentTerm = args.Term

	} else if rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && checkLog(rf, args) {
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.lastReceive = now()

		//如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
		rf.state = 0
		rf.currentTerm = args.Term
	} else {
		reply.VoteGranted = false
	}

	fmt.Println("after vote", rf.me, "term", rf.currentTerm)
}

func checkLog(rf *Raft, args *RequestVoteArgs) bool {
	if len(rf.log) == 0 {
		return true
	}
	lastLogIndex := len(rf.log)
	lastLogTerm := rf.log[lastLogIndex].Term
	if lastLogTerm < args.LastLogTerm {
		return true
	}
	if lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogTerm {
		return true
	}
	return false
}

func now() int {
	time := (int)(time.Now().UnixNano() / int64(time.Millisecond))
	return time
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

// 发送心跳请求
//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 心跳协议

	fmt.Println(rf.me, "收到心跳")

	if args.Entries == nil {
		// heartbeat
		rf.lastReceive = now()
		if args.Term <= rf.currentTerm {

			reply.Success = false
			reply.Term = rf.currentTerm

		} else if args.Term > rf.currentTerm {
			//如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
			rf.currentTerm = args.Term
			rf.state = 0

			reply.Success = true

		}

	} else {
		//	append 日志

	}

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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
//

// 设置超时时间 200 ~ 350 ， 大于题目中说的100就好吧（10 heartbeats per second）
func randTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return 300 + (rand.Int() % 300)
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0

	rf.timeout = randTimeout()
	rf.lastReceive = 0
	rf.n = len(peers)
	rf.votedFor = -1

	fmt.Println("timeout: ", rf.timeout)

	go func(rf *Raft) {
		for {
			time.Sleep(time.Duration(rf.timeout) * time.Millisecond)

			if (now()-rf.lastReceive > rf.timeout && rf.state == 0) || rf.state == 1 {

				fmt.Println(rf.me, "开始选举")

				// 超时重新开始选举
				rf.currentTerm += 1
				rf.voteNum = 1
				rf.timeout = randTimeout()
				rf.votedFor = -1
				rf.state = 1

				fmt.Println(rf.me, "当前状态:", "term", rf.currentTerm, "state", rf.state)

				for i := 0; i < len(peers); i++ {
					if i != rf.me {
						// 新开线程，并行发送RequestVote
						go func(rf *Raft, target int) {
							lastLogIndex := len(rf.log)

							lastLogTerm := 0
							if lastLogIndex != 0 {
								lastLogTerm = rf.log[lastLogIndex-1].Term
							}

							args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
							reply := RequestVoteReply{}

							ok := rf.sendRequestVote(target, &args, &reply)

							if !ok || !reply.VoteGranted {
								return
							}

							fmt.Println(rf.me, "vote receive from", target, reply)

							// 收到投票，原子操作加1
							rf.mu.Lock()
							rf.voteNum += 1
							rf.mu.Unlock()
							fmt.Println(rf.me, "当前票数", rf.voteNum)

							rf.checkElection()

						}(rf, i)
					}
				}
			}

		}

	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) checkElection() {
	if rf.voteNum > rf.n/2 && rf.state == 1 {
		fmt.Println(rf.me, "选举成功")
		rf.state = 2
		// 选举成功
		// 发送不包含 log 的心跳协议

		// 周期心跳协议
		for {

			if rf.state != 2 {
				break
			}

			time.Sleep(time.Duration(100) * time.Millisecond)
			item, isleader := rf.GetState()
			fmt.Println(rf.me, "当前状态", "term", item, "isleader", isleader, "state", rf.state)

			for j := 0; j < rf.n; j++ {
				if j != rf.me {
					fmt.Println(rf.me, "sendAppendEntries", j)
					go func(rf *Raft, j int) {
						appendArgs := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: 0,
							PrevLogTerm:  0,
							Entries:      nil,
							LeaderCommit: 0,
						}
						appendReply := AppendEntriesReply{
							Term:    0,
							Success: false,
						}
						rf.sendAppendEntries(j, &appendArgs, &appendReply)

						//fmt.Println( rf.me, "sendAppendEntries", j, "结果", ok)

						// if ok {
						// 	if !appendReply.Success {
						// 		rf.currentTerm = appendReply.Term
						// 		rf.state = 0
						// 	}
						// }

					}(rf, j)
				}
			}

		}

	}

}
