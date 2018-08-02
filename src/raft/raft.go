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
	"bytes"
	"fmt"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
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

	timeout int

	// 记录上次收到消息的时间
	// 如果收到的请求不满足 leader 的要求，则不更新时间，即认为暂时没收到来自正确的leader的请求。
	lastReceive int
	n           int
	voteNum     int
	state       int // 0 follower, 1 candidate, 2 leader

	applyCh chan ApplyMsg

	killed bool
}

// a struct to hold information about each log entry.
type Entry struct {
	LogIndex int
	Command  int
	Term     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).

	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
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

	fmt.Println(rf.me, "persist", rf.log, rf.currentTerm, rf.votedFor, rf.commitIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var log []Entry
	var term int
	var voteFor int
	var commitIndex int

	d.Decode(&log)
	d.Decode(&term)
	d.Decode(&voteFor)
	d.Decode(&commitIndex)

	rf.log = log
	rf.currentTerm = term
	rf.votedFor = voteFor
	rf.commitIndex = commitIndex

	fmt.Println(rf.me, "readPersist", rf.log, rf.currentTerm, rf.votedFor, rf.commitIndex)
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

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	//如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
	if args.Term > rf.currentTerm {
		rf.state = 0
		rf.currentTerm = args.Term
	}

	// 如果 votedFor 为空或者就是 candidateId，
	// 这个不太清楚，什么情况下 votedFor 是candidateId还会再投票？
	// votedFor在下一轮选举的时候要清空？
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && checkLog(rf, args) {
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.lastReceive = now()

		fmt.Println(rf.me, "rf.votedFor", rf.votedFor, "args.CandidateId", args.CandidateId)
	}

	reply.Term = rf.currentTerm

}

func checkLog(rf *Raft, args *RequestVoteArgs) bool {
	if len(rf.log) == 0 {
		return true
	}
	lastLogIndex := len(rf.log)
	lastLogTerm := rf.log[lastLogIndex-1].Term

	if lastLogTerm < args.LastLogTerm {
		return true
	}
	if lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex {
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

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 一轮选举结束，应该重新初始化的一些变量
	rf.votedFor = -1

	if args.Entries == nil {
		fmt.Println(rf.me, "收到 heartbeat ", args)
	} else {
		fmt.Println(rf.me, "收到 appendEntries", "rf.log", rf.log, "args.Entries", args.Entries)
	}

	// 如果 term < currentTerm 就返回 false
	if args.Term < rf.currentTerm {
		fmt.Println(rf.me, "args.Term < rf.currentTerm", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastReceive = now()
	// fmt.Println(rf.me, "rf.lastReceive", rf.lastReceive)

	// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
	}

	if args.Entries == nil {
		// heartbeat
		if args.Term >= rf.currentTerm {
			reply.Success = true
		}
	} else {
		// append 日志

		if len(rf.log) == 0 {
			// 初始情况len==0
			reply.Success = true
			// 一开始log为空，直接append
			for i := 0; i < len(args.Entries); i++ {
				rf.log = append(rf.log, args.Entries[i])
			}

			fmt.Println(rf.me, rf.log)
		} else if len(rf.log) < args.PrevLogIndex {
			// 当前log比较少
			reply.Success = false

		} else {

			//如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false

			// 这里的匹配应该是 term和cmd都匹配吧？
			// 难道任期号和索引值相同，command也一定相同？

			// 只有在产生冲突（索引值相同但是任期号不同）的时候才删除follower的日志！不能随便删除（并发时候的错误：log已经和leader一致了，
			// 但是因为前面的append请求后到达，导致已经append的后面的日志又删了。）

			if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {

				reply.Success = false
				rf.log = rf.log[0 : args.PrevLogIndex-1]

				// 每次修改log时持久化
				rf.persist()

				// 冲突了，直接return
				return

			} else {

				reply.Success = true
				for i := 0; i < len(args.Entries); i++ {

					entry := args.Entries[i]
					if entry.LogIndex <= len(rf.log) {
						rf.log[entry.LogIndex-1] = entry
					} else {
						rf.log = append(rf.log, entry)
					}

				}

				fmt.Println(rf.me, rf.log)
			}

		}

	}

	// 不管是哪个if分支，最后肯定有这个
	reply.Term = rf.currentTerm

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// 为什么？
	//fmt.Println(rf.me, "args.LeaderCommit > rf.commitIndex", args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {

		if args.PrevLogIndex > 0 && args.PrevLogIndex <= len(rf.log) && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			return
		}

		fmt.Println(rf.me, "args.LeaderCommit > rf.commitIndex", args.LeaderCommit, rf.commitIndex)
		fmt.Println(rf.me, "args.LeaderCommit > rf.commitIndex", rf.log)

		tmpIndex := rf.commitIndex

		// 一种可能的情况，当前server disconnect，此时 leader仍能commit,LeaderCommit还在增加，
		// 等当前server再次connect的时候，commitIndex比较小，而且log比较少。

		rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
		// 只能到PrevLogIndex， 因为 PrevLogIndex 之后的日志可能还未达成一致

		if args.PrevLogIndex >= tmpIndex {
			rf.commitIndex = Min(rf.commitIndex, args.PrevLogIndex)
		}

		// commit之后告诉tester，用于测试

		for i := tmpIndex + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-1].Command,
				CommandIndex: i,
			}
			fmt.Println(rf.me, "send to channel", applyMsg, "rf.log", rf.log, "i", i)

			rf.applyCh <- applyMsg

		}

	}

	// 每次修改log时持久化
	rf.persist()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	fmt.Println("Kill!!")
	rf.killed = true
}

// Election
