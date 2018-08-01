package raft

import (
	"fmt"
	"math/rand"
	"time"

	"../labrpc"
)

// 选举部分代码

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

	rf.applyCh = applyCh
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

							// fmt.Println(rf.me, "vote receive from", target, reply)

							// 收到投票，原子操作加1
							rf.mu.Lock()
							rf.voteNum += 1
							rf.mu.Unlock()

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
	fmt.Println(rf.me, "当前票数", rf.voteNum)
	if rf.voteNum > rf.n/2 && rf.state == 1 {

		fmt.Println(rf.me, "当前票数", rf.voteNum)
		fmt.Println(rf.me, "选举成功")

		rf.state = 2
		rf.nextIndex = make([]int, rf.n)
		for i := 0; i < rf.n; i++ {
			rf.nextIndex[i] = len(rf.log) + 1
		}

		// 周期心跳协议
		for {
			//fmt.Println(rf.me, "发送heartbeat")
			if rf.state != 2 {
				break
			}

			time.Sleep(time.Duration(100) * time.Millisecond)

			for i := 0; i < rf.n; i++ {
				if i != rf.me {

					go func(rf *Raft, i int) {
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[i] - 1,
							PrevLogTerm:  0,
							Entries:      nil,
							LeaderCommit: rf.commitIndex,
						}

						// 解决问题：leader已commit, follower日志还没达成一致,但是因为 heartbeat ，导致follower也commit.
						// 所以更新commitIndex的时候再加些限制
						if args.PrevLogIndex > 0 {
							args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
						}

						reply := AppendEntriesReply{
							Term:    rf.currentTerm,
							Success: false,
						}
						ok := rf.sendAppendEntries(i, &args, &reply)

						// 需要重试吗？
						if !ok {
							return
						}

						if !reply.Success {
							rf.currentTerm = reply.Term
							rf.state = 0
						}

					}(rf, i)

				}
			}

		}

	}

}
