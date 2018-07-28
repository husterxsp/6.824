package raft

import (
	"fmt"
	"time"
)

// 发起一次log共识

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
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	index = len(rf.log) + 1

	cmd := command.(int)
	entry := Entry{
		LogIndex: len(rf.log) + 1,
		Command:  cmd,
		Term:     rf.currentTerm,
	}

	rf.log = append(rf.log, entry)

	fmt.Println(rf.me, "append", cmd, "to itself")

	nCommit := 0
	// 复制log
	for i := 0; i < rf.n; i++ {
		if i == rf.me {
			continue
		}

		go func(rf *Raft, i int) {
		Loop:

			fmt.Println(rf.me, "start append", cmd, "to", i)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  0,
				Entries:      rf.log[rf.nextIndex[i]-1:],
				LeaderCommit: rf.commitIndex,
			}
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
			}

			reply := AppendEntriesReply{
				Term:    rf.currentTerm,
				Success: false,
			}

			ok := rf.sendAppendEntries(i, &args, &reply)

			if !ok {
				return
			}

			fmt.Println("reply", reply)

			if reply.Success {
				// append成功
				nCommit++

				rf.nextIndex[i] += len(args.Entries)

			} else {
				// 失败的两种可能
				if reply.Term > rf.currentTerm {
					// 当前任期小，转换为follower
					rf.currentTerm = reply.Term
					rf.state = 0
				} else {
					// follower的日志太短，减小index重试
					rf.nextIndex[i] = Max(rf.nextIndex[i]-2, 1)
					fmt.Println("重试？")
					goto Loop

				}

			}

		}(rf, i)
	}

	// 检查log是否复制成功
	go func(rf *Raft, cmd int) {
		for {
			fmt.Println("nCommit", nCommit)
			if rf.state != 2 {
				break
			}
			if nCommit >= rf.n/2 {
				// commit成功
				rf.commitIndex++
				// 告知tester
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      cmd,
					CommandIndex: rf.commitIndex,
				}
				rf.applyCh <- applyMsg
				break
			}

			time.Sleep(10 * time.Millisecond)
		}
	}(rf, cmd)

	return index, term, isLeader
}
