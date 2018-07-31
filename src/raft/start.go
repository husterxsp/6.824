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

	// 这里需要加锁，可能出现同时append，多个线程同时写 rf.log
	rf.mu.Lock()

	// 这里就需要加，因为多个线程同时访问，最终可能导致写入的index相同
	index = len(rf.log) + 1

	cmd := command.(int)
	entry := Entry{
		LogIndex: len(rf.log) + 1,
		Command:  cmd,
		Term:     rf.currentTerm,
	}

	rf.log = append(rf.log, entry)

	fmt.Println(rf.me, "append", cmd, "to itself")
	fmt.Println(rf.me, rf.log)

	rf.mu.Unlock()


	// 当前有nCommit个server已经 append数据，初始为1，表示当前leader已append
	nCommit := 1
	// 复制log
	for i := 0; i < rf.n; i++ {
		if i == rf.me {
			continue
		}

		go func(rf *Raft, i int, nextIndex int) {
		Loop:

			// 还是并发导致的错误，
			//if rf.nextIndex[i] > len(rf.log) {
			//	return
			//}

			rf.nextIndex[i] = Min(rf.nextIndex[i], len(rf.log)+1)

			fmt.Println(rf.me, "start append", cmd, "to", i)

			fmt.Println(rf.me, "i", i, "rf.nextIndex[i], rf.log", rf.nextIndex[i], rf.log)

			//if rf.nextIndex[i]-1 < 0 || rf.nextIndex[i] > len(rf.log) {
			//	fmt.Println(rf.me, "i", i, "rf.nextIndex[i]", rf.nextIndex[i])
			//}
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

			if reply.Success {
				// append成功
				nCommit++

				fmt.Println(rf.me, "rf.nextIndex[i] += len(args.Entries)", rf.nextIndex[i], len(args.Entries))

				rf.nextIndex[i] = Max(rf.nextIndex[i], args.PrevLogIndex + len(args.Entries) + 1)
				//rf.nextIndex[i] += len(args.Entries)

				rf.nextIndex[i] = Min(rf.nextIndex[i], len(rf.log)+1)

			} else {
				// 失败的两种可能
				if reply.Term > rf.currentTerm {
					// 当前任期小，转换为follower
					rf.currentTerm = reply.Term
					rf.state = 0
				} else {
					// follower的日志太短，减小index重试
					rf.nextIndex[i] = Max(rf.nextIndex[i]-2, 1)
					fmt.Println(rf.me, "重试append to", i, "， nextIndex", rf.nextIndex[i])

					goto Loop

				}

			}

		}(rf, i, rf.nextIndex[i])
	}

	// 检查log是否复制成功
	go func(rf *Raft, cmd int) {
		for {
			// 一种情况，由于是并发的，所以可能 CommandIndex=3的消息比CommandIndex=2的消息先commit?
			// 此时应该加个同步，CommandIndex=3 commit的时候，检查一下，
			fmt.Println(rf.me, "nCommit", index, nCommit)
			if rf.state != 2 {
				break
			}
			if nCommit > rf.n/2 && rf.commitIndex == index-1 {

				// commit成功
				rf.mu.Lock()
				rf.commitIndex++
				rf.mu.Unlock()

				// 告知tester
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      cmd,
					CommandIndex: index,
				}
				rf.applyCh <- applyMsg
				break
			}

			time.Sleep(10 * time.Millisecond)
		}
	}(rf, cmd)

	return index, term, isLeader
}
