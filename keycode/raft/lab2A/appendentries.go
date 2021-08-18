package raft

import "strconv"

type AppendEntriesArgs struct {
	TERM int              //当前term
	LEADERID int          //leader的index
}
type AppendEntriesReply struct {
	TERM int              //当前term
	SUCCESS bool          //是否成功
}

func (rf *Raft) leaderHeartBreak(){
	for {
		select {
		case <-rf.heartBreakTick.C:
			rf.print(strconv.Itoa(rf.getTerm()))
			if rf.getRole() == Leader {
				//给其他所有server发送 AppendEntries
				rf.electionTime.Reset(rf.getElectionTimeOut())
				rf.sendAppendEntriesToOthers()
			}
		}
	}
}

func (rf *Raft) sendAppendEntriesToOthers(){
	for i:= 0;i<len(rf.peers);i++{
		if i!= rf.me && rf.getRole() == Leader{
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}
			args.TERM = rf.getTerm()
			args.LEADERID = rf.me
			go rf.sendAppendEntries(i,&args,&reply)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term := rf.getTerm()
	if args.TERM <term {
		reply.TERM = term
		reply.SUCCESS = false
		return
	}
	if args.TERM >= term{
		rf.electionTime.Reset(rf.getElectionTimeOut())
		reply.TERM = args.TERM
		reply.SUCCESS = true
	}
	if args.TERM > term{
		rf.setTerm(args.TERM)
		rf.setRole(Follower)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if reply.TERM > rf.getTerm(){
		rf.setTerm(reply.TERM)
		rf.setRole(Follower)
	}
	return ok
}


