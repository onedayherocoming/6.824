package raft

import (
	"math/rand"
	"strconv"
	"time"
)
type RequestVoteArgs struct {
	TERM int              //当前term
	CANDIDATEID int       //候选者id
}


type RequestVoteReply struct {
	TERM int              //当前term
	VOTEGRANTED bool      //是否投候选者一票
}

func (rf* Raft)getElectionTimeOut() time.Duration {
	return time.Millisecond*time.Duration(rand.Intn(151)+300)
}
func (rf *Raft )dealElectionTimeOut(){

	for{
		//下面这一句会阻塞，直到electionTime的定时器到时间
		<- rf.electionTime.C
		if rf.getRole()==Leader{
			continue
		}

		rf.print("ele timeout")
		//选举时间到了，自己的leader丢失，自己发起选举
		rf.setTerm(rf.getTerm()+1)
		rf.setRole(Candidate)
		rf.setVoteFor(rf.me)
		rf.electionTime.Reset(rf.getElectionTimeOut())
		//并发地给其他sever发送RequestVote RPCs

		rf.sendRVToOthers()
		//处理
		//rf.voteCh.
		rf.dealVoteReciver()
	}
}

func (rf *Raft) sendRVToOthers() {
	for i:=0 ; i< len(rf.peers);i++{
		if rf.me != i && rf.role == Candidate{
			args := RequestVoteArgs{}
			args.TERM = rf.getTerm()
			args.CANDIDATEID = rf.me
			reply:=RequestVoteReply{}
			go rf.sendRequestVote(i,&args,&reply)
		}
	}
}
func (rf *Raft) dealVoteReciver(){
	votes := 1
	for {
		select {
		case reply := <-rf.voteCh:
			if rf.getRole() != Candidate {
				rf.print("不再是Candidate")
				return
			}
			rf.print("收到投票反馈: " + strconv.FormatBool(reply.VOTEGRANTED) + " " + strconv.Itoa(reply.TERM))
			if reply.VOTEGRANTED == true && reply.TERM == rf.getTerm() {
				votes++
				rf.print("票数+1: " + strconv.Itoa(votes))
				if votes > len(rf.peers)/2 {
					rf.print("become leader:" + strconv.Itoa(votes))
					//变成leader
					rf.setRole(Leader)
					return
				}
			} else {
				if reply.TERM > rf.getTerm() {
					rf.setTerm(reply.TERM)
					rf.setRole(Follower)
					rf.electionTime.Reset(rf.getElectionTimeOut())
					return
				}
			}
		case <-rf.electionTime.C:
			//选举时间再次到了，还没有出结果，则再次发起一轮新的选举
			rf.electionTime.Reset(time.Nanosecond * 100)
			//fmt.Println("超时，选举时间重置")
			return
		}
	}

}




func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.print("候选者 "+strconv.Itoa(args.CANDIDATEID)+" 发起请求")
	term := rf.getTerm()
	if args.TERM < term{
		reply.VOTEGRANTED=false
		reply.TERM=term
		return
	}else if args.TERM > term{
		term = args.TERM
		rf.setTerm(term)
		rf.setRole(Follower)
		rf.electionTime.Reset(rf.getElectionTimeOut())
	}
	reply.TERM = term
	if rf.getVoteFor() == -1 || rf.getVoteFor()==args.CANDIDATEID{
		rf.setVoteFor(args.CANDIDATEID)
		reply.VOTEGRANTED=true
		rf.print("为 "+ strconv.Itoa(args.CANDIDATEID)+" 投票")
		return
	}else{
		reply.VOTEGRANTED=false
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.voteCh <- reply
	return ok
}
