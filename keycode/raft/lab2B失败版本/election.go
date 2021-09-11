package raft

import (
	"math/rand"
	"strconv"
	"time"
)
type RequestVoteArgs struct {
	TERM int              //当前term
	CANDIDATEID int       //候选者id
	LASTLOGINDEX int      //最后一个Log的Index
	LASTLOGTERM int       //最后一个Log的Term
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
		rf.print("waiting")
		//下面这一句会阻塞，直到electionTime的定时器到时间
		<- rf.ElectionTime.C
		if rf.getRole()==Leader{
			continue
		}

		rf.print("ele timeout")
		//选举时间到了，自己的leader丢失，自己发起选举
		rf.setTerm(rf.getTerm()+1)
		rf.setRole(Candidate)
		rf.setVoteFor(rf.Me)
		rf.ElectionTime.Reset(rf.getElectionTimeOut())
		//并发地给其他sever发送RequestVote RPCs

		rf.sendRVToOthers()
		//处理
		//rf.voteCh.
		rf.dealVoteReciver()
	}
}

func (rf *Raft) sendRVToOthers() {
	for i:=0 ; i< len(rf.Peers);i++{
		rf.Mu.Lock()
		if rf.Me != i && rf.Role == Candidate{
			args := RequestVoteArgs{}
			args.TERM = rf.Term
			args.CANDIDATEID = rf.Me
			args.LASTLOGTERM,args.LASTLOGINDEX = rf.lastLogTermIndex()
			reply:=RequestVoteReply{}
			go rf.sendRequestVote(i,&args,&reply)
		}
		rf.Mu.Unlock()
	}
}
func (rf *Raft) dealVoteReciver(){
	votes := 1
	for {
		select {
		case reply := <-rf.VoteCh:
			if rf.getRole() != Candidate {
				rf.print("不再是Candidate")
				return
			}
			rf.print("收到投票反馈: " + strconv.FormatBool(reply.VOTEGRANTED) + " " + strconv.Itoa(reply.TERM))
			if reply.VOTEGRANTED == true && reply.TERM == rf.getTerm() {
				votes++
				rf.print("票数+1: " + strconv.Itoa(votes))
				if votes > len(rf.Peers)/2 {
					rf.print("become leader:" + strconv.Itoa(votes))
					//变成leader
					rf.setRole(Leader)
					return
				}
			} else {
				if reply.TERM > rf.getTerm() {
					rf.setTerm(reply.TERM)
					rf.setRole(Follower)
					rf.ElectionTime.Reset(rf.getElectionTimeOut())
					return
				}
			}
		case <-rf.ElectionTime.C:
			//选举时间再次到了，还没有出结果，则再次发起一轮新的选举
			rf.ElectionTime.Reset(time.Nanosecond * 100)
			//fmt.Println("超时，选举时间重置")
			return
		}
	}

}




func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Mu.Lock()
	defer  rf.Mu.Unlock()
	rf.print("候选者 "+strconv.Itoa(args.CANDIDATEID)+" 发起请求")
	term := rf.Term
	if args.TERM < term{
		reply.VOTEGRANTED=false
		reply.TERM=term
		return
	}else if args.TERM > term{
		term = args.TERM
		rf.Term = term
		rf.VotedFor=-1
		rf.Role = Follower
		rf.ElectionTime.Reset(rf.getElectionTimeOut())
	}else if args.TERM == term{
		if rf.Role==Leader{
			reply.VOTEGRANTED=false
			reply.TERM = term
			return
		}
		if rf.VotedFor!=-1 && rf.VotedFor!=args.CANDIDATEID{
			reply.VOTEGRANTED=false
			reply.TERM = term
			return
		}
	}
	reply.TERM = term
	lastLogTerm , lastLogIndex := rf.lastLogTermIndex()
	if lastLogTerm > args.LASTLOGTERM || (args.LASTLOGTERM == lastLogTerm && args.LASTLOGINDEX < lastLogIndex){
		reply.VOTEGRANTED=false
		return
	}
	rf.changeRole(Follower)
	rf.VotedFor = args.CANDIDATEID

	rf.resetElectionTimer()
	reply.VOTEGRANTED=true
	rf.print("为 "+ strconv.Itoa(args.CANDIDATEID)+" 投票")
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.Peers[server].Call("Raft.RequestVote", args, reply)
	rf.VoteCh <- reply
	return ok
}
