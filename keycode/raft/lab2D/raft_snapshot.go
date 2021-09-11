package raft

import (
	"log"
	"time"
)

//请求参数
type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

//返回参数
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(peerIdx int){
	rf.lock("send install snapshot")
	args := InstallSnapshotArgs{
		Term :rf.term,
		LeaderId: rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm: rf.lastSnapshotTerm,
		Data: rf.persister.ReadSnapshot(),
	}
	rf.unlock("send install snapshot")
	timer := time.NewTimer(RPCTimeout)
	defer timer.Stop()

	for{
		timer.Stop()
		timer.Reset(RPCTimeout)

		okCh := make(chan bool ,1)
		reply := InstallSnapshotReply{}

		go func(){
			o := rf.peers[peerIdx].Call("Raft.InstallSnapshot",&args,&reply)
			if !o{
				time.Sleep(time.Millisecond*10)
			}
			okCh <- o
		}()

		ok := false
		select {
		case <- rf.stopCh:
			return
		case <- timer.C:
			continue
		case ok = <- okCh:
			if !ok{
				continue
			}
		}
		// ok == true
		rf.lock("send install snapshot")
		defer rf.unlock("send install snapshot")
		if rf.term != args.Term || rf.role != Leader{
			return
		}
		if reply.Term > rf.term{
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.persist()
			return
		}
		// success
		if args.LastIncludedIndex > rf.matchIndex[peerIdx]{
			rf.matchIndex[peerIdx] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peerIdx]{
			rf.nextIndex[peerIdx] = args.LastIncludedIndex+1
		}
		return
	}
}


func (rf *Raft)InstallSnapshot(args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	rf.lock("install snapshot")
	defer rf.unlock("install snapshot")

	reply.Term = rf.term
	if args.Term < rf.term{
		return
	}
	if args.Term > rf.term || rf.role != Follower{
		rf.term = args.Term
		rf.changeRole(Follower)
		rf.resetElectionTimer()
		defer rf.persist()
	}
	if rf.lastSnapshotIndex >= args.LastIncludedIndex{
		return
	}
	// success
	start := args.LastIncludedIndex -rf.lastSnapshotIndex
	if start <0 {
		//不可能的
		log.Fatal("install sn")
	}else if start >= len(rf.logEntries){
		rf.logEntries = make([]LogEntry,1)
		rf.logEntries[0].Term = args.LastIncludedTerm
		rf.logEntries[0].Idx = args.LastIncludedIndex
	}else{
		rf.logEntries = rf.logEntries[start:]
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(),args.Data)
}

//截断自己的logEntries,将logindex以及之前的log放进snapshot
func (rf *Raft) SavePersistAndSnapshot(logIndex int , snapshotData []byte){
	rf.lock("savePS")
	defer rf.unlock("savePS")

	if logIndex <= rf.lastSnapshotIndex{
		return
	}
	if logIndex > rf.commitIndex{
		panic("logIndex > rf.commitIndex")
	}
	lastLog := rf.getLogByIndex(logIndex)
	rf.logEntries = rf.logEntries[rf.getRealIdxByLogIndex(logIndex):]
	rf.lastSnapshotIndex = logIndex
	rf.lastSnapshotTerm = lastLog.Term
	persisterData := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(persisterData,snapshotData)
}