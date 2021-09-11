package raft

import "time"

type Entry struct {
	COMMAND interface{}
	TERM int
}

type AppendEntriesArgs struct {
	TERM int              //当前term
	LEADERID int          //leader的index
	PREVLOGINDEX int
	PREVLOGTERM int
	ENTRIES []Entry
	LEADERCOMMIT int
}
type AppendEntriesReply struct {
	TERM int              //当前term
	SUCCESS bool          //是否成功
	NEXTINDEX int
}

func (rf *Raft) leaderHeartBreak(){
	for i,_ := range rf.Peers{
		if i == rf.Me{
			continue
		}
		//为每个server起一个协程，复制心跳的发送
		go func(index int){
			for{
				select {
					case <- rf.HeartBreakTick[index].C :
						rf.Mu.Lock()
						if rf.Role==Leader {
							rf.print("heart")
							rf.Mu.Unlock()
							rf.sendAppendEntriesToOthers(index)
							rf.Mu.Lock()
						}
						rf.Mu.Unlock()

				}
			}
		}(i)
	}
}

func (rf *Raft)getAppendLogs(index int)(prevLogIndex,prevLogTerm int,res []Entry){
	nextIdx := rf.NextIndex[index]
	lastLogTerm,lastLogIndex := rf.lastLogTermIndex()
	if nextIdx > lastLogIndex{
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}
	res = append([]Entry{},rf.Log[nextIdx:]...)
	prevLogIndex = nextIdx-1
	prevLogTerm = rf.Log[prevLogIndex].TERM
	return
}

func (rf *Raft)getAppendEntriesArgs(index int) AppendEntriesArgs{
	prevLogIndex,prevLogTerm,logs := rf.getAppendLogs(index)
	args := AppendEntriesArgs{
		TERM: rf.Term,
		LEADERID: rf.Me,
		PREVLOGINDEX: prevLogIndex,
		PREVLOGTERM: prevLogTerm,
		ENTRIES: logs,
		LEADERCOMMIT: rf.CommitIndex,
	}
	return args
}

func (rf *Raft)updateCommit(){
	hasCommit := false
	for i:= rf.CommitIndex+1;i<= len(rf.Log);i++{
		count :=0
		for _,m := range rf.MatchIndex{
			if m >= i{
				count+=1
				if count > len(rf.Peers)/2{
					rf.CommitIndex = i
					hasCommit = true
					break
				}
			}
		}
		if rf.CommitIndex != i{
			break
		}
	}
	if hasCommit{
		rf.notifyApplyCh <- struct{}{}
	}
}

func (rf *Raft) sendAppendEntriesToOthers(index int){
	RPCTimer := time.NewTimer(RPCTIMEOUT)
	defer RPCTimer.Stop()

	for {
		rf.Mu.Lock()
		//如果不是leader，则退出
		if rf.Role != Leader {
			rf.Mu.Unlock()
			return
		}
		args := rf.getAppendEntriesArgs(index)
		rf.Mu.Unlock()

		reply := AppendEntriesReply{}
		resCh := make(chan bool,1)
		RPCTimer.Stop()
		RPCTimer.Reset(RPCTIMEOUT)

		go func(args* AppendEntriesArgs,reply *AppendEntriesReply){
			ok := rf.Peers[index].Call("Raft.AppendEntries",args,reply)
			if !ok{
				time.Sleep(time.Millisecond*10)
			}
			resCh <-ok
		}(&args,&reply)

		select {
			case ok:=<- resCh:
				if !ok{
					continue
				}
			case <- RPCTimer.C:
				continue
		}
		//返回成功
		rf.Mu.Lock()
		//
		if reply.TERM > rf.Term{
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.Term = reply.TERM
			rf.Mu.Unlock()
			return
		}
		if rf.Role!=Leader || rf.Term!= args.TERM{
			rf.Mu.Unlock()
			return
		}
		if reply.SUCCESS{
			//更新 NextIndex 和 MatchIndex
			if reply.NEXTINDEX > rf.NextIndex[index]{
				rf.NextIndex[index] = reply.NEXTINDEX
				rf.MatchIndex[index] = reply.NEXTINDEX-1
			}
			//更新 CommitIndex 只commit自己term的entry
			if len(args.ENTRIES)>0 && args.ENTRIES[len(args.ENTRIES)-1].TERM == rf.Term{
				rf.updateCommit()
				rf.Mu.Unlock()
				return
			}
		}
		//SUCCESS==false
		if reply.NEXTINDEX !=0{
			rf.NextIndex[index] = reply.NEXTINDEX
			rf.Mu.Unlock()
			continue
		}
	}
}
func (rf *Raft) getNextIndex() int {
	_, idx := rf.lastLogTermIndex()
	return idx + 1
}

func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool {
	// prevlog 已经对的上
	argsLastIndex := args.PREVLOGINDEX + len(args.ENTRIES)
	lastTerm, lastIndex := rf.lastLogTermIndex()
	if argsLastIndex < lastIndex && lastTerm == args.TERM {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Mu.Lock()
	reply.TERM = rf.Term

	if rf.Term > args.TERM{
		rf.Mu.Unlock()
		reply.SUCCESS=false
		return
	}
	rf.Term = args.TERM
	rf.changeRole(Follower)
	rf.resetElectionTimer()

	_,lastLogIndex := rf.lastLogTermIndex()
	if args.PREVLOGINDEX > lastLogIndex{
		//缺少中间的Log
		reply.SUCCESS = false
		reply.NEXTINDEX = rf.getNextIndex()
	}else if rf.Log[args.PREVLOGINDEX].TERM == args.PREVLOGTERM{
		//对上了，包括两种情况：一种是刚好是后续的log，一种是需要删除
		if rf.outOfOrderAppendEntries(args){
			reply.SUCCESS=false
			reply.NEXTINDEX=0
		}else{
			reply.SUCCESS=true
			rf.Log = append(rf.Log[0:args.PREVLOGINDEX+1],args.ENTRIES...)
			reply.NEXTINDEX = rf.getNextIndex()
		}
	}else{
		//不匹配，尝试跳过一个term
		reply.SUCCESS = false
		term := rf.Log[args.PREVLOGINDEX].TERM
		idx := args.PREVLOGINDEX
		for idx > rf.CommitIndex && rf.Log[idx].TERM == term{
			idx -=1
		}
		reply.NEXTINDEX = idx+1
	}
	if reply.SUCCESS{
		if rf.CommitIndex < args.LEADERCOMMIT {
			rf.CommitIndex = args.LEADERCOMMIT
			rf.notifyApplyCh <- struct{}{}
		}
	}
	rf.Mu.Unlock()
}


func (rf *Raft)dealNotice(){
	for{
		select {
			case <- rf.notifyApplyCh:
				rf.startApplyLogs()
		}
	}
}
func (rf *Raft) startApplyLogs() {
	rf.Mu.Lock()
	var msgs []ApplyMsg
	if rf.CommitIndex <= rf.LastApplied {
		// 无更新
		msgs = make([]ApplyMsg, 0)
	} else {
		msgs = make([]ApplyMsg, 0, rf.CommitIndex-rf.LastApplied)
		for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].COMMAND,
				CommandIndex: i,
			})
		}
	}
	rf.Mu.Unlock()
	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.Mu.Lock()
		rf.LastApplied = msg.CommandIndex
		rf.Mu.Unlock()
	}
}