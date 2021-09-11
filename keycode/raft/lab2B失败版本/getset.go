package raft


func (rf *Raft)setTerm(term int){
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if term != rf.Term{
		//每个term都会清空选票
		rf.VotedFor=-1
	}
	rf.Term = term
}
func (rf *Raft)setVoteFor(voteFor int){
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.VotedFor= voteFor
}

func (rf *Raft)getTerm()int{
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return rf.Term
}
func (rf *Raft)getRole()State{
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return rf.Role
}

func (rf *Raft)getVoteFor()int{
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return rf.VotedFor
}

func (rf *Raft)setRole(role State){
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.Role = role
	if role==Leader{
		rf.print("Leader")
	}else if role==Candidate{
		rf.print("Candidate")
	}else{
		rf.print("Follower")
	}

	if role == Leader{
		rf.IsLeader = true
	}else{
		rf.IsLeader = false
	}
}

func (rf *Raft) lastLogTermIndex()(int,int){
	term := rf.Log[len(rf.Log)-1].TERM
	index := len(rf.Log)-1
	return term,index
}

func (rf *Raft)resetElectionTimer(){
	rf.ElectionTime.Stop()
	rf.ElectionTime.Reset(rf.getElectionTimeOut())
}
func (rf *Raft) changeRole(role State) {
	rf.Role = role
	switch role {
	case Follower:
	case Candidate:
		rf.Term += 1
		rf.VotedFor = rf.Me
		rf.resetElectionTimer()
	case Leader:
		_, lastLogIndex := rf.lastLogTermIndex()
		rf.NextIndex = make([]int, len(rf.Peers))
		for i := 0; i < len(rf.Peers); i++ {
			rf.NextIndex[i] = lastLogIndex + 1
		}
		rf.MatchIndex = make([]int, len(rf.Peers))
		rf.MatchIndex[rf.Me] = lastLogIndex
		rf.resetElectionTimer()
	default:
		panic("unknown role")
	}
}
