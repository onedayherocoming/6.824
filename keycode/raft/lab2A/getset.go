package raft


func (rf *Raft)setTerm(term int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.term{
		//每个term都会清空选票
		rf.votedFor=-1
	}
	rf.term = term
}
func (rf *Raft)setVoteFor(voteFor int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor= voteFor
}

func (rf *Raft)getTerm()int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term
}
func (rf *Raft)getRole()State{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft)getVoteFor()int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft)setRole(role State){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
	if role==Leader{
		rf.print("Leader")
	}else if role==Candidate{
		rf.print("Candidate")
	}else{
		rf.print("Follower")
	}

	if role == Leader{
		rf.isLeader = true
	}else{
		rf.isLeader = false
	}
}

