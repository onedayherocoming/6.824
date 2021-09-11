package raft

import (
	"6.824/labrpc"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
//选举定时器溢出时间
const HEARTBREAKTIMEOUT = time.Millisecond*150
const RPCTIMEOUT= time.Millisecond*100

//状态的枚举
type State int
const(
	Leader State = iota
	Follower
	Candidate
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	Mu        sync.Mutex          // Lock to protect shared access to this peer's state
	Peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	Me        int                 // this peer's index into peers[]
	Dead      int32               // set by Kill()
	ElectionTime *time.Timer      //选举定时器
	HeartBreakTick []*time.Ticker   //心跳定时器
	Role State                    //Raft sever的角色
	Term int                      //term 自己的leader编号，即是第多少个leader
	IsLeader bool                 //自己是不是leader
	VotedFor int                 //自己的投票
	VoteCh chan *RequestVoteReply //选举的结果通道
	Log []Entry
	notifyApplyCh       chan struct{}
	applyCh           chan ApplyMsg
	CommitIndex int
	LastApplied int

	NextIndex[] int
	MatchIndex[] int


	Debug bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.getTerm()
	isLeader := rf.getRole()==Leader
	rf.print("GetState"+strconv.Itoa(term)+" "+strconv.FormatBool(isLeader))
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft)print(format string, a ...interface{}){
	if rf.Debug == false{
		return
	}
	pre := ""
	place :="                       "
	for i:=0;i<rf.Me;i++{
		pre+=place
	}
	fmt.Println(pre+format,a)
}


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
	rf.Mu.Lock()
	defer  rf.Mu.Unlock()
	_,lastLogIndex := rf.lastLogTermIndex()
	index := lastLogIndex+1
	term := rf.Term
	isLeader := rf.IsLeader
	rf.print("收到Command")
	if isLeader{
		rf.Log = append(rf.Log,Entry{
			TERM: rf.Term,
			COMMAND: command,
		})
		rf.MatchIndex[rf.Me] = index
	}
	//rf.resetElectionTimer()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.Dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.Dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.Peers = peers
	rf.Persister = persister
	rf.Me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	//初始化为Follower
	rf.Role = Follower
	//选举计时器开始计时
	rf.ElectionTime = time.NewTimer(rf.getElectionTimeOut())
	//心跳定时器
	rf.HeartBreakTick = make([]*time.Ticker,len(rf.Peers))
	for i,_:= range rf.Peers{
		rf.HeartBreakTick[i] = time.NewTicker(HEARTBREAKTIMEOUT)
	}
	//初始化term为0
	rf.Term = 0
	//chan初始化
	rf.VoteCh = make(chan *RequestVoteReply)
	rf.VotedFor=-1
	rf.notifyApplyCh = make(chan struct{}, 100)

	rf.Debug = false

	rf.NextIndex = make([]int,len(rf.Peers))
	rf.MatchIndex = make([]int,len(rf.Peers))
	rf.Log = make([]Entry,1)
	for i,_ := range rf.Peers{
		rf.NextIndex[i]=1
		rf.MatchIndex[i]=0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//选举定时器溢出处理
	go rf.dealElectionTimeOut()

	//leader工作
	go rf.leaderHeartBreak()

	go rf.dealNotice()

	// start ticker goroutine to start elections
	// go rf.ticker()
	return rf
}
