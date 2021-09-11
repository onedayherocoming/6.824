package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
)

//存储之前的raftstate
func (rf *Raft)readPersist(raftstate []byte){
	if raftstate==nil || len(raftstate)<1{
		return
	}
	r := bytes.NewBuffer(raftstate)
	d := labgob.NewDecoder(r)

	var term   int
	var voteFor int
	var logs []LogEntry
	var commitIndex int

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&logs) != nil{
		log.Fatal("rf read persist err")
	}else{
		rf.term = term
		rf.voteFor = voteFor
		rf.commitIndex = commitIndex
		rf.logEntries = logs
	}
}

func (rf *Raft) getPersistData()[]byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	return data
}

