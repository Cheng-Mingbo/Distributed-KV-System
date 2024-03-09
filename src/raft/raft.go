package raft

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

import (
	"6.5840/labgob"
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	// NodeStateFollower is the state of a Raft node when it is a follower.
	NodeStateFollower = iota
	// NodeStateCandidate is the state of a Raft node when it is a candidate.
	NodeStateCandidate
	// NodeStateLeader is the state of a Raft node when it is a leader.
	NodeStateLeader
)

// NodeState is the state of a Raft node.
type NodeState uint8

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
	state          NodeState

	currentTerm int
	votedFor    int
	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// becomeFollower converts the Raft node to a follower.
func (rf *Raft) becomeFollower(term int) {
	if term < rf.currentTerm {
		return
	}
	rf.state = NodeStateFollower

	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
}

// becomeCandidate converts the Raft node to a candidate.
func (rf *Raft) becomeCandidate() {
	if rf.state == NodeStateLeader {
		return
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = NodeStateCandidate
	rf.persist()
}

// becomeLeader converts the Raft node to a leader.
func (rf *Raft) becomeLeader() {
	if rf.state != NodeStateCandidate {
		return
	}
	rf.state = NodeStateLeader
	rf.electionTimer.Stop()
	for i := range rf.peers {
		rf.nextIndex[i], rf.matchIndex[i] = rf.getLastLog().Index+1, 0
	}
}

// Me
func (rf *Raft) Me() int {
	return rf.me
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.state == NodeStateLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
	DPrintf("{Node %d} persisted state: currentTerm=%d, votedFor=%d, logs=%v", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("Error decoding persisted state")
	} else {
		rf.mu.Lock()
		rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
		rf.mu.Unlock()
		DPrintf("{Node %d} read persisted state: currentTerm=%d, votedFor=%d, logs=%v", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	}
}

// ChangeState changes the state of the Raft node.
func (rf *Raft) ChangeState(state NodeState) {
	rf.state = state
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != NodeStateLeader {
		return -1, -1, false
	}
	DPrintf("{Node %d} received Start command %v", rf.me, command)
	newLog := rf.appendNewEntry(command)
	rf.SendHeartbeat(false)
	return newLog.Index, newLog.Term, true
}

// appendNewEntry appends a new log entry to the Raft log.
func (rf *Raft) appendNewEntry(command interface{}) LogEntry {
	lastLog := rf.getLastLog()
	newLog := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   lastLog.Index + 1,
	}

	rf.logs = append(rf.logs, newLog)
	rf.persist()
	return newLog
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// advanceCommitIndexForFollower advances the commit index for a follower.
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	if leaderCommit > rf.commitIndex {
		lastLog := rf.getLastLog()
		rf.commitIndex = min(leaderCommit, lastLog.Index)

		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
}

// advanceCommitIndexForLeader advances the commit index for a leader.
func (rf *Raft) advanceCommitIndexForLeader() {
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	matchIndex[rf.me] = rf.getLastLog().Index
	matchIndex = append(matchIndex, rf.getFirstLog().Index)
	sort.Ints(matchIndex)
	median := matchIndex[len(matchIndex)/2]
	if median > rf.commitIndex && rf.logs[median-rf.getFirstLog().Index].Term == rf.currentTerm {
		rf.commitIndex = median
		rf.applyCond.Signal()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          NodeStateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),

		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
	}
	// Your initialization code here (3A, 3B, 3C).
	DPrintf("{Node %d} initialized", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu) // initialize applyCond
	lastLog := rf.getLastLog()
	rf.commitIndex, rf.lastApplied = rf.getFirstLog().Index, rf.getFirstLog().Index
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i], rf.matchIndex[i] = lastLog.Index+1, 0
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
