package raft

import "fmt"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // term is candidate's term.
	CandidateId  int // candidateId is candidate requesting vote.
	LastLogIndex int // lastLogIndex is index of candidate's last log entry.
	LastLogTerm  int // lastLogTerm is term of candidate's last log entry.
}

func (req *RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVoteArgs{Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d}", req.Term, req.CandidateId, req.LastLogIndex, req.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // term is currentTerm, for candidate to update itself.
	VoteGranted bool // voteGranted is true means candidate received vote.
}

func (reply *RequestVoteReply) String() string {
	if reply.VoteGranted {
		return fmt.Sprintf("RequestVoteReply{Term: %d, VoteGranted: true}", reply.Term)
	}
	return fmt.Sprintf("RequestVoteReply{Term: %d, VoteGranted: false}", reply.Term)
}

// AppendEntriesRequest is the request for AppendEntries RPC.
type AppendEntriesRequest struct {
	Term         int        // term is leader's term.
	LeaderId     int        // leaderId is so follower can redirect clients.
	PrevLogIndex int        // prevLogIndex is index of log entry immediately preceding new ones.
	PrevLogTerm  int        // prevLogTerm is term of prevLogIndex entry.
	LeaderCommit int        // leaderCommit is leader's commitIndex.
	Entries      []LogEntry // entries is log entries to store (empty for heartbeat; may send more than one for efficiency).
}

func (req *AppendEntriesRequest) String() string {
	return fmt.Sprintf("AppendEntriesRequest{Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %v, LeaderCommit: %d}", req.Term, req.LeaderId, req.PrevLogIndex, req.PrevLogTerm, req.Entries, req.LeaderCommit)
}

// AppendEntriesReply is the reply for AppendEntries RPC.
type AppendEntriesReply struct {
	Term          int  // term is currentTerm, for leader to update itself.
	Success       bool // success is true if follower contained entry matching prevLogIndex and prevLogTerm.
	ConflictTerm  int  // conflictTerm is term of the conflicting entry.
	ConflictIndex int  // conflictIndex is first index it stores for conflictTerm.
}

func (reply *AppendEntriesReply) String() string {
	if reply.Success {
		return fmt.Sprintf("AppendEntriesReply{Term: %d, Success: true}", reply.Term)
	}
	return fmt.Sprintf("AppendEntriesReply{Term: %d, Success: false, ConflictTerm: %d, ConflictIndex: %d}", reply.Term, reply.ConflictTerm, reply.ConflictIndex)
}

// InstallSnapshotRequest is the request for InstallSnapshot RPC.
type InstallSnapshotRequest struct {
	Term              int    // term is leader's term.
	LeaderId          int    // leaderId is so follower can redirect clients.
	LastIncludedIndex int    // lastIncludedIndex is the snapshot replaces all entries up through and including this index.
	LastIncludedTerm  int    // lastIncludedTerm is term of lastIncludedIndex.
	Data              []byte // data[] contains raw bytes of the snapshot chunk, starting at offset.
}

func (req *InstallSnapshotRequest) String() string {
	return fmt.Sprintf("InstallSnapshotRequest{Term: %d, LeaderId: %d, LastIncludedIndex: %d, LastIncludedTerm: %d, Data: %v}", req.Term, req.LeaderId, req.LastIncludedIndex, req.LastIncludedTerm, req.Data)
}

// InstallSnapshotReply is the reply for InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term int // term is currentTerm, for leader to update itself.
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("InstallSnapshotReply{Term: %d}", reply.Term)
}

// generateRequestVote generates a RequestVoteArgs.
func (rf *Raft) generateRequestVote() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
}

// generateAppendEntriesRequest generates an AppendEntriesRequest.
func (rf *Raft) generateAppendEntriesRequest(preLogIndex int) *AppendEntriesRequest {
	lastLog := rf.getLastLog()
	firstLog := rf.getFirstLog()
	entries := make([]LogEntry, lastLog.Index-preLogIndex)
	copy(entries, rf.logs[preLogIndex+1-firstLog.Index:])
	return &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  rf.logs[preLogIndex-firstLog.Index].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
}

// generateInstallSnapshotRequest generates an InstallSnapshotRequest.
func (rf *Raft) generateInstallSnapshotRequest() *InstallSnapshotRequest {
	firstLog := rf.getFirstLog()
	return &InstallSnapshotRequest{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
}
