package raft

import (
	"6.5840/labgob"
	"bytes"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %d} Snapshot: index=%d, snapshot=%v", rf.me, index, snapshot)

	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		return
	}

	rf.logs = shrinkEntriesArray(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftState := w.Bytes()
	rf.persister.Save(raftState, snapshot)
}

// InstallSnapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	rf.logs = make([]LogEntry, 1)
	rf.logs[0].Term, rf.logs[0].Index = args.LastIncludedTerm, args.LastIncludedIndex
	rf.logs[0].Command = nil
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persister.Save(rf.persister.ReadRaftState(), args.Data)
	rf.persist()

	DPrintf("{Node %d} InstallSnapshot: log=%v, reply=%v, args=%v", rf.me, rf.logs, args, reply)
	rf.mu.Unlock()

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

}

// handleInstallSnapshotResponse handles the response for InstallSnapshot RPC.
func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotReply) {
	if rf.state != NodeStateLeader {
		return
	}

	if response.Term > rf.currentTerm {
		rf.becomeFollower(response.Term)
		rf.resetElectionTimer()
		rf.persist()
		return
	}

	if request.Term == rf.currentTerm && response.Term == rf.currentTerm {
		rf.nextIndex[peer] = max(rf.nextIndex[peer], request.LastIncludedIndex+1)
		rf.matchIndex[peer] = max(rf.matchIndex[peer], request.LastIncludedIndex)
	}
}

// sendInstallSnapshot sends an InstallSnapshot RPC to a server.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
