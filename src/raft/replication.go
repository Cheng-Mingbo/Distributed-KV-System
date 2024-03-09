package raft

// replicator
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}

}

// needReplicating
func (rf *Raft) needReplicating(peer int) bool {

	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == NodeStateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// SendHeartbeat sends a heartbeat to all peers.
func (rf *Raft) SendHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			go rf.replicateOneRound(peer) // send heartbeats to all peers
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

// AppendEntries is the handler for AppendEntries RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = 0, false
		DPrintf("{Node %v} received AppendEntriesRequest from {Node %d} %v, but PrevLogIndex %v is less than firstLogIndex %v", rf.me, args.LeaderId, args, args.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	rf.resetElectionTimer()

	rf.becomeFollower(args.Term)

	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < args.PrevLogIndex {
			reply.ConflictTerm, reply.ConflictIndex = -1, lastIndex+1
		} else {
			firstIndex := rf.getFirstLog().Index
			reply.ConflictTerm = rf.logs[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}
		if len(args.Entries) > 0 && args.Entries != nil {
			DPrintf("{Node %v} received AppendEntriesRequest from {Node %d} <Term: %d PreLogTerm: %d PreLogIndex: %d LeaderCommit: %d FirstLogEntry:{Term: %d Command: %d Index: %d}>, but do not match and ConflictTerm: %v ConflictIndex: %v", rf.me, args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit, args.Entries[0].Term, args.Entries[0].Command, args.Entries[0].Index, reply.ConflictTerm, reply.ConflictIndex)
		} else {
			DPrintf("{Node %v} received AppendEntriesRequest from {Node %d} <Term: %d PreLogTerm: %d PreLogIndex: %d LeaderCommit: %d FirstLogEntry:nil>, but do not match and ConflictTerm: %v ConflictIndex: %v", rf.me, args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit, reply.ConflictTerm, reply.ConflictIndex)
		}
		return
	}

	firstIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], args.Entries[index:]...))
			break
		}
	}

	rf.advanceCommitIndexForFollower(args.LeaderCommit)
	if len(args.Entries) > 0 && args.Entries != nil {
		DPrintf("{Node %v} received AppendEntriesRequest from {Node %d} <Term: %d PreLogTerm: %d PreLogIndex: %d LeaderCommit: %d FirstLogEntry:{Term: %d Command: %d Index: %d}>, and successfully appended entries", rf.me, args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit, args.Entries[0].Term, args.Entries[0].Command, args.Entries[0].Index)
	} else {
		DPrintf("{Node %v} received AppendEntriesRequest from {Node %d} <Term: %d PreLogTerm: %d PreLogIndex: %d LeaderCommit: %d FirstLogEntry:nil>, and successfully appended entries", rf.me, args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit)
	}

	reply.Term, reply.Success = rf.currentTerm, true

	return
}

// replicateOneRound replicates log entries to a peer.
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()

	if rf.state != NodeStateLeader {
		rf.mu.Unlock()
		return
	}

	preLogIndex := rf.nextIndex[peer] - 1
	if preLogIndex < rf.getFirstLog().Index {
		request := rf.generateInstallSnapshotRequest()
		DPrintf("{Node %v} is sending InstallSnapshotRequest to {peer %v} with request %v", rf.me, peer, request)
		response := new(InstallSnapshotReply)
		rf.mu.Unlock()
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else {
		request := rf.generateAppendEntriesRequest(preLogIndex)
		rf.mu.Unlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

// handleAppendEntriesResponse handles the response for AppendEntries RPC.
func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesReply) {
	defer DPrintf("{Node %v} received AppendEntriesResponse from {Node %d} %v", rf.me, peer, response)
	if rf.state != NodeStateLeader || rf.currentTerm != request.Term || response.Term < rf.currentTerm {
		return
	}

	if response.Term > rf.currentTerm {
		rf.becomeFollower(response.Term)
		rf.resetElectionTimer()
		rf.persist()
		return
	}

	if request.Term == rf.currentTerm {
		if response.Success {
			rf.nextIndex[peer] = request.PrevLogIndex + len(request.Entries) + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1

			rf.advanceCommitIndexForLeader()
		} else {
			if response.ConflictTerm == -1 {
				if rf.nextIndex[peer] > response.ConflictIndex {
					rf.nextIndex[peer] = response.ConflictIndex
				}
			} else if rf.nextIndex[peer] > response.ConflictIndex {
				if (response.ConflictIndex-rf.getFirstLog().Index > 0) && rf.logs[response.ConflictIndex-rf.getFirstLog().Index].Term == response.ConflictTerm {
					rf.nextIndex[peer] = SearchNextIndex(rf.logs[response.ConflictIndex-rf.getFirstLog().Index:], response.ConflictTerm) + response.ConflictIndex
				} else {
					rf.nextIndex[peer] = response.ConflictIndex
				}
			}
			rf.replicatorCond[peer].Signal()
		}
	}
}

// sendAppendEntries sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
