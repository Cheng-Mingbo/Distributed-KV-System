package raft

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(RandomElectionTimeout())
	rf.heartbeatTimer.Stop()
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	rf.electionTimer.Stop()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.becomeCandidate()
			rf.StartElection()
			rf.resetElectionTimer()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == NodeStateLeader {
				rf.resetHeartbeatTimer()
				rf.SendHeartbeat(true)
			}
			rf.mu.Unlock()
		}
	}
}
