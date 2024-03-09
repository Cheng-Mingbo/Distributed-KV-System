package raft

// StartElection starts an election.
func (rf *Raft) StartElection() {
	request := rf.generateRequestVote()
	grantedVotes := 1
	failed := 0

	DPrintf("{Node %d} StartElection: %+v", rf.me, request)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			response := &RequestVoteReply{
				Term:        0,
				VoteGranted: false,
			}
			rf.mu.RLock()
			if rf.state != NodeStateCandidate {
				rf.mu.RUnlock()
				return
			}
			rf.mu.RUnlock()

			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == request.Term && rf.state == NodeStateCandidate {

					if response.VoteGranted {
						grantedVotes++
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %d} has won the election", rf.me)
							rf.becomeLeader()
							rf.resetHeartbeatTimer()
							rf.SendHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {
						rf.restartElection(response.Term)
					} else {
						failed++
						if failed > len(rf.peers)/2 {
							if rf.state == NodeStateCandidate {
								rf.restartElection(rf.currentTerm)
							}
						}
					}
				}
			}

		}(peer)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %d} Responding to {Node %d} RequestVote: %+v", rf.me, args.CandidateId, reply)

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	reply.Term, reply.VoteGranted = rf.currentTerm, false
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	}
	rf.persist()
	return
}

// restartElection
func (rf *Raft) restartElection(term int) {
	rf.becomeFollower(term)
	rf.resetElectionTimer()
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
