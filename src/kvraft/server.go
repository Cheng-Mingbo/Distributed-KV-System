package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	lastApplied int

	stateMachine   KVStateMachine
	lastOperations map[int64]OperationContext
	notifyChans    map[int]chan *CommandReply

	timeout time.Duration
}

// Command
func (kv *KVServer) Command(request *CommandRequest, reply *CommandReply) {
	// Your code here.
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.me, request, reply)

	kv.mu.RLock()
	if request.Command.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.RequestId) {
		lastResponse := kv.lastOperations[request.ClientId].LastResponse
		reply.Value, reply.Error = lastResponse.Value, lastResponse.Error
		DPrintf("{Node %v} returns duplicate response %v", kv.me, reply)
		kv.mu.RUnlock()
		return
	}

	op := Op{
		Key:       request.Command.Key,
		Value:     request.Command.Value,
		Op:        request.Command.Op,
		ClientId:  request.ClientId,
		RequestId: request.RequestId,
	}

	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Error = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case commandReply := <-ch:
		*reply = *commandReply
	case <-time.After(kv.timeout):
		reply.Error = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int64) bool {
	lastRequest, ok := kv.lastOperations[clientId]
	if !ok {
		return false
	}
	return lastRequest.RequestId >= requestId

}

func (kv *KVServer) getNotifyChan(index int) chan *CommandReply {
	ch, ok := kv.notifyChans[index]
	if !ok {
		ch = make(chan *CommandReply, 1)
		kv.notifyChans[index] = ch
	}
	return ch
}

func (kv *KVServer) applyer() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			kv.mu.Lock()
			DPrintf("{Node %v} received ApplyMsg %v", kv.me, applyMsg)
			if applyMsg.CommandValid {

				if applyMsg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				kv.lastApplied = applyMsg.CommandIndex
				commandRequest := applyMsg.Command.(Op)
				var commandReply *CommandReply
				if commandRequest.Op != OpGet && kv.isDuplicateRequest(commandRequest.ClientId, commandRequest.RequestId) {
					commandReply = kv.lastOperations[commandRequest.ClientId].LastResponse
				} else {
					commandReply = kv.executeCommand(commandRequest)
					if commandRequest.Op != OpGet {
						kv.lastOperations[commandRequest.ClientId] = OperationContext{
							LastResponse: commandReply,
							RequestId:    commandRequest.RequestId,
						}
					}
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader && applyMsg.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(applyMsg.CommandIndex)
					ch <- commandReply
				}
				kv.mu.Unlock()

			} else if applyMsg.SnapshotValid {
				if kv.lastApplied < applyMsg.SnapshotIndex {
					DPrintf("{Node %v} lastApplied: %v, snapshotIndex: %v", kv.me, kv.lastApplied, applyMsg.SnapshotIndex)
					kv.lastApplied = applyMsg.SnapshotIndex
					kv.restoreSnapshot(applyMsg.Snapshot)
				}

				kv.mu.Unlock()
			} else {
				panic("Invalid ApplyMsg")
			}

		}
	}
}

func (kv *KVServer) executeCommand(request Op) *CommandReply {
	switch request.Op {
	case OpGet:
		return kv.executeGet(request)
	case OpPut:
		return kv.executePut(request)
	case OpAppend:
		return kv.executeAppend(request)
	default:
		panic("Invalid command")
	}
}

func (kv *KVServer) executeGet(request Op) *CommandReply {

	value, err := kv.stateMachine.Get(request.Key)
	DPrintf("{Node %v} executes Get {Key: %v, ClientId: %v, Err: %v, Value: %v.", kv.me, request.Key, request.ClientId, err, value)
	return &CommandReply{
		Value: value,
		Error: err,
	}
}

func (kv *KVServer) executePut(request Op) *CommandReply {
	err := kv.stateMachine.Put(request.Key, request.Value)
	DPrintf("{Node %v} executes Put {Key: %v, Value: %v, ClientId: %v, RequestId: %v, Err: %v.", kv.me, request.Key, request.Value, request.ClientId, request.RequestId, err)
	return &CommandReply{
		Error: err,
	}
}

func (kv *KVServer) executeAppend(request Op) *CommandReply {
	err := kv.stateMachine.Append(request.Key, request.Value)
	DPrintf("{Node %v} executes Append {Key: %v, Value: %v, ClientId: %v, RequestId: %v, Err: %v.", kv.me, request.Key, request.Value, request.ClientId, request.RequestId, err)
	return &CommandReply{
		Error: err,
	}
}

func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate <= 0 {
		return false
	}
	return kv.persister.RaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) takeSnapshot(lastIncludedIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.mu.RLock()
	stateMachine := kv.stateMachine.Clone()
	lastOperations := make(map[int64]OperationContext, len(kv.lastOperations))
	for k, v := range kv.lastOperations {
		lastOperations[k] = v
	}
	kv.mu.RUnlock()

	e.Encode(stateMachine)
	e.Encode(lastOperations)

	data := w.Bytes()
	kv.rf.Snapshot(lastIncludedIndex, data)
	DPrintf("{Node %v} takes snapshot at index %v", kv.me, lastIncludedIndex)
}

func (kv *KVServer) snapshotor() {
	for !kv.killed() {
		if kv.needSnapshot() {
			kv.takeSnapshot(kv.lastApplied)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKVStateMachine
	var lastOperations map[int64]OperationContext

	if d.Decode(&stateMachine) != nil || d.Decode(&lastOperations) != nil {
		panic("Error decoding snapshot")
	}
	DPrintf("{Node %v} restores snapshot with stateMachine %v, lastOperations %v", kv.me, stateMachine, lastOperations)

	kv.stateMachine = stateMachine

	kv.lastOperations = lastOperations
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	//labgob.Register(CommandRequest{})
	//labgob.Register(Command{})
	labgob.Register(Snapshot{})
	labgob.Register(OperationContext{})
	labgob.Register(MemoryKVStateMachine{})
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.me = kv.rf.Me()
	kv.persister = persister

	// You may need initialization code here.
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.lastOperations = make(map[int64]OperationContext)
	kv.notifyChans = make(map[int]chan *CommandReply)
	kv.timeout = 500 * time.Millisecond

	kv.restoreSnapshot(persister.ReadSnapshot())

	go kv.applyer()
	go kv.snapshotor()

	return kv
}
