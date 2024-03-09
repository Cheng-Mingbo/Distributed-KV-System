package shardctrler

import (
	"6.5840/raft"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.

	configs []Config // indexed by config num

	lastApplied  int
	stateMachine *CtrlerStateMachine
	notifyChans  map[int]chan *OperationReply
	duplicateOps map[int64]*LastOperation
}

func (sc *ShardCtrler) Command(args Op, reply *OperationReply) {
	//defer DPrintf("ShardCtrler.Command: %v, %v", args, reply)
	sc.mu.RLock()
	if args.Operation != QueryOp && sc.isDuplicate(args.ClientId, args.RequestId) {
		response := sc.duplicateOps[args.ClientId].Reply
		reply.Err = response.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	notifyChan := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case opReply := <-notifyChan:
		reply.ControllerConfig = opReply.ControllerConfig
		reply.Err = opReply.Err
	case <-time.After(Timeout):
		reply.Err = ErrTimeout
	}

	DPrintf("ShardCtrler.Command: %v, %v", args, reply)

	go func() {
		sc.mu.Lock()
		delete(sc.notifyChans, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *OperationReply {
	notifyChan, ok := sc.notifyChans[index]
	if !ok {
		notifyChan = make(chan *OperationReply, 1)
		sc.notifyChans[index] = notifyChan
	}
	return notifyChan
}

func (sc *ShardCtrler) isDuplicate(clientId int64, requestId int64) bool {
	lastOp, ok := sc.duplicateOps[clientId]
	if !ok {
		return false
	}
	return lastOp.RequestId >= requestId
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var opReply OperationReply
	sc.Command(Op{
		Operation: JoinOp,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}, &opReply)
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var opReply OperationReply
	sc.Command(Op{
		Operation: LeaveOp,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}, &opReply)
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var opReply OperationReply
	sc.Command(Op{
		Operation: MoveOp,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}, &opReply)
	reply.Err = opReply.Err

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var opReply OperationReply
	sc.Command(Op{
		Operation: QueryOp,
		Num:       args.Num,
	}, &opReply)
	reply.Err = opReply.Err
	reply.Config = opReply.ControllerConfig
}

func (sc *ShardCtrler) applyer() {
	for !sc.killed() {
		select {
		case applyMsg := <-sc.applyCh:
			DPrintf("ShardCtrler.applyer: %v", applyMsg)
			if applyMsg.CommandValid {
				sc.mu.Lock()
				if applyMsg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = applyMsg.CommandIndex
				op := applyMsg.Command.(Op)
				var opReply *OperationReply

				if op.Operation != QueryOp && sc.isDuplicate(op.ClientId, op.RequestId) {
					opReply = sc.duplicateOps[op.ClientId].Reply
				} else {

					opReply = sc.applyToStateMachine(op)
					if op.Operation != QueryOp {
						sc.duplicateOps[op.ClientId] = &LastOperation{
							RequestId: op.RequestId,
							Reply:     opReply,
						}
					}
				}

				if _, isLeader := sc.rf.GetState(); isLeader {
					notifyChan := sc.getNotifyChan(applyMsg.CommandIndex)
					notifyChan <- opReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyToStateMachine(op Op) *OperationReply {
	var err Err
	var config Config

	switch op.Operation {
	case JoinOp:
		err = sc.stateMachine.Join(op.Servers)
	case LeaveOp:
		err = sc.stateMachine.Leave(op.GIDs)
	case MoveOp:
		err = sc.stateMachine.Move(op.Shard, op.GID)
	case QueryOp:
		config, err = sc.stateMachine.Query(op.Num)
	}
	return &OperationReply{
		Err:              err,
		ControllerConfig: config,
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.dead = 0

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.stateMachine = NewCtrlerStateMachine()
	sc.notifyChans = make(map[int]chan *OperationReply)
	sc.duplicateOps = make(map[int64]*LastOperation)

	go sc.applyer()

	return sc
}
