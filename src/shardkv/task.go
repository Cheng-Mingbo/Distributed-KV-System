package shardkv

import (
	"sync"
	"time"
)

func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.mu.Lock()
				if applyMsg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = applyMsg.CommandIndex

				var opReply *OpReply
				raftCommand := applyMsg.Command.(RaftCommand)
				switch raftCommand.CommandType {
				case ClientOperation:
					op := raftCommand.Command.(Op)
					opReply = kv.applyClientOperation(op)
				default:
					opReply = kv.handleConfigChangeMessage(raftCommand)
				}

				if _, isLeader := kv.rf.GetState(); isLeader {
					notify := kv.getNotifyChan(applyMsg.CommandIndex)
					notify <- opReply
				}

				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(applyMsg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if applyMsg.SnapshotValid {
				kv.mu.Lock()
				if kv.lastApplied < applyMsg.SnapshotIndex {
					kv.lastApplied = applyMsg.SnapshotIndex
					kv.restoreFromSnapshot(applyMsg.Snapshot)
				}
				kv.mu.Unlock()
			} else {
				panic("Invalid ApplyMsg")
			}
		}
	}
}

func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			needFetch := true
			kv.mu.Lock()
			for _, shard := range kv.shards {
				if shard.Status != Normal {
					needFetch = false
					break
				}
			}
			currentNum := kv.currentConfig.Num
			kv.mu.Unlock()

			if needFetch {
				newConfig := kv.mck.Query(currentNum + 1)
				if newConfig.Num == currentNum+1 {
					kv.ConfigCommand(RaftCommand{
						CommandType: ConfigChange,
						Command:     newConfig,
					}, &OpReply{})
				}
			}
		}
		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) shardMigrationTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gidToShards := kv.getShardByStatus(MoveIn)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					getShardArgs := ShardOperationArgs{
						ConfigNum: configNum,
						ShardIds:  shardIds,
					}
					for _, server := range servers {
						var getShardReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.GetShardsData", &getShardArgs, &getShardReply)
						if ok && getShardReply.Err == OK {
							kv.ConfigCommand(RaftCommand{
								CommandType: ShardMigration,
								Command:     getShardReply,
							}, &OpReply{})

						}
					}
				}(kv.preConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}

	}
	time.Sleep(ShardMigrationInterval)
}

func (kv *ShardKV) shardGCTask() {
	for !kv.killed() {

		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gidToShards := kv.getShardByStatus(GC)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					shardGCArgs := ShardOperationArgs{configNum, shardIds}
					for _, server := range servers {
						var shardGCReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.DeleteShardsData", &shardGCArgs, &shardGCReply)
						if ok && shardGCReply.Err == OK {
							kv.ConfigCommand(RaftCommand{ShardGC, shardGCArgs}, &OpReply{})
						}
					}
				}(kv.preConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(ShardGCInterval)
	}
}

func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			// 原来所属的 Group
			gid := kv.preConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}
				gidToShards[gid] = append(gidToShards[gid], i)
			}
		}
	}
	return gidToShards
}

func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// 只需要从 Leader 获取数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 当前 Group 的配置不是所需要的
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// 拷贝 shard 数据
	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardData[shardId] = kv.shards[shardId].CopyData()
	}

	// 拷贝去重表数据
	reply.DuplicateTable = make(map[int64]*LastOperation)
	for clientId, op := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = op.clone()
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// 只需要从 Leader 获取数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opReply OpReply
	kv.ConfigCommand(RaftCommand{ShardGC, *args}, &opReply)

	reply.Err = opReply.Err
}

func (kv *ShardKV) applyClientOperation(op Op) *OpReply {
	if kv.matchGroup(op.Key) {
		var opReply *OpReply
		if op.OpType != GetOp && kv.isDuplicate(op.ClientId, op.RequestId) {
			opReply = kv.duplicateTable[op.ClientId].Reply
		} else {
			// 将操作应用状态机中
			shardId := key2shard(op.Key)
			opReply = kv.applyToStateMachine(op, shardId)
			if op.OpType != GetOp {
				kv.duplicateTable[op.ClientId] = &LastOperation{
					RequestId: op.RequestId,
					Reply:     opReply,
				}
			}
		}
		return opReply
	}
	return &OpReply{Err: ErrWrongGroup}
}
