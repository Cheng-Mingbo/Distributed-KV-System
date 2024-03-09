package shardkv

import (
	"6.5840/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case opReply := <-notifyCh:
		reply.Err = opReply.Err
		reply.Value = opReply.Value
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CommandType {
	case ConfigChange:
		newConfig := command.Command.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	case ShardMigration:
		shardData := command.Command.(ShardOperationReply)
		return kv.applyShardMigration(&shardData)
	case ShardGC:
		shardInfo := command.Command.(ShardOperationArgs)
		return kv.applyShardGC(&shardInfo)
	default:
		panic("Invalid operation type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currentConfig.Num+1 == newConfig.Num {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveIn
				}
			}
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				gid := newConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveOut
				}
			}

		}
		kv.preConfig = kv.currentConfig
		kv.currentConfig = newConfig
		return &OpReply{
			Err: OK,
		}
	}
	return &OpReply{
		Err: ErrWrongConfig,
	}
}

func (kv *ShardKV) applyShardMigration(shardData *ShardOperationReply) *OpReply {
	if shardData.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardData.ShardData {
			shard := kv.shards[shardId]
			if shard.Status == MoveIn {
				for k, v := range shardData {
					shard.KV[k] = v
				}
				shard.Status = GC
			} else {
				break
			}
		}
		for clientId, dupTable := range shardData.DuplicateTable {
			table, ok := kv.duplicateTable[clientId]
			if !ok || table.RequestId < dupTable.RequestId {
				kv.duplicateTable[clientId] = dupTable
			}
		}
		return &OpReply{
			Err: OK,
		}
	}
	return &OpReply{
		Err: ErrWrongConfig,
	}
}

func (kv *ShardKV) applyShardGC(shardInfo *ShardOperationArgs) *OpReply {
	if shardInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range shardInfo.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == GC {
				shard.Status = Normal
			} else if shard.Status == MoveOut {
				kv.shards[shardId] = NewMemoryKVStateMachine()
			} else {
				break
			}
		}
	}
	return &OpReply{
		Err: OK,
	}
}
