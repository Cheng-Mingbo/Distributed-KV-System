package shardkv

import (
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrorTimeout"
	ErrWrongConfig = "ErrWrongConfig"
	ErrNotReady    = "ErrNotReady"
)

const (
	ClientRequestTimeout   = 500 * time.Millisecond
	FetchConfigInterval    = 100 * time.Millisecond
	ShardMigrationInterval = 50 * time.Millisecond
	ShardGCInterval        = 50 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	RequestId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type OperationType uint8

const (
	PutOp OperationType = iota
	AppendOp
	GetOp
)

type Op struct {
	Key       string
	Value     string
	OpType    OperationType
	ClientId  int64
	RequestId int64
}

type OpReply struct {
	Err   Err
	Value string
}

func getOpType(op string) OperationType {
	switch op {
	case "Put":
		return PutOp
	case "Append":
		return AppendOp
	case "Get":
		return GetOp
	default:
		panic("Invalid operation type")
	}
}

type LastOperation struct {
	Reply     *OpReply
	RequestId int64
}

func (op *LastOperation) clone() *LastOperation {
	return &LastOperation{
		Reply: &OpReply{
			Err:   op.Reply.Err,
			Value: op.Reply.Value,
		},
		RequestId: op.RequestId,
	}
}

type RaftCommandType uint8

const (
	ClientOperation RaftCommandType = iota
	ConfigChange
	ShardMigration
	ShardGC
)

type RaftCommand struct {
	CommandType RaftCommandType
	Command     interface{}
}

type ShardStatus uint8

const (
	Normal ShardStatus = iota
	MoveIn
	MoveOut
	GC
)

type ShardOperationArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	ShardData      map[int]map[string]string
	DuplicateTable map[int64]*LastOperation
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
