package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	OpGet          = "Get"
	OpPut          = "Put"
	OpAppend       = "Append"
)

type Err string

type Command struct {
	Key   string
	Value string
	Op    string
}

type CommandRequest struct {
	Command   Command
	ClientId  int64
	RequestId int64
}

type CommandReply struct {
	Error Err
	Value string
}

type OperationContext struct {
	LastResponse *CommandReply
	RequestId    int64
}

type Op struct {
	Key       string
	Value     string
	Op        string
	ClientId  int64
	RequestId int64
}

type Snapshot struct {
	StateMachine   KVStateMachine
	LastOperations map[int64]OperationContext
}
