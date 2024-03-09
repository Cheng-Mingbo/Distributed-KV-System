package kvsrv

type Err string

const (
	OK       Err = "OK"
	ErrNoKey Err = "ErrNoKey"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string // "Put" or "Append"
	ClientId int64  // unique id of the Client
	ReqId    int64  // unique id of the Request
}

type PutAppendReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64 // unique id of the Client
	ReqId    int64 // unique id of the Request
}

type GetReply struct {
	Err   Err
	Value string
}
