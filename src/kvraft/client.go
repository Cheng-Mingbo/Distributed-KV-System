package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int64
	clientId  int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		requestId: 0,
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.Command(&Command{
		Key: key,
		Op:  OpGet,
	})
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.Command(&Command{
		Key:   key,
		Value: value,
		Op:    op,
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

// Command
func (ck *Clerk) Command(command *Command) string {
	// You will have to modify this function.
	request := &CommandRequest{
		Command:   *command,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		var reply CommandReply
		ok := ck.servers[ck.leaderId].Call("KVServer.Command", request, &reply)
		if !ok || reply.Error == ErrWrongLeader || reply.Error == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.requestId++
		return reply.Value
	}
}
