package kvsrv

import (
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId   int64
	reqId      int64
	reqIdMutex sync.Mutex
	interval   time.Duration
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.reqId = 0
	ck.interval = 500 * time.Millisecond
	return ck
}

// nextRequestId should ensure uniqueness within this Clerk instance
func (ck *Clerk) nextRequestId() int64 {
	ck.reqIdMutex.Lock()
	defer ck.reqIdMutex.Unlock()

	ck.reqId++
	return ck.reqId
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{key, ck.clientId, ck.nextRequestId()}
	reply := GetReply{}

	callDone := make(chan bool)
	for {
		go func() {
			ok := ck.server.Call("KVServer.Get", &args, &reply)
			callDone <- ok
		}()

		select {
		case ok := <-callDone:
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			}
		case <-time.After(ck.interval):
			// retry
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{key, value, op, ck.clientId, ck.nextRequestId()}
	reply := PutAppendReply{}

	return ck.sendPutAppendRequest(&args, &reply, op)
}

func (ck *Clerk) sendPutAppendRequest(args *PutAppendArgs, reply *PutAppendReply, op string) string {
	callDone := make(chan bool)
	for {
		go func() {
			var ok bool
			if op == "Put" {
				ok = ck.server.Call("KVServer.Put", args, reply)
			} else { // "Append"
				ok = ck.server.Call("KVServer.Append", args, reply)
			}
			callDone <- ok
		}()

		select {
		case ok := <-callDone:
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			}
		case <-time.After(ck.interval):
			// retry
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
