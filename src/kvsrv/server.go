package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu            sync.Mutex
	keyValueStore map[string]string
	clientOps     map[int64]int64            // map clientID to the last requestID
	clientLogs    map[int64]map[int64]string // 记录Append请求的日志的旧值
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.keyValueStore[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.clientOps[args.ClientId] >= args.ReqId {
		reply.Err = OK
		return
	}
	kv.keyValueStore[args.Key] = args.Value
	kv.clientOps[args.ClientId] = args.ReqId
	reply.Err = OK
	return
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否已经处理过相同的请求
	if kv.clientOps[args.ClientId] >= args.ReqId {
		reply.Err = OK
		reply.Value = kv.clientLogs[args.ClientId][args.ReqId]
		return
	}

	// 检查keyValueStore中是否已有该键对应的值，如果没有，则初始化它
	_, exists := kv.keyValueStore[args.Key]
	if !exists {
		kv.keyValueStore[args.Key] = ""
	}

	reply.Value = kv.keyValueStore[args.Key]

	// 记录客户端Value的旧值
	kv.clientLogs[args.ClientId] = make(map[int64]string)
	kv.clientLogs[args.ClientId][args.ReqId] = reply.Value

	// 追加值到现有的键值
	kv.keyValueStore[args.Key] += args.Value

	// 更新clientOps记录该客户端的最新请求ID
	kv.clientOps[args.ClientId] = args.ReqId

	reply.Err = OK
	return
}

// getLastValue returns the last value for a key
func (kv *KVServer) getLastValue(key, appendValue string) string {
	currentValue, exists := kv.keyValueStore[key]
	if !exists {
		return ""
	}
	return currentValue[:len(currentValue)-len(appendValue)]
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.keyValueStore = make(map[string]string)
	kv.clientOps = make(map[int64]int64)
	kv.clientLogs = make(map[int64]map[int64]string)
	// You may need initialization code here.
	return kv
}
