// kvsrv/server.go

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
	mu           sync.Mutex
	data         map[string]string // 键值存储
	lastSequence map[int64]int     // 客户端ID -> 上次处理的序列号
	lastReply    map[int64]string  // 客户端ID -> 上次的回复，仅用于 Put 和 Append
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 对于 Get 请求，不需要进行重复检测
	value, exists := kv.data[args.Key]
	if !exists {
		value = ""
	}
	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientID := args.ClientID
	seqNum := args.SequenceNum

	lastSeq, ok := kv.lastSequence[clientID]
	if ok {
		if seqNum == lastSeq {
			// 重复请求，返回上次的回复
			reply.Value = kv.lastReply[clientID]
			return
		} else if seqNum < lastSeq {
			// 旧请求，忽略
			return
		}
	}

	// 处理新请求
	kv.data[args.Key] = args.Value
	reply.Value = ""

	// 更新客户端状态
	kv.lastSequence[clientID] = seqNum
	kv.lastReply[clientID] = reply.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientID := args.ClientID
	seqNum := args.SequenceNum

	lastSeq, ok := kv.lastSequence[clientID]
	if ok {
		if seqNum == lastSeq {
			// 重复请求，返回上次的回复
			reply.Value = kv.lastReply[clientID]
			return
		} else if seqNum < lastSeq {
			// 旧请求，忽略
			return
		}
	}

	// 处理新请求
	oldValue, _ := kv.data[args.Key]
	kv.data[args.Key] = oldValue + args.Value
	reply.Value = oldValue

	// 更新客户端状态
	kv.lastSequence[clientID] = seqNum
	kv.lastReply[clientID] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.lastSequence = make(map[int64]int)
	kv.lastReply = make(map[int64]string)
	return kv
}
