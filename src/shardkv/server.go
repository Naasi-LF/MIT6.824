// shardkv/server.go

package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"sync"
	"time"
)

type Op struct {
	// 定义操作结构
	Operation string // "Get", "Put", or "Append"
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// 添加必要的字段
	mck            *shardctrler.Clerk
	data           map[string]string      // KV 数据库
	clientRequests map[int64]int64        // 记录每个客户端的最新请求 ID，避免重复
	resultCh       map[int]chan OpResult  // 通知通道
}

type OpResult struct {
	Err   Err
	Value string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getResultChan(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.Value = result.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getResultChan(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// 如果需要，添加其他清理代码
}

func (kv *ShardKV) getResultChan(index int) chan OpResult {
	if _, ok := kv.resultCh[index]; !ok {
		kv.resultCh[index] = make(chan OpResult, 1)
	}
	return kv.resultCh[index]
}

func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)

			var result OpResult

			// 检查是否是重复请求
			if lastReq, ok := kv.clientRequests[op.ClientID]; !ok || op.RequestID > lastReq {
				// 执行操作
				switch op.Operation {
				case "Put":
					kv.data[op.Key] = op.Value
				case "Append":
					kv.data[op.Key] += op.Value
				case "Get":
					// 不修改数据
				}
				kv.clientRequests[op.ClientID] = op.RequestID
			}

			// 准备结果
			if op.Operation == "Get" {
				value, ok := kv.data[op.Key]
				if ok {
					result.Value = value
					result.Err = OK
				} else {
					result.Value = ""
					result.Err = ErrNoKey
				}
			} else {
				result.Err = OK
			}

			if ch, ok := kv.resultCh[msg.CommandIndex]; ok {
				ch <- result
			}

			kv.mu.Unlock()
		} else {
			// 处理其他类型的消息（如快照）
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// 注册需要序列化的结构
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 初始化字段
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.data = make(map[string]string)
	kv.clientRequests = make(map[int64]int64)
	kv.resultCh = make(map[int]chan OpResult)

	// 启动 applier 协程
	go kv.applier()

	return kv
}
