// shardkv/client.go

package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"crypto/rand"
	"math/big"
	"time"
)

// key2shard 函数请保持不变

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	// 添加必要的字段
	clientID  int64
	requestID int64
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientID = nrand()
	ck.requestID = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.requestID += 1
	args := GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// 尝试每个服务器
			for _, srvName := range servers {
				srv := ck.make_end(srvName)
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrWrongLeader) {
					break
				}
				// 其他情况继续尝试
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestID += 1
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// 尝试每个服务器
			for _, srvName := range servers {
				srv := ck.make_end(srvName)
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrWrongLeader) {
					break
				}
				// 其他情况继续尝试
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
