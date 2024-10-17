// kvsrv/client.go

package kvsrv

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	server      *labrpc.ClientEnd
	clientID    int64
	sequenceNum int
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
	ck.clientID = nrand()
	ck.sequenceNum = 0
	return ck
}

// 获取指定键的当前值。
// 如果键不存在，返回空字符串。
// 在遇到所有其他错误时持续尝试。
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}
	for {
		var reply GetReply
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}
		// 如果调用失败，继续重试
	}
}

// Put 和 Append 共享的方法。
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.sequenceNum++
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		ClientID:    ck.clientID,
		SequenceNum: ck.sequenceNum,
	}
	for {
		var reply PutAppendReply
		var ok bool
		if op == "Put" {
			ok = ck.server.Call("KVServer.Put", &args, &reply)
		} else if op == "Append" {
			ok = ck.server.Call("KVServer.Append", &args, &reply)
		} else {
			// 无效的操作
			return ""
		}
		if ok {
			return reply.Value
		}
		// 如果调用失败，继续重试
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// 将值追加到键的值并返回该值
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
