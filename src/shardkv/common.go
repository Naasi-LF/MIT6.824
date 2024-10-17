// shardkv/common.go

package shardkv

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put 或 Append 请求参数
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" 或 "Append"
	ClientID  int64  // 客户端 ID
	RequestID int64  // 请求 ID
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientID  int64 // 客户端 ID
	RequestID int64 // 请求 ID
}

type GetReply struct {
	Err   Err
	Value string
}
