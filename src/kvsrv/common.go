// kvsrv/common.go

package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	ClientID    int64
	SequenceNum int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// 不需要 ClientID 和 SequenceNum，因为我们不对 Get 请求进行重复检测
}

type GetReply struct {
	Value string
}
