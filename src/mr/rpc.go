// mr/rpc.go

package mr

import (
    "os"
    "strconv"
)

// RPC定义。
// 记得将所有名称首字母大写，以便RPC能够访问它们。

//
// 定义任务类型
//
type TaskType int

const (
    MapTask TaskType = iota //Map任务
    ReduceTask  // Reduce任务
    WaitTask   // 等待任务，表示当前没有可用任务
    ExitTask   // 退出任务，表示作业已完成
)

//
// 请求任务的参数
//
type RequestTaskArgs struct{}

//
// 请求任务的回复
//
type RequestTaskReply struct {
    TaskType   TaskType // 任务类型：Map、Reduce、Wait、Exit
    TaskNumber int      // 任务编号
    NMap       int      // Map任务总数
    NReduce    int      // Reduce任务总数
    Filename   string   // 对于Map任务，输入文件名
}

//
// 报告任务完成的参数
//
type ReportTaskArgs struct {
    TaskType   TaskType // 任务类型：Map或Reduce
    TaskNumber int      // 任务编号
}

//
// 报告任务完成的回复
//
type ReportTaskReply struct{}

//
// 在/var/tmp下制作一个独一无二的UNIX-domain socket名称
// 用于协调器。
// 不能使用当前目录，因为Athena AFS不支持UNIX-domain sockets。
//
func coordinatorSock() string {
    s := "/var/tmp/5840-mr-"
    s += strconv.Itoa(os.Getuid()) // 将用户ID添加到socket名称中，确保唯一性
    // 不同用户的进程不会产生冲突——即每个用户都有自己唯一的套接字文件
    return s
}
