// mr/coordinator.go

package mr

import (
    "log"         // 用于日志记录
    "net"         // 提供网络接口，包括TCP/IP和UNIX域套接字
    "os"          // 提供与操作系统相关的功能，如文件操作
    "net/rpc"     // 提供RPC（远程过程调用）功能
    "net/http"    // 提供HTTP客户端和服务器的实现
    "sync"        // 提供同步原语，如互斥锁
    "time"        // 提供时间相关功能，如睡眠
)

// 定义任务状态的枚举类型
type TaskStatus int

const (
    Idle TaskStatus = iota      // 任务空闲状态，尚未被分配
    InProgress                  // 任务进行中，已被某个Worker接收但尚未完成
    Completed                   // 任务已完成，结果已被记录
)

// Coordinator结构体负责管理Map和Reduce任务的分配和状态跟踪
type Coordinator struct {
    mu sync.Mutex // 互斥锁，用于保护以下共享资源的并发访问

    // Map任务相关
    mapTasks      []string      // 存储所有Map任务的输入文件名列表
    mapTaskStatus []TaskStatus  // 存储每个Map任务的当前状态
    nMap          int           // Map任务的总数

    // Reduce任务相关
    nReduce          int           // Reduce任务的总数
    reduceTaskStatus []TaskStatus  // 存储每个Reduce任务的当前状态

    // 任务完成标志
    mapDone    bool // 标志是否所有Map任务都已完成
    reduceDone bool // 标志是否所有Reduce任务都已完成
}

// RequestTask处理函数用于响应Worker的任务请求
// Worker通过RPC调用Coordinator的RequestTask方法来请求新的任务
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
    c.mu.Lock()         // 加锁，确保以下操作的原子性
    defer c.mu.Unlock() // 函数结束时解锁

    // 如果还有未完成的Map任务
    if !c.mapDone {
        // 遍历所有Map任务，寻找状态为Idle的任务
        for i, status := range c.mapTaskStatus {
            if status == Idle {
                // 找到一个空闲的Map任务，准备分配给Worker
                reply.TaskType = MapTask             // 设置任务类型为MapTask
                reply.TaskNumber = i                 // 设置任务编号为当前索引i
                reply.NReduce = c.nReduce            // 通知Worker Reduce任务的总数
                reply.Filename = c.mapTasks[i]       // 传递Map任务对应的输入文件名

                // 更新Map任务状态为InProgress，表示该任务已被分配
                c.mapTaskStatus[i] = InProgress

                // 启动一个协程监控该任务是否超时（未在10秒内完成）
                go c.monitorTask(MapTask, i)

                return nil // 成功分配任务，返回nil错误
            }
        }
        // 如果没有空闲的Map任务，设置任务类型为WaitTask，表示当前没有可用任务，Worker需要等待
        reply.TaskType = WaitTask
        return nil
    }

    // 如果所有Map任务都已完成，但还有未完成的Reduce任务
    if !c.reduceDone {
        // 遍历所有Reduce任务，寻找状态为Idle的任务
        for i, status := range c.reduceTaskStatus {
            if status == Idle {
                // 找到一个空闲的Reduce任务，准备分配给Worker
                reply.TaskType = ReduceTask            // 设置任务类型为ReduceTask
                reply.TaskNumber = i                  // 设置任务编号为当前索引i
                reply.NMap = c.nMap                    // 通知Worker Map任务的总数

                // 更新Reduce任务状态为InProgress，表示该任务已被分配
                c.reduceTaskStatus[i] = InProgress

                // 启动一个协程监控该任务是否超时（未在10秒内完成）
                go c.monitorTask(ReduceTask, i)

                return nil // 成功分配任务，返回nil错误
            }
        }
        // 如果没有空闲的Reduce任务，设置任务类型为WaitTask，表示当前没有可用任务，Worker需要等待
        reply.TaskType = WaitTask
        return nil
    }

    // 如果所有Map和Reduce任务都已完成，设置任务类型为ExitTask，通知Worker退出
    reply.TaskType = ExitTask
    return nil
}

// ReportTask处理函数用于接收Worker报告任务完成的消息
// Worker通过RPC调用Coordinator的ReportTask方法来报告任务完成
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
    c.mu.Lock()         // 加锁，确保以下操作的原子性
    defer c.mu.Unlock() // 函数结束时解锁

    if args.TaskType == MapTask {
        // 如果报告的是Map任务
        if c.mapTaskStatus[args.TaskNumber] != Completed {
            // 将对应的Map任务状态更新为Completed
            c.mapTaskStatus[args.TaskNumber] = Completed
        }
        // 检查是否所有Map任务都已完成
        c.checkMapDone()
    } else if args.TaskType == ReduceTask {
        // 如果报告的是Reduce任务
        if c.reduceTaskStatus[args.TaskNumber] != Completed {
            // 将对应的Reduce任务状态更新为Completed
            c.reduceTaskStatus[args.TaskNumber] = Completed
        }
        // 检查是否所有Reduce任务都已完成
        c.checkReduceDone()
    }

    return nil // 成功处理报告，返回nil错误
}

// checkMapDone函数用于检查所有Map任务是否已完成
func (c *Coordinator) checkMapDone() {
    for _, status := range c.mapTaskStatus {
        if status != Completed {
            // 如果存在未完成的Map任务，直接返回
            return
        }
    }
    // 如果所有Map任务都已完成，设置mapDone为true
    c.mapDone = true
}

// checkReduceDone函数用于检查所有Reduce任务是否已完成
func (c *Coordinator) checkReduceDone() {
    for _, status := range c.reduceTaskStatus {
        if status != Completed {
            // 如果存在未完成的Reduce任务，直接返回
            return
        }
    }
    // 如果所有Reduce任务都已完成，设置reduceDone为true
    c.reduceDone = true
}

// monitorTask函数用于监控任务是否超时（未在10秒内完成）
// 如果任务超时，将其状态重置为Idle，允许重新分配
func (c *Coordinator) monitorTask(taskType TaskType, taskNumber int) {
    // 等待10秒
    time.Sleep(10 * time.Second)

    c.mu.Lock()         // 加锁，确保以下操作的原子性
    defer c.mu.Unlock() // 函数结束时解锁

    // 检查任务类型并确认任务状态
    if taskType == MapTask && c.mapTaskStatus[taskNumber] == InProgress {
        // 如果是Map任务且仍在进行中，重置状态为Idle
        c.mapTaskStatus[taskNumber] = Idle
    } else if taskType == ReduceTask && c.reduceTaskStatus[taskNumber] == InProgress {
        // 如果是Reduce任务且仍在进行中，重置状态为Idle
        c.reduceTaskStatus[taskNumber] = Idle
    }
}

// server函数用于启动RPC服务器，监听Worker的请求
func (c *Coordinator) server() {
    rpc.Register(c)        // 将Coordinator对象注册为RPC服务器，允许其方法被远程调用
    rpc.HandleHTTP()       // 将RPC服务器绑定到HTTP处理器，允许通过HTTP协议进行通信

    sockname := coordinatorSock() // 获取Coordinator的UNIX套接字文件名
    os.Remove(sockname)           // 删除旧的套接字文件，避免地址冲突

    // 使用UNIX域套接字监听，确保通信仅限于同一台机器上的进程
    l, e := net.Listen("unix", sockname)
    if e != nil {
        log.Fatal("listen error:", e) // 如果监听失败，记录错误并退出程序
    }

    // 在一个新的Go程中启动HTTP服务器，处理来自Worker的RPC请求
    go http.Serve(l, nil)
}

// Done函数用于判断整个MapReduce作业是否已完成
// main/mrcoordinator.go周期性调用此函数来决定何时退出
func (c *Coordinator) Done() bool {
    c.mu.Lock()         // 加锁，确保以下操作的原子性
    defer c.mu.Unlock() // 函数结束时解锁
    return c.mapDone && c.reduceDone // 如果所有Map和Reduce任务都已完成，返回true
}

// MakeCoordinator函数用于初始化Coordinator对象
// 接收输入文件列表和Reduce任务数量作为参数
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    c := Coordinator{
        mapTasks:        files,                 // 初始化Map任务列表为输入文件列表
        nMap:            len(files),            // 设置Map任务总数为输入文件的数量
        nReduce:         nReduce,               // 设置Reduce任务总数为传入的参数nReduce
        mapTaskStatus:   make([]TaskStatus, len(files)), // 初始化Map任务状态切片，初始状态为Idle
        reduceTaskStatus: make([]TaskStatus, nReduce),   // 初始化Reduce任务状态切片，初始状态为Idle
    }

    c.server() // 启动RPC服务器，监听Worker的请求
    return &c  // 返回Coordinator的指针
}
