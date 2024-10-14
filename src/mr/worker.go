// mr/worker.go

package mr

import (
    "fmt"          // 用于格式化输入和输出
    "io/ioutil"    // 用于读取和写入文件
    "log"          // 用于记录日志
    "net/rpc"      // 提供RPC（远程过程调用）功能
    "hash/fnv"     // 提供哈希函数
    "os"           // 提供与操作系统相关的功能，如文件操作
    "encoding/json" // 提供JSON编码和解码功能
    "sort"         // 提供排序功能
	"time" // 新增导入
)

// KeyValue 定义了一个键值对结构体，用于存储Map输出的中间结果
type KeyValue struct {
    Key   string // 键
    Value string // 值
}

// ByKey 定义了一个用于排序的类型，它是KeyValue切片的别名
type ByKey []KeyValue

// Len 方法返回ByKey切片的长度，用于排序接口
func (a ByKey) Len() int { 
    return len(a) 
}

// Swap 方法交换ByKey切片中两个元素的位置，用于排序接口
func (a ByKey) Swap(i, j int) { 
    a[i], a[j] = a[j], a[i] 
}

// Less 方法比较ByKey切片中两个元素的键，用于排序接口
func (a ByKey) Less(i, j int) bool { 
    return a[i].Key < a[j].Key 
}

// ihash 函数用于计算键的哈希值，并根据nReduce进行取模，以决定键值对分配到哪个Reduce任务
func ihash(key string) int {
    h := fnv.New32a()                 // 使用 FNV-1a 哈希算法初始化哈希函数
    h.Write([]byte(key))              // 将键转换为字节数组并写入哈希函数
    return int(h.Sum32() & 0x7fffffff) // 返回非负的哈希值
}

// Worker 函数是Worker进程的主要入口点，负责循环请求任务、执行任务和报告任务完成
func Worker(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string) {

    for {
        // 请求任务
        args := RequestTaskArgs{}      // 创建一个空的请求任务参数
        reply := RequestTaskReply{}    // 创建一个空的请求任务回复结构体

        ok := call("Coordinator.RequestTask", &args, &reply) // 通过RPC调用Coordinator的RequestTask方法
        if !ok {
            log.Fatalf("Failed to call Coordinator.RequestTask") // 如果RPC调用失败，记录错误并退出
            return
        }

        // 根据任务类型执行相应操作
        if reply.TaskType == MapTask {
            // 执行 Map 任务
            doMapTask(reply, mapf)                    // 调用doMapTask函数执行Map任务
            // 报告任务完成
            reportTaskDone(reply.TaskType, reply.TaskNumber) // 调用reportTaskDone函数报告Map任务完成
        } else if reply.TaskType == ReduceTask {
            // 执行 Reduce 任务
            doReduceTask(reply, reducef)               // 调用doReduceTask函数执行Reduce任务
            // 报告任务完成
            reportTaskDone(reply.TaskType, reply.TaskNumber) // 调用reportTaskDone函数报告Reduce任务完成
        } else if reply.TaskType == WaitTask {
            // 当前没有可用任务，Worker需要等待一段时间后再请求
            // 可以适当Sleep，防止频繁请求
            time.Sleep(time.Second) // 休眠1秒钟
            continue                 // 继续下一个循环，重新请求任务
        } else if reply.TaskType == ExitTask {
            // 所有任务已完成，Worker可以退出
            return // 退出Worker函数，结束Worker进程
        }
    }
}

// doMapTask 函数负责执行Map任务，包括读取输入文件、调用Map函数、分区中间结果并写入临时文件
func doMapTask(reply RequestTaskReply, mapf func(string, string) []KeyValue) {
    // 读取输入文件
    filename := reply.Filename // 获取输入文件名
    file, err := os.Open(filename) // 打开输入文件
    if err != nil {
        log.Fatalf("cannot open %v", filename) // 如果无法打开文件，记录错误并退出
    }
    content, err := ioutil.ReadAll(file) // 读取文件内容
    if err != nil {
        log.Fatalf("cannot read %v", filename) // 如果无法读取文件内容，记录错误并退出
    }
    file.Close() // 关闭文件

    // 调用 Map 函数
    kva := mapf(filename, string(content)) // 调用Map函数，生成中间键值对切片

    // 创建 nReduce 个临时文件，用于存储分区后的中间结果
    nReduce := reply.NReduce                   // 获取Reduce任务的总数
    tempFiles := make([]*os.File, nReduce)    // 创建一个文件指针切片，长度为nReduce
    encoders := make([]*json.Encoder, nReduce) // 创建一个JSON编码器切片，长度为nReduce
    for i := 0; i < nReduce; i++ {
        // 创建临时文件，文件名格式为"mr-<MapTaskNumber>-<ReduceTaskNumber>-*"
        tempfile, err := ioutil.TempFile("", fmt.Sprintf("mr-%d-%d-*", reply.TaskNumber, i))
        if err != nil {
            log.Fatalf("cannot create temp file") // 如果无法创建临时文件，记录错误并退出
        }
        tempFiles[i] = tempfile                    // 将临时文件指针存入切片
        encoders[i] = json.NewEncoder(tempfile)   // 为每个临时文件创建一个JSON编码器
    }

    // 将中间键值对写入对应的临时文件
    for _, kv := range kva {
        reduceTaskNumber := ihash(kv.Key) % nReduce // 根据键的哈希值决定分区编号
        err := encoders[reduceTaskNumber].Encode(&kv) // 将键值对编码为JSON并写入对应的临时文件
        if err != nil {
            log.Fatalf("cannot encode kv pair") // 如果编码失败，记录错误并退出
        }
    }

    // 关闭并重命名临时文件，最终文件名格式为"mr-<MapTaskNumber>-<ReduceTaskNumber>"
    for i := 0; i < nReduce; i++ {
        tempFiles[i].Close()                           // 关闭临时文件
        finalName := fmt.Sprintf("mr-%d-%d", reply.TaskNumber, i) // 生成最终文件名
        os.Rename(tempFiles[i].Name(), finalName)       // 原子性重命名临时文件为最终文件名
    }
}

// doReduceTask 函数负责执行Reduce任务，包括读取所有相关的中间文件、排序、调用Reduce函数并写入输出文件
func doReduceTask(reply RequestTaskReply, reducef func(string, []string) string) {
    // 读取所有相关的中间文件
    nMap := reply.NMap                    // 获取Map任务的总数
    intermediate := []KeyValue{}          // 初始化一个空的KeyValue切片，用于存储所有中间结果

    for i := 0; i < nMap; i++ {
        filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskNumber) // 生成中间文件名，格式为"mr-<MapTaskNumber>-<ReduceTaskNumber>"
        file, err := os.Open(filename) // 打开中间文件
        if err != nil {
            continue // 如果文件不存在（可能Map任务尚未完成或失败），则跳过
        }
        dec := json.NewDecoder(file) // 创建一个JSON解码器
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
                break // 如果解码出错（通常是文件结束），则退出循环
            }
            intermediate = append(intermediate, kv) // 将解码后的键值对添加到中间结果切片中
        }
        file.Close() // 关闭文件
    }

    // 排序中间结果，按键排序
    sort.Sort(ByKey(intermediate)) // 使用ByKey类型的排序方法对中间结果进行排序

    // 创建临时输出文件，用于存储Reduce任务的最终结果
    oname := fmt.Sprintf("mr-out-%d", reply.TaskNumber) // 生成输出文件名，格式为"mr-out-<ReduceTaskNumber>"
    tempfile, err := ioutil.TempFile("", oname+"-*")  // 创建一个临时文件，文件名包含输出文件名作为前缀
    if err != nil {
        log.Fatalf("cannot create temp file") // 如果无法创建临时文件，记录错误并退出
    }

    // 执行 Reduce 操作
    i := 0
    for i < len(intermediate) {
        j := i + 1
        // 找到所有具有相同键的连续元素
        for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
            j++
        }
        // 收集所有相同键的值
        values := []string{}
        for k := i; k < j; k++ {
            values = append(values, intermediate[k].Value)
        }
        // 调用 Reduce 函数处理当前键及其所有值
        output := reducef(intermediate[i].Key, values)

        // 将Reduce函数的输出写入临时输出文件，格式为"<Key> <Value>\n"
        fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)

        i = j // 移动到下一个不同键的起始位置
    }

    tempfile.Close() // 关闭临时输出文件
    // 原子性重命名临时文件为最终输出文件名
    os.Rename(tempfile.Name(), oname)
}

// reportTaskDone 函数用于向Coordinator报告任务完成的情况
func reportTaskDone(taskType TaskType, taskNumber int) {
    args := ReportTaskArgs{
        TaskType:   taskType,   // 设置任务类型：MapTask或ReduceTask
        TaskNumber: taskNumber, // 设置任务编号
    }
    reply := ReportTaskReply{} // 创建一个空的报告任务完成的回复结构体
    ok := call("Coordinator.ReportTask", &args, &reply) // 通过RPC调用Coordinator的ReportTask方法
    if !ok {
        log.Fatalf("Failed to call Coordinator.ReportTask") // 如果RPC调用失败，记录错误并退出
    }
}

// call 函数用于发起RPC调用，向指定的RPC方法发送参数并接收回复
func call(rpcname string, args interface{}, reply interface{}) bool {
    sockname := coordinatorSock()                 // 获取Coordinator的UNIX套接字文件名
    c, err := rpc.DialHTTP("unix", sockname)      // 通过UNIX域套接字连接到Coordinator的RPC服务器
    if err != nil {
        log.Fatal("dialing:", err)                // 如果连接失败，记录错误并退出
    }
    defer c.Close()                               // 函数结束时关闭RPC连接

    err = c.Call(rpcname, args, reply)            // 发起RPC调用，调用指定的方法名，传递参数并接收回复
    if err == nil {
        return true                                 // 如果调用成功，返回true
    }

    fmt.Println(err)                               // 如果调用失败，打印错误信息
    return false                                    // 返回false，表示调用失败
}
