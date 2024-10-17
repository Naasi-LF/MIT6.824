package main

//
// 启动协调器进程，实现在 ../mr/coordinator.go 中
//
// 通过命令 `go run mrcoordinator.go pg*.txt` 运行
//
// 请不要更改此文件。
//

import "6.5840/mr"
import "time"
import "os"
import "fmt"


func main() {
    if len(os.Args) < 2 {
        // 如果命令行参数不足，打印用法说明到标准错误输出并退出
        fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
        os.Exit(1)
    }
	// go run mrcoordinator.go pg*.txt

    // 创建一个协调器，参数为输入文件列表和固定的数字10
    m := mr.MakeCoordinator(os.Args[1:], 10)
	// go run mrcoordinator.go ||-> pg*.txt <-||
	// MakeCoordinator需要实现

    // 持续检查是否所有任务都已完成
    for m.Done() == false {
        time.Sleep(time.Second)
    }
	// Done需要实现
    // 所有任务完成后，额外等待一秒以确保所有过程都已正确结束
    time.Sleep(time.Second)
}
