package main

//
// 简单的顺序MapReduce。
//
// 通过命令 `go run mrsequential.go wc.so pg*.txt` 运行
//

import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// 通过键进行排序的类型定义。
type ByKey []mr.KeyValue
// KeyValue :键值对存储

// ByKey类型的排序方法。
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
    // 命令示例：go run mrsequential.go wc.so pg*.txt (three)
    if len(os.Args) < 3 {
        fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
        os.Exit(1)
    }

    mapf, reducef := loadPlugin(os.Args[1])
	// mapf 和 reducef 是从一个插件文件加载的两个函数，分别代表Map函数和Reduce函数。

    //
    // 读取每个输入文件，
    // 将其传递给Map函数，
    // 累积中间Map输出。
    //
    intermediate := []mr.KeyValue{}
	// 存储汇总：
	// 例如：文件: "apple banana apple apple cherry cherry"
	// [
	// 	mr.KeyValue{"apple", "1"},
	// 	mr.KeyValue{"banana", "1"},
	// 	mr.KeyValue{"apple", "1"},
	//  ......
	// 	mr.KeyValue{"cherry", "1"},
	// 	mr.KeyValue{"cherry", "1"}
	// ]
	
    for _, filename := range os.Args[2:] {
		// 占位符，用于忽略循环中返回的索引
        file, err := os.Open(filename)
		// file 文件的指针；err:如果有错误的标记
        if err != nil {
            log.Fatalf("cannot open %v", filename)
        }
        content, err := ioutil.ReadAll(file)
		// 读取这个文件，存放内容到content
        if err != nil {
            log.Fatalf("cannot read %v", filename)
        }
        file.Close()
        kva := mapf(filename, string(content))
        intermediate = append(intermediate, kva...)
		// 举例：
		// data.txt :apple orange banana apple
		// mapf(data.txt,"apple orange banana apple")
		// kva的值:
		// [
		//     mr.KeyValue{Key: "apple", Value: "1"},
		//     mr.KeyValue{Key: "orange", Value: "1"},
		//     mr.KeyValue{Key: "banana", Value: "1"},
		//     mr.KeyValue{Key: "apple", Value: "1"}
		// ]
		// 
		// intermediate = append(intermediate, kva...) 
		// 参数1：原始列表；参数2：添加的数
		// ... 这三个点意味着将kva切片中的每个元素添加到intermediate切片中
    }

    //
    // 与真实的MapReduce的一个大区别是，
    // 所有中间数据都在一个地方，即 intermediate[]，
    // 而不是被分配到NxM的桶中。
    //
    sort.Sort(ByKey(intermediate)) //按照键值对排序
	// 此时已经排好序了例如：
 	//  mr.KeyValue{Key: "apple", Value: "1"},
	//  mr.KeyValue{Key: "apple", Value: "1"},
	//  mr.KeyValue{Key: "banana", Value: "1"},
	//  mr.KeyValue{Key: "orange", Value: "1"}


    oname := "mr-out-0" // 目标文件名
    ofile, _ := os.Create(oname) // 创建文件

    //
    // 对 intermediate[] 中的每个独立键调用Reduce，
    // 并将结果输出到文件 mr-out-0。
    //
    i := 0
	// 初始化 i，从 intermediate 切片的开始处遍历
	for i < len(intermediate) {
		// 设置 j 为当前键的下一个位置
		j := i + 1

		// 找出所有具有相同键的连续元素
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// 初始化一个字符串切片来收集所有具有相同键的值
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 调用 reduce 函数处理当前键及其所有值，生成输出结果
		output := reducef(intermediate[i].Key, values)

		// 将得到的输出以正确格式写入文件。每一行包含一个键和它的归约结果
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		// 移动 i 到下一个不同键的起始位置
		i = j
	}

    ofile.Close()
}

// 下面如何加载文件的不是很重要

// 从插件文件加载应用程序的Map和Reduce函数，
// 例如 ../mrapps/wc.so
// loadPlugin 加载具有指定名称的插件文件，并尝试从中检索Map和Reduce函数。

// 参数 filename 是插件文件的路径，该文件应包含所需的Map和Reduce函数。
// 返回两个函数：一个是Map函数，另一个是Reduce函数。
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
    // 使用 plugin 包的 Open 函数尝试打开指定的插件文件。
    p, err := plugin.Open(filename)
    // 如果无法打开文件，使用 log.Fatalf 记录错误并终止程序。
    // 这通常指示文件不存在或文件不是有效的共享对象。
    if err != nil {
        log.Fatalf("cannot load plugin %v", filename)
    }

    // 使用 Lookup 方法尝试在插件中找到名为 "Map" 的符号。
    // 这里假设插件中有一个名为 Map 的函数。
    xmapf, err := p.Lookup("Map")
    // 如果找不到 Map 函数，记录错误并终止程序。
    if err != nil {
        log.Fatalf("cannot find Map in %v", filename)
    }
    // 类型断言，将从插件中获取的Map符号（接口类型）转换为适当的函数类型。
    // 这里假设 Map 函数的签名为 func(string, string) []mr.KeyValue。
    mapf := xmapf.(func(string, string) []mr.KeyValue)

    // 同样的过程应用于查找 Reduce 函数。
    xreducef, err := p.Lookup("Reduce")
    // 如果在插件中找不到 Reduce 函数，记录错误并终止程序。
    if err != nil {
        log.Fatalf("cannot find Reduce in %v", filename)
    }
    // 类型断言，将从插件中获取的Reduce符号（接口类型）转换为适当的函数类型。
    // 这里假设 Reduce 函数的签名为 func(string, []string) string。
    reducef := xreducef.(func(string, []string) string)

    // 返回加载的Map和Reduce函数。
    return mapf, reducef
}

