package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 从coordinator获取一个Task
func getTask() Task {
	task := Task{}
	args := ExampleArgs{}
	ok := call("Coordinator.AssignTask", &args, &task)
	if ok {
		fmt.Printf("get the %v task\n", task.TaskNo)
	} else {
		fmt.Printf("get task failed!\n")
	}
	return task
}

// 处理Task后，将结果发送给Coordinator
func returnTask(task Task) {
	args := ExampleArgs{}
	ok := call("Coordinator.AssignTask", &args, &task)
	if ok {
		fmt.Printf("get the %v task\n", task.TaskNo)
	} else {
		fmt.Printf("get task failed!\n")
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task := getTask()
		fmt.Printf("Worker::获取第%d个任务 %d类型\n", task.TaskNo, task.TaskType)
		switch task.TaskType {
		case Map:
			content, err := ioutil.ReadFile(task.FileName)
			if err != nil {
				log.Fatal("Failed to read file: "+task.FileName, err)
			}
			kvs := mapf(task.FileName, string(content)) // 调用mapf把内容转化为kv
			kvmap := make(map[int][]KeyValue)           //key:reduce号  v:kv 列表
			//遍历kvs，取出kv 按照key的哈希分区
			for _, kv := range kvs {
				kvmap[ihash(kv.Key)%task.NReduce] = append(kvmap[ihash(kv.Key)%task.NReduce], kv)
			}
			mapResultNames := make(map[int][]string, 0)
			// 将kvmap存储到文件
			for reduceNo, kvs := range kvmap {
				dir, _ := os.Getwd()
				tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
				if err != nil {
					log.Fatal("Failed to create temp file", err)
				}
				enc := json.NewEncoder(tempFile)
				for _, kv := range kvs {
					if err := enc.Encode(&kv); err != nil {
						log.Fatal("Failed to write kv pair", err)
					}
				}
				tempFile.Close()
				outputName := fmt.Sprintf("mr-%d-%d", task.TaskNo, reduceNo)
				os.Rename(tempFile.Name(), outputName)
				filepath.Join(dir, outputName)
				//将文件路径保存
				mapResultNames[reduceNo] = append(mapResultNames[reduceNo], outputName)
			}
			fmt.Printf("Worker::第%d个任务,发回了%d个文件结果\n", task.TaskNo, len(mapResultNames))
			for key, value := range task.MapResultNames {
				fmt.Printf("ReduceNo::%d\n", key)
				fmt.Printf("%v\n", value)
			}
			//将文件路径map装入task 发回Coordinator
			task.MapResultNames = mapResultNames
			returnTask(task)
		case Reduce:

		case Wait:
			time.Sleep(5 * time.Second)
		}
	}

}

//
// example function to show how to make an RPC call to the coordintor.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
