package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

//状态枚举
type Status int

const (
	Map Status = iota
	Reduce
	Wait
	Stop
)

//任务
type Task struct {
	TaskNo   int    //任务序号
	FileName string //文件名
	NReduce  int    //Reducer数量，用于中间结果拆分
	Type     Status //标识0假任务停止,1map,2reduce
}

//任务状态结构体
type TaskStatus struct {
	StatusNow Status //此任务当前的状态
	TaskRef   *Task  //指向任务
}

// 定义在Coordinator中的变量
type Coordinator struct {
	// Your definitions here.
	TaskQueue     chan *Task          //任务队列
	TaskStatusMap map[int]*TaskStatus //记录任务状态的map,key序列号，value任务状态
	NReduce       int                 //记录有多少个Reduce
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:     make(chan *Task, len(files)),
		TaskStatusMap: make(map[int]*TaskStatus),
		NReduce:       nReduce,
	}
	// Your code here.
	// 创建Map任务
	fmt.Printf("Coordinator::开始创建Map任务\n")
	//根据文件数量创建Task
	for index, fileName := range files {
		task := Task{
			TaskNo:   index,
			NReduce:  nReduce,
			Type:     Map,
			FileName: fileName,
		}

	}
	c.server()
	return &c
}
