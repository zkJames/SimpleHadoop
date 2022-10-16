package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// 互斥锁
var mu sync.Mutex

// 任务类型枚举
type Type int

const (
	Map Type = iota
	Reduce
	Wait
	Stop
)

// 协调者观察任务的分配状况
type Status int

const (
	UnAssigned Status = iota //未分配
	Assigned                 //已分配
	Finished                 //完成
)

//任务
type Task struct {
	TaskNo         int              //任务序号
	FileName       string           //文件名
	NReduce        int              //Reducer数量，用于中间结果拆分
	TaskType       Type             //标识任务种类
	MapResultNames map[int][]string //记录Map返回值，key reduce号，value 文件地址
}

//任务状态结构体
type TaskStatus struct {
	StatusNow Status //此任务当前的状态
	TaskRef   *Task  //指向任务
}

// 定义在Coordinator中的变量
type Coordinator struct {
	// Your definitions here.
	TaskQueue       chan *Task          //任务队列
	TaskStatusMap   map[int]*TaskStatus //记录任务状态的map,key序列号，value任务状态
	NReduce         int                 //记录有多少个Reduce
	TotalType       Type                //全局的任务阶段，Map/Reduce
	IntermediateMap map[int][]string    //中间文件表，key：reduce号，value：文件名数组
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
	ret := false

	// Your code here.

	return ret
}

// 由worker调用，分配任务
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	// 从队列取一个任务
	mu.Lock()
	defer mu.Unlock()
	// 如果队列不为空
	if len(c.TaskQueue) != 0 {
		// 出队一个Task
		*reply = *<-c.TaskQueue
		c.TaskStatusMap[reply.TaskNo].StatusNow = Assigned
	} else {
		*reply = Task{TaskType: Wait}
	}
	return nil
}

// 接收到mapper处理过后的task
func (c *Coordinator) ReceiveBackTask(args *ExampleArgs, task *Task) error {
	mu.Lock()
	defer mu.Unlock()
	// 如果在Reduce阶段收到了迟来的MapTask返回，或者此任务已经完成，应该丢弃
	if task.TaskType != c.TotalType || c.TaskStatusMap[task.TaskNo].StatusNow == Finished {
		return nil
	}
	fmt.Printf("coordinator:::::%d 任务已完成\n", task.TaskNo)
	c.TaskStatusMap[task.TaskNo].StatusNow = Finished //标记此任务完成，若每个Task都拥有了此标记，则退出Map
	// 启动协程， 保存中间文件的路径
	go c.handleTaskResult(task)
	return nil
}

// 解析任务，存储信息
func (c *Coordinator) handleTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	// 按照reduceNo，保存返回的文件路径
	for reduceNo, filePaths := range task.MapResultNames {
		c.IntermediateMap[reduceNo] = append(c.IntermediateMap[reduceNo], filePaths...)
	}
	fmt.Printf("存入了第 %v 个任务中间文件路径\n", task.TaskNo)
	// 如果任务全部完成了，全局状态转换为Reduce
	if c.isAllFinished() {
		c.TotalType = Reduce
		fmt.Printf("Map任务已经全部完成\n")
	}
}

// 遍历状态表，检测是否全部完成
func (c *Coordinator) isAllFinished() bool {
	for _, task := range c.TaskStatusMap {
		if task.StatusNow != Finished {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:       make(chan *Task, len(files)),
		TaskStatusMap:   make(map[int]*TaskStatus),
		NReduce:         nReduce,
		TotalType:       Map,
		IntermediateMap: make(map[int][]string),
	}
	// 创建Map任务
	fmt.Printf("Coordinator::开始创建Map任务\n")
	//根据文件数量创建Task
	for index, fileName := range files {
		fmt.Printf("Coordinator::第 %d 个任务完成创建\n", index)
		// 初始化Task
		task := Task{
			TaskNo:   index,
			NReduce:  nReduce,
			TaskType: Map,
			FileName: fileName,
		}
		// 把任务状态存入map中
		c.TaskStatusMap[index] = &TaskStatus{
			TaskRef:   &task,
			StatusNow: UnAssigned,
		}
		// 把Task入队
		c.TaskQueue <- &task
	}
	c.server()
	return &c
}
