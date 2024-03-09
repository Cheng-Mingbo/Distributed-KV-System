package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType = uint8
type SysState = uint8
type TaskState = uint8

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	SysMap SysState = iota
	SysReduce
	SysDone
)

const (
	TaskInit TaskState = iota
	TaskRun
	TaskDone
)

type Task struct {
	TaskId    int
	TaskType  TaskType
	TaskState TaskState
	NReduce   uint
	StartTime time.Time

	Input []string // map: file name; reduce: intermediate file name
}

type Coordinator struct {
	// Your definitions here.
	mtx            sync.Mutex
	MapTasks       chan *Task
	ReduceTasks    chan *Task
	SysState       SysState
	NumReduce      int
	NumMap         int
	NumDoneReduce  int
	NumDoneMap     int
	Timeout        time.Duration
	MapTasksMap    map[int]*Task
	ReduceTasksMap map[int]*Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkerHandler(args *WorkerArgs, reply *WorkerReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	switch c.SysState {
	case SysMap:
		select {
		case reply.Task = <-c.MapTasks:
			c.setTaskToRun(reply.Task)
		default:
			reply.Task = c.tryGetTaskFrom(c.MapTasksMap)
		}
	case SysReduce:
		select {
		case reply.Task = <-c.ReduceTasks:
			c.setTaskToRun(reply.Task)
		default:
			reply.Task = c.tryGetTaskFrom(c.ReduceTasksMap)
		}
	case SysDone:
		reply.WorkerState = WorkerDone
	}

	return nil
}

func (c *Coordinator) setTaskToRun(task *Task) {
	task.TaskState = TaskRun
	task.StartTime = time.Now()
}

func (c *Coordinator) tryGetTaskFrom(mp map[int]*Task) *Task {
	var task *Task
	for _, mapTask := range mp {
		if mapTask.TaskState == TaskRun && time.Since(mapTask.StartTime) > c.Timeout {
			c.setTaskToRun(mapTask)
			task = mapTask
			return task
		} else if mapTask.TaskState == TaskDone {
			delete(mp, mapTask.TaskId)
		}
	}
	return nil
}

// DoneHandler is called when worker done task
func (c *Coordinator) DoneHandler(args *WorkerArgs, reply *WorkerReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	task := args.Task
	c.setTaskDone(task)

	switch c.SysState {
	case SysMap:
		if c.NumDoneMap == c.NumMap {
			c.mapToReduce()
		}
	default:
		if c.NumDoneReduce == c.NumReduce {
			c.reduceToDone()
		}
	}
	return nil
}

func (c *Coordinator) reduceToDone() {
	c.setSysState(SysDone)
}

func (c *Coordinator) setSysState(state SysState) {
	c.SysState = state
}

func (c *Coordinator) mapToReduce() {
	c.setSysState(SysReduce)
	for i := 0; i < c.NumReduce; i++ {
		var input []string
		for j := 0; j < c.NumMap; j++ {
			input = append(input, fmt.Sprintf("mr-%d-%d.tmp", j, i))
		}
		task := &Task{
			TaskId:    i,
			TaskType:  ReduceTask,
			TaskState: TaskInit,
			NReduce:   uint(c.NumReduce),
			StartTime: time.Now(),
			Input:     input,
		}
		c.ReduceTasks <- task
		c.ReduceTasksMap[i] = task
	}
}

func (c *Coordinator) setTaskDone(task *Task) {
	if task.TaskState == TaskRun {
		task.TaskState = TaskDone
		switch task.TaskType {
		case MapTask:
			c.NumDoneMap++
			c.MapTasksMap[task.TaskId].TaskState = TaskDone
		case ReduceTask:
			c.NumDoneReduce++
			c.ReduceTasksMap[task.TaskId].TaskState = TaskDone
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.SysState == SysDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NumReduce = nReduce
	c.NumMap = len(files)
	c.NumDoneReduce = 0
	c.NumDoneMap = 0
	c.SysState = SysMap
	c.MapTasks = make(chan *Task, c.NumMap)
	c.ReduceTasks = make(chan *Task, c.NumReduce)
	c.MapTasksMap = make(map[int]*Task)
	c.ReduceTasksMap = make(map[int]*Task)
	c.Timeout = time.Second * 10

	for i, file := range files {
		input := []string{file}
		task := &Task{
			TaskId:    i,
			TaskType:  MapTask,
			TaskState: TaskInit,
			NReduce:   uint(nReduce),
			StartTime: time.Now(),
			Input:     input,
		}
		c.MapTasks <- task
		c.MapTasksMap[i] = task
	}
	c.server()
	return &c
}
