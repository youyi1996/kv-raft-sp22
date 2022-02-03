package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks      []TaskDetail
	MapTasksMutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *TaskRequestArg, reply *TaskRequestReply) error {

	// Avoid cocurrent by using mutex
	c.MapTasksMutex.Lock()
	defer c.MapTasksMutex.Unlock()

	for id, task := range c.MapTasks {
		if task.State == 0 {
			c.MapTasks[id].State = 1
			reply.Code = 0
			reply.Message = "OK"
			reply.Task = task
			time.Sleep(1 * time.Second)
			return nil
		} else {
			continue
		}
	}
	reply.Code = -1
	reply.Message = "No more tasks"
	return nil
}

func (c *Coordinator) ChangeTaskState(args *TaskStateChangeArg, reply *TaskRequestReply) error {

	// Avoid cocurrent by using mutex
	c.MapTasksMutex.Lock()
	defer c.MapTasksMutex.Unlock()

	if args.Type == 1 {
		c.MapTasks[args.MapTaskId].State = 2
		reply.Code = 0
		reply.Message = "OK"
		fmt.Printf("MapTask %v finished!\n", args.MapTaskId)
		return nil
	}

	return nil
}

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
	fmt.Printf("%v\n", c.MapTasks)
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

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	for id, filename := range files {
		task := TaskDetail{
			Type:         1,
			MapInputPath: filename,
			MapTaskId:    id,
			NReduce:      nReduce,
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	c.server()
	return &c
}
