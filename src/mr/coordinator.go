package mr

import (
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
	MapTasks            []TaskDetail
	CoordinatorMutex    sync.Mutex
	MapTasksNum         int
	FinishedMapTasksNum int

	ReduceTasks            []TaskDetail
	ReduceTasksNum         int
	FinishedReduceTasksNum int

	CurrentStage int // 1 for Map, 2 for Reduce, 3 for closing, 4 for done
	// CurrentStageMutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *TaskRequestArg, reply *TaskRequestReply) error {
	// Avoid cocurrent by using mutex
	c.CoordinatorMutex.Lock()
	defer c.CoordinatorMutex.Unlock()

	if c.CurrentStage == 1 {
		for id, task := range c.MapTasks {
			if task.State == 0 {
				c.MapTasks[id].State = 1
				c.MapTasks[id].DispatchTime = time.Now()
				reply.Code = 0
				reply.Message = "OK"
				reply.Task = c.MapTasks[id]
				return nil
			} else if task.State == 1 && time.Now().Sub(task.DispatchTime) > 10*time.Second {
				c.MapTasks[id].State = 1
				c.MapTasks[id].DispatchTime = time.Now()
				reply.Code = 0
				reply.Message = "OK"
				reply.Task = c.MapTasks[id]
				return nil
			}
		}
	} else if c.CurrentStage == 2 {
		// Reduce

		for id, task := range c.ReduceTasks {
			if task.State == 0 {
				c.ReduceTasks[id].State = 1
				c.ReduceTasks[id].DispatchTime = time.Now()
				reply.Code = 0
				reply.Message = "OK"
				reply.Task = c.ReduceTasks[id]
				return nil
			} else if task.State == 1 && time.Now().Sub(task.DispatchTime) > 10*time.Second {
				c.ReduceTasks[id].State = 1
				c.ReduceTasks[id].DispatchTime = time.Now()
				reply.Code = 0
				reply.Message = "OK"
				reply.Task = c.ReduceTasks[id]
				return nil
			}
		}
	} else if c.CurrentStage == 3 {
		reply.Code = -2
		// reply.Message = "All tasks are done. Closing..."
		return nil
	}
	reply.Code = -1
	reply.Message = "No more tasks"
	return nil
}

func (c *Coordinator) ChangeTaskState(args *TaskStateChangeArg, reply *TaskRequestReply) error {
	// Avoid cocurrent by using mutex
	c.CoordinatorMutex.Lock()
	defer c.CoordinatorMutex.Unlock()
	if c.CurrentStage == 1 {

		if args.Type == 1 && c.MapTasks[args.MapTaskId].State == 1 {
			c.MapTasks[args.MapTaskId].State = 2
			c.MapTasks[args.MapTaskId].MapOutputPaths = args.MapIntermediateFilePaths
			reply.Code = 0
			reply.Message = "OK"
			c.FinishedMapTasksNum++
			// fmt.Printf("MapTask %v finished!\n", args.MapTaskId)
		}
		if c.FinishedMapTasksNum == c.MapTasksNum {
			for i := 0; i < c.ReduceTasksNum; i++ {
				reduceTask := TaskDetail{
					Type:           2,
					State:          0,
					ReduceOutputId: i,
				}
				for _, task := range c.MapTasks {
					reduceTask.ReduceInputPaths = append(reduceTask.ReduceInputPaths, task.MapOutputPaths[i])
				}
				c.ReduceTasks = append(c.ReduceTasks, reduceTask)
			}
			// fmt.Printf("%v\n", c.ReduceTasks)
			c.CurrentStage = 2
			// fmt.Printf("All MapTask finished! Now in Reduce stage!\n")
		}
	} else if c.CurrentStage == 2 {
		if args.Type == 2 && c.ReduceTasks[args.ReduceOutputId].State == 1 {
			c.ReduceTasks[args.ReduceOutputId].State = 2
			reply.Code = 0
			reply.Message = "OK"
			c.FinishedReduceTasksNum++
			// fmt.Printf("ReduceTask %v finished!\n", args.MapTaskId)
		}
		if c.FinishedReduceTasksNum == c.ReduceTasksNum {
			// fmt.Printf("All ReduceTask finished! Now in Closing stage!\n")
			c.CurrentStage = 3
		}

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
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Avoid cocurrent by using mutex
	c.CoordinatorMutex.Lock()
	defer c.CoordinatorMutex.Unlock()
	ret := false

	// Your code here.

	if c.CurrentStage == 3 {
		time.Sleep(15 * time.Second)
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		CurrentStage:           1,
		MapTasksNum:            0,
		FinishedMapTasksNum:    0,
		ReduceTasksNum:         nReduce,
		FinishedReduceTasksNum: 0,
	}

	// Your code here.

	for id, filename := range files {
		task := TaskDetail{
			Type:         1,
			State:        0,
			MapInputPath: filename,
			MapTaskId:    id,
			NReduce:      nReduce,
		}
		c.MapTasks = append(c.MapTasks, task)
		c.MapTasksNum++
	}

	c.server()
	return &c
}
