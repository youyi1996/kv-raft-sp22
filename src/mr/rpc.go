package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskDetail struct {
	Type             int // 1 for map and 2 for reduce. 0 for invalid tasks
	State            int // 0 Waiting, 1 Running, 2 Finished
	DispatchTime     time.Time
	MapTaskId        int
	MapInputPath     string
	ReduceInputPaths []string
	ReduceOutputId   int
	NReduce          int
}

type TaskRequestArg struct {
}

type TaskRequestReply struct {
	Code    int    // if Code!=0, no tasks assigned. the worker should wait for a period of time and then request again.
	Message string // human readable message for debug purpose.
	Task    TaskDetail
}

type TaskStateChangeArg struct {
	Type           int
	NewState       int
	MapTaskId      int
	ReduceOutputId int
}

type TaskStateChangeReply struct {
	Code    int
	Message string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
