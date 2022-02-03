package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

var DEBUG_MODE bool = false

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for true {
		var code int
		var task TaskDetail
		fmt.Printf("Requesting tasks...\n")
		code, task = RequestTask()
		if code == 0 {
			fmt.Printf("Task received! %v\n", task)
			ProceedTask(task, mapf, reducef)
			fmt.Printf("Task finished! \n")
		} else {
			fmt.Printf("No task assigned, wait for 10 seconds...\n")
			time.Sleep(10 * time.Second)
		}
	}

}

func ProceedTask(task TaskDetail, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	if task.Type == 1 {
		// Map

		file, err := os.Open(task.MapInputPath)
		if err != nil {
			log.Fatalf("cannot open %v", task.MapInputPath)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.MapInputPath)
		}
		file.Close()
		kva := mapf(task.MapInputPath, string(content))

		fmt.Printf("%v", len(kva))

		args := TaskStateChangeArg{
			MapInputPath: task.MapInputPath,
			NewState:     2,
		}
		reply := TaskStateChangeReply{}

		call("Coordinator.ChangeTaskState", &args, &reply)

		// sort.Sort(ByKey(intermediate))

		// oname := "mr-out-" + fmt.Sprint(task.OutputId)
		// os.Create(oname)

	}
}

func RequestTask() (int, TaskDetail) {
	args := TaskRequestArg{}
	reply := TaskRequestReply{}
	call("Coordinator.RequestTask", &args, &reply)
	fmt.Printf("%v\n", reply)
	return reply.Code, reply.Task
}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
