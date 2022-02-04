package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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
		// fmt.Printf("Requesting tasks...\n")
		code, task = RequestTask()
		if code == 0 {
			// fmt.Printf("Task received! %v\n", task)
			ProceedTask(task, mapf, reducef)
			// fmt.Printf("Task finished! \n")
		} else if code == -1 {
			// fmt.Printf("No task assigned, wait for 10 seconds...\n")
			time.Sleep(10 * time.Second)
		} else if code == -2 {
			// fmt.Printf("All finished, exiting...\n")
			os.Exit(0)
		}
	}

}

func ProceedTask(task TaskDetail, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	if task.Type == 1 {
		// Map

		intermediate_file_paths := DoMap(task, mapf)

		args := TaskStateChangeArg{
			Type:                     task.Type,
			MapTaskId:                task.MapTaskId,
			NewState:                 2,
			MapIntermediateFilePaths: intermediate_file_paths,
		}
		reply := TaskStateChangeReply{}

		call("Coordinator.ChangeTaskState", &args, &reply)

	} else if task.Type == 2 {
		// Reduce
		DoReduce(task, reducef)

		args := TaskStateChangeArg{
			Type:           task.Type,
			ReduceOutputId: task.ReduceOutputId,
			NewState:       2,
		}
		reply := TaskStateChangeReply{}

		call("Coordinator.ChangeTaskState", &args, &reply)

	}
}

func DoMap(task TaskDetail, mapf func(string, string) []KeyValue) []string {

	mapInputFile, err := os.Open(task.MapInputPath)
	if err != nil {
		log.Fatalf("cannot open %v", task.MapInputPath)
	}
	content, err := ioutil.ReadAll(mapInputFile)
	if err != nil {
		log.Fatalf("cannot read %v", task.MapInputPath)
	}
	mapInputFile.Close()
	kva := mapf(task.MapInputPath, string(content))

	kvMatrix := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % task.NReduce
		kvMatrix[reduceId] = append(kvMatrix[reduceId], kv)
	}

	ret := make([]string, task.NReduce)

	for reduceId := 0; reduceId < task.NReduce; reduceId++ {

		intermediateTempFile, err := ioutil.TempFile("./", "tempfile-")
		if err != nil {
			log.Fatalf("cannot create a tempfile.")
		}

		enc := json.NewEncoder(intermediateTempFile)
		for _, kv := range kvMatrix[reduceId] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Error occurs when generating JSON.")
			}
		}
		os.Rename(intermediateTempFile.Name(), "mr-"+fmt.Sprint(task.MapTaskId)+"-"+fmt.Sprint(reduceId))
		ret[reduceId] = "mr-" + fmt.Sprint(task.MapTaskId) + "-" + fmt.Sprint(reduceId)
		intermediateTempFile.Close()
	}

	return ret
}

func DoReduce(task TaskDetail, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, filename := range task.ReduceInputPaths {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	ofile, _ := ioutil.TempFile("./", "tempfile-")
	oname := "mr-out-" + fmt.Sprint(task.ReduceOutputId)
	// ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(ofile.Name(), oname)
	ofile.Close()

}

func RequestTask() (int, TaskDetail) {
	args := TaskRequestArg{}
	reply := TaskRequestReply{}
	call("Coordinator.RequestTask", &args, &reply)
	// fmt.Printf("%v\n", reply)
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
