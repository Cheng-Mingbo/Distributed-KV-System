package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := WorkerArgs{
			WorkerState: WorkerInit,
		}
		reply := WorkerReply{}

		ok := call("Coordinator.WorkerHandler", &args, &reply)
		if !ok {
			log.Println("call failed!")
			break
		}
		if reply.WorkerState == WorkerDone {
			break
		}

		task := reply.Task
		if task == nil {
			continue
		}
		switch task.TaskType {
		case MapTask:
			mapTaskHandler(mapf, reply.Task)
		case ReduceTask:
			reduceTaskHandler(reducef, reply.Task)
		}
	}
}

func mapTaskHandler(mapf func(string, string) []KeyValue, task *Task) {
	filename := task.Input[0]
	content, _ := os.ReadFile(filename)

	intermediate := mapf(filename, string(content))
	partitioned := partition(intermediate, int(task.NReduce))
	writePartitions(partitioned, task)
	reportTaskDone(task)
}

func reduceTaskHandler(reducef func(string, []string) string, task *Task) {
	kva := readAllKeyValues(task.Input)
	sort.Sort(ByKey(kva))
	writeReduceOutput(kva, task, reducef)
	reportTaskDone(task)
}

func writeReduceOutput(kva []KeyValue, task *Task, reducef func(string, []string) string) {
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
}

func readAllKeyValues(filenames []string) []KeyValue {
	var kva []KeyValue
	for _, filename := range filenames {
		content, err := os.ReadFile(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(bytes.NewReader(content))
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

func partition(intermediate []KeyValue, nReduce int) [][]KeyValue {
	partitioned := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		i := ihash(kv.Key) % nReduce
		partitioned[i] = append(partitioned[i], kv)
	}
	return partitioned
}

func writePartitions(partitioned [][]KeyValue, task *Task) {
	for i, partition := range partitioned {
		filename := fmt.Sprintf("mr-%d-%d.tmp", task.TaskId, i)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		for _, kv := range partition {
			_ = enc.Encode(&kv)
		}
		_ = file.Close()
	}
}

func reportTaskDone(task *Task) {
	args := WorkerArgs{
		WorkerState: WorkerDone,
		Task:        task,
	}
	reply := WorkerReply{}
	ok := call("Coordinator.DoneHandler", &args, &reply)
	if !ok {
		log.Println("call failed!")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
