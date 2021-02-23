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
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var workerId int

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

	// uncomment to send the Example RPC to the master.
	//CallExample()
	reply := GetTask()
	if reply.TaskType == 0 {
		mapfunc(mapf, reply)
	} else {
		reduceFunc(reducef, reply)
	}
}

func mapfunc(mapf func(string, string) []KeyValue, reply GetTaskReply) {
	filename := reply.FileName
	intermediate := []KeyValue{}

	// Read File

	//execute Map Fucntion
	content := readFile(filename)
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	// Write to file

	//intermediateFile, _ := os.Create(tmpFile)

	var intermediateByKey = make([][]KeyValue, 10)
	for _, kv := range intermediate {
		key := ihash(kv.Key) % 10
		intermediateByKey[key] = append(intermediateByKey[key], kv)
	}
	print(len(intermediate))
	for _, kvs := range intermediateByKey {

		oname := "mr-" + strconv.Itoa(workerId) + "-" + strconv.Itoa(ihash(kvs[0].Key))
		tmpFile, err := ioutil.TempFile("/tmp", oname)
		enc := json.NewEncoder(tmpFile)
		err = enc.Encode(&kvs)
		for _, kv := range kvs {
			if err = enc.Encode(&kv); err != nil {
				log.Fatalf("JSON parsed error")
			}
		}
		tmpFile.Close()
		notifyReduceTaskFinished(filename, oname, 0, tmpFile.Name())
	}
}

func reduceFunc(reducef func(string, []string) string, reply GetTaskReply) {
	intermediate := []KeyValue{}
	file, err := os.Open(reply.FileName)
	print(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	oname := "mr-out-final" + strconv.Itoa(workerId)
	tmpFile, err := ioutil.TempFile("/tmp", "mr-out-final"+strconv.Itoa(workerId))
	if err != nil {
		log.Fatalf("cannot create file")
	}
	//

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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpFile.Close()
	notifyReduceTaskFinished(reply.FileName, oname, 1, tmpFile.Name())
}

func readFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func notifyReduceTaskFinished(filename string, intermediateFile string, taskType int, tempFile string) {
	args := NotifyReduceTaskFinishedArgs{
		TaskType:         taskType,
		FileName:         filename,
		IntermediateFile: intermediateFile,
		Tempfile:         tempFile,
		NodeId:           workerId}
	print(taskType)
	reply := NotifyReduceTaskFinishedReply{}
	call("Master.NotifyReduceTaskFinished", &args, &reply)
}

func GetTask() GetTaskReply {
	args := GetTaskArgs{}
	if workerId != 0 {
		args.NodeId = workerId
	}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	workerId = reply.NodeId
	fmt.Printf("replay %v\n", reply.FileName)
	return reply
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
