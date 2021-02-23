package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Master struct {
	// Your definitions here.
	MapTasks    []string
	ReduceTasks []string

	Tasks        map[string]int
	WorkerCount  int
	TimerCounter int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	//reply.FileName = m.Tasks[m.Files[0]]
	workerId := args.NodeId
	if args.NodeId == 0 {
		workerId = m.WorkerCount
		m.WorkerCount += 1
	}
	if len(m.MapTasks) != 0 {
		err := assignMapTask(
			m, workerId, args, reply)
		return err
	} else if len(m.ReduceTasks) != 0 {
		err := assignReduceTask(
			m, workerId, args, reply)
		return err
	}
	return nil
}

func (m *Master) NotifyReduceTaskFinished(args *NotifyReduceTaskFinishedArgs, reply *NotifyReduceTaskFinishedReply) error {
	println("Map")
	fmt.Printf("%v", m.MapTasks)
	println()
	if m.Tasks[args.FileName] == 0 {
		m.Tasks[args.FileName] = args.NodeId
		println(args.TaskType)
		if args.TaskType == 0 {
			m.ReduceTasks = append(m.ReduceTasks, args.IntermediateFile)
			println("Reduce")
			fmt.Printf("%v", m.ReduceTasks)
			println()
		}
		os.Rename(args.Tempfile, args.IntermediateFile)
	}
	return nil
}

func assignMapTask(m *Master, workerId int, args *GetTaskArgs, reply *GetTaskReply) error {
	currentFile := m.MapTasks[0]
	m.MapTasks = m.MapTasks[1:]
	reply.FileName = currentFile
	reply.NodeId = workerId
	reply.TaskType = 0
	timer := time.NewTimer(3 * time.Second)
	m.TimerCounter += 1
	go func(filename string) {
		<-timer.C
		if m.Tasks[filename] == 0 {
			m.MapTasks = append(m.MapTasks, filename)
		}
		m.TimerCounter -= 1
	}(currentFile)
	return nil
}

func assignReduceTask(m *Master, workerId int, args *GetTaskArgs, reply *GetTaskReply) error {
	currentFile := m.ReduceTasks[0]
	m.ReduceTasks = m.ReduceTasks[1:]
	reply.FileName = currentFile
	reply.NodeId = workerId
	reply.TaskType = 1
	timer := time.NewTimer(3 * time.Second)
	m.TimerCounter += 1
	go func(filename string, taskType int) {
		<-timer.C
		if m.Tasks[filename] == 0 {
			m.ReduceTasks = append(m.ReduceTasks, filename)
		}
		m.TimerCounter -= 1
	}(currentFile, reply.TaskType)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if len(m.MapTasks) == 0 && len(m.ReduceTasks) == 0 && m.TimerCounter == 0 {
		print("Task Done")
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		WorkerCount: 1,
	}
	m.MapTasks = files[1:]
	fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
	var tasks = map[string]int{}
	for _, file := range files {
		tasks[file] = 0
	}
	m.Tasks = tasks
	// Your code here.
	m.server()
	return &m
}
