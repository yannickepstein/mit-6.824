package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Worker struct {
	socket string
	status string
}

type Coordinator struct {
	workers map[string]Worker
}

//
// RPC handler to register that a worker is idle
// Should also be used to register a new worker
//
func (c *Coordinator) IdleWorker(args *ReportIdleArgs, reply *ReportIdleReply) error {
	if worker, ok := c.workers[args.Sockname]; ok {
		worker.status = "IDLE"
		return nil
	}
	c.workers[args.Sockname] = Worker{
		socket: args.Sockname,
		status: "IDLE",
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
	c := Coordinator{
		workers: map[string]Worker{},
	}

	// Your code here.

	c.server()
	return &c
}
