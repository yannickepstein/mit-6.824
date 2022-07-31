package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"context"
	"fmt"
	"net/rpc"
	"os"
	"strconv"

	"github.com/google/uuid"
)

type TaskType int

const (
	sockPrefix = "/var/tmp/824-mr-"

	Map TaskType = iota
	Reduce
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

type Endpoint string

type HealthCheckArgs struct{}

type HealthCheckReply struct{}

type HealthCheckFailedErr struct {
	Message string
}

func (err HealthCheckFailedErr) Error() string {
	return err.Message
}

type RegistrationRequestArgs struct {
	WorkerId       string
	WorkerEndpoint Endpoint
}

type RegistrationRequestReply struct{}

// Map: Map all key value pairs in the given list of files & write to temporary locations
// Reduce: Sort all key value pairs in the given list of files & reduce all values belonging to a key
// -> Assumption: For each key in the given files, all values are present in the files slice
type WorkRequestArgs struct {
	WorkerId       string
	WorkerEndpoint Endpoint
}

// TODO: better distinguish between map and reduce replies
type WorkRequestReply struct {
	TaskId    string
	TaskType  TaskType
	Files     []string
	Partition int
}

type PartitionId int

type AssignMapTaskArgs struct {
	TaskId      string
	File        string
	NPartitions int
}

type DoneMapTaskReply struct {
	PartitionedKVA map[int]string
}

type AssignReduceTaskArgs struct {
	Partition     int
	Intermediates []string
}

type DoneReduceTaskReply struct{}

type ShutdownArgs struct{}

type ShutdownReply struct{}

func newUniqueEndpoint(id uuid.UUID) Endpoint {
	s := sockPrefix + id.String()
	return Endpoint(s)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorEndpoint() Endpoint {
	s := sockPrefix + strconv.Itoa(os.Getuid())
	return Endpoint(s)
}

//
// send and RPC request to the given endpoint, wait for the response
// returns true, if the response was received successfully
// return false, if something goes wrong
//
func call(ctx context.Context, endpoint Endpoint, rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP("unix", string(endpoint))
	if err != nil {
		return fmt.Errorf("failed dialing: %v", err)
	}
	defer c.Close()

	done := make(chan error, 1) // buffer size 1, s.t. the goroutine on the next line does not leak
	go func() {
		done <- c.Call(rpcname, args, reply)
	}()

	select {
	case err := <-done:
		if err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("request timeout: %v", rpcname)
	}
}
