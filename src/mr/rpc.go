package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	sockPrefix = "/var/tmp/824-mr-"
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

type ReportIdleArgs struct {
	Sockname string
}

type ReportIdleReply struct{}

type ResponseErr struct {
	Message string
}

func (re ResponseErr) Error() string {
	return re.Message
}

// Add your RPC definitions here.
func workerSock() string {
	return uniqueSock()
}

func coordinatorSock() string {
	return uniqueSock()
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func uniqueSock() string {
	s := sockPrefix + strconv.Itoa(os.Getuid())
	return s
}
