package mr

import "testing"

func Test_IdleWorker(t *testing.T) {
	coordinator := MakeCoordinator([]string{}, 0)
	workerSocket := "sock1"
	args := &ReportIdleArgs{
		Sockname: workerSocket,
	}
	reply := &ReportIdleReply{}

	coordinator.IdleWorker(args, reply)

	if _, ok := coordinator.workers[workerSocket]; !ok {
		t.Error("expected worker to be known by coordinator")
	}
}
