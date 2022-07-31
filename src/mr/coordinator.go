package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type taskStatus int

const (
	pending taskStatus = iota
	running
	done
)

type taskId string

type mapTask struct {
	Id     taskId
	Status taskStatus
	File   string
}

type mapResult struct {
	TaskId            taskId
	IntermediateFiles map[int]string // one per partition
}

type reduceTask struct {
	Partition         int
	Status            taskStatus
	IntermediateFiles map[string]bool
}

type reduceResult struct {
	Partition int
}

type coordinatedWorker struct {
	WorkerId    string
	Endpoint    Endpoint
	NPartitions int
}

type Coordinator struct {
	done                chan bool
	shutdownWorkers     chan struct{}
	isServerOffline     chan struct{}
	workers             sync.WaitGroup
	nPartitions         int
	scheduledMapTask    chan mapTask
	scheduledReduceTask chan reduceTask
	failedMapTask       chan mapTask
	failedReduceTask    chan reduceTask
	mapResults          chan mapResult
	reduceResults       chan reduceResult
}

//
// create a Coordinator
// main/mrcoordinator.go calls this function
// files is the list of map tasks
// nReduce is the number of reduce tasks to use
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workers:             sync.WaitGroup{},
		isServerOffline:     make(chan struct{}),
		shutdownWorkers:     make(chan struct{}),
		done:                make(chan bool),
		nPartitions:         nReduce,
		scheduledMapTask:    make(chan mapTask, len(files)),
		scheduledReduceTask: make(chan reduceTask, nReduce),
		failedMapTask:       make(chan mapTask, 1),
		failedReduceTask:    make(chan reduceTask, 1),
		mapResults:          make(chan mapResult, 1),
		reduceResults:       make(chan reduceResult, 1),
	}

	c.server()
	c.scheduler(files, nReduce)

	return &c
}

//
// A worker calls this method via RPC to register itself for receiving work
//
func (c *Coordinator) RequestRegistration(args *RegistrationRequestArgs, reply *RegistrationRequestReply) error {
	worker := coordinatedWorker{
		WorkerId:    args.WorkerId,
		Endpoint:    args.WorkerEndpoint,
		NPartitions: c.nPartitions,
	}
	c.registerWorker(worker)
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// Will block until the job has finished.
//
func (c *Coordinator) Done() bool {
	return <-c.done
}

func (c *Coordinator) registerWorker(w coordinatedWorker) {
	log.Printf("worker %v has registered\n", w.WorkerId)
	c.workers.Add(1)
	healthCheckFailure := make(chan bool, 1)
	go func() {
		log.Printf("starting health checker for worker %v\n", w.WorkerId)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-c.shutdownWorkers:
				log.Printf("worker %v has been shut down, stopping health check\n", w.WorkerId)
				return
			case <-t.C:
				if err := pingWorker(w); err != nil {
					fmt.Fprintf(os.Stderr, "health check for worker %s failed", w.WorkerId)
					healthCheckFailure <- true
					return
				}
			}
		}
	}()

	go func() {
		log.Printf("worker %v starts to consume tasks from the scheduler\n", w.WorkerId)
		defer c.workers.Done()
		var mapTasks []mapTask
		var reduceTasks []reduceTask
		for {
			select {
			case task := <-c.scheduledMapTask:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				result, err := w.mapf(ctx, task)
				if err != nil {
					c.failedMapTask <- task
					break
				}
				mapTasks = append(mapTasks, task)
				c.mapResults <- result
			case task := <-c.scheduledReduceTask:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				result, err := w.reducef(ctx, task)
				if err != nil {
					c.failedReduceTask <- task
					break
				}
				reduceTasks = append(reduceTasks, task)
				c.reduceResults <- result
			case <-healthCheckFailure:
				// handling of a health check failure and executing some task cannot happen concurrently
				for _, task := range mapTasks {
					log.Printf("rescheduling map task %v\n", task.Id)
					c.failedMapTask <- task
				}
				for _, task := range reduceTasks {
					log.Printf("rescheduling reduce task %v\n", task.Partition)
					c.failedReduceTask <- task
				}
				return
			case <-c.shutdownWorkers:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err := w.shutdown(ctx)
				if err != nil {
					fmt.Fprintln(os.Stderr, fmt.Errorf("failed to request worker %s shutdown", w.WorkerId))
				}
				return
			}
		}
	}()
}

func (w *coordinatedWorker) mapf(ctx context.Context, task mapTask) (mapResult, error) {
	args := AssignMapTaskArgs{
		TaskId:      string(task.Id),
		File:        task.File,
		NPartitions: w.NPartitions,
	}
	reply := DoneMapTaskReply{}
	err := call(ctx, w.Endpoint, "Worker.AssignMapTask", &args, &reply)
	mapResult := mapResult{}
	if err != nil {
		return mapResult, err
	}
	mapResult.TaskId = task.Id
	mapResult.IntermediateFiles = reply.PartitionedKVA
	return mapResult, nil
}

func (w *coordinatedWorker) reducef(ctx context.Context, task reduceTask) (reduceResult, error) {
	var intermediates []string
	for intermediate := range task.IntermediateFiles {
		intermediates = append(intermediates, intermediate)
	}
	args := AssignReduceTaskArgs{
		Partition:     task.Partition,
		Intermediates: intermediates,
	}
	reply := DoneReduceTaskReply{}
	err := call(ctx, w.Endpoint, "Worker.AssignReduceTask", &args, &reply)
	reduceResult := reduceResult{}
	if err != nil {
		return reduceResult, err
	}
	reduceResult.Partition = task.Partition
	return reduceResult, nil
}

func (w *coordinatedWorker) shutdown(ctx context.Context) error {
	args := ShutdownArgs{}
	reply := ShutdownReply{}
	err := call(ctx, w.Endpoint, "Worker.Shutdown", &args, &reply)
	return err
}

func pingWorker(worker coordinatedWorker) error {
	log.Printf("pinging worker %v\n", worker.WorkerId)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	healthArgs := HealthCheckArgs{}
	healthReply := HealthCheckReply{}
	if err := call(ctx, worker.Endpoint, "Worker.HealthCheck", &healthArgs, &healthReply); err != nil {
		return HealthCheckFailedErr{
			Message: fmt.Sprintf("pinging worker %v failed: %v", worker.WorkerId, err),
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := string(coordinatorEndpoint())
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		go func() {
			<-c.shutdownWorkers // if workers are requested to shutdown, we don't want to accept any new workers
			l.Close()
			close(c.isServerOffline)
		}()
		http.Serve(l, nil)
	}()
}

func (c *Coordinator) scheduler(files []string, nReduce int) {
	mapTasks := map[taskId]*mapTask{}
	for _, f := range files {
		id, _ := uuid.NewRandom()
		task := mapTask{
			Id:     taskId(id.String()),
			Status: pending,
			File:   f,
		}
		mapTasks[task.Id] = &task
	}

	reduceTasks := map[int]*reduceTask{}
	for i := 0; i < nReduce; i++ {
		task := reduceTask{
			Partition:         i,
			Status:            pending,
			IntermediateFiles: map[string]bool{},
		}
		reduceTasks[task.Partition] = &task
	}

	mapPhase := make(chan bool, 1)
	reducePhase := make(chan bool, 1)
	mapPhase <- true

	go func() {
		log.Println("started scheduler")
		for {
			select {
			case <-mapPhase:
				log.Println("scheduling all map tasks")
				for _, task := range mapTasks {
					c.scheduledMapTask <- *task
				}
			case <-reducePhase:
				for _, task := range reduceTasks {
					c.scheduledReduceTask <- *task
				}
			case res := <-c.mapResults:
				log.Println("received a map result")
				alreadyDone := mapTasks[res.TaskId].Status == done
				if !alreadyDone {
					mapTasks[res.TaskId].Status = done
					for partition, file := range res.IntermediateFiles {
						reduceTasks[partition].IntermediateFiles[file] = true
					}
					doneAll := true
					for _, task := range mapTasks {
						doneAll = doneAll && task.Status == done
					}
					if doneAll {
						reducePhase <- true
					}
				}
			case res := <-c.reduceResults:
				log.Println("received a reduce result")
				reduceTasks[res.Partition].Status = done
				doneAll := true
				for _, task := range reduceTasks {
					doneAll = doneAll && task.Status == done
				}
				if doneAll {
					c.shutdown()
					return
				}
			case task := <-c.failedMapTask:
				log.Printf("map task %v failed\n", task.Id)
				c.scheduledMapTask <- task
			case task := <-c.failedReduceTask:
				log.Printf("reduce task %v failed\n", task.Partition)
				c.scheduledReduceTask <- task
			}
		}
	}()
}

func (c *Coordinator) shutdown() {
	close(c.shutdownWorkers) // shutdown workers and server
	<-c.isServerOffline      // prevents race conditions with c.workers.Done() and c.workers.Wait()
	c.workers.Wait()         // wait for workers to exit
	c.done <- true           // map-reduce process is done
}
