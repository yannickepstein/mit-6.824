package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type WorkerId string

type Worker struct {
	Id       WorkerId
	Endpoint Endpoint
	Mapf     mapper
	Reducef  reducer
	Done     chan bool
	L        sync.RWMutex
	Files    []string
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
//
type mapper func(string, string) []KeyValue
type reducer func(string, []string) string

func NewWorker(mapf mapper, reducef reducer) {
	id, _ := uuid.NewRandom()
	endpoint := newUniqueEndpoint(id)
	w := Worker{
		Id:       WorkerId(id.String()),
		Endpoint: endpoint,
		Mapf:     mapf,
		Reducef:  reducef,
		Done:     make(chan bool, 1),
		L:        sync.RWMutex{},
		Files:    make([]string, 0),
	}

	w.server()
	err := w.requestRegistration()
	if err != nil {
		return
	}
	<-w.Done
	w.cleanup()
}

func (w *Worker) HealthCheck(args *HealthCheckArgs, reply *HealthCheckReply) error {
	return nil
}

func (w *Worker) AssignMapTask(args *AssignMapTaskArgs, reply *DoneMapTaskReply) error {
	log.Printf("map task %v got assigned\n", args.TaskId)
	file, err := os.Open(args.File)
	if err != nil {
		return fmt.Errorf("cannot open file %v", args.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read content of %v", args.File)
	}
	file.Close()
	kva := w.Mapf(args.File, string(content))
	partitions := map[int]*json.Encoder{}
	reply.PartitionedKVA = make(map[int]string)
	for partition := 0; partition < args.NPartitions; partition++ {
		filename := fmt.Sprintf("mr-%v-%v", args.TaskId, partition)
		f, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("cannot open intermediate file to store partitioned result")
		}
		defer f.Close()
		w.L.Lock()
		w.Files = append(w.Files, filename)
		w.L.Unlock()
		partitions[partition] = json.NewEncoder(f)
		reply.PartitionedKVA[partition] = filename
	}
	for _, kv := range kva {
		partition := ihash(kv.Key) % args.NPartitions
		err := partitions[partition].Encode(&kv)
		if err != nil {
			return fmt.Errorf("could not persist kv pair %v,%v", kv.Key, kv.Value)
		}
	}
	return nil
}

func (w *Worker) AssignReduceTask(args *AssignReduceTaskArgs, reply *DoneReduceTaskReply) error {
	log.Printf("reduce task for partition %v got assigned\n", args.Partition)
	var intermediate []KeyValue
	for _, filename := range args.Intermediates {
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("cannot open intermediate file %v", filename)
		}
		dec := json.NewDecoder(file)
		var kva []KeyValue
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(byKey(intermediate))
	tmpFile, err := ioutil.TempFile("", "reduce-")
	if err != nil {
		return fmt.Errorf("cannot create temporary file")
	}
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
		output := w.Reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%v", args.Partition))
	return nil
}

func (w *Worker) Shutdown(args *ShutdownArgs, reply *ShutdownReply) error {
	close(w.Done)
	return nil
}

func (w *Worker) cleanup() {
	w.L.RLock()
	for _, filename := range w.Files {
		os.Remove(filename)
	}
	w.L.RUnlock()
}

func (w *Worker) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	os.Remove(string(w.Endpoint))
	l, err := net.Listen("unix", string(w.Endpoint))
	if err != nil {
		log.Fatal("listen error:", err)
	}
	log.Println("starting worker server")
	go http.Serve(l, nil)
}

func (w *Worker) requestRegistration() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := RegistrationRequestArgs{
		WorkerId:       string(w.Id),
		WorkerEndpoint: w.Endpoint,
	}
	reply := RegistrationRequestReply{}
	return call(ctx, coordinatorEndpoint(), "Coordinator.RequestRegistration", &args, &reply)
}

// for sorting by key.
type byKey []KeyValue

// for sorting by key.
func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
