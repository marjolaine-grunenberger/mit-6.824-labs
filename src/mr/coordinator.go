package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"
import "sync"

type Coordinator struct {
    nReduce      int // number of reduce tasks
    nMap         int // number of map tasks
    mapTasks     []Task // map tasks, each task is a file
    reduceTasks  []Task // reduce tasks, each task is a partition
    intermediate map[int][]string // intermediate files to reduce, key is the partition number
    mu          sync.Mutex // mutex to ensure thread safety
}

//
// GetTask assigns a task to a worker. It first checks for idle or long-running
// Map tasks and assigns one if available. If no Map tasks are available, it
// then checks for idle or long-running Reduce tasks and assigns one if available.
// If no tasks are available, it sets the TaskType in the reply to "none".
// The function locks the Coordinator's mutex to ensure thread safety while
// accessing and modifying the task lists.
//
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
    
	c.mu.Lock()
    defer c.mu.Unlock() 

    // Check for idle or Map tasks taking too long
    for i, task := range c.mapTasks {
        if task.Status == Idle || (task.Status == InProgress && time.Now().Sub(task.StartTime) >= 10*time.Second) {
			c.mapTasks[i].Status = InProgress
            c.mapTasks[i].StartTime = time.Now()
            reply.TaskType = "map"
            reply.Task = c.mapTasks[i]
            return nil
        }
    }

    // Check for idle or Reduce tasks taking too long
    for i, task := range c.reduceTasks {
        if task.Status == Idle || (task.Status == InProgress && time.Now().Sub(task.StartTime) >= 10*time.Second) {
			c.reduceTasks[i].Status = InProgress
            c.reduceTasks[i].StartTime = time.Now()
            reply.TaskType = "reduce"
            reply.Task = c.reduceTasks[i]
            return nil
        }
    }

    reply.TaskType = "none"
    return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *TaskDoneReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.mapTasks[args.FileId].Status = Completed
    // For each reduceTaskPartition, append "mr-{fileId}-{reduceTaskPartition}" in the list of intermediate files to reduce
    for i := 0; i < c.nReduce; i++ {
        c.intermediate[i] = append(c.intermediate[i], fmt.Sprintf("mr-%d-%d", args.FileId, i))
        // if the number of task to reduce in reduceTask[i] is equal to the number of map tasks, then the reduce task is ready
        if len(c.intermediate[i]) == c.nMap {
            // create a new reduce task if all map tasks are completed
            c.reduceTasks[i] = Task{Status: Idle, StartTime: time.Now(), ReducePartition: i, NMap: c.nMap}            
        }
    }
    return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *TaskDoneReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.reduceTasks[args.ReducePartition].Status = Completed
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
    c.mu.Lock()
    defer c.mu.Unlock()

    ret := true

    // Check if all map tasks are completed
    for _, task := range c.mapTasks {
        if task.Status != Completed {
            ret = false
            break
        }
    }

    if ret {
        // Check if all reduce tasks are completed
        for _, task := range c.reduceTasks {
            if task.Status != Completed {
                ret = false
                break
            }
        }
    }

    return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	nMap := len(files)

    c := Coordinator{
        nReduce:      nReduce,
        nMap:         nMap,
        mapTasks:     make([]Task, nMap),
        reduceTasks:  make([]Task, nReduce),
        intermediate: make(map[int][]string),
        mu:          sync.Mutex{},
    }

	// Initialize Map tasks to idle
	for i := 0; i < nMap; i++ {
		c.mapTasks[i] = Task{Status: Idle, StartTime: time.Now(), FileId: i, FileName: files[i]}
	}

	c.server()
	return &c
}
