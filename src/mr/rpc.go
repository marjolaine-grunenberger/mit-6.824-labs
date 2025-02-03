package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

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

// Add your RPC definitions here.
type TaskStatus string

const (
    Idle       TaskStatus = "idle"
    InProgress TaskStatus = "in progress"
    Completed  TaskStatus = "completed"
)

type Task struct {
    Status   TaskStatus
	StartTime time.Time
	FileId int
	FileName  string
	ReducePartition int
	NMap int
}

type MapTaskDoneArgs struct {
    FileId int // The ID of the file that the map task processed
}

type TaskDoneReply struct {
    // You can add fields here if needed for the reply
}

type ReduceTaskDoneArgs struct {
	ReducePartition int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
    TaskType string
    Task     Task
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
