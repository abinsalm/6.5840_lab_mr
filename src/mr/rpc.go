package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y        int
	TaskType int
}

// Add your RPC definitions here.
type TaskType int64

const (
	MapTaskType TaskType = iota
	ReduceTaskType
	DoneTaskType
	WaitTaskType
)

type TaskStatus int

const (
	NotStarted     TaskStatus = iota
	InProgressTask TaskStatus = iota
	CompletedTask
	FailedTask
)

type MapWorkerTask struct {
	TaskId    int
	FileName  string
	StartTime time.Time
}

type ReduceWorkerTask struct {
	TaskStatus TaskStatus
	StartTime  time.Time
	InputFiles []string
}

type TaskRequest struct {
}

type TaskResponse struct {
	TaskType  TaskType
	FilePaths []string
	TaskId    int
	NReduce   int
}

type MapTaskCompletedRequest struct {
	InputFilePath   string
	ReduceTaskInput map[int][]string
}

type MapTaskCompletedResponse struct {
}

type ReduceTaskCompletedRequest struct {
	ReduceTaskId int
}

type ReduceTaskCompletedResponse struct {
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
