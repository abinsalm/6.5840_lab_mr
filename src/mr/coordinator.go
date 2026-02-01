package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	InputFiles                 []string
	NReduce                    int
	MapTasksQueue              map[string]*MapWorkerTask
	MapMutex                   *sync.Mutex
	AvailableMapTasksQueue     []string
	InProgressMapTasks         map[string]struct{}
	CompletedMapTasks          map[string]struct{}
	ReduceTasks                map[int]*ReduceWorkerTask
	AvailableReduceTasksQueue  []int
	InProgressReduceTasksQueue map[int]struct{}
	CompletedReduceTasksCount  int
	ReduceMutex                *sync.Mutex
	ReduceTaskCV               *sync.Cond
	IsDone                     bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(taskRequest *TaskRequest, taskResponse *TaskResponse) error {
	if c.Done() {
		taskResponse.TaskType = DoneTaskType
		return nil
	}

	c.MapMutex.Lock()
	if len(c.AvailableMapTasksQueue) > 0 {
		serveMap(taskResponse, c)
		c.MapMutex.Unlock()
		return nil
	}

	for len(c.CompletedMapTasks) < len(c.InputFiles) {
		c.ReduceTaskCV.Wait()
	}
	c.MapMutex.Unlock()

	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()

	if c.CompletedReduceTasksCount == c.NReduce {
		taskResponse.TaskType = DoneTaskType
		return nil
	}

	if len(c.AvailableReduceTasksQueue) > 0 {
		serveReduce(taskResponse, c)
	} else {
		taskResponse.TaskType = WaitTaskType
	}
	return nil
}

func serveReduce(taskResponse *TaskResponse, c *Coordinator) {
	log.Printf("Starting to serve reduce task\n")
	queueLength := len(c.AvailableReduceTasksQueue)
	log.Printf("Queue length%d\n", queueLength)

	reduceTaskId := c.AvailableReduceTasksQueue[queueLength-1]
	log.Printf("Task ID %v: Starting to serve reduce task\n", reduceTaskId)

	c.AvailableReduceTasksQueue = c.AvailableReduceTasksQueue[:queueLength-1]
	reduceTask := c.ReduceTasks[reduceTaskId]

	taskResponse.TaskType = ReduceTaskType
	taskResponse.FilePaths = reduceTask.InputFiles
	taskResponse.TaskId = reduceTaskId

	reduceTask.TaskStatus = InProgressTask
	reduceTask.StartTime = time.Now()

	log.Printf("Task ID %v: Finished serving reduce task\n", reduceTaskId)
}

func serveMap(taskResponse *TaskResponse, c *Coordinator) {
	// popping a task from available task queue
	filePath := c.AvailableMapTasksQueue[0]
	c.AvailableMapTasksQueue = c.AvailableMapTasksQueue[1:]

	// add task to in progress task queue
	mapTask := c.MapTasksQueue[filePath]
	c.InProgressMapTasks[mapTask.FileName] = struct{}{}

	taskResponse.TaskType = MapTaskType
	taskResponse.FilePaths = []string{filePath}
	taskResponse.TaskId = mapTask.TaskId
	taskResponse.NReduce = c.NReduce

	log.Printf("Serving map task %v with file %v\n", mapTask.TaskId, taskResponse.FilePaths[0])
}

func (c *Coordinator) MapTaskCompleted(mapTaskCompletedRequest *MapTaskCompletedRequest, mapTaskCompletedResponse *MapTaskCompletedResponse) error {
	inputFilePath := mapTaskCompletedRequest.InputFilePath
	reduceTaskInput := mapTaskCompletedRequest.ReduceTaskInput

	c.MapMutex.Lock()
	c.CompletedMapTasks[inputFilePath] = struct{}{}
	c.MapMutex.Unlock()

	c.ReduceMutex.Lock()
	for reduceTaskId, outputFilePaths := range reduceTaskInput {
		// initialize struct first time if it doesn't exist
		if c.ReduceTasks[reduceTaskId] == nil {
			c.ReduceTasks[reduceTaskId] = &ReduceWorkerTask{
				TaskStatus: NotStarted,
				InputFiles: outputFilePaths,
			}

			// pushing a new reduce task to the queue to be worked on later by a reduce worker
			c.AvailableReduceTasksQueue = append(c.AvailableReduceTasksQueue, reduceTaskId)
		} else {
			c.ReduceTasks[reduceTaskId].InputFiles = append(c.ReduceTasks[reduceTaskId].InputFiles, outputFilePaths...)
		}

		inputFiles := c.ReduceTasks[reduceTaskId].InputFiles
		log.Printf("Added file path %v to reduce task %v input files\n", inputFiles[len(inputFiles)-1], reduceTaskId)
	}
	c.ReduceMutex.Unlock()

	c.MapMutex.Lock()
	c.ReduceTaskCV.Broadcast()
	c.MapMutex.Unlock()

	return nil
}

func (c *Coordinator) ReduceTaskCompleted(reduceTaskCompletedRequest *ReduceTaskCompletedRequest, reduceTaskCompletedResponse *ReduceTaskCompletedResponse) error {
	reduceTaskId := reduceTaskCompletedRequest.ReduceTaskId
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()

	c.ReduceTasks[reduceTaskId].TaskStatus = CompletedTask
	// we need to update this variable to quickly know whether the reduce phase is completed or not
	c.CompletedReduceTasksCount += 1

	if c.CompletedReduceTasksCount == c.NReduce {
		c.IsDone = true
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()
	return c.IsDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// we have N files and need to split them into nReduce buckets
	// device N/nReduce => total number of files
	c.InputFiles = files
	c.NReduce = nReduce

	c.MapTasksQueue = make(map[string]*MapWorkerTask)
	c.AvailableMapTasksQueue = make([]string, 0)
	for fileIndex := 0; fileIndex < len(c.InputFiles); fileIndex += 1 {
		filepath := c.InputFiles[fileIndex]
		c.MapTasksQueue[filepath] = &MapWorkerTask{
			TaskId:   fileIndex,
			FileName: filepath,
		}

		c.AvailableMapTasksQueue = append(c.AvailableMapTasksQueue, filepath)
	}
	c.InProgressMapTasks = make(map[string]struct{})
	c.CompletedMapTasks = make(map[string]struct{})

	c.ReduceTasks = make(map[int]*ReduceWorkerTask)
	c.AvailableReduceTasksQueue = make([]int, 0)
	c.InProgressReduceTasksQueue = make(map[int]struct{})

	c.MapMutex = &sync.Mutex{}
	c.ReduceMutex = &sync.Mutex{}
	c.ReduceTaskCV = sync.NewCond(c.MapMutex)

	c.server()
	return &c
}
