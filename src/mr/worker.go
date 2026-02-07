package mr

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		taskResponse, err := AskForTask()
		if err != nil {
			log.Println("Coordinator unreachable, exiting")
			return
		}

		if taskResponse.TaskType == DoneTaskType {
			log.Println("Job is done")
			return
		}

		if taskResponse.TaskType == WaitTaskType {
			log.Println("Waiting for tasks")
			time.Sleep(1 * time.Second)
			continue
		}

		if taskResponse.TaskType == MapTaskType {
			log.Println("Starting Map task")
			reduceTaskInput := doMap(mapf, taskResponse)

			mapTaskCompletedRequest := MapTaskCompletedRequest{
				InputFilePath:   taskResponse.FilePaths[0],
				ReduceTaskInput: reduceTaskInput,
			}
			mapTaskCompletedResponse := MapTaskCompletedResponse{}

			log.Println("Sending Coordinator.MapTaskCompleted")
			ok := call("Coordinator.MapTaskCompleted", &mapTaskCompletedRequest, &mapTaskCompletedResponse)
			if !ok {
				log.Println("Coordinator unreachable, exiting")
				return
			}
			log.Println("Received successful response for Coordinator.MapTaskCompleted")
		} else if taskResponse.TaskType == ReduceTaskType {
			log.Println("Starting Reduce task")
			reduceTaskId := doReduce(reducef, taskResponse)

			reduceTaskCompletedRequest := ReduceTaskCompletedRequest{
				ReduceTaskId: reduceTaskId,
			}
			reduceTaskCompletedResponse := ReduceTaskCompletedResponse{}

			log.Println("Sending Coordinator.ReduceTaskCompleted")
			ok := call("Coordinator.ReduceTaskCompleted", &reduceTaskCompletedRequest, &reduceTaskCompletedResponse)
			if !ok {
				log.Println("Coordinator unreachable, exiting")
				return
			}
			log.Println("Received successful response for Coordinator.ReduceTaskCompleted")
		}
	}

}

func AskForTask() (TaskResponse, error) {
	taskRequest := TaskRequest{}
	taskResponse := TaskResponse{}

	ok := call("Coordinator.TaskRequest", &taskRequest, &taskResponse)
	if ok {
		log.Printf("Task Type: %v\n", taskResponse.TaskType)

		return taskResponse, nil

	} else {
		log.Printf("call failed!\n")
		return TaskResponse{}, errors.New("Call failed")
	}
}

func doMap(mapf func(string, string) []KeyValue, taskResponse TaskResponse) map[int][]string {
	filename := taskResponse.FilePaths[0]
	log.Printf("Starting Map process on file %v\n", filename)

	content := readFileContent(filename)
	kva := mapf(filename, string(content))
	log.Printf("Finished executing map function for file: %v with %d key/value pairs\n", filename, len(kva))

	mapFiles := make(map[int]string)
	for _, kv := range kva {
		doMapOnKV(kv, taskResponse, mapFiles)
	}

	reduceTaskInput := make(map[int][]string)

	// write content to temp file
	// after all write is finished, move temp file to actual file
	err := os.MkdirAll("map_files", os.ModePerm)
	for reduceId, tempFilePath := range mapFiles {
		outputFileName := fmt.Sprint("mr-", taskResponse.TaskId, "-", reduceId)
		outputFilePath := filepath.Join("map_files", outputFileName)
		err = os.Rename(tempFilePath, outputFilePath)
		check(err)
		log.Printf("Finished renaming temp file to  %v\n", outputFilePath)
		reduceTaskInput[reduceId] = append(reduceTaskInput[reduceId], outputFilePath)
	}

	return reduceTaskInput
}

func doMapOnKV(kv KeyValue, taskResponse TaskResponse, mapFiles map[int]string) {
	// calculate reduce job ID
	reduceId := ihash(kv.Key) % taskResponse.NReduce

	if _, ok := mapFiles[reduceId]; !ok {
		outputFileName := fmt.Sprint("mr-", taskResponse.TaskId, "-", reduceId)
		tempFile, err := os.CreateTemp("", outputFileName)
		check(err)
		// Write output to a temp file first so that nobody observes partially written files in the presence of crashes
		// Now write map output to disk
		mapFiles[reduceId] = tempFile.Name()
		log.Printf("Created temp file %v\n", tempFile.Name())
	}

	outputFile, err := os.OpenFile(mapFiles[reduceId], os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	// close fi on exit and check for its returned error
	defer func() {
		if err := outputFile.Close(); err != nil {
			panic(err)
		}
	}()

	_, err = outputFile.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
	check(err)

	log.Printf("Finished writing to temp file %v\n", outputFile.Name())
}

func doReduce(reducef func(string, []string) string, taskResponse TaskResponse) int {
	reduceTaskID := taskResponse.TaskId
	inputFilePaths := taskResponse.FilePaths
	log.Printf("Starting reduce task with ID: %v and %d input files\n", reduceTaskID, len(inputFilePaths))

	outputFileName := fmt.Sprint("mr-out-", reduceTaskID)

	// Write output to a temp file first so that nobody observes partially written files in the presence of crashes
	tempFile, err := os.CreateTemp("", outputFileName)
	check(err)
	log.Printf("Created temp file %v for reduce task ID: %v\n", tempFile.Name(), reduceTaskID)

	// go over map phase output and convert them to key, value slice
	var intermediate []KeyValue
	for _, inputFilePath := range inputFilePaths {
		intermediate = reduceFile(inputFilePath, reduceTaskID, intermediate)
	}

	log.Printf("Reduce Task ID %v: sorting intermediate key values\n", reduceTaskID)
	sort.Sort(ByKey(intermediate))
	log.Printf("Reduce Task ID %v: finished sorting intermediate key values\n", reduceTaskID)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		log.Printf("Reduce Task ID %v: calling reduce function on reduce key %v\n", reduceTaskID, intermediate[i].Key)
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		log.Printf("Writing reduce output to file %v for reduce task ID %v\n", tempFile.Name(), reduceTaskID)
		_, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return 0
		}
		log.Printf("Finished writing reduce output to file %v for reduce task ID %v\n", tempFile.Name(), reduceTaskID)

		i = j
	}

	// after all write is finished, move temp file to actual file
	err = os.MkdirAll("reduce_files", os.ModePerm)
	outputFilePath := filepath.Join("reduce_files", outputFileName)
	err = os.Rename(tempFile.Name(), outputFilePath)
	check(err)
	log.Printf("Finished renaming temp file to  %v for reduce task ID %v\n", outputFilePath, reduceTaskID)

	return reduceTaskID
}

func reduceFile(inputFilePath string, reduceTaskID int, intermediate []KeyValue) []KeyValue {
	log.Printf("Reading file content of: %v for reduce task ID: %v\n", inputFilePath, reduceTaskID)

	f, err := os.OpenFile(inputFilePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("open file error: %v", err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
		}
	}(f)

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text() // GET the line string
		splits := strings.Split(line, " ")
		if len(splits) == 2 {
			log.Printf("Splitting content of %v", string(line))
			key := splits[0]
			value := splits[1]
			kv := KeyValue{key, value}
			intermediate = append(intermediate, kv)
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("scan file error for file %v: %v", inputFilePath, err)
	}

	log.Printf("Finished Reading file content of: %v for reduce task ID: %v\n", inputFilePath, reduceTaskID)
	return intermediate
}

func readFileContent(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
