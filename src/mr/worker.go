package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "bufio"
import "strings"

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
// main/mrworker.go calls this function
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for true {
		reply := CallGetTask(mapf, reducef)

		if reply.TaskType == "map" {
			Map(mapf, reply.Task.FileName, reply.Task.FileId)
			// Mark this task as completed
			// Reduce will be called on all combination FiledId - NReduce
			args := MapTaskDoneArgs{reply.Task.FileId}
			reply := GetTaskReply{}
			call("Coordinator.MapTaskDone", &args, &reply)

		} else if reply.TaskType == "reduce" {
			Reduce(reducef, reply.Task.ReducePartition, reply.Task.NMap)
			// Mark this task as completed
			args := ReduceTaskDoneArgs{reply.Task.ReducePartition}
			reply := GetTaskReply{}
			call("Coordinator.ReduceTaskDone", &args, &reply)
		}

	}
}

func Map(mapf func(string, string) []KeyValue, filename string, fileId int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	files := make(map[int]*os.File)
	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()

	// Iterate over intermediate key value pairs
	for _, kv := range intermediate {
		// get the reduce task partition using ihash method over the key
		reduceTaskPartition := ihash(kv.Key) % 10 // assuming NReduce is 10

		// open the file for the reduce task partition if not already opened
		if _, ok := files[reduceTaskPartition]; !ok {
			fileName := fmt.Sprintf("mr-%d-%d", fileId, reduceTaskPartition)
			files[reduceTaskPartition], err = os.Create(fileName)
			if err != nil {
				log.Fatalf("Cannot create %v", fileName)
			}
		}

		// append key and value to the file
		fmt.Fprintf(files[reduceTaskPartition], "%v %v\n", kv.Key, kv.Value)
	}
}

func Reduce(reducef func(string, []string) string, reduceTaskPartition int, nMap int) {

	// read all intermediate files, if they exist, for this reduce task partition
	// some files might not exist, if no key are in the corresponding partition
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
        fileName := fmt.Sprintf("mr-%d-%d", i, reduceTaskPartition)
        file, err := os.Open(fileName)
        if err != nil {
            if os.IsNotExist(err) {
                continue
            }       
			log.Fatalf("Cannot open %v: %v", fileName, err) 
		}
        defer file.Close()

        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            parts := strings.Split(line, " ")
            if len(parts) != 2 {
                log.Fatalf("Unexpected line format: %v", line)
            }
            intermediate = append(intermediate, KeyValue{Key: parts[0], Value: parts[1]})
        }
        if err := scanner.Err(); err != nil {
            log.Fatalf("Error reading file %v: %v", fileName, err)
        }
    }

	sort.Sort(ByKey(intermediate))

	// create a temporary file for the reduce task partition
	// mr-out-* files are the final output files, they are to be used by the user program,
	// as another MapReduce job for instance
	fileName := fmt.Sprintf("mr-out-%d", reduceTaskPartition)
	ofile, _ := os.Create(fileName)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-* for each partition
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
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}	


func CallGetTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
