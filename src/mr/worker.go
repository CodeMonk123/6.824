package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	log.SetLevel(log.ErrorLevel)

	// Your worker implementation here.
	// Register the worker to the coordinator.
	workerID, ok := Register()
	if !ok {
		log.Info("can not register, quit.")
		return
	}

	// ask tasks until the whole mr job completed.
	errorCount := 0
	for {
		// keep ask for next task
		reply := AskTask(workerID)
		if reply == nil {
			errorCount += 1
			if errorCount < 3 {
				log.Errorf("unknown error: %d.\n", errorCount)
				time.Sleep(1 * time.Second)
				continue
			} else {
				log.Errorf("encounter 3 errors, quit.")
				break
			}
		}

		if reply.Phase == DONE {
			log.Info("mr completed. quit.")
			break
		} else if reply.Phase == MAP_TASK {
			outputFilePaths, err := DoMap(workerID, reply.TaskID, reply.InputFilePaths, reply.IntermidiateDir, reply.NumReduce, mapf)
			if err != nil {
				log.Errorf("get error when do map: %v\n", err)
				ReportResult(reply.TaskID, MAP_TASK, reply.InputFilePaths, nil, false)
			} else {
				log.Infof("map finished: (%s) -> (%s) \n", reply.InputFilePaths, outputFilePaths)
				ReportResult(reply.TaskID, MAP_TASK, reply.InputFilePaths, outputFilePaths, true)
			}
		} else if reply.Phase == REDUCE_TASK {
			outputFilePath, err := DoReduce(workerID, reply.TaskID, reply.InputFilePaths, reply.ReduceOutputDir, reducef)
			if err != nil {
				log.Errorf("get error when do reduce: %v\n", err)
				ReportResult(reply.TaskID, REDUCE_TASK, reply.InputFilePaths, nil, false)
			} else {
				log.Infof("reduce finished: (%s) -> (%s) \n", reply.InputFilePaths, outputFilePath)
				ReportResult(reply.TaskID, REDUCE_TASK, reply.InputFilePaths, []string{outputFilePath}, true)
			}
		} else if reply.Phase == PENDING {
			log.Info("pending ...")
			time.Sleep(3 * time.Second)
		}

		time.Sleep(1 * time.Second)
	}
}

// call mapf on inputFilePaths, return the output file paths.
func DoMap(workerID, taskID int, inputFilePaths []string, intermidiateDir string, nReduce int, mapf func(string, string) []KeyValue) ([]string, error) {
	res := []KeyValue{}
	for _, inputFilePath := range inputFilePaths {
		inputFile, err := os.Open(inputFilePath)
		if err != nil {
			log.Errorf("can not open input file: %s, err: %v\n", inputFilePath, err)
			return nil, err
		}
		content, err := ioutil.ReadAll(inputFile)
		if err != nil {
			log.Errorf("can not read file content: %s, err: %v\n", inputFilePath, err)
			return nil, err
		}
		kva := mapf(inputFilePath, string(content))
		res = append(res, kva...)
		_ = inputFile.Close()
	}

	outputFilePaths := []string{}
	outputFiles := []*os.File{}
	for i := 0; i < nReduce; i++ {
		outputFilePath := path.Join(intermidiateDir, fmt.Sprintf("slice-%d-%d-%8X.txt", i, taskID, workerID))
		outputFilePaths = append(outputFilePaths, outputFilePath)
		outputFile, err := os.Create(outputFilePath)
		if err != nil {
			log.Errorf("can not create file: %s, err: %v\n", outputFilePath, err)
			return nil, err
		}
		outputFiles = append(outputFiles, outputFile)
	}
	for _, kv := range res {
		sliceIdx := ihash(kv.Key) % nReduce
		fmt.Fprintf(outputFiles[sliceIdx], "%s %s\n", kv.Key, kv.Value)
	}

	for _, file := range outputFiles {
		file.Close()
	}
	return outputFilePaths, nil
}

// do reduce
func DoReduce(workerID, taskID int, inputFilePaths []string, reduceOutputDir string, reducef func(string, []string) string) (string, error) {
	keyValues := make(map[string][]string)
	results := make(map[string]string)
	for _, inputFilePath := range inputFilePaths {
		inputFile, err := os.Open(inputFilePath)
		if err != nil {
			log.Errorf("can not open %v\n", inputFile)
			return "", err
		}
		scanner := bufio.NewScanner(inputFile)
		for scanner.Scan() {
			line := string(scanner.Text())
			kv := strings.Split(line, " ")
			if len(kv) == 2 {
				if _, ok := keyValues[kv[0]]; !ok {
					keyValues[kv[0]] = []string{}
				}
				keyValues[kv[0]] = append(keyValues[kv[0]], kv[1])
			}
		}
		inputFile.Close()
	}

	// reduce
	for k := range keyValues {
		results[k] = reducef(k, keyValues[k])
	}

	outputFilePath := path.Join(reduceOutputDir, fmt.Sprintf("mr-out-%d-%8X.txt", taskID, workerID))
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Errorf("can not create %v\n", outputFilePath)
		return "", err
	}
	defer outputFile.Close()
	keys := make([]string, 0, len(results))
	for k := range results {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	for _, key := range keys {
		fmt.Fprintf(outputFile, "%v %v\n", key, results[key])
	}
	return outputFilePath, nil
}

// call register
func Register() (int, bool) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if !ok || !reply.Accepted {
		if !ok {
			log.Info("call register worker failed")
		} else {
			log.Info("rejected by the coordinator.")
		}
		return -1, false
	}
	return reply.WorkerID, true
}

// ask task
func AskTask(id int) *AskTaskReply {
	args := AskTaskArgs{
		WorkerID: id,
	}
	reply := AskTaskReply{}
	ok := call("Coordinator.AskTask", &args, &reply)
	if !ok {
		log.Info("call ask task failed.")
		return nil
	}
	return &reply
}

// report result
func ReportResult(taskID int, phase TaskPhase, inputFile, outputFiles []string, succeeded bool) {
	args := ReportResultArgs{
		TaskID:          taskID,
		Succeeded:       succeeded,
		Type:            phase,
		OutputFilePaths: outputFiles,
	}

	reply := RegisterWorkerReply{}
	ok := call("Coordinator.ReportResult", &args, &reply)
	if !ok {
		log.Errorf("call report result failed\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
