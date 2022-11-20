package mr

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Coordinator struct {
	// Your definitions here.
	activeWorkers         map[int]interface{}
	unfinishedMapTasks    map[int]string // unfinished map task: task id -> input file path
	unfinishedReduceTasks map[int][]string
	pendingMapTasks       map[int]interface{} // pending map task ids
	pendingReduceTasks    map[int]interface{}
	runningMapTasks       map[int]chan bool // running map task ids
	runningReduceTasks    map[int]chan bool
	l                     sync.RWMutex
	nReduce               int // num of reduce workers.
}

// Your code here -- RPC handlers for the worker to call.
// worker register
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	newWorkerID := int(rand.Int31())
	log.Debug(fmt.Sprintf("Worker call register. allocate worker id: %8x\n", newWorkerID))
	c.l.Lock()
	defer c.l.Unlock()
	c.activeWorkers[newWorkerID] = struct{}{}
	reply.Accepted = true
	reply.WorkerID = newWorkerID
	return nil
}

// create if not exist
func makeDirIfNotExist(path string) {
	_ = os.MkdirAll(path, os.ModePerm)
}

// if a map worker doesn't response for 10s
// reschedule that task
func (c *Coordinator) WaitMapResponse(taskID int, done <-chan bool) {
	timer := time.After(10 * time.Second)
	select {
	case <-done:
		log.Infof("receive response for map task %d\n", taskID)
	case <-timer:
		log.Infof("reschedule map task %d\n", taskID)
		c.l.Lock()
		defer c.l.Unlock()
		if _, ok := c.runningMapTasks[taskID]; !ok {
			return
		} else {
			delete(c.runningMapTasks, taskID)
			c.pendingMapTasks[taskID] = struct{}{}
		}
	}
}

// if a reduce worker doesn't response for 10s
// reschedule that task
func (c *Coordinator) WaitReduceResponse(taskID int, done <-chan bool) {
	timer := time.After(10 * time.Second)
	select {
	case <-done:
		log.Infof("receive response for reduce task: %d\n", taskID)
	case <-timer:
		log.Infof("reschedule reduce task %d\n", taskID)
		c.l.Lock()
		defer c.l.Unlock()
		if _, ok := c.runningReduceTasks[taskID]; !ok {
			return
		} else {
			delete(c.runningReduceTasks, taskID)
			c.pendingReduceTasks[taskID] = struct{}{}
		}
	}
}

// handle AskTask
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.l.Lock()
	defer c.l.Unlock()

	if len(c.pendingMapTasks) != 0 {
		// do map job
		taskID := pickKeyFromMap(c.pendingMapTasks)
		delete(c.pendingMapTasks, taskID)
		if _, ok := c.unfinishedMapTasks[taskID]; !ok {
			return fmt.Errorf("reschedule a finished map task: %d\n", taskID)
		}
		reply.Phase = MAP_TASK
		reply.InputFilePaths = []string{c.unfinishedMapTasks[taskID]}
		reply.NumReduce = c.nReduce
		reply.TaskID = taskID
		curDir, _ := os.Getwd()
		reply.IntermidiateDir = path.Join(curDir, "intermidiate_dir")
		makeDirIfNotExist(reply.IntermidiateDir)
		c.runningMapTasks[taskID] = make(chan bool)
		go c.WaitMapResponse(taskID, c.runningMapTasks[taskID])
		log.Infof("schedule map task %d to worker %d.\n", taskID, args.WorkerID)
		return nil
	} else if len(c.unfinishedMapTasks) != 0 {
		// wait
		reply.Phase = PENDING
		reply.InputFilePaths = nil
		log.Infof("waiting for map tasks.")
		return nil
	} else if len(c.pendingReduceTasks) != 0 {
		taskID := pickKeyFromMap(c.pendingReduceTasks)
		delete(c.pendingReduceTasks, taskID)
		if _, ok := c.unfinishedReduceTasks[taskID]; !ok {
			return fmt.Errorf("reschedule a finished reduce task: %d\n", taskID)
		}
		reply.Phase = REDUCE_TASK
		reply.NumReduce = c.nReduce
		reply.InputFilePaths = c.unfinishedReduceTasks[taskID]
		reply.TaskID = taskID
		reply.ReduceOutputDir, _ = os.Getwd()
		c.runningReduceTasks[taskID] = make(chan bool)
		go c.WaitReduceResponse(taskID, c.runningReduceTasks[taskID])
		log.Infof("schedule reduce task %d to worker %d.\n", taskID, args.WorkerID)
		return nil
	} else {
		done := len(c.unfinishedMapTasks) == 0 && len(c.unfinishedReduceTasks) == 0
		if done {
			log.Info("done.")
			reply.Phase = DONE
		}
		return nil
	}
}

// handle ReportResult
func (c *Coordinator) ReportResult(args *ReportResultArgs, reply *ReportResultReply) error {
	c.l.Lock()
	defer c.l.Unlock()
	// unsucceeded
	if !args.Succeeded {
		// reshedule this task
		if args.Type == MAP_TASK {
			delete(c.runningMapTasks, args.TaskID)
			c.pendingMapTasks[args.TaskID] = struct{}{}
		} else if args.Type == REDUCE_TASK {
			delete(c.runningReduceTasks, args.TaskID)
			c.pendingReduceTasks[args.TaskID] = struct{}{}
		}
		return nil
	}

	// succeeded
	if args.Type == MAP_TASK {
		if done, ok := c.runningMapTasks[args.TaskID]; ok {
			done <- true
		} else {
			// ignore rescheduled task.
			return nil
		}
		// remove this task from running and unfinished map tasks
		delete(c.runningMapTasks, args.TaskID)
		delete(c.unfinishedMapTasks, args.TaskID)
		// move this task to unfinishedReduceTasks and pendingReduceTasks
		for i, path := range args.OutputFilePaths {
			c.unfinishedReduceTasks[i] = append(c.unfinishedReduceTasks[i], path)
		}

	} else if args.Type == REDUCE_TASK {
		if done, ok := c.runningReduceTasks[args.TaskID]; ok {
			done <- true
		} else {
			// ignore rescheduled task.
			return nil
		}
		// remove this task from running and unfinished reduce tasks
		delete(c.runningReduceTasks, args.TaskID)
		delete(c.unfinishedReduceTasks, args.TaskID)
	} else {
		return fmt.Errorf("expect MAP_TASK or REDUCE_TASK")
	}
	return nil
}

func pickKeyFromMap(m map[int]interface{}) int {
	if len(m) == 0 {
		log.Fatal("can not handle empty map")
	}

	count := 0
	target := rand.Intn(len(m))
	for k := range m {
		if count == target {
			return k
		}
		count += 1
	}

	return -1 // can not reach here.
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
	c.l.Lock()
	defer c.l.Unlock()
	ret := len(c.unfinishedMapTasks) == 0 && len(c.unfinishedReduceTasks) == 0
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		activeWorkers:         make(map[int]interface{}),
		l:                     sync.RWMutex{},
		unfinishedMapTasks:    make(map[int]string),
		unfinishedReduceTasks: make(map[int][]string),
		pendingMapTasks:       make(map[int]interface{}),
		pendingReduceTasks:    make(map[int]interface{}),
		runningMapTasks:       make(map[int]chan bool),
		runningReduceTasks:    make(map[int]chan bool),
		nReduce:               nReduce,
	}
	// Your code here.
	for i, fileName := range files {
		c.unfinishedMapTasks[i] = fileName
		c.pendingMapTasks[i] = struct{}{}
	}

	for i := 0; i < nReduce; i++ {
		c.unfinishedReduceTasks[i] = []string{}
		c.pendingReduceTasks[i] = struct{}{}
	}

	c.server()
	return &c
}
