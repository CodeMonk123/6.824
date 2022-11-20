package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskPhase string

const MAP_TASK TaskPhase = "MAP_TASK"
const REDUCE_TASK TaskPhase = "REDUCE_TASK"
const PENDING TaskPhase = "PENDING"
const DONE TaskPhase = "DONE"

// request/reply for register worker
type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	// false if not accpeted
	Accepted bool
	WorkerID int
}

// request/reply for asking a task
type AskTaskArgs struct {
	WorkerID int
}

type AskTaskReply struct {
	Phase           TaskPhase
	InputFilePaths  []string
	ReduceOutputDir string
	IntermidiateDir string
	TaskID          int
	NumReduce       int
}

// request/reply for report
type ReportResultArgs struct {
	Succeeded       bool
	TaskID          int
	Type            TaskPhase
	OutputFilePaths []string
}

type ReportResultReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())

	return s
}
