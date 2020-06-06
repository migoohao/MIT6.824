package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type WorkerState int

const (
	FREE WorkerState = iota
	AS_MAP
	AS_REDUCE
	CLOSE
)

type IntermediateFileInfo struct {
	RId  int
	Name string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type QueryTaskArgs struct {
}

type QueryTaskReply struct {
	S           WorkerState
	R           int
	M           int
	MId         int
	RId         int
	MapFile     string
	ReduceFiles []string
}

type FinishMapArgs struct {
	MId               int
	IntermediateFiles []IntermediateFileInfo
}

type FinishMapReply struct {
}

type FinishReduceArgs struct {
	RId int
}

type FinishReduceReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket Name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
