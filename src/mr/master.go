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

type FileState int

type MasterState int

const (
	IDLE FileState = iota
	RUNNING
	FINISHED
)

const (
	MAP MasterState = iota
	REDUCE
	DONE
)

type MapFileInfo struct {
	name  string
	state FileState
}

type ReduceFileInfo struct {
	names []string
	state FileState
}

type Master struct {
	// Your definitions here.
	m           int
	r           int
	s           MasterState
	mapFiles    []MapFileInfo
	mapLock     sync.Mutex
	reduceFiles []ReduceFileInfo
	reduceLock  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) QueryTask(args *QueryTaskArgs, reply *QueryTaskReply) error {
	reply.S = FREE
	switch m.s {
	case MAP:
		m.assignMapTask(reply)
	case REDUCE:
		m.assignReduceTask(reply)
	case DONE:
		reply.S = CLOSE
	}
	return nil
}

func (m *Master) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {
	if m.mapFiles[args.MId].state == RUNNING {
		m.mapLock.Lock()
		defer m.mapLock.Unlock()
		m.mapFiles[args.MId].state = FINISHED
		for _, info := range args.IntermediateFiles {
			m.reduceFiles[info.RId].names = append(m.reduceFiles[info.RId].names, info.Name)
		}
		m.validateMapDone()
	}
	return nil
}

func (m *Master) FinishReduce(args *FinishReduceArgs, reply *FinishReduceReply) error {
	if m.reduceFiles[args.RId].state == RUNNING {
		m.reduceLock.Lock()
		defer m.reduceLock.Unlock()
		m.reduceFiles[args.RId].state = FINISHED
		m.validateReduceDone()
	}
	return nil
}

func (m *Master) validateMapDone() {
	for i := range m.mapFiles {
		if m.mapFiles[i].state != FINISHED {
			return
		}
	}
	m.s = REDUCE
}

func (m *Master) validateReduceDone() {
	for i := range m.reduceFiles {
		if m.reduceFiles[i].state != FINISHED {
			return
		}
	}
	m.s = DONE
}

func (m *Master) assignMapTask(reply *QueryTaskReply) {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	for i := range m.mapFiles {
		if m.mapFiles[i].state == IDLE {
			m.mapFiles[i].state = RUNNING
			reply.S = AS_MAP
			reply.R = m.r
			reply.M = m.m
			reply.MId = i
			reply.MapFile = m.mapFiles[i].name
			break
		}
	}
}

func (m *Master) assignReduceTask(reply *QueryTaskReply) {
	m.reduceLock.Lock()
	defer m.reduceLock.Unlock()
	for i := range m.reduceFiles {
		if m.reduceFiles[i].state == IDLE {
			m.reduceFiles[i].state = RUNNING
			reply.S = AS_REDUCE
			reply.R = m.r
			reply.M = m.m
			reply.RId = i
			reply.ReduceFiles = m.reduceFiles[i].names
			break
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	return m.s == DONE
}

func (m *Master) reset() {
	for {
		//Todo change hour to second
		time.Sleep(10 * time.Second)
		m.mapLock.Lock()
		for i := range m.mapFiles {
			if m.mapFiles[i].state == RUNNING {
				m.mapFiles[i].state = IDLE
			}
		}
		m.mapLock.Unlock()

		m.reduceLock.Lock()
		for i := range m.reduceFiles {
			if m.reduceFiles[i].state == RUNNING {
				m.reduceFiles[i].state = IDLE
			}
		}
		m.reduceLock.Unlock()
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.s = MAP
	m.r = nReduce
	m.m = len(files)
	m.mapFiles = []MapFileInfo{}
	for _, file := range files {
		m.mapFiles = append(m.mapFiles, MapFileInfo{name: file, state: IDLE})
	}
	m.reduceFiles = []ReduceFileInfo{}
	for i := 0; i < m.r; i++ {
		m.reduceFiles = append(m.reduceFiles, ReduceFileInfo{names: []string{}, state: IDLE})
	}
	m.server()
	go m.reset()
	return &m
}
