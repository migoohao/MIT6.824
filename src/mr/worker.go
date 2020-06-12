package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	for {
		reply := GetTask()
		switch reply.S {
		case AS_MAP:
			doMap(mapf, reply)
		case AS_REDUCE:
			doReduce(reducef, reply)
		case CLOSE:
			os.Exit(0)
		default:
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, reply *QueryTaskReply) {
	filename := reply.MapFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	var tempFiles []*os.File
	var tempEnc []*json.Encoder
	for i := 0; i < reply.R; i++ {
		temp, err := ioutil.TempFile("", "mr-map-*")
		if err != nil {
			log.Fatalf("cannot create tmp file.")
		}
		tempEnc = append(tempEnc, json.NewEncoder(temp))
		tempFiles = append(tempFiles, temp)
	}
	for _, kv := range kva {
		index := ihash(kv.Key) % len(tempEnc)
		if err := tempEnc[index].Encode(&kv); err != nil {
			log.Fatalf("map id %d temp file id %d encode %v fail", reply.MId, index, kv)
		}
	}
	var intermediateFiles []IntermediateFileInfo
	for i, file := range tempFiles {
		newName := "mr-" + strconv.Itoa(reply.MId) + "-" + strconv.Itoa(i)
		intermediateFiles = append(intermediateFiles, IntermediateFileInfo{Name: newName, RId: i})
		if err := os.Rename(file.Name(), fullPathFile(newName)); err != nil {
			log.Fatalf("can not rename temp file %S", file.Name())
		}
		if err := file.Close(); err != nil {
			log.Fatalf("can not close file %S", file.Name())
		}
	}
	args := FinishMapArgs{MId: reply.MId, IntermediateFiles: intermediateFiles}
	call("Master.FinishMap", &args, &FinishMapReply{})
}

func doReduce(reducef func(string, []string) string, reply *QueryTaskReply) {
	keyMap := map[string][]string{}
	for _, filename := range reply.ReduceFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("can not open reduce file %S", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			keyMap[kv.Key] = append(keyMap[kv.Key], kv.Value)
		}
		if err := file.Close(); err != nil {
			log.Fatalf("can not close reduce file %S", filename)
		}
	}
	temp, err := ioutil.TempFile("", "mr-reduce-*")
	if err != nil {
		log.Fatalf("cannot create tmp file.")
	}
	for key := range keyMap {
		output := reducef(key, keyMap[key])
		if _, err := fmt.Fprintf(temp, "%v %v\n", key, output); err != nil {
			log.Fatalf("write reduce temp file:%S key:%S, value:%S error", temp.Name(), key, output)
		}
	}
	if err := os.Rename(temp.Name(), fullPathFile("mr-out-"+strconv.Itoa(reply.RId))); err != nil {
		log.Fatalf("can not rename reduce temp file %S", temp.Name())
	}
	if err := temp.Close(); err != nil {
		log.Fatalf("can not close file %S", temp.Name())
	}
	args := FinishReduceArgs{RId: reply.RId}
	call("Master.FinishReduce", &args, &FinishReduceReply{})
}

func GetTask() *QueryTaskReply {
	args := QueryTaskArgs{}
	reply := QueryTaskReply{}
	call("Master.QueryTask", &args, &reply)
	return &reply
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(S).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func fullPathFile(file string) string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("can not get current dir.")
	}
	return path.Join(dir, file)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
