package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	failed bool
	jobId int
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func remove(slice []string, i int) []string {
    return append(slice[:i], slice[i+1:]...)
}


func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	//listen for workers & then give them a job! 


	mapJob := 0
	reduceJob := 0
	for{
		select {
		// register new workers
		case msg := <- mr.registerChannel:
			// register channel address
			worker := WorkerInfo {address: msg, failed: false, jobId: -1}
			mr.Workers[msg] = &worker
			mr.AvailableWorkers =  append(mr.AvailableWorkers, msg)
			go func(msg string) { mr.jobChannel <- msg }(msg)
		// worker is ready for a job
		case msg := <- mr.jobChannel:
			workerInfo := mr.Workers[msg]
			if mapJob < mr.nMap {
				go func (jobId int, worker *WorkerInfo){
					// send RPC to job to do 
					var reply DoJobReply
					info := *worker
					
					args := DoJobArgs{mr.file, Map, jobId, mr.nReduce }

					ok := call(info.address, "Worker.DoJob", args, &reply)

					// if RPC fails, log failed job num & index of failed worker
					if ok == false {
						fmt.Printf("Map: RPC %s map error\n", info.address)
					}
					go func(msg string) { mr.jobChannel <- msg }(info.address)

				}(mapJob,workerInfo)
				mapJob+=1
				
			} else if reduceJob < mr.nReduce {
				go func (jobId int, worker *WorkerInfo){
					// send RPC to job to do 
					var reply DoJobReply
					info := *worker
					
					args := DoJobArgs{mr.file, Reduce, jobId, mr.nMap }

					ok := call(info.address, "Worker.DoJob", args, &reply)

					// if RPC fails, log failed job num & index of failed worker
					if ok == false {
						fmt.Printf("Reduce: RPC %s reduce error\n", info.address)
					}
					go func(msg string) { mr.jobChannel <- msg }(info.address)

				}(reduceJob,workerInfo)
				reduceJob+=1
			} else {
				return mr.KillWorkers()
			}
		default:
			fmt.Printf("")
		}

	}
	
}
