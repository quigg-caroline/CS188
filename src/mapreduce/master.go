package mapreduce

import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
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
			worker := WorkerInfo {address: msg}
			mr.Workers[msg] = &worker
			mr.AvailableWorkers =  append(mr.AvailableWorkers, msg)
			log.Printf("logging worker... " + msg)
		// map/reduce jobs have completed
		default:
			// if we have map jobs to do and available workers
			if mapJob < mr.nMap-1 && len(mr.AvailableWorkers) > 0 {
				jobs := min(mr.nMap - mapJob, len(mr.AvailableWorkers))
				c := make(chan int, jobs)
				for i :=0; i < jobs ; i++ {
					go func(i int, startingJob int, c chan int){
						// send RPC to job to do 
						var reply DoJobReply

						args := DoJobArgs{mr.file, Map, i + startingJob, mr.nReduce }

						ok := call(mr.AvailableWorkers[i], "Worker.DoJob", args, &reply)

						if ok == false {
							fmt.Printf("Map: RPC %s map error\n", mr.AvailableWorkers[i])
						}

						c <- 1
					}(i,mapJob,c)
				}
				for i := 0; i < jobs; i++ {
					<-c    // wait for one task to complete
				}
				mapJob += jobs
			} else if reduceJob < mr.nReduce-1 && len(mr.AvailableWorkers) >0 {
				jobs := min(mr.nReduce - reduceJob, len(mr.AvailableWorkers))
				c := make(chan int, jobs)
				for i :=0; i < jobs ; i++ {
					go func(i int, startingJob int,c chan int){
						// send RPC to job to do 
						var reply DoJobReply

						args := DoJobArgs{mr.file, Reduce, i + startingJob, mr.nMap }

						ok := call(mr.AvailableWorkers[i], "Worker.DoJob", args, &reply)

						if ok == false {
							fmt.Printf("Reduce: RPC %s reduce error\n", mr.AvailableWorkers[i])
						}
						c <- 1
					}(i, reduceJob,c)
				}
				for i := 0; i < jobs; i++ {
					<-c    // wait for one task to complete
				}
				reduceJob += jobs
			} else {
				return mr.KillWorkers()
			}
		}
	}
	
}
