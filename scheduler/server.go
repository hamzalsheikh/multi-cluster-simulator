package scheduler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

/*
type Node struct {
	Id     uint
	Type   string
	URL    string
	Memory uint
	Cores  []uint

	MemoryAvailable uint
	CoresAvailable  uint
	RunningJobs     map[uint]Job
	mutex           *sync.Mutex
}

*/

// temp cluster instance
var c = Cluster{Id: 1, Nodes: []Node{{Id: 1, MemoryAvailable: 10, CoresAvailable: 10, mutex: new(sync.Mutex), RunningJobs: make(map[uint]Job)}}}

// create an instance of scheduler
var sched = Scheduler{WQueueLock: new(sync.Mutex), RQueueLock: new(sync.Mutex), Policy: FIFO,
	Cluster: &c}

func RegisterHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("recieved!")
		// decode job object
		var j Job
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&j)
		fmt.Printf("%s\n", err)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// add job to ready queue
		sched.RQueueLock.Lock()
		defer sched.RQueueLock.Unlock()

		sched.ReadyQueue = append(sched.ReadyQueue, j)
		fmt.Printf("added job %+v to ready queue %+v\n", j, len(sched.ReadyQueue))
	})
}
