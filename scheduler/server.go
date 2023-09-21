package scheduler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sandbox/registry"
	"sync"
)

// create an instance of scheduler
var sched = Scheduler{WQueueLock: new(sync.Mutex), RQueueLock: new(sync.Mutex), Policy: FIFO}

func RegisterHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("recieved!")
		// decode job object
		var j Job
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&j)

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

	http.HandleFunc("/sched", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("recieved!")
		// decode job object
		var j Job
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&j)

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

func (sched *Scheduler) BorrowResources(j Job) {
	schedURLs, err := registry.GetProviders(registry.Scheduler)
	if err == nil {
		for _, sched := range schedURLs {
			fmt.Printf("URL: %s\n", sched)
		}
	}
}
