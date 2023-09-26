package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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

	http.HandleFunc("/borrow", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("recieved!")
		// decode job object
		var j Job
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&j)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// check if cluster have resources [IMPLEMENT LENDING POLICY ex. check if cluster utilization is below 50%]
		ok := sched.Lend(j)
		if ok == nil {
			w.WriteHeader(http.StatusOK)
			// TODO: wait for a confirmation that you're the chosen scheduler

			// add job to lend queue
			sched.LQueueLock.Lock()
			defer sched.LQueueLock.Unlock()

			j.Ownership = r.Host

			sched.LentQueue = append(sched.LentQueue, j)
			fmt.Printf("added job %+v to Lent Queue %+v\n", j, len(sched.LentQueue))
			return
		} else {
			// check if this status means something else
			w.WriteHeader(http.StatusNotAcceptable)
		}

	})
}

/*
BorrowResources broadcast a message to all other schedulers asking to schedule a job
returns when first scheduler accepts to schedule
or waits for schedulers to say no
*/
func (sched *Scheduler) BorrowResources(j Job) error {
	schedURLs, err := registry.GetProviders(registry.Scheduler)

	// create a wait chan for this dude
	jobScheduled := make(chan error)

	if err != nil {
		return err
	}

	// broadcast borrowing request
	for _, schedURL := range schedURLs {

		if schedURL == sched.URL {
			continue
		}

		// concurrently send borrowing requests to all provider scheds
		go func(j Job, schedURL string) {
			lender, err := url.Parse(schedURL + "/borrow")
			if err != nil {
				jobScheduled <- err
				return
			}
			buf := new(bytes.Buffer)
			enc := json.NewEncoder(buf)
			// do some pre-processing of job
			err = enc.Encode(j)
			if err != nil {
				// should return error to borrowedrequests
				fmt.Printf("couldn't encode job\n")
				jobScheduled <- err
				return
			}

			res, err := http.Post(lender.Path, "application/json", buf)
			if err != nil {
				fmt.Printf("couldn't send job %+v to scheduler %s\n", j, schedURL)
				jobScheduled <- err
				return
			}

			if res.StatusCode == http.StatusOK {
				jobScheduled <- &BorrowSuccess{URL: schedURL}
			} else {
				jobScheduled <- fmt.Errorf("couldn't schedule, status %v", http.StatusText(res.StatusCode))
			}

		}(j, schedURL)

		fmt.Printf("sent job %+v request to scheduler with URL: %s\n", j, schedURL)
	}

	// wait for responses at chan
	borrowed := errors.New("couldn't find a lender")
	for i := 0; i < len(schedURLs); i++ {
		resp := <-jobScheduled
		switch resp.(type) {
		case *BorrowSuccess:

			if borrowed.Error() == "couldn't find a lender" {
				// first scheduler to accept
				borrowed = resp
			}
			/* else {
				// another scheduler accepted the request before this one
				// ask the other sched to abort

			}
			*/

		default:
			fmt.Printf("error recieved: %v", resp)
		}
	}

	return borrowed
}

// error type when succesful
type BorrowSuccess struct {
	URL string
}

func (e *BorrowSuccess) Error() string {
	// can also send url or information through this if we want BorrowSuccess private
	return "ok"
}
