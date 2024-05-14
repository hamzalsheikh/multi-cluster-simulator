package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/registry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// create an instance of scheduler
var sched = Scheduler{WQueueLock: new(sync.Mutex), RQueueLock: new(sync.Mutex), LQueueLock: new(sync.Mutex), BQueueLock: new(sync.Mutex), L0Lock: new(sync.Mutex), L1Lock: new(sync.Mutex), WaitTime: &WaitTime{Lock: new(sync.Mutex)}}

func RegisterHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := sched.tracer.Start(r.Context(), "job-recieved")
		defer span.End()
		fmt.Println("job recieved!")
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
		//span.AddEvent("Appending to ready Queue")
		sched.ReadyQueue = append(sched.ReadyQueue, j)
		fmt.Printf("added job %+v to ready queue %+v\n", j, len(sched.ReadyQueue))

		req, _ := http.NewRequestWithContext(ctx, "GET", r.Header.Get("Referer")+"/jobAdded", nil)
		fmt.Printf("sending request to: %v\n", r.Header.Get("Referer")+"/jobAdded")

		httpClient := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
		req.Header.Set("Content-Type", "application/json")
		_, err = httpClient.Do(req)

	})

	http.HandleFunc("/delay", func(w http.ResponseWriter, r *http.Request) {

		fmt.Println("Job recieved!")
		// decode job object
		var j Job
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&j)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// add job to level 0 and start wait time timer
		sched.L0Lock.Lock()
		j.WaitTime = time.Now()
		sched.Level0 = append(sched.Level0, j)

		sched.WaitTime.Lock.Lock()
		sched.WaitTime.JobsMap[j.Id] = 0
		sched.WaitTime.Total += 1
		sched.WaitTime.Lock.Unlock()
		meter, _ := sched.meter.Int64UpDownCounter("jobs_in_queue")
		meter.Add(context.Background(), 1)
		sched.L0Lock.Unlock()
	})

	http.HandleFunc("/borrow", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("borrow recieved!")
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

			//j.Ownership = r.Response.Request.Host
			fmt.Printf("ownership: %s", j.Ownership)

			sched.LentQueue = append(sched.LentQueue, j)
			fmt.Printf("added job %+v to Lent Queue %+v\n", j, len(sched.LentQueue))
			return
		} else {
			// check if this status means something else
			w.WriteHeader(http.StatusNotAcceptable)
		}

	})

	http.HandleFunc("/lent", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("lent recieved!")
		// decode job object
		var j Job
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&j)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// TODO: implement this in JobFinished
		sched.BQueueLock.Lock()
		defer sched.BQueueLock.Unlock()

		for i := 0; i < len(sched.BorrowedQueue); i++ {
			if j == sched.BorrowedQueue[i] {
				sched.BorrowedQueue = append(sched.BorrowedQueue[:i], sched.BorrowedQueue[i+1:]...)
			}
		}
		fmt.Printf("removed job %+v from borrowed queue %+v\n", j, len(sched.BorrowedQueue))
	})

	http.HandleFunc("/newClient", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("new Client!")
		// get current cluster
		cluster := sched.Cluster
		// preprocessing
		buf := new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		err := enc.Encode(cluster)
		if err != nil {
			return
		}

		w.Write(buf.Bytes())
	})
}

/*
BorrowResources broadcast a message to all other schedulers asking to schedule a job
returns when first scheduler accepts to schedule
or waits for schedulers to say no
*/
func (sched *Scheduler) BorrowResources(j Job) error {
	fmt.Printf("\nIn borrow resources\n")
	schedURLs, err := registry.GetProviders(registry.Scheduler)
	if err != nil {
		return err
	}
	j.Ownership = sched.URL
	fmt.Printf("Borrow resources: got providers %s, i am %s\n", schedURLs, sched.URL)

	// create a wait chan for this dude
	jobScheduled := make(chan error)
	// REMOVE temp for testing
	counter := 0
	// broadcast borrowing request
	for _, schedURL := range schedURLs {

		if schedURL == sched.URL {
			//schedURLs = append(schedURLs[:idx], schedURLs[idx+1:]...)
			counter++
			continue
		}

		// concurrently send borrowing requests to all provider scheds
		go func(j Job, schedURL string) {
			lender, err := url.Parse(schedURL + "/borrow")
			if err != nil {
				fmt.Printf("couldn't parse URL\n")
				jobScheduled <- err
				return
			}
			buf := new(bytes.Buffer)
			enc := json.NewEncoder(buf)
			// do some pre-processing of job
			err = enc.Encode(j)
			if err != nil {
				// should return error to borrowed requests
				fmt.Printf("couldn't encode job\n")
				jobScheduled <- err
				return
			}
			fmt.Printf("lender path is: %s but should be %s\n", lender.Path, schedURL+"/borrow")
			res, err := http.Post(schedURL+"/borrow", "application/json", buf)

			if err != nil {
				fmt.Printf("couldn't send job %v to scheduler %s\n", j.Id, schedURL)
				jobScheduled <- err
				return
			}

			if res.StatusCode == http.StatusOK {
				jobScheduled <- &BorrowSuccess{URL: schedURL}
			} else {
				jobScheduled <- fmt.Errorf("couldn't schedule, status %v", http.StatusText(res.StatusCode))
			}

		}(j, schedURL)

	}

	// wait for responses at chan
	borrowed := errors.New("couldn't find a lender")
	if len(schedURLs) > counter {
		for i := 0; i < len(schedURLs)-counter; i++ {
			// block waiting for a response from goroutines
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
				fmt.Printf("error recieved in BorrowResources: %v\n", resp)
			}
		}
	}
	if borrowed.Error() != "couldn't find a lender" {
		return nil
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

func (sched *Scheduler) ReturnToBorrower(j Job) {
	fmt.Printf("in return to borrower\n")
	borrower, err := url.Parse(j.Ownership + "/lent")
	if err != nil {
		fmt.Printf("couldn't parse borrower url\n")
		return
	}
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	// do some pre-processing of job
	err = enc.Encode(j)
	if err != nil {
		fmt.Printf("couldn't encode job\n")
		return
	}
	attempts := 3

	for i := 0; i < attempts; i++ {
		borrowerURL := j.Ownership + "/lent"
		res, err := http.Post(borrowerURL, "application/json", buf)
		fmt.Printf("sent job %v completion to scheduler with URL: %s\n", j.Id, borrower.Path)
		if err != nil {
			fmt.Printf("couldn't send job to scheduler %s err %s\n", borrower.Path, err)
			return
		}

		if res.StatusCode == http.StatusOK {
			return
		}
	}
}
