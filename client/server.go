package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sandbox/scheduler"
)

// the client sends jobs to scheduler through http requests
// client takes as an input a distribution and knows it's cluster size
// the distribution dictates the job sizes to create and send to the scheduler

var client Client

func RegisterHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, span := client.tracer.Start(r.Context(), "hello-span")
		defer span.End()
		fmt.Fprintf(w, "Hello!")
	})
}

func SendJob(j scheduler.Job) {

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	// do some pre-processing of job
	err := enc.Encode(j)

	if err != nil {
		// should return error to borrowed requests
		fmt.Printf("couldn't encode job\n")
		return
	}

	_, err = http.Post(client.SchedulerURL+"/", "application/json", buf)
	if err != nil {
		fmt.Printf("couldn't send job to scheduler\n")
		return
	}
}
