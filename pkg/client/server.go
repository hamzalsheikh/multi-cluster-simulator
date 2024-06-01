package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/scheduler"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// the client sends jobs to scheduler through http requests
// client takes as an input a distribution and knows it's cluster size
// the distribution dictates the job sizes to create and send to the scheduler

var client Client

func RegisterHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, span := client.Tracer.Start(r.Context(), "hello-span")
		defer span.End()
		fmt.Fprintf(w, "Hello!")
	})
	http.HandleFunc("/jobAdded", func(w http.ResponseWriter, r *http.Request) {
		_, span := client.Tracer.Start(r.Context(), "job-added")
		defer span.End()
		fmt.Printf("ack!")
	})

}

func SendJob(j scheduler.Job) {
	// create send job context
	cont := context.Background()

	ctx, span := client.Tracer.Start(cont, "sendJob-span")
	defer span.End()

	// DO some pre-processing of job
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)

	err := enc.Encode(j)
	if err != nil {
		// should return error to borrowed requests?
		fmt.Printf("couldn't encode job\n")
		return
	}

	req, _ := http.NewRequestWithContext(ctx, "POST", client.SchedulerURL+"/delay", buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Referer", client.URL)

	httpClient := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	_, err = httpClient.Do(req)

	//_, err = http.Post(client.SchedulerURL+"/", "application/json", buf)

	if err != nil {
		fmt.Printf("couldn't send job to scheduler\n")
		return
	}
}
