package client

import (
	"fmt"
	"net/http"
)

// the client sends jobs to scheduler through http requests
// client takes as an input a distribution and knows it's cluster size
// the distribution dictates the job sizes to create and send to the scheduler

func RegisterHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello!")
	})
}
