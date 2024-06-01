package main

import (
	"context"
	"fmt"
	"github.com/hamzalsheikh/multi-cluster-simulator/internal/service"
	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/client"
	"net/http"
	"os"
)

func main() {

	// Create trace
	ctx := context.Background()

	traceProvider, tracer := service.CreateTracer(ctx)

	defer func() { _ = traceProvider.Shutdown(ctx) }()

	client.SetTracer(tracer)

	client.RegisterHandlers()

	// choose a port randomly between 1024 to 49151
	port := fmt.Sprint(6000)

	var server http.Server
	server.Addr = ":" + port

	go server.ListenAndServe()
	// TODO: input distribution and scheduler info

	schedPort := 2001

	client.Run(fmt.Sprintf("http://%v:%v", os.Getenv("SCHEDULER_HOST"), schedPort), fmt.Sprintf("http://%v:%v", "localhost", port))
}
