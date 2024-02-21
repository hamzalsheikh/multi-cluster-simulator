package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader"
)

func main() {

	// Create trace
	// TODO: jeager currently implemented, maybe add stdout / straight to file
	ctx := context.Background()

	traceProvider, tracer := trader.CreateTracer(ctx)

	defer func() { _ = traceProvider.Shutdown(ctx) }()

	trader.SetTracer(tracer)

	trader.RegisterHandlers()

	// choose a port randomly between 1024 to 49151
	port := fmt.Sprint(rand.Intn(49151-1024) + 1024)
	fmt.Printf("Trader port is %v\n", port)

	var server http.Server
	server.Addr = ":" + port

	go server.ListenAndServe()
	schedPort := os.Args[1]

	trader.Run(fmt.Sprintf("http://%v:%v", "localhost", schedPort), fmt.Sprintf("http://%v:%v", "localhost", port))
}
