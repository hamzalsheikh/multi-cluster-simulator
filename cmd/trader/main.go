package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

func main() {

	// Create trace
	// TODO: jeager currently implemented, maybe add stdout / straight to file
	ctx := context.Background()

	traceProvider, tracer := trader.CreateTracer(ctx)

	defer func() { _ = traceProvider.Shutdown(ctx) }()

	trader.SetTracer(tracer)

	//trader.RegisterHandlers()

	// choose a port randomly between 1024 to 49151
	host, port := "localhost", fmt.Sprint(rand.Intn(49151-1024)+1024)
	fmt.Printf("Trader port is %v\n", port)

	schedPort := os.Args[1]
	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", host, schedPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		return
	}
	//defer conn.Close()
	client := pb.NewResourceChannelClient(conn)
	trader.Run(fmt.Sprintf("http://%v:%v", "localhost", schedPort), fmt.Sprintf("http://%v:%v", "localhost", port), client)
}
