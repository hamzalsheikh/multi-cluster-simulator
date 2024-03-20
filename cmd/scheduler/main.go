package main

import (
	"context"
	"encoding/json"
	"fmt"
	stlog "log"
	"math/rand"
	"net"
	"os"

	"github.com/hamzalsheikh/multi-cluster-simulator/internal/service"
	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/registry"
	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/scheduler"
	"google.golang.org/grpc"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

func main() {

	// Create trace
	// TODO: jeager currently implemented, maybe add stdout / straight to file
	ctx_trace := context.Background()

	traceProvider, tracer := scheduler.CreateTracer(ctx_trace)

	defer func() { _ = traceProvider.Shutdown(ctx_trace) }()

	scheduler.SetTracer(tracer)

	// get cluster from file
	jsonFile, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println("Can't read file:", os.Args[1])
		panic(err)
	}

	var cluster scheduler.Cluster
	json.Unmarshal(jsonFile, &cluster)

	// choose a port randomly between 1024 to 49151
	portnb := rand.Intn(49151-1025) + 1025
	host, port := "localhost", fmt.Sprint(portnb)
	serviceAddr := fmt.Sprintf("http://%v:%v", host, port)
	scheduler.Run(cluster, serviceAddr)

	// set rpc server
	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%v", host, portnb-1))
	if err != nil {
		// TODO
		return
	}

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterResourceChannelServer(grpcServer, scheduler.NewtraderServer())
	go grpcServer.Serve(lis)

	var reg registry.Registration
	reg.ServiceName = registry.Scheduler
	reg.ServiceURL = serviceAddr
	reg.RequiredServices = []registry.ServiceName{registry.Scheduler}
	reg.ServiceUpdateURL = reg.ServiceURL + "/services"
	reg.HeartbeatURL = reg.ServiceURL + "/heartbeat"

	ctx, err := service.Start(context.Background(),
		host,
		port,
		reg,
		scheduler.RegisterHandlers)
	if err != nil {
		stlog.Fatal(err)
	}
	/*
		if logProvider, err := registry.GetProvider(registry.LogService); err == nil {
			fmt.Printf("Logging service found at: %v\n", logProvider)
			log.SetClientLogger(logProvider, reg.ServiceName)
		}
	*/
	<-ctx.Done()
	fmt.Println("Shutting down scheduler service")
}
