package main

import (
	"context"
	"encoding/json"
	"fmt"
	stlog "log"
	"net"
	"os"

	"github.com/hamzalsheikh/multi-cluster-simulator/internal/service"
	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/registry"
	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/scheduler"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

func main() {

	// Create trace
	// TODO: jeager currently implemented, maybe add stdout / straight to file
	ctx_trace := context.Background()

	traceProvider, tracer := service.CreateTracer(ctx_trace)

	defer func() { _ = traceProvider.Shutdown(ctx_trace) }()

	scheduler.SetTracer(tracer)

	meterProvider, err := service.CreateMeterProvider(ctx_trace)
	if err != nil {
		fmt.Printf("Couldn't create meter provider")
		panic(err)
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		if err := meterProvider.Shutdown(ctx_trace); err != nil {
			fmt.Println(err)
		}
	}()

	scheduler.SetMeter(meterProvider.Meter(os.Getenv("SERVICE_NAME") + "Scheduler"))

	scheduler.SetLogger(service.CreateLogger())

	// get cluster from file
	jsonFile, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println("Can't read file:", os.Args[1])
		panic(err)
	}

	var cluster scheduler.Cluster
	json.Unmarshal(jsonFile, &cluster)

	// choose a port randomly between 1024 to 49151
	portnb := 2001
	host, port := os.Getenv("HOST"), fmt.Sprint(portnb)
	serviceAddr := fmt.Sprintf("http://%v:%v", host, port)
	scheduler.Run(cluster, serviceAddr)

	// set rpc server
	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%v", host, portnb-1))
	if err != nil {
		fmt.Println(err)
		return
	}

	//var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))

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
	<-ctx.Done()
	fmt.Println("Shutting down scheduler service")
}
