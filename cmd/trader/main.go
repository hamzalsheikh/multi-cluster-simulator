package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"

	"github.com/hamzalsheikh/multi-cluster-simulator/internal/service"
	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/registry"
	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/hamzalsheikh/multi-cluster-simulator/pkg/trader/gen"
)

func main() {

	// Create trace
	// TODO: jeager currently implemented, maybe add stdout / straight to file
	ctx := context.Background()

	traceProvider, tracer := service.CreateTracer(ctx)

	defer func() { _ = traceProvider.Shutdown(ctx) }()

	trader.SetTracer(tracer)

	meterProvider, err := service.CreateMeterProvider(ctx)

	if err != nil {
		fmt.Printf("Couldn't create meter provider")
		panic(err)
	}
	// Handle shutdown properly so nothing leaks.
	defer func() {
		if err := meterProvider.Shutdown(ctx); err != nil {
			fmt.Println(err)
		}
	}()
	trader.SetMeter(meterProvider.Meter("Trader"))
	// choose a port randomly between 1024 to 49151
	portnb := rand.Intn(49151-1025) + 1025
	host, port := "localhost", fmt.Sprint(portnb)
	serviceAddr := fmt.Sprintf("http://%v:%v", host, portnb)
	fmt.Printf("Trader port is %v\n", port)

	schedPort := os.Args[1]
	conn, err := grpc.NewClient(fmt.Sprintf("%v:%v", host, schedPort), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		fmt.Println(err)
		return
	}

	// set rpc server
	grpcURL := fmt.Sprintf("%v:%v", host, portnb-1)
	lis, err := net.Listen("tcp", grpcURL)
	if err != nil {
		fmt.Println(err)
		return
	}
	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))

	pb.RegisterTraderServer(grpcServer, trader.NewTraderServer())
	go grpcServer.Serve(lis)
	var reg registry.Registration

	reg.ServiceName = registry.Trader
	reg.ServiceURL = grpcURL
	reg.RequiredServices = []registry.ServiceName{registry.Trader}
	reg.ServiceUpdateURL = serviceAddr + "/services"
	reg.HeartbeatURL = serviceAddr + "/heartbeat"

	ctx, err = service.StartWithRPC(context.Background(),
		host,
		port,
		reg)
	if err != nil {
		log.Fatal(err)
	}
	//defer conn.Close()
	client := pb.NewResourceChannelClient(conn)
	trader.Run(fmt.Sprintf("http://%v:%v", "localhost", schedPort), grpcURL, client, service.CreateLogger())

}
