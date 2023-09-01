package main

import (
	"context"
	"fmt"
	stlog "log"
	"sandbox/grades"
	"sandbox/log"
	"sandbox/registry"
	"sandbox/service"
)

func main() {
	host, port := "localhost", "6000"
	serviceAddr := fmt.Sprintf("http://%v:%v", host, port)

	var reg registry.Registration
	reg.ServiceName = registry.GradingService
	reg.ServiceURL = serviceAddr
	reg.RequiredServices = []registry.ServiceName{registry.LogService}
	reg.ServiceUpdateURL = reg.ServiceURL + "/services"
	reg.HeartbeatURL = reg.ServiceURL + "/heartbeat"

	ctx, err := service.Start(context.Background(),
		host,
		port,
		reg,
		grades.RegisterHandlers)
	if err != nil {
		stlog.Fatal(err)
	}
	if logProvider, err := registry.GetProvider(registry.LogService); err == nil {
		fmt.Printf("Logging service found at: %v\n", logProvider)
		log.SetClientLogger(logProvider, reg.ServiceName)
	}

	<-ctx.Done()
	fmt.Println("Shutting down grading service")
}
