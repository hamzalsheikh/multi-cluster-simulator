package main

//module grading
//go 1.19

import (
	"context"
	"fmt"
	stlog "log"
	"sandbox/log"
	"sandbox/registry"
	"sandbox/service"
)

func main() {
	log.Run("./grading.log")

	host, port := "localhost", "4000"
	serviceAddr := fmt.Sprintf("http://%v:%v", host, port)

	var reg registry.Registration
	reg.ServiceName = registry.LogService
	reg.ServiceURL = serviceAddr
	reg.RequiredServices = make([]registry.ServiceName, 0)
	reg.ServiceUpdateURL = reg.ServiceURL + "/services"
	reg.HeartbeatURL = reg.ServiceURL + "/heartbeat"

	ctx, err := service.Start(context.Background(), host, port, reg, log.RegisterHandlers)
	if err != nil {
		stlog.Fatal(err)
	}
	<-ctx.Done()
	fmt.Println("Shutting down log service")
}
