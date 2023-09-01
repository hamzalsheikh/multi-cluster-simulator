package main

import (
	"context"
	"fmt"
	stlog "log"
	"sandbox/log"
	"sandbox/registry"
	"sandbox/service"
	"sandbox/teacherportal"
)

func main() {
	err := teacherportal.ImportTemplates()
	if err != nil {
		stlog.Fatal(err)
	}

	host, port := "localhost", "5000"
	serviceAddress := fmt.Sprintf("http://%v:%v", host, port)

	var reg registry.Registration
	reg.ServiceName = registry.TeacherPortal
	reg.ServiceURL = serviceAddress
	reg.RequiredServices = []registry.ServiceName{
		registry.LogService,
		registry.GradingService,
	}
	reg.ServiceUpdateURL = reg.ServiceURL + "/services"
	reg.HeartbeatURL = reg.ServiceURL + "/heartbeat"

	ctx, err := service.Start(context.Background(),
		host,
		port,
		reg,
		teacherportal.RegisterHandlers)
	if err != nil {
		stlog.Fatal(err)
	}
	if logProvider, err := registry.GetProvider(registry.LogService); err == nil {
		log.SetClientLogger(logProvider, reg.ServiceName)
	}

	<-ctx.Done()
	fmt.Println("Shutting down teacher portal")

}
