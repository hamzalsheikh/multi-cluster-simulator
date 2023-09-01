package main

import (
	"context"
	"fmt"
	"grading/registry"
	"log"
	"net/http"
)

func main() {
	registry.SetupRegistryService()
	http.Handle("/services", &registry.RegistryService{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var server http.Server
	server.Addr = registry.ServerPort

	go func() {
		log.Println(server.ListenAndServe())
		cancel()
	}()

	// Go routine listening to shutdown service
	go func() {

		fmt.Println("Registry server started. Press any key to stop")
		var s string
		// waiting for key
		fmt.Scanln(&s)
		server.Shutdown(ctx)
		cancel()
	}()
	<-ctx.Done()
	fmt.Println("Shutting down registry service")
}
