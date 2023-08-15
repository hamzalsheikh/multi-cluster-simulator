package service

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

func Start(ctx context.Context, serviceName, host, port string,
	registerHandlersFunc func()) (context.Context, error) {
	registerHandlersFunc()
	ctx = startService(ctx, serviceName, host, port)

	return ctx, nil
}

func startService(ctx context.Context, serviceName, host, port string) context.Context {
	ctx, cancel := context.WithCancel(ctx)

	var server http.Server
	server.Addr = ":" + port

	go func() {
		log.Println(server.ListenAndServe())
		cancel()
	}()

	// Go routine listening to shutdown service
	go func() {

		fmt.Printf("%v started. Press any key to stop\n", serviceName)
		var s string
		// waiting for key
		fmt.Scanln(&s)
		server.Shutdown(ctx)
		cancel()
	}()
	return ctx
}
