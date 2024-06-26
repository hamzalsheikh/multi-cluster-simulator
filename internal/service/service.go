package service

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/hamzalsheikh/multi-cluster-simulator/pkg/registry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func Start(ctx context.Context, host, port string, reg registry.Registration,
	registerHandlersFunc func()) (context.Context, error) {
	registerHandlersFunc()
	ctx = startService(ctx, reg.ServiceName, host, port)
	err := registry.RegisterService(reg)
	if err != nil {
		return ctx, err
	}
	return ctx, nil
}

// TODO: move all server / client initializations here (from main)
func StartWithRPC(ctx context.Context, host, port string, reg registry.Registration) (context.Context, error) {
	ctx = startService(ctx, reg.ServiceName, host, port)
	err := registry.RegisterService(reg)
	if err != nil {
		return ctx, err
	}
	return ctx, nil

}

func startService(ctx context.Context, serviceName registry.ServiceName, host, port string) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	handler := http.Handler(http.DefaultServeMux)
	handler = otelhttp.NewHandler(handler, "")
	var server http.Server
	server.Addr = ":" + port
	server.Handler = handler

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
		err := registry.ShutdownService(fmt.Sprintf("http://%v:%v", host, port))
		if err != nil {
			log.Println(err)
		}
		server.Shutdown(ctx)
		cancel()
	}()
	return ctx
}
