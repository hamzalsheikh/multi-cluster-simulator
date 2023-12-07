package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sandbox/client"

	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

//var tracer trace.Tracer

func newExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	// Your preferred exporter: console, jaeger, zipkin, OTLP, etc.
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://localhost:14268/api/traces")))
	return exp, err
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	// Ensure default SDK resources and the required service name are set.
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("ExampleServiceClient"),
		),
	)

	if err != nil {
		panic(err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}

/*
	func test() {
		ctx := context.Background()

		_, span := tracer.Start(ctx, "test-span")
		defer span.End()

		fmt.Printf("testing hello span\n")
	}
*/
func main() {
	/*
		ctx := context.Background()

		exp, err := newExporter(ctx)
		if err != nil {
			log.Fatalf("failed to initialize exporter: %v", err)
		}

		// Create a new tracer provider with a batch span processor and the given exporter.
		tp := newTraceProvider(exp)

		// Handle shutdown properly so nothing leaks.
		defer func() { _ = tp.Shutdown(ctx) }()

		otel.SetTracerProvider(tp)

		// Finally, set the tracer that can be used for this package.
		tracer = tp.Tracer("ClientService")

		client.SetTracer(tracer)

		// end of trace code
	*/
	ctx := context.Background()
	client.SetTracer(client.CreateTracer(ctx))

	client.RegisterHandlers()

	// choose a port randomly between 1024 to 49151
	port := fmt.Sprint(rand.Intn(49151-1024) + 1024)
	fmt.Printf("Client port is %v\n", port)

	var server http.Server
	server.Addr = ":" + port

	go server.ListenAndServe()
	// input distribution and scheduler info

	schedPort := os.Args[1]

	client.Run(fmt.Sprintf("http://%v:%v", "localhost", schedPort))
}
