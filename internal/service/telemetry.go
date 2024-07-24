package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
)

var (
	serviceName  = os.Getenv("SERVICE_NAME")
	collectorURL = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	insecure     = os.Getenv("INSECURE_MODE")
	environment  = os.Getenv("GO_ENV")
)

func newResource() (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
}

func newExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	// Your preferred exporter: console, jaeger, zipkin, OTLP, etc.
	secureOption := otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, ""))
	if len(insecure) > 0 {
		secureOption = otlptracegrpc.WithInsecure()
	}

	exp, err := otlptrace.New(
		ctx,
		otlptracegrpc.NewClient(
			secureOption,
			otlptracegrpc.WithEndpoint(collectorURL),
		),
	)
	return exp, err
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	// Ensure default SDK resources and the required service name are set.
	r, err := newResource()
	if err != nil {
		panic(err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}

func CreateTracer(ctx context.Context) (*sdktrace.TracerProvider, trace.Tracer) {

	exp, err := newExporter(ctx)
	if err != nil {
		log.Fatalf("failed to initialize exporter: %v", err)
	}

	// Create a new tracer provider with a batch span processor and the given exporter.
	tp := newTraceProvider(exp)

	// Handle shutdown properly so nothing leaks.
	// defer func() { _ = tp.Shutdown(ctx) }()

	otel.SetTracerProvider(tp)

	//otel.SetTextMapPropagator(propagation.TraceContext{})

	// Finally, set the tracer that can be used for this package.
	return tp, tp.Tracer(serviceName + "Tracer")
}

func CreateMeterProvider(ctx context.Context) (*metric.MeterProvider, error) {

	secureOption := otlpmetricgrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, ""))
	if len(insecure) > 0 {
		secureOption = otlpmetricgrpc.WithInsecure()
	}
	res, err := newResource()
	if err != nil {
		panic(err)
	}

	metricExporter, err := otlpmetricgrpc.New(
		ctx,
		secureOption,
		otlpmetricgrpc.WithEndpoint(collectorURL),
	)
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(10*time.Second))),
	)
	return meterProvider, nil
}

func CreateLogger() zerolog.Logger {
	var output io.Writer = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	if environment != "development" {
		err := os.Mkdir("logs", 0750)
		if err != nil && !os.IsExist(err) {
			log.Fatal(err)
		}
		f, err := os.Create("logs/" + serviceName + "-log-" + fmt.Sprint(time.Now().Format(time.RFC3339)))
		if err != nil {
			fmt.Printf("Couldn't create log file, %s", err)
		}
		// both writes logs to both a log file and console
		if environment == "both" {
			output = zerolog.MultiLevelWriter(output, f)
		} else {
			output = f
		}

	}

	return zerolog.New(output).With().Timestamp().Logger()

}
