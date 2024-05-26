package scheduler

import (
	"context"
	"os"
	"time"

	api "go.opentelemetry.io/otel/metric"
)

func RunMetrics() {

	sched.meter.Int64UpDownCounter(
		"jobs_in_queue",
		api.WithUnit("1"),
		api.WithDescription("The number of jobs not scheduled"),
	)

	waitHistogram, _ := sched.meter.Float64Histogram(
		os.Getenv("SERVICE_NAME")+"waitTime",
		api.WithUnit("ms"),
		api.WithDescription("Average Job Wait Time"),
	)

	go func() {
		for {
			waitHistogram.Record(context.TODO(), sched.WaitTime.GetAverage())
			time.Sleep(5 * time.Second)
		}
	}()
}
