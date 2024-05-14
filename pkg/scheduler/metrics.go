package scheduler

import (
	api "go.opentelemetry.io/otel/metric"
)

func RunMetrics() {

	sched.meter.Int64UpDownCounter(
		"jobs_in_queue",
		api.WithUnit("1"),
		api.WithDescription("The number of jobs not scheduled"),
	)

	sched.meter.Int64Histogram(
		"waitTime",
		api.WithUnit("ms"),
		api.WithDescription("Job wait time"),
	)
}
