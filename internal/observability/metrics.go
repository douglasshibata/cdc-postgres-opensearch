package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var (
	Tracer             trace.Tracer
	Meter              metric.Meter
	SyncLagHistogram   metric.Float64Histogram
	BulkItemsCounter   metric.Int64Counter
	SearchDurationHist metric.Float64Histogram
)

// InitObservability initializes OpenTelemetry tracers, meters, and custom metrics.
func InitObservability(serviceName string) (func(context.Context) error, error) {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	Tracer = otel.Tracer(serviceName)

	mp := sdkmetric.NewMeterProvider()
	otel.SetMeterProvider(mp)
	Meter = otel.Meter(serviceName)

	var err error
	SyncLagHistogram, err = Meter.Float64Histogram(
		"cdc_sync_lag_seconds",
		metric.WithDescription("Lag in seconds from PostgreSQL Outbox commit to OpenSearch indexation"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	BulkItemsCounter, err = Meter.Int64Counter(
		"cdc_bulk_items_indexed_total",
		metric.WithDescription("Total number of CDC events indexed into OpenSearch"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	SearchDurationHist, err = Meter.Float64Histogram(
		"opensearch_query_duration_seconds",
		metric.WithDescription("Duration of search queries against OpenSearch"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	cleanup := func(ctx context.Context) error {
		_ = tp.Shutdown(ctx)
		_ = mp.Shutdown(ctx)
		return nil
	}

	return cleanup, nil
}
