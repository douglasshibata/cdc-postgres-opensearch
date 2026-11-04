package observability

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// StartSpan helper begins a new span using the global tracer.
func StartSpan(ctx context.Context, spanName string) (context.Context, trace.Span) {
	if Tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return Tracer.Start(ctx, spanName)
}
