// package tracing contains some helpers to make working with opentracing
// a tad simpler
package tracing

import (
	"context"
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
)

// NewSpan pulls the span out of the context, creates a new child span, and updates the context
// callers must call span.Finish() when done
// important: the context must contain a span, otherwise you'll see a nilpointer panic here.
func NewSpan(ctx context.Context, tracer opentracing.Tracer, name string) (context.Context, opentracing.Span) {
	span := opentracing.SpanFromContext(ctx)
	spanCtx := span.Context()
	span = tracer.StartSpan(name, opentracing.ChildOf(spanCtx))
	ctx = opentracing.ContextWithSpan(ctx, span)
	return ctx, span
}

// Error logs error
func Error(span opentracing.Span, err error) {
	span.LogFields(log.Error(err))
}

// Errorf logs error
func Errorf(span opentracing.Span, format string, a ...interface{}) {
	span.LogFields(log.Error(fmt.Errorf(format, a...)))
}

// Failure marks the current request as a failure
func Failure(span opentracing.Span) {
	tags.Error.Set(span, true)
}

// ExtractTraceID attempts to extract the traceID from a Context
func ExtractTraceID(ctx context.Context) (string, bool) {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return "", false
	}
	sctx, ok := sp.Context().(jaeger.SpanContext)
	if !ok {
		return "", false
	}

	return sctx.TraceID().String(), sctx.IsSampled()
}
