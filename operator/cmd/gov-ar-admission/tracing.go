package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const tracingInstrumentationName = "github.com/imperium/ai-sovereign-finops-operator/gov-ar-admission"

func initializeTracing(ctx context.Context) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("GOV_AR_TRACING_ENABLED")), "true") {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) == "" && strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) == "" {
		return nil, errors.New("OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is required when GOV_AR_TRACING_ENABLED=true")
	}
	ratio := 1.0
	if raw := strings.TrimSpace(os.Getenv("GOV_AR_TRACE_SAMPLE_RATIO")); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed <= 0 || parsed > 1 {
			return nil, errors.New("GOV_AR_TRACE_SAMPLE_RATIO must be in (0,1]")
		}
		ratio = parsed
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", "gov-ar-admission"),
		attribute.String("service.namespace", "article3"),
		attribute.String("service.version", strings.TrimSpace(os.Getenv("GOV_AR_SOFTWARE_VERSION"))),
	))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func startHTTPServerSpan(r *http.Request, endpoint string) (*http.Request, trace.Span) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := otel.Tracer(tracingInstrumentationName).Start(ctx, "govar.http."+endpoint,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attribute.String("http.request.method", r.Method), attribute.String("http.route", endpoint)),
	)
	return r.WithContext(ctx), span
}

func finishHTTPServerSpan(span trace.Span, status int, started time.Time) {
	span.SetAttributes(
		attribute.Int("http.response.status_code", status),
		attribute.Int64("govar.duration_ns", time.Since(started).Nanoseconds()),
	)
	if status >= http.StatusInternalServerError {
		span.SetStatus(codes.Error, "server error")
	}
	span.End()
}

func startGOVAROperation(ctx context.Context, name string, attributes ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(tracingInstrumentationName).Start(ctx, name, trace.WithAttributes(attributes...))
}

func finishGOVAROperation(span trace.Span, err error, attributes ...attribute.KeyValue) {
	span.SetAttributes(attributes...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "operation failed")
	}
	span.End()
}

func traceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	// A propagated remote parent is a valid W3C context even when this service
	// has no recording tracer. Do not advertise it as a locally observable
	// trace unless the admission span is actually being recorded.
	if !span.IsRecording() {
		return ""
	}
	spanContext := span.SpanContext()
	if !spanContext.IsValid() {
		return ""
	}
	return spanContext.TraceID().String()
}
