package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
)

func TestTracingDisabledPropagatesButDoesNotAdvertiseUnrecordedContext(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	t.Setenv("GOV_AR_TRACING_ENABLED", "false")
	shutdown, err := initializeTracing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	carrier := propagation.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	if got := traceIDFromContext(ctx); got != "" {
		t.Fatalf("unrecorded remote trace was advertised in response: %q", got)
	}
}

func TestTraceIDFromContextReturnsRecordingAdmissionSpan(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})
	ctx, span := otel.Tracer(tracingInstrumentationName).Start(context.Background(), "test")
	defer span.End()
	if got := traceIDFromContext(ctx); got == "" || got != span.SpanContext().TraceID().String() {
		t.Fatalf("recording trace id=%q span=%q", got, span.SpanContext().TraceID())
	}
}

func TestTracingEnabledRequiresOTLPEndpoint(t *testing.T) {
	t.Setenv("GOV_AR_TRACING_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	if _, err := initializeTracing(context.Background()); err == nil {
		t.Fatal("enabled tracing accepted no OTLP endpoint")
	}
}

func TestTracingRejectsInvalidSampleRatio(t *testing.T) {
	t.Setenv("GOV_AR_TRACING_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
	t.Setenv("GOV_AR_TRACE_SAMPLE_RATIO", "0")
	if _, err := initializeTracing(context.Background()); err == nil {
		t.Fatal("enabled tracing accepted zero sample ratio")
	}
}

func TestOTLPExporterOutageDoesNotAlterLedgerReadiness(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Setenv("GOV_AR_TRACING_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("GOV_AR_TRACE_SAMPLE_RATIO", "1")
	shutdown, err := initializeTracing(context.Background())
	if err != nil {
		t.Fatalf("configure nonblocking exporter: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = shutdown(ctx)
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	srv := &server{engine: govar.NewEngine()}
	recorder := httptest.NewRecorder()
	instrumentHTTP(http.HandlerFunc(srv.handleReadyz), nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "ok" {
		t.Fatalf("telemetry outage changed ledger readiness: status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
