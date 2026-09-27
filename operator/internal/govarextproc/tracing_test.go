package govarextproc

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type traceCaptureTransport struct{ traceparent string }

func (t *traceCaptureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.traceparent = request.Header.Get("traceparent")
	return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
}

func TestInternalLedgerPostPropagatesW3CTraceContext(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true,
	}))
	transport := &traceCaptureTransport{}
	server := &Server{AdmissionURL: "http://govar.invalid", MasterSecret: []byte(strings.Repeat("s", 32)), HTTPClient: &http.Client{Transport: transport}}
	state := streamState{binding: RouteBinding{Namespace: "synthetic", TenantID: "tenant", WorkloadUID: "workload"}}
	if err := server.post(ctx, state, "/v1/dispatch", map[string]any{"synthetic": true}, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(transport.traceparent, "00-"+traceID.String()+"-") {
		t.Fatalf("traceparent was not propagated: %q", transport.traceparent)
	}
}
