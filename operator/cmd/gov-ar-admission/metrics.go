package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarobservability"
)

const unclassifiedMetricProfile = "unclassified"

// serviceMetrics owns one private Prometheus registry. Raw tenant, policy,
// request, workload, reservation, and trace identifiers are never labels.
type serviceMetrics struct {
	registry *govarobservability.Registry

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec

	tenantProfileByID   map[string]string
	policyProfileByName map[string]string
}

func newServiceMetricsFromEnvironment() (*serviceMetrics, error) {
	tenantMap, err := parseMetricProfileMap("GOV_AR_TENANT_PROFILE_MAP")
	if err != nil {
		return nil, err
	}
	policyMap, err := parseMetricProfileMap("GOV_AR_POLICY_PROFILE_MAP")
	if err != nil {
		return nil, err
	}
	tenantProfiles := configuredMetricProfiles(tenantMap)
	policyProfiles := configuredMetricProfiles(policyMap)
	native := prometheus.NewRegistry()
	if err := native.Register(collectors.NewGoCollector()); err != nil {
		return nil, err
	}
	if err := native.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, err
	}
	registry, err := govarobservability.NewWith(govarobservability.Config{
		TenantProfiles: tenantProfiles,
		PolicyProfiles: policyProfiles,
	}, native, native)
	if err != nil {
		return nil, err
	}
	metrics := &serviceMetrics{
		registry: registry, tenantProfileByID: tenantMap, policyProfileByName: policyMap,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_http_requests_total",
			Help: "Authenticated GOV-AR API requests by bounded endpoint, method, and status class.",
		}, []string{"endpoint", "method", "status_class"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "govar_http_request_duration_seconds",
			Help:    "GOV-AR API request duration by bounded endpoint and method.",
			Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"endpoint", "method"}),
	}
	if err := native.Register(metrics.httpRequests); err != nil {
		return nil, err
	}
	if err := native.Register(metrics.httpDuration); err != nil {
		return nil, err
	}
	return metrics, nil
}

func parseMetricProfileMap(environment string) (map[string]string, error) {
	raw := strings.TrimSpace(os.Getenv(environment))
	if raw == "" {
		return map[string]string{}, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	result := map[string]string{}
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode %s: %w", environment, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s contains trailing JSON data", environment)
	}
	if len(result) > 64 {
		return nil, fmt.Errorf("%s contains %d mappings; maximum is 64", environment, len(result))
	}
	for identity, profile := range result {
		if strings.TrimSpace(identity) == "" || strings.TrimSpace(profile) != profile || profile == "" {
			return nil, fmt.Errorf("%s contains an empty identity or invalid profile", environment)
		}
	}
	return result, nil
}

func configuredMetricProfiles(mapping map[string]string) []string {
	seen := map[string]struct{}{unclassifiedMetricProfile: {}}
	for _, profile := range mapping {
		seen[profile] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for profile := range seen {
		result = append(result, profile)
	}
	sort.Strings(result)
	return result
}

func (m *serviceMetrics) tenantProfile(tenantID string) string {
	if profile := m.tenantProfileByID[tenantID]; profile != "" {
		return profile
	}
	return unclassifiedMetricProfile
}

func (m *serviceMetrics) handler() http.Handler { return m.registry.Handler() }

type statusCapturingWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCapturingWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Unwrap lets net/http.ResponseController retain optional capabilities of the
// original writer without copying identity-bearing request data into metrics.
func (w *statusCapturingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusCapturingWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func instrumentHTTP(next http.Handler, metrics *serviceMetrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		endpoint := boundedEndpoint(r.URL.Path)
		r, span := startHTTPServerSpan(r, endpoint)
		captured := &statusCapturingWriter{ResponseWriter: w}
		next.ServeHTTP(captured, r)
		status := captured.status
		if status == 0 {
			status = http.StatusOK
		}
		method := r.Method
		if method != http.MethodGet && method != http.MethodPost {
			method = "OTHER"
		}
		if metrics != nil {
			metrics.httpRequests.WithLabelValues(endpoint, method, strconv.Itoa(status/100)+"xx").Inc()
			metrics.httpDuration.WithLabelValues(endpoint, method).Observe(time.Since(started).Seconds())
			if endpoint == "admit" {
				_ = metrics.registry.ObserveDecisionDuration(time.Since(started))
			}
		}
		finishHTTPServerSpan(span, status, started)
	})
}

func boundedEndpoint(path string) string {
	switch {
	case path == "/healthz":
		return "healthz"
	case path == "/readyz":
		return "readyz"
	case path == "/metrics":
		return "metrics"
	case path == "/v1/admit":
		return "admit"
	case path == "/v1/dispatch":
		return "dispatch"
	case path == "/v1/settle":
		return "settle"
	case path == "/v1/cancel":
		return "cancel"
	case strings.HasPrefix(path, "/v1/liability/"):
		return "liability"
	default:
		return "unknown"
	}
}

func recordAdmissionDecision(metrics *serviceMetrics, ctx context.Context, decision govar.Decision, reason govar.ReasonCode, method string) {
	if method == "" {
		method = "none"
	}
	if metrics != nil {
		if err := metrics.registry.RecordAdmissionDecision(string(decision), string(reason), method); err != nil {
			trace.SpanFromContext(ctx).RecordError(err)
		}
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("govar.decision", string(decision)), attribute.String("govar.reason_code", string(reason)), attribute.String("govar.reservation_method", method))
}

func recordCommittedTransition(metrics *serviceMetrics, ctx context.Context, from, to govar.ReservationState, reason govar.ReasonCode, effective bool) {
	if metrics != nil {
		if err := metrics.registry.RecordCommittedTransition(govarobservability.CommittedTransition{From: string(from), To: string(to), Reason: string(reason), Effective: effective}); err != nil {
			trace.SpanFromContext(ctx).RecordError(err)
		}
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("govar.transition_from", string(from)), attribute.String("govar.transition_to", string(to)), attribute.String("govar.reason_code", string(reason)), attribute.Bool("govar.effective", effective))
}

type committedObservabilityBackend interface {
	ObservabilitySnapshot(context.Context, string) (govar.TenantObservabilitySnapshot, error)
	ComponentObservabilitySnapshot(context.Context) (govar.ComponentObservabilitySnapshot, error)
}

func publishCommittedTenantMetrics(ctx context.Context, metrics *serviceMetrics, engine committedObservabilityBackend, tenantID string) error {
	if metrics == nil || strings.TrimSpace(tenantID) == "" {
		return nil
	}
	tenant, err := engine.ObservabilitySnapshot(ctx, tenantID)
	if err != nil {
		return err
	}
	if err := metrics.registry.ApplyCommittedTenantSnapshot(govarobservability.TenantMonetarySnapshot{
		TenantProfile:  metrics.tenantProfile(tenantID),
		ReservedMicros: int64(tenant.ReservedMicros), SettledMicros: int64(tenant.SettledMicros),
		OutstandingLiabilityMicros: int64(tenant.OutstandingLiabilityMicros), CarriedDebtMicros: int64(tenant.CarriedDebtMicros),
	}); err != nil {
		return err
	}
	components, err := engine.ComponentObservabilitySnapshot(ctx)
	if err != nil {
		return err
	}
	reservation := make(map[string]int64, len(components.ReservationMicros))
	settlement := make(map[string]int64, len(components.SettlementMicros))
	for basis, amount := range components.ReservationMicros {
		reservation[basis] = int64(amount)
	}
	for basis, amount := range components.SettlementMicros {
		settlement[basis] = int64(amount)
	}
	return metrics.registry.ApplyCommittedComponentSnapshot(govarobservability.ComponentSnapshot{
		ReservationMicros: reservation, SettlementMicros: settlement,
	})
}

func observeCommittedTransaction(metrics *serviceMetrics, started time.Time, err error) {
	if metrics != nil && err == nil {
		_ = metrics.registry.ObserveCommittedTransactionDuration(time.Since(started))
	}
}

func recordMetricError(ctx context.Context, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		trace.SpanFromContext(ctx).RecordError(err)
	}
}
