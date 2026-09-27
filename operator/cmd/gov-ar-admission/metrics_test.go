package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
)

type snapshotMetricsBackend struct{ *govar.Engine }

func (snapshotMetricsBackend) ObservabilitySnapshot(context.Context, string) (govar.TenantObservabilitySnapshot, error) {
	return govar.TenantObservabilitySnapshot{TenantID: "private-tenant", ReservedMicros: 17,
		SettledMicros: 11, OutstandingLiabilityMicros: 17, CarriedDebtMicros: 3,
		ActiveReservations: 1, CurrentWindowID: "private-window", CapturedAt: time.Unix(1, 0).UTC()}, nil
}

func (snapshotMetricsBackend) ComponentObservabilitySnapshot(context.Context) (govar.ComponentObservabilitySnapshot, error) {
	return govar.ComponentObservabilitySnapshot{ReservationMicros: map[string]govar.MoneyMicros{"input_tokens": 17},
		SettlementMicros: map[string]govar.MoneyMicros{"output_tokens": 11}, CapturedAt: time.Unix(1, 0).UTC()}, nil
}

func TestBoundedEndpointDoesNotExposeTenantOrRequestIdentity(t *testing.T) {
	tests := map[string]string{
		"/v1/admit":                       "admit",
		"/v1/liability/private-tenant-id": "liability",
		"/anything/request-123":           "unknown",
	}
	for path, expected := range tests {
		if got := boundedEndpoint(path); got != expected {
			t.Fatalf("boundedEndpoint(%q)=%q want %q", path, got, expected)
		}
	}
}

func TestInstrumentHTTPPreservesStatus(t *testing.T) {
	metrics, err := newServiceMetricsFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	handler := instrumentHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "synthetic", http.StatusTeapot)
	}), metrics)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/admit", nil))
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("status=%d want %d", recorder.Code, http.StatusTeapot)
	}
}

func TestMetricProfileMapRejectsRawOrUnboundedConfiguration(t *testing.T) {
	t.Setenv("GOV_AR_TENANT_PROFILE_MAP", `{"tenant-a":"balanced","tenant-b":"long-output"}`)
	t.Setenv("GOV_AR_POLICY_PROFILE_MAP", `{"routing":"strict"}`)
	metrics, err := newServiceMetricsFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if got := metrics.tenantProfile("tenant-a"); got != "balanced" {
		t.Fatalf("profile=%q", got)
	}
	if got := metrics.tenantProfile("unmapped-private-id"); got != unclassifiedMetricProfile {
		t.Fatalf("unmapped profile=%q", got)
	}
	t.Setenv("GOV_AR_TENANT_PROFILE_MAP", `{"tenant-a":"raw/profile"}`)
	if _, err := newServiceMetricsFromEnvironment(); err == nil {
		t.Fatal("unbounded profile label was accepted")
	}
}

func TestCommittedMetricPublicationUsesSnapshotBarrierAndNoRawIdentityLabel(t *testing.T) {
	t.Setenv("GOV_AR_TENANT_PROFILE_MAP", `{"private-tenant":"balanced"}`)
	metrics, err := newServiceMetricsFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	backend := snapshotMetricsBackend{Engine: govar.NewEngine()}
	if err := publishCommittedTenantMetrics(context.Background(), metrics, backend, "private-tenant"); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	metrics.handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, expected := range []string{
		`govar_reserved_micros{tenant_profile="balanced"} 17`,
		`govar_settled_micros{tenant_profile="balanced"} 11`,
		`govar_carried_debt_micros{tenant_profile="balanced"} 3`,
		`govar_reservation_component_micros{basis="input_tokens"} 17`,
		`govar_settlement_component_micros{basis="output_tokens"} 11`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("scrape lacks %q", expected)
		}
	}
	for _, forbidden := range []string{"private-tenant", "private-window"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("scrape exposed raw identity %q", forbidden)
		}
	}
}
