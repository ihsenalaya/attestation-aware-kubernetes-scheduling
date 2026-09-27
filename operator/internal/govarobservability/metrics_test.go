package govarobservability

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	registry, err := New(Config{
		TenantProfiles: []string{"balanced", "long-output", "strict-governance"},
		PolicyProfiles: []string{"strict", "adaptive"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return registry
}

func TestMetricContractNamesLabelsAndFrozenBuckets(t *testing.T) {
	r := testRegistry(t)
	mustSeedEveryMetric(t, r)

	families, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	expectedLabels := map[string][]string{
		"govar_admission_decisions_total":    {"decision", "method", "reason"},
		"govar_transition_total":             {"from", "reason", "to"},
		"govar_reserved_micros":              {"tenant_profile"},
		"govar_settled_micros":               {"tenant_profile"},
		"govar_outstanding_liability_micros": {"tenant_profile"},
		"govar_carried_debt_micros":          {"tenant_profile"},
		"govar_reservation_component_micros": {"basis"},
		"govar_settlement_component_micros":  {"basis"},
		"govar_decision_duration_seconds":    nil,
		"govar_transaction_duration_seconds": nil,
		"govar_settlement_delay_seconds":     nil,
		"govar_worker_claims_total":          {"kind", "result"},
		"govar_worker_backlog":               {"kind", "state"},
		"govar_worker_oldest_age_seconds":    {"kind"},
		"govar_worker_heartbeat_age_seconds": {"kind"},
		"govar_calibration_support":          {"detector", "policy_profile"},
		"govar_calibration_coverage_ppb":     {"detector", "policy_profile"},
		"govar_drift_detected":               {"detector", "policy_profile"},
		"govar_conservative_mode":            {"detector", "policy_profile"},
		"govar_arithmetic_overflow_total":    {"operation"},
		"govar_bound_violation_total":        {"basis"},
		"govar_pricing_incomplete_total":     {"reason"},
		"govar_audit_verification_total":     {"result"},
		"govar_unresolved_attempt_total":     {"reason"},
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		name := family.GetName()
		want, ok := expectedLabels[name]
		if !ok {
			t.Fatalf("unexpected metric family %q", name)
		}
		seen[name] = true
		got := metricLabelNames(family)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s labels=%v, want %v", name, got, want)
		}
		for _, forbidden := range []string{"request_id", "reservation_id", "trace_id", "tenant_id", "workload_uid", "prompt", "error"} {
			if contains(got, forbidden) {
				t.Errorf("%s contains forbidden identity/unbounded label %q", name, forbidden)
			}
		}
	}
	for name := range expectedLabels {
		if !seen[name] {
			t.Errorf("required metric family %q was not gathered", name)
		}
	}

	decision, transaction, settlement := Buckets()
	assertHistogramBuckets(t, families, "govar_decision_duration_seconds", decision)
	assertHistogramBuckets(t, families, "govar_transaction_duration_seconds", transaction)
	assertHistogramBuckets(t, families, "govar_settlement_delay_seconds", settlement)

	// Callers cannot mutate the protocol through returned slices.
	decision[0] = 99
	again, _, _ := Buckets()
	if again[0] == 99 {
		t.Fatal("Buckets exposed mutable protocol state")
	}
}

func TestProfilesAndAllRuntimeLabelsAreBounded(t *testing.T) {
	badConfigs := []Config{
		{TenantProfiles: []string{"tenant/raw/id"}, PolicyProfiles: []string{"p"}},
		{TenantProfiles: []string{"t"}, PolicyProfiles: []string{"Namespace.Name"}},
		{TenantProfiles: []string{"same", "same"}, PolicyProfiles: []string{"p"}},
		{TenantProfiles: nil, PolicyProfiles: []string{"p"}},
	}
	tooMany := Config{PolicyProfiles: []string{"p"}}
	for i := 0; i <= maxTenantProfiles; i++ {
		tooMany.TenantProfiles = append(tooMany.TenantProfiles, fmt.Sprintf("t%d", i))
	}
	badConfigs = append(badConfigs, tooMany)
	for i, config := range badConfigs {
		if _, err := New(config); err == nil {
			t.Errorf("bad config %d was accepted: %+v", i, config)
		}
	}

	r := testRegistry(t)
	before := gatheredSeries(t, r)
	invalidCalls := []func() error{
		func() error { return r.RecordAdmissionDecision("tenant-123", "highest_utility_feasible", "mean") },
		func() error { return r.RecordAdmissionDecision("ADMIT", "raw provider error text", "mean") },
		func() error { return r.RecordAdmissionDecision("ADMIT", "highest_utility_feasible", "request-id") },
		func() error {
			return r.RecordCommittedTransition(CommittedTransition{From: "raw-uid", To: "FINALIZED", Reason: "settlement_finalized", Effective: true})
		},
		func() error {
			return r.ApplyCommittedTenantSnapshot(TenantMonetarySnapshot{TenantProfile: "actual-tenant-id"})
		},
		func() error {
			return r.ApplyCommittedComponentSnapshot(ComponentSnapshot{ReservationMicros: map[string]int64{"custom-bill": 1}})
		},
		func() error { return r.RecordWorkerClaim("request-id", "claimed") },
		func() error { return r.SetWorkerSnapshot("expiry", "request-id", 1, time.Second, time.Second) },
		func() error {
			return r.SetCalibrationSnapshot(CalibrationSnapshot{PolicyProfile: "policy-object-name", Detector: "coverage-gap"})
		},
		func() error { return r.RecordArithmeticOverflow("raw-operation") },
		func() error { return r.RecordBoundViolation("raw-basis") },
		func() error { return r.RecordPricingIncomplete("provider response contents") },
		func() error { return r.RecordAuditVerification("raw-error") },
		func() error { return r.RecordUnresolvedAttempt("raw-error") },
	}
	for i, call := range invalidCalls {
		if err := call(); err == nil {
			t.Errorf("invalid metric call %d succeeded", i)
		}
	}
	after := gatheredSeries(t, r)
	if before != after {
		t.Fatalf("invalid labels created metric series: before=%d after=%d", before, after)
	}

	for vocabulary, values := range AllowedLabelValues() {
		if len(values) == 0 {
			t.Errorf("%s has empty label vocabulary", vocabulary)
		}
		if !sort.StringsAreSorted(values) {
			t.Errorf("%s vocabulary is not deterministically sorted", vocabulary)
		}
		for _, value := range values {
			if strings.TrimSpace(value) != value || value == "" {
				t.Errorf("%s contains invalid value %q", vocabulary, value)
			}
		}
	}
}

func TestDuplicateNoopDoesNotIncrementEffectiveTransition(t *testing.T) {
	r := testRegistry(t)
	transition := CommittedTransition{From: "RESERVED", To: "DISPATCH_PENDING", Reason: "dispatch_claimed", Effective: true}
	if err := r.RecordCommittedTransition(transition); err != nil {
		t.Fatal(err)
	}
	transition.Effective = false // committed replay/inbox hit, no state change
	if err := r.RecordCommittedTransition(transition); err != nil {
		t.Fatal(err)
	}
	got := metricValue(t, r, "govar_transition_total", map[string]string{
		"from": "RESERVED", "to": "DISPATCH_PENDING", "reason": "dispatch_claimed",
	})
	if got != 1 {
		t.Fatalf("duplicate/no-op transition count=%v, want 1", got)
	}
}

func TestCommittedSnapshotMatchesAccountingBarrier(t *testing.T) {
	r := testRegistry(t)
	// These values stand in for one aggregate SQL query after its transaction
	// commits. Apply returns before the test opens the scrape barrier.
	sql := TenantMonetarySnapshot{
		TenantProfile: "balanced", ReservedMicros: 15_000, SettledMicros: 9_000,
		OutstandingLiabilityMicros: 6_000, CarriedDebtMicros: 700,
	}
	if err := r.ApplyCommittedTenantSnapshot(sql); err != nil {
		t.Fatal(err)
	}
	components := ComponentSnapshot{
		ReservationMicros: map[string]int64{"input_tokens": 4_000, "output_tokens": 11_000},
		SettlementMicros:  map[string]int64{"input_tokens": 3_000, "output_tokens": 6_000},
	}
	if err := r.ApplyCommittedComponentSnapshot(components); err != nil {
		t.Fatal(err)
	}

	checks := map[string]float64{
		"govar_reserved_micros":              float64(sql.ReservedMicros),
		"govar_settled_micros":               float64(sql.SettledMicros),
		"govar_outstanding_liability_micros": float64(sql.OutstandingLiabilityMicros),
		"govar_carried_debt_micros":          float64(sql.CarriedDebtMicros),
	}
	for name, want := range checks {
		if got := metricValue(t, r, name, map[string]string{"tenant_profile": "balanced"}); got != want {
			t.Errorf("%s scrape=%v, SQL aggregate=%v", name, got, want)
		}
	}
	if got := metricValue(t, r, "govar_reservation_component_micros", map[string]string{"basis": "output_tokens"}); got != 11_000 {
		t.Errorf("reservation output component=%v, want 11000", got)
	}
	if got := metricValue(t, r, "govar_settlement_component_micros", map[string]string{"basis": "cached_input_tokens"}); got != 0 {
		t.Errorf("omitted component was not reset to zero: %v", got)
	}
}

func TestRegistriesAreIndependentAndRaceSafe(t *testing.T) {
	a := testRegistry(t)
	b := testRegistry(t)
	const goroutines = 32
	var wait sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := a.RecordAdmissionDecision("ADMIT", "highest_utility_feasible", "strict_provider_cap"); err != nil {
				t.Errorf("RecordAdmissionDecision: %v", err)
			}
			if err := a.ApplyCommittedTenantSnapshot(TenantMonetarySnapshot{TenantProfile: "balanced", ReservedMicros: 1}); err != nil {
				t.Errorf("ApplyCommittedTenantSnapshot: %v", err)
			}
		}()
	}
	wait.Wait()
	labels := map[string]string{"decision": "ADMIT", "reason": "highest_utility_feasible", "method": "strict_provider_cap"}
	if got := metricValue(t, a, "govar_admission_decisions_total", labels); got != goroutines {
		t.Fatalf("concurrent counter=%v, want %d", got, goroutines)
	}
	if got := metricValueOrZero(t, b, "govar_admission_decisions_total", labels); got != 0 {
		t.Fatalf("private registries leaked state: second=%v", got)
	}
}

func TestRejectsNegativeSnapshotsAndDurationsWithoutPartialMutation(t *testing.T) {
	r := testRegistry(t)
	if err := r.ApplyCommittedTenantSnapshot(TenantMonetarySnapshot{TenantProfile: "balanced", ReservedMicros: -1}); err == nil {
		t.Error("negative money accepted")
	}
	if err := r.ApplyCommittedComponentSnapshot(ComponentSnapshot{ReservationMicros: map[string]int64{"input_tokens": 8}, SettlementMicros: map[string]int64{"output_tokens": -1}}); err == nil {
		t.Error("negative component accepted")
	}
	if got := metricValueOrZero(t, r, "govar_reservation_component_micros", map[string]string{"basis": "input_tokens"}); got != 0 {
		t.Fatalf("component validation partially mutated metrics: %v", got)
	}
	for name, observe := range map[string]func() error{
		"decision":    func() error { return r.ObserveDecisionDuration(-time.Second) },
		"transaction": func() error { return r.ObserveCommittedTransactionDuration(-time.Second) },
		"settlement":  func() error { return r.ObserveCommittedSettlementDelay(-time.Second) },
	} {
		if err := observe(); err == nil {
			t.Errorf("negative %s duration accepted", name)
		}
	}
}

func mustSeedEveryMetric(t *testing.T, r *Registry) {
	t.Helper()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(r.RecordAdmissionDecision("ADMIT", "highest_utility_feasible", "strict_provider_cap"))
	must(r.RecordCommittedTransition(CommittedTransition{From: "NEW", To: "RESERVED", Reason: "highest_utility_feasible", Effective: true}))
	must(r.ApplyCommittedTenantSnapshot(TenantMonetarySnapshot{TenantProfile: "balanced", ReservedMicros: 1, SettledMicros: 1, OutstandingLiabilityMicros: 1, CarriedDebtMicros: 1}))
	must(r.ApplyCommittedComponentSnapshot(ComponentSnapshot{ReservationMicros: map[string]int64{"input_tokens": 1}, SettlementMicros: map[string]int64{"output_tokens": 1}}))
	must(r.ObserveDecisionDuration(time.Millisecond))
	must(r.ObserveCommittedTransactionDuration(time.Millisecond))
	must(r.ObserveCommittedSettlementDelay(time.Second))
	must(r.RecordWorkerClaim("expiry", "claimed"))
	must(r.SetWorkerSnapshot("expiry", "ready", 1, time.Second, time.Second))
	must(r.SetCalibrationSnapshot(CalibrationSnapshot{PolicyProfile: "strict", Detector: "coverage-gap", Support: 1, CoveragePPB: 999_000_000}))
	must(r.RecordArithmeticOverflow("reservation"))
	must(r.RecordBoundViolation("output_tokens"))
	must(r.RecordPricingIncomplete("partial"))
	must(r.RecordAuditVerification("pass"))
	must(r.RecordUnresolvedAttempt("telemetry_missing"))
}

func gatheredSeries(t *testing.T, r *Registry) int {
	t.Helper()
	families, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, family := range families {
		total += len(family.Metric)
	}
	return total
}

func metricLabelNames(family *dto.MetricFamily) []string {
	set := map[string]struct{}{}
	for _, metric := range family.Metric {
		for _, pair := range metric.Label {
			set[pair.GetName()] = struct{}{}
		}
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	return names
}

func assertHistogramBuckets(t *testing.T, families []*dto.MetricFamily, name string, want []float64) {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		if len(family.Metric) != 1 || family.Metric[0].Histogram == nil {
			t.Fatalf("%s is not one histogram", name)
		}
		got := make([]float64, 0, len(family.Metric[0].Histogram.Bucket))
		for _, bucket := range family.Metric[0].Histogram.Bucket {
			got = append(got, bucket.GetUpperBound())
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s buckets=%v, want frozen %v", name, got, want)
		}
		return
	}
	t.Fatalf("histogram %s not gathered", name)
}

func metricValue(t *testing.T, r *Registry, name string, labels map[string]string) float64 {
	t.Helper()
	found, value := findMetricValue(t, r, name, labels)
	if !found {
		t.Fatalf("metric %s%v not found", name, labels)
	}
	return value
}

func metricValueOrZero(t *testing.T, r *Registry, name string, labels map[string]string) float64 {
	t.Helper()
	found, value := findMetricValue(t, r, name, labels)
	if !found {
		return 0
	}
	return value
}

func findMetricValue(t *testing.T, r *Registry, name string, labels map[string]string) (bool, float64) {
	t.Helper()
	families, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if labelsMatch(metric.Label, labels) {
				switch {
				case metric.Gauge != nil:
					return true, metric.Gauge.GetValue()
				case metric.Counter != nil:
					return true, metric.Counter.GetValue()
				}
			}
		}
	}
	return false, 0
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, pair := range pairs {
		if want[pair.GetName()] != pair.GetValue() {
			return false
		}
	}
	return true
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
