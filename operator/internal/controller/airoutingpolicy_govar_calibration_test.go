package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
)

func calibrationFixture(t *testing.T, now time.Time) (*AIRoutingPolicyReconciler, *aiopsv1alpha1.AIRoutingPolicy, *corev1.ConfigMap, *corev1.ConfigMap) {
	t.Helper()
	price, capDigest, software := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	makeRows := func(split string, start time.Time, values []int64) []govarcalibration.Observation {
		out := make([]govarcalibration.Observation, len(values))
		for i, value := range values {
			out[i] = govarcalibration.Observation{RequestID: split + "-" + string(rune('a'+i)), ProviderAttemptID: split + "-attempt-" + string(rune('a'+i)),
				Split: split, Selected: true, AuthoritativeFinal: true, OutputTokens: value,
				FeatureSchemaVersion: "features-v1", PriceRegimeSHA256: price, CapRegimeSHA256: capDigest,
				SettledAt: start.Add(time.Duration(i) * time.Minute)}
		}
		return out
	}
	calRows := makeRows(govarcalibration.SplitCalibration, now.Add(-40*time.Minute), []int64{100, 200, 300, 400, 500})
	config := govarcalibration.BuildConfig{ArtifactRef: "artifact-v1", Version: "v1", FeatureSchemaVersion: "features-v1",
		PriceRegimeSHA256: price, CapRegimeSHA256: capDigest, ProducerSoftwareSHA256: software, CoverageTargetPPB: 800_000_000}
	artifact, err := govarcalibration.BuildArtifact(config, calRows)
	if err != nil {
		t.Fatal(err)
	}
	monitorRows := makeRows(govarcalibration.SplitMonitoring, now.Add(-20*time.Minute), []int64{100, 200, 300, 400, 400})
	calJSON, _ := json.Marshal(calRows)
	monitorJSON, _ := json.Marshal(monitorRows)
	immutable := true
	calCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "calibration", Namespace: "finance"}, Immutable: &immutable, Data: map[string]string{calibrationDataKey: string(calJSON)}}
	monitorCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "monitoring", Namespace: "finance"}, Immutable: &immutable, Data: map[string]string{monitoringDataKey: string(monitorJSON)}}
	policy := &aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "routing", Namespace: "finance", Generation: 3},
		Spec: aiopsv1alpha1.AIRoutingPolicySpec{GOVAR: &aiopsv1alpha1.GOVARRoutingPolicySpec{
			Reservation: aiopsv1alpha1.GOVARReservationPolicy{Method: aiopsv1alpha1.GOVARReservationAdaptiveQuantile},
			Calibration: &aiopsv1alpha1.GOVARCalibrationPolicy{ArtifactRef: config.ArtifactRef, ArtifactSHA256: artifact.ArtifactSHA256,
				CalibrationDataRef: calCM.Name, CalibrationInputSHA256: artifact.SourceObservationsSHA256, MonitoringDataRef: monitorCM.Name,
				Version: config.Version, FeatureSchemaVersion: config.FeatureSchemaVersion, PriceRegimeSHA256: price,
				CapRegimeSHA256: capDigest, ProducerSoftwareSHA256: software, CoverageTargetPPB: config.CoverageTargetPPB,
				MinimumSupport: 5, MaxAgeSeconds: 3600},
			Drift: aiopsv1alpha1.GOVARDriftPolicy{Detector: "coverage-gap", ThresholdPPB: 50_000_000, Fallback: "strict_provider_cap", RevalidationMinimumSupport: 5}}}}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = aiopsv1alpha1.AddToScheme(scheme)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(calCM, monitorCM, policy).Build()
	return &AIRoutingPolicyReconciler{Client: client, Scheme: scheme, Now: func() time.Time { return now }, allowDiagnosticConfigMapEvidence: true}, policy, calCM, monitorCM
}

func TestProductionControllerRejectsSelfDeclaredConfigMapEvidence(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	r, policy, _, _ := calibrationFixture(t, now)
	r.allowDiagnosticConfigMapEvidence = false
	status := r.reconcileGOVAREvidence(context.Background(), policy)
	if status == nil || status.Drift == nil || !status.Drift.ConservativeMode ||
		status.Drift.Reason != "CalibrationSourceNotLedgerAuthoritative" ||
		status.Calibration == nil || status.Calibration.Valid {
		t.Fatalf("untrusted ConfigMap evidence escaped conservative mode: %+v", status)
	}
}

func TestCalibrationProducerValidStaleDriftAndRevalidation(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	r, policy, _, _ := calibrationFixture(t, now)
	status := r.reconcileGOVAREvidence(context.Background(), policy)
	if status == nil || status.Calibration == nil || !status.Calibration.Valid || status.Drift.ConservativeMode ||
		status.Calibration.AdaptiveOutputTokens != 500 || status.Calibration.CalibrationMethod != "split_conformal_order_statistic_v1" ||
		status.Calibration.ExchangeableMarginalCoverageLowerPPB != 833_333_333 || status.Calibration.ConformalRank != 5 {
		t.Fatalf("valid status=%+v", status)
	}
	r.Now = func() time.Time { return now.Add(2 * time.Hour) }
	stale := r.reconcileGOVAREvidence(context.Background(), policy)
	if !stale.Drift.ConservativeMode || stale.Drift.Reason != "CalibrationStale" {
		t.Fatalf("stale status=%+v", stale)
	}

	r, policy, _, monitorCM := calibrationFixture(t, now)
	var monitoring []govarcalibration.Observation
	_ = json.Unmarshal([]byte(monitorCM.Data[monitoringDataKey]), &monitoring)
	for i := range monitoring {
		monitoring[i].OutputTokens = 10_000
	}
	raw, _ := json.Marshal(monitoring)
	monitorCM.Data[monitoringDataKey] = string(raw)
	if err := r.Update(context.Background(), monitorCM); err != nil {
		t.Fatal(err)
	}
	drifted := r.reconcileGOVAREvidence(context.Background(), policy)
	if !drifted.Drift.Detected || !drifted.Drift.ConservativeMode || drifted.Calibration.Valid {
		t.Fatalf("drift status=%+v", drifted)
	}
	// A fresh independent monitoring replacement with enough nominal support
	// deterministically revalidates; an old detected status is not authoritative.
	for i := range monitoring {
		monitoring[i].OutputTokens = 100
	}
	raw, _ = json.Marshal(monitoring)
	monitorCM.Data[monitoringDataKey] = string(raw)
	if err := r.Update(context.Background(), monitorCM); err != nil {
		t.Fatal(err)
	}
	revalidated := r.reconcileGOVAREvidence(context.Background(), policy)
	if revalidated.Drift.Detected || revalidated.Drift.ConservativeMode || !revalidated.Calibration.Valid {
		t.Fatalf("revalidated status=%+v", revalidated)
	}
}

func TestCalibrationInputMustBeImmutableAndDigestBoundAcrossReplacement(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	r, policy, calCM, _ := calibrationFixture(t, now)
	mutable := false
	calCM.Immutable = &mutable
	if err := r.Update(context.Background(), calCM); err != nil {
		t.Fatal(err)
	}
	status := r.reconcileGOVAREvidence(context.Background(), policy)
	if !status.Drift.ConservativeMode || status.Drift.Reason != "CalibrationInputUnavailable" {
		t.Fatalf("mutable source status=%+v", status)
	}
	// Simulate an API replacement carrying different rows under the same name.
	// Even if a test client permits this impossible immutable update, the frozen
	// input and artifact digests prevent publication of a valid replacement.
	immutable := true
	calCM.Immutable = &immutable
	var observations []govarcalibration.Observation
	_ = json.Unmarshal([]byte(calCM.Data[calibrationDataKey]), &observations)
	observations[0].OutputTokens++
	raw, _ := json.Marshal(observations)
	calCM.Data[calibrationDataKey] = string(raw)
	if err := r.Update(context.Background(), calCM); err != nil {
		t.Fatal(err)
	}
	replaced := r.reconcileGOVAREvidence(context.Background(), policy)
	if !replaced.Drift.ConservativeMode || replaced.Drift.Reason != "CalibrationDigestMismatch" {
		t.Fatalf("replacement source status=%+v", replaced)
	}
}

func TestCalibrationConfigMapMapperEnqueuesOnlyReferencedPolicies(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	r, _, calCM, _ := calibrationFixture(t, now)
	requests := r.policiesForCalibrationConfigMap(context.Background(), calCM)
	if len(requests) != 1 || requests[0].Name != "routing" || requests[0].Namespace != "finance" {
		t.Fatalf("mapped requests=%+v", requests)
	}
	unrelated := calCM.DeepCopy()
	unrelated.Name = "unrelated"
	if got := r.policiesForCalibrationConfigMap(context.Background(), unrelated); len(got) != 0 {
		t.Fatalf("unrelated ConfigMap mapped=%+v", got)
	}
}
