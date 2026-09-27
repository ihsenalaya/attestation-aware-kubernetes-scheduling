package govar

import (
	"testing"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestExecutableReservationPolicies(t *testing.T) {
	req := admitRequest("policy", testTenant, testWorkload)
	candidate := defaultCandidates()[0]
	at := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		method     aiopsv1alpha1.GOVARReservationMethod
		value      int64
		margin     int64
		want       MoneyMicros
		wantMethod string
	}{
		{aiopsv1alpha1.GOVARReservationMean, 500, 0, 1_200, "mean"},
		{aiopsv1alpha1.GOVARReservationFixedMargin, 500, 250, 1_600, "fixed_margin"},
		{aiopsv1alpha1.GOVARReservationFixedQuantile, 1000, 0, 2_000, "fixed_quantile"},
		{aiopsv1alpha1.GOVARReservationAdaptiveQuantile, 1200, 0, 2_320, "adaptive_quantile"},
		{aiopsv1alpha1.GOVARReservationFixedCohort, 1200, 0, 2_320, "govar_fixed_cohort"},
	}
	for _, tt := range tests {
		routing := typedEstimateRouting(tt.method, tt.value, tt.margin)
		if tt.method == aiopsv1alpha1.GOVARReservationAdaptiveQuantile || tt.method == aiopsv1alpha1.GOVARReservationFixedCohort {
			routing = typedAdaptiveRouting(tt.method, tt.value, at, candidate)
		}
		policyReq := req
		if tt.method == aiopsv1alpha1.GOVARReservationFixedCohort {
			policyReq.CohortID, policyReq.CohortIndex = "fixed-cohort-a", 0
		}
		choice, reason, err := chooseAdmission(policyReq, routing, []Candidate{candidate}, 1_000_000, at)
		if err != nil || reason != "" || choice.Reservation != tt.want || choice.Method != tt.wantMethod {
			t.Fatalf("%s choice=%+v reason=%s err=%v", tt.method, choice, reason, err)
		}
	}
}

func TestDriftFallsBackToVerifiedStrictBound(t *testing.T) {
	req := admitRequest("drift", testTenant, testWorkload)
	at := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	routing := typedAdaptiveRouting(aiopsv1alpha1.GOVARReservationAdaptiveQuantile, 500, at, defaultCandidates()[0])
	routing.Status.GOVAR.Drift.Detected = true
	routing.Status.GOVAR.Drift.ConservativeMode = true
	choice, reason, err := chooseAdmission(req, routing, defaultCandidates(), 1_000_000, at)
	if err != nil || reason != "" || choice.Method != "strict_provider_cap" || choice.Reservation != 3_600 || choice.FallbackReason != ReasonCalibrationDrift {
		t.Fatalf("fallback choice=%+v reason=%s err=%v", choice, reason, err)
	}
}

func TestTypedCalibrationStatusMatrixFailsClosed(t *testing.T) {
	at := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	candidate := defaultCandidates()[0]
	mutations := map[string]struct {
		mutate func(*aiopsv1alpha1.AIRoutingPolicy)
		want   ReasonCode
	}{
		"missing":    {func(v *aiopsv1alpha1.AIRoutingPolicy) { v.Status.GOVAR = nil }, ReasonCalibrationMissing},
		"generation": {func(v *aiopsv1alpha1.AIRoutingPolicy) { v.Status.ObservedGeneration-- }, ReasonCalibrationGeneration},
		"artifact-hash": {func(v *aiopsv1alpha1.AIRoutingPolicy) {
			v.Status.GOVAR.Calibration.ArtifactSHA256 = eventPayloadHash("wrong")
		}, ReasonCalibrationMismatch},
		"support": {func(v *aiopsv1alpha1.AIRoutingPolicy) { v.Status.GOVAR.Calibration.Support = 1 }, ReasonCalibrationSupport},
		"future": {func(v *aiopsv1alpha1.AIRoutingPolicy) {
			v.Status.GOVAR.Calibration.CalibrationWindowEnd = metav1.NewTime(at.Add(time.Minute))
		}, ReasonCalibrationStale},
		"stale": {func(v *aiopsv1alpha1.AIRoutingPolicy) {
			v.Status.GOVAR.Calibration.CalibrationWindowEnd = metav1.NewTime(at.Add(-2 * time.Hour))
		}, ReasonCalibrationStale},
		"drift": {func(v *aiopsv1alpha1.AIRoutingPolicy) { v.Status.GOVAR.Drift.Detected = true }, ReasonCalibrationDrift},
		"unsupported": {func(v *aiopsv1alpha1.AIRoutingPolicy) {
			v.Spec.GOVAR.Drift.Detector, v.Status.GOVAR.Drift.Detector = "ks", "ks"
		}, ReasonCalibrationUnsupported},
	}
	for name, tt := range mutations {
		t.Run(name, func(t *testing.T) {
			routing := typedAdaptiveRouting(aiopsv1alpha1.GOVARReservationAdaptiveQuantile, 500, at, candidate)
			routing.Spec.GOVAR.Drift.Fallback = "abstain"
			tt.mutate(&routing)
			_, reason, err := chooseAdmission(admitRequest("status-"+name, testTenant, testWorkload), routing, []Candidate{candidate}, 1_000_000, at)
			if err != nil || reason != tt.want {
				t.Fatalf("reason=%s err=%v want=%s", reason, err, tt.want)
			}
		})
	}
}

func TestConfiguredCalibrationFallbackDecisionAndLegacyAnnotationsIgnored(t *testing.T) {
	at := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	candidate := defaultCandidates()[0]
	for fallback, want := range map[aiopsv1alpha1.GOVARConservativeFallback]Decision{
		"queue": DecisionQueue, "reject": DecisionReject, "abstain": DecisionAbstain, "require_approval": DecisionRequireApproval,
	} {
		routing := typedAdaptiveRouting(aiopsv1alpha1.GOVARReservationAdaptiveQuantile, 500, at, candidate)
		routing.Spec.GOVAR.Drift.Fallback = fallback
		routing.Status.GOVAR = nil
		if got := decisionForAdmissionFailure(routing, ReasonCalibrationMissing); got != want {
			t.Fatalf("fallback %s decision=%s want=%s", fallback, got, want)
		}
	}
	routing := typedEstimateRouting(aiopsv1alpha1.GOVARReservationMean, 500, 0)
	routing.Annotations = map[string]string{AnnotationReservationMethod: "fixed_quantile", AnnotationQuantileTokens: "1900", AnnotationCalibrationDrift: "true"}
	choice, reason, err := chooseAdmission(admitRequest("legacy-ignored", testTenant, testWorkload), routing, []Candidate{candidate}, 1_000_000, at)
	if err != nil || reason != "" || choice.Method != "mean" || choice.Reservation != 1_200 {
		t.Fatalf("legacy annotation controlled reservation: choice=%+v reason=%s err=%v", choice, reason, err)
	}
}

func TestJointSelectionConsidersAvailabilityBeforeQuality(t *testing.T) {
	req := admitRequest("joint", testTenant, testWorkload)
	routing := defaultRouting()
	routing.Spec.Objective = "quality"
	expensive := defaultCandidates()[0]
	expensive.ModelRef, expensive.QualityScore, expensive.OutputPriceMicrosPerMillion = "premium", 0.99, 10_000_000
	for i := range expensive.PricingSnapshot.Charges {
		if expensive.PricingSnapshot.Charges[i].Basis == "output_tokens" {
			expensive.PricingSnapshot.Charges[i].PriceMicrosPerUnit = 10_000_000
		}
	}
	expensive.PricingSnapshot.SnapshotSHA256 = govarpricing.SnapshotDigest(expensive.PricingSnapshot)
	cheap := defaultCandidates()[0]
	cheap.ModelRef, cheap.QualityScore = "economy", 0.8
	choice, reason, err := chooseAdmission(req, routing, []Candidate{expensive, cheap}, 4_000, time.Now().UTC())
	if err != nil || reason != "" || choice.Candidate.ModelRef != "economy" {
		t.Fatalf("joint choice=%+v reason=%s err=%v", choice, reason, err)
	}
}

func TestStrictRoughInputUsesContextWindowBound(t *testing.T) {
	req := admitRequest("rough", testTenant, testWorkload)
	req.InputTokensExact = false
	candidate := defaultCandidates()[0]
	candidate.ContextWindow = 10_000
	choice, reason, err := chooseAdmission(req, defaultRouting(), []Candidate{candidate}, 1_000_000, time.Now().UTC())
	if err != nil || reason != "" || choice.Method != "strict_context_window_bound" || choice.InputTokensBound != 8_000 {
		t.Fatalf("rough strict choice=%+v reason=%s err=%v", choice, reason, err)
	}
}

func TestRoughInputForcesNonStrictPolicyToStrictContextBound(t *testing.T) {
	req := admitRequest("rough-mean", testTenant, testWorkload)
	req.InputTokensExact = false
	routing := typedEstimateRouting(aiopsv1alpha1.GOVARReservationMean, 10, 0)
	candidate := defaultCandidates()[0]
	candidate.ContextWindow = 10_000
	choice, reason, err := chooseAdmission(req, routing, []Candidate{candidate}, 1_000_000, time.Now().UTC())
	if err != nil || reason != "" || choice.Method != "strict_context_window_bound" || choice.InputTokensBound != 8_000 {
		t.Fatalf("rough mean choice=%+v reason=%s err=%v", choice, reason, err)
	}
}

func TestLatencyObjectiveFailsClosedWithoutObservation(t *testing.T) {
	routing := defaultRouting()
	routing.Spec.Objective = "latency"
	_, reason, err := chooseAdmission(admitRequest("latency", testTenant, testWorkload), routing, defaultCandidates(), 1_000_000, time.Now().UTC())
	if err != nil || reason != ReasonLatencyUnavailable {
		t.Fatalf("latency reason=%s err=%v", reason, err)
	}
}

func TestHardFeasibilityReasonsArePropagated(t *testing.T) {
	candidate := defaultCandidates()[0]
	candidate.Feasible = false
	candidate.InfeasibleReason = ReasonPricingStale
	_, reason, err := chooseAdmission(admitRequest("stale-price", testTenant, testWorkload), defaultRouting(), []Candidate{candidate}, 1_000_000, time.Now().UTC())
	if err != nil || reason != ReasonPricingStale {
		t.Fatalf("reason=%s err=%v", reason, err)
	}
}
