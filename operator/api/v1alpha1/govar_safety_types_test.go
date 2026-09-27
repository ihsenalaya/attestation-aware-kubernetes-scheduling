package v1alpha1

import (
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestAIWorkloadBindingSpecValidate(t *testing.T) {
	valid := AIWorkloadBindingSpec{
		ServiceAccountName: "payments-api",
		TenantID:           "tenant-a",
		Team:               "payments",
		Application:        "payments-api",
		BudgetPolicyRef:    "payments-budget",
		RoutingPolicyRef:   "payments-routing",
		Sensitivity:        TierHigh,
		AllowedZones:       []string{"eu", "francecentral"},
		RequireGateway:     true,
	}
	if errs := valid.Validate("payments-api", field.NewPath("spec")); len(errs) != 0 {
		t.Fatalf("valid binding returned errors: %v", errs)
	}

	invalid := valid
	invalid.TenantID = " "
	invalid.AllowedZones = []string{"EU", "eu", " francecentral"}
	errs := invalid.Validate("other-service-account", field.NewPath("spec"))
	joined := errs.ToAggregate().Error()
	for _, want := range []string{"tenantID", "must equal metadata.name", "lower-case", "Duplicate value"} {
		if !strings.Contains(joined, want) {
			t.Errorf("errors %q do not contain %q", joined, want)
		}
	}
}

func TestGOVARRouteApprovalScopeDigestBindsPolicyModelProviderRouteAndExpiry(t *testing.T) {
	scope := GOVARRouteApprovalScope{
		RoutingPolicy:       AIWorkloadBindingResolvedReference{Name: "routing", UID: "routing-uid", Generation: 3},
		Model:               AIWorkloadBindingResolvedReference{Name: "model", UID: "model-uid", Generation: 4},
		Provider:            AIWorkloadBindingResolvedReference{Name: "provider", UID: "provider-uid", Generation: 5},
		RouteSnapshotDigest: strings.Repeat("a", 64),
		ValidUntil:          metav1.NewTime(time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)),
	}
	scope.ScopeDigest = scope.ComputeDigest()
	if len(scope.ScopeDigest) != 64 || scope.ScopeDigest != scope.ComputeDigest() {
		t.Fatalf("unstable digest %q", scope.ScopeDigest)
	}
	mutations := []func(*GOVARRouteApprovalScope){
		func(v *GOVARRouteApprovalScope) { v.RoutingPolicy.Generation++ },
		func(v *GOVARRouteApprovalScope) { v.RoutingPolicy.UID = "other-routing" },
		func(v *GOVARRouteApprovalScope) { v.Model.Generation++ },
		func(v *GOVARRouteApprovalScope) { v.Model.UID = "other-model" },
		func(v *GOVARRouteApprovalScope) { v.Provider.Generation++ },
		func(v *GOVARRouteApprovalScope) { v.Provider.UID = "other-provider" },
		func(v *GOVARRouteApprovalScope) { v.RouteSnapshotDigest = strings.Repeat("b", 64) },
		func(v *GOVARRouteApprovalScope) { v.ValidUntil = metav1.NewTime(v.ValidUntil.Add(time.Second)) },
	}
	for i, mutate := range mutations {
		changed := scope
		mutate(&changed)
		if changed.ComputeDigest() == scope.ScopeDigest {
			t.Fatalf("scope mutation %d did not change digest", i)
		}
	}
}

func TestGOVARApprovalDecisionBindsDistinctActorsObjectScopeAndAdmission(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	scope := GOVARRouteApprovalScope{
		RoutingPolicy:       AIWorkloadBindingResolvedReference{Name: "routing", UID: "routing-uid", Generation: 3},
		Model:               AIWorkloadBindingResolvedReference{Name: "model", UID: "model-uid", Generation: 4},
		Provider:            AIWorkloadBindingResolvedReference{Name: "provider", UID: "provider-uid", Generation: 5},
		RouteSnapshotDigest: strings.Repeat("a", 64), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
	}
	scope.ScopeDigest = scope.ComputeDigest()
	change := &AIChangeRequest{ObjectMeta: metav1.ObjectMeta{Name: "approval", Namespace: "finance", UID: "object-uid"},
		Spec: AIChangeRequestSpec{Action: AIChangeRequestActionAuthorizeGOVARRoute, Approval: AIChangeRequestApprovalApproved,
			GOVARRouteApproval: &scope, RequestedBy: "alice", Reason: "exact route"}}
	change.Spec.ProposalDigest = change.Spec.ComputeProposalDigest(change.Namespace, change.Name)
	change.Spec.GOVARDecision = &GOVARRouteApprovalDecision{
		Outcome: AIChangeRequestApprovalApproved, ReviewerIdentity: "bob", ReviewerGroup: GOVARApprovalReviewerGroup,
		DecidedAt: metav1.NewTime(now), ValidUntil: scope.ValidUntil, RequestUID: string(change.UID), AdmissionUID: "admission-uid",
		ProposalDigest: change.Spec.ProposalDigest, ScopeDigest: scope.ScopeDigest,
	}
	change.Spec.GOVARDecision.DecisionDigest = change.Spec.GOVARDecision.ComputeDigest(change.Namespace, change.Name, change.Spec.RequestedBy)
	if err := change.ValidateGOVARDecision(now); err != nil {
		t.Fatalf("valid decision rejected: %v", err)
	}
	mutations := []func(*AIChangeRequest){
		func(v *AIChangeRequest) { v.Spec.RequestedBy = "bob" },
		func(v *AIChangeRequest) { v.Spec.GOVARDecision.ReviewerIdentity = "alice" },
		func(v *AIChangeRequest) { v.Spec.GOVARDecision.ReviewerGroup = "system:masters" },
		func(v *AIChangeRequest) { v.Spec.GOVARDecision.RequestUID = "copied-object" },
		func(v *AIChangeRequest) { v.Spec.GOVARDecision.AdmissionUID = "" },
		func(v *AIChangeRequest) { v.Spec.GOVARDecision.ScopeDigest = strings.Repeat("b", 64) },
		func(v *AIChangeRequest) { v.Spec.GOVARDecision.DecisionDigest = strings.Repeat("0", 64) },
	}
	for i, mutate := range mutations {
		changed := change.DeepCopy()
		mutate(changed)
		if err := changed.ValidateGOVARDecision(now); err == nil {
			t.Fatalf("decision mutation %d was accepted", i)
		}
	}
}

func TestRequestLevelApprovalCRDsAreRemoved(t *testing.T) {
	for _, path := range []string{
		"../../config/crd/bases/aiops.imperium.io_aiadmissionapprovals.yaml",
		"../../config/crd/bases/aiops.imperium.io_aiadmissionapprovaldecisions.yaml",
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("obsolete per-request CRD still exists at %s: err=%v", path, err)
		}
	}
	change, err := os.ReadFile("../../config/crd/bases/aiops.imperium.io_aichangerequests.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"authorize-gov-ar-route", "govarRouteApproval:", "govarDecision:", "requestedBy:", "proposalDigest:", "reviewerIdentity:", "approvedScopeDigest:", "approvedDecisionDigest:", "routeSnapshotDigest:", "scopeDigest:", "validUntil:"} {
		if !strings.Contains(string(change), required) {
			t.Fatalf("AIChangeRequest CRD lacks %q", required)
		}
	}
}

func TestGOVARRoutingPolicySpecValidate(t *testing.T) {
	adaptive := int64(1200)
	valid := GOVARRoutingPolicySpec{
		Reservation: GOVARReservationPolicy{
			Method:                       GOVARReservationFixedCohort,
			AdaptiveQuantileOutputTokens: &adaptive,
		},
		Calibration: &GOVARCalibrationPolicy{
			ArtifactRef: "calibration-v1", ArtifactSHA256: strings.Repeat("c", 64),
			CalibrationDataRef: "calibration", CalibrationInputSHA256: strings.Repeat("d", 64), MonitoringDataRef: "monitoring",
			Version: "v1", FeatureSchemaVersion: "features-v1", PriceRegimeSHA256: strings.Repeat("e", 64),
			CapRegimeSHA256: strings.Repeat("f", 64), ProducerSoftwareSHA256: strings.Repeat("1", 64), CoverageTargetPPB: 990_000_000,
			MinimumSupport: 100, MaxAgeSeconds: 3600,
		},
		Drift: GOVARDriftPolicy{
			Detector:                   "coverage-gap",
			ThresholdPPB:               10_000_000,
			Fallback:                   "strict_provider_cap",
			RevalidationMinimumSupport: 100,
		},
		Cohort: &GOVARCohortPolicy{
			RegistryRef:        "cohort-a",
			Size:               10,
			OpportunitySetHash: strings.Repeat("a", 64),
			WeightsHash:        strings.Repeat("b", 64),
		},
		Risk: &GOVARRiskPolicy{TenantRiskPPB: 1_000_000, Allocation: "uniform"},
	}
	if errs := valid.Validate(field.NewPath("spec", "govar")); len(errs) != 0 {
		t.Fatalf("valid GOV-AR policy returned errors: %v", errs)
	}

	invalid := valid
	invalid.Reservation.AdaptiveQuantileOutputTokens = nil
	invalid.Calibration = nil
	invalid.Cohort = nil
	invalid.Risk = nil
	errs := invalid.Validate(field.NewPath("spec", "govar"))
	if len(errs) != 3 {
		t.Fatalf("invalid GOV-AR policy errors = %d, want calibration/cohort/risk: %v", len(errs), errs)
	}
	joined := errs.ToAggregate().Error()
	for _, want := range []string{"calibration", "cohort", "risk"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("conditional errors %q lack %q", joined, want)
		}
	}
}
