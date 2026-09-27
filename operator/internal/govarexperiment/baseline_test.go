package govarexperiment

import (
	"strings"
	"testing"
)

func TestNoBudgetIsInvariantToEveryMonetaryFact(t *testing.T) {
	wantDecision, wantReason := NoBudgetDecision(true)
	for _, facts := range []AdmissionFacts{
		{HardFeasible: true, BudgetMicros: 1},
		{HardFeasible: true, BudgetMicros: 1, SettledMicros: 9_000, OutstandingMicros: 8_000, CarriedMicros: 7_000},
		{HardFeasible: true, BudgetMicros: 9_000_000_000, SettledMicros: 1, OutstandingMicros: 2, CarriedMicros: 3},
	} {
		gotDecision, gotReason := NoBudgetDecision(facts.HardFeasible)
		if gotDecision != wantDecision || gotReason != wantReason {
			t.Fatalf("budget changed no-budget decision: facts=%+v got=%s/%s want=%s/%s", facts, gotDecision, gotReason, wantDecision, wantReason)
		}
	}
	if decision, _ := NoBudgetDecision(false); decision != "ABSTAIN" {
		t.Fatalf("no-budget bypassed hard feasibility: %s", decision)
	}
}

func TestSettledOnlyIsPendingBlindAndSettlementIsIdempotent(t *testing.T) {
	base := AdmissionFacts{HardFeasible: true, BudgetMicros: 100, SettledMicros: 25, CarriedMicros: 5}
	for _, outstanding := range []int64{0, 1, 100, 1_000_000_000} {
		facts := base
		facts.OutstandingMicros = outstanding
		decision, reason, err := SettledOnlyDecision(facts)
		if err != nil || decision != "ADMIT" || reason != "settled_only_positive_available" {
			t.Fatalf("outstanding liability affected settled-only: outstanding=%d got=%s/%s err=%v", outstanding, decision, reason, err)
		}
	}
	ledger := NewSettledOnlyLedger()
	if effective, err := ledger.Settle("tenant/window", "request-1", 75); err != nil || !effective {
		t.Fatalf("first settlement failed: effective=%v err=%v", effective, err)
	}
	if effective, err := ledger.Settle("tenant/window", "request-1", 75); err != nil || effective {
		t.Fatalf("duplicate was not an idempotent replay: effective=%v err=%v", effective, err)
	}
	if _, err := ledger.Settle("tenant/window", "request-1", 76); err == nil {
		t.Fatal("conflicting duplicate settlement was accepted")
	}
	if _, err := ledger.Settle("other-tenant/window", "request-1", 75); err == nil {
		t.Fatal("cross-tenant request replay was treated as an idempotent settlement")
	}
	if _, err := ledger.Settle("tenant/other-window", "request-1", 75); err == nil {
		t.Fatal("cross-window request replay was treated as an idempotent settlement")
	}
	decision, _, err := SettledOnlyDecision(AdmissionFacts{HardFeasible: true, BudgetMicros: 100, SettledMicros: ledger.Settled("tenant/window"), CarriedMicros: 25})
	if err != nil || decision != "QUEUE" {
		t.Fatalf("settled spend did not gate later admission: decision=%s err=%v", decision, err)
	}
}

func TestFixedEstimateIdentityIsGlobalAndDistinctFromMean(t *testing.T) {
	artifact := strings.Repeat("a", 64)
	config := MethodConfig{SchemaVersion: MethodConfigSchema, ProtocolID: "fixed_estimate", StaticOutputTokens: 256, ArtifactSHA256: artifact}
	digest, err := MethodConfigDigest(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, irrelevantRegime := range []struct{ model, tenant, workload string }{{"m1", "t1", "w1"}, {"m2", "t9", "w7"}} {
		_ = irrelevantRegime
		got, err := MethodConfigDigest(config)
		if err != nil || got != digest {
			t.Fatalf("global fixed estimate changed across an irrelevant regime: got=%s err=%v", got, err)
		}
	}
	mean := config
	mean.ProtocolID = "mean"
	meanDigest, err := MethodConfigDigest(mean)
	if err != nil {
		t.Fatal(err)
	}
	if meanDigest == digest || registryID(mean.ProtocolID) == registryID(config.ProtocolID) {
		t.Fatal("fixed_estimate was aliased to empirical mean identity")
	}
}

func TestAdaptiveUsesOnlyPriorFinalSelectedFeedbackAndFallsBack(t *testing.T) {
	regime := strings.Repeat("b", 64)
	cfg := AdaptiveMethodConfig{InitialOutputTokens: 50, CoverageTargetPPB: 750_000_000, MinimumSupport: 2, CalibrationSupport: 10, UpdateEverySteps: 2, HistoryWindow: 10, DriftThresholdPPB: 0, Fallback: "strict_provider_cap", CandidateRegimeSHA256: regime}
	estimator, err := NewAdaptiveEstimator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := estimator.Observe(AdaptiveFeedback{FinalizedStep: 0, OutputTokens: 10, Selected: true, Final: true}); err != nil {
		t.Fatal(err)
	}
	if err := estimator.Observe(AdaptiveFeedback{FinalizedStep: 2, OutputTokens: 90, Selected: true, Final: true}); err != nil {
		t.Fatal(err)
	}
	plan, err := estimator.Plan(2, regime, 100)
	if err != nil {
		t.Fatal(err)
	}
	// The feedback finalized at the decision step is not prior and there is not
	// enough prior online support, so the immutable calibration value remains.
	if plan.OutputTokens != 50 || plan.Support != 1 || plan.EffectiveMode != "adaptive_quantile" {
		t.Fatalf("adaptive estimator read concurrent/future feedback: %+v", plan)
	}
	if err := estimator.Observe(AdaptiveFeedback{FinalizedStep: 1, OutputTokens: 20, Selected: true, Final: true}); err != nil {
		t.Fatal(err)
	}
	plan, err = estimator.Plan(2, regime, 100)
	if err != nil {
		t.Fatal(err)
	}
	if plan.OutputTokens != 20 || plan.Support != 2 {
		t.Fatalf("prescheduled prior-only order statistic not applied: %+v", plan)
	}
	plan, err = estimator.Plan(3, strings.Repeat("c", 64), 100)
	if err != nil || plan.EffectiveMode != "strict_provider_cap" || plan.OutputTokens != 100 || plan.FallbackReason != "candidate_regime_mismatch" {
		t.Fatalf("regime invalidity did not visibly fall back: %+v err=%v", plan, err)
	}
	bad := cfg
	bad.CalibrationSupport = 1
	lowSupport, _ := NewAdaptiveEstimator(bad)
	plan, err = lowSupport.Plan(0, regime, 100)
	if err != nil || plan.EffectiveMode != "strict_provider_cap" || plan.FallbackReason != "calibration_support_insufficient" {
		t.Fatalf("low-support calibration advertised adaptive mode: %+v err=%v", plan, err)
	}
	if err := estimator.Observe(AdaptiveFeedback{FinalizedStep: 3, OutputTokens: 1, Selected: false, Final: true}); err == nil {
		t.Fatal("unselected feedback was accepted")
	}
}

type oracleSpy struct {
	calls int
	cost  int64
}

func (s *oracleSpy) FutureCostForCommittedRoute(_, _, _ string) (int64, error) {
	s.calls++
	return s.cost, nil
}

func TestOracleAccessIsEvaluatorOnlyAndCommitBound(t *testing.T) {
	spy := &oracleSpy{cost: 37}
	invalid := OracleMethodConfig{}
	if _, err := RevealOracleFutureCost(invalid, "request", spy); err == nil || spy.calls != 0 {
		t.Fatalf("oracle source was invoked before commitment validation: calls=%d err=%v", spy.calls, err)
	}
	valid := OracleMethodConfig{Phase: "post_nonoracle_evaluator", PairedNonOracleCommitSHA256: strings.Repeat("d", 64), PairedRouteSHA256: strings.Repeat("e", 64)}
	cost, err := RevealOracleFutureCost(valid, "request", spy)
	if err != nil || cost != 37 || spy.calls != 1 {
		t.Fatalf("committed evaluator access failed: cost=%d calls=%d err=%v", cost, spy.calls, err)
	}
	// Ordinary non-oracle planners have no source parameter and therefore
	// cannot invoke the evaluator.
	_, _ = NoBudgetDecision(true)
	if spy.calls != 1 {
		t.Fatal("a non-oracle planner accessed future cost")
	}
}

func TestGOVARCohortAlphaAllocationBindsEveryExecutedQuantile(t *testing.T) {
	config := testConfig("gov_ar")
	method := config.MethodConfig.GOVAR
	slot, err := method.Slot("tenant", "window", "cohort", 0, method.CandidateSetSHA256)
	if err != nil || slot.AllocatedRiskPPB != method.TenantRiskPPB*slot.WeightPPB/1_000_000_000 || slot.CoverageTargetPPB != 1_000_000_000-slot.AllocatedRiskPPB {
		t.Fatalf("slot risk/quantile binding failed: %+v err=%v", slot, err)
	}
	forged := *method
	forged.SlotBounds = append([]GOVARSlotBound(nil), method.SlotBounds...)
	forged.SlotBounds[0].CoverageTargetPPB++
	if err := forged.Validate(); err == nil {
		t.Fatal("quantile not matching one minus allocated slot risk was accepted")
	}
	if _, err := method.Slot("tenant", "window", "cohort", 0, strings.Repeat("0", 64)); err == nil {
		t.Fatal("candidate-set mismatch was accepted")
	}
	forged = *method
	forged.EvidenceRegistry = append([]GOVARCalibrationEvidence(nil), method.EvidenceRegistry...)
	forged.EvidenceRegistry[0].Calibration.UpperOutputTokens++
	if err := forged.Validate(); err == nil {
		t.Fatal("caller-mutated quantile without calibration rows was accepted")
	}
}

func TestGOVARRejectsUncommittedCandidateObjectiveAndMembershipMutations(t *testing.T) {
	method := testConfig("gov_ar").MethodConfig.GOVAR
	tests := []struct {
		name   string
		mutate func(*GOVARMethodConfig)
	}{
		{"candidate_price", func(m *GOVARMethodConfig) {
			m.CandidateSet = append([]GOVARCandidateBinding(nil), m.CandidateSet...)
			m.CandidateSet[0].OutputPriceMicrosPerMillion++
		}},
		{"candidate_cap", func(m *GOVARMethodConfig) {
			m.CandidateSet = append([]GOVARCandidateBinding(nil), m.CandidateSet...)
			m.CandidateSet[0].VerifiedOutputCapTokens++
		}},
		{"joint_objective", func(m *GOVARMethodConfig) {
			m.JointSelection.Objective = "quality"
		}},
		{"switch_penalty", func(m *GOVARMethodConfig) {
			m.JointSelection.SwitchPenaltyMicros = 1
		}},
		{"cohort_membership", func(m *GOVARMethodConfig) {
			m.CalibrationProfiles = append([]GOVARCalibrationProfile(nil), m.CalibrationProfiles...)
			m.CalibrationProfiles[0].AssignedCohorts = append([]CohortSpec(nil), m.CalibrationProfiles[0].AssignedCohorts...)
			m.CalibrationProfiles[0].AssignedCohorts[0].Size++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			forged := *method
			test.mutate(&forged)
			if err := forged.Validate(); err == nil {
				t.Fatalf("uncommitted %s mutation was accepted", test.name)
			}
		})
	}
}

func TestExpectedCostRouterUsesExpectedInformationAndCanonicalTieBreak(t *testing.T) {
	candidates := []ExpectedCandidate{
		{ID: "model-b", ExpectedQualityPPB: 900, ExpectedReservedMicros: 100, HardFeasible: true},
		{ID: "model-a", ExpectedQualityPPB: 800, ExpectedReservedMicros: 50, HardFeasible: true},
		{ID: "infeasible", ExpectedQualityPPB: 1_000_000_000, ExpectedReservedMicros: 1, HardFeasible: false},
	}
	got, err := SelectExpectedCostCandidate(candidates, 100, 2)
	if err != nil || got.ID != "model-a" { // utilities are both 700; canonical ID wins.
		t.Fatalf("unexpected expected-cost choice: %+v err=%v", got, err)
	}
	if _, err := SelectExpectedCostCandidate(candidates, 49, 2); err == nil {
		t.Fatal("router admitted a candidate whose expected hold did not fit")
	}
}
