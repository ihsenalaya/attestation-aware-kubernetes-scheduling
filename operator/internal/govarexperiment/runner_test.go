package govarexperiment

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
)

type usage struct {
	input, output int64
	score         float64
}

type frozenTestIssuer struct{ *DevelopmentFixtureIssuer }

func (f frozenTestIssuer) EvidenceTier() string { return "frozen_authorized" }

func testConfig(comparator string) Config {
	h := strings.Repeat("a", 64)
	catalogID := strings.Repeat("9", 64)
	modelMap := map[string]string{"experiment-model": catalogID}
	modelMapSHA, _ := SelectedFeedbackModelMapDigest(modelMap)
	c := Config{SchemaVersion: ConfigSchema, RecordType: "experiment_config", ExperimentID: "E1", RunID: "run-001", CellID: "cell-a", Scenario: "delay", Seed: 17, RegistryID: registryID(comparator), ProtocolMethodID: comparator, ComparatorMethod: comparator, ClusterID: "local", TrialID: "trial-1", VirtualStart: time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), EvidenceTier: "development_debug", SoftwareSHA256: h, DataSHA256: strings.Repeat("b", 64), ProtocolSHA256: strings.Repeat("c", 64), SplitSHA256: strings.Repeat("d", 64), SelectedFeedbackDatasetID: "routereval_math_outcomes", SelectedFeedbackSplit: "development", SelectedFeedbackModelMapSHA256: modelMapSHA, SelectedFeedbackModelIDs: modelMap, InputPriceMicrosPerMillion: 1_000_000, OutputPriceMicrosPerMillion: 1_000_000, VerifiedOutputCapTokens: 100}
	c.SourceSHA256 = SourceRoot(c.SoftwareSHA256, c.DataSHA256, c.ProtocolSHA256, c.SplitSHA256)
	c.MethodConfig = MethodConfig{SchemaVersion: MethodConfigSchema, ProtocolID: comparator}
	switch comparator {
	case "no_budget":
		c.ProductionReservationMode = "observational"
	case "settled_only":
		c.ProductionReservationMode = "settled_only"
	case "strict_max":
		c.ProductionReservationMode = "strict_provider_cap"
	case "mean":
		c.ProductionReservationMode = "mean"
		c.EstimateOutputTokens = 10
		c.MethodConfig.StaticOutputTokens = 10
		c.MethodConfig.ArtifactSHA256 = strings.Repeat("1", 64)
	case "mean_margin":
		c.ProductionReservationMode = "fixed_margin"
		c.EstimateOutputTokens = 5
		c.MarginOutputTokens = 5
		c.MethodConfig.StaticOutputTokens = 5
		c.MethodConfig.MeanMarginOutputTokens = 5
		c.MethodConfig.ArtifactSHA256 = strings.Repeat("2", 64)
	case "fixed_quantile":
		c.ProductionReservationMode = "fixed_quantile"
		c.EstimateOutputTokens = 10
		c.MethodConfig.StaticOutputTokens = 10
		c.MethodConfig.ArtifactSHA256 = strings.Repeat("3", 64)
	case "fixed_estimate":
		c.ProductionReservationMode = "mean"
		c.EstimateOutputTokens = 10
		c.MethodConfig.StaticOutputTokens = 10
		c.MethodConfig.ArtifactSHA256 = strings.Repeat("4", 64)
	case "expected_cost_router":
		c.ProductionReservationMode = "mean"
		c.EstimateOutputTokens = 10
		c.MethodConfig.StaticOutputTokens = 10
		c.MethodConfig.ExpectedCost = &ExpectedCostMethodConfig{UtilityCostPenaltyPPBPerMicro: 1, PredictorArtifactSHA256: strings.Repeat("5", 64)}
	case "adaptive_quantile":
		c.ProductionReservationMode = "adaptive_quantile"
		c.EstimateOutputTokens = 10
		c.MethodConfig.ArtifactSHA256 = strings.Repeat("6", 64)
		c.MethodConfig.Adaptive = &AdaptiveMethodConfig{InitialOutputTokens: 10, CoverageTargetPPB: 950_000_000, MinimumSupport: 19, CalibrationSupport: 19, UpdateEverySteps: 2, HistoryWindow: 25, DriftThresholdPPB: 10_000_000, Fallback: "strict_provider_cap", CandidateRegimeSHA256: strings.Repeat("7", 64)}
	case "oracle_future_cost":
		c.ProductionReservationMode = "mean"
		c.MethodConfig.Oracle = &OracleMethodConfig{Phase: "post_nonoracle_evaluator", PairedNonOracleCommitSHA256: strings.Repeat("8", 64), PairedRouteSHA256: strings.Repeat("a", 64)}
	case "gov_ar":
		c.ProductionReservationMode = "govar_fixed_cohort"
		op := opportunity(0, "request-gov_ar", "tenant", "window")
		op.CohortID = "cohort"
		c.Cohorts = []CohortSpec{{TenantID: "tenant", BudgetWindowID: "window", CohortID: "cohort", Size: 1}}
		built, err := BuildProspectiveGOVARMethodConfig(c, []Opportunity{op}, 100_000_000, 100_000_000, 7200, 1,
			time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC), testProspectiveEvidence(c.Cohorts))
		if err != nil {
			panic(err)
		}
		c.MethodConfig = built
	}
	c.MethodConfigSHA256, _ = MethodConfigDigest(c.MethodConfig)
	return c
}

func testProspectiveEvidence(cohorts []CohortSpec) GOVARProspectiveEvidence {
	base := time.Date(2026, 7, 12, 22, 0, 0, 0, time.UTC)
	profile := GOVARProspectiveProfileFixture{ProfileID: "nominal", FeatureSchemaVersion: "p1-features-v1",
		TaskClass: "public-math", TailClass: "nominal", AssignmentRule: "declared-profile-v1", Cohorts: cohorts,
		ArtifactVersion: "p1-test-v1", MinimumSupport: 10}
	for index := 0; index < 10; index++ {
		profile.Calibration = append(profile.Calibration, GOVARProspectiveSample{RequestID: fmt.Sprintf("cal-%02d", index),
			ProviderAttemptID: fmt.Sprintf("cal-attempt-%02d", index), OpportunityID: fmt.Sprintf("cal-op-%02d", index),
			OutputTokens: int64(index + 1), SettledAt: base.Add(time.Duration(index) * time.Minute)})
	}
	for index := 0; index < 5; index++ {
		profile.Monitoring = append(profile.Monitoring, GOVARProspectiveSample{RequestID: fmt.Sprintf("mon-%02d", index),
			ProviderAttemptID: fmt.Sprintf("mon-attempt-%02d", index), OpportunityID: fmt.Sprintf("mon-op-%02d", index),
			OutputTokens: int64(index + 1), SettledAt: base.Add(30*time.Minute + time.Duration(index)*time.Minute)})
	}
	return GOVARProspectiveEvidence{Profiles: []GOVARProspectiveProfileFixture{profile}}
}

func TestNonReservingComparatorsExecuteDistinctFaithfulLedgerSemantics(t *testing.T) {
	first := opportunity(0, "r1", "tenant", "window")
	second := opportunity(1, "r2", "tenant", "window")
	first.BudgetMicros, second.BudgetMicros = 10, 10
	first.SettlementDelaySteps = 0
	first.DuplicateSettlement = true
	first.ConflictingSettlementReplay = true
	values := map[string]usage{"r1": {1, 20, .5}, "r2": {1, 20, .5}}
	noBudget := runOne(t, testConfig("no_budget"), []Opportunity{first, second}, values)
	if noBudget.DecisionCounts["ADMIT"] != 2 || noBudget.Metrics.TenantBudgetWindowOvershoot.Numerator != 1 {
		t.Fatalf("no-budget did not remain budget invariant or expose overshoot: decisions=%v metrics=%+v", noBudget.DecisionCounts, noBudget.Metrics)
	}
	settledOnly := runOne(t, testConfig("settled_only"), []Opportunity{first, second}, values)
	if settledOnly.DecisionCounts["ADMIT"] != 1 || settledOnly.DecisionCounts["QUEUE"] != 1 {
		t.Fatalf("settled-only did not gate only after effective settlement: %v", settledOnly.DecisionCounts)
	}
	if settledOnly.LifecycleCounts["settlement_duplicate"] != 1 || settledOnly.LifecycleCounts["settlement_conflict_rejected"] != 1 {
		t.Fatalf("settled-only rows were not driven by real idempotent/conflict ledger outcomes: %v", settledOnly.LifecycleCounts)
	}
	for _, row := range settledOnly.Records {
		if row.OutstandingMicros != 0 || row.ActiveReservations != 0 || row.ReservedMicros != 0 {
			t.Fatalf("settled-only created a pre-dispatch monetary hold: %+v", row)
		}
	}
}

func TestSettledOnlyDelayedLiabilityIsVisibleInRawRecomputedMetrics(t *testing.T) {
	first := opportunity(0, "latent-r1", "tenant", "window")
	second := opportunity(1, "latent-r2", "tenant", "window")
	first.BudgetMicros, second.BudgetMicros = 30, 30
	first.SettlementDelaySteps, second.SettlementDelaySteps = 10, 10
	values := map[string]usage{
		first.RequestID:  {1, 20, .5},
		second.RequestID: {1, 20, .5},
	}
	run := runOne(t, testConfig("settled_only"), []Opportunity{first, second}, values)
	if run.DecisionCounts["ADMIT"] != 2 {
		t.Fatalf("settled-only observed an unfinalized liability: %v", run.DecisionCounts)
	}
	if run.Metrics.PeakActualOutstandingLiabilityMicros != 42 ||
		run.Metrics.TotalWindowPeakActualOutstandingMicros != 42 ||
		run.Metrics.PeakLatentFinancialExposureMicros != 42 ||
		run.Metrics.TotalWindowPeakLatentExposureMicros != 42 ||
		run.Metrics.PeakLatentExposureRatioPPB != 1_400_000_000 {
		t.Fatalf("delayed latent exposure did not recompute from opportunity snapshots: %+v", run.Metrics)
	}
	if run.Metrics.TenantBudgetWindowOvershoot != (ExactRate{Numerator: 1, Denominator: 1, RatePPB: 1_000_000_000}) ||
		run.Metrics.TenantWindowOvershootMagnitudeMicros != 12 {
		t.Fatalf("tenant-window overshoot did not include the delayed admitted liabilities: %+v", run.Metrics)
	}
	for _, row := range run.Records {
		if row.RecordType == RecordSnapshot && row.StreamSequence == second.Sequence && row.ActiveActualMicros != 42 {
			t.Fatalf("second opportunity snapshot omitted latent actual liability: %+v", row)
		}
	}
}

func TestStaticEstimateAndExpectedCostUseDistinctImmutableIdentities(t *testing.T) {
	op := opportunity(0, "r", "tenant", "window")
	op.BudgetMicros = 100
	values := map[string]usage{"r": {1, 20, .5}}
	staticRun := runOne(t, testConfig("fixed_estimate"), []Opportunity{op}, values)
	expectedRun := runOne(t, testConfig("expected_cost_router"), []Opportunity{op}, values)
	for name, run := range map[string]RunResult{"fixed_estimate": staticRun, "expected_cost_router": expectedRun} {
		if run.DecisionCounts["ADMIT"] != 1 || len(run.Records) == 0 {
			t.Fatalf("%s did not execute: %+v", name, run.DecisionCounts)
		}
		for _, row := range run.Records {
			if row.RegistryID != registryID(name) || row.ProtocolMethodID != name || row.MethodConfigSHA256 != run.Config.MethodConfigSHA256 {
				t.Fatalf("%s raw identity was aliased or unbound: %+v", name, row)
			}
		}
	}
	if staticRun.Config.MethodConfigSHA256 == expectedRun.Config.MethodConfigSHA256 {
		t.Fatal("fixed estimate and expected-cost comparator share a method identity")
	}
}

func TestAllElevenProtocolIdentitiesValidateWithoutAliases(t *testing.T) {
	methods := []string{"no_budget", "settled_only", "mean", "mean_margin", "fixed_quantile", "strict_max", "fixed_estimate", "adaptive_quantile", "expected_cost_router", "oracle_future_cost", "gov_ar"}
	seen := map[string]string{}
	for _, method := range methods {
		config := testConfig(method)
		if err := config.Validate(); err != nil {
			t.Fatalf("%s config rejected: %v", method, err)
		}
		if prior, duplicate := seen[config.MethodConfigSHA256]; duplicate {
			t.Fatalf("method %s aliases method %s at method_config_sha256", method, prior)
		}
		seen[config.MethodConfigSHA256] = method
	}
}

func TestGOVARProductionCohortAdapterAndVisibleStaleFallback(t *testing.T) {
	config := testConfig("gov_ar")
	op := opportunity(0, "request-gov_ar", "tenant", "window")
	op.CohortID = "cohort"
	op.BudgetMicros = 1_000
	run := runOne(t, config, []Opportunity{op}, map[string]usage{op.RequestID: {1, 5, .75}})
	if run.DecisionCounts["ADMIT"] != 1 {
		t.Fatalf("GOV-AR did not admit through production core: %+v", run.DecisionCounts)
	}
	admission := run.Records[0]
	if admission.EffectiveMethod != "govar_fixed_cohort" || admission.AllocatedRiskPPB != 100_000_000 ||
		admission.CalibrationState != "calibrated" || admission.CalibrationArtifactSHA256 == "" ||
		admission.CohortRegistrySHA256 == "" || admission.ReservedMicros != 11 {
		t.Fatalf("calibrated production binding is incomplete: %+v", admission)
	}

	stale := config
	method := *config.MethodConfig.GOVAR
	method.MaxAgeSeconds = 1
	stale.MethodConfig.GOVAR = &method
	stale.MethodConfigSHA256, _ = MethodConfigDigest(stale.MethodConfig)
	staleRun := runOne(t, stale, []Opportunity{op}, map[string]usage{op.RequestID: {1, 5, .75}})
	staleAdmission := staleRun.Records[0]
	if staleAdmission.EffectiveMethod != "strict_provider_cap" || staleAdmission.AllocatedRiskPPB != 0 ||
		staleAdmission.CalibrationArtifactSHA256 != "" || staleAdmission.CalibrationState != "conservative_fallback" ||
		staleAdmission.ConservativeFallbackReason != "calibration_stale" || staleAdmission.ReservedMicros != 101 {
		t.Fatalf("stale evidence did not visibly fail into strict mode: %+v", staleAdmission)
	}

	driftFixture := testProspectiveEvidence(config.Cohorts)
	for index := range driftFixture.Profiles[0].Monitoring {
		driftFixture.Profiles[0].Monitoring[index].OutputTokens = 100
	}
	driftMethod, err := BuildProspectiveGOVARMethodConfig(config, []Opportunity{op}, 100_000_000, 100_000_000, 7200, 1,
		time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC), driftFixture)
	if err != nil {
		t.Fatal(err)
	}
	drifted := config
	drifted.MethodConfig = driftMethod
	drifted.MethodConfigSHA256, _ = MethodConfigDigest(drifted.MethodConfig)
	driftRun := runOne(t, drifted, []Opportunity{op}, map[string]usage{op.RequestID: {1, 5, .75}})
	driftAdmission := driftRun.Records[0]
	if driftAdmission.EffectiveMethod != "strict_provider_cap" || driftAdmission.AllocatedRiskPPB != 0 ||
		driftAdmission.CalibrationArtifactSHA256 != "" || driftAdmission.CalibrationState != "conservative_fallback" ||
		driftAdmission.ConservativeFallbackReason != "calibration_drift_detected" || driftAdmission.ReservedMicros != 101 {
		t.Fatalf("detected drift did not visibly fail into strict mode: %+v", driftAdmission)
	}
}

func TestProspectiveGOVARBuilderIsOutcomeFreeAndFailsClosed(t *testing.T) {
	config := testConfig("mean")
	config.ComparatorMethod, config.ProtocolMethodID, config.RegistryID = "gov_ar", "gov_ar", registryID("gov_ar")
	config.ProductionReservationMode = "govar_fixed_cohort"
	op := opportunity(0, "prospective-run", "tenant", "window")
	op.CohortID = "cohort"
	config.Cohorts = []CohortSpec{{TenantID: "tenant", BudgetWindowID: "window", CohortID: "cohort", Size: 1}}
	fixture := testProspectiveEvidence(config.Cohorts)
	build := func(c Config, f GOVARProspectiveEvidence) (MethodConfig, error) {
		return BuildProspectiveGOVARMethodConfig(c, []Opportunity{op}, 100_000_000, 100_000_000, 7200, 1,
			time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC), f)
	}
	first, err := build(config, fixture)
	if err != nil {
		t.Fatal(err)
	}
	changedOutcomeRoot := config
	changedOutcomeRoot.DataSHA256 = strings.Repeat("e", 64)
	changedOutcomeRoot.SourceSHA256 = SourceRoot(changedOutcomeRoot.SoftwareSHA256, changedOutcomeRoot.DataSHA256, changedOutcomeRoot.ProtocolSHA256, changedOutcomeRoot.SplitSHA256)
	second, err := build(changedOutcomeRoot, fixture)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("future selected-feedback data root influenced candidate/profile/quantile binding: equal=%v err=%v", reflect.DeepEqual(first, second), err)
	}
	overCap := fixture
	overCap.Profiles = append([]GOVARProspectiveProfileFixture(nil), fixture.Profiles...)
	overCap.Profiles[0].Calibration = append([]GOVARProspectiveSample(nil), fixture.Profiles[0].Calibration...)
	overCap.Profiles[0].Calibration[0].OutputTokens = 101
	if _, err := build(config, overCap); err == nil {
		t.Fatal("calibration row exceeding the claimed cap regime was accepted")
	}
	crossSplit := fixture
	crossSplit.Profiles = append([]GOVARProspectiveProfileFixture(nil), fixture.Profiles...)
	crossSplit.Profiles[0].Monitoring = append([]GOVARProspectiveSample(nil), fixture.Profiles[0].Monitoring...)
	crossSplit.Profiles[0].Monitoring[0].RequestID = fixture.Profiles[0].Calibration[0].RequestID
	crossSplit.Profiles[0].Monitoring[0].ProviderAttemptID = fixture.Profiles[0].Calibration[0].ProviderAttemptID
	if _, err := build(config, crossSplit); err == nil {
		t.Fatal("calibration identity reused in monitoring was accepted")
	}
	runReuse := fixture
	runReuse.Profiles = append([]GOVARProspectiveProfileFixture(nil), fixture.Profiles...)
	runReuse.Profiles[0].Calibration = append([]GOVARProspectiveSample(nil), fixture.Profiles[0].Calibration...)
	runReuse.Profiles[0].Calibration[0].RequestID = op.RequestID
	if _, err := build(config, runReuse); err == nil {
		t.Fatal("run request identity reused in calibration was accepted")
	}
	mutated := *first.GOVAR
	mutated.CalibrationProfiles = append([]GOVARCalibrationProfile(nil), first.GOVAR.CalibrationProfiles...)
	mutated.CalibrationProfiles[0].TailClass = "heavy"
	if err := mutated.Validate(); err == nil {
		t.Fatal("calibration profile mutation without digest update was accepted")
	}
}

func TestRecanonicalizedGOVARArtifactsCannotCrossProductionConfigOrStream(t *testing.T) {
	base := testConfig("gov_ar")
	op := opportunity(0, "request-gov_ar", "tenant", "window")
	op.CohortID = "cohort"
	build := func(c Config, candidate Opportunity, evidence GOVARProspectiveEvidence) MethodConfig {
		t.Helper()
		method, err := BuildProspectiveGOVARMethodConfig(c, []Opportunity{candidate}, 100_000_000, 100_000_000, 7200, 1,
			time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC), evidence)
		if err != nil {
			t.Fatal(err)
		}
		return method
	}
	attach := func(c Config, method MethodConfig) Config {
		c.MethodConfig = method
		c.MethodConfigSHA256, _ = MethodConfigDigest(method)
		return c
	}

	for name, mutate := range map[string]func(*Config){
		"price": func(c *Config) { c.OutputPriceMicrosPerMillion++ },
		"cap":   func(c *Config) { c.VerifiedOutputCapTokens++ },
	} {
		t.Run(name, func(t *testing.T) {
			producerConfig := base
			mutate(&producerConfig)
			// The prospective builder re-canonicalizes candidate, price/cap
			// regimes, calibration/drift artifacts, publication, joint binding,
			// slots, and the outer method digest for the mutated source config.
			method := build(producerConfig, op, testProspectiveEvidence(producerConfig.Cohorts))
			forged := attach(base, method)
			if err := forged.Validate(); err == nil {
				t.Fatalf("fully re-canonicalized %s artifact crossed its production config binding", name)
			}
		})
	}

	changedRequest := op
	changedRequest.RequestID = "different-request"
	changedRequest.FeedbackItemID = SHA256([]byte("prepared-item-" + changedRequest.RequestID))
	requestMethod := build(base, changedRequest, testProspectiveEvidence(base.Cohorts))
	requestForged := attach(base, requestMethod)
	if err := requestForged.Validate(); err != nil {
		t.Fatalf("internally canonical request-mutated artifact was rejected before stream binding: %v", err)
	}
	if err := prepareGOVAREngine(govar.NewEngine(), requestForged, []Opportunity{op}); err == nil {
		t.Fatal("fully re-canonicalized request artifact crossed the exact production stream binding")
	}

	membershipConfig := base
	membershipConfig.Cohorts = []CohortSpec{{TenantID: "other-tenant", BudgetWindowID: "window", CohortID: "cohort", Size: 1}}
	membershipOp := op
	membershipOp.TenantID = "other-tenant"
	membershipMethod := build(membershipConfig, membershipOp, testProspectiveEvidence(membershipConfig.Cohorts))
	membershipForged := attach(base, membershipMethod)
	if err := membershipForged.Validate(); err == nil {
		t.Fatal("fully re-canonicalized cohort-membership artifact crossed its production config registry")
	}

	wrongSizeConfig := base
	wrongSizeConfig.Cohorts = []CohortSpec{{TenantID: "tenant", BudgetWindowID: "window", CohortID: "cohort", Size: 2}}
	secondSlot := op
	secondSlot.Sequence = 1
	secondSlot.RequestID = "request-gov-ar-slot-2"
	secondSlot.FeedbackItemID = SHA256([]byte("prepared-item-" + secondSlot.RequestID))
	secondSlot.CohortIndex = 1
	wrongSizeMethod, err := BuildProspectiveGOVARMethodConfig(wrongSizeConfig, []Opportunity{op, secondSlot}, 200_000_000, 100_000_000, 7200, 1,
		time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC), testProspectiveEvidence(wrongSizeConfig.Cohorts))
	if err != nil {
		t.Fatal(err)
	}
	wrongSizeForged := attach(base, wrongSizeMethod)
	if err := wrongSizeForged.Validate(); err == nil || !strings.Contains(err.Error(), "assigned cohort") || !strings.Contains(err.Error(), "size 2") {
		t.Fatalf("fully re-canonicalized wrong assigned-cohort size was not rejected by the exact profile/config comparison: %v", err)
	}

	objective := base.MethodConfig.GOVAR.JointSelection
	objective.Objective = "quality"
	if _, err := GOVARJointSelectionDigest(objective); err == nil {
		t.Fatal("an out-of-contract objective could be re-canonicalized by the fixed P1 producer")
	}
	objective = base.MethodConfig.GOVAR.JointSelection
	objective.SwitchPenaltyMicros = 1
	if _, err := GOVARJointSelectionDigest(objective); err == nil {
		t.Fatal("an out-of-contract switch penalty could be re-canonicalized by the fixed P1 producer")
	}
}

func TestUnintegratedAuthorityBoundMethodsFailClosedInsteadOfAliasing(t *testing.T) {
	for _, method := range []string{"adaptive_quantile", "oracle_future_cost"} {
		config := testConfig(method)
		op := opportunity(0, "request-"+method, "tenant", "window")
		configRaw := marshalConfig(t, config)
		_, err := RunWithIssuer(configRaw, marshalStream(t, op), issuerFor(t, config, configRaw, map[string]usage{op.RequestID: {1, 1, .5}}))
		if err == nil || !strings.Contains(err.Error(), method) {
			t.Fatalf("%s did not fail closed with its own identity: %v", method, err)
		}
	}
}
func opportunity(sequence int64, id, tenant, window string) Opportunity {
	return Opportunity{SchemaVersion: OpportunitySchema, RecordType: "opportunity", Sequence: sequence, RequestID: id, TenantID: tenant, BudgetWindowID: window, WorkloadUID: "workload-1", BudgetMicros: 100, InputTokens: 1, MaxOutputTokens: 100, SettlementDelaySteps: 0, FaultMode: FaultNominal, FeedbackItemID: SHA256([]byte("prepared-item-" + id))}
}
func marshalConfig(t *testing.T, c Config) []byte {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}
func marshalStream(t *testing.T, ops ...Opportunity) []byte {
	t.Helper()
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	for _, op := range ops {
		if err := e.Encode(op); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}
func issuerFor(t *testing.T, c Config, configRaw []byte, values map[string]usage) *DevelopmentFixtureIssuer {
	t.Helper()
	return issuerForRaw(t, c, configRaw, marshalDevelopmentOutcomes(t, c, values))
}
func marshalDevelopmentOutcomes(t *testing.T, c Config, values map[string]usage) []byte {
	t.Helper()
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	ids := make([]string, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		v := values[id]
		row := FixtureOutcome{SchemaVersion: "govar-development-outcome-v1", RecordType: "development_outcome", RequestID: id, FeedbackItemID: SHA256([]byte("prepared-item-" + id)), SelectedModelID: c.SelectedFeedbackModelIDs["experiment-model"], ActualInputTokens: v.input, ActualOutputTokens: v.output, SelectedScore: v.score}
		if err := e.Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}
func issuerForRaw(t *testing.T, c Config, configRaw, feedbackRaw []byte) *DevelopmentFixtureIssuer {
	t.Helper()
	evidence := FeedbackEvidence{RunID: c.RunID, DatasetID: c.SelectedFeedbackDatasetID, Split: c.SelectedFeedbackSplit, ProtocolSHA256: c.ProtocolSHA256, FeedbackArtifactSHA256: SHA256(feedbackRaw), SoftwareSHA256: c.SoftwareSHA256, ConfigSHA256: SHA256(configRaw), AuthoritySHA256: strings.Repeat("f", 64), ModelMapSHA256: c.SelectedFeedbackModelMapSHA256}
	issuer, err := ParseDevelopmentFixtureOutcomes(feedbackRaw, evidence)
	if err != nil {
		t.Fatal(err)
	}
	return issuer
}
func runOne(t *testing.T, c Config, ops []Opportunity, values map[string]usage) RunResult {
	t.Helper()
	cr := marshalConfig(t, c)
	run, err := RunWithIssuer(cr, marshalStream(t, ops...), issuerFor(t, c, cr, values))
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestProductionLifecycleAndFixedOpportunitySnapshot(t *testing.T) {
	c := testConfig("mean")
	op := opportunity(0, "request-1", "tenant-1", "window-1")
	op.SettlementDelaySteps = 5
	op.DuplicateSettlement = true
	op.ConflictingSettlementReplay = true
	run := runOne(t, c, []Opportunity{op}, map[string]usage{op.RequestID: {1, 20, .75}})
	if len(run.Records) != 7 {
		t.Fatalf("records=%d", len(run.Records))
	}
	if run.Metrics.ActiveLiabilitySnapshots != 1 || run.Metrics.InstantaneousActiveLiabilityExceedance != (ExactRate{1, 1, 1_000_000_000}) {
		t.Fatalf("instant=%+v", run.Metrics)
	}
	if run.Metrics.RequestUnderReservation != (ExactRate{1, 1, 1_000_000_000}) {
		t.Fatalf("under=%+v", run.Metrics.RequestUnderReservation)
	}
	if run.Metrics.BudgetUtilizationPPB != 210_000_000 || run.Metrics.PeakExposureRatioPPB != 210_000_000 {
		t.Fatalf("util=%d peak=%d", run.Metrics.BudgetUtilizationPPB, run.Metrics.PeakExposureRatioPPB)
	}
	if run.LifecycleCounts["settlement_duplicate"] != 1 || run.LifecycleCounts["settlement_conflict_rejected"] != 1 {
		t.Fatal(run.LifecycleCounts)
	}
}

func TestPreparedCanonicalItemAndCatalogModelIDsAreNotRehashed(t *testing.T) {
	c := testConfig("mean")
	op := opportunity(0, "prepared-regression", "tenant", "window")
	catalogID := c.SelectedFeedbackModelIDs["experiment-model"]
	run := runOne(t, c, []Opportunity{op}, map[string]usage{op.RequestID: {1, 2, .5}})
	var checked bool
	for _, row := range run.Records {
		if !row.Selected {
			continue
		}
		checked = true
		if row.FeedbackItemSHA256 != op.FeedbackItemID {
			t.Fatalf("prepared item identity was rehashed: got=%s want=%s", row.FeedbackItemSHA256, op.FeedbackItemID)
		}
		if row.FeedbackSelectedModelSHA256 != catalogID {
			t.Fatalf("catalog model identity was derived from deployment or double-hashed: got=%s want=%s", row.FeedbackSelectedModelSHA256, catalogID)
		}
		if row.FeedbackModelMapSHA256 != c.SelectedFeedbackModelMapSHA256 {
			t.Fatalf("raw outcome omitted immutable deployment-to-catalog map binding: got=%s", row.FeedbackModelMapSHA256)
		}
	}
	if !checked {
		t.Fatal("no selected lifecycle row was checked")
	}
	forged := c
	forged.SelectedFeedbackModelMapSHA256 = strings.Repeat("0", 64)
	if err := forged.Validate(); err == nil {
		t.Fatal("forged deployment-to-catalog model map digest was accepted")
	}
}

func TestOpportunitySnapshotDelaySensitivityAndZeroActiveRule(t *testing.T) {
	c := testConfig("mean")
	var delayed, immediate []Opportunity
	vals := map[string]usage{}
	for n := int64(0); n < 3; n++ {
		id := "r" + string(rune('a'+n))
		a := opportunity(n, id, "tenant", "window")
		a.BudgetMicros = 1000
		a.SettlementDelaySteps = 10
		delayed = append(delayed, a)
		b := a
		b.SettlementDelaySteps = 0
		immediate = append(immediate, b)
		vals[id] = usage{1, 20, .5}
	}
	slow := runOne(t, c, delayed, vals)
	fast := runOne(t, c, immediate, vals)
	if slow.Metrics.ActiveLiabilitySnapshots != 3 || slow.Metrics.InstantaneousActiveLiabilityExceedance.Numerator != 3 {
		t.Fatalf("slow=%+v", slow.Metrics.InstantaneousActiveLiabilityExceedance)
	}
	if fast.Metrics.ActiveLiabilitySnapshots != 3 || fast.Metrics.InstantaneousActiveLiabilityExceedance.Numerator != 0 {
		t.Fatalf("fast=%+v", fast.Metrics.InstantaneousActiveLiabilityExceedance)
	}
}

func TestNominalBoundsAndTypedFaults(t *testing.T) {
	c := testConfig("strict_max")
	op := opportunity(0, "r", "t", "w")
	op.BudgetMicros = 1000
	cr := marshalConfig(t, c)
	sr := marshalStream(t, op)
	if _, err := RunWithIssuer(cr, sr, issuerFor(t, c, cr, map[string]usage{"r": {2, 20, .5}})); err == nil {
		t.Fatal("known input mismatch accepted in nominal cell")
	}
	if _, err := RunWithIssuer(cr, sr, issuerFor(t, c, cr, map[string]usage{"r": {1, 101, .5}})); err == nil {
		t.Fatal("provider cap violation accepted in nominal cell")
	}
	op.FaultMode = FaultProviderCapViolation
	if _, err := RunWithIssuer(cr, marshalStream(t, op), issuerFor(t, c, cr, map[string]usage{"r": {1, 101, .5}})); err != nil {
		t.Fatal(err)
	}
	op.FaultMode = FaultKnownInputMismatch
	if _, err := RunWithIssuer(cr, marshalStream(t, op), issuerFor(t, c, cr, map[string]usage{"r": {2, 20, .5}})); err != nil {
		t.Fatal(err)
	}
}

func TestCohortRegistryIsTenantWindowScopedAndComplete(t *testing.T) {
	c := testConfig("mean")
	a := opportunity(0, "a", "tenant-a", "window")
	b := opportunity(1, "b", "tenant-b", "window")
	a.CohortID = "cohort"
	b.CohortID = "cohort"
	c.Cohorts = []CohortSpec{{"tenant-a", "window", "cohort", 1}, {"tenant-b", "window", "cohort", 1}}
	if _, err := ParseStream(marshalStream(t, a, b), c); err != nil {
		t.Fatal(err)
	}
	c.Cohorts[0].Size = 2
	if _, err := ParseStream(marshalStream(t, a, b), c); err == nil {
		t.Fatal("incomplete cohort accepted")
	}
}

func TestArtifactsPublishAndVerifyExactInputs(t *testing.T) {
	c := testConfig("mean")
	op := opportunity(0, "r", "t", "w")
	cr, sr := marshalConfig(t, c), marshalStream(t, op)
	feedbackRaw := marshalDevelopmentOutcomes(t, c, map[string]usage{"r": {1, 20, .5}})
	issuer := issuerForRaw(t, c, cr, feedbackRaw)
	result, err := WriteArtifacts(t.TempDir(), cr, sr, feedbackRaw, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(result.ConfigPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(result.StreamPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(result.FeedbackPath); err != nil {
		t.Fatal(err)
	}
	accessRaw, err := os.ReadFile(result.FeedbackAccessPath)
	if err != nil {
		t.Fatal(err)
	}
	if result.Manifest.EvidenceLabel != "development_non_citable" || result.Manifest.FeedbackInput.SHA256 != SHA256(feedbackRaw) ||
		result.Manifest.FeedbackAccessInput.SHA256 != SHA256(accessRaw) || result.Manifest.FeedbackAccessInput.ExpectedRecordCount != 2 {
		t.Fatalf("development feedback binding/label missing: %+v", result.Manifest)
	}
	if _, err := VerifyManifestDirectory(result.ManifestPath, issuer); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(result.FeedbackAccessPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(result.FeedbackAccessPath, append(accessRaw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyManifestDirectory(result.ManifestPath, issuer); err == nil {
		t.Fatal("tampered selected-feedback access receipt remained valid")
	}
	if _, err := WriteArtifacts(t.TempDir(), cr, append(sr, '\n'), feedbackRaw, issuer); err == nil {
		t.Fatal("mutated exact stream unexpectedly remained valid")
	}
}

func TestPermanentAdversarialRawRegressions(t *testing.T) {
	c := testConfig("mean")
	op := opportunity(0, "r", "t", "w")
	op.SettlementDelaySteps = 3
	cr, sr := marshalConfig(t, c), marshalStream(t, op)
	issuer := issuerFor(t, c, cr, map[string]usage{"r": {1, 20, .5}})
	run, err := RunWithIssuer(cr, sr, issuer)
	if err != nil {
		t.Fatal(err)
	}
	assertReject := func(name string, mutate func([]LifecycleRecord)) {
		t.Helper()
		rows := append([]LifecycleRecord(nil), run.Records...)
		mutate(rows)
		raw, _ := EncodeJSONL(rows)
		gz, _ := deterministicGzip(raw)
		if _, _, _, _, _, err := VerifyRaw(gz, cr, sr, issuer); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	assertReject("method-forgery", func(r []LifecycleRecord) {
		for i := range r {
			r[i].ComparatorMethod = "strict_max"
		}
	})
	assertReject("invented-event", func(r []LifecycleRecord) { r[1].LifecycleEvent = "invented" })
	assertReject("negative-logical-time", func(r []LifecycleRecord) { r[1].LogicalTimeStep = -1 })
	assertReject("tenant-window-forgery", func(r []LifecycleRecord) {
		for i := range r {
			r[i].TenantID = "other"
		}
	})
	assertReject("reservation-cost-forgery", func(r []LifecycleRecord) {
		for i := range r {
			if r[i].RecordType == RecordLifecycle && r[i].Selected {
				r[i].ReservedMicros = 21
			}
		}
	})
	assertReject("ledger-delta-forgery", func(r []LifecycleRecord) {
		r[0].SettledMicros = 101
		r[0].OutstandingMicros = 0
		r[0].AvailableMicros = -1
	})
	assertReject("snapshot-heartbeat", func(r []LifecycleRecord) {
		last := len(r) - 1
		dup := r[3]
		dup.EventIndex = r[last].EventIndex
		dup.Timestamp = time.Date(2026, 7, 13, 0, 0, 0, int(dup.EventIndex), time.UTC).Format(time.RFC3339Nano)
		r[last] = dup
	})
	assertReject("unterminated", func(r []LifecycleRecord) { r[len(r)-1].LifecycleEvent = "" })
}

func TestComparatorAndProductionModeCannotBeRelabeled(t *testing.T) {
	c := testConfig("mean")
	c.ComparatorMethod = "strict_max"
	if err := c.Validate(); err == nil {
		t.Fatal("comparator/reservation mode mismatch accepted")
	}
}

func TestInMemoryTraceCoreFailsClosedForFrozenSelectedFeedback(t *testing.T) {
	c := testConfig("mean")
	c.EvidenceTier = "frozen_authorized"
	op := opportunity(0, "r", "t", "w")
	cr := marshalConfig(t, c)
	issuer := frozenTestIssuer{issuerFor(t, c, cr, map[string]usage{"r": {1, 20, .5}})}
	if _, err := RunWithIssuer(cr, marshalStream(t, op), issuer); err == nil || !strings.Contains(err.Error(), "PostgreSQL authority ledger") {
		t.Fatalf("frozen in-memory run did not fail closed: %v", err)
	}
}

func TestDevelopmentEvidenceCannotNameFrozenTest(t *testing.T) {
	c := testConfig("mean")
	c.SelectedFeedbackSplit = "frozen_test"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "may not access frozen_test") {
		t.Fatalf("development/frozen-test firewall did not fail closed: %v", err)
	}
}

func TestSyntheticP0IdentityIsDevelopmentOnly(t *testing.T) {
	c := testConfig("strict_max")
	c.SelectedFeedbackDatasetID = "synthetic_p0_outcomes"
	c.SelectedFeedbackSplit = "development"
	if err := c.Validate(); err != nil {
		t.Fatalf("development synthetic P0 identity rejected: %v", err)
	}
	c.EvidenceTier = "frozen_authorized"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "RouterEval authority") {
		t.Fatalf("synthetic P0 identity escaped development tier: %v", err)
	}
	c.EvidenceTier = "development_debug"
	c.SelectedFeedbackSplit = "frozen_test"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "may not access frozen_test") {
		t.Fatalf("synthetic P0 identity accepted frozen_test: %v", err)
	}
}

func TestDevelopmentFixtureRejectsMissingExtraAndUnmappedOutcomes(t *testing.T) {
	c := testConfig("strict_max")
	c.SelectedFeedbackDatasetID = "synthetic_p0_outcomes"
	op := opportunity(0, "exact", "tenant", "window")
	cr := marshalConfig(t, c)
	feedback := marshalDevelopmentOutcomes(t, c, map[string]usage{"exact": {1, 2, .5}, "extra": {1, 2, .5}})
	issuer := issuerForRaw(t, c, cr, feedback)
	if err := issuer.ValidateOpportunityCoverage([]Opportunity{op}, c.SelectedFeedbackModelIDs); err == nil {
		t.Fatal("extra selected-outcome row accepted")
	}
	feedback = marshalDevelopmentOutcomes(t, c, map[string]usage{"missing": {1, 2, .5}})
	issuer = issuerForRaw(t, c, cr, feedback)
	if err := issuer.ValidateOpportunityCoverage([]Opportunity{op}, c.SelectedFeedbackModelIDs); err == nil {
		t.Fatal("missing selected-outcome row accepted")
	}
	feedback = marshalDevelopmentOutcomes(t, c, map[string]usage{"exact": {1, 2, .5}})
	issuer = issuerForRaw(t, c, cr, feedback)
	row := issuer.outcomes["exact"]
	row.SelectedModelID = strings.Repeat("8", 64)
	issuer.outcomes["exact"] = row
	if err := issuer.ValidateOpportunityCoverage([]Opportunity{op}, c.SelectedFeedbackModelIDs); err == nil {
		t.Fatal("outcome outside the selected deployment map accepted")
	}
}

func TestDeterministicBytes(t *testing.T) {
	c := testConfig("mean")
	op := opportunity(0, "r", "t", "w")
	cr, sr := marshalConfig(t, c), marshalStream(t, op)
	a, _ := RunWithIssuer(cr, sr, issuerFor(t, c, cr, map[string]usage{"r": {1, 20, .5}}))
	b, _ := RunWithIssuer(cr, sr, issuerFor(t, c, cr, map[string]usage{"r": {1, 20, .5}}))
	ar, _ := EncodeJSONL(a.Records)
	br, _ := EncodeJSONL(b.Records)
	if !reflect.DeepEqual(ar, br) {
		t.Fatal("raw bytes differ")
	}
}
