package govarexperiment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
)

// forgedReceiptIssuerV2 models the exact interface-substitution attack that
// the concrete runner signature must make unrepresentable at its call site.
type forgedReceiptIssuerV2 struct{ *DevelopmentUsageIssuerV2 }

func (i *forgedReceiptIssuerV2) RevealUsage(ctx context.Context, request UsageRevealRequestV2) (UsageReceiptV2, error) {
	receipt, err := i.DevelopmentUsageIssuerV2.revealUsage(ctx, request)
	if err != nil {
		return UsageReceiptV2{}, err
	}
	receipt.ActualOutputTokens++ // remains below the fixture's verified cap
	receipt.ReceiptSHA256 = usageReceiptDigestV2(receipt)
	return receipt, nil
}

func (*forgedReceiptIssuerV2) VerifyUsage(UsageRevealRequestV2, UsageReceiptV2) error { return nil }

func marshalConfigV2(t *testing.T, config ConfigV2) []byte {
	t.Helper()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func marshalPreOutcomeMappingsV2(t *testing.T, opportunities ...OpportunityV2) []byte {
	t.Helper()
	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	for _, opportunity := range opportunities {
		mapping := PreOutcomeMappingV2{
			SchemaVersion: PreOutcomeMappingSchemaV2, RecordType: "preoutcome_mapping",
			Sequence: opportunity.Sequence, RequestID: opportunity.RequestID,
			TenantID: opportunity.TenantID, BudgetWindowID: opportunity.BudgetWindowID,
			WorkloadUID: opportunity.WorkloadUID, BudgetMicros: opportunity.BudgetMicros,
			CohortID: opportunity.CohortID, CohortIndex: opportunity.CohortIndex,
			InputTokens: opportunity.InputTokens, MaxOutputTokens: opportunity.MaxOutputTokens,
			ArrivalAt: opportunity.ArrivalAt, ProviderResponseAt: opportunity.ProviderResponseAt,
			UsageAvailableAt: opportunity.UsageAvailableAt, SettlementAt: opportunity.SettlementAt,
			FaultMode: opportunity.FaultMode, UsageItemID: opportunity.UsageItemID,
			SourceRow: opportunity.SourceRow, SourceTimestamp: opportunity.SourceTimestamp,
			ContextTokens: opportunity.ContextTokens, PreOutcomeID: opportunity.PreOutcomeID,
			DuplicateSettlement:         opportunity.DuplicateSettlement,
			ConflictingSettlementReplay: opportunity.ConflictingSettlementReplay,
		}
		if err := encoder.Encode(mapping); err != nil {
			t.Fatal(err)
		}
	}
	return raw.Bytes()
}

func testUsageIssuerForStreamV2(t *testing.T, streamRaw []byte, opportunities []OpportunityV2, rows []UsageObservationV2) (*DevelopmentUsageIssuerV2, UsageProducerEvidenceV2) {
	t.Helper()
	mappingRaw := marshalPreOutcomeMappingsV2(t, opportunities...)
	usageRaw := marshalUsageV2(t, rows...)
	evidence := testUsageProducerEvidenceV2(usageRaw, mappingRaw)
	evidence.ProvenanceLock.OpportunityStreamSHA256 = SHA256(streamRaw)
	evidence.ProvenanceLock.ProtocolSHA256 = testConfig("mean").ProtocolSHA256
	evidence.ProvenanceLock = sealTestProvenanceLockV2(evidence.ProvenanceLock)
	issuer, err := ParseDevelopmentUsageV2(usageRaw, mappingRaw, evidence)
	if err != nil {
		t.Fatal(err)
	}
	return issuer, evidence
}

func runnerV2Fixture(t *testing.T, method string, factors E1Factors, opportunities []OpportunityV2, rows []UsageObservationV2) ([]byte, []byte, *DevelopmentUsageIssuerV2) {
	t.Helper()
	streamRaw := marshalStreamV2(t, opportunities...)
	issuer, evidence := testUsageIssuerForStreamV2(t, streamRaw, opportunities, rows)
	config := testConfigV2FromV1(testConfig(method), factors)
	config.ProvenanceLock = evidence.ProvenanceLock
	config.UsageBindingSHA256 = issuer.Binding().BindingSHA256
	return marshalConfigV2(t, config), streamRaw, issuer
}

func oneRunnerOpportunityV2(id string, base time.Time, delay time.Duration) (OpportunityV2, UsageObservationV2) {
	response := base.Add(time.Second)
	usage := response
	if delay > 0 {
		usage = response.Add(time.Second)
	}
	opportunity := testOpportunityV2(0, id, base, response, usage, response.Add(delay))
	opportunity.BudgetMicros = 1_000
	row := testUsageObservationForOpportunityV2(opportunity, 10)
	return opportunity, row
}

func prospectiveEvidenceWithSupportV2(cohorts []CohortSpec, support int) GOVARProspectiveEvidence {
	prospective := testProspectiveEvidence(cohorts)
	prospective.Profiles[0].Calibration = nil
	calibrationBase := time.Date(2026, 7, 12, 22, 0, 0, 0, time.UTC)
	for index := 0; index < support; index++ {
		prospective.Profiles[0].Calibration = append(prospective.Profiles[0].Calibration, GOVARProspectiveSample{
			RequestID:         fmt.Sprintf("cal-v2-%02d", index),
			ProviderAttemptID: fmt.Sprintf("cal-v2-attempt-%02d", index),
			OpportunityID:     fmt.Sprintf("cal-v2-op-%02d", index),
			OutputTokens:      int64(index + 1), SettledAt: calibrationBase.Add(time.Duration(index) * time.Minute),
		})
	}
	return prospective
}

func TestRunnerV2IsCausalAndDoesNotRetroInjectUsage(t *testing.T) {
	base := time.Date(2026, 7, 14, 15, 0, 0, 0, time.UTC)
	opportunity, row := oneRunnerOpportunityV2("request-causal", base, 10*time.Second)
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayTen,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, issuer := runnerV2Fixture(t, "fixed_quantile", factors, []OpportunityV2{opportunity}, []UsageObservationV2{row})
	result, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if result.RuntimeProvenance != RuntimeProvenanceV2 || result.FinalEvidenceEligible || result.DecisionCounts["ADMIT"] != 1 {
		t.Fatalf("wrong qualification provenance or decision: %+v", result)
	}
	seenUsage := false
	for _, record := range result.Records {
		if record.LifecycleEvent == "usage_available" {
			seenUsage = true
		}
		if !seenUsage && (record.ActualInputTokens != nil || record.ActualOutputTokens != nil || record.ActualCostMicros != nil || record.UsageReceiptSHA256 != "") {
			t.Fatalf("future use was retro-injected into %s: %+v", record.LifecycleEvent, record)
		}
		if record.LifecycleEvent == "usage_available" || record.LifecycleEvent == "settlement_final" {
			if record.ActualInputTokens == nil || record.ActualOutputTokens == nil || record.ActualCostMicros == nil || record.UsageReceiptSHA256 == "" {
				t.Fatalf("post-availability record lacks verified use: %+v", record)
			}
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		lower := bytes.ToLower(raw)
		for _, forbidden := range [][]byte{[]byte("selected_feedback"), []byte("selected_score"), []byte(`"score`), []byte(`"quality`)} {
			if bytes.Contains(lower, forbidden) {
				t.Fatalf("usage-only lifecycle contains %q: %s", forbidden, raw)
			}
		}
	}
	if !seenUsage || result.LifecycleCounts["settlement_final"] != 1 {
		t.Fatalf("causal lifecycle is incomplete: %v", result.LifecycleCounts)
	}
	access := issuer.UsageAccessLog()
	if len(access) != 2 || access[0].Kind != "authorize" || access[1].Kind != "reveal" || !access[0].Effective || !access[1].Effective ||
		access[0].At != opportunity.ArrivalAt || access[1].At != opportunity.UsageAvailableAt {
		t.Fatalf("usage authority was called outside causal events: %+v", access)
	}
}

func TestRunnerV2DeterministicAcrossFreshAuthorities(t *testing.T) {
	base := time.Date(2026, 7, 14, 15, 30, 0, 0, time.UTC)
	opportunity, row := oneRunnerOpportunityV2("request-deterministic", base, 0)
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, firstIssuer := runnerV2Fixture(t, "mean_margin", factors, []OpportunityV2{opportunity}, []UsageObservationV2{row})
	secondIssuer, _ := testUsageIssuerForStreamV2(t, streamRaw, []OpportunityV2{opportunity}, []UsageObservationV2{row})
	first, err := RunWithUsageIssuerV2(configRaw, streamRaw, firstIssuer)
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunWithUsageIssuerV2(configRaw, streamRaw, secondIssuer)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(firstIssuer.UsageAccessLog(), secondIssuer.UsageAccessLog()) {
		t.Fatalf("fresh deterministic runs differ:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func TestRunnerV2ExecutesOnlineNonGOVARMethods(t *testing.T) {
	base := time.Date(2026, 7, 14, 16, 0, 0, 0, time.UTC)
	for _, method := range []string{
		"no_budget", "settled_only", "strict_max", "mean", "mean_margin",
		"fixed_quantile", "fixed_estimate", "adaptive_quantile", "expected_cost_router",
	} {
		t.Run(method, func(t *testing.T) {
			risk := E1RiskTargetOnePercent
			if method == "adaptive_quantile" {
				risk = E1RiskTargetFivePercent
			}
			factors := E1Factors{
				CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
				BudgetLevel: E1BudgetMedium, RiskTargetPPB: risk,
			}
			opportunity, row := oneRunnerOpportunityV2("request-"+method, base, 0)
			configRaw, streamRaw, issuer := runnerV2Fixture(t, method, factors, []OpportunityV2{opportunity}, []UsageObservationV2{row})
			result, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
			if err != nil {
				t.Fatal(err)
			}
			if result.DecisionCounts["ADMIT"] != 1 || result.LifecycleCounts["settlement_final"] != 1 {
				t.Fatalf("method did not complete its real core path: decisions=%v lifecycle=%v", result.DecisionCounts, result.LifecycleCounts)
			}
		})
	}
}

func TestRunnerV2NonReservingMethodsUseHardFeasibilityFilter(t *testing.T) {
	base := time.Date(2026, 7, 14, 16, 5, 0, 0, time.UTC)
	for _, method := range []string{"no_budget", "settled_only"} {
		t.Run(method, func(t *testing.T) {
			opportunity, row := oneRunnerOpportunityV2("request-hard-filter-"+method, base, 0)
			opportunity.InputTokens = 10_000_000
			opportunity.ContextTokens = opportunity.InputTokens
			var err error
			opportunity.PreOutcomeID, err = AzurePreOutcomeIDV2(opportunity.SourceRow, opportunity.SourceTimestamp, opportunity.ContextTokens)
			if err != nil {
				t.Fatal(err)
			}
			opportunity.UsageItemID = opportunity.PreOutcomeID
			row = testUsageObservationForOpportunityV2(opportunity, row.ActualOutputTokens)
			factors := E1Factors{
				CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
				BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
			}
			configRaw, streamRaw, issuer := runnerV2Fixture(t, method, factors, []OpportunityV2{opportunity}, []UsageObservationV2{row})
			result, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
			if err != nil {
				t.Fatal(err)
			}
			if result.DecisionCounts["ABSTAIN"] != 1 || result.LifecycleCounts["dispatch_delivered"] != 0 || result.LifecycleCounts["settlement_final"] != 0 {
				t.Fatalf("non-reserving method bypassed hard feasibility: decisions=%v lifecycle=%v", result.DecisionCounts, result.LifecycleCounts)
			}
		})
	}
}

func TestRunnerV2ExpectedCostRouterUsesUtilityCoefficient(t *testing.T) {
	base := time.Date(2026, 7, 14, 16, 7, 0, 0, time.UTC)
	run := func(t *testing.T, penalty int64) RunResultV2 {
		t.Helper()
		opportunity, row := oneRunnerOpportunityV2(fmt.Sprintf("request-expected-cost-%d", penalty), base, 0)
		streamRaw := marshalStreamV2(t, opportunity)
		issuer, evidence := testUsageIssuerForStreamV2(t, streamRaw, []OpportunityV2{opportunity}, []UsageObservationV2{row})
		common := testConfig("expected_cost_router")
		common.MethodConfig.ExpectedCost.UtilityCostPenaltyPPBPerMicro = penalty
		var err error
		common.MethodConfigSHA256, err = MethodConfigDigest(common.MethodConfig)
		if err != nil {
			t.Fatal(err)
		}
		config := testConfigV2FromV1(common, E1Factors{
			CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
			BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
		})
		config.ProvenanceLock = evidence.ProvenanceLock
		config.UsageBindingSHA256 = issuer.Binding().BindingSHA256
		result, err := RunWithUsageIssuerV2(marshalConfigV2(t, config), streamRaw, issuer)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	selected := func(result RunResultV2) (string, int64) {
		for _, record := range result.Records {
			if record.LifecycleEvent == "admission" && record.Decision == "ADMIT" {
				return record.SelectedDeployment, record.ReservedMicros
			}
		}
		return "", 0
	}
	qualityFirst, qualityFirstReserved := selected(run(t, 0))
	costFirst, costFirstReserved := selected(run(t, 10))
	if qualityFirst != "experiment-model-expected-premium" || costFirst != "experiment-model-expected-economy" {
		t.Fatalf("expected-cost coefficient did not control routing: penalty0=%s penalty10=%s", qualityFirst, costFirst)
	}
	if !(qualityFirstReserved > costFirstReserved && costFirstReserved > 0) {
		t.Fatalf("expected-cost reservation did not follow selected candidate prices: premium=%d economy=%d", qualityFirstReserved, costFirstReserved)
	}
}

func TestRunnerV2RejectsOracleAsOnlineMethod(t *testing.T) {
	base := time.Date(2026, 7, 14, 16, 10, 0, 0, time.UTC)
	opportunity, row := oneRunnerOpportunityV2("request-oracle-online-v2", base, 0)
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, issuer := runnerV2Fixture(t, "oracle_future_cost", factors, []OpportunityV2{opportunity}, []UsageObservationV2{row})
	_, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
	if err == nil || !strings.Contains(err.Error(), "post-nonoracle evaluator") {
		t.Fatalf("oracle_future_cost was not kept out of the online runner: %v", err)
	}
}

func TestRunnerV2ExecutesGOVARMethodCore(t *testing.T) {
	base := time.Date(2026, 7, 14, 16, 30, 0, 0, time.UTC)
	opportunity, row := oneRunnerOpportunityV2("request-gov-ar-v2", base, 0)
	opportunity.CohortID = "cohort-v2"
	opportunity.CohortIndex = 0

	common := testConfig("strict_max")
	common.RegistryID = registryID("gov_ar")
	common.ProtocolMethodID = "gov_ar"
	common.ComparatorMethod = "gov_ar"
	common.ProductionReservationMode = "govar_fixed_cohort"
	common.Cohorts = []CohortSpec{{
		TenantID: opportunity.TenantID, BudgetWindowID: opportunity.BudgetWindowID,
		CohortID: opportunity.CohortID, Size: 1,
	}}
	prospective := prospectiveEvidenceWithSupportV2(common.Cohorts, 19)
	method, err := BuildProspectiveGOVARMethodConfig(common, []Opportunity{projectOpportunityV2(opportunity)},
		int64(E1RiskTargetFivePercent), 100_000_000, 7200, 1,
		time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC), prospective)
	if err != nil {
		t.Fatal(err)
	}
	common.MethodConfig = method
	common.MethodConfigSHA256, err = MethodConfigDigest(method)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfigV2FromV1(common, E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetFivePercent,
	})
	streamRaw := marshalStreamV2(t, opportunity)
	issuer, evidence := testUsageIssuerForStreamV2(t, streamRaw, []OpportunityV2{opportunity}, []UsageObservationV2{row})
	config.ProvenanceLock = evidence.ProvenanceLock
	config.UsageBindingSHA256 = issuer.Binding().BindingSHA256
	result, err := RunWithUsageIssuerV2(marshalConfigV2(t, config), streamRaw, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if result.DecisionCounts["ADMIT"] != 1 || result.LifecycleCounts["settlement_final"] != 1 {
		t.Fatalf("GOV-AR core did not complete: decisions=%v lifecycle=%v", result.DecisionCounts, result.LifecycleCounts)
	}
	foundReservation := false
	for _, record := range result.Records {
		if record.LifecycleEvent == "admission" && record.ReservedMicros > 0 {
			foundReservation = true
		}
	}
	if !foundReservation {
		t.Fatal("GOV-AR did not execute its slot-bound reservation")
	}
}

func TestRunnerV2EventClockDrivesGOVARFreshnessAndConservativeFallback(t *testing.T) {
	virtualStart := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)
	freshAt := virtualStart.Add(10 * time.Minute)
	staleAt := virtualStart.Add(45 * time.Minute)
	first := testOpportunityV2(0, "request-clock-fresh", freshAt, freshAt.Add(time.Second), freshAt.Add(time.Second), freshAt.Add(time.Second))
	second := testOpportunityV2(1, "request-clock-stale", staleAt, staleAt.Add(time.Second), staleAt.Add(time.Second), staleAt.Add(time.Second))
	for index, opportunity := range []*OpportunityV2{&first, &second} {
		opportunity.BudgetMicros = 1_000
		opportunity.CohortID = "cohort-clock-v2"
		opportunity.CohortIndex = int64(index)
	}
	rows := []UsageObservationV2{
		testUsageObservationForOpportunityV2(first, 10),
		testUsageObservationForOpportunityV2(second, 10),
	}

	common := testConfig("strict_max")
	common.RegistryID = registryID("gov_ar")
	common.ProtocolMethodID = "gov_ar"
	common.ComparatorMethod = "gov_ar"
	common.ProductionReservationMode = "govar_fixed_cohort"
	common.Cohorts = []CohortSpec{{
		TenantID: first.TenantID, BudgetWindowID: first.BudgetWindowID,
		CohortID: first.CohortID, Size: 2,
	}}
	prospective := prospectiveEvidenceWithSupportV2(common.Cohorts, 39)
	method, err := BuildProspectiveGOVARMethodConfig(common, []Opportunity{projectOpportunityV2(first), projectOpportunityV2(second)},
		int64(E1RiskTargetFivePercent), 100_000_000, 7200, 1,
		time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC), prospective)
	if err != nil {
		t.Fatal(err)
	}
	common.MethodConfig = method
	common.MethodConfigSHA256, err = MethodConfigDigest(method)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfigV2FromV1(common, E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetFivePercent,
	})
	streamRaw := marshalStreamV2(t, first, second)
	issuer, evidence := testUsageIssuerForStreamV2(t, streamRaw, []OpportunityV2{first, second}, rows)
	config.ProvenanceLock = evidence.ProvenanceLock
	config.UsageBindingSHA256 = issuer.Binding().BindingSHA256

	result, err := RunWithUsageIssuerV2(marshalConfigV2(t, config), streamRaw, issuer)
	if err != nil {
		t.Fatal(err)
	}
	admissions := map[string]LifecycleRecordV2{}
	var previous time.Time
	for _, record := range result.Records {
		at, parseErr := time.Parse(time.RFC3339Nano, record.Timestamp)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		if !previous.IsZero() && at.Before(previous) {
			t.Fatalf("lifecycle time moved backwards from %s to %s", previous, at)
		}
		previous = at
		if record.LifecycleEvent == "admission" {
			admissions[record.RequestID] = record
		}
	}
	fresh := admissions[first.RequestID]
	stale := admissions[second.RequestID]
	freshUpper := method.GOVAR.EvidenceRegistry[0].Calibration.UpperOutputTokens
	if fresh.Timestamp != first.ArrivalAt || fresh.Decision != "ADMIT" || fresh.ReasonCode != string(govar.ReasonHighestUtility) ||
		fresh.ReservedMicros != 1+freshUpper {
		t.Fatalf("fresh event did not use event-time GOV-AR calibration: %+v upper=%d", fresh, freshUpper)
	}
	if stale.Timestamp != second.ArrivalAt || stale.Decision != "ADMIT" || stale.ReasonCode != string(govar.ReasonCalibrationStale) ||
		stale.ReservedMicros != 101 {
		t.Fatalf("stale event did not use event-time conservative fallback: %+v", stale)
	}
}

func TestGOVAR95PercentFiniteSplitConformalSupportBoundary(t *testing.T) {
	base := time.Date(2026, 7, 14, 16, 35, 0, 0, time.UTC)
	opportunity, _ := oneRunnerOpportunityV2("request-gov-ar-boundary", base, 0)
	opportunity.CohortID = "cohort-boundary"
	common := testConfig("strict_max")
	common.Cohorts = []CohortSpec{{
		TenantID: opportunity.TenantID, BudgetWindowID: opportunity.BudgetWindowID,
		CohortID: opportunity.CohortID, Size: 1,
	}}
	build := func(support int) (MethodConfig, error) {
		return BuildProspectiveGOVARMethodConfig(common, []Opportunity{projectOpportunityV2(opportunity)},
			int64(E1RiskTargetFivePercent), 100_000_000, 7200, 1,
			time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC), prospectiveEvidenceWithSupportV2(common.Cohorts, support))
	}
	if _, err := build(18); err == nil || !strings.Contains(err.Error(), "support 18") || !strings.Contains(err.Error(), "950000000") {
		t.Fatalf("support below the exact 95%% finite-bound threshold was not rejected: %v", err)
	}
	method, err := build(19)
	if err != nil {
		t.Fatalf("minimum mathematical support for 95%% was rejected: %v", err)
	}
	if len(method.GOVAR.EvidenceRegistry) != 1 || method.GOVAR.EvidenceRegistry[0].Calibration.Support != 19 ||
		method.GOVAR.EvidenceRegistry[0].Calibration.ConformalRank != 19 ||
		method.GOVAR.EvidenceRegistry[0].Calibration.CoverageTargetPPB != 950_000_000 {
		t.Fatalf("95%% boundary artifact is not exact: %+v", method.GOVAR.EvidenceRegistry)
	}
}

func TestRunnerV2SettledOnlyActiveActualExposureRisesThenClears(t *testing.T) {
	base := time.Date(2026, 7, 14, 16, 45, 0, 0, time.UTC)
	opportunity, row := oneRunnerOpportunityV2("request-settled-exposure", base, 10*time.Second)
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayTen,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, issuer := runnerV2Fixture(t, "settled_only", factors, []OpportunityV2{opportunity}, []UsageObservationV2{row})
	result, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range result.Records {
		switch record.LifecycleEvent {
		case "admission", "dispatch_claim", "dispatch_delivered", "provider_response":
			if record.ActiveActualMicros != 0 || record.ActualCostMicros != nil {
				t.Fatalf("pre-usage settled-only record was retro-filled: %+v", record)
			}
		case "usage_available":
			if record.ActiveActualMicros != 11 {
				t.Fatalf("usage availability did not raise active actual exposure: %+v", record)
			}
		case "settlement_final":
			if record.ActiveActualMicros != 0 || record.SettledMicros != 11 {
				t.Fatalf("settlement did not transfer active exposure to settled spend: %+v", record)
			}
		}
	}
}

func TestRunnerV2WorkGuardrailsCoverOneTenThousandRowFourHundredWindowCell(t *testing.T) {
	opportunities := make([]OpportunityV2, 10_000)
	for index := range opportunities {
		window := index % 400
		opportunities[index] = OpportunityV2{
			TenantID: fmt.Sprintf("tenant-%03d", window), BudgetWindowID: fmt.Sprintf("window-%03d", window),
			RequestID: fmt.Sprintf("request-%05d", index),
		}
	}
	work, err := planRuntimeWorkV2(opportunities, "gov_ar")
	if err != nil {
		t.Fatal(err)
	}
	if work.QualificationOpportunityLimit != 10_000 || work.QualificationWindowLimit != 400 ||
		work.OpportunityCount != 10_000 || work.ScheduledEventCount != 40_000 || work.WindowContextCount != 400 ||
		work.EngineCount != 400 || work.GOVARArtifactIndexBuildCount != 1 || work.GOVARWindowInitializationCount != 400 ||
		work.GOVAROpportunityBindingCheckCount != 10_000 {
		t.Fatalf("runtime work report does not describe the bounded P1b cell: %+v", work)
	}
	tooManyRows := append(append([]OpportunityV2(nil), opportunities...), OpportunityV2{TenantID: "tenant-000", BudgetWindowID: "window-000"})
	if _, err := planRuntimeWorkV2(tooManyRows, "gov_ar"); err == nil || !strings.Contains(err.Error(), "10000 opportunities") {
		t.Fatalf("qualification runtime accepted more than one P1b cell: %v", err)
	}
	tooManyWindows := make([]OpportunityV2, 401)
	for index := range tooManyWindows {
		tooManyWindows[index] = OpportunityV2{TenantID: fmt.Sprintf("tenant-%03d", index), BudgetWindowID: fmt.Sprintf("window-%03d", index)}
	}
	if _, err := planRuntimeWorkV2(tooManyWindows, "gov_ar"); err == nil || !strings.Contains(err.Error(), "400 tenant-window") {
		t.Fatalf("qualification runtime accepted more than 400 tenant-window contexts: %v", err)
	}
}

func TestRunnerV2UsesOneSharedEngineAcrossTenants(t *testing.T) {
	base := time.Date(2026, 7, 14, 17, 10, 0, 0, time.UTC)
	first, firstRow := oneRunnerOpportunityV2("request-shared-engine-a", base, 0)
	second := testOpportunityV2(1, "request-shared-engine-b", base.Add(3*time.Second), base.Add(4*time.Second), base.Add(4*time.Second), base.Add(4*time.Second))
	second.BudgetMicros = 1_000
	secondRow := testUsageObservationForOpportunityV2(second, 10)
	first.TenantID, first.BudgetWindowID = "tenant-shared-a", "window-shared"
	second.TenantID, second.BudgetWindowID = "tenant-shared-b", "window-shared"
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, issuer := runnerV2Fixture(t, "strict_max", factors, []OpportunityV2{first, second}, []UsageObservationV2{firstRow, secondRow})
	result, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if result.Work.EngineCount != 1 || result.Work.WindowContextCount != 2 ||
		result.DecisionCounts["ADMIT"] != 2 || result.LifecycleCounts["settlement_final"] != 2 {
		t.Fatalf("runner did not execute two tenants through one shared engine: work=%+v decisions=%v lifecycle=%v", result.Work, result.DecisionCounts, result.LifecycleCounts)
	}
}

func TestRunnerV2TieOrderProcessesAllArrivalsBeforeLaterEventKinds(t *testing.T) {
	base := time.Date(2026, 7, 14, 17, 30, 0, 0, time.UTC)
	response := base.Add(time.Second)
	opportunities := make([]OpportunityV2, 0, 50)
	rows := make([]UsageObservationV2, 0, 50)
	for index := int64(0); index < 50; index++ {
		requestID := fmt.Sprintf("request-tie-%02d", index)
		opportunity := testOpportunityV2(index, requestID, base, response, response, response)
		opportunity.TenantID = fmt.Sprintf("tenant-tie-%02d", index)
		opportunity.BudgetWindowID = fmt.Sprintf("window-tie-%02d", index)
		opportunity.BudgetMicros = 1_000
		row := testUsageObservationForOpportunityV2(opportunity, 10)
		opportunities = append(opportunities, opportunity)
		rows = append(rows, row)
	}
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyFifty, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, issuer := runnerV2Fixture(t, "fixed_quantile", factors, opportunities, rows)
	result, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
	if err != nil {
		t.Fatal(err)
	}
	secondIssuer, _ := testUsageIssuerForStreamV2(t, streamRaw, opportunities, rows)
	replayed, err := RunWithUsageIssuerV2(configRaw, streamRaw, secondIssuer)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Records, replayed.Records) || !reflect.DeepEqual(result.DecisionCounts, replayed.DecisionCounts) ||
		!reflect.DeepEqual(result.LifecycleCounts, replayed.LifecycleCounts) {
		t.Fatal("equal-time replay order is not byte-stable across fresh authorities")
	}
	stage := 0
	admissionSequence := int64(0)
	providerCount, usageCount, settlementCount := 0, 0, 0
	for _, record := range result.Records {
		switch record.LifecycleEvent {
		case "admission", "dispatch_claim", "dispatch_delivered":
			if stage != 0 {
				t.Fatalf("arrival subevent appeared after a later tied event: %+v", record)
			}
			if record.LifecycleEvent == "admission" {
				if record.StreamSequence != admissionSequence {
					t.Fatalf("arrival sequence is not total and stable: got %d want %d", record.StreamSequence, admissionSequence)
				}
				admissionSequence++
			}
		case "provider_response":
			if stage > 1 {
				t.Fatalf("provider response appeared after usage/settlement: %+v", record)
			}
			stage = 1
			providerCount++
		case "usage_available":
			if stage > 2 || providerCount != 50 {
				t.Fatalf("usage appeared before all tied provider responses: provider=%d record=%+v", providerCount, record)
			}
			stage = 2
			usageCount++
		case "settlement_final":
			if usageCount != 50 {
				t.Fatalf("settlement appeared before all tied usage reveals: usage=%d record=%+v", usageCount, record)
			}
			stage = 3
			settlementCount++
		}
	}
	if admissionSequence != 50 || providerCount != 50 || usageCount != 50 || settlementCount != 50 {
		t.Fatalf("tied scheduler lost events: admissions=%d provider=%d usage=%d settlement=%d", admissionSequence, providerCount, usageCount, settlementCount)
	}
}

func TestRunnerV2IsolatesSameTenantAcrossBudgetWindows(t *testing.T) {
	base := time.Date(2026, 7, 14, 17, 0, 0, 0, time.UTC)
	first, firstRow := oneRunnerOpportunityV2("request-window-a", base, 0)
	second, secondRow := oneRunnerOpportunityV2("request-window-b", base.Add(2*time.Second), 0)
	first.BudgetWindowID, second.BudgetWindowID = "window-a", "window-b"
	first.BudgetMicros, second.BudgetMicros = 12, 12
	second.Sequence = 1
	second.SourceRow = 1
	second.PreOutcomeID, _ = AzurePreOutcomeIDV2(second.SourceRow, second.SourceTimestamp, second.ContextTokens)
	second.UsageItemID = second.PreOutcomeID
	secondRow = testUsageObservationForOpportunityV2(second, secondRow.ActualOutputTokens)
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetRestrictive, RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, issuer := runnerV2Fixture(t, "fixed_quantile", factors,
		[]OpportunityV2{first, second}, []UsageObservationV2{firstRow, secondRow})
	result, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if result.DecisionCounts["ADMIT"] != 2 {
		t.Fatalf("one tenant's independent windows contaminated each other: %v", result.DecisionCounts)
	}
	finalAvailable := map[string]int64{}
	for _, record := range result.Records {
		if record.LifecycleEvent == "settlement_final" {
			finalAvailable[record.BudgetWindowID] = record.AvailableMicros
		}
	}
	if finalAvailable["window-a"] != 1 || finalAvailable["window-b"] != 1 {
		t.Fatalf("tenant-window states are not isolated: %v", finalAvailable)
	}
}

func TestRunnerV2RequiresConcreteIssuerAndIndependentlyRejectsFutureUseReceipt(t *testing.T) {
	runType := reflect.TypeOf(RunWithUsageIssuerV2)
	concreteIssuerType := reflect.TypeOf((*DevelopmentUsageIssuerV2)(nil))
	if runType.NumIn() != 3 || runType.In(2) != concreteIssuerType {
		t.Fatalf("runner issuer parameter is not sealed to *DevelopmentUsageIssuerV2: %s", runType)
	}
	forgedType := reflect.TypeOf((*forgedReceiptIssuerV2)(nil))
	if forgedType.AssignableTo(runType.In(2)) {
		t.Fatal("receipt-forging wrapper is assignable to the sealed runner issuer parameter")
	}

	base := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	opportunity, row := oneRunnerOpportunityV2("request-future-receipt", base, 0)
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, issuer := runnerV2Fixture(t, "fixed_quantile", factors, []OpportunityV2{opportunity}, []UsageObservationV2{row})
	config, _, err := ParseConfigV2(configRaw)
	if err != nil {
		t.Fatal(err)
	}
	request := UsageRevealRequestV2{
		RunID: config.RunID, RequestID: opportunity.RequestID, UsageItemID: opportunity.UsageItemID,
		DispatchID: strings.Repeat("d", 64), ConfigSHA256: SHA256(configRaw), RevealAt: opportunity.UsageAvailableAt,
	}
	availableAt, _ := time.Parse(time.RFC3339Nano, opportunity.UsageAvailableAt)
	receipt := UsageReceiptV2{
		SchemaVersion: UsageReceiptSchemaV2, RecordType: "usage_receipt", RunID: config.RunID,
		RequestID: opportunity.RequestID, UsageItemID: opportunity.UsageItemID, DispatchID: request.DispatchID,
		ConfigSHA256: request.ConfigSHA256, UsageBindingSHA256: config.UsageBindingSHA256,
		UsageAvailableAt:  opportunity.UsageAvailableAt,
		RevealedAt:        availableAt.Add(time.Nanosecond).Format(time.RFC3339Nano),
		ActualInputTokens: row.ActualInputTokens, ActualOutputTokens: row.ActualOutputTokens,
	}
	receipt.ReceiptSHA256 = usageReceiptDigestV2(receipt)
	if err := validateUsageReceiptForRunnerV2(config, request.ConfigSHA256, opportunity, request, receipt); err == nil || !strings.Contains(err.Error(), "causal availability") {
		t.Fatalf("raw runner validator accepted a future-use receipt: %v", err)
	}
	if _, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer); err != nil {
		t.Fatalf("concrete byte-bound issuer no longer executes: %v", err)
	}
}
