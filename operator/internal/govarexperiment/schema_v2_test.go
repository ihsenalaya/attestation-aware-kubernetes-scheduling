package govarexperiment

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func testE1ConfigV2(concurrency E1Concurrency, delay E1SettlementDelaySeconds) ConfigV2 {
	common := testConfig("mean")
	return testConfigV2FromV1(common, E1Factors{
		CellType: E1CellPrincipal, Concurrency: concurrency, SettlementDelaySeconds: delay,
		BudgetLevel: E1BudgetMedium, RiskTargetPPB: E1RiskTargetOnePercent,
	})
}

func sealTestProvenanceLockV2(lock E1ProvenanceLockV2) E1ProvenanceLockV2 {
	lock.ExecutionSubjectSHA256 = lock.preOutcomeExecutionSubjectDigest()
	lock.ExecutionLockSHA256 = lock.executionDigest()
	return lock
}

func testProvenanceLockV2() E1ProvenanceLockV2 {
	return sealTestProvenanceLockV2(E1ProvenanceLockV2{
		SupportMode:                    E1SupportModeFixture,
		OpportunityStreamSHA256:        strings.Repeat("1", 64),
		SourceCodeBindingSHA256:        strings.Repeat("2", 64),
		SourceManifestSHA256:           strings.Repeat("3", 64),
		PilotUsageBindingSHA256:        strings.Repeat("4", 64),
		RuntimeCapabilitySHA256:        strings.Repeat("5", 64),
		MappingArtifactSHA256:          strings.Repeat("6", 64),
		PilotPreOutcomeSetSHA256:       strings.Repeat("7", 64),
		AssignedPreOutcomeSetSHA256:    strings.Repeat("0", 64),
		StreamPreOutcomeSequenceSHA256: strings.Repeat("8", 64),
		ProtocolSHA256:                 strings.Repeat("9", 64),
		DesignSpecSHA256:               strings.Repeat("a", 64),
		DecisionConfigTemplateSHA256:   strings.Repeat("b", 64),
		DesignLockSHA256:               strings.Repeat("c", 64),
		IndependentDesignReviewSHA256:  strings.Repeat("d", 64),
		DesignManifestSHA256:           strings.Repeat("e", 64),
		CohortRegistrySHA256:           strings.Repeat("f", 64),
		ProfileExecutionContractSHA256: strings.Repeat("7", 64),
		BudgetCalibrationSHA256:        strings.Repeat("8", 64),
		CalibrationArtifactSHA256:      strings.Repeat("3", 64),
		CandidateSetSHA256:             strings.Repeat("4", 64),
		ProfileRegistrySHA256:          strings.Repeat("5", 64),
		SlotTemplateSHA256:             strings.Repeat("6", 64),
		MethodBuilderBundleSHA256:      strings.Repeat("2", 64),
		ExecutionIntentSHA256:          strings.Repeat("1", 64),
		IndependentAuthorizationSHA256: SHA256([]byte("fixture authorization")),
	})
}

func testConfigV2FromV1(common Config, factors E1Factors) ConfigV2 {
	config := ConfigV2{
		SchemaVersion: ConfigSchemaV2, RecordType: common.RecordType,
		ExperimentID: common.ExperimentID, RunID: common.RunID, CellID: common.CellID,
		Scenario: common.Scenario, Seed: common.Seed, RegistryID: common.RegistryID,
		ProtocolMethodID: common.ProtocolMethodID, ComparatorMethod: common.ComparatorMethod,
		ProductionReservationMode: common.ProductionReservationMode, MethodConfig: common.MethodConfig,
		MethodConfigSHA256: common.MethodConfigSHA256, ClusterID: common.ClusterID, TrialID: common.TrialID,
		VirtualStart: common.VirtualStart, EvidenceTier: EvidenceTierV2,
		SoftwareSHA256: common.SoftwareSHA256, DataSHA256: common.DataSHA256,
		ProtocolSHA256: common.ProtocolSHA256, SplitSHA256: common.SplitSHA256,
		SourceSHA256: common.SourceSHA256, UsageDatasetID: UsageDatasetV2, UsageSplit: UsageSplitV2,
		ProvenanceLock:              testProvenanceLockV2(),
		UsageBindingSHA256:          strings.Repeat("e", 64),
		InputPriceMicrosPerMillion:  common.InputPriceMicrosPerMillion,
		OutputPriceMicrosPerMillion: common.OutputPriceMicrosPerMillion,
		VerifiedOutputCapTokens:     common.VerifiedOutputCapTokens,
		EstimateOutputTokens:        common.EstimateOutputTokens, MarginOutputTokens: common.MarginOutputTokens,
		Cohorts: append([]CohortSpec(nil), common.Cohorts...), E1Factors: factors,
	}
	config.ProvenanceLock.ProtocolSHA256 = common.ProtocolSHA256
	config.ProvenanceLock = sealTestProvenanceLockV2(config.ProvenanceLock)
	return config
}

func testOpportunityV2(sequence int64, id string, arrival, response, usage, settlement time.Time) OpportunityV2 {
	sourceTimestamp := "2024-01-01 00:00:00Z"
	preOutcomeID, err := AzurePreOutcomeIDV2(sequence, sourceTimestamp, 1)
	if err != nil {
		panic(err)
	}
	return OpportunityV2{
		SchemaVersion: OpportunitySchemaV2, RecordType: "opportunity", Sequence: sequence,
		RequestID: id, TenantID: "tenant", BudgetWindowID: "window", WorkloadUID: "workload-1",
		BudgetMicros: 100, InputTokens: 1, MaxOutputTokens: 100,
		ArrivalAt: arrival.UTC().Format(time.RFC3339Nano), ProviderResponseAt: response.UTC().Format(time.RFC3339Nano),
		UsageAvailableAt: usage.UTC().Format(time.RFC3339Nano), SettlementAt: settlement.UTC().Format(time.RFC3339Nano),
		FaultMode: FaultNominal, UsageItemID: preOutcomeID, SourceRow: sequence,
		SourceTimestamp: sourceTimestamp, ContextTokens: 1, PreOutcomeID: preOutcomeID,
	}
}

func marshalStreamV2(t *testing.T, opportunities ...OpportunityV2) []byte {
	t.Helper()
	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	for _, opportunity := range opportunities {
		if err := encoder.Encode(opportunity); err != nil {
			t.Fatal(err)
		}
	}
	return raw.Bytes()
}

func TestV2SchemasAreSeparateAndV1WireShapeIsUnchanged(t *testing.T) {
	v1Config := testConfig("mean")
	v1ConfigRaw, err := json.Marshal(v1Config)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(v1ConfigRaw, []byte(`"cell_type"`)) || bytes.Contains(v1ConfigRaw, []byte(`"risk_target_ppb"`)) {
		t.Fatalf("v2 factors leaked into the v1 config wire shape: %s", v1ConfigRaw)
	}
	v1OpportunityRaw, err := json.Marshal(opportunity(0, "v1-request", "tenant", "window"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(v1OpportunityRaw, []byte(`"arrival_at"`)) || bytes.Contains(v1OpportunityRaw, []byte(`"settlement_at"`)) {
		t.Fatalf("v2 timestamps leaked into the v1 opportunity wire shape: %s", v1OpportunityRaw)
	}

	v2 := testE1ConfigV2(E1ConcurrencyFifty, E1SettlementDelayTen)
	raw, err := json.Marshal(v2)
	if err != nil {
		t.Fatal(err)
	}
	parsed, digest, err := ParseConfigV2(append(raw, '\n'))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Concurrency != E1ConcurrencyFifty || parsed.RiskTargetPPB != E1RiskTargetOnePercent || digest != SHA256(raw) {
		t.Fatalf("v2 config did not round-trip exact factors: parsed=%+v digest=%s", parsed.E1Factors, digest)
	}
	if _, _, err := ParseConfig(append(raw, '\n')); err == nil {
		t.Fatal("v2 config entered the v1 parser")
	}
	forged := append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"unexpected":true}`)...)
	if _, _, err := ParseConfigV2(forged); err == nil {
		t.Fatal("unknown v2 config field was accepted")
	}
}

func TestConfigV2RejectsOutOfGridFactors(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ConfigV2)
	}{
		{"cell-type", func(c *ConfigV2) { c.CellType = "pilot" }},
		{"concurrency", func(c *ConfigV2) { c.Concurrency = 49 }},
		{"delay", func(c *ConfigV2) { c.SettlementDelaySeconds = 30 }},
		{"budget", func(c *ConfigV2) { c.BudgetLevel = "cheap" }},
		{"risk", func(c *ConfigV2) { c.RiskTargetPPB = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testE1ConfigV2(E1ConcurrencyOne, E1SettlementDelayZero)
			test.mutate(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("out-of-grid factor was accepted")
			}
		})
	}
}

func TestE1FactorsAcceptExactlyTheDeclaredGrid(t *testing.T) {
	cellTypes := []E1CellType{E1CellPrincipal, E1CellSensitivity}
	concurrencies := []E1Concurrency{E1ConcurrencyOne, E1ConcurrencyFifty, E1ConcurrencyFiveHundred}
	delays := []E1SettlementDelaySeconds{E1SettlementDelayZero, E1SettlementDelayTen, E1SettlementDelaySixty}
	budgets := []E1BudgetLevel{E1BudgetRestrictive, E1BudgetMedium, E1BudgetUnconstrained}
	risks := []E1RiskTargetPPB{E1RiskTargetZero, E1RiskTargetOnePercent, E1RiskTargetFivePercent}
	for _, cellType := range cellTypes {
		for _, concurrency := range concurrencies {
			for _, delay := range delays {
				for _, budget := range budgets {
					for _, risk := range risks {
						factors := E1Factors{CellType: cellType, Concurrency: concurrency, SettlementDelaySeconds: delay, BudgetLevel: budget, RiskTargetPPB: risk}
						if err := factors.Validate(); err != nil {
							t.Fatalf("declared factor coordinate was rejected: %+v: %v", factors, err)
						}
					}
				}
			}
		}
	}
}

func TestConfigV2BindsRiskAwareMethodArtifact(t *testing.T) {
	common := testConfig("adaptive_quantile")
	common.MethodConfig.Adaptive.CoverageTargetPPB = 950_000_000
	common.MethodConfigSHA256, _ = MethodConfigDigest(common.MethodConfig)
	config := testConfigV2FromV1(common, E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetRestrictive, RiskTargetPPB: E1RiskTargetFivePercent,
	})
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	config.RiskTargetPPB = E1RiskTargetOnePercent
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "coverage target") {
		t.Fatalf("risk/artifact mismatch was accepted or misdiagnosed: %v", err)
	}
}

func TestConfigV2RejectsZeroRiskForGOVAR(t *testing.T) {
	common := testConfig("strict_max")
	common.RegistryID = registryID("gov_ar")
	common.ProtocolMethodID = "gov_ar"
	common.ComparatorMethod = "gov_ar"
	common.ProductionReservationMode = "govar_fixed_cohort"
	common.MethodConfig = MethodConfig{ProtocolID: "gov_ar", GOVAR: &GOVARMethodConfig{TenantRiskPPB: 0}}
	common.MethodConfigSHA256, _ = MethodConfigDigest(common.MethodConfig)
	config := testConfigV2FromV1(common, E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne, SettlementDelaySeconds: E1SettlementDelayZero,
		BudgetLevel: E1BudgetRestrictive, RiskTargetPPB: E1RiskTargetZero,
	})
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "strictly positive") {
		t.Fatalf("GOV-AR accepted a zero risk target or misdiagnosed it: %v", err)
	}
}

func TestParseStreamV2RecomputesConcurrentTieBatch(t *testing.T) {
	config := testE1ConfigV2(E1ConcurrencyFifty, E1SettlementDelayTen)
	base, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	opportunities := make([]OpportunityV2, 0, 50)
	for index := int64(0); index < 50; index++ {
		// Arrival, provider response, and usage availability deliberately tie.
		// The frozen tie rule must count every arrival before any response.
		opportunities = append(opportunities, testOpportunityV2(index, fmt.Sprintf("request-%03d", index),
			base, base, base, base.Add(10*time.Second)))
	}
	raw := marshalStreamV2(t, opportunities...)
	parsed, facts, err := ParseStreamV2(raw, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 50 || facts.RecordCount != 50 || facts.ArrivalBatchCount != 1 || facts.MaximumArrivalBatchSize != 50 ||
		facts.ObservedPeakConcurrency != E1ConcurrencyFifty || facts.TieBreakPolicy != OpportunityTieBreakV2 {
		t.Fatalf("incorrect recomputed stream facts: %+v", facts)
	}

	events := make([]opportunityEventV2, 0, len(opportunities)*4)
	for _, opportunity := range opportunities {
		times, _ := opportunity.times()
		events = append(events,
			opportunityEventV2{at: times.settlement, kind: eventSettlementV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.usageAvailable, kind: eventUsageAvailableV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.providerResponse, kind: eventProviderResponseV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.arrival, kind: eventArrivalV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
		)
	}
	orderedOpportunityEventsV2(events)
	for index := 0; index < 50; index++ {
		if events[index].kind != eventArrivalV2 || events[index].sequence != int64(index) {
			t.Fatalf("arrival tie order is not deterministic at event %d: %+v", index, events[index])
		}
	}
	for index := 50; index < 100; index++ {
		if events[index].kind != eventProviderResponseV2 || events[index].sequence != int64(index-50) {
			t.Fatalf("provider-response tie order is not deterministic at event %d: %+v", index, events[index])
		}
	}
}

func TestStreamV2AllowsIndependentWindowRollover(t *testing.T) {
	config := testE1ConfigV2(E1ConcurrencyOne, E1SettlementDelayTen)
	base, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	first := testOpportunityV2(0, "request-first", base, base.Add(time.Second), base.Add(2*time.Second), base.Add(11*time.Second))
	second := testOpportunityV2(1, "request-second", base.Add(2*time.Second), base.Add(3*time.Second), base.Add(4*time.Second), base.Add(13*time.Second))
	second.BudgetWindowID = "window-next"
	second.BudgetMicros = 200
	_, facts, err := ParseStreamV2(marshalStreamV2(t, first, second), config)
	if err != nil {
		t.Fatal(err)
	}
	if facts.ObservedPeakConcurrency != E1ConcurrencyOne || facts.ArrivalBatchCount != 2 {
		t.Fatalf("unexpected rollover stream facts: %+v", facts)
	}
}

func TestStreamV2RejectsForgedTemporalFactorsAndOrder(t *testing.T) {
	config := testE1ConfigV2(E1ConcurrencyFifty, E1SettlementDelayTen)
	base, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	opportunities := make([]OpportunityV2, 0, 50)
	for index := int64(0); index < 50; index++ {
		opportunities = append(opportunities, testOpportunityV2(index, fmt.Sprintf("adversarial-%03d", index),
			base, base.Add(time.Second), base.Add(2*time.Second), base.Add(11*time.Second)))
	}

	assertReject := func(name string, mutateConfig func(*ConfigV2), mutateStream func([]OpportunityV2), fragment string) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			candidateConfig := config
			candidateStream := append([]OpportunityV2(nil), opportunities...)
			if mutateConfig != nil {
				mutateConfig(&candidateConfig)
			}
			if mutateStream != nil {
				mutateStream(candidateStream)
			}
			_, err := ValidateStreamV2(candidateStream, candidateConfig)
			if err == nil || (fragment != "" && !strings.Contains(err.Error(), fragment)) {
				t.Fatalf("forgery was accepted or misdiagnosed: %v", err)
			}
		})
	}

	assertReject("metadata-concurrency", func(c *ConfigV2) { c.Concurrency = E1ConcurrencyFiveHundred }, nil, "peak concurrency")
	assertReject("metadata-delay", func(c *ConfigV2) { c.SettlementDelaySeconds = E1SettlementDelaySixty }, nil, "settlement delay")
	assertReject("usage-before-response", nil, func(ops []OpportunityV2) {
		ops[0].UsageAvailableAt = base.Format(time.RFC3339Nano)
	}, "usage_available_at precedes")
	assertReject("response-before-arrival", nil, func(ops []OpportunityV2) {
		ops[0].ArrivalAt = base.Add(2 * time.Second).Format(time.RFC3339Nano)
	}, "provider_response_at precedes")
	assertReject("settlement-before-usage", nil, func(ops []OpportunityV2) {
		ops[0].SettlementAt = base.Add(time.Second).Format(time.RFC3339Nano)
	}, "settlement_at precedes")
	assertReject("noncanonical-time", nil, func(ops []OpportunityV2) {
		ops[0].ArrivalAt = strings.TrimSuffix(ops[0].ArrivalAt, "Z") + "+00:00"
	}, "canonical UTC")
	assertReject("arrival-regression", nil, func(ops []OpportunityV2) {
		ops[0].ArrivalAt = base.Add(time.Second).Format(time.RFC3339Nano)
	}, "regresses absolute arrival")
	assertReject("sequence-gap", nil, func(ops []OpportunityV2) { ops[1].Sequence = 2 }, "contiguous sequence")
	assertReject("duplicate-request", nil, func(ops []OpportunityV2) { ops[1].RequestID = ops[0].RequestID }, "duplicates request_id")
	assertReject("mutable-window-budget", nil, func(ops []OpportunityV2) { ops[1].BudgetMicros++ }, "immutable tenant-window budget")
}

func TestParseStreamV2RejectsUnknownFieldsAndV1EntryPointFailsClosed(t *testing.T) {
	config := testE1ConfigV2(E1ConcurrencyOne, E1SettlementDelayZero)
	base, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	opportunity := testOpportunityV2(0, "request", base, base, base, base)
	line, err := json.Marshal(opportunity)
	if err != nil {
		t.Fatal(err)
	}
	forged := append(append([]byte(nil), line[:len(line)-1]...), []byte(`,"unexpected":true}\n`)...)
	if _, _, err := ParseStreamV2(forged, config); err == nil {
		t.Fatal("unknown v2 opportunity field was accepted")
	}
	raw := marshalStreamV2(t, opportunity)
	v1Entry := config.runtimeConfig()
	v1Entry.SchemaVersion = ConfigSchemaV2
	if _, err := ParseStream(raw, v1Entry); err == nil || !strings.Contains(err.Error(), "ParseStreamV2") {
		t.Fatalf("v2 stream did not fail closed at the v1 entry point: %v", err)
	}
}

func TestParseStreamV2RejectsNonCanonicalJSONLEnvelopesAndForms(t *testing.T) {
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	opportunity := testOpportunityV2(0, "request-canonical-stream", base, base, base, base)
	raw := marshalStreamV2(t, opportunity)
	config := testE1ConfigV2(E1ConcurrencyOne, E1SettlementDelayZero)
	if _, _, err := ParseStreamV2(raw, config); err != nil {
		t.Fatalf("canonical stream fixture was rejected: %v", err)
	}

	withoutLF := append([]byte(nil), raw[:len(raw)-1]...)
	crlf := bytes.ReplaceAll(raw, []byte{'\n'}, []byte{'\r', '\n'})
	leadingSpace := append([]byte{' '}, raw...)
	interiorSpace := bytes.Replace(raw, []byte(`":"`), []byte(`": "`), 1)
	blankLine := append(append([]byte(nil), raw...), '\n')
	whitespaceLine := append(append([]byte(nil), raw...), ' ', '\n')
	escapedIdentity := bytes.Replace(raw, []byte("request-canonical-stream"), []byte(`\u0072equest-canonical-stream`), 1)

	for name, candidate := range map[string][]byte{
		"missing-final-lf": withoutLF,
		"crlf":             crlf,
		"leading-space":    leadingSpace,
		"interior-space":   interiorSpace,
		"blank-line":       blankLine,
		"whitespace-line":  whitespaceLine,
		"escaped-form":     escapedIdentity,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseStreamV2(candidate, config); err == nil {
				t.Fatal("non-canonical opportunity JSONL was accepted")
			}
		})
	}
}
