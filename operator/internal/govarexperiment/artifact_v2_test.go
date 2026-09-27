package govarexperiment

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func artifactInputsFixtureV2(t *testing.T) ArtifactInputsV2 {
	return artifactInputsFixtureForMethodV2(t, "fixed_quantile")
}

func artifactInputsFixtureForMethodV2(t *testing.T, method string) ArtifactInputsV2 {
	t.Helper()
	base := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	opportunity, usageRow := oneRunnerOpportunityV2("request-artifact-v2", base, 10*time.Second)
	if method == "gov_ar" {
		opportunity.CohortID = "cohort-artifact-v2"
		opportunity.CohortIndex = 0
	} else if method != "fixed_quantile" {
		t.Fatalf("unsupported artifact fixture method %q", method)
	}
	streamRaw := marshalStreamV2(t, opportunity)
	mappingRaw := marshalPreOutcomeMappingsV2(t, opportunity)
	usageRaw := marshalUsageV2(t, usageRow)
	setSHA, err := StreamPreOutcomeSetSHA256V2([]OpportunityV2{opportunity})
	if err != nil {
		t.Fatal(err)
	}
	sequenceSHA, err := StreamPreOutcomeSequenceSHA256V2([]OpportunityV2{opportunity})
	if err != nil {
		t.Fatal(err)
	}

	protocolRaw := fixtureEnvelopeRawV2(t, fixtureProtocolSchemaV2)
	common := testConfig("fixed_quantile")
	common.ProtocolSHA256 = SHA256(protocolRaw)
	common.SourceSHA256 = SourceRoot(common.SoftwareSHA256, common.DataSHA256, common.ProtocolSHA256, common.SplitSHA256)
	riskTarget := E1RiskTargetOnePercent
	if method == "gov_ar" {
		common.RegistryID = registryID("gov_ar")
		common.ProtocolMethodID = "gov_ar"
		common.ComparatorMethod = "gov_ar"
		common.ProductionReservationMode = "govar_fixed_cohort"
		common.EstimateOutputTokens = 0
		common.MarginOutputTokens = 0
		common.Cohorts = []CohortSpec{{
			TenantID: opportunity.TenantID, BudgetWindowID: opportunity.BudgetWindowID,
			CohortID: opportunity.CohortID, Size: 1,
		}}
		built, buildErr := BuildProspectiveGOVARMethodConfig(
			common, []Opportunity{projectOpportunityV2(opportunity)},
			int64(E1RiskTargetFivePercent), 100_000_000, 7200, 1,
			time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC),
			prospectiveEvidenceWithSupportV2(common.Cohorts, 19),
		)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		common.MethodConfig = built
		common.MethodConfigSHA256, buildErr = MethodConfigDigest(built)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		riskTarget = E1RiskTargetFivePercent
	}
	config := testConfigV2FromV1(common, E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne,
		SettlementDelaySeconds: E1SettlementDelayTen, BudgetLevel: E1BudgetMedium,
		RiskTargetPPB: riskTarget,
	})

	sourceCodeBindingRaw := mustPrettyArtifactV2(t, fixtureSourceCodeBindingV2{
		fixtureEnvelopeV2: fixtureEnvelopeV2{SchemaVersion: fixtureSourceCodeBindingSchemaV2, Status: "test_fixture",
			EvidenceLabel: fixtureEvidenceLabelV2, SupportMode: E1SupportModeFixture, TestOnly: true, OutcomeFieldsUsed: []string{}},
		ReviewedSourceRootSHA256: SHA256([]byte("reviewed test-fixture source root")),
	})
	sourceManifestRaw := mustPrettyArtifactV2(t, fixtureSourceManifestV2{
		SchemaVersion: fixtureSourceManifestSchemaV2, Status: "test_fixture", EvidenceLabel: fixtureEvidenceLabelV2,
		DatasetID: UsageDatasetV2, Split: UsageSplitV2, Rows: 1, OrderedPreOutcomeIDSHA256: sequenceSHA,
	})
	pilotBindingRaw := mustPrettyArtifactV2(t, fixturePilotBindingV2{
		SchemaVersion: fixturePilotUsageBindingSchemaV2, Status: "test_fixture", EvidenceLabel: fixtureEvidenceLabelV2,
		DatasetID: UsageDatasetV2, ParentSplit: "test_fixture", Subsplit: UsageSplitV2, Mode: "usage_only",
		SubsplitRows: 1, SubsplitPreOutcomeIDSHA256: sequenceSHA, SourceManifestSHA256: SHA256(sourceManifestRaw),
	})
	runtimeCapabilityRaw := mustPrettyArtifactV2(t, fixtureRuntimeCapabilityV2{
		fixtureEnvelopeV2: fixtureEnvelopeV2{SchemaVersion: fixtureRuntimeCapabilitySchemaV2, Status: "test_fixture",
			EvidenceLabel: fixtureEvidenceLabelV2, SupportMode: E1SupportModeFixture, TestOnly: true, OutcomeFieldsUsed: []string{}},
		SourceCodeBindingSHA256: SHA256(sourceCodeBindingRaw), FullCampaignSupported: false,
	})
	budgetCalibrationRaw := fixtureEnvelopeRawV2(t, budgetCalibrationArtifactSchemaV2)
	calibrationRaw := fixtureEnvelopeRawV2(t, calibrationArtifactSchemaV2)
	candidateSetRaw := fixtureEnvelopeRawV2(t, candidateSetArtifactSchemaV2)
	profileRegistryRaw := fixtureEnvelopeRawV2(t, profileRegistryArtifactSchemaV2)
	slotTemplateRaw := fixtureEnvelopeRawV2(t, slotTemplateArtifactSchemaV2)

	decisionTemplate := map[string]any{
		"schema_version": p1bDecisionConfigTemplateSchemaV2, "evidence_tier": EvidenceTierV2,
		"usage_dataset_id": UsageDatasetV2, "usage_split": UsageSplitV2, "virtual_start": config.VirtualStart,
		"method_templates":   map[string]any{config.ComparatorMethod: map[string]any{"method_config_sha256": config.MethodConfigSHA256}},
		"scenario_templates": map[string]any{config.Scenario: map[string]any{"concurrency": config.Concurrency, "settlement_delay_seconds": config.SettlementDelaySeconds}},
	}
	decisionTemplateRaw, err := json.Marshal(decisionTemplate)
	if err != nil {
		t.Fatal(err)
	}
	preOutcomeSidecarSHA := SHA256([]byte("test-only pre-outcome sidecar"))
	design := map[string]any{
		"schema_version": p1bDesignSpecSchemaV2, "status": "test_fixture", "evidence_label": fixtureEvidenceLabelV2,
		"dataset_id": UsageDatasetV2, "split": UsageSplitV2, "source_manifest_sha256": SHA256(sourceManifestRaw),
		"pilot_usage_binding_sha256": SHA256(pilotBindingRaw), "preoutcome_sidecar_sha256": preOutcomeSidecarSHA,
		"protocol_sha256": SHA256(protocolRaw), "decision_config_template": decisionTemplate,
		"decision_config_template_sha256": SHA256(decisionTemplateRaw),
	}
	designWithoutLock, err := json.Marshal(design)
	if err != nil {
		t.Fatal(err)
	}
	designLockSHA := SHA256(designWithoutLock)
	design["design_lock_sha256"] = designLockSHA
	designRaw, err := json.Marshal(design)
	if err != nil {
		t.Fatal(err)
	}
	reviewRaw := mustPrettyArtifactV2(t, p1bIndependentReviewV2{
		SchemaVersion: fixtureIndependentDesignReviewSchemaV2, Verdict: "test_fixture_only",
		ReviewScope: "outcome_free_pretrace_preparation_only", PretracePreparationAuthorized: true,
		DatasetID: UsageDatasetV2, Split: UsageSplitV2, DesignLockSHA256: designLockSHA,
		DecisionConfigTemplateSHA256: SHA256(decisionTemplateRaw), GeneratorSHA256: SHA256([]byte("fixture generator")),
		ProtocolSHA256: SHA256(protocolRaw), SourceManifestSHA256: SHA256(sourceManifestRaw),
		PilotUsageBindingSHA256: SHA256(pilotBindingRaw), PreOutcomeSidecarSHA256: preOutcomeSidecarSHA,
	})
	profileExecution := json.RawMessage(`{"profile_id":"fixture-profile","outcome_fields_used":[]}`)
	cohortRegistryRaw := mustPrettyArtifactV2(t, p1bCohortRegistryV2{
		SchemaVersion: p1bCohortRegistrySchemaV2, Scenario: config.Scenario, Seed: config.Seed,
		HierarchicalTenantWindowUnits: 1, EventsPerHierarchicalTenantWindow: 1,
		ProspectiveProfileExecution: profileExecution, Cohorts: append([]CohortSpec(nil), config.Cohorts...),
	})
	profileContract := map[string]any{
		"schema_version": p1bProfileExecutionContractSchemaV2, "status": "prospective_assignment_only",
		"scenario": config.Scenario, "seed": config.Seed, "full_campaign_execution_authorized": false,
		"final_evidence_authorized": false, "scientific_claims_authorized": false,
		"observed_output_distribution_claimed": false, "output_values_transformed": false,
		"pretrace_artifact_bindings": map[string]any{"mapping_artifact_sha256": SHA256(mappingRaw),
			"opportunity_stream_artifact_sha256": SHA256(streamRaw), "cohort_registry_artifact_sha256": SHA256(cohortRegistryRaw)},
		"outcome_fields_used": []string{},
	}
	profileContractRaw := mustPrettyArtifactV2(t, profileContract)

	projectionRaw, err := json.Marshal(projectExecutionIntentConfigV2(config))
	if err != nil {
		t.Fatal(err)
	}
	bundleRaw := mustPrettyArtifactV2(t, MethodBuilderBundleV2{
		SchemaVersion: E1MethodBuilderBundleSchemaV2, Status: "test_fixture", EvidenceLabel: fixtureEvidenceLabelV2,
		SupportMode: E1SupportModeFixture, TestOnly: true, BuilderKind: "fixture_exact_method_config",
		ComparatorMethod: config.ComparatorMethod, BuilderConfigSHA256: DomainHash("govar-e1-builder-config-v1", projectionRaw),
		MethodConfig: config.MethodConfig, MethodConfigSHA256: config.MethodConfigSHA256,
		BudgetCalibrationSHA256:   SHA256(budgetCalibrationRaw),
		CalibrationArtifactSHA256: SHA256(calibrationRaw), CandidateSetSHA256: SHA256(candidateSetRaw),
		ProfileRegistrySHA256: SHA256(profileRegistryRaw), SlotTemplateSHA256: SHA256(slotTemplateRaw), OutcomeFieldsUsed: []string{},
	})

	streamKey := config.Scenario + "/seed-17"
	streamManifest := p1bStreamManifestV2{
		StreamKey: streamKey, Scenario: config.Scenario, Seed: config.Seed,
		ScenarioFactorBinding: json.RawMessage(`{"fixture":true}`), Rows: 1,
		HierarchicalTenantWindowUnits: 1, EventsPerHierarchicalTenantWindow: 1,
		AssignedPreOutcomeSetSHA256: setSHA, StreamPreOutcomeSequenceSHA256: sequenceSHA,
		WindowAssignmentSHA256:       SHA256([]byte("fixture window assignment")),
		DecisionConfigTemplateSHA256: SHA256(decisionTemplateRaw), ProspectiveProfileExecution: profileExecution,
		MappingArtifact:                  p1bArtifactSpecV2{Path: "delay/seed-17/preoutcome_mapping.jsonl", SHA256: SHA256(mappingRaw), Bytes: int64(len(mappingRaw)), Rows: 1},
		OpportunityStreamArtifact:        p1bArtifactSpecV2{Path: "delay/seed-17/opportunities.jsonl", SHA256: SHA256(streamRaw), Bytes: int64(len(streamRaw)), Rows: 1},
		CohortRegistryArtifact:           p1bArtifactSpecV2{Path: "delay/seed-17/cohort_registry.json", SHA256: SHA256(cohortRegistryRaw), Bytes: int64(len(cohortRegistryRaw)), Rows: 1},
		ProfileExecutionContractArtifact: p1bArtifactSpecV2{Path: "delay/seed-17/profile_execution_contract.json", SHA256: SHA256(profileContractRaw), Bytes: int64(len(profileContractRaw))},
		OutcomeFieldsUsed:                []string{},
	}
	designManifest := map[string]any{
		"schema_version": p1bDesignManifestSchemaV2, "status": "test_fixture", "evidence_label": fixtureEvidenceLabelV2,
		"final_evidence_eligible": false, "scientific_claims_authorized": false, "full_p1b_execution_authorized": false,
		"test_only_fixture_mode": true, "dataset_id": UsageDatasetV2, "split": UsageSplitV2,
		"source_manifest_sha256": SHA256(sourceManifestRaw), "pilot_usage_binding_sha256": SHA256(pilotBindingRaw),
		"protocol_sha256": SHA256(protocolRaw), "design_spec_sha256": SHA256(designRaw),
		"design_lock_sha256": designLockSHA, "independent_design_review_sha256": SHA256(reviewRaw),
		"decision_config_template_sha256": SHA256(decisionTemplateRaw), "streams": []p1bStreamManifestV2{streamManifest},
		"outcome_fields_used": []string{},
	}
	manifestPayload, err := json.Marshal(designManifest)
	if err != nil {
		t.Fatal(err)
	}
	var manifestObject map[string]json.RawMessage
	if err := json.Unmarshal(manifestPayload, &manifestObject); err != nil {
		t.Fatal(err)
	}
	designManifest["manifest_payload_sha256"] = canonicalObjectDigestWithoutFieldV2(
		manifestObject, "manifest_payload_sha256",
	)
	designManifestRaw := mustPrettyArtifactV2(t, designManifest)

	intentRaw := mustPrettyArtifactV2(t, ExecutionIntentV2{
		SchemaVersion: E1ExecutionIntentSchemaV2, Status: "test_fixture", EvidenceLabel: fixtureEvidenceLabelV2,
		SupportMode: E1SupportModeFixture, TestOnly: true, StreamKey: streamKey, Config: projectExecutionIntentConfigV2(config),
		OpportunityStreamSHA256: SHA256(streamRaw), MappingArtifactSHA256: SHA256(mappingRaw),
		CohortRegistrySHA256: SHA256(cohortRegistryRaw), ProfileExecutionContractSHA256: SHA256(profileContractRaw),
		BudgetCalibrationSHA256:   SHA256(budgetCalibrationRaw),
		CalibrationArtifactSHA256: SHA256(calibrationRaw), CandidateSetSHA256: SHA256(candidateSetRaw),
		ProfileRegistrySHA256: SHA256(profileRegistryRaw), SlotTemplateSHA256: SHA256(slotTemplateRaw),
		MethodBuilderBundleSHA256: SHA256(bundleRaw), DesignManifestSHA256: SHA256(designManifestRaw),
		DecisionConfigTemplateSHA256: SHA256(decisionTemplateRaw), OutcomeFieldsUsed: []string{},
	})

	lock := testProvenanceLockV2()
	lock.SupportMode = E1SupportModeFixture
	lock.OpportunityStreamSHA256 = SHA256(streamRaw)
	lock.SourceCodeBindingSHA256 = SHA256(sourceCodeBindingRaw)
	lock.SourceManifestSHA256 = SHA256(sourceManifestRaw)
	lock.PilotUsageBindingSHA256 = SHA256(pilotBindingRaw)
	lock.RuntimeCapabilitySHA256 = SHA256(runtimeCapabilityRaw)
	lock.MappingArtifactSHA256 = SHA256(mappingRaw)
	lock.PilotPreOutcomeSetSHA256 = sequenceSHA
	lock.AssignedPreOutcomeSetSHA256 = setSHA
	lock.StreamPreOutcomeSequenceSHA256 = sequenceSHA
	lock.ProtocolSHA256 = SHA256(protocolRaw)
	lock.DesignSpecSHA256 = SHA256(designRaw)
	lock.DecisionConfigTemplateSHA256 = SHA256(decisionTemplateRaw)
	lock.DesignLockSHA256 = designLockSHA
	lock.IndependentDesignReviewSHA256 = SHA256(reviewRaw)
	lock.DesignManifestSHA256 = SHA256(designManifestRaw)
	lock.CohortRegistrySHA256 = SHA256(cohortRegistryRaw)
	lock.ProfileExecutionContractSHA256 = SHA256(profileContractRaw)
	lock.BudgetCalibrationSHA256 = SHA256(budgetCalibrationRaw)
	lock.CalibrationArtifactSHA256 = SHA256(calibrationRaw)
	lock.CandidateSetSHA256 = SHA256(candidateSetRaw)
	lock.ProfileRegistrySHA256 = SHA256(profileRegistryRaw)
	lock.SlotTemplateSHA256 = SHA256(slotTemplateRaw)
	lock.MethodBuilderBundleSHA256 = SHA256(bundleRaw)
	lock.ExecutionIntentSHA256 = SHA256(intentRaw)
	lock = sealTestProvenanceLockV2(lock)
	authorityRaw := mustPrettyArtifactV2(t, IndependentAuthorizationV2{
		SchemaVersion: fixtureIndependentAuthorizationSchemaV2, Verdict: "test_fixture_only",
		SupportMode: E1SupportModeFixture, EvidenceLabel: fixtureEvidenceLabelV2, AuthorizationContext: "fixture-context-001",
		TestOnly: true, DatasetID: UsageDatasetV2, Split: UsageSplitV2, ExperimentID: config.ExperimentID,
		RunID: config.RunID, CellID: config.CellID, Scenario: config.Scenario, Seed: config.Seed, StreamKey: streamKey,
		ExecutionSubjectSHA256: lock.ExecutionSubjectSHA256, ExecutionIntentSHA256: lock.ExecutionIntentSHA256,
		ProtocolSHA256: lock.ProtocolSHA256, SourceCodeBindingSHA256: lock.SourceCodeBindingSHA256,
		SourceManifestSHA256: lock.SourceManifestSHA256, PilotUsageBindingSHA256: lock.PilotUsageBindingSHA256,
		RuntimeCapabilitySHA256: lock.RuntimeCapabilitySHA256, DesignSpecSHA256: lock.DesignSpecSHA256,
		DesignLockSHA256: lock.DesignLockSHA256, IndependentDesignReviewSHA256: lock.IndependentDesignReviewSHA256,
		DesignManifestSHA256: lock.DesignManifestSHA256, CohortRegistrySHA256: lock.CohortRegistrySHA256,
		ProfileExecutionContractSHA256: lock.ProfileExecutionContractSHA256,
		BudgetCalibrationSHA256:        lock.BudgetCalibrationSHA256,
		CalibrationArtifactSHA256:      lock.CalibrationArtifactSHA256, CandidateSetSHA256: lock.CandidateSetSHA256,
		ProfileRegistrySHA256: lock.ProfileRegistrySHA256, SlotTemplateSHA256: lock.SlotTemplateSHA256,
		MethodBuilderBundleSHA256: lock.MethodBuilderBundleSHA256,
	})
	lock.IndependentAuthorizationSHA256 = SHA256(authorityRaw)
	lock = sealTestProvenanceLockV2(lock)
	config.ProvenanceLock = lock
	evidence := UsageProducerEvidenceV2{
		SchemaVersion: UsageProducerSchemaV2, DatasetID: UsageDatasetV2, Split: UsageSplitV2,
		ProvenanceLock: lock, ProducerSoftwareSHA256: strings.Repeat("4", 64), UsageArtifactSHA256: SHA256(usageRaw),
	}
	evidenceRaw := mustPrettyArtifactV2(t, evidence)
	issuer, err := ParseDevelopmentUsageV2(usageRaw, mappingRaw, evidence)
	if err != nil {
		t.Fatal(err)
	}
	config.ProvenanceLock = lock
	config.UsageBindingSHA256 = issuer.Binding().BindingSHA256
	return ArtifactInputsV2{
		SupportMode: E1SupportModeFixture, ConfigRaw: marshalConfigV2(t, config), StreamRaw: streamRaw,
		PreOutcomeMappingRaw: mappingRaw, DevelopmentUsageRaw: usageRaw,
		ProducerEvidenceRaw: evidenceRaw, ProtocolRaw: protocolRaw, SourceCodeBindingRaw: sourceCodeBindingRaw,
		SourceManifestRaw: sourceManifestRaw, PilotUsageBindingRaw: pilotBindingRaw, RuntimeCapabilityRaw: runtimeCapabilityRaw,
		DesignSpecRaw: designRaw, DecisionConfigTemplateRaw: decisionTemplateRaw, IndependentDesignReviewRaw: reviewRaw,
		DesignManifestRaw: designManifestRaw, CohortRegistryRaw: cohortRegistryRaw, ProfileExecutionContractRaw: profileContractRaw,
		BudgetCalibrationRaw:   budgetCalibrationRaw,
		CalibrationArtifactRaw: calibrationRaw, CandidateSetRaw: candidateSetRaw, ProfileRegistryRaw: profileRegistryRaw,
		SlotTemplateRaw: slotTemplateRaw, MethodBuilderBundleRaw: bundleRaw, ExecutionIntentRaw: intentRaw,
		IndependentAuthorizationRaw: authorityRaw,
	}
}

func fixtureEnvelopeRawV2(t *testing.T, schema string) []byte {
	t.Helper()
	return mustPrettyArtifactV2(t, fixtureEnvelopeV2{SchemaVersion: schema, Status: "test_fixture",
		EvidenceLabel: fixtureEvidenceLabelV2, SupportMode: E1SupportModeFixture, TestOnly: true, OutcomeFieldsUsed: []string{}})
}

func mustPrettyArtifactV2(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := artifactPrettyJSONV2(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestWriteArtifactsV2CreateOnceDeterministicAndSelfVerifying(t *testing.T) {
	inputs := artifactInputsFixtureV2(t)
	firstDirectory := t.TempDir()
	first, err := WriteArtifactsV2(firstDirectory, inputs)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyArtifactManifestV2(first.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, first.Manifest) || first.Manifest.FinalEvidenceEligible ||
		first.Manifest.EvidenceLabel != fixtureEvidenceLabelV2 || first.Completion.SelfVerification != "passed" {
		t.Fatalf("wrong non-final artifact result: manifest=%+v completion=%+v", first.Manifest, first.Completion)
	}
	manifestRaw, err := os.ReadFile(first.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	completionRaw, err := os.ReadFile(first.CompletionPath)
	if err != nil {
		t.Fatal(err)
	}
	if first.Manifest.QualityAvailable || first.Manifest.ComparativeEffectEstimationPermitted ||
		first.Manifest.PValuesPermitted || first.Manifest.ClaimLabelsPermitted {
		t.Fatal("qualification manifest permits unavailable analysis")
	}
	for _, forbidden := range [][]byte{[]byte(`"metrics"`), []byte(`"effect_estimate"`), []byte(`"quality_metric"`), []byte(`"p_value_computed"`), []byte(`"claim_label_assigned"`)} {
		if bytes.Contains(bytes.ToLower(manifestRaw), forbidden) || bytes.Contains(bytes.ToLower(completionRaw), forbidden) {
			t.Fatalf("manifest/completion contains forbidden analysis field %q", forbidden)
		}
	}

	secondDirectory := t.TempDir()
	second, err := WriteArtifactsV2(secondDirectory, inputs)
	if err != nil {
		t.Fatal(err)
	}
	firstNames := namesForRunV2(first.Manifest.RunID)
	for _, name := range firstNames.allWithCompletion() {
		left, err := os.ReadFile(filepath.Join(firstDirectory, name))
		if err != nil {
			t.Fatal(err)
		}
		right, err := os.ReadFile(filepath.Join(secondDirectory, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(left, right) {
			t.Fatalf("artifact %s is not deterministic", name)
		}
	}
	if second.Manifest.ArtifactSetRootSHA256 != first.Manifest.ArtifactSetRootSHA256 {
		t.Fatal("deterministic artifact roots differ")
	}
	if _, err := WriteArtifactsV2(firstDirectory, inputs); err == nil || !strings.Contains(err.Error(), "create-once") {
		t.Fatalf("artifact overwrite was not rejected: %v", err)
	}
}

func TestWriteArtifactsV2FailureBeforeAtomicPublicationIsRecoverable(t *testing.T) {
	inputs := artifactInputsFixtureV2(t)
	output := t.TempDir()
	injected := errors.New("injected failure before atomic publication")
	var stagingPath string
	_, err := writeArtifactsV2WithBeforePublish(output, inputs, func(stagingDirectory string) error {
		stagingPath = stagingDirectory
		if _, statErr := os.Stat(filepath.Join(stagingDirectory, ArtifactCompletionFileNameV2)); statErr != nil {
			return errors.Join(injected, errors.New("completed staging set was not present"), statErr)
		}
		if _, statErr := os.Lstat(filepath.Join(output, ArtifactCompletionFileNameV2)); !os.IsNotExist(statErr) {
			return errors.Join(injected, fmt.Errorf("partial final COMPLETED marker became visible: %v", statErr))
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("failure injection did not stop before publication: %v", err)
	}
	if stagingPath == "" {
		t.Fatal("failure injection did not observe a staging directory")
	}
	if _, err := os.Lstat(filepath.Join(output, ArtifactCompletionFileNameV2)); !os.IsNotExist(err) {
		t.Fatalf("failed attempt exposed a final COMPLETED marker: %v", err)
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed attempt exposed partial final artifacts: %v", entries)
	}
	if _, err := os.Lstat(stagingPath); !os.IsNotExist(err) {
		t.Fatalf("ordinary failure did not clean its staging directory: %v", err)
	}

	// Model process-crash debris, including a misleading terminal-looking file.
	// The next lock owner must remove the reserved staging directory rather than
	// treating any bytes in it as published evidence.
	if err := os.Mkdir(stagingPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stagingPath, ArtifactCompletionFileNameV2), []byte("partial\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := WriteArtifactsV2(output, inputs)
	if err != nil {
		t.Fatalf("identical retry was blocked by stale staging debris: %v", err)
	}
	if _, err := VerifyArtifactManifestV2(result.ManifestPath); err != nil {
		t.Fatalf("retried atomic artifact set did not verify: %v", err)
	}
	if _, err := os.Lstat(stagingPath); !os.IsNotExist(err) {
		t.Fatalf("stale staging directory survived successful retry: %v", err)
	}
}

func TestWriteArtifactsV2ConcurrentCollisionFailsClosed(t *testing.T) {
	inputs := artifactInputsFixtureV2(t)
	output := t.TempDir()
	start := make(chan struct{})
	staged := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWriter := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWriter()
	type result struct {
		artifact ArtifactResultV2
		err      error
	}
	results := make(chan result, 2)
	hook := func(string) error {
		staged <- struct{}{}
		<-release
		return nil
	}
	var workers sync.WaitGroup
	workers.Add(2)
	for range 2 {
		go func() {
			defer workers.Done()
			<-start
			artifact, err := writeArtifactsV2WithBeforePublish(output, inputs, hook)
			results <- result{artifact: artifact, err: err}
		}()
	}
	close(start)
	select {
	case <-staged:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent writer did not reach the pre-publication barrier")
	}
	if _, err := os.Lstat(filepath.Join(output, ArtifactCompletionFileNameV2)); !os.IsNotExist(err) {
		t.Fatalf("in-progress writer exposed COMPLETED before atomic publication: %v", err)
	}
	var collision result
	select {
	case collision = <-results:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent collision did not fail closed while the winner held the publication lock")
	}
	if collision.err == nil || !strings.Contains(collision.err.Error(), "create-once") {
		t.Fatalf("concurrent collision was not rejected fail-closed: %v", collision.err)
	}
	releaseWriter()
	var winner result
	select {
	case winner = <-results:
	case <-time.After(10 * time.Second):
		t.Fatal("winning writer did not finish after publication was released")
	}
	workers.Wait()
	if winner.err != nil {
		t.Fatalf("single lock owner failed to publish: %v", winner.err)
	}
	if _, err := VerifyArtifactManifestV2(winner.artifact.ManifestPath); err != nil {
		t.Fatalf("concurrent winner did not publish a valid artifact set: %v", err)
	}
}

func TestWriteArtifactsV2LeavesExactCellParentTree(t *testing.T) {
	inputs := artifactInputsFixtureV2(t)
	parent := t.TempDir()
	output := filepath.Join(parent, "planned-cell")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatal(err)
	}
	result, err := WriteArtifactsV2(output, inputs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyArtifactManifestV2(result.ManifestPath); err != nil {
		t.Fatalf("published exact-tree artifact set did not verify: %v", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "planned-cell" || !entries[0].IsDir() {
		t.Fatalf("publication left an artifact outside the planned cell: %v", entries)
	}
}

// TestArtifactV2CrossLanguageFixture is opt-in so the independent Python raw
// verifier can consume bytes emitted by the real Go writer without exposing a
// synthetic fixture constructor in the production API.
func TestArtifactV2CrossLanguageFixture(t *testing.T) {
	output := os.Getenv("ARTICLE3_CROSS_FIXTURE_OUTPUT")
	if output == "" {
		t.Skip("ARTICLE3_CROSS_FIXTURE_OUTPUT is not set")
	}
	method := os.Getenv("ARTICLE3_CROSS_FIXTURE_METHOD")
	if method == "" {
		method = "fixed_quantile"
	}
	if _, err := WriteArtifactsV2(output, artifactInputsFixtureForMethodV2(t, method)); err != nil {
		t.Fatal(err)
	}
}

func TestWriteArtifactsV2RejectsDuplicateUnknownAndBadLockedInputs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ArtifactInputsV2)
	}{
		{
			name: "duplicate-config-key",
			mutate: func(inputs *ArtifactInputsV2) {
				trimmed := bytes.TrimSpace(inputs.ConfigRaw)
				inputs.ConfigRaw = append(append([]byte(nil), trimmed[:len(trimmed)-1]...), []byte(`,"run_id":"duplicate"}`)...)
			},
		},
		{
			name: "unknown-producer-evidence-key",
			mutate: func(inputs *ArtifactInputsV2) {
				trimmed := bytes.TrimSpace(inputs.ProducerEvidenceRaw)
				inputs.ProducerEvidenceRaw = append(append([]byte(nil), trimmed[:len(trimmed)-1]...), []byte(`,"unknown":true}`)...)
			},
		},
		{
			name: "wrong-source-manifest-bytes",
			mutate: func(inputs *ArtifactInputsV2) {
				inputs.SourceManifestRaw = append(append([]byte(nil), inputs.SourceManifestRaw...), ' ')
			},
		},
		{
			name: "wrong-pilot-preoutcome-set",
			mutate: func(inputs *ArtifactInputsV2) {
				var pilot fixturePilotBindingV2
				if err := json.Unmarshal(inputs.PilotUsageBindingRaw, &pilot); err != nil {
					panic(err)
				}
				pilot.SubsplitPreOutcomeIDSHA256 = strings.Repeat("6", 64)
				inputs.PilotUsageBindingRaw, _ = artifactPrettyJSONV2(pilot)
				var config ConfigV2
				if err := json.Unmarshal(inputs.ConfigRaw, &config); err != nil {
					panic(err)
				}
				config.ProvenanceLock.PilotUsageBindingSHA256 = SHA256(inputs.PilotUsageBindingRaw)
				config.ProvenanceLock = sealTestProvenanceLockV2(config.ProvenanceLock)
				inputs.ConfigRaw, _ = artifactPrettyJSONV2(config)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := artifactInputsFixtureV2(t)
			test.mutate(&inputs)
			directory := t.TempDir()
			if _, err := WriteArtifactsV2(directory, inputs); err == nil {
				t.Fatal("invalid artifact input was accepted")
			}
			if _, err := os.Lstat(filepath.Join(directory, ArtifactCompletionFileNameV2)); !os.IsNotExist(err) {
				t.Fatalf("invalid run published a completion marker: %v", err)
			}
		})
	}
}

func TestWriteArtifactsV2AfterPreflightRejectsAuthorizationBeforeProtectedRead(t *testing.T) {
	inputs := artifactInputsFixtureV2(t)
	preflight := artifactPreflightInputsFromV2(inputs)
	preflight.IndependentAuthorizationRaw = append(append([]byte(nil), preflight.IndependentAuthorizationRaw...), ' ')
	protectedReads := 0
	output := t.TempDir()
	_, err := WriteArtifactsV2AfterPreflight(output, preflight, func() (ArtifactProtectedInputsV2, error) {
		protectedReads++
		return ArtifactProtectedInputsV2{DevelopmentUsageRaw: inputs.DevelopmentUsageRaw, ProducerEvidenceRaw: inputs.ProducerEvidenceRaw}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "authorization") {
		t.Fatalf("invalid authorization did not fail the staged preflight: %v", err)
	}
	if protectedReads != 0 {
		t.Fatalf("protected reader was invoked %d times before authorization", protectedReads)
	}
	if entries, readErr := os.ReadDir(output); readErr != nil || len(entries) != 0 {
		t.Fatalf("invalid preflight unexpectedly published an artifact: entries=%v err=%v", entries, readErr)
	}
}

func TestWriteArtifactsV2AfterPreflightLocksBeforeProtectedRead(t *testing.T) {
	inputs := artifactInputsFixtureV2(t)
	preflight := artifactPreflightInputsFromV2(inputs)
	parent := t.TempDir()
	output := filepath.Join(parent, "planned-cell")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		_, err := WriteArtifactsV2AfterPreflight(
			output, preflight, func() (ArtifactProtectedInputsV2, error) {
				close(entered)
				<-release
				return ArtifactProtectedInputsV2{
					DevelopmentUsageRaw: inputs.DevelopmentUsageRaw,
					ProducerEvidenceRaw: inputs.ProducerEvidenceRaw,
				}, nil
			},
		)
		firstResult <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first writer did not reach the protected reader")
	}
	secondProtectedReads := 0
	_, secondErr := WriteArtifactsV2AfterPreflight(
		output, preflight, func() (ArtifactProtectedInputsV2, error) {
			secondProtectedReads++
			return ArtifactProtectedInputsV2{}, nil
		},
	)
	if secondErr == nil || !strings.Contains(secondErr.Error(), "create-once") {
		t.Fatalf("concurrent staged writer did not fail before reveal: %v", secondErr)
	}
	if secondProtectedReads != 0 {
		t.Fatalf("colliding writer opened protected usage %d times", secondProtectedReads)
	}
	close(release)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("lock-owning writer failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("lock-owning writer did not finish")
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 1 || entries[0].Name() != "planned-cell" {
		t.Fatalf("staged execution left an unplanned parent artifact: %v, %v", entries, err)
	}
}

func TestP1bExactStagedWriterRejectsExternalFinalConfigBeforeProtectedRead(t *testing.T) {
	protectedReads := 0
	_, err := WriteArtifactsV2AfterPreflight(t.TempDir(), ArtifactPreflightInputsV2{
		SupportMode: E1SupportModeP1bExact,
		ConfigRaw:   []byte(`{"externally_supplied":true}`),
	}, func() (ArtifactProtectedInputsV2, error) {
		protectedReads++
		return ArtifactProtectedInputsV2{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "externally supplied ConfigRaw") {
		t.Fatalf("p1b_exact external final config was not rejected: %v", err)
	}
	if protectedReads != 0 {
		t.Fatalf("protected reader was invoked %d times for an external p1b_exact config", protectedReads)
	}
}

// TestP1bExactRejectsEmptyInputsBeforeProtectedRead pins the ordering that the
// closed latch used to provide: whatever rejects an invalid p1b_exact request
// must reject it before the protected usage reader is ever invoked. With the
// latch open for the development pilot, the empty-input rejection now comes
// from the exact validators instead, and the ordering guarantee must survive.
func TestP1bExactRejectsEmptyInputsBeforeProtectedRead(t *testing.T) {
	protectedReads := 0
	_, err := WriteArtifactsV2AfterPreflight(t.TempDir(), ArtifactPreflightInputsV2{
		SupportMode: E1SupportModeP1bExact,
	}, func() (ArtifactProtectedInputsV2, error) {
		protectedReads++
		return ArtifactProtectedInputsV2{}, nil
	})
	if err == nil {
		t.Fatal("empty p1b_exact inputs were accepted")
	}
	if protectedReads != 0 {
		t.Fatalf("protected reader was invoked %d times before p1b_exact inputs validated", protectedReads)
	}
}

func TestP1bExactDirectWriterIsClosed(t *testing.T) {
	_, err := WriteArtifactsV2(t.TempDir(), ArtifactInputsV2{SupportMode: E1SupportModeP1bExact})
	if err == nil || !strings.Contains(err.Error(), "requires WriteArtifactsV2AfterPreflight") {
		t.Fatalf("direct p1b_exact protected-input writer was not closed: %v", err)
	}
}

func TestP1bExactMaterializationBuildsEvidenceBindingAndConfigInternally(t *testing.T) {
	fixture := artifactInputsFixtureV2(t)
	var config ConfigV2
	if err := json.Unmarshal(fixture.ConfigRaw, &config); err != nil {
		t.Fatal(err)
	}
	config.ProvenanceLock.SupportMode = E1SupportModeP1bExact
	config.ProvenanceLock = sealTestProvenanceLockV2(config.ProvenanceLock)
	config.UsageBindingSHA256 = strings.Repeat("0", 64)

	base := ArtifactInputsV2{
		SupportMode:          E1SupportModeP1bExact,
		PreOutcomeMappingRaw: fixture.PreOutcomeMappingRaw,
		DevelopmentUsageRaw:  fixture.DevelopmentUsageRaw,
	}
	materialized, err := materializeP1bExactProtectedInputsV2(artifactPreflightResultV2{config: config}, base)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := ParseUsageProducerEvidenceV2(materialized.ProducerEvidenceRaw)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.ProvenanceLock != config.ProvenanceLock || evidence.ProducerSoftwareSHA256 != config.SoftwareSHA256 ||
		evidence.UsageArtifactSHA256 != SHA256(fixture.DevelopmentUsageRaw) {
		t.Fatalf("internally materialized producer evidence differs: %+v", evidence)
	}
	issuer, err := ParseDevelopmentUsageV2(fixture.DevelopmentUsageRaw, fixture.PreOutcomeMappingRaw, evidence)
	if err != nil {
		t.Fatal(err)
	}
	finalConfig, _, err := ParseConfigV2(materialized.ConfigRaw)
	if err != nil {
		t.Fatal(err)
	}
	if finalConfig.ProvenanceLock != config.ProvenanceLock || finalConfig.UsageBindingSHA256 != issuer.Binding().BindingSHA256 {
		t.Fatalf("internally materialized config is not bound to exact usage: %+v", finalConfig)
	}

	base.ProducerEvidenceRaw = []byte(`{"externally_supplied":true}`)
	if _, err := materializeP1bExactProtectedInputsV2(artifactPreflightResultV2{config: config}, base); err == nil ||
		!strings.Contains(err.Error(), "externally supplied ProducerEvidenceRaw") {
		t.Fatalf("external p1b_exact producer evidence was not rejected: %v", err)
	}
}

func TestArtifactV2AcceptsCanonicalEmptyUsageAccessLog(t *testing.T) {
	events, err := decodeUsageAccessJSONLV2(nil)
	if err != nil || len(events) != 0 {
		t.Fatalf("canonical empty access log was rejected: events=%v err=%v", events, err)
	}
	if _, err := decodeUsageAccessJSONLV2([]byte("\n")); err == nil {
		t.Fatal("non-canonical blank access log was accepted")
	}
}

func TestWriteAndVerifyArtifactsV2RejectSymlinksAndTampering(t *testing.T) {
	inputs := artifactInputsFixtureV2(t)
	realDirectory := t.TempDir()
	symlinkParent := t.TempDir()
	symlinkDirectory := filepath.Join(symlinkParent, "output-link")
	if err := os.Symlink(realDirectory, symlinkDirectory); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := WriteArtifactsV2(symlinkDirectory, inputs); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink output directory was accepted: %v", err)
	}

	directory := t.TempDir()
	result, err := WriteArtifactsV2(directory, inputs)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(result.RawPath, 0o644); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(result.RawPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0}); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyArtifactManifestV2(result.ManifestPath); err == nil {
		t.Fatal("tampered lifecycle was accepted")
	}

	inputFile := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(inputFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inputLink := inputFile + ".link"
	if err := os.Symlink(inputFile, inputLink); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRegularFileNoSymlinkV2(inputLink); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink input was accepted: %v", err)
	}
	traversalPath := filepath.Dir(inputFile) + string(os.PathSeparator) + ".." + string(os.PathSeparator) +
		filepath.Base(filepath.Dir(inputFile)) + string(os.PathSeparator) + filepath.Base(inputFile)
	if _, err := ReadRegularFileNoSymlinkV2(traversalPath); err == nil || !strings.Contains(err.Error(), "traversal") {
		t.Fatalf("traversal input was accepted: %v", err)
	}
}
