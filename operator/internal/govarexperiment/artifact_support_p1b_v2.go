package govarexperiment

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	p1bPretraceGeneratorSHA256V2    = "692294672bb034e4f7b491bf5d319b4345aa85e1a17fbcf87cdd31035e72fba5"
	p1bPretraceArtifactRootDomainV2 = "govar-p1b-pretrace-artifact-set-root-v1"
	// Release latch, not a runtime option. Opened for the P1b full development
	// pilot once the exact support schemas were frozen (one stream-bound slot
	// template, calibration artifact and profile registry per stream, giving
	// 145 pretrace artifacts), the cross-language contract was made consistent
	// between prepare_p1b_design, experiment_raw_v2 and this validator, and the
	// assembled 100-cell package validated end to end.
	//
	// Everything this admits remains development_non_citable: it authorizes no
	// final evidence and no scientific claim. Final E1 still requires a frozen
	// protocol and the strict Q1 gate.
	p1bExactProductionReleaseReadyV2 = true
)

var p1bCommitPatternV2 = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)

type p1bScenarioFactorBindingV2 struct {
	Concurrency                   E1Concurrency            `json:"concurrency"`
	SettlementDelaySeconds        E1SettlementDelaySeconds `json:"settlement_delay_seconds"`
	BudgetLevel                   E1BudgetLevel            `json:"budget_level"`
	BudgetMicros                  int64                    `json:"budget_micros"`
	BudgetAssignmentMode          string                   `json:"budget_assignment_mode"`
	TenantBudgetScheduleSHA256    *string                  `json:"tenant_budget_schedule_sha256"`
	DistinctTenantBudgetLevels    int64                    `json:"distinct_tenant_budget_levels"`
	ProspectiveUsageProfileSHA256 string                   `json:"prospective_usage_profile_sha256"`
	DecisionConfigTemplateSHA256  string                   `json:"decision_config_template_sha256"`
}

type p1bSelectionContractV2 struct {
	ConditioningPopulation           string   `json:"conditioning_population"`
	ConditioningScope                string   `json:"conditioning_scope"`
	PoolSelectionAlgorithm           string   `json:"pool_selection_algorithm"`
	PoolSelectionDomain              string   `json:"pool_selection_domain"`
	PoolSelectionHashInput           string   `json:"pool_selection_hash_input"`
	PairedAcrossScenarios            bool     `json:"paired_across_scenarios"`
	ScenarioPoolRule                 string   `json:"scenario_pool_rule"`
	SeedPartitionAlgorithm           string   `json:"seed_partition_algorithm"`
	SeedPartitionRule                string   `json:"seed_partition_rule"`
	SeedBlockRule                    string   `json:"seed_block_rule"`
	StreamOrderAlgorithm             string   `json:"stream_order_algorithm"`
	StreamOrderDomain                string   `json:"stream_order_domain"`
	StreamOrderHashInput             string   `json:"stream_order_hash_input"`
	WindowAssignmentAlgorithm        string   `json:"window_assignment_algorithm"`
	WindowAssignmentDomain           string   `json:"window_assignment_domain"`
	WindowAssignmentHashInput        string   `json:"window_assignment_hash_input"`
	SourceAssignmentIDDomain         string   `json:"source_assignment_id_domain"`
	SourceAssignmentIDHashInput      string   `json:"source_assignment_id_hash_input"`
	SourceAssignmentIDExcludedFields []string `json:"source_assignment_id_excluded_fields"`
	WindowIndexRule                  string   `json:"window_index_rule"`
	CohortIndexRule                  string   `json:"cohort_index_rule"`
	StreamRestorationRule            string   `json:"stream_restoration_rule"`
	OutcomeFieldsForbidden           []string `json:"outcome_fields_forbidden"`
}

func expectedP1bSelectionContractV2() p1bSelectionContractV2 {
	return p1bSelectionContractV2{
		ConditioningPopulation:           "validated_preoutcome_sidecar_rows",
		ConditioningScope:                "P1b randomization is covariate-invariant conditional on the committed sidecar; the upstream development subsplit is a separate pre-P1b design stage",
		PoolSelectionAlgorithm:           "global_paired_sort_sha256_then_source_assignment_id_take_first_n_v2",
		PoolSelectionDomain:              "govar-p1b-global-paired-pool-source-unit-v2",
		PoolSelectionHashInput:           "domain_utf8 || NUL || lower_hex_source_assignment_id_ascii",
		PairedAcrossScenarios:            true,
		ScenarioPoolRule:                 "one global pool reused unchanged by all four scenarios",
		SeedPartitionAlgorithm:           "global_pool_rank_mod_seed_stream_count_v1",
		SeedPartitionRule:                "seed_index = global_pool_rank % seed_streams",
		SeedBlockRule:                    "each seed receives the same source-unit set in every scenario",
		StreamOrderAlgorithm:             "sort_sha256_then_source_assignment_id_v2",
		StreamOrderDomain:                "govar-p1b-opportunity-order-source-unit-v2",
		StreamOrderHashInput:             "domain_utf8 || NUL || canonical_decimal_seed_ascii || NUL || scenario_utf8 || NUL || lower_hex_source_assignment_id_ascii",
		WindowAssignmentAlgorithm:        "sort_sha256_then_source_assignment_id_rank_div_cohort_size_v2",
		WindowAssignmentDomain:           "govar-p1b-window-assignment-source-unit-v2",
		WindowAssignmentHashInput:        "domain_utf8 || NUL || canonical_decimal_seed_ascii || NUL || scenario_utf8 || NUL || lower_hex_source_assignment_id_ascii",
		SourceAssignmentIDDomain:         "article3-azure-llm-code-source-assignment-id-v1",
		SourceAssignmentIDHashInput:      "domain_utf8 || NUL || dataset_id_utf8 || NUL || canonical_decimal_source_row || NUL || timestamp_utf8",
		SourceAssignmentIDExcludedFields: []string{"context_tokens", "generated_tokens"},
		WindowIndexRule:                  "window_rank // events_per_hierarchical_tenant_window_unit",
		CohortIndexRule:                  "window_rank % events_per_hierarchical_tenant_window_unit",
		StreamRestorationRule:            "restore stream-order sequence after window assignment",
		OutcomeFieldsForbidden: []string{
			"actual_cost_micros", "actual_input_cost_micros", "actual_input_tokens",
			"actual_output_cost_micros", "actual_output_tokens", "event_id", "generated_tokens",
			"outcome", "outcomes", "quality", "reward", "score", "selected_score",
			"usage_receipt", "usage_receipt_sha256",
		},
	}
}

func validateP1bDesignMatrixAndSelectionV2(design map[string]json.RawMessage) error {
	var seeds []uint64
	if err := artifactDecodeStrictJSONV2(design["seeds"], &seeds); err != nil {
		return fmt.Errorf("p1b design seeds: %w", err)
	}
	if !reflect.DeepEqual(seeds, []uint64{91001, 91002, 91003, 91004, 91005}) {
		return errors.New("p1b design seeds differ from the fixed five-seed matrix")
	}
	var scenarios []string
	if err := artifactDecodeStrictJSONV2(design["scenarios"], &scenarios); err != nil {
		return fmt.Errorf("p1b design scenarios: %w", err)
	}
	if !reflect.DeepEqual(scenarios, []string{"S0_reference", "S1_liability", "S2_tail", "S3_isolation"}) {
		return errors.New("p1b design scenarios differ from the fixed four-scenario matrix")
	}
	var selection p1bSelectionContractV2
	if err := artifactDecodeStrictJSONV2(design["selection_contract"], &selection); err != nil {
		return fmt.Errorf("p1b selection contract: %w", err)
	}
	if !reflect.DeepEqual(selection, expectedP1bSelectionContractV2()) {
		return errors.New("p1b selection contract differs from the source-assignment v2 executable constants")
	}
	return nil
}

type p1bSourceBindingV2 struct {
	SchemaVersion            string            `json:"schema_version"`
	RepositoryCommit         string            `json:"repository_commit"`
	WorkingTreeSource        bool              `json:"working_tree_source"`
	ReviewedSourceRootSHA256 string            `json:"reviewed_source_root_sha256"`
	Sources                  map[string]string `json:"sources"`
	Toolchain                p1bToolchainV2    `json:"toolchain"`
}

type p1bToolchainV2 struct {
	SchemaVersion          string `json:"schema_version"`
	GoVersion              string `json:"go_version"`
	PythonVersion          string `json:"python_version"`
	ProducerBinarySHA256   string `json:"producer_binary_sha256"`
	GoExecutableSHA256     string `json:"go_executable_sha256"`
	PythonExecutableSHA256 string `json:"python_executable_sha256"`
}

type p1bRuntimeCapabilityV2 struct {
	SchemaVersion                                 string            `json:"schema_version"`
	Status                                        string            `json:"status"`
	Verdict                                       string            `json:"verdict"`
	UsageOnlyDatasetID                            string            `json:"usage_only_dataset_id"`
	UsageOnlySubsplit                             string            `json:"usage_only_subsplit"`
	QualityRequired                               bool              `json:"quality_required"`
	QualityAvailable                              bool              `json:"quality_available"`
	H3QualityMeasured                             bool              `json:"h3_quality_measured"`
	HierarchicalTenantWindowRegistrySupported     bool              `json:"hierarchical_tenant_window_registry_supported"`
	MinimumHierarchicalTenantWindowUnitsSupported int64             `json:"minimum_hierarchical_tenant_window_units_supported"`
	InferenceUnitContract                         json.RawMessage   `json:"inference_unit_contract"`
	PreOutcomeIdentity                            json.RawMessage   `json:"preoutcome_identity"`
	DesignKeyContract                             json.RawMessage   `json:"design_key_contract"`
	SelectionIdentityField                        string            `json:"selection_identity_field"`
	OrderingIdentityField                         string            `json:"ordering_identity_field"`
	WindowAssignmentIdentityField                 string            `json:"window_assignment_identity_field"`
	OutcomeBoundEventIDUsedForDesign              bool              `json:"outcome_bound_event_id_used_for_design"`
	GeneratedTokensUsedForDesign                  bool              `json:"generated_tokens_used_for_design"`
	ConfigSchema                                  string            `json:"config_schema"`
	LifecycleSchema                               string            `json:"lifecycle_schema"`
	ProducerPath                                  string            `json:"producer_path"`
	ProducerSHA256                                string            `json:"producer_sha256"`
	ReviewedSourceHashes                          map[string]string `json:"reviewed_source_hashes"`
	IndependentReview                             bool              `json:"independent_review"`
	UnresolvedCriticalMajor                       int64             `json:"unresolved_critical_major"`
}

type p1bRawSupportEnvelopeV2 struct {
	SchemaVersion     string          `json:"schema_version"`
	Status            string          `json:"status"`
	EvidenceLabel     string          `json:"evidence_label"`
	SupportMode       E1SupportModeV2 `json:"support_mode"`
	TestOnly          bool            `json:"test_only"`
	OutcomeFieldsUsed []string        `json:"outcome_fields_used"`
}

type p1bBudgetCalibrationArtifactV2 struct {
	p1bRawSupportEnvelopeV2
	Scenario               string        `json:"scenario"`
	BudgetLevel            E1BudgetLevel `json:"budget_level"`
	BudgetMicros           int64         `json:"budget_micros"`
	CalibrationBasisSHA256 string        `json:"calibration_basis_sha256"`
}

type p1bCalibrationArtifactV2 struct {
	p1bRawSupportEnvelopeV2
	ProspectiveEvidence GOVARProspectiveEvidence `json:"prospective_evidence"`
}

type p1bCandidateSetArtifactV2 struct {
	p1bRawSupportEnvelopeV2
	Candidates []GOVARCandidateBinding `json:"candidates"`
}

type p1bProfileRegistryArtifactV2 struct {
	p1bRawSupportEnvelopeV2
	ProspectiveEvidence GOVARProspectiveEvidence `json:"prospective_evidence"`
}

type p1bSlotTemplateArtifactV2 struct {
	p1bRawSupportEnvelopeV2
	OpportunityStreamSHA256 string       `json:"opportunity_stream_sha256"`
	Cohorts                 []CohortSpec `json:"cohorts"`
}

func preflightP1bExactArtifactInputsV2(inputs ArtifactPreflightInputsV2) (artifactPreflightResultV2, error) {
	return preflightP1bExactArtifactInputsWithReleaseV2(inputs, p1bExactProductionReleaseReadyV2)
}

// preflightP1bExactArtifactInputsWithReleaseV2 keeps the release decision an
// explicit, non-exported dependency. Production has exactly one caller above,
// permanently wired to the compile-time latch. Package tests may pass true to
// exercise every exact pre-release validator without creating a runtime or
// environment-controlled path around that latch.
func preflightP1bExactArtifactInputsWithReleaseV2(inputs ArtifactPreflightInputsV2, releaseReady bool) (artifactPreflightResultV2, error) {
	if inputs.SupportMode != E1SupportModeP1bExact {
		return artifactPreflightResultV2{}, errors.New("p1b exact preflight requires explicit p1b_exact mode")
	}
	if !releaseReady {
		return artifactPreflightResultV2{}, errors.New("p1b_exact production release latch is closed pending frozen supports and independent cross-language verification")
	}
	var intent ExecutionIntentV2
	if err := artifactDecodeStrictJSONV2(inputs.ExecutionIntentRaw, &intent); err != nil {
		return artifactPreflightResultV2{}, fmt.Errorf("p1b execution intent: %w", err)
	}
	if intent.SchemaVersion != E1ExecutionIntentSchemaV2 || intent.Status != "locked_preoutcome" ||
		intent.EvidenceLabel != p1bEvidenceLabelV2 || intent.SupportMode != E1SupportModeP1bExact || intent.TestOnly ||
		len(intent.OutcomeFieldsUsed) != 0 {
		return artifactPreflightResultV2{}, errors.New("p1b execution intent is not an exact locked pre-outcome intent")
	}
	manifest, err := decodeJSONObjectV2(inputs.DesignManifestRaw)
	if err != nil {
		return artifactPreflightResultV2{}, fmt.Errorf("p1b design manifest: %w", err)
	}
	selected, err := selectedP1bStreamV2(manifest, intent.StreamKey)
	if err != nil {
		return artifactPreflightResultV2{}, err
	}
	pilot, err := decodeJSONObjectV2(inputs.PilotUsageBindingRaw)
	if err != nil {
		return artifactPreflightResultV2{}, fmt.Errorf("p1b pilot binding: %w", err)
	}
	pilotSetSHA, err := jsonStringFieldV2(pilot, "subsplit_preoutcome_ids_sha256")
	if err != nil || !shaPattern.MatchString(pilotSetSHA) {
		return artifactPreflightResultV2{}, errors.New("p1b pilot binding lacks a canonical pre-outcome set digest")
	}
	design, err := decodeJSONObjectV2(inputs.DesignSpecRaw)
	if err != nil {
		return artifactPreflightResultV2{}, err
	}
	designLockSHA, err := jsonStringFieldV2(design, "design_lock_sha256")
	if err != nil || !shaPattern.MatchString(designLockSHA) {
		return artifactPreflightResultV2{}, errors.New("p1b design lacks a canonical design lock")
	}
	lock := E1ProvenanceLockV2{
		SupportMode:             E1SupportModeP1bExact,
		OpportunityStreamSHA256: SHA256(inputs.StreamRaw), SourceCodeBindingSHA256: SHA256(inputs.SourceCodeBindingRaw),
		SourceManifestSHA256: SHA256(inputs.SourceManifestRaw), PilotUsageBindingSHA256: SHA256(inputs.PilotUsageBindingRaw),
		RuntimeCapabilitySHA256: SHA256(inputs.RuntimeCapabilityRaw), MappingArtifactSHA256: SHA256(inputs.PreOutcomeMappingRaw),
		PilotPreOutcomeSetSHA256: pilotSetSHA, AssignedPreOutcomeSetSHA256: selected.AssignedPreOutcomeSetSHA256,
		StreamPreOutcomeSequenceSHA256: selected.StreamPreOutcomeSequenceSHA256, ProtocolSHA256: SHA256(inputs.ProtocolRaw),
		DesignSpecSHA256: SHA256(inputs.DesignSpecRaw), DecisionConfigTemplateSHA256: SHA256(inputs.DecisionConfigTemplateRaw),
		DesignLockSHA256: designLockSHA, IndependentDesignReviewSHA256: SHA256(inputs.IndependentDesignReviewRaw),
		DesignManifestSHA256: SHA256(inputs.DesignManifestRaw), CohortRegistrySHA256: SHA256(inputs.CohortRegistryRaw),
		ProfileExecutionContractSHA256: SHA256(inputs.ProfileExecutionContractRaw), BudgetCalibrationSHA256: SHA256(inputs.BudgetCalibrationRaw),
		CalibrationArtifactSHA256: SHA256(inputs.CalibrationArtifactRaw), CandidateSetSHA256: SHA256(inputs.CandidateSetRaw),
		ProfileRegistrySHA256: SHA256(inputs.ProfileRegistryRaw), SlotTemplateSHA256: SHA256(inputs.SlotTemplateRaw),
		MethodBuilderBundleSHA256: SHA256(inputs.MethodBuilderBundleRaw), ExecutionIntentSHA256: SHA256(inputs.ExecutionIntentRaw),
		IndependentAuthorizationSHA256: SHA256(inputs.IndependentAuthorizationRaw),
	}
	lock.ExecutionSubjectSHA256 = lock.preOutcomeExecutionSubjectDigest()
	lock.ExecutionLockSHA256 = lock.executionDigest()
	if err := lock.Validate(); err != nil {
		return artifactPreflightResultV2{}, err
	}
	config := intent.Config.materialize(lock, strings.Repeat("0", 64))
	if len(inputs.ConfigRaw) != 0 {
		if err := artifactRejectDuplicateJSONV2(inputs.ConfigRaw); err != nil {
			return artifactPreflightResultV2{}, fmt.Errorf("config JSON: %w", err)
		}
		published, _, parseErr := ParseConfigV2(inputs.ConfigRaw)
		if parseErr != nil {
			return artifactPreflightResultV2{}, parseErr
		}
		if published.ProvenanceLock != lock || !reflect.DeepEqual(projectExecutionIntentConfigV2(published), intent.Config) {
			return artifactPreflightResultV2{}, errors.New("published config differs from the authorized execution intent and reconstructed lock")
		}
		config = published
	}
	if err := config.Validate(); err != nil {
		return artifactPreflightResultV2{}, fmt.Errorf("p1b intent config: %w", err)
	}
	opportunities, _, err := ParseStreamV2(inputs.StreamRaw, config)
	if err != nil {
		return artifactPreflightResultV2{}, err
	}
	mappings, err := ParsePreOutcomeMappingV2(inputs.PreOutcomeMappingRaw)
	if err != nil {
		return artifactPreflightResultV2{}, err
	}
	if len(mappings) != len(opportunities) {
		return artifactPreflightResultV2{}, errors.New("p1b mapping and stream row counts differ")
	}
	for index := range opportunities {
		if mappings[index].Opportunity() != opportunities[index] {
			return artifactPreflightResultV2{}, fmt.Errorf("p1b mapping row %d differs from the opportunity stream", index+1)
		}
	}
	setSHA, setErr := StreamPreOutcomeSetSHA256V2(opportunities)
	sequenceSHA, sequenceErr := StreamPreOutcomeSequenceSHA256V2(opportunities)
	if setErr != nil || sequenceErr != nil || setSHA != lock.AssignedPreOutcomeSetSHA256 || sequenceSHA != lock.StreamPreOutcomeSequenceSHA256 {
		return artifactPreflightResultV2{}, errors.New("p1b stream set or sequence does not recompute from exact opportunity bytes")
	}
	schemas, err := supportSchemasForModeV2(E1SupportModeP1bExact)
	if err != nil {
		return artifactPreflightResultV2{}, err
	}
	validated, err := validateP1bExactSupportInputsV2(config, opportunities, inputs, schemas)
	if err != nil {
		return artifactPreflightResultV2{}, err
	}
	return artifactPreflightResultV2{config: config, opportunities: opportunities, mappings: mappings, support: validated}, nil
}

func validateP1bExactSupportInputsV2(config ConfigV2, opportunities []OpportunityV2, inputs ArtifactPreflightInputsV2, schemas artifactSupportValidationV2) (artifactSupportValidationV2, error) {
	protocol, err := validateP1bProtocolV2(inputs.ProtocolRaw)
	if err != nil {
		return artifactSupportValidationV2{}, err
	}
	sourceBinding, err := validateP1bSourceBindingV2(inputs.SourceCodeBindingRaw)
	if err != nil {
		return artifactSupportValidationV2{}, err
	}
	if config.SoftwareSHA256 != SHA256(inputs.SourceCodeBindingRaw) {
		return artifactSupportValidationV2{}, errors.New("p1b config software hash does not equal exact reviewed source-binding bytes")
	}
	if err := validateP1bSourceAndPilotV2(inputs.SourceManifestRaw, inputs.PilotUsageBindingRaw, config.ProvenanceLock.PilotPreOutcomeSetSHA256); err != nil {
		return artifactSupportValidationV2{}, err
	}
	if err := validateP1bRuntimeCapabilityV2(inputs.RuntimeCapabilityRaw, sourceBinding); err != nil {
		return artifactSupportValidationV2{}, err
	}
	design, decisionTemplate, designLockSHA, err := validateP1bDesignV2(config, inputs)
	if err != nil {
		return artifactSupportValidationV2{}, err
	}
	if err := validateP1bDesignReviewV2(inputs, design, designLockSHA); err != nil {
		return artifactSupportValidationV2{}, err
	}
	stream, factor, err := validateP1bDesignManifestV2(config, opportunities, inputs, designLockSHA)
	if err != nil {
		return artifactSupportValidationV2{}, err
	}
	if err := validateP1bCohortAndProfileContractV2(config, opportunities, inputs, stream, factor, decisionTemplate); err != nil {
		return artifactSupportValidationV2{}, err
	}
	if err := validateP1bBuilderSupportsAndIntentV2(config, opportunities, inputs, decisionTemplate, stream, factor); err != nil {
		return artifactSupportValidationV2{}, err
	}
	if err := validateExactLockByteBindingsV2(config.ProvenanceLock, inputs, config.ProvenanceLock.PilotPreOutcomeSetSHA256, designLockSHA); err != nil {
		return artifactSupportValidationV2{}, err
	}
	if err := validateP1bAuthorizationV2(config, inputs, sourceBinding); err != nil {
		return artifactSupportValidationV2{}, err
	}
	_ = protocol
	return schemas, nil
}

func validateP1bProtocolV2(raw []byte) (map[string]json.RawMessage, error) {
	protocol, err := decodeJSONObjectV2(raw)
	if err != nil {
		return nil, fmt.Errorf("p1b protocol: %w", err)
	}
	for key, want := range map[string]string{
		"schema_version": p1bProtocolSchemaV2, "evidence_label": p1bEvidenceLabelV2,
		"budget_calibration_design_status": "locked_exact_reviewed_artifact",
		"window_registry_rule":             "exactly 400 prospective tenant-window identities per scenario and seed derived from source_assignment_id; every opportunity is assigned before generated_tokens is opened",
	} {
		got, fieldErr := jsonStringFieldV2(protocol, key)
		if fieldErr != nil || got != want {
			return nil, fmt.Errorf("p1b protocol field %s differs from the locked full-execution contract", key)
		}
	}
	for _, key := range []string{"final_manifest_eligible", "scientific_claims_authorized", "comparative_effect_estimation_authorized", "p_values_authorized"} {
		value, fieldErr := jsonBoolFieldV2(protocol, key)
		if fieldErr != nil || value {
			return nil, fmt.Errorf("p1b protocol field %s is not fail-closed", key)
		}
	}
	if number, fieldErr := jsonInt64FieldV2(protocol, "opportunities_per_cell"); fieldErr != nil || number != 10_000 {
		return nil, errors.New("p1b protocol does not require exactly 10,000 opportunities per cell")
	}
	if number, fieldErr := jsonInt64FieldV2(protocol, "expected_cell_count"); fieldErr != nil || number != 100 {
		return nil, errors.New("p1b protocol does not require exactly 100 cells")
	}
	return protocol, nil
}

func validateP1bSourceBindingV2(raw []byte) (p1bSourceBindingV2, error) {
	var binding p1bSourceBindingV2
	if err := artifactDecodeStrictJSONV2(raw, &binding); err != nil {
		return binding, fmt.Errorf("p1b source-code binding: %w", err)
	}
	if binding.SchemaVersion != p1bSourceCodeBindingSchemaV2 || binding.WorkingTreeSource ||
		!p1bCommitPatternV2.MatchString(binding.RepositoryCommit) || len(binding.Sources) == 0 ||
		binding.Toolchain.SchemaVersion != "govar-p1b-full-toolchain-v1" {
		return binding, errors.New("p1b source-code binding is not an exact reviewed HEAD/full-toolchain binding")
	}
	type sourceRow struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	}
	rows := make([]sourceRow, 0, len(binding.Sources))
	for path, digest := range binding.Sources {
		if path == "" || filepath.IsAbs(path) || strings.Contains(path, "\\") || strings.Contains(path, "..") || !shaPattern.MatchString(digest) {
			return binding, fmt.Errorf("p1b reviewed source entry %q is unsafe", path)
		}
		rows = append(rows, sourceRow{path, digest})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Path < rows[j].Path })
	rootRaw, _ := json.Marshal(rows)
	if binding.ReviewedSourceRootSHA256 != DomainHash("govar-p1b-reviewed-source-root-v1", rootRaw) {
		return binding, errors.New("p1b reviewed source root does not recompute")
	}
	for name, digest := range map[string]string{
		"producer binary": binding.Toolchain.ProducerBinarySHA256, "go executable": binding.Toolchain.GoExecutableSHA256,
		"python executable": binding.Toolchain.PythonExecutableSHA256,
	} {
		if !shaPattern.MatchString(digest) {
			return binding, fmt.Errorf("p1b toolchain %s digest is invalid", name)
		}
	}
	if binding.Toolchain.GoVersion == "" || binding.Toolchain.PythonVersion == "" ||
		binding.Sources["article3/experiments/pilot/P1b/prepare_p1b_design.py"] != p1bPretraceGeneratorSHA256V2 {
		return binding, errors.New("p1b reviewed source binding omits the contract-final pretrace producer")
	}
	return binding, nil
}

func validateP1bSourceAndPilotV2(sourceRaw, pilotRaw []byte, pilotSetSHA string) error {
	source, err := decodeJSONObjectV2(sourceRaw)
	if err != nil {
		return fmt.Errorf("p1b source manifest: %w", err)
	}
	if err := requireExactJSONKeysV2(source, []string{"assignment", "dataset_id", "inspected_id_registry", "manifest_payload_sha256", "outputs", "preoutcome_identity", "schema_version", "source"}, "p1b source manifest"); err != nil {
		return err
	}
	if schema, _ := jsonStringFieldV2(source, "schema_version"); schema != p1bSourceManifestSchemaV2 {
		return errors.New("p1b source manifest schema differs")
	}
	if dataset, _ := jsonStringFieldV2(source, "dataset_id"); dataset != UsageDatasetV2 {
		return errors.New("p1b source manifest dataset differs")
	}
	payload, err := jsonStringFieldV2(source, "manifest_payload_sha256")
	if err != nil || payload != canonicalP1bObjectDigestWithoutFieldV2(source, "manifest_payload_sha256") {
		return errors.New("p1b source-manifest payload digest does not recompute")
	}
	pilot, err := decodeJSONObjectV2(pilotRaw)
	if err != nil {
		return fmt.Errorf("p1b pilot usage binding: %w", err)
	}
	required := []string{"schema_version", "status", "dataset_id", "parent_split", "subsplit", "mode", "quality_available", "h3_quality_measured", "parent_artifact_sha256", "parent_event_ids_integrity_sha256", "parent_preoutcome_ids_sha256", "subsplit_manifest_path", "subsplit_manifest_sha256", "generator_path", "generator_sha256", "subsplit_artifact_path", "subsplit_artifact_sha256", "subsplit_event_ids_integrity_sha256", "subsplit_preoutcome_ids_sha256", "subsplit_rows", "preoutcome_identity", "derivation", "design_key_contract", "event_id_role", "inspected_registry_binding", "historically_inspected_rows_excluded", "values_inspected_before_authorization", "independent_split_review"}
	if err := requireExactJSONKeysV2(pilot, required, "p1b pilot usage binding"); err != nil {
		return err
	}
	for key, want := range map[string]string{"schema_version": p1bPilotUsageBindingSchemaV2, "status": "authorized", "dataset_id": UsageDatasetV2, "parent_split": "development", "subsplit": UsageSplitV2, "mode": "usage_only", "subsplit_manifest_sha256": SHA256(sourceRaw), "subsplit_preoutcome_ids_sha256": pilotSetSHA} {
		got, fieldErr := jsonStringFieldV2(pilot, key)
		if fieldErr != nil || got != want {
			return fmt.Errorf("p1b pilot usage binding field %s differs", key)
		}
	}
	for _, key := range []string{"quality_available", "h3_quality_measured", "values_inspected_before_authorization"} {
		value, fieldErr := jsonBoolFieldV2(pilot, key)
		if fieldErr != nil || value {
			return fmt.Errorf("p1b pilot field %s is not false", key)
		}
	}
	excluded, err := jsonBoolFieldV2(pilot, "historically_inspected_rows_excluded")
	if err != nil || !excluded {
		return errors.New("p1b pilot does not exclude historically inspected rows")
	}
	rows, err := jsonInt64FieldV2(pilot, "subsplit_rows")
	if err != nil || rows < 50_000 {
		return errors.New("p1b pilot has fewer than 50,000 prespecified usage rows")
	}
	review, err := nestedJSONObjectV2(pilot, "independent_split_review")
	if err != nil {
		return err
	}
	verdict, _ := jsonStringFieldV2(review, "verdict")
	reviewed, _ := jsonStringFieldV2(review, "reviewed_manifest_sha256")
	unresolved, _ := jsonInt64FieldV2(review, "unresolved_critical_major")
	if verdict != "approved" || reviewed != SHA256(sourceRaw) || unresolved != 0 {
		return errors.New("p1b pilot split lacks an exact independent approval")
	}
	return nil
}

func validateP1bRuntimeCapabilityV2(raw []byte, source p1bSourceBindingV2) error {
	var capability p1bRuntimeCapabilityV2
	if err := artifactDecodeStrictJSONV2(raw, &capability); err != nil {
		return fmt.Errorf("p1b runtime capability: %w", err)
	}
	if capability.SchemaVersion != p1bRuntimeCapabilitySchemaV2 || capability.Status != "approved" || capability.Verdict != "approved" ||
		capability.UsageOnlyDatasetID != UsageDatasetV2 || capability.UsageOnlySubsplit != UsageSplitV2 || capability.QualityRequired ||
		capability.QualityAvailable || capability.H3QualityMeasured || !capability.HierarchicalTenantWindowRegistrySupported ||
		capability.MinimumHierarchicalTenantWindowUnitsSupported < 400 || capability.SelectionIdentityField != "source_assignment_id" ||
		capability.OrderingIdentityField != "source_assignment_id" || capability.WindowAssignmentIdentityField != "source_assignment_id" ||
		capability.OutcomeBoundEventIDUsedForDesign || capability.GeneratedTokensUsedForDesign || capability.ConfigSchema != ConfigSchemaV2 ||
		capability.LifecycleSchema != LifecycleSchemaV2 || !capability.IndependentReview || capability.UnresolvedCriticalMajor != 0 {
		return errors.New("p1b runtime capability is incomplete, stale, or outcome-bound")
	}
	if capability.ProducerPath == "" || filepath.IsAbs(capability.ProducerPath) || strings.Contains(capability.ProducerPath, "..") ||
		!shaPattern.MatchString(capability.ProducerSHA256) || source.Sources[capability.ProducerPath] != capability.ProducerSHA256 ||
		capability.ReviewedSourceHashes[capability.ProducerPath] != capability.ProducerSHA256 {
		return errors.New("p1b runtime capability producer does not match reviewed source bytes")
	}
	return nil
}

func validateP1bDesignV2(config ConfigV2, inputs ArtifactPreflightInputsV2) (map[string]json.RawMessage, map[string]json.RawMessage, string, error) {
	design, err := decodeJSONObjectV2(inputs.DesignSpecRaw)
	if err != nil {
		return nil, nil, "", err
	}
	keys := []string{"schema_version", "status", "evidence_label", "dataset_id", "split", "seeds", "scenarios", "scenario_pool_size", "stream_size", "seed_streams", "hierarchical_tenant_window_units_per_stream", "events_per_hierarchical_tenant_window_unit", "source_manifest_sha256", "pilot_usage_binding_sha256", "preoutcome_sidecar_sha256", "protocol_sha256", "decision_config_template", "decision_config_template_sha256", "selection_contract", "govar_calibration_basis", "design_lock_sha256"}
	if err := requireExactJSONKeysV2(design, keys, "p1b design specification"); err != nil {
		return nil, nil, "", err
	}
	if err := validateP1bDesignMatrixAndSelectionV2(design); err != nil {
		return nil, nil, "", err
	}
	for key, want := range map[string]string{"schema_version": p1bDesignSpecSchemaV2, "status": "locked_candidate", "evidence_label": p1bEvidenceLabelV2, "dataset_id": UsageDatasetV2, "split": UsageSplitV2, "source_manifest_sha256": SHA256(inputs.SourceManifestRaw), "pilot_usage_binding_sha256": SHA256(inputs.PilotUsageBindingRaw), "protocol_sha256": SHA256(inputs.ProtocolRaw), "decision_config_template_sha256": SHA256(inputs.DecisionConfigTemplateRaw)} {
		got, fieldErr := jsonStringFieldV2(design, key)
		if fieldErr != nil || got != want {
			return nil, nil, "", fmt.Errorf("p1b design field %s differs", key)
		}
	}
	for key, want := range map[string]int64{"scenario_pool_size": 50_000, "stream_size": 10_000, "seed_streams": 5, "hierarchical_tenant_window_units_per_stream": 400, "events_per_hierarchical_tenant_window_unit": 25} {
		got, fieldErr := jsonInt64FieldV2(design, key)
		if fieldErr != nil || got != want {
			return nil, nil, "", fmt.Errorf("p1b design dimension %s differs", key)
		}
	}
	embedded := design["decision_config_template"]
	if !bytes.Equal(mustCanonicalJSONV2(embedded), inputs.DecisionConfigTemplateRaw) {
		return nil, nil, "", errors.New("p1b copied decision template differs from exact canonical embedded bytes")
	}
	lockSHA, err := jsonStringFieldV2(design, "design_lock_sha256")
	if err != nil || lockSHA != canonicalP1bObjectDigestWithoutFieldV2(design, "design_lock_sha256") {
		return nil, nil, "", errors.New("p1b design lock does not recompute")
	}
	template, err := decodeJSONObjectV2(inputs.DecisionConfigTemplateRaw)
	if err != nil {
		return nil, nil, "", err
	}
	if err := requireExactJSONKeysV2(template, []string{"schema_version", "evidence_tier", "usage_dataset_id", "usage_split", "virtual_start", "input_price_micros_per_million", "output_price_micros_per_million", "verified_output_cap_tokens", "method_templates", "scenario_templates"}, "p1b decision template"); err != nil {
		return nil, nil, "", err
	}
	if schema, _ := jsonStringFieldV2(template, "schema_version"); schema != p1bDecisionConfigTemplateSchemaV2 {
		return nil, nil, "", errors.New("p1b decision-template schema differs")
	}
	if tier, _ := jsonStringFieldV2(template, "evidence_tier"); tier != config.EvidenceTier || config.EvidenceTier != EvidenceTierV2 {
		return nil, nil, "", errors.New("p1b decision-template evidence tier differs")
	}
	if start, _ := jsonStringFieldV2(template, "virtual_start"); start != config.VirtualStart {
		return nil, nil, "", errors.New("p1b decision-template virtual start differs")
	}
	for key, want := range map[string]int64{"input_price_micros_per_million": config.InputPriceMicrosPerMillion, "output_price_micros_per_million": config.OutputPriceMicrosPerMillion, "verified_output_cap_tokens": config.VerifiedOutputCapTokens} {
		got, fieldErr := jsonInt64FieldV2(template, key)
		if fieldErr != nil || got != want {
			return nil, nil, "", fmt.Errorf("p1b decision-template field %s differs from config", key)
		}
	}
	return design, template, lockSHA, nil
}

func validateP1bDesignReviewV2(inputs ArtifactPreflightInputsV2, design map[string]json.RawMessage, designLockSHA string) error {
	var review p1bIndependentReviewV2
	if err := artifactDecodeStrictJSONV2(inputs.IndependentDesignReviewRaw, &review); err != nil {
		return fmt.Errorf("p1b independent design review: %w", err)
	}
	decisionSHA, _ := jsonStringFieldV2(design, "decision_config_template_sha256")
	sidecarSHA, _ := jsonStringFieldV2(design, "preoutcome_sidecar_sha256")
	if review.SchemaVersion != p1bIndependentDesignReviewSchemaV2 || review.Verdict != "approved" ||
		review.ReviewScope != "outcome_free_pretrace_preparation_only" || !review.PretracePreparationAuthorized ||
		review.FullCampaignExecutionAuthorized || review.FinalEvidenceAuthorized || review.ScientificClaimsAuthorized ||
		!review.IndependentReview || !review.FreshReview || review.UnresolvedCritical != 0 || review.UnresolvedMajor != 0 ||
		review.OutcomesInspected || review.DatasetID != UsageDatasetV2 || review.Split != UsageSplitV2 ||
		review.DesignLockSHA256 != designLockSHA || review.DecisionConfigTemplateSHA256 != decisionSHA ||
		review.GeneratorSHA256 != p1bPretraceGeneratorSHA256V2 || review.ProtocolSHA256 != SHA256(inputs.ProtocolRaw) ||
		review.SourceManifestSHA256 != SHA256(inputs.SourceManifestRaw) || review.PilotUsageBindingSHA256 != SHA256(inputs.PilotUsageBindingRaw) ||
		review.PreOutcomeSidecarSHA256 != sidecarSHA {
		return errors.New("p1b design review does not independently bind the exact outcome-free pretrace")
	}
	return nil
}

func validateP1bDesignManifestV2(config ConfigV2, opportunities []OpportunityV2, inputs ArtifactPreflightInputsV2, designLockSHA string) (p1bStreamManifestV2, p1bScenarioFactorBindingV2, error) {
	manifest, err := decodeJSONObjectV2(inputs.DesignManifestRaw)
	if err != nil {
		return p1bStreamManifestV2{}, p1bScenarioFactorBindingV2{}, err
	}
	for key, want := range map[string]string{"schema_version": p1bDesignManifestSchemaV2, "status": "locked_candidate", "evidence_label": p1bEvidenceLabelV2, "dataset_id": UsageDatasetV2, "split": UsageSplitV2, "source_manifest_sha256": SHA256(inputs.SourceManifestRaw), "pilot_usage_binding_sha256": SHA256(inputs.PilotUsageBindingRaw), "protocol_sha256": SHA256(inputs.ProtocolRaw), "design_spec_sha256": SHA256(inputs.DesignSpecRaw), "design_lock_sha256": designLockSHA, "independent_design_review_sha256": SHA256(inputs.IndependentDesignReviewRaw), "decision_config_template_sha256": SHA256(inputs.DecisionConfigTemplateRaw), "artifact_set_root_domain": p1bPretraceArtifactRootDomainV2} {
		got, fieldErr := jsonStringFieldV2(manifest, key)
		if fieldErr != nil || got != want {
			return p1bStreamManifestV2{}, p1bScenarioFactorBindingV2{}, fmt.Errorf("p1b design-manifest field %s differs", key)
		}
	}
	for _, key := range []string{"final_evidence_eligible", "scientific_claims_authorized", "full_p1b_execution_authorized", "execution_intents_generated", "test_only_fixture_mode"} {
		value, fieldErr := jsonBoolFieldV2(manifest, key)
		if fieldErr != nil || value {
			return p1bStreamManifestV2{}, p1bScenarioFactorBindingV2{}, fmt.Errorf("p1b design-manifest field %s is not the expected locked-pretrace value", key)
		}
	}
	intentCount, _ := jsonInt64FieldV2(manifest, "execution_intent_count")
	expectedIntentCount, _ := jsonInt64FieldV2(manifest, "expected_execution_intent_count")
	if intentCount != 0 || expectedIntentCount != 100 {
		return p1bStreamManifestV2{}, p1bScenarioFactorBindingV2{}, errors.New("p1b pretrace intent-count boundary differs")
	}
	payload, err := jsonStringFieldV2(manifest, "manifest_payload_sha256")
	if err != nil || payload != canonicalP1bObjectDigestWithoutFieldV2(manifest, "manifest_payload_sha256") {
		return p1bStreamManifestV2{}, p1bScenarioFactorBindingV2{}, errors.New("p1b design-manifest payload digest does not recompute")
	}
	stream, err := selectedP1bStreamV2(manifest, fmt.Sprintf("%s/seed-%d", config.Scenario, config.Seed))
	if err != nil {
		return p1bStreamManifestV2{}, p1bScenarioFactorBindingV2{}, err
	}
	if stream.Scenario != config.Scenario || stream.Seed != config.Seed || stream.Rows != 10_000 || len(opportunities) != 10_000 ||
		stream.HierarchicalTenantWindowUnits != 400 || stream.EventsPerHierarchicalTenantWindow != 25 || stream.TenantWindowsIndependent ||
		stream.MappingArtifact.SHA256 != SHA256(inputs.PreOutcomeMappingRaw) || stream.MappingArtifact.Bytes != int64(len(inputs.PreOutcomeMappingRaw)) || stream.MappingArtifact.Rows != 10_000 ||
		stream.OpportunityStreamArtifact.SHA256 != SHA256(inputs.StreamRaw) || stream.OpportunityStreamArtifact.Bytes != int64(len(inputs.StreamRaw)) || stream.OpportunityStreamArtifact.Rows != 10_000 ||
		stream.CohortRegistryArtifact.SHA256 != SHA256(inputs.CohortRegistryRaw) || stream.CohortRegistryArtifact.Bytes != int64(len(inputs.CohortRegistryRaw)) || stream.CohortRegistryArtifact.Rows != 400 ||
		stream.ProfileExecutionContractArtifact.SHA256 != SHA256(inputs.ProfileExecutionContractRaw) || stream.ProfileExecutionContractArtifact.Bytes != int64(len(inputs.ProfileExecutionContractRaw)) || len(stream.OutcomeFieldsUsed) != 0 {
		return p1bStreamManifestV2{}, p1bScenarioFactorBindingV2{}, errors.New("p1b selected stream does not bind exact 10k/400x25 cell artifacts")
	}
	var factor p1bScenarioFactorBindingV2
	if err := artifactDecodeStrictJSONV2(stream.ScenarioFactorBinding, &factor); err != nil {
		return p1bStreamManifestV2{}, factor, fmt.Errorf("p1b scenario factor binding: %w", err)
	}
	if factor.Concurrency != config.Concurrency || factor.SettlementDelaySeconds != config.SettlementDelaySeconds ||
		factor.BudgetLevel != config.BudgetLevel || factor.BudgetMicros <= 0 || factor.DecisionConfigTemplateSHA256 != SHA256(inputs.DecisionConfigTemplateRaw) ||
		!shaPattern.MatchString(factor.ProspectiveUsageProfileSHA256) {
		return p1bStreamManifestV2{}, factor, errors.New("p1b selected stream factors differ from exact config")
	}
	if err := validateP1bPretraceArtifactRootV2(manifest); err != nil {
		return p1bStreamManifestV2{}, factor, err
	}
	return stream, factor, nil
}

func validateP1bPretraceArtifactRootV2(manifest map[string]json.RawMessage) error {
	inputArtifacts, err := nestedJSONObjectV2(manifest, "input_artifacts")
	if err != nil {
		return err
	}
	if err := requireExactJSONKeysV2(inputArtifacts, []string{"design_spec", "source_manifest", "pilot_usage_binding", "independent_design_review", "decision_config_template"}, "p1b input artifacts"); err != nil {
		return err
	}
	type rootRow struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	}
	rows := make([]rootRow, 0, 85)
	for name, raw := range inputArtifacts {
		var spec p1bArtifactSpecV2
		if err := artifactDecodeStrictJSONV2(raw, &spec); err != nil {
			return fmt.Errorf("p1b input artifact %s: %w", name, err)
		}
		if err := validateP1bPretraceSpecV2(spec, false); err != nil {
			return err
		}
		rows = append(rows, rootRow{spec.Path, spec.SHA256})
	}
	var streams []p1bStreamManifestV2
	if err := json.Unmarshal(manifest["streams"], &streams); err != nil || len(streams) != 20 {
		return errors.New("p1b design manifest must contain exactly 20 streams")
	}
	seenStreams := map[string]struct{}{}
	expectedStreams := map[string]struct{}{}
	for _, scenario := range []string{"S0_reference", "S1_liability", "S2_tail", "S3_isolation"} {
		for _, seed := range []uint64{91001, 91002, 91003, 91004, 91005} {
			expectedStreams[fmt.Sprintf("%s/seed-%d", scenario, seed)] = struct{}{}
		}
	}
	for _, stream := range streams {
		wantKey := fmt.Sprintf("%s/seed-%d", stream.Scenario, stream.Seed)
		if stream.StreamKey != wantKey {
			return errors.New("p1b design manifest stream key does not match scenario/seed")
		}
		if _, expected := expectedStreams[stream.StreamKey]; !expected {
			return errors.New("p1b design manifest contains a stream outside the fixed 4x5 product")
		}
		if _, duplicate := seenStreams[stream.StreamKey]; duplicate {
			return errors.New("p1b design manifest duplicates a stream key")
		}
		seenStreams[stream.StreamKey] = struct{}{}
		if stream.Rows != 10_000 || stream.HierarchicalTenantWindowUnits != 400 ||
			stream.EventsPerHierarchicalTenantWindow != 25 || stream.TenantWindowsIndependent ||
			len(stream.OutcomeFieldsUsed) != 0 {
			return errors.New("p1b design manifest stream differs from the exact 10k/400x25 contract")
		}
		for _, spec := range []p1bArtifactSpecV2{stream.MappingArtifact, stream.OpportunityStreamArtifact, stream.CohortRegistryArtifact} {
			if err := validateP1bPretraceSpecV2(spec, true); err != nil {
				return err
			}
			rows = append(rows, rootRow{spec.Path, spec.SHA256})
		}
		for _, spec := range []p1bArtifactSpecV2{stream.ProfileExecutionContractArtifact, stream.SlotTemplateArtifact, stream.CalibrationArtifact, stream.ProfileRegistryArtifact} {
			if err := validateP1bPretraceSpecV2(spec, false); err != nil {
				return err
			}
			rows = append(rows, rootRow{spec.Path, spec.SHA256})
		}
	}
	if !reflect.DeepEqual(seenStreams, expectedStreams) {
		return errors.New("p1b design manifest does not contain the exact four-scenario by five-seed product")
	}
	if len(rows) != 145 {
		return errors.New("p1b pretrace root does not contain exactly 145 artifacts")
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Path < rows[j].Path })
	for index := 1; index < len(rows); index++ {
		if rows[index].Path == rows[index-1].Path {
			return errors.New("p1b pretrace root aliases artifact paths")
		}
	}
	raw, _ := json.Marshal(rows)
	want := DomainHash(p1bPretraceArtifactRootDomainV2, raw)
	got, _ := jsonStringFieldV2(manifest, "artifact_set_root_sha256")
	count, _ := jsonInt64FieldV2(manifest, "artifact_set_count")
	if got != want || count != 145 {
		return errors.New("p1b pretrace artifact root/count do not recompute")
	}
	return nil
}

func validateP1bPretraceSpecV2(spec p1bArtifactSpecV2, rowsRequired bool) error {
	if spec.Path == "" || filepath.IsAbs(spec.Path) || strings.Contains(spec.Path, "\\") || strings.Contains(spec.Path, "..") ||
		!shaPattern.MatchString(spec.SHA256) || spec.Bytes <= 0 || (rowsRequired && spec.Rows <= 0) {
		return errors.New("p1b pretrace artifact specification is invalid")
	}
	return nil
}

func validateP1bCohortAndProfileContractV2(config ConfigV2, opportunities []OpportunityV2, inputs ArtifactPreflightInputsV2, stream p1bStreamManifestV2, factor p1bScenarioFactorBindingV2, template map[string]json.RawMessage) error {
	var cohort p1bCohortRegistryV2
	if err := artifactDecodeStrictJSONV2(inputs.CohortRegistryRaw, &cohort); err != nil {
		return err
	}
	if cohort.SchemaVersion != p1bCohortRegistrySchemaV2 || cohort.Scenario != config.Scenario || cohort.Seed != config.Seed ||
		cohort.HierarchicalTenantWindowUnits != 400 || cohort.EventsPerHierarchicalTenantWindow != 25 || cohort.TenantWindowsIndependent ||
		len(cohort.Cohorts) != 400 || !equalCohortSpecsV2(cohort.Cohorts, config.Cohorts) ||
		!bytes.Equal(mustCanonicalJSONV2(cohort.ProspectiveProfileExecution), mustCanonicalJSONV2(stream.ProspectiveProfileExecution)) {
		return errors.New("p1b cohort registry differs from exact config/stream 400x25 hierarchy")
	}
	counts := make(map[string]int64, 400)
	for _, opportunity := range opportunities {
		counts[cohortKey(opportunity.TenantID, opportunity.BudgetWindowID, opportunity.CohortID)]++
	}
	for _, spec := range cohort.Cohorts {
		if spec.Size != 25 || counts[cohortKey(spec.TenantID, spec.BudgetWindowID, spec.CohortID)] != 25 {
			return errors.New("p1b stream does not fill every declared cohort with 25 slots")
		}
	}
	contract, err := decodeJSONObjectV2(inputs.ProfileExecutionContractRaw)
	if err != nil {
		return err
	}
	for key, want := range map[string]string{"schema_version": p1bProfileExecutionContractSchemaV2, "status": "prospective_assignment_only", "scenario": config.Scenario} {
		got, fieldErr := jsonStringFieldV2(contract, key)
		if fieldErr != nil || got != want {
			return fmt.Errorf("p1b profile contract field %s differs", key)
		}
	}
	seed, _ := jsonUint64FieldV2(contract, "seed")
	if seed != config.Seed {
		return errors.New("p1b profile contract seed differs")
	}
	for _, key := range []string{"full_campaign_execution_authorized", "final_evidence_authorized", "scientific_claims_authorized", "observed_output_distribution_claimed", "output_values_transformed"} {
		value, fieldErr := jsonBoolFieldV2(contract, key)
		if fieldErr != nil || value {
			return fmt.Errorf("p1b profile contract field %s is not false", key)
		}
	}
	bindings, err := nestedJSONObjectV2(contract, "pretrace_artifact_bindings")
	if err != nil {
		return err
	}
	for key, want := range map[string]string{"mapping_artifact_sha256": SHA256(inputs.PreOutcomeMappingRaw), "opportunity_stream_artifact_sha256": SHA256(inputs.StreamRaw), "cohort_registry_artifact_sha256": SHA256(inputs.CohortRegistryRaw)} {
		got, _ := jsonStringFieldV2(bindings, key)
		if got != want {
			return fmt.Errorf("p1b profile contract binding %s differs", key)
		}
	}
	budgetSupport, err := nestedJSONObjectV2(contract, "external_budget_calibration_support")
	if err != nil {
		return err
	}
	budgetSHA, _ := jsonStringFieldV2(budgetSupport, "sha256")
	required, _ := jsonBoolFieldV2(budgetSupport, "required")
	embedded, _ := jsonBoolFieldV2(budgetSupport, "raw_support_embedded_by_pretrace")
	if !required || embedded || budgetSHA != SHA256(inputs.BudgetCalibrationRaw) {
		return errors.New("p1b profile contract does not bind exact external budget-calibration bytes")
	}
	govarSupports, err := nestedJSONObjectV2(contract, "external_govar_builder_supports")
	if err != nil {
		return err
	}
	for key, want := range map[string]string{"calibration_artifact_sha256": SHA256(inputs.CalibrationArtifactRaw), "candidate_set_sha256": SHA256(inputs.CandidateSetRaw), "profile_registry_sha256": SHA256(inputs.ProfileRegistryRaw), "slot_template_sha256": SHA256(inputs.SlotTemplateRaw)} {
		got, _ := jsonStringFieldV2(govarSupports, key)
		if got != want {
			return fmt.Errorf("p1b profile contract GOV-AR support %s differs", key)
		}
	}
	profileSupport, err := nestedJSONObjectV2(contract, "external_profile_registry_support")
	if err != nil {
		return err
	}
	profileSHA, _ := jsonStringFieldV2(profileSupport, "sha256")
	profileRequired, _ := jsonBoolFieldV2(profileSupport, "required")
	if !profileRequired || profileSHA != SHA256(inputs.ProfileRegistryRaw) {
		return errors.New("p1b profile registry raw support differs")
	}
	builder, err := nestedJSONObjectV2(contract, "govar_builder_binding")
	if err != nil {
		return err
	}
	builderTemplateSHA, _ := jsonStringFieldV2(builder, "builder_config_template_sha256")
	streamBindingSHA, _ := jsonStringFieldV2(builder, "stream_builder_input_binding_sha256")
	profileBinding, _ := nestedJSONObjectV2(contract, "prospective_profile_binding")
	profileSHA256, _ := jsonStringFieldV2(profileBinding, "prospective_usage_profile_sha256")
	wantBinding := DomainHash("govar-p1b-govar-stream-builder-input-binding-v1", []byte(config.Scenario), []byte(strconv.FormatUint(config.Seed, 10)), []byte(builderTemplateSHA), []byte(profileSHA256), []byte(SHA256(inputs.PreOutcomeMappingRaw)), []byte(SHA256(inputs.StreamRaw)), []byte(SHA256(inputs.CohortRegistryRaw)))
	if streamBindingSHA != wantBinding || stream.GOVARStreamBuilderInputSHA256 != wantBinding || factor.ProspectiveUsageProfileSHA256 != profileSHA256 {
		return errors.New("p1b GOV-AR stream-builder input binding does not recompute")
	}
	if err := validateP1bOpportunityFactorsV2(config, opportunities, factor, template); err != nil {
		return err
	}
	return nil
}

func validateP1bOpportunityFactorsV2(config ConfigV2, opportunities []OpportunityV2, factor p1bScenarioFactorBindingV2, template map[string]json.RawMessage) error {
	scenarios, err := nestedJSONObjectV2(template, "scenario_templates")
	if err != nil {
		return err
	}
	selected, err := nestedJSONObjectV2(scenarios, config.Scenario)
	if err != nil {
		return err
	}
	batchSize, err := jsonInt64FieldV2(selected, "arrival_batch_size")
	if err != nil || batchSize != int64(config.Concurrency) || batchSize <= 0 {
		return errors.New("p1b scenario arrival batch does not equal the concurrency factor")
	}
	spacingMS, spacingErr := jsonInt64FieldV2(selected, "arrival_batch_spacing_milliseconds")
	responseMS, responseErr := jsonInt64FieldV2(selected, "provider_response_delay_milliseconds")
	usageMS, usageErr := jsonInt64FieldV2(selected, "usage_available_after_response_milliseconds")
	settlementMS, settlementErr := jsonInt64FieldV2(selected, "settlement_after_response_milliseconds")
	budgetMicros, budgetErr := jsonInt64FieldV2(selected, "budget_micros")
	budgetLevel, levelErr := jsonStringFieldV2(selected, "budget_level")
	if spacingErr != nil || responseErr != nil || usageErr != nil || settlementErr != nil || budgetErr != nil || levelErr != nil ||
		spacingMS != 2_000 || responseMS != 1_000 || usageMS < 0 || settlementMS < 0 ||
		settlementMS != int64(config.SettlementDelaySeconds)*1_000 || budgetMicros != factor.BudgetMicros ||
		budgetLevel != string(config.BudgetLevel) {
		return errors.New("p1b scenario clock or budget template differs from the selected factors")
	}
	start, err := parseCanonicalAbsoluteTimeV2("p1b virtual_start", config.VirtualStart)
	if err != nil {
		return err
	}
	tenantBudgets := map[int64]int64{0: budgetMicros}
	if config.Scenario == "S3_isolation" {
		schedule, scheduleErr := nestedJSONObjectV2(selected, "tenant_budget_schedule")
		if scheduleErr != nil {
			return scheduleErr
		}
		scheduleCanonical, marshalErr := json.Marshal(schedule)
		if marshalErr != nil {
			return errors.New("p1b S3 tenant-budget schedule cannot be canonicalized")
		}
		scheduleSHA := SHA256(scheduleCanonical)
		if factor.TenantBudgetScheduleSHA256 == nil || *factor.TenantBudgetScheduleSHA256 != scheduleSHA ||
			factor.BudgetAssignmentMode != "exact_asymmetric_tenant_schedule" || factor.DistinctTenantBudgetLevels != 2 {
			return errors.New("p1b S3 factor does not bind the exact asymmetric tenant-budget schedule")
		}
		var entries []struct {
			TenantIndex  int64 `json:"tenant_index"`
			BudgetMicros int64 `json:"budget_micros"`
		}
		if err := artifactDecodeStrictJSONV2(schedule["tenant_budgets"], &entries); err != nil || len(entries) != 8 {
			return errors.New("p1b S3 tenant-budget schedule must contain exactly eight entries")
		}
		tenantBudgets = make(map[int64]int64, 8)
		for _, entry := range entries {
			expected := budgetMicros
			if entry.TenantIndex == 0 {
				expected = budgetMicros / 2
			}
			if entry.TenantIndex < 0 || entry.TenantIndex >= 8 || entry.BudgetMicros != expected {
				return errors.New("p1b S3 tenant-budget entry differs from the locked half-budget noisy-tenant design")
			}
			if _, duplicate := tenantBudgets[entry.TenantIndex]; duplicate {
				return errors.New("p1b S3 tenant-budget schedule duplicates a tenant")
			}
			tenantBudgets[entry.TenantIndex] = entry.BudgetMicros
		}
	} else if factor.TenantBudgetScheduleSHA256 != nil || factor.BudgetAssignmentMode != "single_scenario_budget" || factor.DistinctTenantBudgetLevels != 1 {
		return errors.New("p1b non-isolation factor contains an unexpected tenant-budget schedule")
	}
	for index, opportunity := range opportunities {
		if opportunity.Sequence != int64(index) {
			return errors.New("p1b opportunity sequence is not canonical")
		}
		batch := opportunity.Sequence / batchSize
		arrival := start.Add(time.Duration(batch*spacingMS) * time.Millisecond)
		provider := arrival.Add(time.Duration(responseMS) * time.Millisecond)
		usage := provider.Add(time.Duration(usageMS) * time.Millisecond)
		settlement := provider.Add(time.Duration(settlementMS) * time.Millisecond)
		if opportunity.ArrivalAt != arrival.Format(time.RFC3339Nano) ||
			opportunity.ProviderResponseAt != provider.Format(time.RFC3339Nano) ||
			opportunity.UsageAvailableAt != usage.Format(time.RFC3339Nano) ||
			opportunity.SettlementAt != settlement.Format(time.RFC3339Nano) {
			return fmt.Errorf("p1b opportunity %d clock differs from the locked scenario", index)
		}
		tenantIndex := int64(0)
		if config.Scenario == "S3_isolation" {
			separator := strings.LastIndexByte(opportunity.TenantID, '-')
			if separator < 0 {
				return errors.New("p1b S3 tenant identity lacks its prospective index")
			}
			parsed, parseErr := strconv.ParseInt(opportunity.TenantID[separator+1:], 10, 64)
			if parseErr != nil {
				return errors.New("p1b S3 tenant identity has a non-canonical index")
			}
			tenantIndex = parsed
		}
		if expected, exists := tenantBudgets[tenantIndex]; !exists || opportunity.BudgetMicros != expected {
			return fmt.Errorf("p1b opportunity %d budget differs from the locked tenant schedule", index)
		}
	}
	return nil
}

func validateP1bBuilderSupportsAndIntentV2(config ConfigV2, opportunities []OpportunityV2, inputs ArtifactPreflightInputsV2, template map[string]json.RawMessage, stream p1bStreamManifestV2, factor p1bScenarioFactorBindingV2) error {
	var budget p1bBudgetCalibrationArtifactV2
	if err := artifactDecodeStrictJSONV2(inputs.BudgetCalibrationRaw, &budget); err != nil {
		return fmt.Errorf("p1b budget calibration: %w", err)
	}
	if err := validateP1bRawEnvelopeV2(budget.p1bRawSupportEnvelopeV2, budgetCalibrationArtifactSchemaV2); err != nil || budget.Scenario != config.Scenario || budget.BudgetLevel != config.BudgetLevel || budget.BudgetMicros != factor.BudgetMicros || !shaPattern.MatchString(budget.CalibrationBasisSHA256) {
		return errors.New("p1b raw budget calibration does not match the selected scenario")
	}
	var calibration p1bCalibrationArtifactV2
	if err := artifactDecodeStrictJSONV2(inputs.CalibrationArtifactRaw, &calibration); err != nil {
		return err
	}
	if err := validateP1bRawEnvelopeV2(calibration.p1bRawSupportEnvelopeV2, calibrationArtifactSchemaV2); err != nil {
		return err
	}
	var candidates p1bCandidateSetArtifactV2
	if err := artifactDecodeStrictJSONV2(inputs.CandidateSetRaw, &candidates); err != nil {
		return err
	}
	if err := validateP1bRawEnvelopeV2(candidates.p1bRawSupportEnvelopeV2, candidateSetArtifactSchemaV2); err != nil {
		return err
	}
	var profiles p1bProfileRegistryArtifactV2
	if err := artifactDecodeStrictJSONV2(inputs.ProfileRegistryRaw, &profiles); err != nil {
		return err
	}
	if err := validateP1bRawEnvelopeV2(profiles.p1bRawSupportEnvelopeV2, profileRegistryArtifactSchemaV2); err != nil {
		return err
	}
	if !reflect.DeepEqual(calibration.ProspectiveEvidence, profiles.ProspectiveEvidence) {
		return errors.New("p1b calibration and profile-registry raw evidence differ")
	}
	var slots p1bSlotTemplateArtifactV2
	if err := artifactDecodeStrictJSONV2(inputs.SlotTemplateRaw, &slots); err != nil {
		return err
	}
	if err := validateP1bRawEnvelopeV2(slots.p1bRawSupportEnvelopeV2, slotTemplateArtifactSchemaV2); err != nil || slots.OpportunityStreamSHA256 != SHA256(inputs.StreamRaw) || !equalCohortSpecsV2(slots.Cohorts, config.Cohorts) {
		return errors.New("p1b slot template does not bind exact stream/cohorts")
	}
	var bundle MethodBuilderBundleV2
	if err := artifactDecodeStrictJSONV2(inputs.MethodBuilderBundleRaw, &bundle); err != nil {
		return err
	}
	if bundle.SchemaVersion != E1MethodBuilderBundleSchemaV2 || bundle.Status != "locked_preoutcome" || bundle.EvidenceLabel != p1bEvidenceLabelV2 || bundle.SupportMode != E1SupportModeP1bExact || bundle.TestOnly || bundle.ComparatorMethod != config.ComparatorMethod || bundle.BudgetCalibrationSHA256 != SHA256(inputs.BudgetCalibrationRaw) || bundle.CalibrationArtifactSHA256 != SHA256(inputs.CalibrationArtifactRaw) || bundle.CandidateSetSHA256 != SHA256(inputs.CandidateSetRaw) || bundle.ProfileRegistrySHA256 != SHA256(inputs.ProfileRegistryRaw) || bundle.SlotTemplateSHA256 != SHA256(inputs.SlotTemplateRaw) || len(bundle.OutcomeFieldsUsed) != 0 {
		return errors.New("p1b method-builder bundle does not bind every exact raw support")
	}
	methodTemplates, err := nestedJSONObjectV2(template, "method_templates")
	if err != nil {
		return err
	}
	selectedTemplate, err := nestedJSONObjectV2(methodTemplates, config.ComparatorMethod)
	if err != nil {
		return err
	}
	if config.ComparatorMethod == "gov_ar" {
		builderTemplateSHA, _ := jsonStringFieldV2(selectedTemplate, "builder_config_template_sha256")
		if bundle.BuilderKind != "govar_prospective_builder" || bundle.BuilderConfigSHA256 != builderTemplateSHA || bundle.ProspectiveEvidence == nil || !reflect.DeepEqual(*bundle.ProspectiveEvidence, profiles.ProspectiveEvidence) {
			return errors.New("p1b GOV-AR builder bundle omits exact prospective raw evidence")
		}
		frozen, err := time.Parse(time.RFC3339Nano, bundle.FrozenAt)
		if err != nil {
			return errors.New("p1b GOV-AR builder frozen_at is invalid")
		}
		projected := make([]Opportunity, len(opportunities))
		for index := range opportunities {
			projected[index] = projectOpportunityV2(opportunities[index])
		}
		built, err := BuildProspectiveGOVARMethodConfig(config.runtimeConfig(), projected, bundle.TenantRiskPPB, bundle.DriftThresholdPPB, bundle.MaxAgeSeconds, bundle.RevalidationMinimumSupport, frozen, *bundle.ProspectiveEvidence)
		if err != nil || !reflect.DeepEqual(built, config.MethodConfig) {
			return fmt.Errorf("p1b GOV-AR MethodConfig does not independently rebuild: %w", err)
		}
		if !reflect.DeepEqual(built.GOVAR.CandidateSet, candidates.Candidates) {
			return errors.New("p1b raw candidate set differs from rebuilt GOV-AR candidates")
		}
	} else {
		methodSHA, _ := jsonStringFieldV2(selectedTemplate, "method_config_sha256")
		if bundle.BuilderKind != "fixed_method_template" || bundle.ProspectiveEvidence != nil || methodSHA != config.MethodConfigSHA256 {
			return errors.New("p1b fixed method config differs from its prospective template")
		}
	}
	if !reflect.DeepEqual(bundle.MethodConfig, config.MethodConfig) || bundle.MethodConfigSHA256 != config.MethodConfigSHA256 {
		return errors.New("p1b bundle final MethodConfig differs from config")
	}
	var intent ExecutionIntentV2
	if err := artifactDecodeStrictJSONV2(inputs.ExecutionIntentRaw, &intent); err != nil {
		return err
	}
	if intent.SchemaVersion != E1ExecutionIntentSchemaV2 || intent.Status != "locked_preoutcome" || intent.EvidenceLabel != p1bEvidenceLabelV2 || intent.SupportMode != E1SupportModeP1bExact || intent.TestOnly || intent.StreamKey != stream.StreamKey || !reflect.DeepEqual(intent.Config, projectExecutionIntentConfigV2(config)) || intent.OpportunityStreamSHA256 != SHA256(inputs.StreamRaw) || intent.MappingArtifactSHA256 != SHA256(inputs.PreOutcomeMappingRaw) || intent.CohortRegistrySHA256 != SHA256(inputs.CohortRegistryRaw) || intent.ProfileExecutionContractSHA256 != SHA256(inputs.ProfileExecutionContractRaw) || intent.BudgetCalibrationSHA256 != SHA256(inputs.BudgetCalibrationRaw) || intent.CalibrationArtifactSHA256 != SHA256(inputs.CalibrationArtifactRaw) || intent.CandidateSetSHA256 != SHA256(inputs.CandidateSetRaw) || intent.ProfileRegistrySHA256 != SHA256(inputs.ProfileRegistryRaw) || intent.SlotTemplateSHA256 != SHA256(inputs.SlotTemplateRaw) || intent.MethodBuilderBundleSHA256 != SHA256(inputs.MethodBuilderBundleRaw) || intent.DesignManifestSHA256 != SHA256(inputs.DesignManifestRaw) || intent.DecisionConfigTemplateSHA256 != SHA256(inputs.DecisionConfigTemplateRaw) || len(intent.OutcomeFieldsUsed) != 0 {
		return errors.New("p1b execution intent differs from exact stream/method/config/support projection")
	}
	return nil
}

func validateP1bRawEnvelopeV2(value p1bRawSupportEnvelopeV2, schema string) error {
	if value.SchemaVersion != schema || value.Status != "locked_preoutcome" || value.EvidenceLabel != p1bEvidenceLabelV2 || value.SupportMode != E1SupportModeP1bExact || value.TestOnly || len(value.OutcomeFieldsUsed) != 0 {
		return errors.New("p1b raw support envelope is not locked, outcome-free, and non-citable")
	}
	return nil
}

func validateP1bAuthorizationV2(config ConfigV2, inputs ArtifactPreflightInputsV2, source p1bSourceBindingV2) error {
	var value IndependentAuthorizationV2
	if err := artifactDecodeStrictJSONV2(inputs.IndependentAuthorizationRaw, &value); err != nil {
		return err
	}
	lock := config.ProvenanceLock
	if value.SchemaVersion != p1bIndependentAuthorizationSchemaV2 || value.Verdict != "approved" || value.SupportMode != E1SupportModeP1bExact || value.EvidenceLabel != p1bEvidenceLabelV2 || !validIdentity(value.AuthorizationContext) || value.TestOnly || !value.IndependentAuthorization || !value.FreshAuthorization || !value.FullCampaignExecutionAuthorized || value.FinalEvidenceAuthorized || value.ScientificClaimsAuthorized || value.UnresolvedCritical != 0 || value.UnresolvedMajor != 0 || value.OutcomesInspected || value.PilotValuesInspected || value.DatasetID != UsageDatasetV2 || value.Split != UsageSplitV2 || value.ExperimentID != config.ExperimentID || value.RunID != config.RunID || value.CellID != config.CellID || value.Scenario != config.Scenario || value.Seed != config.Seed || value.StreamKey != fmt.Sprintf("%s/seed-%d", config.Scenario, config.Seed) || value.ExecutionSubjectSHA256 != lock.ExecutionSubjectSHA256 || value.ExecutionIntentSHA256 != lock.ExecutionIntentSHA256 || value.ProtocolSHA256 != lock.ProtocolSHA256 || value.SourceCodeBindingSHA256 != lock.SourceCodeBindingSHA256 || value.SourceManifestSHA256 != lock.SourceManifestSHA256 || value.PilotUsageBindingSHA256 != lock.PilotUsageBindingSHA256 || value.RuntimeCapabilitySHA256 != lock.RuntimeCapabilitySHA256 || value.DesignSpecSHA256 != lock.DesignSpecSHA256 || value.DesignLockSHA256 != lock.DesignLockSHA256 || value.IndependentDesignReviewSHA256 != lock.IndependentDesignReviewSHA256 || value.DesignManifestSHA256 != lock.DesignManifestSHA256 || value.CohortRegistrySHA256 != lock.CohortRegistrySHA256 || value.ProfileExecutionContractSHA256 != lock.ProfileExecutionContractSHA256 || value.BudgetCalibrationSHA256 != lock.BudgetCalibrationSHA256 || value.CalibrationArtifactSHA256 != lock.CalibrationArtifactSHA256 || value.CandidateSetSHA256 != lock.CandidateSetSHA256 || value.ProfileRegistrySHA256 != lock.ProfileRegistrySHA256 || value.SlotTemplateSHA256 != lock.SlotTemplateSHA256 || value.MethodBuilderBundleSHA256 != lock.MethodBuilderBundleSHA256 {
		return errors.New("p1b independent authorization does not bind exact fresh context, subject, intent, and supports")
	}
	for name, digest := range map[string]string{"combined E0": value.CombinedE0AuthorizationSHA256, "D-G checks": value.DGChecksSHA256, "focused tests": value.FocusedTestsSHA256} {
		if !shaPattern.MatchString(digest) || digest == strings.Repeat("0", 64) {
			return fmt.Errorf("p1b authorization %s proof digest is absent", name)
		}
	}
	if config.SoftwareSHA256 != SHA256(inputs.SourceCodeBindingRaw) || !shaPattern.MatchString(source.ReviewedSourceRootSHA256) || SHA256(inputs.IndependentAuthorizationRaw) != lock.IndependentAuthorizationSHA256 {
		return errors.New("p1b authorization/source bytes do not bind final execution lock")
	}
	return nil
}

// canonicalP1bObjectDigestWithoutFieldV2 mirrors Python's sort_keys=True
// canonicalization recursively. Marshaling a map[string]json.RawMessage sorts
// only its outer keys and preserves nested source order, which is insufficient
// for the P1b design/source manifests produced by the independent Python
// implementation.
func canonicalP1bObjectDigestWithoutFieldV2(object map[string]json.RawMessage, field string) string {
	payload := make(map[string]json.RawMessage, len(object)-1)
	for key, value := range object {
		if key != field {
			payload[key] = value
		}
	}
	raw, _ := json.Marshal(payload)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var recursivelySortable any
	if err := decoder.Decode(&recursivelySortable); err != nil {
		return ""
	}
	var canonical bytes.Buffer
	encoder := json.NewEncoder(&canonical)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(recursivelySortable); err != nil {
		return ""
	}
	return SHA256(bytes.TrimSuffix(canonical.Bytes(), []byte{'\n'}))
}

func selectedP1bStreamV2(manifest map[string]json.RawMessage, streamKey string) (p1bStreamManifestV2, error) {
	var streams []p1bStreamManifestV2
	if err := json.Unmarshal(manifest["streams"], &streams); err != nil {
		return p1bStreamManifestV2{}, errors.New("p1b design manifest streams are invalid")
	}
	var selected *p1bStreamManifestV2
	for index := range streams {
		if streams[index].StreamKey == streamKey {
			if selected != nil {
				return p1bStreamManifestV2{}, errors.New("p1b design manifest duplicates selected stream")
			}
			selected = &streams[index]
		}
	}
	if selected == nil {
		return p1bStreamManifestV2{}, fmt.Errorf("p1b design manifest lacks selected stream %s", streamKey)
	}
	return *selected, nil
}

func requireExactJSONKeysV2(object map[string]json.RawMessage, keys []string, label string) error {
	if len(object) != len(keys) {
		return fmt.Errorf("%s keys differ", label)
	}
	for _, key := range keys {
		if _, ok := object[key]; !ok {
			return fmt.Errorf("%s lacks exact key %s", label, key)
		}
	}
	return nil
}

func nestedJSONObjectV2(parent map[string]json.RawMessage, key string) (map[string]json.RawMessage, error) {
	raw, ok := parent[key]
	if !ok {
		return nil, fmt.Errorf("missing JSON object %s", key)
	}
	return decodeJSONObjectV2(raw)
}

func jsonInt64FieldV2(object map[string]json.RawMessage, key string) (int64, error) {
	raw, ok := object[key]
	if !ok {
		return 0, fmt.Errorf("missing JSON field %s", key)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value json.Number
	if err := decoder.Decode(&value); err != nil {
		return 0, fmt.Errorf("JSON field %s is not an integer", key)
	}
	number, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("JSON field %s is not a canonical int64", key)
	}
	return number, nil
}
