package govarexperiment

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

const (
	E1ExecutionIntentSchemaV2     = "govar-e1-execution-intent-v1"
	E1MethodBuilderBundleSchemaV2 = "govar-e1-method-builder-bundle-v1"

	fixtureEvidenceLabelV2 = "test_fixture_non_citable"
	p1bEvidenceLabelV2     = "development_non_citable"

	fixtureProtocolSchemaV2                 = "govar-p1b-protocol-test-fixture-v1"
	p1bProtocolSchemaV2                     = "govar-p1b-protocol-candidate-v2"
	fixtureSourceCodeBindingSchemaV2        = "govar-p1b-source-code-binding-test-fixture-v1"
	p1bSourceCodeBindingSchemaV2            = "govar-p1b-source-binding-v1"
	fixtureSourceManifestSchemaV2           = "govar-p1b-source-manifest-test-fixture-v1"
	p1bSourceManifestSchemaV2               = "article3-azure-development-subsplits-v2"
	fixturePilotUsageBindingSchemaV2        = "govar-p1b-pilot-usage-binding-test-fixture-v1"
	p1bPilotUsageBindingSchemaV2            = "govar-p1b-pilot-usage-binding-v2"
	fixtureRuntimeCapabilitySchemaV2        = "govar-p1b-runtime-capability-test-fixture-v1"
	p1bRuntimeCapabilitySchemaV2            = "govar-p1b-runtime-capability-v2"
	p1bDesignSpecSchemaV2                   = "govar-p1b-outcome-free-design-v1"
	p1bDecisionConfigTemplateSchemaV2       = "govar-p1b-decision-config-template-v1"
	fixtureIndependentDesignReviewSchemaV2  = "govar-p1b-pretrace-review-test-fixture-v1"
	p1bIndependentDesignReviewSchemaV2      = "govar-p1b-pretrace-independent-design-review-v1"
	p1bDesignManifestSchemaV2               = "govar-p1b-outcome-free-design-manifest-v1"
	p1bCohortRegistrySchemaV2               = "govar-p1b-cohort-registry-v1"
	p1bProfileExecutionContractSchemaV2     = "govar-p1b-prospective-profile-execution-contract-v1"
	budgetCalibrationArtifactSchemaV2       = "govar-e1-budget-calibration-artifact-v1"
	calibrationArtifactSchemaV2             = "govar-e1-calibration-artifact-v1"
	candidateSetArtifactSchemaV2            = "govar-e1-candidate-set-v1"
	profileRegistryArtifactSchemaV2         = "govar-e1-profile-registry-v1"
	slotTemplateArtifactSchemaV2            = "govar-e1-slot-template-v1"
	fixtureIndependentAuthorizationSchemaV2 = "govar-p1b-independent-authorization-test-fixture-v2"
	p1bIndependentAuthorizationSchemaV2     = "govar-p1b-independent-authorization-v2"
)

// ArtifactPreflightInputsV2 deliberately has no usage observation or producer
// evidence field. A caller can therefore validate the complete pre-outcome
// execution subject and independent authorization without opening protected
// development-usage bytes.
type ArtifactPreflightInputsV2 struct {
	SupportMode                 E1SupportModeV2
	ConfigRaw                   []byte
	StreamRaw                   []byte
	PreOutcomeMappingRaw        []byte
	ProtocolRaw                 []byte
	SourceCodeBindingRaw        []byte
	SourceManifestRaw           []byte
	PilotUsageBindingRaw        []byte
	RuntimeCapabilityRaw        []byte
	DesignSpecRaw               []byte
	DecisionConfigTemplateRaw   []byte
	IndependentDesignReviewRaw  []byte
	DesignManifestRaw           []byte
	CohortRegistryRaw           []byte
	ProfileExecutionContractRaw []byte
	BudgetCalibrationRaw        []byte
	CalibrationArtifactRaw      []byte
	CandidateSetRaw             []byte
	ProfileRegistryRaw          []byte
	SlotTemplateRaw             []byte
	MethodBuilderBundleRaw      []byte
	ExecutionIntentRaw          []byte
	IndependentAuthorizationRaw []byte
}

type artifactSupportValidationV2 struct {
	evidenceLabel                  string
	protocolSchema                 string
	sourceCodeBindingSchema        string
	sourceManifestSchema           string
	pilotUsageBindingSchema        string
	runtimeCapabilitySchema        string
	designSpecSchema               string
	decisionConfigTemplateSchema   string
	independentDesignReviewSchema  string
	designManifestSchema           string
	cohortRegistrySchema           string
	profileExecutionContractSchema string
	budgetCalibrationSchema        string
	calibrationArtifactSchema      string
	candidateSetSchema             string
	profileRegistrySchema          string
	slotTemplateSchema             string
	methodBuilderBundleSchema      string
	executionIntentSchema          string
	independentAuthorizationSchema string
}

type artifactPreflightResultV2 struct {
	config        ConfigV2
	opportunities []OpportunityV2
	mappings      []PreOutcomeMappingV2
	support       artifactSupportValidationV2
}

func artifactPreflightInputsFromV2(inputs ArtifactInputsV2) ArtifactPreflightInputsV2 {
	return ArtifactPreflightInputsV2{
		SupportMode: inputs.SupportMode, ConfigRaw: inputs.ConfigRaw, StreamRaw: inputs.StreamRaw,
		PreOutcomeMappingRaw: inputs.PreOutcomeMappingRaw, ProtocolRaw: inputs.ProtocolRaw,
		SourceCodeBindingRaw: inputs.SourceCodeBindingRaw, SourceManifestRaw: inputs.SourceManifestRaw,
		PilotUsageBindingRaw: inputs.PilotUsageBindingRaw, RuntimeCapabilityRaw: inputs.RuntimeCapabilityRaw,
		DesignSpecRaw: inputs.DesignSpecRaw, DecisionConfigTemplateRaw: inputs.DecisionConfigTemplateRaw,
		IndependentDesignReviewRaw: inputs.IndependentDesignReviewRaw, DesignManifestRaw: inputs.DesignManifestRaw,
		CohortRegistryRaw: inputs.CohortRegistryRaw, ProfileExecutionContractRaw: inputs.ProfileExecutionContractRaw,
		BudgetCalibrationRaw:   inputs.BudgetCalibrationRaw,
		CalibrationArtifactRaw: inputs.CalibrationArtifactRaw, CandidateSetRaw: inputs.CandidateSetRaw,
		ProfileRegistryRaw: inputs.ProfileRegistryRaw, SlotTemplateRaw: inputs.SlotTemplateRaw,
		MethodBuilderBundleRaw: inputs.MethodBuilderBundleRaw, ExecutionIntentRaw: inputs.ExecutionIntentRaw,
		IndependentAuthorizationRaw: inputs.IndependentAuthorizationRaw,
	}
}

// ValidateArtifactPreflightV2 is the public no-reveal boundary used by the CLI.
// It validates config, exact stream/mapping, every raw support, execution
// intent, and authorization. It never accepts or reads a usage observation.
func ValidateArtifactPreflightV2(inputs ArtifactPreflightInputsV2) error {
	_, err := preflightArtifactInputsV2(inputs)
	return err
}

func preflightArtifactInputsV2(inputs ArtifactPreflightInputsV2) (artifactPreflightResultV2, error) {
	if inputs.SupportMode == E1SupportModeP1bExact {
		return preflightP1bExactArtifactInputsV2(inputs)
	}
	if err := artifactRejectDuplicateJSONV2(inputs.ConfigRaw); err != nil {
		return artifactPreflightResultV2{}, fmt.Errorf("config JSON: %w", err)
	}
	config, _, err := ParseConfigV2(inputs.ConfigRaw)
	if err != nil {
		return artifactPreflightResultV2{}, err
	}
	if inputs.SupportMode != config.ProvenanceLock.SupportMode {
		return artifactPreflightResultV2{}, errors.New("explicit support mode differs from the config execution subject")
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
		return artifactPreflightResultV2{}, errors.New("pre-outcome mapping and opportunity stream row counts differ")
	}
	for index := range opportunities {
		if mappings[index].Opportunity() != opportunities[index] {
			return artifactPreflightResultV2{}, fmt.Errorf("pre-outcome mapping row %d does not exactly project to the opportunity stream", index+1)
		}
	}
	lock := config.ProvenanceLock
	if lock.OpportunityStreamSHA256 != SHA256(inputs.StreamRaw) || lock.MappingArtifactSHA256 != SHA256(inputs.PreOutcomeMappingRaw) {
		return artifactPreflightResultV2{}, errors.New("execution subject does not bind exact stream and mapping bytes")
	}
	setSHA, err := StreamPreOutcomeSetSHA256V2(opportunities)
	if err != nil || setSHA != lock.AssignedPreOutcomeSetSHA256 {
		return artifactPreflightResultV2{}, errors.New("assigned pre-outcome set does not recompute from the exact stream")
	}
	sequenceSHA, err := StreamPreOutcomeSequenceSHA256V2(opportunities)
	if err != nil || sequenceSHA != lock.StreamPreOutcomeSequenceSHA256 {
		return artifactPreflightResultV2{}, errors.New("pre-outcome sequence does not recompute from the exact stream")
	}
	support, err := validateLockedSupportInputsV2(config, opportunities, inputs)
	if err != nil {
		return artifactPreflightResultV2{}, err
	}
	return artifactPreflightResultV2{config: config, opportunities: opportunities, mappings: mappings, support: support}, nil
}

func evidenceLabelForSupportModeV2(mode E1SupportModeV2) (string, error) {
	switch mode {
	case E1SupportModeFixture:
		return fixtureEvidenceLabelV2, nil
	case E1SupportModeP1bExact:
		return p1bEvidenceLabelV2, nil
	default:
		return "", fmt.Errorf("unsupported E1 support mode %q", mode)
	}
}

func supportSchemasForModeV2(mode E1SupportModeV2) (artifactSupportValidationV2, error) {
	label, err := evidenceLabelForSupportModeV2(mode)
	if err != nil {
		return artifactSupportValidationV2{}, err
	}
	result := artifactSupportValidationV2{
		evidenceLabel: label, designSpecSchema: p1bDesignSpecSchemaV2,
		decisionConfigTemplateSchema: p1bDecisionConfigTemplateSchemaV2,
		designManifestSchema:         p1bDesignManifestSchemaV2, cohortRegistrySchema: p1bCohortRegistrySchemaV2,
		profileExecutionContractSchema: p1bProfileExecutionContractSchemaV2,
		budgetCalibrationSchema:        budgetCalibrationArtifactSchemaV2,
		calibrationArtifactSchema:      calibrationArtifactSchemaV2, candidateSetSchema: candidateSetArtifactSchemaV2,
		profileRegistrySchema: profileRegistryArtifactSchemaV2, slotTemplateSchema: slotTemplateArtifactSchemaV2,
		methodBuilderBundleSchema: E1MethodBuilderBundleSchemaV2, executionIntentSchema: E1ExecutionIntentSchemaV2,
	}
	if mode == E1SupportModeFixture {
		result.protocolSchema = fixtureProtocolSchemaV2
		result.sourceCodeBindingSchema = fixtureSourceCodeBindingSchemaV2
		result.sourceManifestSchema = fixtureSourceManifestSchemaV2
		result.pilotUsageBindingSchema = fixturePilotUsageBindingSchemaV2
		result.runtimeCapabilitySchema = fixtureRuntimeCapabilitySchemaV2
		result.independentDesignReviewSchema = fixtureIndependentDesignReviewSchemaV2
		result.independentAuthorizationSchema = fixtureIndependentAuthorizationSchemaV2
	} else {
		result.protocolSchema = p1bProtocolSchemaV2
		result.sourceCodeBindingSchema = p1bSourceCodeBindingSchemaV2
		result.sourceManifestSchema = p1bSourceManifestSchemaV2
		result.pilotUsageBindingSchema = p1bPilotUsageBindingSchemaV2
		result.runtimeCapabilitySchema = p1bRuntimeCapabilitySchemaV2
		result.independentDesignReviewSchema = p1bIndependentDesignReviewSchemaV2
		result.independentAuthorizationSchema = p1bIndependentAuthorizationSchemaV2
	}
	return result, nil
}

func validateLockedSupportInputsV2(config ConfigV2, opportunities []OpportunityV2, inputs ArtifactPreflightInputsV2) (artifactSupportValidationV2, error) {
	schemas, err := supportSchemasForModeV2(inputs.SupportMode)
	if err != nil {
		return artifactSupportValidationV2{}, err
	}
	if inputs.SupportMode == E1SupportModeP1bExact {
		return validateP1bExactSupportInputsV2(config, opportunities, inputs, schemas)
	}
	if err := validateFixtureSupportInputsV2(config, opportunities, inputs, schemas); err != nil {
		return artifactSupportValidationV2{}, err
	}
	return schemas, nil
}

type fixtureEnvelopeV2 struct {
	SchemaVersion     string          `json:"schema_version"`
	Status            string          `json:"status"`
	EvidenceLabel     string          `json:"evidence_label"`
	SupportMode       E1SupportModeV2 `json:"support_mode"`
	TestOnly          bool            `json:"test_only"`
	OutcomeFieldsUsed []string        `json:"outcome_fields_used"`
}

type fixtureSourceManifestV2 struct {
	SchemaVersion             string `json:"schema_version"`
	Status                    string `json:"status"`
	EvidenceLabel             string `json:"evidence_label"`
	DatasetID                 string `json:"dataset_id"`
	Split                     string `json:"split"`
	Rows                      int64  `json:"rows"`
	OrderedPreOutcomeIDSHA256 string `json:"ordered_preoutcome_id_sha256"`
}

type fixturePilotBindingV2 struct {
	SchemaVersion              string `json:"schema_version"`
	Status                     string `json:"status"`
	EvidenceLabel              string `json:"evidence_label"`
	DatasetID                  string `json:"dataset_id"`
	ParentSplit                string `json:"parent_split"`
	Subsplit                   string `json:"subsplit"`
	Mode                       string `json:"mode"`
	QualityAvailable           bool   `json:"quality_available"`
	H3QualityMeasured          bool   `json:"h3_quality_measured"`
	SubsplitRows               int64  `json:"subsplit_rows"`
	SubsplitPreOutcomeIDSHA256 string `json:"subsplit_preoutcome_ids_sha256"`
	SourceManifestSHA256       string `json:"source_manifest_sha256"`
}

type fixtureSourceCodeBindingV2 struct {
	fixtureEnvelopeV2
	ReviewedSourceRootSHA256 string `json:"reviewed_source_root_sha256"`
}

type fixtureRuntimeCapabilityV2 struct {
	fixtureEnvelopeV2
	SourceCodeBindingSHA256 string `json:"source_code_binding_sha256"`
	FullCampaignSupported   bool   `json:"full_campaign_supported"`
}

type p1bIndependentReviewV2 struct {
	SchemaVersion                   string `json:"schema_version"`
	Verdict                         string `json:"verdict"`
	ReviewScope                     string `json:"review_scope"`
	PretracePreparationAuthorized   bool   `json:"pretrace_preparation_authorized"`
	FullCampaignExecutionAuthorized bool   `json:"full_campaign_execution_authorized"`
	FinalEvidenceAuthorized         bool   `json:"final_evidence_authorized"`
	ScientificClaimsAuthorized      bool   `json:"scientific_claims_authorized"`
	IndependentReview               bool   `json:"independent_review"`
	FreshReview                     bool   `json:"fresh_review"`
	UnresolvedCritical              int64  `json:"unresolved_critical"`
	UnresolvedMajor                 int64  `json:"unresolved_major"`
	OutcomesInspected               bool   `json:"outcomes_inspected"`
	DatasetID                       string `json:"dataset_id"`
	Split                           string `json:"split"`
	DesignLockSHA256                string `json:"design_lock_sha256"`
	DecisionConfigTemplateSHA256    string `json:"decision_config_template_sha256"`
	GeneratorSHA256                 string `json:"generator_sha256"`
	ProtocolSHA256                  string `json:"protocol_sha256"`
	SourceManifestSHA256            string `json:"source_manifest_sha256"`
	PilotUsageBindingSHA256         string `json:"pilot_usage_binding_sha256"`
	PreOutcomeSidecarSHA256         string `json:"preoutcome_sidecar_sha256"`
}

type p1bArtifactSpecV2 struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Rows   int64  `json:"rows,omitempty"`
}

type p1bStreamManifestV2 struct {
	StreamKey                         string            `json:"stream_key"`
	Scenario                          string            `json:"scenario"`
	Seed                              uint64            `json:"seed"`
	ScenarioFactorBinding             json.RawMessage   `json:"scenario_factor_binding"`
	Rows                              int64             `json:"rows"`
	HierarchicalTenantWindowUnits     int64             `json:"hierarchical_tenant_window_units"`
	EventsPerHierarchicalTenantWindow int64             `json:"events_per_hierarchical_tenant_window_unit"`
	TenantWindowsIndependent          bool              `json:"tenant_windows_are_independent_replicates"`
	AssignedPreOutcomeSetSHA256       string            `json:"assigned_preoutcome_set_sha256"`
	StreamPreOutcomeSequenceSHA256    string            `json:"stream_preoutcome_sequence_sha256"`
	WindowAssignmentSHA256            string            `json:"window_assignment_sha256"`
	DecisionConfigTemplateSHA256      string            `json:"decision_config_template_sha256"`
	GOVARStreamBuilderInputSHA256     string            `json:"govar_stream_builder_input_binding_sha256,omitempty"`
	ProspectiveProfileExecution       json.RawMessage   `json:"prospective_profile_execution"`
	MappingArtifact                   p1bArtifactSpecV2 `json:"mapping_artifact"`
	OpportunityStreamArtifact         p1bArtifactSpecV2 `json:"opportunity_stream_artifact"`
	CohortRegistryArtifact            p1bArtifactSpecV2 `json:"cohort_registry_artifact"`
	ProfileExecutionContractArtifact  p1bArtifactSpecV2 `json:"profile_execution_contract_artifact"`
	SlotTemplateArtifact              p1bArtifactSpecV2 `json:"slot_template_artifact"`
	CalibrationArtifact               p1bArtifactSpecV2 `json:"calibration_artifact"`
	ProfileRegistryArtifact           p1bArtifactSpecV2 `json:"profile_registry_artifact"`
	OutcomeFieldsUsed                 []string          `json:"outcome_fields_used"`
}

type p1bCohortRegistryV2 struct {
	SchemaVersion                     string          `json:"schema_version"`
	Scenario                          string          `json:"scenario"`
	Seed                              uint64          `json:"seed"`
	HierarchicalTenantWindowUnits     int64           `json:"hierarchical_tenant_window_units"`
	EventsPerHierarchicalTenantWindow int64           `json:"events_per_hierarchical_tenant_window_unit"`
	TenantWindowsIndependent          bool            `json:"tenant_windows_are_independent_replicates"`
	ProspectiveProfileExecution       json.RawMessage `json:"prospective_profile_execution"`
	Cohorts                           []CohortSpec    `json:"cohorts"`
}

type executionIntentConfigProjectionV2 struct {
	SchemaVersion               string                   `json:"schema_version"`
	RecordType                  string                   `json:"record_type"`
	ExperimentID                string                   `json:"experiment_id"`
	RunID                       string                   `json:"run_id"`
	CellID                      string                   `json:"cell_id"`
	Scenario                    string                   `json:"scenario"`
	Seed                        uint64                   `json:"seed"`
	RegistryID                  string                   `json:"registry_id"`
	ProtocolMethodID            string                   `json:"protocol_method_id"`
	ComparatorMethod            string                   `json:"comparator_method"`
	ProductionReservationMode   string                   `json:"production_reservation_mode"`
	MethodConfig                MethodConfig             `json:"method_config"`
	MethodConfigSHA256          string                   `json:"method_config_sha256"`
	ClusterID                   string                   `json:"cluster_id,omitempty"`
	TrialID                     string                   `json:"trial_id,omitempty"`
	VirtualStart                string                   `json:"virtual_start"`
	EvidenceTier                string                   `json:"evidence_tier"`
	SoftwareSHA256              string                   `json:"software_sha256"`
	DataSHA256                  string                   `json:"data_sha256"`
	ProtocolSHA256              string                   `json:"protocol_sha256"`
	SplitSHA256                 string                   `json:"split_sha256"`
	SourceSHA256                string                   `json:"source_sha256"`
	UsageDatasetID              string                   `json:"usage_dataset_id"`
	UsageSplit                  string                   `json:"usage_split"`
	InputPriceMicrosPerMillion  int64                    `json:"input_price_micros_per_million"`
	OutputPriceMicrosPerMillion int64                    `json:"output_price_micros_per_million"`
	VerifiedOutputCapTokens     int64                    `json:"verified_output_cap_tokens"`
	EstimateOutputTokens        int64                    `json:"estimate_output_tokens,omitempty"`
	MarginOutputTokens          int64                    `json:"margin_output_tokens,omitempty"`
	Cohorts                     []CohortSpec             `json:"cohorts,omitempty"`
	CellType                    E1CellType               `json:"cell_type"`
	Concurrency                 E1Concurrency            `json:"concurrency"`
	SettlementDelaySeconds      E1SettlementDelaySeconds `json:"settlement_delay_seconds"`
	BudgetLevel                 E1BudgetLevel            `json:"budget_level"`
	RiskTargetPPB               E1RiskTargetPPB          `json:"risk_target_ppb"`
}

func projectExecutionIntentConfigV2(config ConfigV2) executionIntentConfigProjectionV2 {
	return executionIntentConfigProjectionV2{
		SchemaVersion: config.SchemaVersion, RecordType: config.RecordType, ExperimentID: config.ExperimentID,
		RunID: config.RunID, CellID: config.CellID, Scenario: config.Scenario, Seed: config.Seed,
		RegistryID: config.RegistryID, ProtocolMethodID: config.ProtocolMethodID, ComparatorMethod: config.ComparatorMethod,
		ProductionReservationMode: config.ProductionReservationMode, MethodConfig: config.MethodConfig,
		MethodConfigSHA256: config.MethodConfigSHA256, ClusterID: config.ClusterID, TrialID: config.TrialID,
		VirtualStart: config.VirtualStart, EvidenceTier: config.EvidenceTier, SoftwareSHA256: config.SoftwareSHA256,
		DataSHA256: config.DataSHA256, ProtocolSHA256: config.ProtocolSHA256, SplitSHA256: config.SplitSHA256,
		SourceSHA256: config.SourceSHA256, UsageDatasetID: config.UsageDatasetID, UsageSplit: config.UsageSplit,
		InputPriceMicrosPerMillion:  config.InputPriceMicrosPerMillion,
		OutputPriceMicrosPerMillion: config.OutputPriceMicrosPerMillion,
		VerifiedOutputCapTokens:     config.VerifiedOutputCapTokens, EstimateOutputTokens: config.EstimateOutputTokens,
		MarginOutputTokens: config.MarginOutputTokens, Cohorts: append([]CohortSpec(nil), config.Cohorts...),
		CellType: config.CellType, Concurrency: config.Concurrency,
		SettlementDelaySeconds: config.SettlementDelaySeconds, BudgetLevel: config.BudgetLevel,
		RiskTargetPPB: config.RiskTargetPPB,
	}
}

func (projection executionIntentConfigProjectionV2) materialize(lock E1ProvenanceLockV2, usageBindingSHA256 string) ConfigV2 {
	return ConfigV2{
		SchemaVersion: projection.SchemaVersion, RecordType: projection.RecordType,
		ExperimentID: projection.ExperimentID, RunID: projection.RunID, CellID: projection.CellID,
		Scenario: projection.Scenario, Seed: projection.Seed, RegistryID: projection.RegistryID,
		ProtocolMethodID: projection.ProtocolMethodID, ComparatorMethod: projection.ComparatorMethod,
		ProductionReservationMode: projection.ProductionReservationMode, MethodConfig: projection.MethodConfig,
		MethodConfigSHA256: projection.MethodConfigSHA256, ClusterID: projection.ClusterID, TrialID: projection.TrialID,
		VirtualStart: projection.VirtualStart, EvidenceTier: projection.EvidenceTier,
		SoftwareSHA256: projection.SoftwareSHA256, DataSHA256: projection.DataSHA256,
		ProtocolSHA256: projection.ProtocolSHA256, SplitSHA256: projection.SplitSHA256,
		SourceSHA256: projection.SourceSHA256, UsageDatasetID: projection.UsageDatasetID, UsageSplit: projection.UsageSplit,
		ProvenanceLock: lock, UsageBindingSHA256: usageBindingSHA256,
		InputPriceMicrosPerMillion:  projection.InputPriceMicrosPerMillion,
		OutputPriceMicrosPerMillion: projection.OutputPriceMicrosPerMillion,
		VerifiedOutputCapTokens:     projection.VerifiedOutputCapTokens,
		EstimateOutputTokens:        projection.EstimateOutputTokens, MarginOutputTokens: projection.MarginOutputTokens,
		Cohorts: append([]CohortSpec(nil), projection.Cohorts...), E1Factors: E1Factors{
			CellType: projection.CellType, Concurrency: projection.Concurrency,
			SettlementDelaySeconds: projection.SettlementDelaySeconds, BudgetLevel: projection.BudgetLevel,
			RiskTargetPPB: projection.RiskTargetPPB,
		},
	}
}

type ExecutionIntentV2 struct {
	SchemaVersion                  string                            `json:"schema_version"`
	Status                         string                            `json:"status"`
	EvidenceLabel                  string                            `json:"evidence_label"`
	SupportMode                    E1SupportModeV2                   `json:"support_mode"`
	TestOnly                       bool                              `json:"test_only"`
	StreamKey                      string                            `json:"stream_key"`
	Config                         executionIntentConfigProjectionV2 `json:"config"`
	OpportunityStreamSHA256        string                            `json:"opportunity_stream_sha256"`
	MappingArtifactSHA256          string                            `json:"mapping_artifact_sha256"`
	CohortRegistrySHA256           string                            `json:"cohort_registry_sha256"`
	ProfileExecutionContractSHA256 string                            `json:"profile_execution_contract_sha256"`
	BudgetCalibrationSHA256        string                            `json:"budget_calibration_sha256"`
	CalibrationArtifactSHA256      string                            `json:"calibration_artifact_sha256"`
	CandidateSetSHA256             string                            `json:"candidate_set_sha256"`
	ProfileRegistrySHA256          string                            `json:"profile_registry_sha256"`
	SlotTemplateSHA256             string                            `json:"slot_template_sha256"`
	MethodBuilderBundleSHA256      string                            `json:"method_builder_bundle_sha256"`
	DesignManifestSHA256           string                            `json:"design_manifest_sha256"`
	DecisionConfigTemplateSHA256   string                            `json:"decision_config_template_sha256"`
	OutcomeFieldsUsed              []string                          `json:"outcome_fields_used"`
}

type MethodBuilderBundleV2 struct {
	SchemaVersion              string                    `json:"schema_version"`
	Status                     string                    `json:"status"`
	EvidenceLabel              string                    `json:"evidence_label"`
	SupportMode                E1SupportModeV2           `json:"support_mode"`
	TestOnly                   bool                      `json:"test_only"`
	BuilderKind                string                    `json:"builder_kind"`
	ComparatorMethod           string                    `json:"comparator_method"`
	BuilderConfigSHA256        string                    `json:"builder_config_sha256"`
	MethodConfig               MethodConfig              `json:"method_config"`
	MethodConfigSHA256         string                    `json:"method_config_sha256"`
	TenantRiskPPB              int64                     `json:"tenant_risk_ppb,omitempty"`
	DriftThresholdPPB          int64                     `json:"drift_threshold_ppb,omitempty"`
	MaxAgeSeconds              int64                     `json:"max_age_seconds,omitempty"`
	RevalidationMinimumSupport int64                     `json:"revalidation_minimum_support,omitempty"`
	FrozenAt                   string                    `json:"frozen_at,omitempty"`
	ProspectiveEvidence        *GOVARProspectiveEvidence `json:"prospective_evidence,omitempty"`
	BudgetCalibrationSHA256    string                    `json:"budget_calibration_sha256"`
	CalibrationArtifactSHA256  string                    `json:"calibration_artifact_sha256"`
	CandidateSetSHA256         string                    `json:"candidate_set_sha256"`
	ProfileRegistrySHA256      string                    `json:"profile_registry_sha256"`
	SlotTemplateSHA256         string                    `json:"slot_template_sha256"`
	OutcomeFieldsUsed          []string                  `json:"outcome_fields_used"`
}

type IndependentAuthorizationV2 struct {
	SchemaVersion                   string          `json:"schema_version"`
	Verdict                         string          `json:"verdict"`
	SupportMode                     E1SupportModeV2 `json:"support_mode"`
	EvidenceLabel                   string          `json:"evidence_label"`
	AuthorizationContext            string          `json:"authorization_context"`
	TestOnly                        bool            `json:"test_only"`
	IndependentAuthorization        bool            `json:"independent_authorization"`
	FreshAuthorization              bool            `json:"fresh_authorization"`
	FullCampaignExecutionAuthorized bool            `json:"full_campaign_execution_authorized"`
	FinalEvidenceAuthorized         bool            `json:"final_evidence_authorized"`
	ScientificClaimsAuthorized      bool            `json:"scientific_claims_authorized"`
	UnresolvedCritical              int64           `json:"unresolved_critical"`
	UnresolvedMajor                 int64           `json:"unresolved_major"`
	OutcomesInspected               bool            `json:"outcomes_inspected"`
	PilotValuesInspected            bool            `json:"pilot_values_inspected"`
	DatasetID                       string          `json:"dataset_id"`
	Split                           string          `json:"split"`
	ExperimentID                    string          `json:"experiment_id"`
	RunID                           string          `json:"run_id"`
	CellID                          string          `json:"cell_id"`
	Scenario                        string          `json:"scenario"`
	Seed                            uint64          `json:"seed"`
	StreamKey                       string          `json:"stream_key"`
	ExecutionSubjectSHA256          string          `json:"execution_subject_sha256"`
	ExecutionIntentSHA256           string          `json:"execution_intent_sha256"`
	ProtocolSHA256                  string          `json:"protocol_sha256"`
	SourceCodeBindingSHA256         string          `json:"source_code_binding_sha256"`
	SourceManifestSHA256            string          `json:"source_manifest_sha256"`
	PilotUsageBindingSHA256         string          `json:"pilot_usage_binding_sha256"`
	RuntimeCapabilitySHA256         string          `json:"runtime_capability_sha256"`
	DesignSpecSHA256                string          `json:"design_spec_sha256"`
	DesignLockSHA256                string          `json:"design_lock_sha256"`
	IndependentDesignReviewSHA256   string          `json:"independent_design_review_sha256"`
	DesignManifestSHA256            string          `json:"design_manifest_sha256"`
	CohortRegistrySHA256            string          `json:"cohort_registry_sha256"`
	ProfileExecutionContractSHA256  string          `json:"profile_execution_contract_sha256"`
	BudgetCalibrationSHA256         string          `json:"budget_calibration_sha256"`
	CalibrationArtifactSHA256       string          `json:"calibration_artifact_sha256"`
	CandidateSetSHA256              string          `json:"candidate_set_sha256"`
	ProfileRegistrySHA256           string          `json:"profile_registry_sha256"`
	SlotTemplateSHA256              string          `json:"slot_template_sha256"`
	MethodBuilderBundleSHA256       string          `json:"method_builder_bundle_sha256"`
	CombinedE0AuthorizationSHA256   string          `json:"combined_e0_authorization_sha256,omitempty"`
	DGChecksSHA256                  string          `json:"d_g_checks_sha256,omitempty"`
	FocusedTestsSHA256              string          `json:"focused_tests_sha256,omitempty"`
}

func validateFixtureSupportInputsV2(config ConfigV2, opportunities []OpportunityV2, inputs ArtifactPreflightInputsV2, schemas artifactSupportValidationV2) error {
	lock := config.ProvenanceLock
	if lock.SupportMode != E1SupportModeFixture {
		return errors.New("fixture supports require fixture execution-subject mode")
	}
	protocol := fixtureEnvelopeV2{}
	if err := artifactDecodeStrictJSONV2(inputs.ProtocolRaw, &protocol); err != nil {
		return fmt.Errorf("fixture protocol: %w", err)
	}
	if err := validateFixtureEnvelopeV2(protocol, schemas.protocolSchema); err != nil {
		return fmt.Errorf("fixture protocol: %w", err)
	}
	sourceCode := fixtureSourceCodeBindingV2{}
	if err := artifactDecodeStrictJSONV2(inputs.SourceCodeBindingRaw, &sourceCode); err != nil {
		return fmt.Errorf("fixture source-code binding: %w", err)
	}
	if err := validateFixtureEnvelopeV2(sourceCode.fixtureEnvelopeV2, schemas.sourceCodeBindingSchema); err != nil || !shaPattern.MatchString(sourceCode.ReviewedSourceRootSHA256) {
		return errors.New("fixture source-code binding is not an exact test-only binding")
	}
	var source fixtureSourceManifestV2
	if err := artifactDecodeStrictJSONV2(inputs.SourceManifestRaw, &source); err != nil {
		return fmt.Errorf("fixture source manifest: %w", err)
	}
	if source.SchemaVersion != schemas.sourceManifestSchema || source.Status != "test_fixture" || source.EvidenceLabel != fixtureEvidenceLabelV2 ||
		source.DatasetID != UsageDatasetV2 || source.Split != UsageSplitV2 || source.Rows <= 0 || !shaPattern.MatchString(source.OrderedPreOutcomeIDSHA256) {
		return errors.New("fixture source manifest differs from the exact non-citable contract")
	}
	var pilot fixturePilotBindingV2
	if err := artifactDecodeStrictJSONV2(inputs.PilotUsageBindingRaw, &pilot); err != nil {
		return fmt.Errorf("fixture pilot binding: %w", err)
	}
	if pilot.SchemaVersion != schemas.pilotUsageBindingSchema || pilot.Status != "test_fixture" || pilot.EvidenceLabel != fixtureEvidenceLabelV2 ||
		pilot.DatasetID != UsageDatasetV2 || pilot.ParentSplit != "test_fixture" || pilot.Subsplit != UsageSplitV2 || pilot.Mode != "usage_only" ||
		pilot.QualityAvailable || pilot.H3QualityMeasured || pilot.SubsplitRows != source.Rows ||
		pilot.SubsplitPreOutcomeIDSHA256 != source.OrderedPreOutcomeIDSHA256 || pilot.SourceManifestSHA256 != SHA256(inputs.SourceManifestRaw) {
		return errors.New("fixture pilot binding differs from the exact non-citable contract")
	}
	runtime := fixtureRuntimeCapabilityV2{}
	if err := artifactDecodeStrictJSONV2(inputs.RuntimeCapabilityRaw, &runtime); err != nil {
		return fmt.Errorf("fixture runtime capability: %w", err)
	}
	if err := validateFixtureEnvelopeV2(runtime.fixtureEnvelopeV2, schemas.runtimeCapabilitySchema); err != nil ||
		runtime.SourceCodeBindingSHA256 != SHA256(inputs.SourceCodeBindingRaw) || runtime.FullCampaignSupported {
		return errors.New("fixture runtime capability is not a fail-closed test-only capability")
	}
	if err := validateFixtureRawSupportV2(inputs.CalibrationArtifactRaw, schemas.calibrationArtifactSchema); err != nil {
		return fmt.Errorf("fixture calibration artifact: %w", err)
	}
	if err := validateFixtureRawSupportV2(inputs.BudgetCalibrationRaw, schemas.budgetCalibrationSchema); err != nil {
		return fmt.Errorf("fixture budget calibration: %w", err)
	}
	if err := validateFixtureRawSupportV2(inputs.CandidateSetRaw, schemas.candidateSetSchema); err != nil {
		return fmt.Errorf("fixture candidate set: %w", err)
	}
	if err := validateFixtureRawSupportV2(inputs.ProfileRegistryRaw, schemas.profileRegistrySchema); err != nil {
		return fmt.Errorf("fixture profile registry: %w", err)
	}
	if err := validateFixtureRawSupportV2(inputs.SlotTemplateRaw, schemas.slotTemplateSchema); err != nil {
		return fmt.Errorf("fixture slot template: %w", err)
	}

	design, err := decodeJSONObjectV2(inputs.DesignSpecRaw)
	if err != nil {
		return fmt.Errorf("fixture design specification: %w", err)
	}
	if err := validateFixtureDesignV2(design, inputs, schemas); err != nil {
		return err
	}
	designLock, _ := jsonStringFieldV2(design, "design_lock_sha256")
	decisionSHA, _ := jsonStringFieldV2(design, "decision_config_template_sha256")
	preOutcomeSidecarSHA, _ := jsonStringFieldV2(design, "preoutcome_sidecar_sha256")

	var review p1bIndependentReviewV2
	if err := artifactDecodeStrictJSONV2(inputs.IndependentDesignReviewRaw, &review); err != nil {
		return fmt.Errorf("fixture independent design review: %w", err)
	}
	if review.SchemaVersion != schemas.independentDesignReviewSchema || review.Verdict != "test_fixture_only" ||
		review.ReviewScope != "outcome_free_pretrace_preparation_only" || !review.PretracePreparationAuthorized ||
		review.FullCampaignExecutionAuthorized || review.FinalEvidenceAuthorized || review.ScientificClaimsAuthorized ||
		review.IndependentReview || review.FreshReview || review.UnresolvedCritical != 0 || review.UnresolvedMajor != 0 || review.OutcomesInspected ||
		review.DatasetID != UsageDatasetV2 || review.Split != UsageSplitV2 || review.DesignLockSHA256 != designLock ||
		review.DecisionConfigTemplateSHA256 != decisionSHA || review.ProtocolSHA256 != SHA256(inputs.ProtocolRaw) ||
		review.SourceManifestSHA256 != SHA256(inputs.SourceManifestRaw) || review.PilotUsageBindingSHA256 != SHA256(inputs.PilotUsageBindingRaw) ||
		review.PreOutcomeSidecarSHA256 != preOutcomeSidecarSHA || !shaPattern.MatchString(review.GeneratorSHA256) {
		return errors.New("fixture review does not bind only the exact non-citable pretrace")
	}

	var cohort p1bCohortRegistryV2
	if err := artifactDecodeStrictJSONV2(inputs.CohortRegistryRaw, &cohort); err != nil {
		return fmt.Errorf("fixture cohort registry: %w", err)
	}
	if cohort.SchemaVersion != schemas.cohortRegistrySchema || cohort.Scenario != config.Scenario || cohort.Seed != config.Seed ||
		cohort.TenantWindowsIndependent || !equalCohortSpecsV2(cohort.Cohorts, config.Cohorts) || len(bytes.TrimSpace(cohort.ProspectiveProfileExecution)) == 0 {
		return errors.New("fixture cohort registry does not exactly match the prospective config")
	}

	stream, err := validateFixtureDesignManifestV2(config, opportunities, inputs, decisionSHA)
	if err != nil {
		return err
	}
	if stream.CohortRegistryArtifact.SHA256 != SHA256(inputs.CohortRegistryRaw) ||
		stream.ProfileExecutionContractArtifact.SHA256 != SHA256(inputs.ProfileExecutionContractRaw) {
		return errors.New("fixture selected pretrace stream does not bind copied cohort/profile-contract bytes")
	}
	if err := validateFixtureProfileExecutionContractV2(config, inputs, stream); err != nil {
		return err
	}

	var bundle MethodBuilderBundleV2
	if err := artifactDecodeStrictJSONV2(inputs.MethodBuilderBundleRaw, &bundle); err != nil {
		return fmt.Errorf("fixture method-builder bundle: %w", err)
	}
	projectionRaw, _ := json.Marshal(projectExecutionIntentConfigV2(config))
	builderConfigSHA := DomainHash("govar-e1-builder-config-v1", projectionRaw)
	if bundle.SchemaVersion != schemas.methodBuilderBundleSchema || bundle.Status != "test_fixture" || bundle.EvidenceLabel != fixtureEvidenceLabelV2 ||
		bundle.SupportMode != E1SupportModeFixture || !bundle.TestOnly || bundle.BuilderKind != "fixture_exact_method_config" ||
		bundle.ComparatorMethod != config.ComparatorMethod || bundle.BuilderConfigSHA256 != builderConfigSHA ||
		!reflect.DeepEqual(bundle.MethodConfig, config.MethodConfig) || bundle.MethodConfigSHA256 != config.MethodConfigSHA256 ||
		bundle.BudgetCalibrationSHA256 != SHA256(inputs.BudgetCalibrationRaw) ||
		bundle.CalibrationArtifactSHA256 != SHA256(inputs.CalibrationArtifactRaw) || bundle.CandidateSetSHA256 != SHA256(inputs.CandidateSetRaw) ||
		bundle.ProfileRegistrySHA256 != SHA256(inputs.ProfileRegistryRaw) || bundle.SlotTemplateSHA256 != SHA256(inputs.SlotTemplateRaw) ||
		len(bundle.OutcomeFieldsUsed) != 0 || bundle.ProspectiveEvidence != nil {
		return errors.New("fixture method-builder bundle does not reproduce the exact method config")
	}

	var intent ExecutionIntentV2
	if err := artifactDecodeStrictJSONV2(inputs.ExecutionIntentRaw, &intent); err != nil {
		return fmt.Errorf("fixture execution intent: %w", err)
	}
	wantStreamKey := fmt.Sprintf("%s/seed-%d", config.Scenario, config.Seed)
	if intent.SchemaVersion != schemas.executionIntentSchema || intent.Status != "test_fixture" || intent.EvidenceLabel != fixtureEvidenceLabelV2 ||
		intent.SupportMode != E1SupportModeFixture || !intent.TestOnly || intent.StreamKey != wantStreamKey ||
		!reflect.DeepEqual(intent.Config, projectExecutionIntentConfigV2(config)) || intent.OpportunityStreamSHA256 != SHA256(inputs.StreamRaw) ||
		intent.MappingArtifactSHA256 != SHA256(inputs.PreOutcomeMappingRaw) || intent.CohortRegistrySHA256 != SHA256(inputs.CohortRegistryRaw) ||
		intent.ProfileExecutionContractSHA256 != SHA256(inputs.ProfileExecutionContractRaw) ||
		intent.BudgetCalibrationSHA256 != SHA256(inputs.BudgetCalibrationRaw) ||
		intent.CalibrationArtifactSHA256 != SHA256(inputs.CalibrationArtifactRaw) || intent.CandidateSetSHA256 != SHA256(inputs.CandidateSetRaw) ||
		intent.ProfileRegistrySHA256 != SHA256(inputs.ProfileRegistryRaw) || intent.SlotTemplateSHA256 != SHA256(inputs.SlotTemplateRaw) ||
		intent.MethodBuilderBundleSHA256 != SHA256(inputs.MethodBuilderBundleRaw) || intent.DesignManifestSHA256 != SHA256(inputs.DesignManifestRaw) ||
		intent.DecisionConfigTemplateSHA256 != decisionSHA || len(intent.OutcomeFieldsUsed) != 0 {
		return errors.New("fixture execution intent does not exactly project the pre-outcome cell config and raw builder supports")
	}

	if err := validateExactLockByteBindingsV2(lock, inputs, pilot.SubsplitPreOutcomeIDSHA256, designLock); err != nil {
		return err
	}
	var authorization IndependentAuthorizationV2
	if err := artifactDecodeStrictJSONV2(inputs.IndependentAuthorizationRaw, &authorization); err != nil {
		return fmt.Errorf("fixture independent authorization: %w", err)
	}
	if err := validateFixtureAuthorizationV2(config, inputs, authorization); err != nil {
		return err
	}
	return nil
}

func equalCohortSpecsV2(left, right []CohortSpec) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validateFixtureEnvelopeV2(value fixtureEnvelopeV2, schema string) error {
	if value.SchemaVersion != schema || value.Status != "test_fixture" || value.EvidenceLabel != fixtureEvidenceLabelV2 ||
		value.SupportMode != E1SupportModeFixture || !value.TestOnly || len(value.OutcomeFieldsUsed) != 0 {
		return errors.New("document is not an exact non-citable test fixture")
	}
	return nil
}

func validateFixtureRawSupportV2(raw []byte, schema string) error {
	var value fixtureEnvelopeV2
	if err := artifactDecodeStrictJSONV2(raw, &value); err != nil {
		return err
	}
	return validateFixtureEnvelopeV2(value, schema)
}

func validateFixtureDesignV2(design map[string]json.RawMessage, inputs ArtifactPreflightInputsV2, schemas artifactSupportValidationV2) error {
	wants := map[string]string{
		"schema_version": schemas.designSpecSchema, "status": "test_fixture", "evidence_label": fixtureEvidenceLabelV2,
		"dataset_id": UsageDatasetV2, "split": UsageSplitV2,
		"source_manifest_sha256": SHA256(inputs.SourceManifestRaw), "pilot_usage_binding_sha256": SHA256(inputs.PilotUsageBindingRaw),
		"protocol_sha256": SHA256(inputs.ProtocolRaw), "decision_config_template_sha256": SHA256(inputs.DecisionConfigTemplateRaw),
	}
	for key, want := range wants {
		got, err := jsonStringFieldV2(design, key)
		if err != nil || got != want {
			return fmt.Errorf("fixture design field %s differs", key)
		}
	}
	embedded, ok := design["decision_config_template"]
	if !ok || SHA256(mustCanonicalJSONV2(embedded)) != SHA256(inputs.DecisionConfigTemplateRaw) {
		return errors.New("fixture design does not embed the exact decision-config template")
	}
	lock, err := jsonStringFieldV2(design, "design_lock_sha256")
	if err != nil || lock != canonicalObjectDigestWithoutFieldV2(design, "design_lock_sha256") {
		return errors.New("fixture design lock does not recompute")
	}
	if _, err := jsonStringFieldV2(design, "preoutcome_sidecar_sha256"); err != nil {
		return errors.New("fixture design lacks an exact pre-outcome sidecar binding")
	}
	return nil
}

func validateFixtureDesignManifestV2(config ConfigV2, opportunities []OpportunityV2, inputs ArtifactPreflightInputsV2, decisionSHA string) (p1bStreamManifestV2, error) {
	manifest, err := decodeJSONObjectV2(inputs.DesignManifestRaw)
	if err != nil {
		return p1bStreamManifestV2{}, fmt.Errorf("fixture design manifest: %w", err)
	}
	wants := map[string]string{
		"schema_version": p1bDesignManifestSchemaV2, "status": "test_fixture", "evidence_label": fixtureEvidenceLabelV2,
		"dataset_id": UsageDatasetV2, "split": UsageSplitV2, "source_manifest_sha256": SHA256(inputs.SourceManifestRaw),
		"pilot_usage_binding_sha256": SHA256(inputs.PilotUsageBindingRaw), "protocol_sha256": SHA256(inputs.ProtocolRaw),
		"design_spec_sha256": SHA256(inputs.DesignSpecRaw), "independent_design_review_sha256": SHA256(inputs.IndependentDesignReviewRaw),
		"decision_config_template_sha256": decisionSHA,
	}
	for key, want := range wants {
		got, fieldErr := jsonStringFieldV2(manifest, key)
		if fieldErr != nil || got != want {
			return p1bStreamManifestV2{}, fmt.Errorf("fixture design manifest field %s differs", key)
		}
	}
	for key := range map[string]struct{}{"final_evidence_eligible": {}, "scientific_claims_authorized": {}, "full_p1b_execution_authorized": {}} {
		value, fieldErr := jsonBoolFieldV2(manifest, key)
		if fieldErr != nil || value {
			return p1bStreamManifestV2{}, fmt.Errorf("fixture design manifest field %s is not fail-closed", key)
		}
	}
	testOnly, err := jsonBoolFieldV2(manifest, "test_only_fixture_mode")
	if err != nil || !testOnly {
		return p1bStreamManifestV2{}, errors.New("fixture design manifest is not explicitly test-only")
	}
	suppliedPayload, err := jsonStringFieldV2(manifest, "manifest_payload_sha256")
	if err != nil || suppliedPayload != canonicalObjectDigestWithoutFieldV2(manifest, "manifest_payload_sha256") {
		return p1bStreamManifestV2{}, errors.New("fixture design-manifest payload digest does not recompute")
	}
	var streams []p1bStreamManifestV2
	if raw, ok := manifest["streams"]; !ok || json.Unmarshal(raw, &streams) != nil || len(streams) == 0 {
		return p1bStreamManifestV2{}, errors.New("fixture design manifest has no decodable streams")
	}
	wantKey := fmt.Sprintf("%s/seed-%d", config.Scenario, config.Seed)
	var selected *p1bStreamManifestV2
	for index := range streams {
		if streams[index].StreamKey == wantKey {
			if selected != nil {
				return p1bStreamManifestV2{}, errors.New("fixture design manifest duplicates the selected stream key")
			}
			selected = &streams[index]
		}
	}
	if selected == nil || selected.Scenario != config.Scenario || selected.Seed != config.Seed || selected.Rows != int64(len(opportunities)) ||
		selected.TenantWindowsIndependent || selected.AssignedPreOutcomeSetSHA256 != config.ProvenanceLock.AssignedPreOutcomeSetSHA256 ||
		selected.StreamPreOutcomeSequenceSHA256 != config.ProvenanceLock.StreamPreOutcomeSequenceSHA256 ||
		selected.DecisionConfigTemplateSHA256 != decisionSHA || selected.MappingArtifact.SHA256 != SHA256(inputs.PreOutcomeMappingRaw) ||
		selected.OpportunityStreamArtifact.SHA256 != SHA256(inputs.StreamRaw) || len(selected.OutcomeFieldsUsed) != 0 {
		return p1bStreamManifestV2{}, errors.New("fixture design manifest selected stream differs from exact cell inputs")
	}
	return *selected, nil
}

func validateFixtureProfileExecutionContractV2(config ConfigV2, inputs ArtifactPreflightInputsV2, stream p1bStreamManifestV2) error {
	contract, err := decodeJSONObjectV2(inputs.ProfileExecutionContractRaw)
	if err != nil {
		return fmt.Errorf("fixture profile execution contract: %w", err)
	}
	for key, want := range map[string]string{"schema_version": p1bProfileExecutionContractSchemaV2, "status": "prospective_assignment_only", "scenario": config.Scenario} {
		got, fieldErr := jsonStringFieldV2(contract, key)
		if fieldErr != nil || got != want {
			return fmt.Errorf("fixture profile-execution contract field %s differs", key)
		}
	}
	seed, err := jsonUint64FieldV2(contract, "seed")
	if err != nil || seed != config.Seed {
		return errors.New("fixture profile-execution contract seed differs")
	}
	for key := range map[string]struct{}{"full_campaign_execution_authorized": {}, "final_evidence_authorized": {}, "scientific_claims_authorized": {}, "observed_output_distribution_claimed": {}, "output_values_transformed": {}} {
		value, fieldErr := jsonBoolFieldV2(contract, key)
		if fieldErr != nil || value {
			return fmt.Errorf("fixture profile-execution contract field %s is not fail-closed", key)
		}
	}
	bindingsRaw, ok := contract["pretrace_artifact_bindings"]
	if !ok {
		return errors.New("fixture profile-execution contract lacks pretrace bindings")
	}
	bindings, err := decodeJSONObjectV2(bindingsRaw)
	if err != nil {
		return err
	}
	for key, want := range map[string]string{
		"mapping_artifact_sha256":            SHA256(inputs.PreOutcomeMappingRaw),
		"opportunity_stream_artifact_sha256": SHA256(inputs.StreamRaw),
		"cohort_registry_artifact_sha256":    SHA256(inputs.CohortRegistryRaw),
	} {
		got, fieldErr := jsonStringFieldV2(bindings, key)
		if fieldErr != nil || got != want {
			return fmt.Errorf("fixture profile-execution binding %s differs", key)
		}
	}
	if stream.ProfileExecutionContractArtifact.SHA256 != SHA256(inputs.ProfileExecutionContractRaw) {
		return errors.New("fixture stream does not bind exact profile-execution contract bytes")
	}
	return nil
}

func validateExactLockByteBindingsV2(lock E1ProvenanceLockV2, inputs ArtifactPreflightInputsV2, pilotSetSHA, designLockSHA string) error {
	checks := map[string]bool{
		"protocol":                   lock.ProtocolSHA256 == SHA256(inputs.ProtocolRaw),
		"source code":                lock.SourceCodeBindingSHA256 == SHA256(inputs.SourceCodeBindingRaw),
		"source manifest":            lock.SourceManifestSHA256 == SHA256(inputs.SourceManifestRaw),
		"pilot binding":              lock.PilotUsageBindingSHA256 == SHA256(inputs.PilotUsageBindingRaw),
		"runtime capability":         lock.RuntimeCapabilitySHA256 == SHA256(inputs.RuntimeCapabilityRaw),
		"design spec":                lock.DesignSpecSHA256 == SHA256(inputs.DesignSpecRaw),
		"decision template":          lock.DecisionConfigTemplateSHA256 == SHA256(inputs.DecisionConfigTemplateRaw),
		"design lock":                lock.DesignLockSHA256 == designLockSHA,
		"design review":              lock.IndependentDesignReviewSHA256 == SHA256(inputs.IndependentDesignReviewRaw),
		"design manifest":            lock.DesignManifestSHA256 == SHA256(inputs.DesignManifestRaw),
		"cohort registry":            lock.CohortRegistrySHA256 == SHA256(inputs.CohortRegistryRaw),
		"profile execution contract": lock.ProfileExecutionContractSHA256 == SHA256(inputs.ProfileExecutionContractRaw),
		"budget calibration":         lock.BudgetCalibrationSHA256 == SHA256(inputs.BudgetCalibrationRaw),
		"calibration artifact":       lock.CalibrationArtifactSHA256 == SHA256(inputs.CalibrationArtifactRaw),
		"candidate set":              lock.CandidateSetSHA256 == SHA256(inputs.CandidateSetRaw),
		"profile registry":           lock.ProfileRegistrySHA256 == SHA256(inputs.ProfileRegistryRaw),
		"slot template":              lock.SlotTemplateSHA256 == SHA256(inputs.SlotTemplateRaw),
		"method-builder bundle":      lock.MethodBuilderBundleSHA256 == SHA256(inputs.MethodBuilderBundleRaw),
		"execution intent":           lock.ExecutionIntentSHA256 == SHA256(inputs.ExecutionIntentRaw),
		"authorization":              lock.IndependentAuthorizationSHA256 == SHA256(inputs.IndependentAuthorizationRaw),
		"pilot pre-outcome set":      lock.PilotPreOutcomeSetSHA256 == pilotSetSHA,
	}
	for label, valid := range checks {
		if !valid {
			return fmt.Errorf("execution subject does not bind exact %s", label)
		}
	}
	return nil
}

func validateFixtureAuthorizationV2(config ConfigV2, inputs ArtifactPreflightInputsV2, value IndependentAuthorizationV2) error {
	lock := config.ProvenanceLock
	wantStreamKey := fmt.Sprintf("%s/seed-%d", config.Scenario, config.Seed)
	if value.SchemaVersion != fixtureIndependentAuthorizationSchemaV2 || value.Verdict != "test_fixture_only" ||
		value.SupportMode != E1SupportModeFixture || value.EvidenceLabel != fixtureEvidenceLabelV2 ||
		!validIdentity(value.AuthorizationContext) || !value.TestOnly || value.IndependentAuthorization || value.FreshAuthorization ||
		value.FullCampaignExecutionAuthorized || value.FinalEvidenceAuthorized || value.ScientificClaimsAuthorized ||
		value.UnresolvedCritical != 0 || value.UnresolvedMajor != 0 || value.OutcomesInspected || value.PilotValuesInspected ||
		value.DatasetID != UsageDatasetV2 || value.Split != UsageSplitV2 || value.ExperimentID != config.ExperimentID ||
		value.RunID != config.RunID || value.CellID != config.CellID || value.Scenario != config.Scenario || value.Seed != config.Seed ||
		value.StreamKey != wantStreamKey || value.ExecutionSubjectSHA256 != lock.ExecutionSubjectSHA256 ||
		value.ExecutionIntentSHA256 != lock.ExecutionIntentSHA256 || value.ProtocolSHA256 != lock.ProtocolSHA256 ||
		value.SourceCodeBindingSHA256 != lock.SourceCodeBindingSHA256 || value.SourceManifestSHA256 != lock.SourceManifestSHA256 ||
		value.PilotUsageBindingSHA256 != lock.PilotUsageBindingSHA256 || value.RuntimeCapabilitySHA256 != lock.RuntimeCapabilitySHA256 ||
		value.DesignSpecSHA256 != lock.DesignSpecSHA256 || value.DesignLockSHA256 != lock.DesignLockSHA256 ||
		value.IndependentDesignReviewSHA256 != lock.IndependentDesignReviewSHA256 || value.DesignManifestSHA256 != lock.DesignManifestSHA256 ||
		value.CohortRegistrySHA256 != lock.CohortRegistrySHA256 || value.ProfileExecutionContractSHA256 != lock.ProfileExecutionContractSHA256 ||
		value.BudgetCalibrationSHA256 != lock.BudgetCalibrationSHA256 ||
		value.CalibrationArtifactSHA256 != lock.CalibrationArtifactSHA256 || value.CandidateSetSHA256 != lock.CandidateSetSHA256 ||
		value.ProfileRegistrySHA256 != lock.ProfileRegistrySHA256 || value.SlotTemplateSHA256 != lock.SlotTemplateSHA256 ||
		value.MethodBuilderBundleSHA256 != lock.MethodBuilderBundleSHA256 || value.CombinedE0AuthorizationSHA256 != "" ||
		value.DGChecksSHA256 != "" || value.FocusedTestsSHA256 != "" {
		return errors.New("fixture authorization is not an exact non-citable, non-executive binding")
	}
	if SHA256(inputs.IndependentAuthorizationRaw) != lock.IndependentAuthorizationSHA256 || lock.ExecutionLockSHA256 != lock.executionDigest() {
		return errors.New("fixture authorization bytes do not bind the final execution lock")
	}
	return nil
}

func decodeJSONObjectV2(raw []byte) (map[string]json.RawMessage, error) {
	if err := artifactRejectDuplicateJSONV2(raw); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var result map[string]json.RawMessage
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("JSON value is not an object")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return result, nil
}

func jsonStringFieldV2(object map[string]json.RawMessage, key string) (string, error) {
	raw, ok := object[key]
	if !ok {
		return "", fmt.Errorf("missing JSON field %s", key)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("JSON field %s is not a string", key)
	}
	return value, nil
}

func jsonBoolFieldV2(object map[string]json.RawMessage, key string) (bool, error) {
	raw, ok := object[key]
	if !ok {
		return false, fmt.Errorf("missing JSON field %s", key)
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, fmt.Errorf("JSON field %s is not a boolean", key)
	}
	return value, nil
}

func jsonUint64FieldV2(object map[string]json.RawMessage, key string) (uint64, error) {
	raw, ok := object[key]
	if !ok {
		return 0, fmt.Errorf("missing JSON field %s", key)
	}
	var value uint64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("JSON field %s is not an unsigned integer", key)
	}
	return value, nil
}

func mustCanonicalJSONV2(raw json.RawMessage) []byte {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil
	}
	result, _ := json.Marshal(value)
	return result
}

func canonicalObjectDigestWithoutFieldV2(object map[string]json.RawMessage, field string) string {
	return canonicalP1bObjectDigestWithoutFieldV2(object, field)
}

// BuildProspectiveGOVARMethodConfigV2 builds the canonical prospective GOV-AR
// method config from a v2 opportunity stream. It applies exactly the
// projection the p1b_exact validator applies before rebuilding the config, so
// a config produced here is byte-identical to the one the validator
// independently recomputes. It exists because the v1 command cannot serve a
// P1b stream: v1 ParseStream refuses more than one budget window per tenant,
// while the P1b design carries 400 windows per stream.
func BuildProspectiveGOVARMethodConfigV2(
	config Config, opportunities []OpportunityV2, tenantRiskPPB, driftThresholdPPB,
	maxAgeSeconds, revalidationMinimumSupport int64, frozenAt time.Time,
	evidence GOVARProspectiveEvidence,
) (MethodConfig, error) {
	projected := make([]Opportunity, len(opportunities))
	for index := range opportunities {
		projected[index] = projectOpportunityV2(opportunities[index])
	}
	return BuildProspectiveGOVARMethodConfig(config, projected, tenantRiskPPB,
		driftThresholdPPB, maxAgeSeconds, revalidationMinimumSupport, frozenAt, evidence)
}
