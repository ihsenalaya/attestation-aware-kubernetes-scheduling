package govarexperiment

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	ManifestSchemaV2             = "govar-e1-run-manifest-v2"
	CompletionSchemaV2           = "govar-e1-run-completion-v2"
	ArtifactSetSchemaV2          = "govar-e1-artifact-set-v2"
	QualificationEvidenceLabelV2 = "qualification_nonfinal"
	ArtifactCompletionFileNameV2 = "COMPLETED.json"
	// Keep ordinary support documents tightly bounded.  The uncompressed
	// lifecycle has a separate ceiling because a valid 10k-opportunity cell can
	// contain 60k records and legitimately exceed 64 MiB even though its
	// compressed artifact remains small.
	maxArtifactInputBytesV2  int64 = 64 << 20
	maxLifecyclePlainBytesV2 int64 = 256 << 20
)

// ArtifactInputsV2 contains exact input bytes. Every upstream document is
// copied into the cell directory, rather than represented only by hashes.
// That lets a verifier inspect the authorization chain without consulting a
// mutable path outside the create-once artifact set.
type ArtifactInputsV2 struct {
	SupportMode                 E1SupportModeV2
	ConfigRaw                   []byte
	StreamRaw                   []byte
	PreOutcomeMappingRaw        []byte
	DevelopmentUsageRaw         []byte
	ProducerEvidenceRaw         []byte
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

// ArtifactManifestV2 intentionally contains identities, exact byte bindings,
// and descriptive record counts only. It has no effect estimate, comparative
// metric, quality field, p-value, or claim label.
type ArtifactManifestV2 struct {
	SchemaVersion                        string          `json:"schema_version"`
	RecordType                           string          `json:"record_type"`
	SupportMode                          E1SupportModeV2 `json:"support_mode"`
	EvidenceLabel                        string          `json:"evidence_label"`
	EvidenceStatus                       string          `json:"evidence_status"`
	FinalEvidenceEligible                bool            `json:"final_evidence_eligible"`
	QualityAvailable                     bool            `json:"quality_available"`
	ComparativeEffectEstimationPermitted bool            `json:"comparative_effect_estimation_permitted"`
	PValuesPermitted                     bool            `json:"p_values_permitted"`
	ClaimLabelsPermitted                 bool            `json:"claim_labels_permitted"`

	ExperimentID              string `json:"experiment_id"`
	RunID                     string `json:"run_id"`
	CellID                    string `json:"cell_id"`
	Scenario                  string `json:"scenario"`
	Seed                      uint64 `json:"seed"`
	ComparatorMethod          string `json:"comparator_method"`
	ProductionReservationMode string `json:"production_reservation_mode"`
	MethodConfigSHA256        string `json:"method_config_sha256"`
	SourceSHA256              string `json:"source_sha256"`
	ConfigSHA256              string `json:"config_sha256"`
	CanonicalConfigSHA256     string `json:"canonical_config_sha256"`
	StreamSHA256              string `json:"stream_sha256"`
	MatchedStreamKey          string `json:"matched_stream_key"`
	UsageBindingSHA256        string `json:"usage_binding_sha256"`
	ExecutionLockSHA256       string `json:"execution_lock_sha256"`

	OpportunityRecordCount int64 `json:"opportunity_record_count"`
	LifecycleRecordCount   int64 `json:"lifecycle_record_count"`
	UsageAccessRecordCount int64 `json:"usage_access_record_count"`

	ConfigInput                   ArtifactFileSpec `json:"config_input"`
	StreamInput                   ArtifactFileSpec `json:"stream_input"`
	PreOutcomeMappingInput        ArtifactFileSpec `json:"preoutcome_mapping_input"`
	DevelopmentUsageInput         ArtifactFileSpec `json:"development_usage_input"`
	ProducerEvidenceInput         ArtifactFileSpec `json:"producer_evidence_input"`
	ProtocolInput                 ArtifactFileSpec `json:"protocol_input"`
	SourceCodeBindingInput        ArtifactFileSpec `json:"source_code_binding_input"`
	SourceManifestInput           ArtifactFileSpec `json:"source_manifest_input"`
	PilotUsageBindingInput        ArtifactFileSpec `json:"pilot_usage_binding_input"`
	RuntimeCapabilityInput        ArtifactFileSpec `json:"runtime_capability_input"`
	DesignSpecInput               ArtifactFileSpec `json:"design_spec_input"`
	DecisionConfigTemplateInput   ArtifactFileSpec `json:"decision_config_template_input"`
	IndependentDesignReviewInput  ArtifactFileSpec `json:"independent_design_review_input"`
	DesignManifestInput           ArtifactFileSpec `json:"design_manifest_input"`
	CohortRegistryInput           ArtifactFileSpec `json:"cohort_registry_input"`
	ProfileExecutionContractInput ArtifactFileSpec `json:"profile_execution_contract_input"`
	BudgetCalibrationInput        ArtifactFileSpec `json:"budget_calibration_input"`
	CalibrationArtifactInput      ArtifactFileSpec `json:"calibration_artifact_input"`
	CandidateSetInput             ArtifactFileSpec `json:"candidate_set_input"`
	ProfileRegistryInput          ArtifactFileSpec `json:"profile_registry_input"`
	SlotTemplateInput             ArtifactFileSpec `json:"slot_template_input"`
	MethodBuilderBundleInput      ArtifactFileSpec `json:"method_builder_bundle_input"`
	ExecutionIntentInput          ArtifactFileSpec `json:"execution_intent_input"`
	IndependentAuthorizationInput ArtifactFileSpec `json:"independent_authorization_input"`
	UsageBindingInput             ArtifactFileSpec `json:"usage_binding_input"`
	UsageAccessInput              ArtifactFileSpec `json:"usage_access_input"`
	RawFile                       RawFileSpec      `json:"raw_file"`
	ArtifactSetRootSHA256         string           `json:"artifact_set_root_sha256"`
}

type ArtifactCompletionV2 struct {
	SchemaVersion         string          `json:"schema_version"`
	RecordType            string          `json:"record_type"`
	RunID                 string          `json:"run_id"`
	SupportMode           E1SupportModeV2 `json:"support_mode"`
	EvidenceLabel         string          `json:"evidence_label"`
	FinalEvidenceEligible bool            `json:"final_evidence_eligible"`
	ManifestPath          string          `json:"manifest_path"`
	ManifestSHA256        string          `json:"manifest_sha256"`
	ArtifactSetRootSHA256 string          `json:"artifact_set_root_sha256"`
	SelfVerification      string          `json:"self_verification"`
}

type ArtifactResultV2 struct {
	ManifestPath   string
	CompletionPath string
	RawPath        string
	Manifest       ArtifactManifestV2
	Completion     ArtifactCompletionV2
}

// ArtifactProtectedInputsV2 contains inputs that may only be opened after the
// pre-outcome authorization has validated. ProducerEvidenceRaw is fixture-only:
// p1b_exact derives producer evidence from the validated lock and exact usage
// bytes after the callback returns.
type ArtifactProtectedInputsV2 struct {
	DevelopmentUsageRaw []byte
	ProducerEvidenceRaw []byte
}

// ReadArtifactProtectedInputsV2 is invoked only after the complete pre-outcome
// execution subject and independent authorization have passed validation.
type ReadArtifactProtectedInputsV2 func() (ArtifactProtectedInputsV2, error)

// WriteArtifactsV2AfterPreflight enforces a two-stage read boundary. The
// callback can open the usage/evidence paths only after preflight succeeds;
// WriteArtifactsV2 then independently repeats the same validation before it
// parses or reveals any returned usage row.
func WriteArtifactsV2AfterPreflight(outputDirectory string, preflightInputs ArtifactPreflightInputsV2, readProtected ReadArtifactProtectedInputsV2) (ArtifactResultV2, error) {
	if readProtected == nil {
		return ArtifactResultV2{}, errors.New("protected artifact reader is required")
	}
	if preflightInputs.SupportMode == E1SupportModeP1bExact && len(preflightInputs.ConfigRaw) != 0 {
		return ArtifactResultV2{}, errors.New("p1b_exact staged execution forbids externally supplied ConfigRaw")
	}
	preflight, err := preflightArtifactInputsV2(preflightInputs)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	if !IsSafeArtifactIdentity(preflight.config.RunID) {
		return ArtifactResultV2{}, errors.New("run_id is unsafe for an artifact path")
	}
	// Reserve and lock the destination before invoking the only callback that
	// may reveal protected usage. A completed/colliding cell therefore fails
	// without opening outcomes, and concurrent writers cannot both reveal the
	// same stream for the same destination.
	publication, err := beginArtifactPublicationV2(outputDirectory)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	defer publication.Close()
	protected, err := readProtected()
	if err != nil {
		return ArtifactResultV2{}, err
	}
	inputs := ArtifactInputsV2{
		SupportMode: preflightInputs.SupportMode, ConfigRaw: preflightInputs.ConfigRaw,
		StreamRaw: preflightInputs.StreamRaw, PreOutcomeMappingRaw: preflightInputs.PreOutcomeMappingRaw,
		DevelopmentUsageRaw: protected.DevelopmentUsageRaw, ProducerEvidenceRaw: protected.ProducerEvidenceRaw,
		ProtocolRaw: preflightInputs.ProtocolRaw, SourceCodeBindingRaw: preflightInputs.SourceCodeBindingRaw,
		SourceManifestRaw: preflightInputs.SourceManifestRaw, PilotUsageBindingRaw: preflightInputs.PilotUsageBindingRaw,
		RuntimeCapabilityRaw: preflightInputs.RuntimeCapabilityRaw, DesignSpecRaw: preflightInputs.DesignSpecRaw,
		DecisionConfigTemplateRaw:  preflightInputs.DecisionConfigTemplateRaw,
		IndependentDesignReviewRaw: preflightInputs.IndependentDesignReviewRaw,
		DesignManifestRaw:          preflightInputs.DesignManifestRaw, CohortRegistryRaw: preflightInputs.CohortRegistryRaw,
		ProfileExecutionContractRaw: preflightInputs.ProfileExecutionContractRaw,
		BudgetCalibrationRaw:        preflightInputs.BudgetCalibrationRaw,
		CalibrationArtifactRaw:      preflightInputs.CalibrationArtifactRaw, CandidateSetRaw: preflightInputs.CandidateSetRaw,
		ProfileRegistryRaw: preflightInputs.ProfileRegistryRaw, SlotTemplateRaw: preflightInputs.SlotTemplateRaw,
		MethodBuilderBundleRaw: preflightInputs.MethodBuilderBundleRaw, ExecutionIntentRaw: preflightInputs.ExecutionIntentRaw,
		IndependentAuthorizationRaw: preflightInputs.IndependentAuthorizationRaw,
	}
	if preflightInputs.SupportMode == E1SupportModeP1bExact {
		inputs, err = materializeP1bExactProtectedInputsV2(preflight, inputs)
		if err != nil {
			return ArtifactResultV2{}, err
		}
		return writeArtifactsV2WithPublication(
			outputDirectory, inputs, nil, publication,
		)
	}
	return writeArtifactsV2WithPublication(
		outputDirectory, inputs, nil, publication,
	)
}

// materializeP1bExactProtectedInputsV2 is the sole production transition from
// a validated pre-reveal execution subject to usage-bound bytes. Neither the
// final ConfigV2 nor producer evidence is accepted from the caller.
func materializeP1bExactProtectedInputsV2(preflight artifactPreflightResultV2, inputs ArtifactInputsV2) (ArtifactInputsV2, error) {
	if inputs.SupportMode != E1SupportModeP1bExact || preflight.config.ProvenanceLock.SupportMode != E1SupportModeP1bExact {
		return ArtifactInputsV2{}, errors.New("p1b_exact materialization requires an exact production preflight")
	}
	if len(inputs.ConfigRaw) != 0 {
		return ArtifactInputsV2{}, errors.New("p1b_exact materialization forbids externally supplied ConfigRaw")
	}
	if len(inputs.ProducerEvidenceRaw) != 0 {
		return ArtifactInputsV2{}, errors.New("p1b_exact materialization forbids externally supplied ProducerEvidenceRaw")
	}
	if len(inputs.DevelopmentUsageRaw) == 0 {
		return ArtifactInputsV2{}, errors.New("p1b_exact protected usage is empty")
	}

	config := preflight.config
	evidence := UsageProducerEvidenceV2{
		SchemaVersion:          UsageProducerSchemaV2,
		DatasetID:              UsageDatasetV2,
		Split:                  UsageSplitV2,
		ProvenanceLock:         config.ProvenanceLock,
		ProducerSoftwareSHA256: config.SoftwareSHA256,
		UsageArtifactSHA256:    SHA256(inputs.DevelopmentUsageRaw),
	}
	if err := evidence.Validate(); err != nil {
		return ArtifactInputsV2{}, fmt.Errorf("construct p1b_exact producer evidence: %w", err)
	}
	evidenceRaw, err := artifactPrettyJSONV2(evidence)
	if err != nil {
		return ArtifactInputsV2{}, fmt.Errorf("encode p1b_exact producer evidence: %w", err)
	}
	issuer, err := ParseDevelopmentUsageV2(inputs.DevelopmentUsageRaw, inputs.PreOutcomeMappingRaw, evidence)
	if err != nil {
		return ArtifactInputsV2{}, err
	}
	config.UsageBindingSHA256 = issuer.Binding().BindingSHA256
	if err := config.Validate(); err != nil {
		return ArtifactInputsV2{}, fmt.Errorf("materialized p1b_exact config: %w", err)
	}
	configRaw, err := artifactPrettyJSONV2(config)
	if err != nil {
		return ArtifactInputsV2{}, fmt.Errorf("encode p1b_exact config: %w", err)
	}
	inputs.ConfigRaw = configRaw
	inputs.ProducerEvidenceRaw = evidenceRaw
	return inputs, nil
}

type artifactNamesV2 struct {
	config, stream, mapping, usage, evidence           string
	protocol, sourceCodeBinding, sourceManifest        string
	pilotBinding, runtimeCapability, designSpec        string
	decisionTemplate, designReview, designManifest     string
	cohortRegistry, profileExecutionContract           string
	budgetCalibration                                  string
	calibrationArtifact, candidateSet                  string
	profileRegistry, slotTemplate, methodBuilderBundle string
	executionIntent, authority                         string
	usageBinding, usageAccess, lifecycle, manifest     string
}

func namesForRunV2(runID string) artifactNamesV2 {
	return artifactNamesV2{
		config: runID + ".config.json", stream: runID + ".opportunities.jsonl",
		mapping: runID + ".preoutcome-mapping.jsonl", usage: runID + ".development-usage.jsonl",
		evidence: runID + ".usage-producer-evidence.json", protocol: runID + ".protocol.json",
		sourceCodeBinding: runID + ".source-code-binding.json", sourceManifest: runID + ".source-manifest.json",
		pilotBinding: runID + ".pilot-usage-binding.json", runtimeCapability: runID + ".runtime-capability.json",
		designSpec: runID + ".design-spec.json", decisionTemplate: runID + ".decision-config-template.json",
		designReview: runID + ".independent-design-review.json", designManifest: runID + ".design-manifest.json",
		cohortRegistry: runID + ".cohort-registry.json", profileExecutionContract: runID + ".profile-execution-contract.json",
		budgetCalibration:   runID + ".budget-calibration.json",
		calibrationArtifact: runID + ".calibration-artifact.json",
		candidateSet:        runID + ".candidate-set.json", profileRegistry: runID + ".profile-registry.json",
		slotTemplate: runID + ".slot-template.json", methodBuilderBundle: runID + ".method-builder-bundle.json",
		executionIntent: runID + ".execution-intent.json", authority: runID + ".independent-authorization.json",
		usageBinding: runID + ".usage-binding.json",
		usageAccess:  runID + ".usage-access.jsonl", lifecycle: runID + ".lifecycle.jsonl.gz",
		manifest: runID + ".manifest.json",
	}
}

func (n artifactNamesV2) allWithCompletion() []string {
	return []string{n.config, n.stream, n.mapping, n.usage, n.evidence, n.protocol,
		n.sourceCodeBinding, n.sourceManifest, n.pilotBinding, n.runtimeCapability, n.designSpec,
		n.decisionTemplate, n.designReview, n.designManifest, n.cohortRegistry,
		n.profileExecutionContract, n.budgetCalibration,
		n.calibrationArtifact, n.candidateSet, n.profileRegistry, n.slotTemplate,
		n.methodBuilderBundle, n.executionIntent, n.authority,
		n.usageBinding, n.usageAccess, n.lifecycle,
		n.manifest, ArtifactCompletionFileNameV2}
}

// ParseUsageProducerEvidenceV2 rejects duplicate keys at every JSON nesting
// level as well as unknown fields and multiple top-level values.
func ParseUsageProducerEvidenceV2(raw []byte) (UsageProducerEvidenceV2, error) {
	var evidence UsageProducerEvidenceV2
	if err := artifactDecodeStrictJSONV2(raw, &evidence); err != nil {
		return UsageProducerEvidenceV2{}, fmt.Errorf("decode E1 v2 producer evidence: %w", err)
	}
	if err := evidence.Validate(); err != nil {
		return UsageProducerEvidenceV2{}, err
	}
	return evidence, nil
}

// WriteArtifactsV2 executes the concrete, package-sealed usage issuer and
// publishes one create-once non-final qualification cell. COMPLETED.json is
// staged only after a fresh issuer has replayed every staged byte; the complete
// verified directory then becomes visible in one atomic rename.
func WriteArtifactsV2(outputDirectory string, inputs ArtifactInputsV2) (ArtifactResultV2, error) {
	if inputs.SupportMode == E1SupportModeP1bExact {
		return ArtifactResultV2{}, errors.New("p1b_exact requires WriteArtifactsV2AfterPreflight; direct protected inputs are forbidden")
	}
	return writeArtifactsV2(outputDirectory, inputs)
}

func writeArtifactsV2(outputDirectory string, inputs ArtifactInputsV2) (ArtifactResultV2, error) {
	return writeArtifactsV2WithBeforePublish(outputDirectory, inputs, nil)
}

// artifactBeforePublishV2 exists only as an internal failure-injection seam.
// Production callers always pass nil through writeArtifactsV2. Keeping the
// seam as a parameter (rather than mutable package state) preserves race-free
// publication tests and makes it impossible for one run to affect another.
type artifactBeforePublishV2 func(stagingDirectory string) error

func writeArtifactsV2WithBeforePublish(outputDirectory string, inputs ArtifactInputsV2, beforePublish artifactBeforePublishV2) (ArtifactResultV2, error) {
	return writeArtifactsV2WithPublication(
		outputDirectory, inputs, beforePublish, nil,
	)
}

func writeArtifactsV2WithPublication(outputDirectory string, inputs ArtifactInputsV2, beforePublish artifactBeforePublishV2, publication *artifactPublicationV2) (ArtifactResultV2, error) {
	preflight, err := preflightArtifactInputsV2(artifactPreflightInputsFromV2(inputs))
	if err != nil {
		return ArtifactResultV2{}, err
	}
	config := preflight.config
	support := preflight.support
	if !IsSafeArtifactIdentity(config.RunID) {
		return ArtifactResultV2{}, errors.New("run_id is unsafe for an artifact path")
	}
	names := namesForRunV2(config.RunID)
	evidence, err := ParseUsageProducerEvidenceV2(inputs.ProducerEvidenceRaw)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	issuer, err := ParseDevelopmentUsageV2(inputs.DevelopmentUsageRaw, inputs.PreOutcomeMappingRaw, evidence)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	run, err := RunWithUsageIssuerV2(inputs.ConfigRaw, inputs.StreamRaw, issuer)
	if err != nil {
		return ArtifactResultV2{}, err
	}

	lifecycleJSONL, err := encodeLifecycleJSONLV2(run.Records)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	lifecycleGzip, err := deterministicGzip(lifecycleJSONL)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	access := issuer.UsageAccessLog()
	usageAccessRaw, err := encodeUsageAccessJSONLV2(access)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	bindingRaw, err := artifactPrettyJSONV2(issuer.Binding())
	if err != nil {
		return ArtifactResultV2{}, err
	}

	rows, err := ParsePreOutcomeMappingV2(inputs.PreOutcomeMappingRaw)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	manifest := ArtifactManifestV2{
		SchemaVersion: ManifestSchemaV2, RecordType: "run_manifest", SupportMode: inputs.SupportMode,
		EvidenceLabel:  support.evidenceLabel,
		EvidenceStatus: RuntimeEvidenceStatusV2, FinalEvidenceEligible: false,
		ExperimentID: config.ExperimentID, RunID: config.RunID, CellID: config.CellID, Scenario: config.Scenario,
		Seed: config.Seed, ComparatorMethod: config.ComparatorMethod,
		ProductionReservationMode: config.ProductionReservationMode, MethodConfigSHA256: config.MethodConfigSHA256,
		SourceSHA256: config.SourceSHA256, ConfigSHA256: run.ConfigSHA256,
		CanonicalConfigSHA256: run.CanonicalConfigSHA256, StreamSHA256: run.StreamSHA256,
		MatchedStreamKey: run.MatchedStreamKey, UsageBindingSHA256: issuer.Binding().BindingSHA256,
		ExecutionLockSHA256:    config.ProvenanceLock.ExecutionLockSHA256,
		OpportunityRecordCount: int64(len(rows)), LifecycleRecordCount: int64(len(run.Records)),
		UsageAccessRecordCount:        int64(len(access)),
		ConfigInput:                   artifactFileSpecV2(names.config, inputs.ConfigRaw, ConfigSchemaV2, 0),
		StreamInput:                   artifactFileSpecV2(names.stream, inputs.StreamRaw, OpportunitySchemaV2, int64(len(rows))),
		PreOutcomeMappingInput:        artifactFileSpecV2(names.mapping, inputs.PreOutcomeMappingRaw, PreOutcomeMappingSchemaV2, int64(len(rows))),
		DevelopmentUsageInput:         artifactFileSpecV2(names.usage, inputs.DevelopmentUsageRaw, UsageObservationSchemaV2, int64(len(rows))),
		ProducerEvidenceInput:         artifactFileSpecV2(names.evidence, inputs.ProducerEvidenceRaw, UsageProducerSchemaV2, 0),
		ProtocolInput:                 artifactFileSpecV2(names.protocol, inputs.ProtocolRaw, support.protocolSchema, 0),
		SourceCodeBindingInput:        artifactFileSpecV2(names.sourceCodeBinding, inputs.SourceCodeBindingRaw, support.sourceCodeBindingSchema, 0),
		SourceManifestInput:           artifactFileSpecV2(names.sourceManifest, inputs.SourceManifestRaw, support.sourceManifestSchema, 0),
		PilotUsageBindingInput:        artifactFileSpecV2(names.pilotBinding, inputs.PilotUsageBindingRaw, support.pilotUsageBindingSchema, 0),
		RuntimeCapabilityInput:        artifactFileSpecV2(names.runtimeCapability, inputs.RuntimeCapabilityRaw, support.runtimeCapabilitySchema, 0),
		DesignSpecInput:               artifactFileSpecV2(names.designSpec, inputs.DesignSpecRaw, support.designSpecSchema, 0),
		DecisionConfigTemplateInput:   artifactFileSpecV2(names.decisionTemplate, inputs.DecisionConfigTemplateRaw, support.decisionConfigTemplateSchema, 0),
		IndependentDesignReviewInput:  artifactFileSpecV2(names.designReview, inputs.IndependentDesignReviewRaw, support.independentDesignReviewSchema, 0),
		DesignManifestInput:           artifactFileSpecV2(names.designManifest, inputs.DesignManifestRaw, support.designManifestSchema, 0),
		CohortRegistryInput:           artifactFileSpecV2(names.cohortRegistry, inputs.CohortRegistryRaw, support.cohortRegistrySchema, 0),
		ProfileExecutionContractInput: artifactFileSpecV2(names.profileExecutionContract, inputs.ProfileExecutionContractRaw, support.profileExecutionContractSchema, 0),
		BudgetCalibrationInput:        artifactFileSpecV2(names.budgetCalibration, inputs.BudgetCalibrationRaw, support.budgetCalibrationSchema, 0),
		CalibrationArtifactInput:      artifactFileSpecV2(names.calibrationArtifact, inputs.CalibrationArtifactRaw, support.calibrationArtifactSchema, 0),
		CandidateSetInput:             artifactFileSpecV2(names.candidateSet, inputs.CandidateSetRaw, support.candidateSetSchema, 0),
		ProfileRegistryInput:          artifactFileSpecV2(names.profileRegistry, inputs.ProfileRegistryRaw, support.profileRegistrySchema, 0),
		SlotTemplateInput:             artifactFileSpecV2(names.slotTemplate, inputs.SlotTemplateRaw, support.slotTemplateSchema, 0),
		MethodBuilderBundleInput:      artifactFileSpecV2(names.methodBuilderBundle, inputs.MethodBuilderBundleRaw, support.methodBuilderBundleSchema, 0),
		ExecutionIntentInput:          artifactFileSpecV2(names.executionIntent, inputs.ExecutionIntentRaw, support.executionIntentSchema, 0),
		IndependentAuthorizationInput: artifactFileSpecV2(names.authority, inputs.IndependentAuthorizationRaw, support.independentAuthorizationSchema, 0),
		UsageBindingInput:             artifactFileSpecV2(names.usageBinding, bindingRaw, UsageBindingSchemaV2, 0),
		UsageAccessInput:              artifactFileSpecV2(names.usageAccess, usageAccessRaw, UsageAccessSchemaV2, int64(len(access))),
		RawFile: RawFileSpec{Path: names.lifecycle, SHA256: SHA256(lifecycleGzip),
			UncompressedSHA256: SHA256(lifecycleJSONL), Schema: LifecycleSchemaV2,
			ExpectedRecordCount: int64(len(run.Records))},
	}
	manifest.ArtifactSetRootSHA256 = artifactSetRootV2(manifest)
	manifestRaw, err := artifactPrettyJSONV2(manifest)
	if err != nil {
		return ArtifactResultV2{}, err
	}

	publications := []struct {
		name string
		raw  []byte
	}{
		{names.config, inputs.ConfigRaw}, {names.stream, inputs.StreamRaw},
		{names.mapping, inputs.PreOutcomeMappingRaw}, {names.usage, inputs.DevelopmentUsageRaw},
		{names.evidence, inputs.ProducerEvidenceRaw}, {names.protocol, inputs.ProtocolRaw},
		{names.sourceCodeBinding, inputs.SourceCodeBindingRaw}, {names.sourceManifest, inputs.SourceManifestRaw},
		{names.pilotBinding, inputs.PilotUsageBindingRaw}, {names.runtimeCapability, inputs.RuntimeCapabilityRaw},
		{names.designSpec, inputs.DesignSpecRaw}, {names.decisionTemplate, inputs.DecisionConfigTemplateRaw},
		{names.designReview, inputs.IndependentDesignReviewRaw}, {names.designManifest, inputs.DesignManifestRaw},
		{names.cohortRegistry, inputs.CohortRegistryRaw}, {names.profileExecutionContract, inputs.ProfileExecutionContractRaw},
		{names.budgetCalibration, inputs.BudgetCalibrationRaw},
		{names.calibrationArtifact, inputs.CalibrationArtifactRaw},
		{names.candidateSet, inputs.CandidateSetRaw}, {names.profileRegistry, inputs.ProfileRegistryRaw},
		{names.slotTemplate, inputs.SlotTemplateRaw}, {names.methodBuilderBundle, inputs.MethodBuilderBundleRaw},
		{names.executionIntent, inputs.ExecutionIntentRaw},
		{names.authority, inputs.IndependentAuthorizationRaw}, {names.usageBinding, bindingRaw},
		{names.usageAccess, usageAccessRaw}, {names.lifecycle, lifecycleGzip}, {names.manifest, manifestRaw},
	}
	if publication == nil {
		publication, err = beginArtifactPublicationV2(outputDirectory)
		if err != nil {
			return ArtifactResultV2{}, err
		}
		defer publication.Close()
	}
	directory := publication.staging
	if err := preflightArtifactNamesV2(directory, names.allWithCompletion()); err != nil {
		return ArtifactResultV2{}, err
	}
	for _, publication := range publications {
		if err := createOnceAtV2(directory, publication.name, publication.raw); err != nil {
			return ArtifactResultV2{}, fmt.Errorf("stage %s: %w", publication.name, err)
		}
	}
	manifestPath := filepath.Join(directory.Name(), names.manifest)
	if _, err := verifyArtifactManifestV2(manifestPath, false); err != nil {
		return ArtifactResultV2{}, fmt.Errorf("self-verify staged E1 v2 qualification artifacts: %w", err)
	}
	completion := ArtifactCompletionV2{
		SchemaVersion: CompletionSchemaV2, RecordType: "completion", RunID: config.RunID,
		SupportMode: inputs.SupportMode, EvidenceLabel: support.evidenceLabel, FinalEvidenceEligible: false,
		ManifestPath: names.manifest, ManifestSHA256: SHA256(manifestRaw),
		ArtifactSetRootSHA256: manifest.ArtifactSetRootSHA256, SelfVerification: "passed",
	}
	completionRaw, err := artifactPrettyJSONV2(completion)
	if err != nil {
		return ArtifactResultV2{}, err
	}
	if err := createOnceAtV2(directory, ArtifactCompletionFileNameV2, completionRaw); err != nil {
		return ArtifactResultV2{}, fmt.Errorf("stage %s: %w", ArtifactCompletionFileNameV2, err)
	}
	if _, err := verifyArtifactManifestV2(manifestPath, true); err != nil {
		return ArtifactResultV2{}, fmt.Errorf("verify completed staged E1 v2 qualification artifacts: %w", err)
	}
	if beforePublish != nil {
		if err := beforePublish(directory.Name()); err != nil {
			return ArtifactResultV2{}, fmt.Errorf("before atomic E1 v2 publication: %w", err)
		}
	}
	if err := publication.Publish(); err != nil {
		return ArtifactResultV2{}, err
	}
	manifestPath = filepath.Join(publication.finalPath, names.manifest)
	if _, err := verifyArtifactManifestV2(manifestPath, true); err != nil {
		return ArtifactResultV2{}, fmt.Errorf("verify atomically published E1 v2 qualification artifacts: %w", err)
	}
	return ArtifactResultV2{
		ManifestPath: manifestPath, CompletionPath: filepath.Join(publication.finalPath, ArtifactCompletionFileNameV2),
		RawPath: filepath.Join(publication.finalPath, names.lifecycle), Manifest: manifest, Completion: completion,
	}, nil
}

// VerifyArtifactManifestV2 independently reconstructs a fresh concrete usage
// issuer and reruns the exact published cell. It accepts only an artifact set
// whose terminal completion marker binds the verified manifest and file root.
func VerifyArtifactManifestV2(manifestPath string) (ArtifactManifestV2, error) {
	return verifyArtifactManifestV2(manifestPath, true)
}

func verifyArtifactManifestV2(manifestPath string, requireCompletion bool) (ArtifactManifestV2, error) {
	manifestRaw, err := ReadRegularFileNoSymlinkV2(manifestPath)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	var manifest ArtifactManifestV2
	if err := artifactDecodeStrictJSONV2(manifestRaw, &manifest); err != nil {
		return ArtifactManifestV2{}, fmt.Errorf("decode E1 v2 manifest: %w", err)
	}
	if err := validateManifestEnvelopeV2(manifest, filepath.Base(manifestPath)); err != nil {
		return ArtifactManifestV2{}, err
	}
	directory := filepath.Dir(manifestPath)
	read := func(spec ArtifactFileSpec) ([]byte, error) {
		if err := validateArtifactFileSpecV2(spec); err != nil {
			return nil, err
		}
		raw, err := ReadRegularFileNoSymlinkV2(filepath.Join(directory, spec.Path))
		if err != nil {
			return nil, err
		}
		if SHA256(raw) != spec.SHA256 {
			return nil, fmt.Errorf("artifact %s digest mismatch", spec.Path)
		}
		return raw, nil
	}
	configRaw, err := read(manifest.ConfigInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	streamRaw, err := read(manifest.StreamInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	mappingRaw, err := read(manifest.PreOutcomeMappingInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	protocolRaw, err := read(manifest.ProtocolInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	sourceCodeBindingRaw, err := read(manifest.SourceCodeBindingInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	sourceManifestRaw, err := read(manifest.SourceManifestInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	pilotBindingRaw, err := read(manifest.PilotUsageBindingInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	runtimeCapabilityRaw, err := read(manifest.RuntimeCapabilityInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	designSpecRaw, err := read(manifest.DesignSpecInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	decisionTemplateRaw, err := read(manifest.DecisionConfigTemplateInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	designReviewRaw, err := read(manifest.IndependentDesignReviewInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	designManifestRaw, err := read(manifest.DesignManifestInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	cohortRegistryRaw, err := read(manifest.CohortRegistryInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	profileExecutionContractRaw, err := read(manifest.ProfileExecutionContractInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	budgetCalibrationRaw, err := read(manifest.BudgetCalibrationInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	calibrationArtifactRaw, err := read(manifest.CalibrationArtifactInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	candidateSetRaw, err := read(manifest.CandidateSetInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	profileRegistryRaw, err := read(manifest.ProfileRegistryInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	slotTemplateRaw, err := read(manifest.SlotTemplateInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	methodBuilderBundleRaw, err := read(manifest.MethodBuilderBundleInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	executionIntentRaw, err := read(manifest.ExecutionIntentInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	authorityRaw, err := read(manifest.IndependentAuthorizationInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	preflightInputs := ArtifactPreflightInputsV2{
		SupportMode: manifest.SupportMode,
		ConfigRaw:   configRaw, StreamRaw: streamRaw, PreOutcomeMappingRaw: mappingRaw,
		ProtocolRaw: protocolRaw, SourceCodeBindingRaw: sourceCodeBindingRaw,
		SourceManifestRaw: sourceManifestRaw, PilotUsageBindingRaw: pilotBindingRaw,
		RuntimeCapabilityRaw: runtimeCapabilityRaw, DesignSpecRaw: designSpecRaw,
		DecisionConfigTemplateRaw: decisionTemplateRaw, IndependentDesignReviewRaw: designReviewRaw,
		DesignManifestRaw: designManifestRaw, CohortRegistryRaw: cohortRegistryRaw,
		ProfileExecutionContractRaw: profileExecutionContractRaw,
		BudgetCalibrationRaw:        budgetCalibrationRaw,
		CalibrationArtifactRaw:      calibrationArtifactRaw, CandidateSetRaw: candidateSetRaw,
		ProfileRegistryRaw: profileRegistryRaw, SlotTemplateRaw: slotTemplateRaw,
		MethodBuilderBundleRaw: methodBuilderBundleRaw, ExecutionIntentRaw: executionIntentRaw,
		IndependentAuthorizationRaw: authorityRaw,
	}
	preflight, err := preflightArtifactInputsV2(preflightInputs)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	config := preflight.config
	canonicalConfigRaw, err := json.Marshal(config)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	canonicalConfigSHA := SHA256(canonicalConfigRaw)
	support := preflight.support
	// Protected/post-preflight files are not opened until the exact subject,
	// intent, builder supports, and authorization above have all validated.
	evidenceRaw, err := read(manifest.ProducerEvidenceInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	evidence, err := ParseUsageProducerEvidenceV2(evidenceRaw)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	usageRaw, err := read(manifest.DevelopmentUsageInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	issuer, err := ParseDevelopmentUsageV2(usageRaw, mappingRaw, evidence)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	bindingRaw, err := read(manifest.UsageBindingInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	accessRaw, err := read(manifest.UsageAccessInput)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	var publishedBinding UsageBindingV2
	if err := artifactDecodeStrictJSONV2(bindingRaw, &publishedBinding); err != nil {
		return ArtifactManifestV2{}, err
	}
	if err := publishedBinding.Validate(); err != nil || publishedBinding != issuer.Binding() {
		return ArtifactManifestV2{}, errors.New("published usage binding does not equal full recomputation")
	}
	run, err := RunWithUsageIssuerV2(configRaw, streamRaw, issuer)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	wantAccessRaw, err := encodeUsageAccessJSONLV2(issuer.UsageAccessLog())
	if err != nil || !bytes.Equal(wantAccessRaw, accessRaw) {
		return ArtifactManifestV2{}, errors.New("published usage access log does not equal causal replay")
	}
	accessRows, err := decodeUsageAccessJSONLV2(accessRaw)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	wantLifecycleRaw, err := encodeLifecycleJSONLV2(run.Records)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	rawPath := filepath.Join(directory, manifest.RawFile.Path)
	if filepath.Base(manifest.RawFile.Path) != manifest.RawFile.Path || !safeFileName(manifest.RawFile.Path) {
		return ArtifactManifestV2{}, errors.New("manifest contains unsafe lifecycle artifact path")
	}
	compressed, err := ReadRegularFileNoSymlinkV2(rawPath)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	decodedRecords, lifecycleRaw, err := decodeLifecycleGzipJSONLV2(compressed)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	if !bytes.Equal(lifecycleRaw, wantLifecycleRaw) || !reflect.DeepEqual(decodedRecords, run.Records) {
		return ArtifactManifestV2{}, errors.New("published lifecycle does not equal concrete runner replay")
	}
	if SHA256(compressed) != manifest.RawFile.SHA256 || SHA256(lifecycleRaw) != manifest.RawFile.UncompressedSHA256 ||
		manifest.RawFile.Schema != LifecycleSchemaV2 || manifest.RawFile.ExpectedRecordCount != int64(len(run.Records)) {
		return ArtifactManifestV2{}, errors.New("lifecycle manifest binding does not recompute")
	}
	rows, err := ParsePreOutcomeMappingV2(mappingRaw)
	if err != nil {
		return ArtifactManifestV2{}, err
	}
	if manifest.ConfigSHA256 != SHA256(configRaw) || manifest.CanonicalConfigSHA256 != canonicalConfigSHA ||
		manifest.StreamSHA256 != SHA256(streamRaw) || manifest.MatchedStreamKey != run.MatchedStreamKey ||
		manifest.UsageBindingSHA256 != issuer.Binding().BindingSHA256 ||
		manifest.ExecutionLockSHA256 != config.ProvenanceLock.ExecutionLockSHA256 ||
		manifest.OpportunityRecordCount != int64(len(rows)) || manifest.LifecycleRecordCount != int64(len(run.Records)) ||
		manifest.UsageAccessRecordCount != int64(len(accessRows)) {
		return ArtifactManifestV2{}, errors.New("manifest hashes or descriptive counts do not recompute")
	}
	if manifest.ExperimentID != config.ExperimentID || manifest.RunID != config.RunID || manifest.CellID != config.CellID ||
		manifest.Scenario != config.Scenario || manifest.Seed != config.Seed ||
		manifest.ComparatorMethod != config.ComparatorMethod || manifest.ProductionReservationMode != config.ProductionReservationMode ||
		manifest.MethodConfigSHA256 != config.MethodConfigSHA256 || manifest.SourceSHA256 != config.SourceSHA256 {
		return ArtifactManifestV2{}, errors.New("manifest identity does not equal exact config")
	}
	if manifest.SupportMode != config.ProvenanceLock.SupportMode || manifest.EvidenceLabel != support.evidenceLabel {
		return ArtifactManifestV2{}, errors.New("manifest support mode or evidence label differs from the verified authorization chain")
	}
	if err := validateManifestFileContractsV2(manifest, int64(len(rows)), int64(len(run.Records)), int64(len(accessRows))); err != nil {
		return ArtifactManifestV2{}, err
	}
	if artifactSetRootV2(manifest) != manifest.ArtifactSetRootSHA256 {
		return ArtifactManifestV2{}, errors.New("artifact-set root does not recompute")
	}
	if requireCompletion {
		completionPath := filepath.Join(directory, ArtifactCompletionFileNameV2)
		completionRaw, err := ReadRegularFileNoSymlinkV2(completionPath)
		if err != nil {
			return ArtifactManifestV2{}, err
		}
		var completion ArtifactCompletionV2
		if err := artifactDecodeStrictJSONV2(completionRaw, &completion); err != nil {
			return ArtifactManifestV2{}, err
		}
		expected := ArtifactCompletionV2{
			SchemaVersion: CompletionSchemaV2, RecordType: "completion", RunID: manifest.RunID,
			SupportMode: manifest.SupportMode, EvidenceLabel: manifest.EvidenceLabel, FinalEvidenceEligible: false,
			ManifestPath: filepath.Base(manifestPath), ManifestSHA256: SHA256(manifestRaw),
			ArtifactSetRootSHA256: manifest.ArtifactSetRootSHA256, SelfVerification: "passed",
		}
		if completion != expected {
			return ArtifactManifestV2{}, errors.New("terminal completion marker does not bind the verified manifest and artifact root")
		}
	}
	return manifest, nil
}

func artifactFileSpecV2(path string, raw []byte, schema string, count int64) ArtifactFileSpec {
	return ArtifactFileSpec{Path: path, SHA256: SHA256(raw), Schema: schema, ExpectedRecordCount: count}
}

func artifactSetRootV2(manifest ArtifactManifestV2) string {
	payload := struct {
		Schema string             `json:"schema"`
		Files  []ArtifactFileSpec `json:"files"`
		Raw    RawFileSpec        `json:"raw"`
	}{
		Schema: ArtifactSetSchemaV2,
		Files: []ArtifactFileSpec{
			manifest.ConfigInput, manifest.StreamInput, manifest.PreOutcomeMappingInput,
			manifest.DevelopmentUsageInput, manifest.ProducerEvidenceInput, manifest.ProtocolInput,
			manifest.SourceCodeBindingInput, manifest.SourceManifestInput, manifest.PilotUsageBindingInput,
			manifest.RuntimeCapabilityInput, manifest.DesignSpecInput, manifest.DecisionConfigTemplateInput,
			manifest.IndependentDesignReviewInput, manifest.DesignManifestInput, manifest.CohortRegistryInput,
			manifest.ProfileExecutionContractInput,
			manifest.BudgetCalibrationInput,
			manifest.CalibrationArtifactInput, manifest.CandidateSetInput, manifest.ProfileRegistryInput,
			manifest.SlotTemplateInput, manifest.MethodBuilderBundleInput, manifest.ExecutionIntentInput,
			manifest.IndependentAuthorizationInput,
			manifest.UsageBindingInput, manifest.UsageAccessInput,
		},
		Raw: manifest.RawFile,
	}
	raw, _ := json.Marshal(payload)
	return DomainHash(ArtifactSetSchemaV2, raw)
}

func validateManifestEnvelopeV2(manifest ArtifactManifestV2, manifestName string) error {
	wantLabel, err := evidenceLabelForSupportModeV2(manifest.SupportMode)
	if err != nil {
		return err
	}
	if manifest.SchemaVersion != ManifestSchemaV2 || manifest.RecordType != "run_manifest" ||
		manifest.EvidenceLabel != wantLabel || manifest.EvidenceStatus != RuntimeEvidenceStatusV2 ||
		manifest.FinalEvidenceEligible || manifest.QualityAvailable || manifest.ComparativeEffectEstimationPermitted ||
		manifest.PValuesPermitted || manifest.ClaimLabelsPermitted {
		return errors.New("unsupported or final-claiming E1 v2 manifest")
	}
	if !IsSafeArtifactIdentity(manifest.RunID) || manifestName != namesForRunV2(manifest.RunID).manifest {
		return errors.New("E1 v2 manifest path does not match its safe run identity")
	}
	return nil
}

func validateArtifactFileSpecV2(spec ArtifactFileSpec) error {
	if filepath.Base(spec.Path) != spec.Path || !safeFileName(spec.Path) || !shaPattern.MatchString(spec.SHA256) || spec.Schema == "" || spec.ExpectedRecordCount < 0 {
		return errors.New("manifest contains an invalid artifact file specification")
	}
	return nil
}

func validateManifestFileContractsV2(manifest ArtifactManifestV2, opportunities, lifecycle, access int64) error {
	names := namesForRunV2(manifest.RunID)
	schemas, err := supportSchemasForModeV2(manifest.SupportMode)
	if err != nil {
		return err
	}
	tests := []struct {
		spec   ArtifactFileSpec
		path   string
		schema string
		count  int64
	}{
		{manifest.ConfigInput, names.config, ConfigSchemaV2, 0},
		{manifest.StreamInput, names.stream, OpportunitySchemaV2, opportunities},
		{manifest.PreOutcomeMappingInput, names.mapping, PreOutcomeMappingSchemaV2, opportunities},
		{manifest.DevelopmentUsageInput, names.usage, UsageObservationSchemaV2, opportunities},
		{manifest.ProducerEvidenceInput, names.evidence, UsageProducerSchemaV2, 0},
		{manifest.ProtocolInput, names.protocol, schemas.protocolSchema, 0},
		{manifest.SourceCodeBindingInput, names.sourceCodeBinding, schemas.sourceCodeBindingSchema, 0},
		{manifest.SourceManifestInput, names.sourceManifest, schemas.sourceManifestSchema, 0},
		{manifest.PilotUsageBindingInput, names.pilotBinding, schemas.pilotUsageBindingSchema, 0},
		{manifest.RuntimeCapabilityInput, names.runtimeCapability, schemas.runtimeCapabilitySchema, 0},
		{manifest.DesignSpecInput, names.designSpec, schemas.designSpecSchema, 0},
		{manifest.DecisionConfigTemplateInput, names.decisionTemplate, schemas.decisionConfigTemplateSchema, 0},
		{manifest.IndependentDesignReviewInput, names.designReview, schemas.independentDesignReviewSchema, 0},
		{manifest.DesignManifestInput, names.designManifest, schemas.designManifestSchema, 0},
		{manifest.CohortRegistryInput, names.cohortRegistry, schemas.cohortRegistrySchema, 0},
		{manifest.ProfileExecutionContractInput, names.profileExecutionContract, schemas.profileExecutionContractSchema, 0},
		{manifest.BudgetCalibrationInput, names.budgetCalibration, schemas.budgetCalibrationSchema, 0},
		{manifest.CalibrationArtifactInput, names.calibrationArtifact, schemas.calibrationArtifactSchema, 0},
		{manifest.CandidateSetInput, names.candidateSet, schemas.candidateSetSchema, 0},
		{manifest.ProfileRegistryInput, names.profileRegistry, schemas.profileRegistrySchema, 0},
		{manifest.SlotTemplateInput, names.slotTemplate, schemas.slotTemplateSchema, 0},
		{manifest.MethodBuilderBundleInput, names.methodBuilderBundle, schemas.methodBuilderBundleSchema, 0},
		{manifest.ExecutionIntentInput, names.executionIntent, schemas.executionIntentSchema, 0},
		{manifest.IndependentAuthorizationInput, names.authority, schemas.independentAuthorizationSchema, 0},
		{manifest.UsageBindingInput, names.usageBinding, UsageBindingSchemaV2, 0},
		{manifest.UsageAccessInput, names.usageAccess, UsageAccessSchemaV2, access},
	}
	seen := map[string]struct{}{}
	for _, test := range tests {
		if test.spec.Path != test.path || test.spec.Schema != test.schema || test.spec.ExpectedRecordCount != test.count {
			return fmt.Errorf("manifest file contract differs for %s", test.path)
		}
		if _, duplicate := seen[test.spec.Path]; duplicate {
			return errors.New("manifest aliases two inputs to one artifact path")
		}
		seen[test.spec.Path] = struct{}{}
	}
	if manifest.RawFile.Path != names.lifecycle || manifest.RawFile.Schema != LifecycleSchemaV2 ||
		manifest.RawFile.ExpectedRecordCount != lifecycle {
		return errors.New("manifest lifecycle file contract differs")
	}
	if _, duplicate := seen[manifest.RawFile.Path]; duplicate {
		return errors.New("manifest aliases lifecycle to an input artifact path")
	}
	return nil
}

func encodeLifecycleJSONLV2(records []LifecycleRecordV2) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func encodeUsageAccessJSONLV2(events []UsageAccessEventV2) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func decodeUsageAccessJSONLV2(raw []byte) ([]UsageAccessEventV2, error) {
	if len(raw) == 0 {
		return []UsageAccessEventV2{}, nil
	}
	var events []UsageAccessEventV2
	err := scanArtifactJSONLV2(raw, "usage access", func(line int, body []byte) error {
		var event UsageAccessEventV2
		if err := artifactDecodeStrictJSONV2(body, &event); err != nil {
			return err
		}
		if event.SchemaVersion != UsageAccessSchemaV2 || event.EventIndex != int64(len(events)+1) ||
			(event.Kind != "authorize" && event.Kind != "reveal") || !identityPattern.MatchString(event.RequestID) ||
			!shaPattern.MatchString(event.UsageItemID) || !shaPattern.MatchString(event.DispatchID) || !event.Effective || event.Replayed {
			return errors.New("invalid causal usage-access event")
		}
		if _, err := parseCanonicalAbsoluteTimeV2("usage access time", event.At); err != nil {
			return err
		}
		events = append(events, event)
		return nil
	})
	return events, err
}

func decodeLifecycleGzipJSONLV2(compressed []byte) ([]LifecycleRecordV2, []byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, nil, fmt.Errorf("open E1 v2 lifecycle gzip: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(reader, maxLifecyclePlainBytesV2+1))
	closeErr := reader.Close()
	if err != nil || closeErr != nil {
		return nil, nil, errors.Join(err, closeErr)
	}
	if int64(len(raw)) > maxLifecyclePlainBytesV2 {
		return nil, nil, errors.New("E1 v2 lifecycle exceeds the verification size limit")
	}
	reencoded, err := deterministicGzip(raw)
	if err != nil || !bytes.Equal(reencoded, compressed) {
		return nil, nil, errors.New("E1 v2 lifecycle gzip is not the deterministic encoding")
	}
	var records []LifecycleRecordV2
	err = scanArtifactJSONLV2(raw, "lifecycle", func(line int, body []byte) error {
		var record LifecycleRecordV2
		if err := artifactDecodeStrictJSONV2(body, &record); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return records, raw, nil
}

func scanArtifactJSONLV2(raw []byte, label string, consume func(int, []byte) error) error {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		body := scanner.Bytes()
		if len(bytes.TrimSpace(body)) == 0 {
			return fmt.Errorf("%s line %d is empty", label, line)
		}
		if err := consume(line, body); err != nil {
			return fmt.Errorf("%s line %d: %w", label, line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if line == 0 {
		return fmt.Errorf("%s is empty", label)
	}
	return nil
}

func artifactPrettyJSONV2(value any) ([]byte, error) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func artifactDecodeStrictJSONV2(raw []byte, target any) error {
	if err := artifactRejectDuplicateJSONV2(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

// artifactRejectDuplicateJSONV2 walks nested objects. encoding/json otherwise
// silently accepts a repeated key and keeps its last value.
func artifactRejectDuplicateJSONV2(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var consume func() error
	consume = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key is not a string")
				}
				if _, duplicate := seen[key]; duplicate {
					return fmt.Errorf("JSON object duplicates key %q", key)
				}
				seen[key] = struct{}{}
				if err := consume(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("invalid JSON object terminator")
			}
		case '[':
			for decoder.More() {
				if err := consume(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("invalid JSON array terminator")
			}
		default:
			return errors.New("invalid JSON delimiter")
		}
		return nil
	}
	if err := consume(); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

// ReadRegularFileNoSymlinkV2 rejects traversal components, every existing
// symlink component, non-regular inputs, and a final-component symlink at open.
func ReadRegularFileNoSymlinkV2(path string) ([]byte, error) {
	abs, err := checkedAbsolutePathV2(path)
	if err != nil {
		return nil, err
	}
	components := absolutePathComponentsV2(abs)
	if len(components) == 0 {
		return nil, errors.New("artifact input path names no regular file")
	}
	parent, err := openDirectoryComponentsV2(components[:len(components)-1], false)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parent)
	fd, err := unix.Openat(parent, components[len(components)-1], unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("artifact input path contains a symlink or cannot be opened safely: %w", err)
	}
	file := os.NewFile(uintptr(fd), abs)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open regular artifact input")
	}
	defer file.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		return nil, err
	}
	if opened.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, errors.New("artifact input is not a regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxArtifactInputBytesV2+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxArtifactInputBytesV2 {
		return nil, errors.New("artifact input exceeds the size limit")
	}
	return raw, nil
}

func checkedAbsolutePathV2(path string) (string, error) {
	if path == "" || strings.IndexByte(path, 0) >= 0 {
		return "", errors.New("artifact path is empty or contains NUL")
	}
	for _, component := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' || r == '\\' }) {
		if component == ".." {
			return "", errors.New("artifact path traversal is forbidden")
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

// artifactPublicationV2 owns a single-writer advisory lock on the already-open
// parent directory and a create-once staging directory beside the final
// directory. Locking the directory descriptor leaves no lock-file artifact in
// the exact campaign tree. The sibling placement is essential: the final rename
// cannot cross a filesystem boundary. A process crash releases the advisory
// lock; the next identical attempt removes only this reserved staging name and
// can safely rebuild from the immutable inputs.
type artifactPublicationV2 struct {
	parent      *os.File
	staging     *os.File
	stagingName string
	stagingPath string
	finalName   string
	finalPath   string
	finalMode   uint32
	finalExists bool
	finalDev    uint64
	finalIno    uint64
	published   bool
}

func beginArtifactPublicationV2(path string) (*artifactPublicationV2, error) {
	abs, err := checkedAbsolutePathV2(path)
	if err != nil {
		return nil, err
	}
	components := absolutePathComponentsV2(abs)
	if len(components) == 0 {
		return nil, errors.New("artifact output directory cannot be the filesystem root")
	}
	parentFD, err := openDirectoryComponentsV2(components[:len(components)-1], true)
	if err != nil {
		return nil, err
	}
	parentPath := filepath.Dir(abs)
	parent := os.NewFile(uintptr(parentFD), parentPath)
	if parent == nil {
		_ = unix.Close(parentFD)
		return nil, errors.New("open artifact publication parent")
	}

	token := SHA256([]byte(abs))[:24]
	stagingName := ".govar-e1-v2-" + token + ".staging"
	if err := unix.Flock(parentFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = parent.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.New("create-once E1 v2 artifact publication is already in progress")
		}
		return nil, fmt.Errorf("lock E1 v2 artifact publication parent: %w", err)
	}

	finalMode, finalExists, finalDev, finalIno, err := inspectArtifactPublicationTargetV2(parent, components[len(components)-1])
	if err != nil {
		_ = unix.Flock(parentFD, unix.LOCK_UN)
		_ = parent.Close()
		return nil, err
	}
	stagingPath := filepath.Join(parentPath, stagingName)
	if err := os.RemoveAll(stagingPath); err != nil {
		_ = unix.Flock(parentFD, unix.LOCK_UN)
		_ = parent.Close()
		return nil, fmt.Errorf("remove stale E1 v2 staging directory: %w", err)
	}
	if err := unix.Mkdirat(parentFD, stagingName, 0o700); err != nil {
		_ = unix.Flock(parentFD, unix.LOCK_UN)
		_ = parent.Close()
		return nil, fmt.Errorf("create-once E1 v2 staging directory: %w", err)
	}
	stagingFD, err := unix.Openat(parentFD, stagingName,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
	if err != nil {
		_ = os.RemoveAll(stagingPath)
		_ = unix.Flock(parentFD, unix.LOCK_UN)
		_ = parent.Close()
		return nil, fmt.Errorf("open E1 v2 staging directory: %w", err)
	}
	staging := os.NewFile(uintptr(stagingFD), stagingPath)
	if staging == nil {
		_ = unix.Close(stagingFD)
		_ = os.RemoveAll(stagingPath)
		_ = unix.Flock(parentFD, unix.LOCK_UN)
		_ = parent.Close()
		return nil, errors.New("open E1 v2 staging directory file")
	}
	if err := parent.Sync(); err != nil {
		_ = staging.Close()
		_ = os.RemoveAll(stagingPath)
		_ = unix.Flock(parentFD, unix.LOCK_UN)
		_ = parent.Close()
		return nil, fmt.Errorf("sync E1 v2 staging creation: %w", err)
	}
	return &artifactPublicationV2{
		parent: parent, staging: staging, stagingName: stagingName, stagingPath: stagingPath,
		finalName: components[len(components)-1], finalPath: abs, finalMode: finalMode, finalExists: finalExists,
		finalDev: finalDev, finalIno: finalIno,
	}, nil
}

// inspectArtifactPublicationTargetV2 accepts only an absent target or a
// caller-created empty directory. Any non-empty, non-directory, or symlink
// target is an immutable create-once collision and is never removed.
func inspectArtifactPublicationTargetV2(parent *os.File, finalName string) (uint32, bool, uint64, uint64, error) {
	var stat unix.Stat_t
	err := unix.Fstatat(int(parent.Fd()), finalName, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return 0o755, false, 0, 0, nil
	}
	if err != nil {
		return 0, false, 0, 0, err
	}
	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		return 0, true, uint64(stat.Dev), stat.Ino, errors.New("artifact output path is a symlink")
	case unix.S_IFDIR:
		fd, openErr := unix.Openat(int(parent.Fd()), finalName,
			unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
		if openErr != nil {
			return 0, true, uint64(stat.Dev), stat.Ino, fmt.Errorf("open existing artifact output directory: %w", openErr)
		}
		directory := os.NewFile(uintptr(fd), filepath.Join(parent.Name(), finalName))
		if directory == nil {
			_ = unix.Close(fd)
			return 0, true, uint64(stat.Dev), stat.Ino, errors.New("open existing artifact output directory file")
		}
		_, readErr := directory.Readdirnames(1)
		closeErr := directory.Close()
		if readErr == nil {
			return 0, true, uint64(stat.Dev), stat.Ino, errors.New("create-once E1 v2 artifact directory already exists and is non-empty")
		}
		if !errors.Is(readErr, io.EOF) {
			return 0, true, uint64(stat.Dev), stat.Ino, fmt.Errorf("inspect existing artifact output directory: %w", readErr)
		}
		if closeErr != nil {
			return 0, true, uint64(stat.Dev), stat.Ino, closeErr
		}
		mode := uint32(stat.Mode & 0o777)
		if mode == 0 {
			mode = 0o755
		}
		return mode, true, uint64(stat.Dev), stat.Ino, nil
	default:
		return 0, true, uint64(stat.Dev), stat.Ino, errors.New("create-once E1 v2 artifact output path already exists")
	}
}

// Publish makes the fully completed and independently replayed staging
// directory visible in one atomic rename. If the caller did not supply an
// empty output directory, Mkdirat first reserves the final name create-once.
// The parent-directory flock serializes cooperating writers without leaving a
// persistent file, including on filesystems that do not implement Linux
// renameat2 flags (notably DrvFS-mounted workspaces).
func (publication *artifactPublicationV2) Publish() error {
	if publication == nil || publication.parent == nil || publication.staging == nil || publication.published {
		return errors.New("invalid E1 v2 artifact publication state")
	}
	if err := unix.Fchmod(int(publication.staging.Fd()), publication.finalMode); err != nil {
		return fmt.Errorf("set final E1 v2 artifact directory mode: %w", err)
	}
	if err := publication.staging.Sync(); err != nil {
		return fmt.Errorf("sync completed E1 v2 staging directory: %w", err)
	}
	_, exists, dev, ino, err := inspectArtifactPublicationTargetV2(publication.parent, publication.finalName)
	if err != nil {
		return err
	}
	if publication.finalExists {
		if !exists {
			return errors.New("create-once E1 v2 artifact output reservation disappeared before publication")
		}
		if dev != publication.finalDev || ino != publication.finalIno {
			return errors.New("create-once E1 v2 artifact output reservation changed before publication")
		}
	} else {
		if exists {
			return errors.New("create-once E1 v2 artifact publication collision")
		}
		if err := unix.Mkdirat(int(publication.parent.Fd()), publication.finalName, 0o700); err != nil {
			if errors.Is(err, unix.EEXIST) {
				return errors.New("create-once E1 v2 artifact publication collision")
			}
			return fmt.Errorf("reserve final E1 v2 artifact directory: %w", err)
		}
	}
	// Resolve both names through the already-open parent descriptor. This keeps
	// the no-symlink parent binding while using the plain rename syscall, whose
	// directory replacement semantics are consistent on Linux and v9fs/DrvFS.
	parentFDPath := fmt.Sprintf("/proc/self/fd/%d", publication.parent.Fd())
	if err := unix.Rename(
		filepath.Join(parentFDPath, publication.stagingName),
		filepath.Join(parentFDPath, publication.finalName),
	); err != nil {
		if errors.Is(err, unix.EEXIST) || errors.Is(err, unix.ENOTEMPTY) {
			return errors.New("create-once E1 v2 artifact publication collision")
		}
		return fmt.Errorf("atomically publish E1 v2 artifact directory: %w", err)
	}
	publication.published = true
	// Release the old-name directory handle before reopening the final path.
	// This also establishes immediate rename visibility on shared v9fs mounts.
	if err := publication.staging.Close(); err != nil {
		publication.staging = nil
		return fmt.Errorf("close atomically published E1 v2 staging handle: %w", err)
	}
	publication.staging = nil
	if err := publication.parent.Sync(); err != nil {
		return fmt.Errorf("sync atomically published E1 v2 artifact directory: %w", err)
	}
	return nil
}

func (publication *artifactPublicationV2) Close() {
	if publication == nil {
		return
	}
	if publication.staging != nil {
		_ = publication.staging.Close()
		publication.staging = nil
	}
	if !publication.published && publication.stagingPath != "" {
		_ = os.RemoveAll(publication.stagingPath)
		if publication.parent != nil {
			_ = publication.parent.Sync()
		}
	}
	if publication.parent != nil {
		_ = unix.Flock(int(publication.parent.Fd()), unix.LOCK_UN)
		_ = publication.parent.Close()
		publication.parent = nil
	}
}

func absolutePathComponentsV2(abs string) []string {
	volume := filepath.VolumeName(abs)
	root := volume + string(os.PathSeparator)
	rest := strings.TrimPrefix(abs, root)
	components := make([]string, 0)
	for _, part := range strings.Split(rest, string(os.PathSeparator)) {
		if part == "" {
			continue
		}
		components = append(components, part)
	}
	return components
}

// openDirectoryComponentsV2 walks from an already-open filesystem root using
// O_NOFOLLOW at every level. Unlike lstat-then-open, an ancestor cannot be
// swapped to a symlink between validation and use.
func openDirectoryComponentsV2(components []string, create bool) (int, error) {
	fd, err := unix.Open(string(os.PathSeparator), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
	if err != nil {
		return -1, err
	}
	for _, component := range components {
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
		if openErr != nil && create && errors.Is(openErr, unix.ENOENT) {
			if mkdirErr := unix.Mkdirat(fd, component, 0o755); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				_ = unix.Close(fd)
				return -1, mkdirErr
			}
			next, openErr = unix.Openat(fd, component, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
		}
		if openErr != nil {
			_ = unix.Close(fd)
			return -1, fmt.Errorf("artifact path contains a symlink or non-directory component %q: %w", component, openErr)
		}
		_ = unix.Close(fd)
		fd = next
	}
	return fd, nil
}

func preflightArtifactNamesV2(directory *os.File, names []string) error {
	seen := map[string]struct{}{}
	for _, name := range names {
		if !safeFileName(name) || filepath.Base(name) != name {
			return fmt.Errorf("unsafe E1 v2 artifact name %q", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate E1 v2 artifact name %q", name)
		}
		seen[name] = struct{}{}
		var stat unix.Stat_t
		err := unix.Fstatat(int(directory.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
		if err == nil {
			return fmt.Errorf("create-once artifact path already exists: %s", name)
		}
		if !errors.Is(err, unix.ENOENT) {
			return err
		}
	}
	return nil
}

func createOnceAtV2(directory *os.File, name string, body []byte) error {
	if filepath.Base(name) != name || !safeFileName(name) {
		return errors.New("unsafe create-once artifact name")
	}
	fd, err := unix.Openat(int(directory.Fd()), name,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o444)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(directory.Name(), name))
	if file == nil {
		_ = unix.Close(fd)
		return errors.New("create artifact file")
	}
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Persist the create-once directory entry before a later entry (especially
	// the terminal marker) can refer to it.
	return directory.Sync()
}
