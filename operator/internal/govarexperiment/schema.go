// Package govarexperiment runs deterministic trace experiments against the
// production GOV-AR decision core. It does not implement an alternative ledger.
package govarexperiment

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"
)

const (
	ConfigSchema      = "govar-experiment-config-v1"
	OpportunitySchema = "govar-opportunity-v1"
	FeedbackSchema    = "govar-selected-feedback-receipt-v1"
	LifecycleSchema   = "govar-lifecycle-v1"
	ManifestSchema    = "govar-run-manifest-v1"

	RecordLifecycle = "lifecycle"
	RecordSnapshot  = "tenant_window_snapshot"

	FaultNominal              = "nominal"
	FaultProviderCapViolation = "provider_output_cap_violation"
	FaultKnownInputMismatch   = "known_input_mismatch"
)

var (
	identityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	shaPattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type CohortSpec struct {
	TenantID       string `json:"tenant_id"`
	BudgetWindowID string `json:"budget_window_id"`
	CohortID       string `json:"cohort_id"`
	Size           int64  `json:"size"`
}

// Config contains no outcome-derived value. SourceSHA256 is not caller-chosen:
// it is a domain-separated root over the immutable software/data/protocol/split
// evidence hashes. Changing Method changes the policy under comparison but not
// MatchedStreamKey or deterministic scheduling.
type Config struct {
	SchemaVersion                  string            `json:"schema_version"`
	RecordType                     string            `json:"record_type"`
	ExperimentID                   string            `json:"experiment_id"`
	RunID                          string            `json:"run_id"`
	CellID                         string            `json:"cell_id"`
	Scenario                       string            `json:"scenario"`
	Seed                           uint64            `json:"seed"`
	RegistryID                     string            `json:"registry_id"`
	ProtocolMethodID               string            `json:"protocol_method_id"`
	ComparatorMethod               string            `json:"comparator_method"`
	ProductionReservationMode      string            `json:"production_reservation_mode"`
	MethodConfig                   MethodConfig      `json:"method_config"`
	MethodConfigSHA256             string            `json:"method_config_sha256"`
	ClusterID                      string            `json:"cluster_id,omitempty"`
	TrialID                        string            `json:"trial_id,omitempty"`
	VirtualStart                   string            `json:"virtual_start"`
	EvidenceTier                   string            `json:"evidence_tier"`
	SoftwareSHA256                 string            `json:"software_sha256"`
	DataSHA256                     string            `json:"data_sha256"`
	ProtocolSHA256                 string            `json:"protocol_sha256"`
	SplitSHA256                    string            `json:"split_sha256"`
	SelectedFeedbackDatasetID      string            `json:"selected_feedback_dataset_id"`
	SelectedFeedbackSplit          string            `json:"selected_feedback_split"`
	SelectedFeedbackModelMapSHA256 string            `json:"selected_feedback_model_map_sha256"`
	SelectedFeedbackModelIDs       map[string]string `json:"selected_feedback_model_ids"`
	SourceSHA256                   string            `json:"source_sha256"`
	InputPriceMicrosPerMillion     int64             `json:"input_price_micros_per_million"`
	OutputPriceMicrosPerMillion    int64             `json:"output_price_micros_per_million"`
	VerifiedOutputCapTokens        int64             `json:"verified_output_cap_tokens"`
	EstimateOutputTokens           int64             `json:"estimate_output_tokens,omitempty"`
	MarginOutputTokens             int64             `json:"margin_output_tokens,omitempty"`
	Cohorts                        []CohortSpec      `json:"cohorts,omitempty"`
}

func SourceRoot(software, data, protocol, split string) string {
	return DomainHash("govar-experiment-source-root-v1", []byte(software), []byte(data), []byte(protocol), []byte(split))
}

func (c Config) Validate() error {
	if c.SchemaVersion != ConfigSchema || c.RecordType != "experiment_config" {
		return errors.New("unsupported experiment config schema or record type")
	}
	for name, value := range map[string]string{
		"experiment_id": c.ExperimentID, "run_id": c.RunID, "cell_id": c.CellID, "scenario": c.Scenario,
	} {
		if !identityPattern.MatchString(value) {
			return fmt.Errorf("%s is not a safe immutable identity", name)
		}
	}
	for name, value := range map[string]string{"cluster_id": c.ClusterID, "trial_id": c.TrialID} {
		if value != "" && !identityPattern.MatchString(value) {
			return fmt.Errorf("%s is not a safe immutable identity", name)
		}
	}
	if c.EvidenceTier != "development_debug" && c.EvidenceTier != "frozen_authorized" {
		return errors.New("evidence_tier must be development_debug or frozen_authorized")
	}
	if c.EvidenceTier == "development_debug" && c.SelectedFeedbackSplit == "frozen_test" {
		return errors.New("development_debug may not access frozen_test")
	}
	if c.EvidenceTier == "frozen_authorized" && c.SelectedFeedbackDatasetID != "routereval_math_outcomes" {
		return errors.New("frozen_authorized evidence requires the production RouterEval authority")
	}
	validRouterEval := c.SelectedFeedbackDatasetID == "routereval_math_outcomes" &&
		(c.SelectedFeedbackSplit == "train" || c.SelectedFeedbackSplit == "calibration" || c.SelectedFeedbackSplit == "development" || c.SelectedFeedbackSplit == "frozen_test")
	validSyntheticP0 := c.EvidenceTier == "development_debug" && c.SelectedFeedbackDatasetID == "synthetic_p0_outcomes" && c.SelectedFeedbackSplit == "development"
	validSyntheticP1A := c.EvidenceTier == "development_debug" && c.SelectedFeedbackDatasetID == "synthetic_p1a_outcomes" && c.SelectedFeedbackSplit == "development"
	if !validRouterEval && !validSyntheticP0 && !validSyntheticP1A {
		return errors.New("selected-feedback dataset/split is unsupported")
	}
	modelMapDigest, err := SelectedFeedbackModelMapDigest(c.SelectedFeedbackModelIDs)
	if err != nil || c.SelectedFeedbackModelMapSHA256 != modelMapDigest {
		return errors.New("selected-feedback deployment-to-catalog model map/digest is invalid")
	}
	for name, value := range map[string]string{
		"software_sha256": c.SoftwareSHA256, "data_sha256": c.DataSHA256,
		"protocol_sha256": c.ProtocolSHA256, "split_sha256": c.SplitSHA256, "source_sha256": c.SourceSHA256,
	} {
		if !shaPattern.MatchString(value) {
			return fmt.Errorf("%s must be a lower-case SHA-256", name)
		}
	}
	if c.SourceSHA256 != SourceRoot(c.SoftwareSHA256, c.DataSHA256, c.ProtocolSHA256, c.SplitSHA256) {
		return errors.New("source_sha256 does not recompute from software/data/protocol/split hashes")
	}
	mode, known := productionMode(c.ComparatorMethod)
	if !known || c.RegistryID != registryID(c.ComparatorMethod) || c.ProtocolMethodID != c.ComparatorMethod || c.MethodConfig.ProtocolID != c.ComparatorMethod {
		return errors.New("budget/admission registry, protocol, comparator, and method-config identities do not agree")
	}
	if c.ProductionReservationMode != mode {
		return fmt.Errorf("comparator %s must execute production mode %s", c.ComparatorMethod, mode)
	}
	methodDigest, err := MethodConfigDigest(c.MethodConfig)
	if err != nil {
		return fmt.Errorf("method_config: %w", err)
	}
	if c.MethodConfigSHA256 != methodDigest {
		return errors.New("method_config_sha256 does not recompute from canonical method config")
	}
	start, err := time.Parse(time.RFC3339Nano, c.VirtualStart)
	if err != nil || start.Format(time.RFC3339Nano) != c.VirtualStart {
		return errors.New("virtual_start must be canonical RFC3339Nano")
	}
	if c.InputPriceMicrosPerMillion < 0 || c.OutputPriceMicrosPerMillion < 0 ||
		c.VerifiedOutputCapTokens <= 0 || c.EstimateOutputTokens < 0 || c.MarginOutputTokens < 0 {
		return errors.New("prices, cap, estimates, and margins must be non-negative with a positive cap")
	}
	switch c.ComparatorMethod {
	case "strict_max":
		if c.EstimateOutputTokens != 0 || c.MarginOutputTokens != 0 {
			return errors.New("strict_max does not accept estimate or margin")
		}
	case "mean":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens || c.MarginOutputTokens != 0 {
			return errors.New("mean estimate is outside cap or margin is inapplicable")
		}
	case "fixed_quantile":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens || c.MarginOutputTokens != 0 {
			return errors.New("estimate is outside the verified cap or margin is inapplicable")
		}
	case "mean_margin":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.MarginOutputTokens != c.MethodConfig.MeanMarginOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens-c.MarginOutputTokens {
			return errors.New("mean plus margin exceeds the verified cap")
		}
	case "fixed_estimate":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.MarginOutputTokens != 0 || c.EstimateOutputTokens > c.VerifiedOutputCapTokens {
			return errors.New("fixed_estimate must execute the one global method-config estimate")
		}
	case "no_budget", "settled_only":
		if c.EstimateOutputTokens != 0 || c.MarginOutputTokens != 0 {
			return errors.New("non-reserving comparator contains a reservation estimate")
		}
	case "adaptive_quantile":
		if c.MethodConfig.Adaptive == nil || c.EstimateOutputTokens != c.MethodConfig.Adaptive.InitialOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens || c.MarginOutputTokens != 0 {
			return errors.New("adaptive_quantile initial estimate is not bound to method_config/cap")
		}
	case "expected_cost_router":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens || c.MarginOutputTokens != 0 {
			return errors.New("expected_cost_router trace regime is not bound to its expected output estimate")
		}
	case "oracle_future_cost":
		if c.EstimateOutputTokens != 0 || c.MarginOutputTokens != 0 {
			return errors.New("oracle future cost cannot be prefilled into the online config estimate")
		}
	case "gov_ar":
		if c.EstimateOutputTokens != 0 || c.MarginOutputTokens != 0 {
			return errors.New("GOV-AR must resolve the slot-bound quantile rather than a global scalar")
		}
		if err := validateGOVARConfig(c); err != nil {
			return err
		}
	}
	seen := map[string]struct{}{}
	for index, cohort := range c.Cohorts {
		if !validIdentity(cohort.TenantID) || !validIdentity(cohort.BudgetWindowID) || !validIdentity(cohort.CohortID) || cohort.Size <= 0 {
			return fmt.Errorf("cohort registry entry %d is invalid", index)
		}
		key := cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("cohort registry entry %d is duplicate", index)
		}
		seen[key] = struct{}{}
	}
	return nil
}

type Opportunity struct {
	SchemaVersion               string `json:"schema_version"`
	RecordType                  string `json:"record_type"`
	Sequence                    int64  `json:"sequence"`
	RequestID                   string `json:"request_id"`
	TenantID                    string `json:"tenant_id"`
	BudgetWindowID              string `json:"budget_window_id"`
	WorkloadUID                 string `json:"workload_uid"`
	BudgetMicros                int64  `json:"budget_micros"`
	CohortID                    string `json:"cohort_id,omitempty"`
	CohortIndex                 int64  `json:"cohort_index,omitempty"`
	InputTokens                 int64  `json:"input_tokens"`
	MaxOutputTokens             int64  `json:"max_output_tokens"`
	SettlementDelaySteps        int64  `json:"settlement_delay_steps"`
	FaultMode                   string `json:"fault_mode"`
	FeedbackItemID              string `json:"feedback_item_id"`
	DuplicateSettlement         bool   `json:"duplicate_settlement,omitempty"`
	ConflictingSettlementReplay bool   `json:"conflicting_settlement_replay,omitempty"`
}

func (o Opportunity) Validate() error {
	if o.SchemaVersion != OpportunitySchema || o.RecordType != "opportunity" {
		return errors.New("unsupported opportunity schema or record type")
	}
	for name, value := range map[string]string{
		"request_id": o.RequestID, "tenant_id": o.TenantID, "budget_window_id": o.BudgetWindowID,
		"workload_uid": o.WorkloadUID,
	} {
		if !identityPattern.MatchString(value) {
			return fmt.Errorf("%s is not a safe immutable identity", name)
		}
	}
	if !shaPattern.MatchString(o.FeedbackItemID) {
		return errors.New("feedback_item_id must be the exact canonical prepared-dataset SHA-256 identity")
	}
	if o.CohortID != "" && !identityPattern.MatchString(o.CohortID) {
		return errors.New("cohort_id is not a safe immutable identity")
	}
	if o.Sequence < 0 || o.BudgetMicros <= 0 || o.InputTokens < 0 || o.MaxOutputTokens <= 0 ||
		o.SettlementDelaySteps < 0 || o.CohortIndex < 0 {
		return errors.New("sequence/tokens/delay/index must be non-negative and budget/max output must be positive")
	}
	if o.CohortID == "" && o.CohortIndex != 0 {
		return errors.New("cohort_index requires cohort_id")
	}
	switch o.FaultMode {
	case FaultNominal, FaultProviderCapViolation, FaultKnownInputMismatch:
	default:
		return fmt.Errorf("unsupported typed fault_mode %q", o.FaultMode)
	}
	return nil
}

// SelectedFeedbackReceipt mirrors the audited selected-feedback evaluator
// contract. The evaluator is given only an authorized dispatch digest, never
// request features or an outcome matrix.
type SelectedFeedbackReceipt struct {
	DispatchID      string  `json:"dispatch_id"`
	RequestID       string  `json:"request_id"`
	ItemID          string  `json:"item_id"`
	SelectedModelID string  `json:"selected_model_id"`
	SelectedScore   float64 `json:"selected_score"`
	Replayed        bool    `json:"replayed"`
	ObservationID   string  `json:"observation_id"`
}

func (r SelectedFeedbackReceipt) Validate() error {
	if !shaPattern.MatchString(r.DispatchID) || !shaPattern.MatchString(r.RequestID) || !shaPattern.MatchString(r.ItemID) ||
		!shaPattern.MatchString(r.SelectedModelID) || !shaPattern.MatchString(r.ObservationID) {
		return errors.New("selected-feedback receipt has invalid dispatch or identity")
	}
	if r.SelectedScore != r.SelectedScore || r.SelectedScore < 0 || r.SelectedScore > 1 {
		return errors.New("selected-feedback receipt score must be finite in [0,1]")
	}
	return nil
}

// FeedbackEvidence is copied from the trusted dispatch-authorization row. Its
// hashes bind the authorization to the run, selected deployment, split,
// protocol, selected-feedback artifact, software, and config without exposing
// any non-selected outcome. The synthetic identity is development-only.
type FeedbackEvidence struct {
	RunID                  string `json:"run_id"`
	DatasetID              string `json:"dataset_id"`
	Split                  string `json:"split"`
	ProtocolSHA256         string `json:"protocol_sha256"`
	FeedbackArtifactSHA256 string `json:"feedback_artifact_sha256"`
	SoftwareSHA256         string `json:"software_sha256"`
	ConfigSHA256           string `json:"config_sha256"`
	AuthoritySHA256        string `json:"authority_sha256"`
	ModelMapSHA256         string `json:"model_map_sha256"`
}

func (e FeedbackEvidence) Validate() error {
	validRouterEval := e.DatasetID == "routereval_math_outcomes" && (e.Split == "train" || e.Split == "calibration" || e.Split == "development" || e.Split == "frozen_test")
	validSyntheticP0 := e.DatasetID == "synthetic_p0_outcomes" && e.Split == "development"
	validSyntheticP1A := e.DatasetID == "synthetic_p1a_outcomes" && e.Split == "development"
	if !validIdentity(e.RunID) || (!validRouterEval && !validSyntheticP0 && !validSyntheticP1A) {
		return errors.New("selected-feedback RunBinding identity is invalid")
	}
	for _, value := range []string{e.ProtocolSHA256, e.FeedbackArtifactSHA256, e.SoftwareSHA256, e.ConfigSHA256, e.AuthoritySHA256, e.ModelMapSHA256} {
		if !shaPattern.MatchString(value) {
			return errors.New("selected-feedback authority evidence hash is invalid")
		}
	}
	return nil
}

// DispatchedOutcome is provider usage observed only after dispatch plus the
// selected-model quality receipt. Actual usage is deliberately absent from the
// pre-decision Opportunity type.
type DispatchedOutcome struct {
	ActualInputTokens  int64
	ActualOutputTokens int64
	UsageReceiptSHA256 string
	Feedback           SelectedFeedbackReceipt
	Evidence           FeedbackEvidence
}

type LifecycleRecord struct {
	SchemaVersion               string  `json:"schema_version"`
	RecordType                  string  `json:"record_type"`
	ExperimentID                string  `json:"experiment_id"`
	RunID                       string  `json:"run_id"`
	CellID                      string  `json:"cell_id"`
	Scenario                    string  `json:"scenario"`
	Seed                        uint64  `json:"seed"`
	StreamSHA256                string  `json:"stream_sha256"`
	ConfigSHA256                string  `json:"config_sha256"`
	MatchedStreamKey            string  `json:"matched_stream_key"`
	RegistryID                  string  `json:"registry_id"`
	ProtocolMethodID            string  `json:"protocol_method_id"`
	ComparatorMethod            string  `json:"comparator_method"`
	ProductionReservationMode   string  `json:"production_reservation_mode"`
	MethodConfigSHA256          string  `json:"method_config_sha256"`
	EffectiveMethod             string  `json:"effective_reservation_method,omitempty"`
	AllocatedRiskPPB            int64   `json:"allocated_risk_ppb,omitempty"`
	CalibrationArtifactSHA256   string  `json:"calibration_artifact_sha256,omitempty"`
	CohortRegistrySHA256        string  `json:"cohort_registry_sha256,omitempty"`
	CandidateSetSHA256          string  `json:"candidate_set_sha256,omitempty"`
	JointSelectionSHA256        string  `json:"joint_selection_sha256,omitempty"`
	TrustedEvaluationTime       string  `json:"trusted_evaluation_time,omitempty"`
	CalibrationState            string  `json:"calibration_state,omitempty"`
	ConservativeFallbackReason  string  `json:"conservative_fallback_reason,omitempty"`
	ClusterID                   string  `json:"cluster_id,omitempty"`
	TrialID                     string  `json:"trial_id,omitempty"`
	SourceSHA256                string  `json:"source_sha256"`
	SoftwareSHA256              string  `json:"software_sha256"`
	DataSHA256                  string  `json:"data_sha256"`
	ProtocolSHA256              string  `json:"protocol_sha256"`
	SplitSHA256                 string  `json:"split_sha256"`
	Timestamp                   string  `json:"timestamp"`
	LogicalTimeStep             int64   `json:"logical_time_step"`
	EventIndex                  int64   `json:"event_index"`
	StreamSequence              int64   `json:"stream_sequence"`
	ScheduleTieBreaker          uint64  `json:"schedule_tie_breaker"`
	RequestID                   string  `json:"request_id,omitempty"`
	TenantID                    string  `json:"tenant_id"`
	BudgetWindowID              string  `json:"budget_window_id"`
	EngineWindowID              string  `json:"engine_window_id,omitempty"`
	WorkloadUID                 string  `json:"workload_uid,omitempty"`
	CohortID                    string  `json:"cohort_id,omitempty"`
	CohortIndex                 int64   `json:"cohort_index,omitempty"`
	FaultMode                   string  `json:"fault_mode"`
	LifecycleEvent              string  `json:"lifecycle_event"`
	Decision                    string  `json:"decision,omitempty"`
	ReasonCode                  string  `json:"reason_code"`
	PreviousState               string  `json:"previous_state,omitempty"`
	CurrentState                string  `json:"current_state,omitempty"`
	Effective                   bool    `json:"effective"`
	Selected                    bool    `json:"selected"`
	InputTokens                 int64   `json:"input_tokens"`
	MaxOutputTokens             int64   `json:"max_output_tokens"`
	ActualInputTokens           int64   `json:"actual_input_tokens"`
	ActualOutputTokens          int64   `json:"actual_output_tokens"`
	InputReservedMicros         int64   `json:"input_reserved_micros"`
	OutputReservedMicros        int64   `json:"output_reserved_micros"`
	ActualInputCostMicros       int64   `json:"actual_input_cost_micros"`
	ActualOutputCostMicros      int64   `json:"actual_output_cost_micros"`
	ReservedMicros              int64   `json:"reserved_micros"`
	ActualCostMicros            int64   `json:"actual_estimated_cost_micros"`
	InputPriceMicrosPerMillion  int64   `json:"input_price_micros_per_million"`
	OutputPriceMicrosPerMillion int64   `json:"output_price_micros_per_million"`
	VerifiedOutputCapTokens     int64   `json:"verified_output_cap_tokens"`
	PricingVersion              string  `json:"pricing_version,omitempty"`
	PricingSnapshotSHA256       string  `json:"pricing_snapshot_sha256,omitempty"`
	CapEvidenceSHA256           string  `json:"cap_evidence_sha256,omitempty"`
	PolicyVersion               string  `json:"policy_version,omitempty"`
	SelectedDeployment          string  `json:"selected_deployment,omitempty"`
	SelectedModel               string  `json:"selected_model,omitempty"`
	SelectedProvider            string  `json:"selected_provider,omitempty"`
	ProviderAttemptID           string  `json:"provider_attempt_id,omitempty"`
	DispatchID                  string  `json:"dispatch_id,omitempty"`
	FeedbackObservationID       string  `json:"feedback_observation_id,omitempty"`
	FeedbackRequestSHA256       string  `json:"feedback_request_sha256,omitempty"`
	FeedbackItemSHA256          string  `json:"feedback_item_sha256,omitempty"`
	FeedbackSelectedModelSHA256 string  `json:"feedback_selected_model_sha256,omitempty"`
	FeedbackItemID              string  `json:"feedback_item_id,omitempty"`
	SelectedScore               float64 `json:"selected_score,omitempty"`
	FeedbackReplayed            bool    `json:"feedback_replayed,omitempty"`
	UsageReceiptSHA256          string  `json:"usage_receipt_sha256,omitempty"`
	FeedbackAuthoritySHA256     string  `json:"feedback_authority_sha256,omitempty"`
	FeedbackModelMapSHA256      string  `json:"feedback_model_map_sha256,omitempty"`
	FeedbackArtifactSHA256      string  `json:"feedback_artifact_sha256,omitempty"`
	Attempted                   bool    `json:"attempted"`
	Retried                     int64   `json:"retried"`
	Failed                      bool    `json:"failed"`
	Excluded                    bool    `json:"excluded"`
	Analyzed                    bool    `json:"analyzed"`
	ExclusionReason             string  `json:"exclusion_reason,omitempty"`
	BudgetMicros                int64   `json:"budget_micros"`
	SettledMicros               int64   `json:"settled_micros"`
	OutstandingMicros           int64   `json:"outstanding_liability_micros"`
	CarriedMicros               int64   `json:"carried_adjustment_micros"`
	AvailableMicros             int64   `json:"available_budget_micros"`
	ActiveReservations          int64   `json:"active_reservations"`
	ActiveActualMicros          int64   `json:"active_actual_micros"`
	ActiveReservedMicros        int64   `json:"active_reserved_micros"`
}

type ExactRate struct {
	Numerator   int64 `json:"numerator"`
	Denominator int64 `json:"denominator"`
	RatePPB     int64 `json:"rate_ppb"`
}

type Metrics struct {
	RawRecordCount                         int64     `json:"raw_record_count"`
	UniqueRequestCount                     int64     `json:"unique_request_count"`
	AdmittedRequestCount                   int64     `json:"admitted_request_count"`
	DispatchedRequestCount                 int64     `json:"dispatched_request_count"`
	AttemptedObservationCount              int64     `json:"attempted_observation_count"`
	AnalyzedObservationCount               int64     `json:"analyzed_observation_count"`
	FailedObservationCount                 int64     `json:"failed_observation_count"`
	ExcludedObservationCount               int64     `json:"excluded_observation_count"`
	RequestUnderReservation                ExactRate `json:"request_under_reservation"`
	RequestUnderReservationMagnitudeMicros int64     `json:"request_under_reservation_magnitude_micros"`
	FixedCohortCount                       int64     `json:"fixed_cohort_count"`
	FixedCohortLiabilityExceedance         ExactRate `json:"fixed_cohort_liability_exceedance"`
	FixedCohortExceedanceMagnitudeMicros   int64     `json:"fixed_cohort_exceedance_magnitude_micros"`
	ActiveLiabilitySnapshots               int64     `json:"active_liability_snapshots"`
	InstantaneousActiveLiabilityExceedance ExactRate `json:"instantaneous_active_liability_exceedance"`
	InstantaneousExceedanceMagnitudeMicros int64     `json:"instantaneous_exceedance_magnitude_micros"`
	TenantWindowCount                      int64     `json:"tenant_window_count"`
	TenantBudgetWindowOvershoot            ExactRate `json:"tenant_budget_window_overshoot"`
	TenantWindowOvershootMagnitudeMicros   int64     `json:"tenant_window_overshoot_magnitude_micros"`
	PeakExposureMicros                     int64     `json:"peak_exposure_micros"`
	TotalWindowPeakExposureMicros          int64     `json:"total_window_peak_exposure_micros"`
	PeakLatentFinancialExposureMicros      int64     `json:"peak_latent_financial_exposure_micros"`
	TotalWindowPeakLatentExposureMicros    int64     `json:"total_window_peak_latent_exposure_micros"`
	PeakActualOutstandingLiabilityMicros   int64     `json:"peak_actual_outstanding_liability_micros"`
	TotalWindowPeakActualOutstandingMicros int64     `json:"total_window_peak_actual_outstanding_micros"`
	TotalWindowFinalSettledMicros          int64     `json:"total_window_final_settled_micros"`
	TotalWindowBudgetMicros                int64     `json:"total_window_budget_micros"`
	BudgetUtilizationPPB                   int64     `json:"budget_utilization_ppb"`
	PeakExposureRatioPPB                   int64     `json:"peak_exposure_ratio_ppb"`
	PeakLatentExposureRatioPPB             int64     `json:"peak_latent_exposure_ratio_ppb"`
}

type ArtifactFileSpec struct {
	Path                string `json:"path"`
	SHA256              string `json:"sha256"`
	Schema              string `json:"schema"`
	ExpectedRecordCount int64  `json:"expected_record_count,omitempty"`
}
type RawFileSpec struct {
	Path                string `json:"path"`
	SHA256              string `json:"sha256"`
	UncompressedSHA256  string `json:"uncompressed_sha256"`
	Schema              string `json:"schema"`
	ExpectedRecordCount int64  `json:"expected_record_count"`
}

type Manifest struct {
	SchemaVersion                  string           `json:"schema_version"`
	RecordType                     string           `json:"record_type"`
	ExperimentID                   string           `json:"experiment_id"`
	RunID                          string           `json:"run_id"`
	CellID                         string           `json:"cell_id"`
	Scenario                       string           `json:"scenario"`
	Seed                           uint64           `json:"seed"`
	RegistryID                     string           `json:"registry_id"`
	ProtocolMethodID               string           `json:"protocol_method_id"`
	ComparatorMethod               string           `json:"comparator_method"`
	ProductionReservationMode      string           `json:"production_reservation_mode"`
	MethodConfigSHA256             string           `json:"method_config_sha256"`
	ClusterID                      string           `json:"cluster_id,omitempty"`
	TrialID                        string           `json:"trial_id,omitempty"`
	EvidenceTier                   string           `json:"evidence_tier"`
	EvidenceLabel                  string           `json:"evidence_label"`
	SoftwareSHA256                 string           `json:"software_sha256"`
	DataSHA256                     string           `json:"data_sha256"`
	ProtocolSHA256                 string           `json:"protocol_sha256"`
	SplitSHA256                    string           `json:"split_sha256"`
	SelectedFeedbackModelMapSHA256 string           `json:"selected_feedback_model_map_sha256"`
	SourceSHA256                   string           `json:"source_sha256"`
	ConfigSHA256                   string           `json:"config_sha256"`
	CanonicalConfigSHA256          string           `json:"canonical_config_sha256"`
	StreamSHA256                   string           `json:"stream_sha256"`
	MatchedStreamKey               string           `json:"matched_stream_key"`
	OutputSHA256                   string           `json:"output_sha256"`
	ConfigInput                    ArtifactFileSpec `json:"config_input"`
	StreamInput                    ArtifactFileSpec `json:"stream_input"`
	FeedbackInput                  ArtifactFileSpec `json:"feedback_input"`
	FeedbackAccessInput            ArtifactFileSpec `json:"selected_feedback_access_input"`
	RawFiles                       []RawFileSpec    `json:"raw_files"`
	DecisionCounts                 map[string]int64 `json:"decision_counts"`
	LifecycleCounts                map[string]int64 `json:"lifecycle_counts"`
	Metrics                        Metrics          `json:"metrics"`
}

func cohortKey(tenant, window, cohort string) string {
	return tenant + "\x00" + window + "\x00" + cohort
}
func sortedWindowKeys(budgets map[string]int64) []string {
	keys := make([]string, 0, len(budgets))
	for k := range budgets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func validIdentity(value string) bool { return identityPattern.MatchString(value) }
