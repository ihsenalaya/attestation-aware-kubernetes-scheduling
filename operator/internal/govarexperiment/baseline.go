package govarexperiment

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
)

const MethodConfigSchema = "govar-budget-admission-method-v1"

var budgetAdmissionMethods = map[string]string{
	"no_budget":            "observational",
	"settled_only":         "settled_only",
	"mean":                 "mean",
	"mean_margin":          "fixed_margin",
	"fixed_quantile":       "fixed_quantile",
	"strict_max":           "strict_provider_cap",
	"fixed_estimate":       "mean",
	"adaptive_quantile":    "adaptive_quantile",
	"expected_cost_router": "mean",
	"oracle_future_cost":   "mean",
	"gov_ar":               "govar_fixed_cohort",
}

// MethodConfig is the immutable, prospectively selected comparator artifact.
// It contains no test outcome. MethodConfigSHA256 binds its canonical JSON into
// every config, lifecycle row, and manifest.
type MethodConfig struct {
	SchemaVersion string `json:"schema_version"`
	ProtocolID    string `json:"protocol_id"`

	// StaticOutputTokens is the globally invariant E_fixed for fixed_estimate.
	// For mean/fixed_quantile it is the value of the pre-test artifact for the
	// single declared trace regime. MeanMarginOutputTokens is additive.
	StaticOutputTokens     int64  `json:"static_output_tokens,omitempty"`
	MeanMarginOutputTokens int64  `json:"mean_margin_output_tokens,omitempty"`
	ArtifactSHA256         string `json:"artifact_sha256,omitempty"`

	Adaptive     *AdaptiveMethodConfig     `json:"adaptive,omitempty"`
	ExpectedCost *ExpectedCostMethodConfig `json:"expected_cost,omitempty"`
	Oracle       *OracleMethodConfig       `json:"oracle,omitempty"`
	GOVAR        *GOVARMethodConfig        `json:"gov_ar,omitempty"`
}

type AdaptiveMethodConfig struct {
	InitialOutputTokens   int64  `json:"initial_output_tokens"`
	CoverageTargetPPB     int64  `json:"coverage_target_ppb"`
	MinimumSupport        int64  `json:"minimum_support"`
	CalibrationSupport    int64  `json:"calibration_support"`
	UpdateEverySteps      int64  `json:"update_every_steps"`
	HistoryWindow         int64  `json:"history_window"`
	DriftThresholdPPB     int64  `json:"drift_threshold_ppb"`
	Fallback              string `json:"fallback"`
	CandidateRegimeSHA256 string `json:"candidate_regime_sha256"`
}

type ExpectedCostMethodConfig struct {
	UtilityCostPenaltyPPBPerMicro int64  `json:"utility_cost_penalty_ppb_per_micro"`
	PredictorArtifactSHA256       string `json:"predictor_artifact_sha256"`
}

type OracleMethodConfig struct {
	Phase                       string `json:"phase"`
	PairedNonOracleCommitSHA256 string `json:"paired_nonoracle_commit_sha256"`
	PairedRouteSHA256           string `json:"paired_route_sha256"`
}

type GOVARMethodConfig struct {
	TenantRiskPPB              int64                      `json:"tenant_risk_ppb"`
	FrozenAt                   string                     `json:"frozen_at"`
	Fallback                   string                     `json:"fallback"`
	DriftThresholdPPB          int64                      `json:"drift_threshold_ppb"`
	MaxAgeSeconds              int64                      `json:"max_age_seconds"`
	RevalidationMinimumSupport int64                      `json:"revalidation_minimum_support"`
	CandidateSet               []GOVARCandidateBinding    `json:"candidate_set"`
	CandidateSetSHA256         string                     `json:"candidate_set_sha256"`
	JointSelection             GOVARJointSelectionBinding `json:"joint_selection"`
	CalibrationProfiles        []GOVARCalibrationProfile  `json:"calibration_profiles"`
	EvidenceRegistry           []GOVARCalibrationEvidence `json:"evidence_registry"`
	SlotBounds                 []GOVARSlotBound           `json:"slot_bounds"`
}

// GOVARCandidateBinding closes the exact feasible candidate, price snapshot,
// verified cap path, and monetary regime supplied to the production chooser.
// P1 deliberately has one candidate, but still commits the complete set so a
// future outcome cannot influence which candidate was presented.
type GOVARCandidateBinding struct {
	CandidateID                 string `json:"candidate_id"`
	RouteSnapshotSHA256         string `json:"route_snapshot_sha256"`
	PricingSnapshotSHA256       string `json:"pricing_snapshot_sha256"`
	PriceRegimeSHA256           string `json:"price_regime_sha256"`
	CapEvidenceSHA256           string `json:"cap_evidence_sha256"`
	CapRegimeSHA256             string `json:"cap_regime_sha256"`
	InputPriceMicrosPerMillion  int64  `json:"input_price_micros_per_million"`
	OutputPriceMicrosPerMillion int64  `json:"output_price_micros_per_million"`
	VerifiedOutputCapTokens     int64  `json:"verified_output_cap_tokens"`
	HardFeasible                bool   `json:"hard_feasible"`
	FeasibilitySHA256           string `json:"feasibility_sha256"`
}

// GOVARJointSelectionBinding commits the exact objective implemented by
// chooseAdmission. A zero switch penalty is still explicit: silently changing
// it would change the artifact digest and invalidate the run.
type GOVARJointSelectionBinding struct {
	Objective           string `json:"objective"`
	QualityWeightPPB    int64  `json:"quality_weight_ppb"`
	CostWeightPPB       int64  `json:"cost_weight_ppb"`
	SwitchPenaltyMicros int64  `json:"switch_penalty_micros"`
	TieBreak            string `json:"tie_break"`
	CandidateSetSHA256  string `json:"candidate_set_sha256"`
	SelectedCandidateID string `json:"selected_candidate_id"`
	PriorCandidateID    string `json:"prior_candidate_id"`
	ArtifactSHA256      string `json:"artifact_sha256"`
}

type GOVARSlotBound struct {
	TenantID                  string `json:"tenant_id"`
	BudgetWindowID            string `json:"budget_window_id"`
	CohortID                  string `json:"cohort_id"`
	CohortIndex               int64  `json:"cohort_index"`
	RequestID                 string `json:"request_id"`
	OpportunityDigest         string `json:"opportunity_digest"`
	WeightPPB                 int64  `json:"weight_ppb"`
	AllocatedRiskPPB          int64  `json:"allocated_risk_ppb"`
	CoverageTargetPPB         int64  `json:"coverage_target_ppb"`
	CalibrationProfileSHA256  string `json:"calibration_profile_sha256"`
	EvidencePublicationSHA256 string `json:"evidence_publication_sha256"`
}

type GOVARCalibrationProfile struct {
	ProfileID            string       `json:"profile_id"`
	Scenario             string       `json:"scenario"`
	FeatureSchemaVersion string       `json:"feature_schema_version"`
	TaskClass            string       `json:"task_class"`
	TailClass            string       `json:"tail_class"`
	AssignmentRule       string       `json:"assignment_rule"`
	DefinitionSHA256     string       `json:"definition_sha256"`
	CandidateSetSHA256   string       `json:"candidate_set_sha256"`
	AssignedCohorts      []CohortSpec `json:"assigned_cohorts"`
	ProfileSHA256        string       `json:"profile_sha256"`
}

type GOVARCalibrationEvidence struct {
	PublicationSHA256        string                                      `json:"publication_sha256"`
	CalibrationProfileSHA256 string                                      `json:"calibration_profile_sha256"`
	Calibration              govarcalibration.Artifact                   `json:"calibration"`
	CalibrationRows          []govarcalibration.AuthoritativeObservation `json:"calibration_rows"`
	Drift                    govarcalibration.AuthoritativeDriftWindow   `json:"drift"`
	MonitoringRows           []govarcalibration.AuthoritativeObservation `json:"monitoring_rows"`
}

func GOVARCandidateSetDigest(candidates []GOVARCandidateBinding) (string, error) {
	if len(candidates) != 1 {
		return "", errors.New("P1 GOV-AR candidate set must contain exactly one fixed candidate")
	}
	normalized := append([]GOVARCandidateBinding(nil), candidates...)
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].CandidateID < normalized[j].CandidateID })
	for _, candidate := range normalized {
		if !validIdentity(candidate.CandidateID) || !shaPattern.MatchString(candidate.RouteSnapshotSHA256) ||
			!shaPattern.MatchString(candidate.PricingSnapshotSHA256) || !shaPattern.MatchString(candidate.PriceRegimeSHA256) ||
			!shaPattern.MatchString(candidate.CapEvidenceSHA256) || !shaPattern.MatchString(candidate.CapRegimeSHA256) || candidate.InputPriceMicrosPerMillion < 0 ||
			candidate.OutputPriceMicrosPerMillion < 0 || candidate.VerifiedOutputCapTokens <= 0 || !candidate.HardFeasible ||
			!shaPattern.MatchString(candidate.FeasibilitySHA256) {
			return "", errors.New("GOV-AR candidate set contains an incomplete price/cap binding")
		}
	}
	raw, err := json.Marshal(struct {
		Schema     string                  `json:"schema"`
		Candidates []GOVARCandidateBinding `json:"candidates"`
	}{Schema: "govar-p1-fixed-candidate-set-v1", Candidates: normalized})
	if err != nil {
		return "", err
	}
	return DomainHash("govar-p1-fixed-candidate-set-v1", raw), nil
}

func GOVARJointSelectionDigest(binding GOVARJointSelectionBinding) (string, error) {
	if binding.Objective != "cost" || binding.QualityWeightPPB != 0 || binding.CostWeightPPB != 1_000_000_000 ||
		binding.SwitchPenaltyMicros != 0 || binding.PriorCandidateID != "" || binding.TieBreak != "reserved_cost_then_model_ref" ||
		!shaPattern.MatchString(binding.CandidateSetSHA256) || !validIdentity(binding.SelectedCandidateID) {
		return "", errors.New("GOV-AR joint selection is not the fixed P1 production objective")
	}
	copy := binding
	copy.ArtifactSHA256 = ""
	raw, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	return DomainHash("govar-p1-joint-selection-v1", raw), nil
}

func GOVARCalibrationProfileDigest(profile GOVARCalibrationProfile) (string, error) {
	if !validIdentity(profile.ProfileID) || !validIdentity(profile.Scenario) || !validIdentity(profile.FeatureSchemaVersion) ||
		!validIdentity(profile.TaskClass) || !validIdentity(profile.TailClass) || !validIdentity(profile.AssignmentRule) ||
		!shaPattern.MatchString(profile.DefinitionSHA256) ||
		!shaPattern.MatchString(profile.CandidateSetSHA256) {
		return "", errors.New("GOV-AR calibration profile is incomplete")
	}
	seen := map[string]struct{}{}
	for _, cohort := range profile.AssignedCohorts {
		if !validIdentity(cohort.TenantID) || !validIdentity(cohort.BudgetWindowID) || !validIdentity(cohort.CohortID) || cohort.Size <= 0 {
			return "", errors.New("GOV-AR calibration profile cohort assignment is incomplete")
		}
		key := cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)
		if _, duplicate := seen[key]; duplicate {
			return "", errors.New("GOV-AR calibration profile cohort assignment is duplicate")
		}
		seen[key] = struct{}{}
	}
	if len(seen) == 0 {
		return "", errors.New("GOV-AR calibration profile has no cohort assignment")
	}
	copy := profile
	copy.ProfileSHA256 = ""
	raw, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	return DomainHash("govar-p1-calibration-profile-v1", raw), nil
}

func registryID(method string) string { return "budget_admission/" + method }

func productionMode(method string) (string, bool) {
	mode, ok := budgetAdmissionMethods[method]
	return mode, ok
}

func MethodConfigDigest(config MethodConfig) (string, error) {
	if err := config.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	return DomainHash("govar-budget-admission-method-config-v1", raw), nil
}

func (c MethodConfig) Validate() error {
	if c.SchemaVersion != MethodConfigSchema {
		return errors.New("unsupported method_config schema")
	}
	mode, known := productionMode(c.ProtocolID)
	if !known || mode == "" {
		return fmt.Errorf("unknown budget/admission protocol_id %q", c.ProtocolID)
	}
	if c.StaticOutputTokens < 0 || c.MeanMarginOutputTokens < 0 {
		return errors.New("static output estimates and margin must be non-negative")
	}
	if c.ArtifactSHA256 != "" && !shaPattern.MatchString(c.ArtifactSHA256) {
		return errors.New("method artifact must be a SHA-256")
	}
	if c.Adaptive != nil && c.ProtocolID != "adaptive_quantile" {
		return errors.New("adaptive config is allowed only for adaptive_quantile")
	}
	if c.ExpectedCost != nil && c.ProtocolID != "expected_cost_router" {
		return errors.New("expected_cost config is allowed only for expected_cost_router")
	}
	if c.Oracle != nil && c.ProtocolID != "oracle_future_cost" {
		return errors.New("oracle config is allowed only for oracle_future_cost")
	}
	if c.GOVAR != nil && c.ProtocolID != "gov_ar" {
		return errors.New("GOV-AR config is allowed only for gov_ar")
	}
	switch c.ProtocolID {
	case "no_budget", "settled_only", "strict_max":
		if c.StaticOutputTokens != 0 || c.MeanMarginOutputTokens != 0 || c.ArtifactSHA256 != "" || c.Adaptive != nil || c.ExpectedCost != nil || c.Oracle != nil || c.GOVAR != nil {
			return fmt.Errorf("%s accepts no fitted reservation artifact", c.ProtocolID)
		}
	case "mean", "fixed_quantile":
		if c.ArtifactSHA256 == "" || c.MeanMarginOutputTokens != 0 {
			return fmt.Errorf("%s requires one hash-bound pre-test artifact and no margin", c.ProtocolID)
		}
	case "mean_margin":
		if c.ArtifactSHA256 == "" {
			return errors.New("mean_margin requires one hash-bound pre-test artifact")
		}
	case "fixed_estimate":
		if c.ArtifactSHA256 == "" {
			return errors.New("fixed_estimate requires provenance for the one global estimate")
		}
		if c.MeanMarginOutputTokens != 0 {
			return errors.New("fixed_estimate cannot contain a margin")
		}
	case "adaptive_quantile":
		if c.Adaptive == nil || c.ArtifactSHA256 == "" {
			return errors.New("adaptive_quantile requires its initial artifact and update contract")
		}
		if err := c.Adaptive.Validate(); err != nil {
			return err
		}
	case "expected_cost_router":
		if c.ExpectedCost == nil || !shaPattern.MatchString(c.ExpectedCost.PredictorArtifactSHA256) || c.ExpectedCost.UtilityCostPenaltyPPBPerMicro < 0 {
			return errors.New("expected_cost_router requires a hash-bound predictor and non-negative integer utility coefficient")
		}
	case "oracle_future_cost":
		if c.Oracle == nil || c.Oracle.Phase != "post_nonoracle_evaluator" || !shaPattern.MatchString(c.Oracle.PairedNonOracleCommitSHA256) || !shaPattern.MatchString(c.Oracle.PairedRouteSHA256) {
			return errors.New("oracle_future_cost requires a post-nonoracle evaluator phase and paired commitment/route hashes")
		}
	case "gov_ar":
		if c.GOVAR == nil {
			return errors.New("gov_ar requires an immutable risk allocation and slot-bound artifact")
		}
		if err := c.GOVAR.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (c AdaptiveMethodConfig) Validate() error {
	if c.InitialOutputTokens < 0 || c.CoverageTargetPPB <= 0 || c.CoverageTargetPPB > 1_000_000_000 || c.MinimumSupport <= 0 || c.CalibrationSupport < 0 || c.UpdateEverySteps <= 0 || c.HistoryWindow < c.MinimumSupport || c.DriftThresholdPPB < 0 || c.DriftThresholdPPB > 1_000_000_000 || !shaPattern.MatchString(c.CandidateRegimeSHA256) {
		return errors.New("adaptive method parameters are outside their valid domain")
	}
	if c.Fallback != "strict_provider_cap" {
		return errors.New("trace comparator currently supports only the conservative strict_provider_cap adaptive fallback")
	}
	return nil
}

func (c GOVARMethodConfig) Validate() error {
	if c.TenantRiskPPB < 0 || c.TenantRiskPPB > 1_000_000_000 || c.DriftThresholdPPB < 0 ||
		c.DriftThresholdPPB > 1_000_000_000 || c.MaxAgeSeconds <= 0 || c.RevalidationMinimumSupport <= 0 ||
		c.Fallback != "strict_provider_cap" || len(c.SlotBounds) == 0 {
		return errors.New("GOV-AR risk, fallback, drift, and slot bounds are incomplete")
	}
	frozenAt, err := time.Parse(time.RFC3339Nano, c.FrozenAt)
	if err != nil || frozenAt.IsZero() || frozenAt.Format(time.RFC3339Nano) != c.FrozenAt {
		return errors.New("GOV-AR frozen_at must be canonical RFC3339Nano")
	}
	candidateSetSHA, err := GOVARCandidateSetDigest(c.CandidateSet)
	if err != nil || c.CandidateSetSHA256 != candidateSetSHA {
		return errors.New("GOV-AR candidate set digest does not recompute")
	}
	jointSHA, err := GOVARJointSelectionDigest(c.JointSelection)
	if err != nil || c.JointSelection.ArtifactSHA256 != jointSHA ||
		c.JointSelection.CandidateSetSHA256 != c.CandidateSetSHA256 ||
		c.JointSelection.SelectedCandidateID != c.CandidateSet[0].CandidateID {
		return errors.New("GOV-AR joint candidate/reservation objective binding does not recompute")
	}
	profiles := make(map[string]GOVARCalibrationProfile, len(c.CalibrationProfiles))
	profileAssignments := map[string]string{}
	for _, profile := range c.CalibrationProfiles {
		digest, err := GOVARCalibrationProfileDigest(profile)
		if err != nil || profile.ProfileSHA256 != digest {
			return errors.New("GOV-AR calibration profile digest does not recompute")
		}
		if _, duplicate := profiles[digest]; duplicate {
			return errors.New("GOV-AR calibration profile registry contains a duplicate")
		}
		profiles[digest] = profile
		for _, cohort := range profile.AssignedCohorts {
			key := cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)
			if _, duplicate := profileAssignments[key]; duplicate {
				return errors.New("GOV-AR cohort is assigned to multiple calibration profiles")
			}
			profileAssignments[key] = digest
		}
	}
	if len(profiles) == 0 {
		return errors.New("GOV-AR calibration profile registry is empty")
	}
	evidenceRegistry := make(map[string]GOVARCalibrationEvidence, len(c.EvidenceRegistry))
	for _, evidence := range c.EvidenceRegistry {
		if _, duplicate := evidenceRegistry[evidence.PublicationSHA256]; duplicate {
			return errors.New("GOV-AR evidence registry contains a duplicate publication")
		}
		if _, ok := profiles[evidence.CalibrationProfileSHA256]; !ok {
			return errors.New("GOV-AR evidence references an absent calibration profile")
		}
		if err := validateGOVAREvidence(evidence, c.CandidateSet[0], c.DriftThresholdPPB, c.CandidateSetSHA256, c.JointSelection.ArtifactSHA256); err != nil {
			return fmt.Errorf("GOV-AR evidence %s: %w", evidence.PublicationSHA256, err)
		}
		evidenceRegistry[evidence.PublicationSHA256] = evidence
	}
	if len(evidenceRegistry) == 0 {
		return errors.New("GOV-AR evidence registry is empty")
	}
	byCohort := map[string][]GOVARSlotBound{}
	seen := map[string]struct{}{}
	for _, slot := range c.SlotBounds {
		cohortKey := slot.TenantID + "\x00" + slot.BudgetWindowID + "\x00" + slot.CohortID
		key := fmt.Sprintf("%s\x00%d", cohortKey, slot.CohortIndex)
		if !validIdentity(slot.TenantID) || !validIdentity(slot.BudgetWindowID) || !validIdentity(slot.CohortID) ||
			!validIdentity(slot.RequestID) || !shaPattern.MatchString(slot.OpportunityDigest) || slot.CohortIndex < 0 ||
			slot.WeightPPB < 0 || slot.WeightPPB > 1_000_000_000 || !shaPattern.MatchString(slot.CalibrationProfileSHA256) ||
			!shaPattern.MatchString(slot.EvidencePublicationSHA256) {
			return errors.New("GOV-AR contains an invalid slot-bound artifact")
		}
		if _, duplicate := seen[key]; duplicate {
			return errors.New("GOV-AR contains a duplicate cohort slot")
		}
		if profileAssignments[cohortKey] != slot.CalibrationProfileSHA256 {
			return errors.New("GOV-AR slot profile does not match its pre-outcome cohort assignment")
		}
		seen[key] = struct{}{}
		expectedRisk := c.TenantRiskPPB * slot.WeightPPB / 1_000_000_000
		if slot.AllocatedRiskPPB != expectedRisk || slot.CoverageTargetPPB != 1_000_000_000-expectedRisk {
			return errors.New("GOV-AR slot quantile target is not bound to its allocated risk")
		}
		evidence, ok := evidenceRegistry[slot.EvidencePublicationSHA256]
		if !ok || evidence.Calibration.CoverageTargetPPB != slot.CoverageTargetPPB ||
			evidence.CalibrationProfileSHA256 != slot.CalibrationProfileSHA256 {
			return fmt.Errorf("GOV-AR slot %s/%d does not resolve exact risk-bound evidence", cohortKey, slot.CohortIndex)
		}
		byCohort[cohortKey] = append(byCohort[cohortKey], slot)
	}
	for cohortID, slots := range byCohort {
		sort.Slice(slots, func(i, j int) bool { return slots[i].CohortIndex < slots[j].CohortIndex })
		var sum int64
		for index, slot := range slots {
			if slot.CohortIndex != int64(index) || sum > 1_000_000_000-slot.WeightPPB {
				return fmt.Errorf("GOV-AR cohort %s is not complete or its weights overflow", cohortID)
			}
			sum += slot.WeightPPB
		}
		if sum != 1_000_000_000 {
			return fmt.Errorf("GOV-AR cohort %s weights sum to %d", cohortID, sum)
		}
	}
	return nil
}

func validateGOVAREvidence(evidence GOVARCalibrationEvidence, candidate GOVARCandidateBinding, threshold int64, candidateSetSHA, jointSHA string) error {
	a := evidence.Calibration
	if a.SchemaVersion != govarcalibration.AuthoritativeArtifactSchema || a.CalibrationMethod != "split_conformal_order_statistic_v1" ||
		!validIdentity(a.ArtifactRef) || !validIdentity(a.Version) || !validIdentity(a.FeatureSchemaVersion) ||
		a.PriceRegimeSHA256 != candidate.PriceRegimeSHA256 || a.CapRegimeSHA256 != candidate.CapRegimeSHA256 ||
		!shaPattern.MatchString(evidence.CalibrationProfileSHA256) || a.CohortRegimeSHA256 != evidence.CalibrationProfileSHA256 ||
		!shaPattern.MatchString(a.ProducerSoftwareSHA256) || !shaPattern.MatchString(a.FeatureRegimeSHA256) ||
		!shaPattern.MatchString(a.SplitOpportunityRegimeSHA256) || !shaPattern.MatchString(a.CohortRegimeSHA256) ||
		a.CoverageTargetPPB <= 0 || a.MinimumSupport <= 0 || a.Support < a.MinimumSupport ||
		a.ConformalRank <= 0 || a.ConformalRank > a.Support || a.CoverageBoundKind != govarcalibration.ExactMarginalBoundKind ||
		a.CoverageBoundNumerator != a.ConformalRank || a.CoverageBoundDenominator != a.Support+1 ||
		a.ExchangeableMarginalCoverageLowerPPB != a.ConformalRank*1_000_000_000/(a.Support+1) ||
		a.CoverageIntervalLowerPPB != a.ExchangeableMarginalCoverageLowerPPB ||
		a.CoverageIntervalLowerPPB < a.CoverageTargetPPB || a.CoverageIntervalUpperPPB != 1_000_000_000 ||
		a.CoverageConfidencePPB != 1_000_000_000 || a.UpperOutputTokens < 0 ||
		!shaPattern.MatchString(a.SourceObservationsSHA256) || a.WindowStart.IsZero() || a.WindowEnd.Before(a.WindowStart) {
		return errors.New("authoritative calibration fields are incomplete or inconsistent")
	}
	rebuilt, err := govarcalibration.BuildAuthoritativeArtifact(govarcalibration.AuthoritativeBuildConfig{
		ArtifactRef: a.ArtifactRef, Version: a.Version, FeatureSchemaVersion: a.FeatureSchemaVersion,
		CoverageTargetPPB: a.CoverageTargetPPB, MinimumSupport: a.MinimumSupport,
		Regimes: govarcalibration.RegimeDigests{FeatureSHA256: a.FeatureRegimeSHA256, PriceSHA256: a.PriceRegimeSHA256,
			CapPathAdapterSHA256: a.CapRegimeSHA256, SplitOpportunitySHA256: a.SplitOpportunityRegimeSHA256,
			CohortSHA256: a.CohortRegimeSHA256, ProducerSoftwareSHA256: a.ProducerSoftwareSHA256},
	}, evidence.CalibrationRows)
	if err != nil || !reflect.DeepEqual(rebuilt, a) {
		return errors.New("calibration rows do not independently rebuild the exact rank, upper token bound, and artifact")
	}
	d := evidence.Drift
	if d.SchemaVersion != "govar-authoritative-drift-v1" ||
		d.ArtifactSHA256 != a.ArtifactSHA256 || d.Regimes.FeatureSHA256 != a.FeatureRegimeSHA256 ||
		d.Regimes.PriceSHA256 != a.PriceRegimeSHA256 || d.Regimes.CapPathAdapterSHA256 != a.CapRegimeSHA256 ||
		d.Regimes.SplitOpportunitySHA256 != a.SplitOpportunityRegimeSHA256 || d.Regimes.CohortSHA256 != a.CohortRegimeSHA256 ||
		d.Regimes.ProducerSoftwareSHA256 != a.ProducerSoftwareSHA256 || d.Result.Detector != "coverage-gap" ||
		d.Result.ThresholdPPB != threshold || !shaPattern.MatchString(d.Result.MonitoringInputSHA256) || d.Result.Support <= 0 ||
		d.Result.Covered < 0 || d.Result.Covered > d.Result.Support || d.Result.EmpiricalCoveragePPB != d.Result.Covered*1_000_000_000/d.Result.Support ||
		d.Result.CoverageGapPPB != max64(0, a.CoverageTargetPPB-d.Result.EmpiricalCoveragePPB) ||
		d.Result.Detected != (d.Result.CoverageGapPPB > threshold) || d.Result.WindowStart.IsZero() || d.Result.WindowEnd.Before(d.Result.WindowStart) {
		return errors.New("authoritative drift artifact does not recompute or match calibration regimes")
	}
	rebuiltDrift, err := govarcalibration.BuildAuthoritativeDriftWindow(a, d.Regimes, threshold, evidence.MonitoringRows)
	if err != nil || !reflect.DeepEqual(rebuiltDrift, d) {
		return errors.New("monitoring rows do not independently rebuild the exact drift artifact")
	}
	publication, err := GOVAREvidencePublicationDigest(evidence, candidateSetSHA, jointSHA)
	if err != nil || evidence.PublicationSHA256 != publication {
		return errors.New("evidence publication digest does not bind calibration, drift, candidate set, and objective")
	}
	return nil
}

func GOVAREvidencePublicationDigest(evidence GOVARCalibrationEvidence, candidateSetSHA, jointSHA string) (string, error) {
	if !shaPattern.MatchString(candidateSetSHA) || !shaPattern.MatchString(jointSHA) ||
		!shaPattern.MatchString(evidence.Calibration.ArtifactSHA256) || !shaPattern.MatchString(evidence.Drift.DriftSHA256) {
		return "", errors.New("slot publication inputs are incomplete")
	}
	raw, err := json.Marshal(struct {
		Schema                   string `json:"schema"`
		CalibrationProfileSHA256 string `json:"calibration_profile_sha256"`
		CoverageTargetPPB        int64  `json:"coverage_target_ppb"`
		CalibrationSHA256        string `json:"calibration_sha256"`
		DriftSHA256              string `json:"drift_sha256"`
		CandidateSetSHA256       string `json:"candidate_set_sha256"`
		JointSelectionSHA256     string `json:"joint_selection_sha256"`
	}{"govar-p1-evidence-publication-v1", evidence.CalibrationProfileSHA256, evidence.Calibration.CoverageTargetPPB,
		evidence.Calibration.ArtifactSHA256, evidence.Drift.DriftSHA256, candidateSetSHA, jointSHA})
	if err != nil {
		return "", err
	}
	return DomainHash("govar-p1-evidence-publication-v1", raw), nil
}

func (c GOVARMethodConfig) Evidence(publication string) (GOVARCalibrationEvidence, error) {
	for _, evidence := range c.EvidenceRegistry {
		if evidence.PublicationSHA256 == publication {
			return evidence, nil
		}
	}
	return GOVARCalibrationEvidence{}, errors.New("GOV-AR evidence publication is absent from the immutable registry")
}

func (c GOVARMethodConfig) Slot(tenant, window, cohort string, index int64, candidateSetSHA string) (GOVARSlotBound, error) {
	if err := c.Validate(); err != nil {
		return GOVARSlotBound{}, err
	}
	if candidateSetSHA != c.CandidateSetSHA256 {
		return GOVARSlotBound{}, errors.New("GOV-AR exact candidate set mismatch")
	}
	for _, slot := range c.SlotBounds {
		if slot.TenantID == tenant && slot.BudgetWindowID == window && slot.CohortID == cohort && slot.CohortIndex == index {
			return slot, nil
		}
	}
	return GOVARSlotBound{}, errors.New("GOV-AR opportunity is absent from the immutable slot-bound artifact")
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type AdmissionFacts struct {
	HardFeasible      bool
	BudgetMicros      int64
	SettledMicros     int64
	OutstandingMicros int64
	CarriedMicros     int64
}

// NoBudgetDecision deliberately has no monetary parameters. This type-level
// boundary makes budget/liability metamorphic independence auditable.
func NoBudgetDecision(hardFeasible bool) (string, string) {
	if hardFeasible {
		return "ADMIT", "hard_feasible_budget_disabled"
	}
	return "ABSTAIN", "no_candidate_after_governance"
}

// SettledOnlyDecision reads settled and carried spend, never outstanding
// liability. A zero or negative available balance is unavailable.
func SettledOnlyDecision(f AdmissionFacts) (string, string, error) {
	if f.BudgetMicros <= 0 || f.SettledMicros < 0 || f.CarriedMicros < 0 {
		return "", "", errors.New("invalid settled-only accounting facts")
	}
	if !f.HardFeasible {
		return "ABSTAIN", "no_candidate_after_governance", nil
	}
	if f.BudgetMicros-f.SettledMicros-f.CarriedMicros <= 0 {
		return "QUEUE", "budget_unavailable", nil
	}
	return "ADMIT", "settled_only_positive_available", nil
}

type SettledOnlyLedger struct {
	mu          sync.Mutex
	settled     map[string]int64
	settlements map[string]settledOnlyEntry
}

type settledOnlyEntry struct {
	TenantWindow string
	ActualMicros int64
}

func NewSettledOnlyLedger() *SettledOnlyLedger {
	return &SettledOnlyLedger{settled: map[string]int64{}, settlements: map[string]settledOnlyEntry{}}
}

func (l *SettledOnlyLedger) Settled(tenantWindow string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.settled[tenantWindow]
}

// Settle is idempotent for an identical request/cost and rejects a conflicting
// replay. Pending admissions are intentionally absent from this ledger.
func (l *SettledOnlyLedger) Settle(tenantWindow, requestID string, actualMicros int64) (bool, error) {
	if tenantWindow == "" || requestID == "" || actualMicros < 0 {
		return false, errors.New("invalid settled-only settlement")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if prior, ok := l.settlements[requestID]; ok {
		if prior.TenantWindow != tenantWindow || prior.ActualMicros != actualMicros {
			return false, errors.New("conflicting settled-only settlement replay")
		}
		return false, nil
	}
	if l.settled[tenantWindow] > int64(^uint64(0)>>1)-actualMicros {
		return false, errors.New("settled-only accounting overflow")
	}
	l.settlements[requestID] = settledOnlyEntry{TenantWindow: tenantWindow, ActualMicros: actualMicros}
	l.settled[tenantWindow] += actualMicros
	return true, nil
}

type AdaptiveFeedback struct {
	FinalizedStep int64
	OutputTokens  int64
	Selected      bool
	Final         bool
}

type AdaptivePlan struct {
	OutputTokens   int64
	EffectiveMode  string
	FallbackReason string
	Support        int64
}

// AdaptiveEstimator consumes only finalized selected feedback strictly before
// the decision step. Feedback at the current or a future step is never read.
type AdaptiveEstimator struct {
	config   AdaptiveMethodConfig
	feedback []AdaptiveFeedback
}

func NewAdaptiveEstimator(config AdaptiveMethodConfig) (*AdaptiveEstimator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &AdaptiveEstimator{config: config}, nil
}

func (e *AdaptiveEstimator) Observe(feedback AdaptiveFeedback) error {
	if feedback.FinalizedStep < 0 || feedback.OutputTokens < 0 {
		return errors.New("adaptive feedback has an invalid step or output")
	}
	if !feedback.Selected || !feedback.Final {
		return errors.New("adaptive updates require selected, finalized feedback")
	}
	e.feedback = append(e.feedback, feedback)
	return nil
}

func (e *AdaptiveEstimator) Plan(decisionStep int64, candidateRegime string, capTokens int64) (AdaptivePlan, error) {
	if decisionStep < 0 || capTokens <= 0 {
		return AdaptivePlan{}, errors.New("adaptive decision step/cap is invalid")
	}
	strict := func(reason string, support int64) AdaptivePlan {
		return AdaptivePlan{OutputTokens: capTokens, EffectiveMode: "strict_provider_cap", FallbackReason: reason, Support: support}
	}
	if candidateRegime != e.config.CandidateRegimeSHA256 {
		return strict("candidate_regime_mismatch", 0), nil
	}
	if e.config.CalibrationSupport < e.config.MinimumSupport {
		return strict("calibration_support_insufficient", e.config.CalibrationSupport), nil
	}
	prior := make([]int64, 0, len(e.feedback))
	for _, item := range e.feedback {
		if item.FinalizedStep < decisionStep {
			prior = append(prior, item.OutputTokens)
		}
	}
	if int64(len(prior)) > e.config.HistoryWindow {
		prior = prior[len(prior)-int(e.config.HistoryWindow):]
	}
	estimate := e.config.InitialOutputTokens
	if decisionStep > 0 && decisionStep%e.config.UpdateEverySteps == 0 && int64(len(prior)) >= e.config.MinimumSupport {
		sort.Slice(prior, func(i, j int) bool { return prior[i] < prior[j] })
		rank := conformalRank(int64(len(prior)), e.config.CoverageTargetPPB)
		estimate = prior[rank-1]
		var covered int64
		for _, value := range prior {
			if value <= estimate {
				covered++
			}
		}
		empirical := covered * 1_000_000_000 / int64(len(prior))
		if empirical+e.config.DriftThresholdPPB < e.config.CoverageTargetPPB {
			return strict("coverage_drift_detected", int64(len(prior))), nil
		}
	}
	if estimate > capTokens {
		estimate = capTokens
	}
	return AdaptivePlan{OutputTokens: estimate, EffectiveMode: "adaptive_quantile", Support: int64(len(prior))}, nil
}

func conformalRank(n, targetPPB int64) int64 {
	// ceil((n+1)*target), clipped to the n available observations.
	rank := ((n+1)*targetPPB + 1_000_000_000 - 1) / 1_000_000_000
	if rank < 1 {
		return 1
	}
	if rank > n {
		return n
	}
	return rank
}

type ExpectedCandidate struct {
	ID                     string
	ExpectedQualityPPB     int64
	ExpectedReservedMicros int64
	HardFeasible           bool
}

// SelectExpectedCostCandidate is a deterministic integer implementation of
// the prospective expected-quality/cost utility. It never accepts actual cost
// or an upper-tail estimate.
func SelectExpectedCostCandidate(candidates []ExpectedCandidate, availableMicros, costPenaltyPPBPerMicro int64) (ExpectedCandidate, error) {
	if availableMicros < 0 || costPenaltyPPBPerMicro < 0 {
		return ExpectedCandidate{}, errors.New("expected-cost availability/coefficient is invalid")
	}
	var selected ExpectedCandidate
	selectedSet := false
	var selectedUtility int64
	for _, candidate := range candidates {
		if !validIdentity(candidate.ID) || candidate.ExpectedQualityPPB < 0 || candidate.ExpectedQualityPPB > 1_000_000_000 || candidate.ExpectedReservedMicros < 0 {
			return ExpectedCandidate{}, errors.New("expected-cost candidate is invalid")
		}
		if !candidate.HardFeasible || candidate.ExpectedReservedMicros > availableMicros {
			continue
		}
		if costPenaltyPPBPerMicro != 0 && candidate.ExpectedReservedMicros > (int64(^uint64(0)>>1)-candidate.ExpectedQualityPPB)/costPenaltyPPBPerMicro {
			return ExpectedCandidate{}, errors.New("expected-cost utility overflows int64")
		}
		utility := candidate.ExpectedQualityPPB - candidate.ExpectedReservedMicros*costPenaltyPPBPerMicro
		if !selectedSet || utility > selectedUtility || (utility == selectedUtility && candidate.ID < selected.ID) {
			selected, selectedUtility, selectedSet = candidate, utility, true
		}
	}
	if !selectedSet {
		return ExpectedCandidate{}, errors.New("no hard-feasible expected-cost candidate fits full liability")
	}
	return selected, nil
}

type OracleFutureCostSource interface {
	FutureCostForCommittedRoute(requestID, pairedRouteSHA256, nonOracleCommitSHA256 string) (int64, error)
}

// RevealOracleFutureCost is intentionally separate from every non-oracle
// planner. Its phase and immutable paired decision/route commitments must be
// present before the evaluator source is invoked.
func RevealOracleFutureCost(config OracleMethodConfig, requestID string, source OracleFutureCostSource) (int64, error) {
	if source == nil {
		return 0, errors.New("oracle evaluator source is required")
	}
	if config.Phase != "post_nonoracle_evaluator" || !shaPattern.MatchString(config.PairedNonOracleCommitSHA256) || !shaPattern.MatchString(config.PairedRouteSHA256) {
		return 0, errors.New("oracle access attempted without a committed non-oracle phase and paired route")
	}
	cost, err := source.FutureCostForCommittedRoute(requestID, config.PairedRouteSHA256, config.PairedNonOracleCommitSHA256)
	if err != nil {
		return 0, err
	}
	if cost < 0 {
		return 0, errors.New("oracle evaluator returned negative cost")
	}
	return cost, nil
}
