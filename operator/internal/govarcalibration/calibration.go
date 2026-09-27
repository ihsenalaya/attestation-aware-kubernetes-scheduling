// Package govarcalibration deterministically constructs immutable upper-token
// calibration artifacts and evaluates an independent monitoring stream. It has
// no clock, Kubernetes, or database dependency, so two producers given the same
// frozen rows must produce byte-identical digests.
package govarcalibration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	SplitTrain       = "train"
	SplitDevelopment = "development"
	SplitCalibration = "calibration"
	SplitMonitoring  = "monitoring"
	SplitFrozenTest  = "frozen_test"
	// This bound keeps every integer PPB product below int64 overflow and also
	// rejects an operationally implausible in-memory artifact build.
	maxArtifactSupport = 10_000_000
)

// Observation is the minimal authoritative-final outcome used by the producer.
// Selected=false represents a hidden counterfactual outcome and is rejected.
type Observation struct {
	RequestID            string    `json:"request_id"`
	ProviderAttemptID    string    `json:"provider_attempt_id"`
	Split                string    `json:"split"`
	Selected             bool      `json:"selected"`
	AuthoritativeFinal   bool      `json:"authoritative_final"`
	Excluded             bool      `json:"excluded"`
	OutputTokens         int64     `json:"output_tokens"`
	FeatureSchemaVersion string    `json:"feature_schema_version"`
	PriceRegimeSHA256    string    `json:"price_regime_sha256"`
	CapRegimeSHA256      string    `json:"cap_regime_sha256"`
	SettledAt            time.Time `json:"settled_at"`
}

// BuildConfig contains only frozen values and therefore deliberately excludes
// a wall clock or mutable resource version.
type BuildConfig struct {
	ArtifactRef            string `json:"artifact_ref"`
	Version                string `json:"version"`
	FeatureSchemaVersion   string `json:"feature_schema_version"`
	PriceRegimeSHA256      string `json:"price_regime_sha256"`
	CapRegimeSHA256        string `json:"cap_regime_sha256"`
	ProducerSoftwareSHA256 string `json:"producer_software_sha256"`
	CoverageTargetPPB      int64  `json:"coverage_target_ppb"`
}

// Artifact is the complete immutable evidence published into policy status.
type Artifact struct {
	SchemaVersion                string `json:"schema_version"`
	CalibrationMethod            string `json:"calibration_method"`
	ArtifactRef                  string `json:"artifact_ref"`
	Version                      string `json:"version"`
	FeatureSchemaVersion         string `json:"feature_schema_version"`
	PriceRegimeSHA256            string `json:"price_regime_sha256"`
	CapRegimeSHA256              string `json:"cap_regime_sha256"`
	ProducerSoftwareSHA256       string `json:"producer_software_sha256"`
	FeatureRegimeSHA256          string `json:"feature_regime_sha256,omitempty"`
	SplitOpportunityRegimeSHA256 string `json:"split_opportunity_regime_sha256,omitempty"`
	CohortRegimeSHA256           string `json:"cohort_regime_sha256,omitempty"`
	CoverageTargetPPB            int64  `json:"coverage_target_ppb"`
	MinimumSupport               int64  `json:"minimum_support,omitempty"`
	EmpiricalCoveragePPB         int64  `json:"empirical_coverage_ppb"`
	// ExchangeableMarginalCoverageLowerPPB is the finite-sample lower
	// coverage implied by the selected split-conformal rank, assuming the
	// calibration observations and the next outcome are exchangeable. It is
	// not a conditional or drift-robust guarantee.
	ExchangeableMarginalCoverageLowerPPB int64     `json:"exchangeable_marginal_coverage_lower_ppb"`
	CoverageBoundKind                    string    `json:"coverage_bound_kind"`
	CoverageBoundNumerator               int64     `json:"coverage_bound_numerator"`
	CoverageBoundDenominator             int64     `json:"coverage_bound_denominator"`
	CoverageIntervalLowerPPB             int64     `json:"coverage_interval_lower_ppb"`
	CoverageIntervalUpperPPB             int64     `json:"coverage_interval_upper_ppb"`
	CoverageConfidencePPB                int64     `json:"coverage_confidence_ppb"`
	ConformalRank                        int64     `json:"conformal_rank"`
	Support                              int64     `json:"support"`
	UpperOutputTokens                    int64     `json:"upper_output_tokens"`
	WindowStart                          time.Time `json:"window_start"`
	WindowEnd                            time.Time `json:"window_end"`
	SourceObservationsSHA256             string    `json:"source_observations_sha256"`
	ArtifactSHA256                       string    `json:"artifact_sha256"`
}

// DriftResult is derived only from monitoring observations.
type DriftResult struct {
	Detector              string    `json:"detector"`
	ThresholdPPB          int64     `json:"threshold_ppb"`
	MonitoringInputSHA256 string    `json:"monitoring_input_sha256"`
	Support               int64     `json:"support"`
	Covered               int64     `json:"covered"`
	EmpiricalCoveragePPB  int64     `json:"empirical_coverage_ppb"`
	ConfidencePPB         int64     `json:"confidence_ppb"`
	IntervalLowerPPB      int64     `json:"interval_lower_ppb"`
	IntervalUpperPPB      int64     `json:"interval_upper_ppb"`
	CoverageGapPPB        int64     `json:"coverage_gap_ppb"`
	Detected              bool      `json:"detected"`
	WindowStart           time.Time `json:"window_start"`
	WindowEnd             time.Time `json:"window_end"`
}

func ParseObservations(raw []byte) ([]Observation, error) {
	var rows []Observation
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rows); err != nil {
		return nil, fmt.Errorf("decode observations: %w", err)
	}
	if len(rows) == 0 {
		return nil, errors.New("observation set is empty")
	}
	return rows, nil
}

// ObservationDigest returns the digest over normalized, identity-sorted rows.
// It rejects duplicate attempt identities so ordering cannot hide replacement.
func ObservationDigest(rows []Observation) (string, error) {
	normalized := append([]Observation(nil), rows...)
	for i := range normalized {
		normalized[i].Split = strings.ToLower(strings.TrimSpace(normalized[i].Split))
		normalized[i].SettledAt = normalized[i].SettledAt.UTC()
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].RequestID != normalized[j].RequestID {
			return normalized[i].RequestID < normalized[j].RequestID
		}
		return normalized[i].ProviderAttemptID < normalized[j].ProviderAttemptID
	})
	for i, row := range normalized {
		if row.RequestID == "" || row.ProviderAttemptID == "" {
			return "", errors.New("request_id and provider_attempt_id are required")
		}
		if i > 0 && normalized[i-1].RequestID == row.RequestID && normalized[i-1].ProviderAttemptID == row.ProviderAttemptID {
			return "", fmt.Errorf("duplicate observation identity %s/%s", row.RequestID, row.ProviderAttemptID)
		}
	}
	b, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// BuildArtifact accepts only selected authoritative-final calibration rows.
// Development, frozen-test, monitoring, counterfactual, provisional, or
// excluded rows cause the whole build to fail rather than being silently used.
// It uses a split-conformal order statistic: k=ceil((n+1)*target). If k>n,
// the finite calibration set cannot support the target without an infinite or
// externally validated cap, so the build fails into the conservative fallback.
func BuildArtifact(cfg BuildConfig, rows []Observation) (Artifact, error) {
	if err := validateConfig(cfg); err != nil {
		return Artifact{}, err
	}
	if len(rows) == 0 || len(rows) > maxArtifactSupport {
		return Artifact{}, errors.New("unsupported calibration support")
	}
	for _, row := range rows {
		if err := validateRow(row, cfg, false); err != nil {
			return Artifact{}, err
		}
	}
	digest, err := ObservationDigest(rows)
	if err != nil {
		return Artifact{}, err
	}
	tokens := make([]int64, len(rows))
	start, end := rows[0].SettledAt.UTC(), rows[0].SettledAt.UTC()
	for i, row := range rows {
		tokens[i] = row.OutputTokens
		at := row.SettledAt.UTC()
		if at.Before(start) {
			start = at
		}
		if at.After(end) {
			end = at
		}
	}
	sort.Slice(tokens, func(i, j int) bool { return tokens[i] < tokens[j] })
	// Split-conformal upper order statistic. Under exchangeability, a fresh
	// outcome is no greater than X_(k) with marginal probability at least
	// k/(n+1). This is deliberately not called a distribution-free claim when
	// exchangeability, selected-route sampling, or the frozen regime fails.
	n := int64(len(tokens))
	rank := (cfg.CoverageTargetPPB*(n+1) + 1_000_000_000 - 1) / 1_000_000_000
	if rank > n {
		return Artifact{}, fmt.Errorf("support %d cannot establish target %d ppb with a finite split-conformal upper bound", n, cfg.CoverageTargetPPB)
	}
	upper := tokens[rank-1]
	covered := int64(0)
	for _, value := range tokens {
		if value <= upper {
			covered++
		}
	}
	artifact := Artifact{
		SchemaVersion: "govar-calibration-v2", CalibrationMethod: "split_conformal_order_statistic_v1", ArtifactRef: cfg.ArtifactRef,
		Version: cfg.Version, FeatureSchemaVersion: cfg.FeatureSchemaVersion,
		PriceRegimeSHA256: cfg.PriceRegimeSHA256, CapRegimeSHA256: cfg.CapRegimeSHA256,
		ProducerSoftwareSHA256:               cfg.ProducerSoftwareSHA256,
		CoverageTargetPPB:                    cfg.CoverageTargetPPB,
		EmpiricalCoveragePPB:                 covered * 1_000_000_000 / n,
		ExchangeableMarginalCoverageLowerPPB: rank * 1_000_000_000 / (n + 1),
		CoverageBoundKind:                    ExactMarginalBoundKind, CoverageBoundNumerator: rank, CoverageBoundDenominator: n + 1,
		CoverageIntervalLowerPPB: rank * 1_000_000_000 / (n + 1), CoverageIntervalUpperPPB: 1_000_000_000,
		CoverageConfidencePPB: 1_000_000_000,
		ConformalRank:         rank,
		Support:               n, UpperOutputTokens: upper, WindowStart: start, WindowEnd: end,
		SourceObservationsSHA256: digest,
	}
	b, err := json.Marshal(artifact)
	if err != nil {
		return Artifact{}, err
	}
	sum := sha256.Sum256(b)
	artifact.ArtifactSHA256 = hex.EncodeToString(sum[:])
	return artifact, nil
}

// DetectCoverageGap evaluates only selected authoritative-final monitoring rows
// from the same feature, pricing, and cap regimes as the artifact.
func DetectCoverageGap(artifact Artifact, thresholdPPB int64, rows []Observation) (DriftResult, error) {
	if thresholdPPB < 0 || thresholdPPB > 1_000_000_000 {
		return DriftResult{}, errors.New("threshold_ppb must be in [0,1000000000]")
	}
	cfg := BuildConfig{FeatureSchemaVersion: artifact.FeatureSchemaVersion, PriceRegimeSHA256: artifact.PriceRegimeSHA256, CapRegimeSHA256: artifact.CapRegimeSHA256}
	if len(rows) == 0 {
		return DriftResult{}, errors.New("monitoring observation set is empty")
	}
	for _, row := range rows {
		if err := validateRow(row, cfg, true); err != nil {
			return DriftResult{}, err
		}
	}
	digest, err := ObservationDigest(rows)
	if err != nil {
		return DriftResult{}, err
	}
	start, end := rows[0].SettledAt.UTC(), rows[0].SettledAt.UTC()
	covered := int64(0)
	for _, row := range rows {
		at := row.SettledAt.UTC()
		if at.Before(start) {
			start = at
		}
		if at.After(end) {
			end = at
		}
		if row.OutputTokens <= artifact.UpperOutputTokens {
			covered++
		}
	}
	support := int64(len(rows))
	coverage := covered * 1_000_000_000 / support
	lower, upper := wilsonIntervalPPB(covered, support)
	gap := artifact.CoverageTargetPPB - coverage
	if gap < 0 {
		gap = 0
	}
	return DriftResult{Detector: "coverage-gap", ThresholdPPB: thresholdPPB,
		MonitoringInputSHA256: digest, Support: support, Covered: covered, EmpiricalCoveragePPB: coverage,
		ConfidencePPB: 950_000_000, IntervalLowerPPB: lower, IntervalUpperPPB: upper,
		CoverageGapPPB: gap, Detected: gap > thresholdPPB, WindowStart: start, WindowEnd: end}, nil
}

func wilsonIntervalPPB(covered, support int64) (int64, int64) {
	if support <= 0 || covered < 0 || covered > support {
		return 0, 1_000_000_000
	}
	z := 1.959963984540054
	n := float64(support)
	p := float64(covered) / n
	denominator := 1 + z*z/n
	center := (p + z*z/(2*n)) / denominator
	half := z * math.Sqrt((p*(1-p)+z*z/(4*n))/n) / denominator
	lower := int64(math.Floor(math.Max(0, center-half) * 1_000_000_000))
	upper := int64(math.Ceil(math.Min(1, center+half) * 1_000_000_000))
	return lower, upper
}

func validateConfig(cfg BuildConfig) error {
	if strings.TrimSpace(cfg.ArtifactRef) == "" || strings.TrimSpace(cfg.Version) == "" || strings.TrimSpace(cfg.FeatureSchemaVersion) == "" {
		return errors.New("artifact_ref, version, and feature_schema_version are required")
	}
	for name, value := range map[string]string{"price_regime_sha256": cfg.PriceRegimeSHA256, "cap_regime_sha256": cfg.CapRegimeSHA256, "producer_software_sha256": cfg.ProducerSoftwareSHA256} {
		if !isSHA256(value) {
			return fmt.Errorf("%s must be a lower-case SHA-256", name)
		}
	}
	if cfg.CoverageTargetPPB < 1 || cfg.CoverageTargetPPB > 1_000_000_000 {
		return errors.New("coverage_target_ppb must be in [1,1000000000]")
	}
	return nil
}

func validateRow(row Observation, cfg BuildConfig, monitoring bool) error {
	if row.RequestID == "" || row.ProviderAttemptID == "" || row.OutputTokens < 0 || row.SettledAt.IsZero() {
		return fmt.Errorf("observation %q has missing identity, negative output, or zero settlement time", row.RequestID)
	}
	if !row.Selected {
		return fmt.Errorf("observation %s is an unselected counterfactual outcome", row.RequestID)
	}
	if !row.AuthoritativeFinal {
		return fmt.Errorf("observation %s is not authoritative-final", row.RequestID)
	}
	if row.Excluded {
		return fmt.Errorf("observation %s is excluded", row.RequestID)
	}
	split := strings.ToLower(strings.TrimSpace(row.Split))
	if monitoring {
		if split != SplitMonitoring {
			return fmt.Errorf("observation %s split %q is not monitoring", row.RequestID, row.Split)
		}
	} else if split != SplitCalibration {
		return fmt.Errorf("observation %s split %q is forbidden calibration input", row.RequestID, row.Split)
	}
	if row.FeatureSchemaVersion != cfg.FeatureSchemaVersion || row.PriceRegimeSHA256 != cfg.PriceRegimeSHA256 || row.CapRegimeSHA256 != cfg.CapRegimeSHA256 {
		return fmt.Errorf("observation %s feature/price/cap regime mismatch", row.RequestID)
	}
	return nil
}

func isSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
