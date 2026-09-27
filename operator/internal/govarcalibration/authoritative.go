package govarcalibration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	AuthoritativeArtifactSchema = "govar-calibration-authoritative-v1"
	ExactMarginalBoundKind      = "exchangeable_split_conformal_exact_rank"
)

// RegimeDigests are the closed identities that must agree at opportunity
// registration, ledger materialization, artifact publication, and admission.
// They are digests rather than user labels so a semantic change cannot reuse an
// artifact by retaining a mutable name.
type RegimeDigests struct {
	FeatureSHA256          string `json:"feature_sha256"`
	PriceSHA256            string `json:"price_sha256"`
	CapPathAdapterSHA256   string `json:"cap_path_adapter_sha256"`
	SplitOpportunitySHA256 string `json:"split_opportunity_sha256"`
	CohortSHA256           string `json:"cohort_sha256"`
	ProducerSoftwareSHA256 string `json:"producer_software_sha256"`
}

type AuthoritativeObservation struct {
	RequestID         string        `json:"request_id"`
	ProviderAttemptID string        `json:"provider_attempt_id"`
	OpportunityID     string        `json:"opportunity_id"`
	Split             string        `json:"split"`
	OutputTokens      int64         `json:"output_tokens"`
	SettledAt         time.Time     `json:"settled_at"`
	Regimes           RegimeDigests `json:"regimes"`
}

type AuthoritativeBuildConfig struct {
	ArtifactRef          string        `json:"artifact_ref"`
	Version              string        `json:"version"`
	FeatureSchemaVersion string        `json:"feature_schema_version"`
	CoverageTargetPPB    int64         `json:"coverage_target_ppb"`
	MinimumSupport       int64         `json:"minimum_support"`
	Regimes              RegimeDigests `json:"regimes"`
}

type AuthoritativeDriftWindow struct {
	SchemaVersion  string        `json:"schema_version"`
	ArtifactSHA256 string        `json:"artifact_sha256"`
	DriftSHA256    string        `json:"drift_sha256"`
	Result         DriftResult   `json:"result"`
	Regimes        RegimeDigests `json:"regimes"`
}

// CanonicalDigest length-prefixes every field. This avoids delimiter ambiguity
// and makes the digest definition portable to SQL, Python, and artifact tools.
func CanonicalDigest(domain string, fields ...string) (string, error) {
	domain = strings.TrimSpace(domain)
	if domain == "" {
		return "", errors.New("digest domain is required")
	}
	h := sha256.New()
	for _, value := range append([]string{domain}, fields...) {
		if len(value) > 1<<24 {
			return "", errors.New("canonical digest field is too large")
		}
		_, _ = h.Write([]byte(strconv.Itoa(len(value))))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func CanonicalFeatureRegime(featureSchemaVersion, canonicalFeatureSHA256 string) (string, error) {
	if strings.TrimSpace(featureSchemaVersion) == "" || !isSHA256(canonicalFeatureSHA256) {
		return "", errors.New("feature schema and canonical feature SHA-256 are required")
	}
	return CanonicalDigest("govar-feature-regime-v1", featureSchemaVersion, canonicalFeatureSHA256)
}

func CanonicalPriceRegime(pricingSnapshotSHA256, providerUID, deployment string) (string, error) {
	if !isSHA256(pricingSnapshotSHA256) || strings.TrimSpace(providerUID) == "" || strings.TrimSpace(deployment) == "" {
		return "", errors.New("price snapshot, provider UID, and deployment are required")
	}
	return CanonicalDigest("govar-price-regime-v1", pricingSnapshotSHA256, providerUID, deployment)
}

func CanonicalCapPathAdapterRegime(capEvidenceSHA256, providerUID string, providerGeneration int64, deployment, pathMode, adapterVersion, requestParameter string, maxOutputTokens int64) (string, error) {
	if !isSHA256(capEvidenceSHA256) || strings.TrimSpace(providerUID) == "" || providerGeneration <= 0 ||
		strings.TrimSpace(deployment) == "" || strings.TrimSpace(pathMode) == "" || strings.TrimSpace(adapterVersion) == "" ||
		strings.TrimSpace(requestParameter) == "" || maxOutputTokens <= 0 {
		return "", errors.New("complete verified cap and path-adapter identity is required")
	}
	return CanonicalDigest("govar-cap-path-adapter-regime-v1", capEvidenceSHA256, providerUID,
		strconv.FormatInt(providerGeneration, 10), deployment, pathMode, adapterVersion, requestParameter,
		strconv.FormatInt(maxOutputTokens, 10))
}

func CanonicalSplitOpportunityRegime(registrySHA256, opportunitySetSHA256, split, cohortSHA256 string) (string, error) {
	if !isSHA256(registrySHA256) || !isSHA256(opportunitySetSHA256) || !isSHA256(cohortSHA256) || !validSplit(split) {
		return "", errors.New("registry, opportunity-set, split, and cohort identities are required")
	}
	return CanonicalDigest("govar-split-opportunity-regime-v1", registrySHA256, opportunitySetSHA256, split, cohortSHA256)
}

func ValidateRegimes(r RegimeDigests) error {
	for name, value := range map[string]string{
		"feature": r.FeatureSHA256, "price": r.PriceSHA256, "cap_path_adapter": r.CapPathAdapterSHA256,
		"split_opportunity": r.SplitOpportunitySHA256, "cohort": r.CohortSHA256,
		"producer_software": r.ProducerSoftwareSHA256,
	} {
		if !isSHA256(value) {
			return fmt.Errorf("%s regime must be a lower-case SHA-256", name)
		}
	}
	return nil
}

func AuthoritativeObservationDigest(rows []AuthoritativeObservation) (string, error) {
	normalized := append([]AuthoritativeObservation(nil), rows...)
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].RequestID != normalized[j].RequestID {
			return normalized[i].RequestID < normalized[j].RequestID
		}
		return normalized[i].ProviderAttemptID < normalized[j].ProviderAttemptID
	})
	for i := range normalized {
		normalized[i].Split = strings.ToLower(strings.TrimSpace(normalized[i].Split))
		normalized[i].SettledAt = normalized[i].SettledAt.UTC()
		if err := validateAuthoritativeObservation(normalized[i]); err != nil {
			return "", err
		}
		if i > 0 && normalized[i-1].RequestID == normalized[i].RequestID {
			return "", fmt.Errorf("duplicate authoritative request %s", normalized[i].RequestID)
		}
	}
	b, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// BuildAuthoritativeArtifact accepts no selected/final/excluded booleans. The
// PostgreSQL producer creates these rows only by joining the one selected
// reservation to its authoritative-final settlement and pre-admission split
// assignment. Frozen-test rows are rejected and are never materialized by the
// database path.
func BuildAuthoritativeArtifact(cfg AuthoritativeBuildConfig, rows []AuthoritativeObservation) (Artifact, error) {
	if strings.TrimSpace(cfg.ArtifactRef) == "" || strings.TrimSpace(cfg.Version) == "" || strings.TrimSpace(cfg.FeatureSchemaVersion) == "" {
		return Artifact{}, errors.New("artifact_ref, version, and feature schema are required")
	}
	if err := ValidateRegimes(cfg.Regimes); err != nil {
		return Artifact{}, err
	}
	if cfg.MinimumSupport <= 0 || int64(len(rows)) < cfg.MinimumSupport {
		return Artifact{}, fmt.Errorf("support %d is below required minimum %d", len(rows), cfg.MinimumSupport)
	}
	if len(rows) > maxArtifactSupport || cfg.CoverageTargetPPB < 1 || cfg.CoverageTargetPPB > 1_000_000_000 {
		return Artifact{}, errors.New("coverage target or support is invalid")
	}
	tokens := make([]int64, len(rows))
	var start, end time.Time
	for i, row := range rows {
		if err := validateAuthoritativeObservation(row); err != nil {
			return Artifact{}, err
		}
		if row.Split != SplitCalibration {
			return Artifact{}, fmt.Errorf("request %s split %q is structurally forbidden calibration input", row.RequestID, row.Split)
		}
		if row.Regimes != cfg.Regimes {
			return Artifact{}, fmt.Errorf("request %s has a regime mismatch", row.RequestID)
		}
		tokens[i] = row.OutputTokens
		at := row.SettledAt.UTC()
		if i == 0 || at.Before(start) {
			start = at
		}
		if i == 0 || at.After(end) {
			end = at
		}
	}
	sourceDigest, err := AuthoritativeObservationDigest(rows)
	if err != nil {
		return Artifact{}, err
	}
	sort.Slice(tokens, func(i, j int) bool { return tokens[i] < tokens[j] })
	n := int64(len(tokens))
	rank := (cfg.CoverageTargetPPB*(n+1) + 999_999_999) / 1_000_000_000
	if rank > n {
		return Artifact{}, fmt.Errorf("support %d cannot establish target %d ppb with a finite split-conformal upper bound", n, cfg.CoverageTargetPPB)
	}
	covered := int64(0)
	upper := tokens[rank-1]
	for _, value := range tokens {
		if value <= upper {
			covered++
		}
	}
	a := Artifact{
		SchemaVersion: AuthoritativeArtifactSchema, CalibrationMethod: "split_conformal_order_statistic_v1",
		ArtifactRef: cfg.ArtifactRef, Version: cfg.Version,
		FeatureSchemaVersion: cfg.FeatureSchemaVersion, PriceRegimeSHA256: cfg.Regimes.PriceSHA256,
		CapRegimeSHA256: cfg.Regimes.CapPathAdapterSHA256, ProducerSoftwareSHA256: cfg.Regimes.ProducerSoftwareSHA256,
		FeatureRegimeSHA256: cfg.Regimes.FeatureSHA256, SplitOpportunityRegimeSHA256: cfg.Regimes.SplitOpportunitySHA256,
		CohortRegimeSHA256: cfg.Regimes.CohortSHA256,
		CoverageTargetPPB:  cfg.CoverageTargetPPB, MinimumSupport: cfg.MinimumSupport, EmpiricalCoveragePPB: covered * 1_000_000_000 / n,
		ExchangeableMarginalCoverageLowerPPB: rank * 1_000_000_000 / (n + 1),
		CoverageBoundKind:                    ExactMarginalBoundKind, CoverageBoundNumerator: rank, CoverageBoundDenominator: n + 1,
		CoverageIntervalLowerPPB: rank * 1_000_000_000 / (n + 1), CoverageIntervalUpperPPB: 1_000_000_000,
		CoverageConfidencePPB: 1_000_000_000, ConformalRank: rank, Support: n, UpperOutputTokens: upper,
		WindowStart: start, WindowEnd: end, SourceObservationsSHA256: sourceDigest,
	}
	b, err := json.Marshal(a)
	if err != nil {
		return Artifact{}, err
	}
	sum := sha256.Sum256(b)
	a.ArtifactSHA256 = hex.EncodeToString(sum[:])
	return a, nil
}

func BuildAuthoritativeDriftWindow(artifact Artifact, regimes RegimeDigests, thresholdPPB int64, rows []AuthoritativeObservation) (AuthoritativeDriftWindow, error) {
	var window AuthoritativeDriftWindow
	if artifact.SchemaVersion != AuthoritativeArtifactSchema || !isSHA256(artifact.ArtifactSHA256) ||
		thresholdPPB < 0 || thresholdPPB > 1_000_000_000 || len(rows) == 0 {
		return window, errors.New("authoritative artifact, threshold, and monitoring rows are required")
	}
	if err := ValidateRegimes(regimes); err != nil {
		return window, err
	}
	if artifact.FeatureRegimeSHA256 != regimes.FeatureSHA256 || artifact.PriceRegimeSHA256 != regimes.PriceSHA256 ||
		artifact.CapRegimeSHA256 != regimes.CapPathAdapterSHA256 || artifact.CohortRegimeSHA256 != regimes.CohortSHA256 ||
		artifact.ProducerSoftwareSHA256 != regimes.ProducerSoftwareSHA256 {
		return window, errors.New("drift regime does not match immutable artifact")
	}
	start, end := rows[0].SettledAt.UTC(), rows[0].SettledAt.UTC()
	covered := int64(0)
	for _, row := range rows {
		if err := validateAuthoritativeObservation(row); err != nil {
			return window, err
		}
		if row.Split != SplitMonitoring || row.Regimes.FeatureSHA256 != regimes.FeatureSHA256 ||
			row.Regimes.PriceSHA256 != regimes.PriceSHA256 || row.Regimes.CapPathAdapterSHA256 != regimes.CapPathAdapterSHA256 ||
			row.Regimes.SplitOpportunitySHA256 != regimes.SplitOpportunitySHA256 ||
			row.Regimes.CohortSHA256 != regimes.CohortSHA256 || row.Regimes.ProducerSoftwareSHA256 != regimes.ProducerSoftwareSHA256 {
			return window, fmt.Errorf("monitoring request %s has forbidden split or regime mismatch", row.RequestID)
		}
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
	digest, err := AuthoritativeObservationDigest(rows)
	if err != nil {
		return window, err
	}
	support := int64(len(rows))
	coverage := covered * 1_000_000_000 / support
	lower, upper := wilsonIntervalPPB(covered, support)
	gap := artifact.CoverageTargetPPB - coverage
	if gap < 0 {
		gap = 0
	}
	window = AuthoritativeDriftWindow{SchemaVersion: "govar-authoritative-drift-v1", ArtifactSHA256: artifact.ArtifactSHA256,
		Regimes: regimes, Result: DriftResult{Detector: "coverage-gap", ThresholdPPB: thresholdPPB,
			MonitoringInputSHA256: digest, Support: support, Covered: covered, EmpiricalCoveragePPB: coverage,
			ConfidencePPB: 950_000_000, IntervalLowerPPB: lower, IntervalUpperPPB: upper,
			CoverageGapPPB: gap, Detected: gap > thresholdPPB, WindowStart: start, WindowEnd: end}}
	b, err := json.Marshal(window)
	if err != nil {
		return AuthoritativeDriftWindow{}, err
	}
	sum := sha256.Sum256(b)
	window.DriftSHA256 = hex.EncodeToString(sum[:])
	return window, nil
}

func validateAuthoritativeObservation(row AuthoritativeObservation) error {
	if strings.TrimSpace(row.RequestID) == "" || strings.TrimSpace(row.ProviderAttemptID) == "" || strings.TrimSpace(row.OpportunityID) == "" ||
		row.OutputTokens < 0 || row.SettledAt.IsZero() || !validSplit(row.Split) {
		return fmt.Errorf("authoritative observation %q is incomplete", row.RequestID)
	}
	return ValidateRegimes(row.Regimes)
}

func validSplit(split string) bool {
	switch strings.ToLower(strings.TrimSpace(split)) {
	case SplitTrain, SplitDevelopment, SplitCalibration, SplitMonitoring, SplitFrozenTest:
		return true
	default:
		return false
	}
}
