package govar

import (
	"errors"
	"sort"
	"strings"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
)

const (
	CalibrationEvidenceSourcePostgreSQLV8 = "postgresql-v8"
	// CalibrationEvidenceSourceInMemoryQualificationV1 is accepted only by an
	// Engine explicitly placed in non-final qualification mode. PostgreSQLEngine
	// never exposes that mode and remains bound to the PostgreSQL evidence source.
	CalibrationEvidenceSourceInMemoryQualificationV1 = "in_memory_qualification_nonfinal_v1"

	// Deprecated compatibility keys. They may be present on legacy objects but
	// are never read as monetary, calibration, or risk authority.
	AnnotationReservationMethod  = "aiops.imperium.io/govar-reservation-method"
	AnnotationMeanOutputTokens   = "aiops.imperium.io/govar-mean-output-tokens"
	AnnotationMarginTokens       = "aiops.imperium.io/govar-margin-tokens"
	AnnotationQuantileTokens     = "aiops.imperium.io/govar-quantile-output-tokens"
	AnnotationAdaptiveTokens     = "aiops.imperium.io/govar-adaptive-output-tokens"
	AnnotationCalibrationSupport = "aiops.imperium.io/govar-calibration-support"
	AnnotationCalibrationDrift   = "aiops.imperium.io/govar-calibration-drift"
	AnnotationCohortSize         = "aiops.imperium.io/govar-cohort-size"
	AnnotationTenantRiskPPB      = "aiops.imperium.io/govar-tenant-risk-ppb"
)

type admissionChoice struct {
	Candidate        Candidate
	Reservation      MoneyMicros
	Method           string
	AllocatedRiskPPB int64
	InputTokensBound int64
	Components       []govarpricing.ChargeComponent
	FallbackReason   ReasonCode
}

func validatePolicyAndTarget(req AdmitRequest, budget aiopsv1alpha1.AIBudgetPolicy, routing aiopsv1alpha1.AIRoutingPolicy, evaluatedAt time.Time) ReasonCode {
	if budget.Status.ObservedGeneration != budget.Generation || !readyConditionTrue(budget.Status.Conditions) ||
		routing.Status.ObservedGeneration != routing.Generation || !readyConditionTrue(routing.Status.Conditions) {
		return ReasonPolicyNotReady
	}
	if routing.Status.LastEvaluatedAt == nil || routing.Status.LastEvaluatedAt.After(evaluatedAt.UTC()) || evaluatedAt.UTC().Sub(routing.Status.LastEvaluatedAt.Time) > observationFreshnessLimit {
		return ReasonPolicyNotReady
	}
	target := budget.Spec.Target
	if target.Namespace != "" && target.Namespace != req.Namespace {
		return ReasonBudgetTargetMismatch
	}
	if target.Team != "" && target.Team != req.Team {
		return ReasonBudgetTargetMismatch
	}
	if target.Application != "" && target.Application != req.Application {
		return ReasonBudgetTargetMismatch
	}
	return ""
}

func chooseAdmission(req AdmitRequest, routing aiopsv1alpha1.AIRoutingPolicy, candidates []Candidate, available MoneyMicros, evaluatedAt time.Time) (admissionChoice, ReasonCode, error) {
	return chooseAdmissionWithEvidenceSource(req, routing, candidates, available, evaluatedAt, CalibrationEvidenceSourcePostgreSQLV8)
}

func chooseAdmissionWithEvidenceSource(req AdmitRequest, routing aiopsv1alpha1.AIRoutingPolicy, candidates []Candidate, available MoneyMicros, evaluatedAt time.Time, evidenceSource string) (admissionChoice, ReasonCode, error) {
	var choices []admissionChoice
	lastReason := ReasonNoCandidate
	feasibleBeforeBudget := false
	for _, candidate := range candidates {
		if !candidate.Feasible {
			if candidate.InfeasibleReason != "" {
				lastReason = candidate.InfeasibleReason
			}
			continue
		}
		if candidate.QualityScore < routing.Spec.Guardrails.MinQualityScore {
			lastReason = ReasonQualityBelowMinimum
			continue
		}
		if (routing.Spec.Guardrails.MaxLatencyMillis > 0 || strings.EqualFold(routing.Spec.Objective, "latency")) && candidate.LatencyObservedAt.IsZero() {
			lastReason = ReasonLatencyUnavailable
			continue
		}
		if routing.Spec.Guardrails.MaxLatencyMillis > 0 && candidate.LatencyMillis > int64(routing.Spec.Guardrails.MaxLatencyMillis) {
			lastReason = ReasonLatencyExceeded
			continue
		}
		if candidate.ContextWindow <= 0 || (req.InputTokensExact && req.InputTokens+req.MaxOutputTokens > candidate.ContextWindow) || (!req.InputTokensExact && req.MaxOutputTokens >= candidate.ContextWindow) {
			lastReason = ReasonContextLimit
			continue
		}
		outputTokens, allocatedRisk, effectiveMethod, fallbackReason, reason := reservationTokens(req, routing, candidate, evaluatedAt, evidenceSource)
		if reason != "" {
			lastReason = reason
			continue
		}
		inputBound := req.InputTokens
		if strings.HasPrefix(effectiveMethod, "strict_") && !req.InputTokensExact {
			inputBound = candidate.ContextWindow - outputTokens
			effectiveMethod = "strict_context_window_bound"
		}
		components, err := govarpricing.ReserveComponents(candidate.PricingSnapshot, govarpricing.RequestChargeContext{
			InputTokens: inputBound, OutputTokens: outputTokens, MaxToolCalls: req.MaxToolCalls,
			MaxMediaUnits: req.MaxMediaUnits, TimeoutSeconds: req.TimeoutSeconds,
			MaxRetryAttempts: req.MaxRetryAttempts, CancellationPossible: req.CancellationPossible,
			DeclaredBounds: req.ChargeBounds,
		}, evaluatedAt)
		if err != nil {
			return admissionChoice{}, ReasonPricingIncomplete, err
		}
		reserved, err := govarpricing.SumComponents(components)
		if err != nil {
			return admissionChoice{}, ReasonPricingIncomplete, err
		}
		reservation := MoneyMicros(reserved)
		feasibleBeforeBudget = true
		if reservation > available {
			continue
		}
		choices = append(choices, admissionChoice{Candidate: candidate, Reservation: reservation, Method: effectiveMethod, AllocatedRiskPPB: allocatedRisk, InputTokensBound: inputBound, Components: components, FallbackReason: fallbackReason})
	}
	if len(choices) == 0 {
		if feasibleBeforeBudget {
			return admissionChoice{}, ReasonBudgetUnavailable, nil
		}
		return admissionChoice{}, lastReason, nil
	}
	sort.Slice(choices, func(i, j int) bool {
		if strings.EqualFold(routing.Spec.Objective, "quality") && choices[i].Candidate.QualityScore != choices[j].Candidate.QualityScore {
			return choices[i].Candidate.QualityScore > choices[j].Candidate.QualityScore
		}
		if strings.EqualFold(routing.Spec.Objective, "latency") && choices[i].Candidate.LatencyMillis != choices[j].Candidate.LatencyMillis {
			return choices[i].Candidate.LatencyMillis < choices[j].Candidate.LatencyMillis
		}
		if choices[i].Reservation != choices[j].Reservation {
			return choices[i].Reservation < choices[j].Reservation
		}
		return choices[i].Candidate.ModelRef < choices[j].Candidate.ModelRef
	})
	return choices[0], "", nil
}

func reservationTokens(req AdmitRequest, routing aiopsv1alpha1.AIRoutingPolicy, candidate Candidate, evaluatedAt time.Time, evidenceSource string) (int64, int64, string, ReasonCode, ReasonCode) {
	method := typedReservationMethod(routing)
	capTokens := req.MaxOutputTokens
	if capTokens <= 0 {
		return 0, 0, method, "", ReasonInsufficientEvidence
	}
	strict := func(fallback ReasonCode) (int64, int64, string, ReasonCode, ReasonCode) {
		if !candidate.VerifiedOutputCap || capTokens > candidate.VerifiedOutputCapTokens {
			return 0, 0, "strict_provider_cap", fallback, ReasonStrictCapUnverified
		}
		return capTokens, 0, "strict_provider_cap", fallback, ""
	}
	if !req.InputTokensExact && method != "strict_provider_cap" {
		return strict(ReasonInputBoundFallback)
	}
	clamp := func(v int64) int64 {
		if v > capTokens {
			return capTokens
		}
		return v
	}
	switch method {
	case "strict_provider_cap":
		return strict("")
	case "mean":
		if routing.Spec.GOVAR == nil || routing.Spec.GOVAR.Reservation.MeanOutputTokens == nil {
			return 0, 0, method, "", ReasonInsufficientCalibration
		}
		if *routing.Spec.GOVAR.Reservation.MeanOutputTokens < 0 {
			return 0, 0, method, "", ReasonInsufficientCalibration
		}
		return clamp(*routing.Spec.GOVAR.Reservation.MeanOutputTokens), 0, method, "", ""
	case "fixed_margin":
		if routing.Spec.GOVAR == nil || routing.Spec.GOVAR.Reservation.MeanOutputTokens == nil || routing.Spec.GOVAR.Reservation.MarginOutputTokens == nil {
			return 0, 0, method, "", ReasonInsufficientCalibration
		}
		mean, margin := *routing.Spec.GOVAR.Reservation.MeanOutputTokens, *routing.Spec.GOVAR.Reservation.MarginOutputTokens
		if mean < 0 || margin < 0 || mean > capTokens-margin {
			return 0, 0, method, "", ReasonInsufficientCalibration
		}
		return clamp(mean + margin), 0, method, "", ""
	case "fixed_quantile":
		if routing.Spec.GOVAR == nil || routing.Spec.GOVAR.Reservation.FixedQuantileOutputTokens == nil {
			return 0, 0, method, "", ReasonInsufficientCalibration
		}
		if *routing.Spec.GOVAR.Reservation.FixedQuantileOutputTokens < 0 {
			return 0, 0, method, "", ReasonInsufficientCalibration
		}
		return clamp(*routing.Spec.GOVAR.Reservation.FixedQuantileOutputTokens), 0, method, "", ""
	case "adaptive_quantile", "govar_fixed_cohort":
		invalid := validateCalibrationEvidence(routing, candidate, evaluatedAt, evidenceSource)
		if invalid != "" {
			if routing.Spec.GOVAR != nil && routing.Spec.GOVAR.Drift.Fallback == aiopsv1alpha1.GOVARConservativeFallback("strict_provider_cap") {
				return strict(invalid)
			}
			return 0, 0, method, invalid, invalid
		}
		adaptive := routing.Status.GOVAR.Calibration.AdaptiveOutputTokens
		if method == "adaptive_quantile" {
			return clamp(adaptive), 0, method, "", ""
		}
		if req.CohortID == "" || req.CohortIndex < 0 {
			return 0, 0, method, "", ReasonInsufficientCalibration
		}
		// N, alpha, and the slot weight come only from the immutable server-side
		// FrozenCohort registry and are applied by Engine/PostgresEngine.
		return clamp(adaptive), 0, method, "", ""
	default:
		return 0, 0, method, "", ReasonReservationMethodUnknown
	}
}

func typedReservationMethod(routing aiopsv1alpha1.AIRoutingPolicy) string {
	if routing.Spec.GOVAR == nil || strings.TrimSpace(string(routing.Spec.GOVAR.Reservation.Method)) == "" {
		return string(aiopsv1alpha1.GOVARReservationStrictProviderCap)
	}
	return string(routing.Spec.GOVAR.Reservation.Method)
}

func validateCalibrationEvidence(routing aiopsv1alpha1.AIRoutingPolicy, candidate Candidate, evaluatedAt time.Time, evidenceSource string) ReasonCode {
	if routing.Spec.GOVAR == nil || routing.Spec.GOVAR.Calibration == nil || routing.Status.GOVAR == nil || routing.Status.GOVAR.Calibration == nil || routing.Status.GOVAR.Drift == nil {
		return ReasonCalibrationMissing
	}
	spec, status, drift := routing.Spec.GOVAR.Calibration, routing.Status.GOVAR.Calibration, routing.Status.GOVAR.Drift
	if status.EvidenceSource != evidenceSource || !isSHA(status.PublicationSHA256) || status.CalibrationMethod != "split_conformal_order_statistic_v1" ||
		status.CoverageBoundKind != govarcalibration.ExactMarginalBoundKind ||
		!isSHA(status.FeatureRegimeSHA256) || !isSHA(status.SplitOpportunityRegimeSHA256) || !isSHA(status.CohortRegimeSHA256) {
		return ReasonCalibrationMismatch
	}
	if routing.Status.ObservedGeneration != routing.Generation {
		return ReasonCalibrationGeneration
	}
	if status.ArtifactRef != spec.ArtifactRef || status.Version != spec.Version || status.ArtifactSHA256 != spec.ArtifactSHA256 ||
		status.CalibrationInputSHA256 != spec.CalibrationInputSHA256 || status.FeatureSchemaVersion != spec.FeatureSchemaVersion ||
		status.PriceRegimeSHA256 != spec.PriceRegimeSHA256 || status.CapRegimeSHA256 != spec.CapRegimeSHA256 ||
		status.ProducerSoftwareSHA256 != spec.ProducerSoftwareSHA256 || status.CoverageTargetPPB != spec.CoverageTargetPPB {
		return ReasonCalibrationMismatch
	}
	priceRegime, err := govarcalibration.CanonicalPriceRegime(candidate.PricingSnapshot.SnapshotSHA256,
		candidate.RouteSnapshot.ProviderUID, candidate.RouteSnapshot.ProviderDeployment)
	capRegime, capErr := CanonicalCandidateCapPathAdapterRegime(candidate)
	if err != nil || capErr != nil || status.PriceRegimeSHA256 != priceRegime || status.CapRegimeSHA256 != capRegime {
		return ReasonCalibrationRegime
	}
	if !status.Valid || status.Support < spec.MinimumSupport || drift.Support < routing.Spec.GOVAR.Drift.RevalidationMinimumSupport {
		return ReasonCalibrationSupport
	}
	if status.CoverageBoundNumerator != status.ConformalRank || status.CoverageBoundDenominator != status.Support+1 ||
		status.CoverageIntervalLowerPPB < status.CoverageTargetPPB || status.CoverageIntervalUpperPPB > 1_000_000_000 ||
		status.CoverageConfidencePPB != 1_000_000_000 {
		return ReasonCalibrationMismatch
	}
	if status.ObservedAt.IsZero() || status.CalibrationWindowEnd.IsZero() || status.ObservedAt.After(evaluatedAt) || status.CalibrationWindowEnd.After(evaluatedAt) || evaluatedAt.Sub(status.CalibrationWindowEnd.Time) > time.Duration(spec.MaxAgeSeconds)*time.Second {
		return ReasonCalibrationStale
	}
	if drift.Detector != routing.Spec.GOVAR.Drift.Detector || drift.ThresholdPPB != routing.Spec.GOVAR.Drift.ThresholdPPB {
		return ReasonCalibrationMismatch
	}
	if drift.Detector != aiopsv1alpha1.GOVARDriftDetector("coverage-gap") {
		return ReasonCalibrationUnsupported
	}
	if drift.ObservedAt.IsZero() || drift.MonitoringWindowEnd.IsZero() || drift.ObservedAt.After(evaluatedAt) || drift.MonitoringWindowEnd.After(evaluatedAt) || evaluatedAt.Sub(drift.MonitoringWindowEnd.Time) > time.Duration(spec.MaxAgeSeconds)*time.Second {
		return ReasonCalibrationStale
	}
	if drift.Detected || drift.ConservativeMode {
		return ReasonCalibrationDrift
	}
	return ""
}

// CanonicalCandidateCapPathAdapterRegime binds the attested provider cap to
// the exact provider generation, deployment, gateway path, adapter version,
// request parameter, and enforced token limit used by admission. A bare cap
// evidence digest is not a complete calibration regime.
func CanonicalCandidateCapPathAdapterRegime(candidate Candidate) (string, error) {
	requestParameter, ok := govarpricing.OutputCapRequestParameter(candidate.ProviderType,
		aiopsv1alpha1.GOVARRoutePathMode(candidate.RouteSnapshot.PathMode))
	if !ok {
		return "", errors.New("candidate provider/path has no supported output-cap request parameter")
	}
	return govarcalibration.CanonicalCapPathAdapterRegime(candidate.CapEvidenceDigest,
		candidate.RouteSnapshot.ProviderUID, candidate.RouteSnapshot.ProviderGeneration,
		candidate.RouteSnapshot.ProviderDeployment, candidate.RouteSnapshot.PathMode,
		govarpricing.CurrentAdapterVersion, requestParameter, candidate.VerifiedOutputCapTokens)
}

func decisionForAdmissionFailure(routing aiopsv1alpha1.AIRoutingPolicy, reason ReasonCode) Decision {
	if reason == ReasonBudgetUnavailable {
		return DecisionQueue
	}
	if isCalibrationReason(reason) && routing.Spec.GOVAR != nil {
		switch routing.Spec.GOVAR.Drift.Fallback {
		case aiopsv1alpha1.GOVARConservativeFallback("queue"):
			return DecisionQueue
		case aiopsv1alpha1.GOVARConservativeFallback("reject"):
			return DecisionReject
		case aiopsv1alpha1.GOVARConservativeFallback("require_approval"):
			return DecisionRequireApproval
		default:
			return DecisionAbstain
		}
	}
	return DecisionAbstain
}

func isCalibrationReason(reason ReasonCode) bool {
	switch reason {
	case ReasonInsufficientCalibration, ReasonCalibrationMissing, ReasonCalibrationGeneration,
		ReasonCalibrationMismatch, ReasonCalibrationRegime, ReasonCalibrationSupport,
		ReasonCalibrationStale, ReasonCalibrationDrift, ReasonCalibrationUnsupported:
		return true
	default:
		return false
	}
}

var errBudgetWindowChanged = errors.New("budget policy or window changed while liabilities remain")
