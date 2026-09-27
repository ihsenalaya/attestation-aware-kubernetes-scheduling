package qualitystats

import "math"

const (
	VerdictCandidateSafe    = "candidate-safe"
	VerdictCandidateRisk    = "candidate-risk"
	VerdictInsufficientData = "insufficient-data"
)

type BernoulliSample struct {
	Successes int
	Total     int
}

type NonInferiorityConfig struct {
	Delta               float64
	ConfidenceLevel     float64
	Power               float64
	BaselineSuccessRate float64
}

type NonInferiorityResult struct {
	Verdict                string
	RequiredSamplesPerArm  int
	ObservedSourceSamples  int
	ObservedCandidateSlots int
	SourceRate             float64
	CandidateRate          float64
	Difference             float64
	LowerConfidenceBound   float64
	Delta                  float64
}

type CompositeWeights struct {
	Quality    float64
	ErrorRate  float64
	LatencyP95 float64
	Cost       float64
}

type CompositeInput struct {
	Quality    float64
	ErrorRate  float64
	LatencyP95 float64
	Cost       float64
}

type CompositeResult struct {
	Score      float64
	Normalized CompositeWeights
	Components CompositeInput
}

type HysteresisConfig struct {
	EnterSafe float64
	ExitSafe  float64
}

func DefaultNonInferiorityConfig() NonInferiorityConfig {
	return NonInferiorityConfig{
		Delta:               0.05,
		ConfidenceLevel:     0.95,
		Power:               0.80,
		BaselineSuccessRate: 0.90,
	}
}

func DefaultCompositeWeights() CompositeWeights {
	return CompositeWeights{Quality: 0.40, ErrorRate: 0.20, LatencyP95: 0.20, Cost: 0.20}
}

func DefaultHysteresisConfig() HysteresisConfig {
	return HysteresisConfig{EnterSafe: 0.80, ExitSafe: 0.70}
}

func EvaluateNonInferiority(cfg NonInferiorityConfig, source, candidate BernoulliSample) NonInferiorityResult {
	cfg = normalizeNonInferiorityConfig(cfg)
	required := RequiredSampleSize(cfg)
	result := NonInferiorityResult{
		RequiredSamplesPerArm:  required,
		ObservedSourceSamples:  source.Total,
		ObservedCandidateSlots: candidate.Total,
		Delta:                  cfg.Delta,
	}
	if source.Total > 0 {
		result.SourceRate = float64(source.Successes) / float64(source.Total)
	}
	if candidate.Total > 0 {
		result.CandidateRate = float64(candidate.Successes) / float64(candidate.Total)
	}
	result.Difference = result.CandidateRate - result.SourceRate
	if source.Total < required || candidate.Total < required || source.Total == 0 || candidate.Total == 0 {
		result.Verdict = VerdictInsufficientData
		return result
	}
	se := math.Sqrt(variance(result.SourceRate, source.Total) + variance(result.CandidateRate, candidate.Total))
	z := zForConfidence(cfg.ConfidenceLevel)
	result.LowerConfidenceBound = result.Difference - z*se
	if result.LowerConfidenceBound >= -cfg.Delta {
		result.Verdict = VerdictCandidateSafe
	} else {
		result.Verdict = VerdictCandidateRisk
	}
	return result
}

func RequiredSampleSize(cfg NonInferiorityConfig) int {
	cfg = normalizeNonInferiorityConfig(cfg)
	p := clamp01(cfg.BaselineSuccessRate)
	zAlpha := zForConfidence(cfg.ConfidenceLevel)
	zBeta := zForPower(cfg.Power)
	n := 2 * (zAlpha + zBeta) * (zAlpha + zBeta) * p * (1 - p) / (cfg.Delta * cfg.Delta)
	return int(math.Ceil(n))
}

func NormalizeWeights(w CompositeWeights) CompositeWeights {
	w = CompositeWeights{
		Quality:    math.Max(0, w.Quality),
		ErrorRate:  math.Max(0, w.ErrorRate),
		LatencyP95: math.Max(0, w.LatencyP95),
		Cost:       math.Max(0, w.Cost),
	}
	sum := w.Quality + w.ErrorRate + w.LatencyP95 + w.Cost
	if sum == 0 {
		return DefaultCompositeWeights()
	}
	return CompositeWeights{
		Quality:    w.Quality / sum,
		ErrorRate:  w.ErrorRate / sum,
		LatencyP95: w.LatencyP95 / sum,
		Cost:       w.Cost / sum,
	}
}

func EvaluateComposite(weights CompositeWeights, in CompositeInput) CompositeResult {
	weights = NormalizeWeights(weights)
	in = CompositeInput{
		Quality:    clamp01(in.Quality),
		ErrorRate:  clamp01(in.ErrorRate),
		LatencyP95: clamp01(in.LatencyP95),
		Cost:       clamp01(in.Cost),
	}
	score := weights.Quality*in.Quality + weights.ErrorRate*in.ErrorRate + weights.LatencyP95*in.LatencyP95 + weights.Cost*in.Cost
	return CompositeResult{Score: clamp01(score), Normalized: weights, Components: in}
}

func ApplyHysteresis(previousVerdict string, score float64, cfg HysteresisConfig) string {
	cfg = normalizeHysteresis(cfg)
	score = clamp01(score)
	wasSafe := previousVerdict == VerdictCandidateSafe
	switch {
	case wasSafe && score >= cfg.ExitSafe:
		return VerdictCandidateSafe
	case wasSafe && score < cfg.ExitSafe:
		return VerdictCandidateRisk
	case !wasSafe && score >= cfg.EnterSafe:
		return VerdictCandidateSafe
	default:
		return VerdictCandidateRisk
	}
}

func PercentileScore(source, candidate, maxRegressionRatio float64) float64 {
	if source <= 0 || candidate <= 0 {
		return 0
	}
	if maxRegressionRatio <= 1 {
		maxRegressionRatio = 1.25
	}
	ratio := candidate / source
	if ratio <= 1 {
		return 1
	}
	if ratio >= maxRegressionRatio {
		return 0
	}
	return 1 - ((ratio - 1) / (maxRegressionRatio - 1))
}

func ErrorRateScore(sourceErrors, sourceRequests, candidateErrors, candidateRequests int) float64 {
	if sourceRequests <= 0 || candidateRequests <= 0 {
		return 0
	}
	sourceRate := float64(sourceErrors) / float64(sourceRequests)
	candidateRate := float64(candidateErrors) / float64(candidateRequests)
	if candidateRate <= sourceRate {
		return 1
	}
	if candidateRate >= 1 {
		return 0
	}
	return clamp01(1 - candidateRate)
}

func CostScore(sourceCost, candidateCost float64) float64 {
	if sourceCost <= 0 || candidateCost <= 0 {
		return 0
	}
	if candidateCost <= sourceCost {
		return 1
	}
	return clamp01(sourceCost / candidateCost)
}

func normalizeNonInferiorityConfig(cfg NonInferiorityConfig) NonInferiorityConfig {
	if cfg.Delta <= 0 {
		cfg.Delta = DefaultNonInferiorityConfig().Delta
	}
	if cfg.ConfidenceLevel <= 0 {
		cfg.ConfidenceLevel = DefaultNonInferiorityConfig().ConfidenceLevel
	}
	if cfg.Power <= 0 {
		cfg.Power = DefaultNonInferiorityConfig().Power
	}
	if cfg.BaselineSuccessRate <= 0 {
		cfg.BaselineSuccessRate = DefaultNonInferiorityConfig().BaselineSuccessRate
	}
	return cfg
}

func normalizeHysteresis(cfg HysteresisConfig) HysteresisConfig {
	if cfg.EnterSafe <= 0 {
		cfg.EnterSafe = DefaultHysteresisConfig().EnterSafe
	}
	if cfg.ExitSafe <= 0 {
		cfg.ExitSafe = DefaultHysteresisConfig().ExitSafe
	}
	if cfg.ExitSafe > cfg.EnterSafe {
		cfg.ExitSafe = cfg.EnterSafe
	}
	return cfg
}

func variance(p float64, n int) float64 {
	if n <= 0 {
		return 0
	}
	return p * (1 - p) / float64(n)
}

func zForConfidence(confidence float64) float64 {
	switch {
	case confidence >= 0.995:
		return 2.576
	case confidence >= 0.99:
		return 2.326
	case confidence >= 0.95:
		return 1.645
	case confidence >= 0.90:
		return 1.282
	default:
		return 1.645
	}
}

func zForPower(power float64) float64 {
	switch {
	case power >= 0.90:
		return 1.282
	case power >= 0.80:
		return 0.842
	case power >= 0.70:
		return 0.524
	default:
		return 0.842
	}
}

func clamp01(v float64) float64 {
	return math.Max(0, math.Min(1, v))
}
