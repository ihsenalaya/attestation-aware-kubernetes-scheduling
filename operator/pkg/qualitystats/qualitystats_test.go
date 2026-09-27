package qualitystats

import (
	"math"
	"testing"
)

func TestRequiredSampleSize(t *testing.T) {
	n := RequiredSampleSize(NonInferiorityConfig{
		Delta:               0.05,
		ConfidenceLevel:     0.95,
		Power:               0.80,
		BaselineSuccessRate: 0.90,
	})
	if n != 446 {
		t.Fatalf("required sample size = %d, want 446", n)
	}
}

func TestEvaluateNonInferiorityPass(t *testing.T) {
	got := EvaluateNonInferiority(DefaultNonInferiorityConfig(),
		BernoulliSample{Successes: 423, Total: 446},
		BernoulliSample{Successes: 414, Total: 446},
	)
	if got.Verdict != VerdictCandidateSafe {
		t.Fatalf("verdict = %s, lower=%.4f", got.Verdict, got.LowerConfidenceBound)
	}
	if got.LowerConfidenceBound < -0.05 {
		t.Fatalf("lower bound = %.4f, want >= -0.05", got.LowerConfidenceBound)
	}
}

func TestEvaluateNonInferiorityInsufficientData(t *testing.T) {
	got := EvaluateNonInferiority(DefaultNonInferiorityConfig(),
		BernoulliSample{Successes: 80, Total: 100},
		BernoulliSample{Successes: 79, Total: 100},
	)
	if got.Verdict != VerdictInsufficientData {
		t.Fatalf("verdict = %s, want insufficient-data", got.Verdict)
	}
}

func TestEvaluateComposite(t *testing.T) {
	got := EvaluateComposite(CompositeWeights{Quality: 0.5, ErrorRate: 0.2, LatencyP95: 0.2, Cost: 0.1}, CompositeInput{
		Quality:    0.92,
		ErrorRate:  0.95,
		LatencyP95: 0.80,
		Cost:       0.75,
	})
	if math.Abs(got.Score-0.885) > 1e-9 {
		t.Fatalf("score = %v, want 0.885", got.Score)
	}
}

func TestApplyHysteresis(t *testing.T) {
	if got := ApplyHysteresis(VerdictCandidateRisk, 0.79, HysteresisConfig{EnterSafe: 0.80, ExitSafe: 0.70}); got != VerdictCandidateRisk {
		t.Fatalf("verdict = %s, want risk", got)
	}
	if got := ApplyHysteresis(VerdictCandidateRisk, 0.81, HysteresisConfig{EnterSafe: 0.80, ExitSafe: 0.70}); got != VerdictCandidateSafe {
		t.Fatalf("verdict = %s, want safe", got)
	}
	if got := ApplyHysteresis(VerdictCandidateSafe, 0.71, HysteresisConfig{EnterSafe: 0.80, ExitSafe: 0.70}); got != VerdictCandidateSafe {
		t.Fatalf("verdict = %s, want safe", got)
	}
	if got := ApplyHysteresis(VerdictCandidateSafe, 0.69, HysteresisConfig{EnterSafe: 0.80, ExitSafe: 0.70}); got != VerdictCandidateRisk {
		t.Fatalf("verdict = %s, want risk", got)
	}
}

func TestAuxiliaryScores(t *testing.T) {
	if got := PercentileScore(100, 100, 1.25); got != 1 {
		t.Fatalf("percentile score = %v, want 1", got)
	}
	if got := ErrorRateScore(1, 100, 3, 100); got >= 1 || got <= 0 {
		t.Fatalf("error score = %v, want partial", got)
	}
	if got := CostScore(10, 20); got != 0.5 {
		t.Fatalf("cost score = %v, want 0.5", got)
	}
}
