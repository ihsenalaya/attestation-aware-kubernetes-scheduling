/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package qualityengine

import "testing"

func ptr(v float64) *float64 { return &v }

func TestDeterministicScores(t *testing.T) {
	if got := ExactMatchScore(" Hello   WORLD ", "hello world"); got != 100 {
		t.Fatalf("exact match = %v, want 100", got)
	}
	if got := JSONValidityScore(`{"risk":"low"}`); got != 100 {
		t.Fatalf("json validity = %v, want 100", got)
	}
	if got := JSONValidityScore(`{"risk":`); got != 0 {
		t.Fatalf("invalid json = %v, want 0", got)
	}
	if got := FieldF1Score(map[string]string{"risk": "low", "zone": "EU"}, map[string]string{"risk": "low", "zone": "US"}); got != 50 {
		t.Fatalf("field f1 = %v, want 50", got)
	}
	if got := EditDistance("kitten", "sitting"); got != 3 {
		t.Fatalf("edit distance = %v, want 3", got)
	}
	if got := RougeLScore("counterparty liquidity risk", "liquidity risk"); got <= 0 || got >= 100 {
		t.Fatalf("rouge-l = %v, want partial score", got)
	}
	if got := TokenF1Score("counterparty liquidity risk", "The answer explains liquidity risk for a counterparty exposure."); got <= 50 {
		t.Fatalf("token f1 = %v, want a useful partial score", got)
	}
	if got := ContentTokenCoverageScore("counterparty liquidity risk", "The answer explains liquidity risk for a counterparty exposure."); got != 100 {
		t.Fatalf("content coverage = %v, want 100", got)
	}
}

func TestReferenceScoringDoesNotRequireExactMatch(t *testing.T) {
	actual := "Counterparty liquidity risk is high when a supplier cannot fund operations or meet obligations."
	correctness := ReferenceCorrectnessScore("counterparty liquidity risk", actual)
	if correctness < 70 {
		t.Fatalf("reference correctness = %v, want plausible non-zero score", correctness)
	}
	semantic := SemanticSimilarityScore("counterparty liquidity risk", actual)
	if semantic < 75 {
		t.Fatalf("semantic similarity = %v, want plausible non-zero score", semantic)
	}
}

func TestLatencyScoreShape(t *testing.T) {
	cases := []struct {
		name      string
		latency   float64
		threshold float64
		want      float64
	}{
		{name: "half threshold", latency: 500, threshold: 1000, want: 100},
		{name: "at threshold", latency: 1000, threshold: 1000, want: 50},
		{name: "double threshold", latency: 2000, threshold: 1000, want: 0},
		{name: "between threshold and double", latency: 1500, threshold: 1000, want: 25},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := LatencyScore(Telemetry{LatencyMillis: tc.latency, LatencyObserved: true}, tc.threshold)
			if !ok {
				t.Fatal("latency score missing")
			}
			if got != tc.want {
				t.Fatalf("latency score = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvaluateCandidateSafe(t *testing.T) {
	source := []EvidenceSample{{
		ID:            "risk",
		Expected:      "counterparty liquidity risk",
		Actual:        "counterparty liquidity risk",
		SemanticScore: ptr(96),
	}}
	candidate := []EvidenceSample{{
		ID:            "risk",
		Expected:      "counterparty liquidity risk",
		Actual:        "counterparty liquidity risk",
		SemanticScore: ptr(94),
	}}
	got := Evaluate(EvaluateInput{
		SourceSamples:      source,
		CandidateSamples:   candidate,
		SourceTelemetry:    Telemetry{Requests: 10, LatencyMillis: 1000, LatencyObserved: true},
		CandidateTelemetry: Telemetry{Requests: 10, LatencyMillis: 900, LatencyObserved: true},
		LatencyThresholdMs: 1000,
		Weights:            Weights{Correctness: 0.5, Reliability: 0.25, Latency: 0.15, Semantic: 0.10},
		TolerancePoints:    3,
	})
	if got.Verdict != VerdictCandidateSafe {
		t.Fatalf("verdict = %s, reason=%s", got.Verdict, got.Reason)
	}
	if got.Candidate.Overall <= 0 || got.Candidate.Overall > 100 {
		t.Fatalf("candidate overall out of bounds: %v", got.Candidate.Overall)
	}
}

func TestEvaluateCandidateRiskRegression(t *testing.T) {
	source := []EvidenceSample{{
		ID:            "risk",
		Expected:      "counterparty liquidity risk",
		Actual:        "counterparty liquidity risk",
		SemanticScore: ptr(100),
	}}
	candidate := []EvidenceSample{{
		ID:            "risk",
		Expected:      "counterparty liquidity risk",
		Actual:        "unrelated answer",
		SemanticScore: ptr(20),
	}}
	got := Evaluate(EvaluateInput{
		SourceSamples:      source,
		CandidateSamples:   candidate,
		SourceTelemetry:    Telemetry{Requests: 10, LatencyMillis: 500, LatencyObserved: true},
		CandidateTelemetry: Telemetry{Requests: 10, Errors: 2, LatencyMillis: 2000, LatencyObserved: true},
		LatencyThresholdMs: 1000,
		Weights:            Weights{Correctness: 0.5, Reliability: 0.25, Latency: 0.15, Semantic: 0.10},
		TolerancePoints:    3,
	})
	if got.Verdict != VerdictCandidateRisk {
		t.Fatalf("verdict = %s, want risk", got.Verdict)
	}
}

func TestEvaluateInsufficientData(t *testing.T) {
	got := Evaluate(EvaluateInput{
		CandidateSamples:   []EvidenceSample{{ID: "only-candidate", Expected: "a", Actual: "a", SemanticScore: ptr(100)}},
		CandidateTelemetry: Telemetry{Requests: 1, LatencyMillis: 100, LatencyObserved: true},
		LatencyThresholdMs: 1000,
		MinSamples:         2,
		Weights:            Weights{Correctness: 1},
	})
	if got.Verdict != VerdictInsufficientData {
		t.Fatalf("verdict = %s, want insufficient-data", got.Verdict)
	}
	if len(got.MissingInput) == 0 {
		t.Fatal("missing input should explain insufficient data")
	}
}

func TestWeightsAreNormalizedAndBounded(t *testing.T) {
	w := NormalizeWeights(Weights{Correctness: 2, Reliability: 1, Latency: -1})
	sum := w.Correctness + w.Reliability + w.Latency + w.Semantic + w.Judged
	if sum < 0.999 || sum > 1.001 {
		t.Fatalf("normalized sum = %v, want 1", sum)
	}
	got := EvaluateModel([]EvidenceSample{{Expected: "a", Actual: "a"}}, Telemetry{}, Weights{Correctness: 1}, 0)
	if got.Overall != 100 {
		t.Fatalf("overall = %v, want 100", got.Overall)
	}
}
