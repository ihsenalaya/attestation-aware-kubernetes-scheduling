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

package routingscore

import (
	"testing"

	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
	"github.com/imperium/ai-sovereign-finops-operator/internal/costengine"
)

func TestComputeUsesMeasuredLatencyWhenAvailable(t *testing.T) {
	scores := Compute([]collectors.UsageSample{
		{Namespace: "finance", Application: "risk", Provider: "cohere", Model: "cohere-command-a-latest", Requests: 2, InputTokens: 100, OutputTokens: 200, LatencyMillis: 800},
		{Namespace: "marketing", Application: "writer", Provider: "mistral", Model: "mistral-large-latest", Requests: 2, InputTokens: 100, OutputTokens: 200, LatencyMillis: 1200},
	}, costengine.PriceBook{
		"cohere-command-a-latest": {InputPerMillion: 2, OutputPerMillion: 8},
		"mistral-large-latest":    {InputPerMillion: 2, OutputPerMillion: 6},
	}, map[string]ModelInfo{
		"cohere-command-a-latest": {QualityTier: "high"},
		"mistral-large-latest":    {QualityTier: "high"},
	}, DefaultWeights())

	if len(scores) != 2 {
		t.Fatalf("got %d scores, want 2", len(scores))
	}
	var fast, slow Score
	for _, s := range scores {
		if !s.LatencyTelemetryAvailable {
			t.Fatalf("%s latency marked unavailable despite measured latency", s.Model)
		}
		if s.Model == "cohere-command-a-latest" {
			fast = s
		} else {
			slow = s
		}
	}
	if fast.ObservedLatencyMillis != 800 || slow.ObservedLatencyMillis != 1200 {
		t.Fatalf("observed latency mismatch: fast=%.0f slow=%.0f", fast.ObservedLatencyMillis, slow.ObservedLatencyMillis)
	}
	if fast.LatencyScore <= slow.LatencyScore {
		t.Fatalf("faster model should have higher latency score: fast=%f slow=%f", fast.LatencyScore, slow.LatencyScore)
	}
	if fast.Score <= 0 || slow.Score <= 0 {
		t.Fatalf("scores must always be positive numeric values: %+v", scores)
	}
}

func TestComputeAlwaysScoresWhenLatencyMissing(t *testing.T) {
	scores := Compute([]collectors.UsageSample{
		{Namespace: "finance", Application: "risk", Provider: "cohere", Model: "cohere-command-a-latest", Requests: 2, InputTokens: 100, OutputTokens: 200},
	}, costengine.PriceBook{
		"cohere-command-a-latest": {InputPerMillion: 2, OutputPerMillion: 8},
	}, map[string]ModelInfo{
		"cohere-command-a-latest": {QualityTier: "medium"},
	}, DefaultWeights())

	if len(scores) != 1 {
		t.Fatalf("got %d scores, want 1", len(scores))
	}
	s := scores[0]
	if s.LatencyTelemetryAvailable {
		t.Fatal("latency must be unavailable when no measured latency is present")
	}
	if s.LatencySource != SourceUnavailable {
		t.Fatalf("latency source = %q, want %q", s.LatencySource, SourceUnavailable)
	}
	if s.LatencyScore != NeutralLatencyScore {
		t.Fatalf("latency score = %f, want neutral %f", s.LatencyScore, NeutralLatencyScore)
	}
	if s.Score <= 0 {
		t.Fatalf("total score must still be present, got %f", s.Score)
	}
}
