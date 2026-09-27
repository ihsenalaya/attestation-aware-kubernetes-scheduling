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

package recommendationengine

import (
	"math"
	"testing"
)

func TestRecommend(t *testing.T) {
	// finance uses gpt-4o (expensive); gpt-4o-mini is a far cheaper candidate.
	usages := []Usage{
		{Namespace: "finance", Application: "risk-assistant", Model: "gpt-4o",
			InputTokens: 1_000_000, OutputTokens: 1_000_000, Requests: 100, CostEUR: 11.50}, // 2.30+9.20
	}
	candidates := []Candidate{
		{Name: "gpt-4o", InputPerMillion: 2.30, OutputPerMillion: 9.20, Compliant: true},
		{Name: "gpt-4o-mini", InputPerMillion: 0.14, OutputPerMillion: 0.55, Compliant: true},
	}
	risks := []Risk{
		{Namespace: "finance", Application: "risk-assistant", Provider: "openai-us", Zone: "US", Requests: 100, CostEUR: 11.50},
	}

	recs, total := Recommend(usages, candidates, risks, nil, false)

	var cost, sov bool
	for _, r := range recs {
		switch r.Type {
		case TypeCostSaving:
			cost = true
			// saving = 11.50 - (0.14+0.55) = 10.81
			if math.Abs(r.EstimatedSavingsEUR-10.81) > 0.01 {
				t.Errorf("saving = %.4f, want ~10.81", r.EstimatedSavingsEUR)
			}
		case TypeSovereignty:
			sov = true
			if r.Severity != SeverityCritical {
				t.Errorf("sovereignty severity = %s, want critical", r.Severity)
			}
		}
	}
	if !cost || !sov {
		t.Fatalf("expected cost-saving and sovereignty recs, got %+v", recs)
	}
	if math.Abs(total-10.81) > 0.01 {
		t.Errorf("totalSavings = %.4f, want ~10.81", total)
	}
	// Critical (sovereignty) must sort before info (cost-saving).
	if recs[0].Type != TypeSovereignty {
		t.Errorf("first rec = %s, want sovereignty (critical first)", recs[0].Type)
	}
}

func TestRecommendNoCheaperNoRisk(t *testing.T) {
	usages := []Usage{{Namespace: "rh", Application: "a", Model: "cheap", InputTokens: 1000, OutputTokens: 1000, CostEUR: 0.001}}
	candidates := []Candidate{{Name: "cheap", InputPerMillion: 0.1, OutputPerMillion: 0.1, Compliant: true}}
	recs, total := Recommend(usages, candidates, nil, nil, true)
	if len(recs) != 0 || total != 0 {
		t.Errorf("expected no recommendations, got %+v (total %.4f)", recs, total)
	}
}

// A cheaper-but-non-compliant model must NOT be recommended: the sovereign
// product can't tell a user to save money by routing into a forbidden zone.
func TestRecommendSkipsNonCompliantCheaperModel(t *testing.T) {
	// marketing runs on a compliant EU model (mistral-large). The only cheaper
	// candidate (gpt-4o-mini) is US/non-compliant — so no cost-saving swap exists.
	usages := []Usage{
		{Namespace: "marketing", Application: "content-writer", Model: "mistral-large",
			InputTokens: 1_000_000, OutputTokens: 1_000_000, Requests: 50, CostEUR: 8.00},
	}
	candidates := []Candidate{
		{Name: "mistral-large", InputPerMillion: 2.00, OutputPerMillion: 6.00, Compliant: true},
		{Name: "gpt-4o-mini", InputPerMillion: 0.14, OutputPerMillion: 0.55, Compliant: false}, // US, forbidden
	}
	recs, total := Recommend(usages, candidates, nil, nil, true)
	for _, r := range recs {
		if r.Type == TypeCostSaving {
			t.Fatalf("recommended a cost-saving swap to a non-compliant model: %+v", r)
		}
	}
	if total != 0 {
		t.Errorf("totalSavings = %.4f, want 0 (no compliant cheaper model)", total)
	}

	// Add a compliant cheaper model and the swap reappears (proves the filter, not
	// a blanket suppression).
	candidates = append(candidates, Candidate{Name: "mistral-small", InputPerMillion: 0.20, OutputPerMillion: 0.60, Compliant: true})
	recs, total = Recommend(usages, candidates, nil, nil, true)
	var swapped bool
	for _, r := range recs {
		if r.Type == TypeCostSaving {
			swapped = true
			if r.RecommendedModel != "mistral-small" {
				t.Errorf("recommended %q, want mistral-small (the compliant cheaper model)", r.RecommendedModel)
			}
		}
	}
	if !swapped || total <= 0 {
		t.Errorf("expected a compliant cost-saving swap, got recs=%+v total=%.4f", recs, total)
	}
}
