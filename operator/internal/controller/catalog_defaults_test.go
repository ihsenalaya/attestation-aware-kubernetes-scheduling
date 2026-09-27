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

package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

// TestEmptyCatalogResolvesDefaults is the TASK #2 acceptance: with NO AIProvider /
// AIModel CRs at all, a well-known model still resolves a price and a sovereignty
// zone from the built-in default catalog — so the operator is useful at install.
func TestEmptyCatalogResolvesDefaults(t *testing.T) {
	cat := catalog{providers: map[string]aiopsv1alpha1.AIProvider{}, models: nil}

	pb := cat.priceBook()
	p, ok := pb["gpt-4o"]
	if !ok {
		t.Fatal("empty catalog should price gpt-4o from defaults")
	}
	if p.InputPerMillion <= 0 || p.OutputPerMillion <= 0 {
		t.Errorf("gpt-4o default price not populated: %+v", p)
	}

	if z := cat.zoneForModel("gpt-4o"); z != "US" {
		t.Errorf("zoneForModel(gpt-4o) = %q, want US (default)", z)
	}

	// An unknown model stays unpriced (no invented number) → the existing
	// data-quality recommendation path handles it.
	if _, ok := pb["totally-unknown-model"]; ok {
		t.Error("unknown model must not get a default price")
	}
}

// TestUserCROverridesDefault verifies a user AIModel/AIProvider takes precedence
// over the built-in default for the same model id.
func TestUserCROverridesDefault(t *testing.T) {
	prov := aiopsv1alpha1.AIProvider{}
	prov.Name = "my-openai"
	prov.Spec.Pricing.Currency = "EUR"
	prov.Spec.Pricing.InputTokenPricePerMillion = resource.MustParse("99")
	prov.Spec.Pricing.OutputTokenPricePerMillion = resource.MustParse("123")

	model := aiopsv1alpha1.AIModel{}
	model.Spec.ProviderRef = "my-openai"
	model.Spec.ModelName = "gpt-4o"

	cat := catalog{
		providers: map[string]aiopsv1alpha1.AIProvider{"my-openai": prov},
		models:    []aiopsv1alpha1.AIModel{model},
	}
	p := cat.priceBook()["gpt-4o"]
	if p.InputPerMillion != 99 || p.OutputPerMillion != 123 {
		t.Errorf("user CR did not override default: got %+v", p)
	}
}

// TestFlowsAutonomousZone proves sovereignty works WITHOUT any AIProvider CR: the
// flow's zone is derived from the model via the default catalog.
func TestFlowsAutonomousZone(t *testing.T) {
	cat := catalog{providers: map[string]aiopsv1alpha1.AIProvider{}, models: nil}

	// Provider name present in telemetry but no matching AIProvider CR → zone must
	// still resolve from the model default (gpt-4o → US).
	got := cat.flows([]collectors.UsageSample{
		{Namespace: "rh", Application: "chatbot", Provider: "openai-us", Model: "gpt-4o", Requests: 5},
	})
	if len(got) != 1 {
		t.Fatalf("expected 1 flow, got %d", len(got))
	}
	if got[0].Zone != "US" {
		t.Errorf("flow zone = %q, want US (from default catalog)", got[0].Zone)
	}
	if !got[0].Managed {
		t.Error("default-catalog model should be marked managed")
	}

	// Provider-less telemetry with a known model → still classified, provider filled.
	got = cat.flows([]collectors.UsageSample{
		{Namespace: "legal", Application: "rev", Model: "mistral-large-latest", Requests: 2},
	})
	if len(got) != 1 || got[0].Zone != "EU" {
		t.Fatalf("provider-less known model: got %+v, want one EU flow", got)
	}
	if got[0].Provider != "mistral" {
		t.Errorf("provider not filled from default: %q", got[0].Provider)
	}

	// Unknown model AND no provider → unplaceable → skipped (no spurious finding).
	got = cat.flows([]collectors.UsageSample{
		{Namespace: "x", Application: "y", Model: "mystery-model", Requests: 1},
	})
	if len(got) != 0 {
		t.Errorf("unplaceable flow should be skipped, got %+v", got)
	}
}
