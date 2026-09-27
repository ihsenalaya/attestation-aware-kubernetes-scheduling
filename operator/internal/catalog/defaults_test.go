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

package catalog

import (
	"math"
	"testing"
)

func TestEndpointToZone(t *testing.T) {
	cases := []struct {
		endpoint     string
		wantZone     string
		wantProvider string
	}{
		// Direct provider APIs — fixed zone.
		{"https://api.openai.com/v1/chat/completions", "US", "openai"},
		{"api.openai.com", "US", "openai"},
		{"https://api.anthropic.com", "US", "anthropic"},
		{"https://api.mistral.ai/v1/chat/completions", "EU", "mistral"},
		{"api.cohere.com", "US", "cohere"},
		{"https://api.groq.com/openai/v1", "US", "groq"},
		{"generativelanguage.googleapis.com", "US", "vertex"},
		// Azure — provider known, zone unknown from host alone (needs Region).
		{"https://greenops-foundry.services.ai.azure.com", "", "azure-openai"},
		{"myres.openai.azure.com", "", "azure-openai"},
		// Azure with a region label in the host → zone derived.
		{"myres.francecentral.cognitiveservices.azure.com", "FR", "azure-openai"},
		{"x.eastus.openai.azure.com", "US", "azure-openai"},
		// Bedrock — zone from region token.
		{"https://bedrock-runtime.us-east-1.amazonaws.com", "US", "bedrock"},
		{"bedrock-runtime.eu-west-1.amazonaws.com", "EU", "bedrock"},
		// Vertex — region prefix label.
		{"https://us-central1-aiplatform.googleapis.com", "US", "vertex"},
		{"europe-west4-aiplatform.googleapis.com", "EU", "vertex"},
		// Unknown host → no guess.
		{"https://example.com/v1", "", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		zone, provider := EndpointToZone(c.endpoint)
		if zone != c.wantZone || provider != c.wantProvider {
			t.Errorf("EndpointToZone(%q) = (%q,%q), want (%q,%q)",
				c.endpoint, zone, provider, c.wantZone, c.wantProvider)
		}
	}
}

func TestZoneForModel(t *testing.T) {
	cases := map[string]string{
		"gpt-4o":                     "US",
		"gpt-4o-mini":                "US",
		"mistral-large-latest":       "EU",
		"claude-3-5-sonnet":          "US",
		"claude-3-5-sonnet-20241022": "US", // dated suffix normalized
		"unknown-model-xyz":          "",
	}
	for model, want := range cases {
		if got := ZoneForModel(model); got != want {
			t.Errorf("ZoneForModel(%q) = %q, want %q", model, got, want)
		}
	}
}

func TestPriceForModel(t *testing.T) {
	p, ok := PriceForModel("gpt-4o")
	if !ok {
		t.Fatal("expected gpt-4o to be priced by defaults")
	}
	if p.Currency != "EUR" {
		t.Errorf("currency = %q, want EUR", p.Currency)
	}
	// 2.50 USD * 0.92 = 2.30 EUR per million input.
	if math.Abs(p.InputPerMillion-2.30) > 1e-9 {
		t.Errorf("gpt-4o input EUR = %v, want 2.30", p.InputPerMillion)
	}
	if math.Abs(p.OutputPerMillion-9.20) > 1e-9 {
		t.Errorf("gpt-4o output EUR = %v, want 9.20", p.OutputPerMillion)
	}
	// Case-insensitive + dated suffix.
	if _, ok := PriceForModel("Mistral-Large-2407"); !ok {
		t.Error("expected Mistral-Large-2407 to normalize to a known model")
	}
	if _, ok := PriceForModel("totally-made-up"); ok {
		t.Error("unknown model must not be priced (no invented numbers)")
	}
}

func TestKnown(t *testing.T) {
	if !Known("gpt-4o-mini") {
		t.Error("gpt-4o-mini should be known")
	}
	if Known("nope") {
		t.Error("nope should be unknown")
	}
}

func TestDefaultPriceBook(t *testing.T) {
	pb := DefaultPriceBook()
	if len(pb) == 0 {
		t.Fatal("default price book is empty")
	}
	if _, ok := pb["gpt-4o"]; !ok {
		t.Error("default price book missing gpt-4o")
	}
	// Every entry must be priced in EUR (the conversion target).
	for id, p := range pb {
		if p.Currency != "EUR" {
			t.Errorf("%s priced in %q, want EUR", id, p.Currency)
		}
	}
}
