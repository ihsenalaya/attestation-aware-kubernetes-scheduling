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

package costengine

import (
	"math"
	"testing"

	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestComputeBasic(t *testing.T) {
	samples := []collectors.UsageSample{
		{Namespace: "rh", Team: "rh", Application: "chatbot", Provider: "az", Model: "gpt-4o",
			Requests: 100, InputTokens: 2_000_000, OutputTokens: 1_000_000},
		{Namespace: "rh", Team: "rh", Application: "chatbot", Provider: "az", Model: "mistral-small",
			Requests: 400, InputTokens: 1_000_000, OutputTokens: 500_000},
	}
	prices := PriceBook{
		"gpt-4o":        {Currency: "EUR", InputPerMillion: 5, OutputPerMillion: 15},
		"mistral-small": {Currency: "EUR", InputPerMillion: 2.5, OutputPerMillion: 10},
	}

	b := Compute(samples, prices)

	// gpt-4o: 2*5 + 1*15 = 25 ; mistral: 1*2.5 + 0.5*10 = 7.5 ; total = 32.5
	if !approx(b.Total.CostTotal, 32.5) {
		t.Fatalf("total cost = %v, want 32.5", b.Total.CostTotal)
	}
	if !approx(b.ByModel["gpt-4o"].CostTotal, 25) {
		t.Errorf("gpt-4o cost = %v, want 25", b.ByModel["gpt-4o"].CostTotal)
	}
	if b.Currency != "EUR" {
		t.Errorf("currency = %q, want EUR", b.Currency)
	}
	if b.Total.Requests != 500 {
		t.Errorf("requests = %d, want 500", b.Total.Requests)
	}
	// avg per request = 32.5 / 500 = 0.065
	if !approx(b.AvgCostPerRequest(), 0.065) {
		t.Errorf("avg/req = %v, want 0.065", b.AvgCostPerRequest())
	}
	// per token = 32.5 / 4_500_000
	if !approx(b.CostPerToken(), 32.5/4_500_000) {
		t.Errorf("cost/token = %v", b.CostPerToken())
	}
	if len(b.UnpricedModels) != 0 {
		t.Errorf("unpriced = %v, want none", b.UnpricedModels)
	}
}

func TestComputeUnpricedModel(t *testing.T) {
	samples := []collectors.UsageSample{
		{Model: "mystery", Requests: 10, InputTokens: 1_000_000, OutputTokens: 0},
	}
	b := Compute(samples, PriceBook{})
	if b.Total.CostTotal != 0 {
		t.Errorf("cost = %v, want 0 for unpriced", b.Total.CostTotal)
	}
	if len(b.UnpricedModels) != 1 || b.UnpricedModels[0] != "mystery" {
		t.Errorf("unpriced = %v, want [mystery]", b.UnpricedModels)
	}
}

func TestTopByCost(t *testing.T) {
	m := map[string]LineItem{
		"a": {Key: "a", CostTotal: 10},
		"b": {Key: "b", CostTotal: 30},
		"c": {Key: "c", CostTotal: 20},
	}
	top := TopByCost(m, 2)
	if len(top) != 2 || top[0].Key != "b" || top[1].Key != "c" {
		t.Fatalf("top = %+v, want [b,c]", top)
	}
}
