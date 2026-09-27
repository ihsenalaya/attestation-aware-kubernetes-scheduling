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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

func TestBudgetFallbackDesired(t *testing.T) {
	cat := testBudgetCatalog()
	spec := aiopsv1alpha1.AIBudgetPolicySpec{
		Target:           aiopsv1alpha1.BudgetTarget{Namespace: "finance", Application: "risk"},
		FallbackModelRef: "gpt-4o-mini",
		EnforcementMode:  aiopsv1alpha1.EnforcementEnforce,
		FallbackOnPhase:  aiopsv1alpha1.BudgetFallbackOnExceeded,
	}
	all := []collectors.UsageSample{
		{Namespace: "finance", Application: "risk", Model: "gpt-4o", Requests: 10, InputTokens: 1000, OutputTokens: 1000},
	}
	got := budgetFallbackDesired(cat, all, all, spec, "Exceeded", "aigw", false)
	if got.activeModel != "gpt-4o-mini" {
		t.Fatalf("activeModel = %q, want gpt-4o-mini", got.activeModel)
	}
	if got.desired["gpt-4o"] != "gpt-4o-mini" {
		t.Fatalf("desired = %+v, want gpt-4o -> gpt-4o-mini", got.desired)
	}
}

func TestBudgetFallbackSkipsSharedModel(t *testing.T) {
	cat := testBudgetCatalog()
	spec := aiopsv1alpha1.AIBudgetPolicySpec{
		Target:           aiopsv1alpha1.BudgetTarget{Namespace: "finance", Application: "risk"},
		FallbackModelRef: "gpt-4o-mini",
		EnforcementMode:  aiopsv1alpha1.EnforcementEnforce,
		FallbackOnPhase:  aiopsv1alpha1.BudgetFallbackOnExceeded,
	}
	all := []collectors.UsageSample{
		{Namespace: "finance", Application: "risk", Model: "gpt-4o", Requests: 10, InputTokens: 1000, OutputTokens: 1000},
		{Namespace: "finance", Application: "other", Model: "gpt-4o", Requests: 3, InputTokens: 500, OutputTokens: 500},
	}
	scoped := filterByBudgetTarget(all, spec.Target)
	got := budgetFallbackDesired(cat, all, scoped, spec, "Exceeded", "aigw", false)
	if len(got.desired) != 0 {
		t.Fatalf("desired = %+v, want no reroute for shared model", got.desired)
	}
	if !strings.Contains(got.reason, "no dedicated scoped model") {
		t.Fatalf("reason = %q, want shared-model skip explanation", got.reason)
	}
}

func TestBudgetFallbackLatencyGuardrailRequiresObservedFallbackLatency(t *testing.T) {
	cat := testBudgetCatalog()
	spec := aiopsv1alpha1.AIBudgetPolicySpec{
		Target:                   aiopsv1alpha1.BudgetTarget{Namespace: "finance", Application: "risk"},
		FallbackModelRef:         "gpt-4o-mini",
		EnforcementMode:          aiopsv1alpha1.EnforcementEnforce,
		FallbackOnPhase:          aiopsv1alpha1.BudgetFallbackOnCritical,
		MaxFallbackLatencyMillis: 700,
	}
	all := []collectors.UsageSample{
		{Namespace: "finance", Application: "risk", Model: "gpt-4o", Requests: 10, InputTokens: 1000, OutputTokens: 1000},
	}
	got := budgetFallbackDesired(cat, all, all, spec, "Critical", "aigw", false)
	if len(got.desired) != 0 {
		t.Fatalf("desired = %+v, want latency guardrail to block actuation", got.desired)
	}
	if !strings.Contains(got.reason, "no observed latency yet") {
		t.Fatalf("reason = %q, want missing observed latency explanation", got.reason)
	}
}

func testBudgetCatalog() catalog {
	return catalog{
		providers: map[string]aiopsv1alpha1.AIProvider{
			"openai-premium": {
				ObjectMeta: metav1.ObjectMeta{Name: "openai-premium"},
				Spec: aiopsv1alpha1.AIProviderSpec{
					Type: "openai", Managed: true,
					Pricing: aiopsv1alpha1.ProviderPricing{
						Currency:                   "EUR",
						InputTokenPricePerMillion:  resource.MustParse("5.00"),
						OutputTokenPricePerMillion: resource.MustParse("15.00"),
					},
				},
			},
			"openai-economy": {
				ObjectMeta: metav1.ObjectMeta{Name: "openai-economy"},
				Spec: aiopsv1alpha1.AIProviderSpec{
					Type: "openai", Managed: true,
					Pricing: aiopsv1alpha1.ProviderPricing{
						Currency:                   "EUR",
						InputTokenPricePerMillion:  resource.MustParse("1.00"),
						OutputTokenPricePerMillion: resource.MustParse("4.00"),
					},
				},
			},
		},
		models: []aiopsv1alpha1.AIModel{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "gpt-4o"},
				Spec: aiopsv1alpha1.AIModelSpec{
					ProviderRef: "openai-premium",
					ModelName:   "gpt-4o",
					Type:        "llm",
					QualityTier: aiopsv1alpha1.TierHigh,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "gpt-4o-mini"},
				Spec: aiopsv1alpha1.AIModelSpec{
					ProviderRef: "openai-economy",
					ModelName:   "gpt-4o-mini",
					Type:        "llm",
					QualityTier: aiopsv1alpha1.TierMedium,
				},
			},
		},
	}
}
