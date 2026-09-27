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
	"fmt"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
	"github.com/imperium/ai-sovereign-finops-operator/internal/costengine"
)

const budgetRerouteAnnotation = "aiops.imperium.io/budget-fallback-reroutes"

type budgetFallbackDecision struct {
	desired     map[string]string
	activeModel string
	reason      string
}

type scopedModelUsage struct {
	requests     int64
	inputTokens  int64
	outputTokens int64
}

func budgetFallbackDesired(cat catalog, allSamples, scopedSamples []collectors.UsageSample, spec aiopsv1alpha1.AIBudgetPolicySpec, phase, collectorName string, sovereigntyEnforced bool) budgetFallbackDecision {
	if spec.FallbackModelRef == "" {
		return budgetFallbackDecision{reason: "no fallback model configured"}
	}
	if spec.EnforcementMode != aiopsv1alpha1.EnforcementEnforce {
		return budgetFallbackDecision{reason: fmt.Sprintf("fallback remains advisory in mode %q", spec.EnforcementMode)}
	}
	if !budgetFallbackPhaseReached(phase, spec.FallbackOnPhase) {
		return budgetFallbackDecision{reason: fmt.Sprintf("budget phase %s is below fallback threshold %s", phase, fallbackPhaseOrDefault(spec.FallbackOnPhase))}
	}
	if sovereigntyEnforced {
		return budgetFallbackDecision{reason: "sovereignty enforcement is active; skip budget fallback to avoid route conflicts"}
	}

	fallback, ok := cat.modelByName(spec.FallbackModelRef)
	if !ok {
		return budgetFallbackDecision{reason: fmt.Sprintf("fallback model ref %q was not found", spec.FallbackModelRef)}
	}
	prov, ok := cat.providers[fallback.Spec.ProviderRef]
	if !ok {
		return budgetFallbackDecision{reason: fmt.Sprintf("fallback provider %q was not found", fallback.Spec.ProviderRef)}
	}
	if !prov.Spec.Managed {
		return budgetFallbackDecision{reason: "fallback provider is not a managed API"}
	}
	if spec.MinFallbackQualityTier != "" && tierRank(fallback.Spec.QualityTier) < tierRank(spec.MinFallbackQualityTier) {
		return budgetFallbackDecision{reason: fmt.Sprintf("fallback quality tier %q is below required %q", fallback.Spec.QualityTier, spec.MinFallbackQualityTier)}
	}
	if spec.MaxFallbackLatencyMillis > 0 {
		if !collectorSupportsLatency(collectorName) {
			return budgetFallbackDecision{reason: fmt.Sprintf("latency guardrail requires telemetry with latency; collector %q does not provide it", collectorName)}
		}
		_, _, lat, ok := observedModelPerformance(allSamples, fallback.Spec.ModelName)
		if !ok {
			return budgetFallbackDecision{reason: fmt.Sprintf("fallback model %q has no observed latency yet", fallback.Spec.ModelName)}
		}
		if lat > float64(spec.MaxFallbackLatencyMillis) {
			return budgetFallbackDecision{reason: fmt.Sprintf("fallback latency %.0fms exceeds guardrail %dms", lat, spec.MaxFallbackLatencyMillis)}
		}
	}
	if spec.MaxFallbackErrorPercent > 0 {
		if !collectorSupportsErrors(collectorName) {
			return budgetFallbackDecision{reason: fmt.Sprintf("error-rate guardrail requires telemetry with errors; collector %q does not provide it", collectorName)}
		}
		reqs, errs, _, ok := observedModelPerformance(allSamples, fallback.Spec.ModelName)
		if !ok || reqs <= 0 {
			return budgetFallbackDecision{reason: fmt.Sprintf("fallback model %q has no observed error rate yet", fallback.Spec.ModelName)}
		}
		errPct := float64(errs) * 100 / float64(reqs)
		if errPct > float64(spec.MaxFallbackErrorPercent) {
			return budgetFallbackDecision{reason: fmt.Sprintf("fallback error rate %.1f%% exceeds guardrail %d%%", errPct, spec.MaxFallbackErrorPercent)}
		}
	}

	pb := cat.priceBook()
	fallbackPrice, ok := pb[fallback.Spec.ModelName]
	if !ok {
		return budgetFallbackDecision{reason: fmt.Sprintf("fallback model %q has no known price", fallback.Spec.ModelName)}
	}
	usages := usageByModel(scopedSamples)
	if len(usages) == 0 {
		return budgetFallbackDecision{reason: "no scoped usage observed for the budget target"}
	}
	shared := sharedModelsOutsideTarget(allSamples, spec.Target)

	desired := map[string]string{}
	for model, u := range usages {
		if model == fallback.Spec.ModelName {
			continue
		}
		if shared[model] {
			continue
		}
		curPrice, ok := pb[model]
		if !ok {
			continue
		}
		if pricedUsage(u, fallbackPrice) < pricedUsage(u, curPrice) {
			desired[model] = fallback.Spec.ModelName
		}
	}
	if len(desired) == 0 {
		return budgetFallbackDecision{reason: "no dedicated scoped model is more expensive than the fallback"}
	}
	return budgetFallbackDecision{
		desired:     desired,
		activeModel: fallback.Spec.ModelName,
		reason:      fmt.Sprintf("fallback %q selected for %d scoped model(s)", fallback.Spec.ModelName, len(desired)),
	}
}

func budgetFallbackPhaseReached(phase string, threshold aiopsv1alpha1.BudgetFallbackPhase) bool {
	return budgetPhaseRank(phase) >= budgetPhaseRank(string(fallbackPhaseOrDefault(threshold)))
}

func fallbackPhaseOrDefault(v aiopsv1alpha1.BudgetFallbackPhase) aiopsv1alpha1.BudgetFallbackPhase {
	if v == "" {
		return aiopsv1alpha1.BudgetFallbackOnExceeded
	}
	return v
}

func budgetPhaseRank(phase string) int {
	switch phase {
	case "Exceeded":
		return 4
	case "Critical":
		return 3
	case "Warning":
		return 2
	case "WithinBudget":
		return 1
	default:
		return 0
	}
}

func collectorSupportsLatency(name string) bool {
	return name == "configmap" || name == "fake" || name == "prometheus" || name == "aigw"
}

func collectorSupportsErrors(name string) bool {
	return name == "configmap" || name == "fake" || name == "prometheus"
}

func tierRank(t aiopsv1alpha1.Tier) int {
	switch t {
	case aiopsv1alpha1.TierHigh:
		return 3
	case aiopsv1alpha1.TierMedium:
		return 2
	case aiopsv1alpha1.TierLow:
		return 1
	default:
		return 0
	}
}

func usageByModel(samples []collectors.UsageSample) map[string]scopedModelUsage {
	out := map[string]scopedModelUsage{}
	for _, s := range samples {
		u := out[s.Model]
		u.requests += s.Requests
		u.inputTokens += s.InputTokens
		u.outputTokens += s.OutputTokens
		out[s.Model] = u
	}
	return out
}

func pricedUsage(u scopedModelUsage, p costengine.TokenPricing) float64 {
	return float64(u.inputTokens)/1e6*p.InputPerMillion + float64(u.outputTokens)/1e6*p.OutputPerMillion
}

func observedModelPerformance(samples []collectors.UsageSample, model string) (requests, errors int64, latencyMillis float64, ok bool) {
	var weightedLat float64
	for _, s := range samples {
		if s.Model != model {
			continue
		}
		ok = true
		requests += s.Requests
		errors += s.Errors
		weightedLat += s.LatencyMillis * float64(s.Requests)
	}
	if requests > 0 {
		latencyMillis = weightedLat / float64(requests)
	}
	return requests, errors, latencyMillis, ok
}

func sharedModelsOutsideTarget(samples []collectors.UsageSample, target aiopsv1alpha1.BudgetTarget) map[string]bool {
	inTarget := map[string]bool{}
	shared := map[string]bool{}
	for _, s := range samples {
		if budgetTargetMatchesSample(s, target) {
			inTarget[s.Model] = true
			continue
		}
		if inTarget[s.Model] {
			shared[s.Model] = true
		}
	}
	for _, s := range samples {
		if !budgetTargetMatchesSample(s, target) && inTarget[s.Model] {
			shared[s.Model] = true
		}
	}
	return shared
}

func budgetTargetMatchesSample(s collectors.UsageSample, t aiopsv1alpha1.BudgetTarget) bool {
	if t.Namespace != "" && s.Namespace != t.Namespace {
		return false
	}
	if t.Team != "" && s.Team != t.Team {
		return false
	}
	if t.Application != "" && s.Application != t.Application {
		return false
	}
	return true
}
