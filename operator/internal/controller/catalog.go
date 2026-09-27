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
	"context"
	"fmt"
	"math"

	"sigs.k8s.io/controller-runtime/pkg/client"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	catalogdefaults "github.com/imperium/ai-sovereign-finops-operator/internal/catalog"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
	aigwcollector "github.com/imperium/ai-sovereign-finops-operator/internal/collectors/aigw"
	cmcollector "github.com/imperium/ai-sovereign-finops-operator/internal/collectors/configmap"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors/fake"
	promcollector "github.com/imperium/ai-sovereign-finops-operator/internal/collectors/prometheus"
	"github.com/imperium/ai-sovereign-finops-operator/internal/costengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/routingscore"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

// catalog holds the cluster-side model/provider definitions needed by the engines.
type catalog struct {
	providers map[string]aiopsv1alpha1.AIProvider // by metadata.name
	models    []aiopsv1alpha1.AIModel
}

// loadCatalog lists AIProviders and AIModels in the given namespace.
func loadCatalog(ctx context.Context, c client.Client, namespace string) (catalog, error) {
	var provList aiopsv1alpha1.AIProviderList
	if err := c.List(ctx, &provList, client.InNamespace(namespace)); err != nil {
		return catalog{}, err
	}
	var modelList aiopsv1alpha1.AIModelList
	if err := c.List(ctx, &modelList, client.InNamespace(namespace)); err != nil {
		return catalog{}, err
	}
	cat := catalog{providers: map[string]aiopsv1alpha1.AIProvider{}, models: modelList.Items}
	for i := range provList.Items {
		cat.providers[provList.Items[i].Name] = provList.Items[i]
	}
	return cat, nil
}

// priceBook builds a costengine.PriceBook keyed by model name. It seeds the book
// with the built-in default catalog (well-known public list prices) so spend is
// computed out of the box, then overlays user AIModel/AIProvider pricing on top —
// user CRs always win; defaults only fill the gaps.
func (cat catalog) priceBook() costengine.PriceBook {
	pb := catalogdefaults.DefaultPriceBook()
	for i := range cat.models {
		m := cat.models[i]
		prov, ok := cat.providers[m.Spec.ProviderRef]
		if !ok {
			continue
		}
		pb[m.Spec.ModelName] = costengine.TokenPricing{
			Currency:         orDefault(prov.Spec.Pricing.Currency, "EUR"),
			InputPerMillion:  prov.Spec.Pricing.InputTokenPricePerMillion.AsApproximateFloat64(),
			OutputPerMillion: prov.Spec.Pricing.OutputTokenPricePerMillion.AsApproximateFloat64(),
		}
	}
	return pb
}

// routingModelInfo builds ModelInfo keyed by provider-side model name. When a
// sovereignty policy is active, models in forbidden zones are flagged non-compliant
// so the routing score engine can hard-gate them to 0.
func (cat catalog) routingModelInfo(pe *sovereigntyengine.Policy) map[string]routingscore.ModelInfo {
	out := map[string]routingscore.ModelInfo{}
	// Seed from the built-in default catalog so well-known models get zone info.
	for _, name := range catalogdefaults.AllModelNames() {
		zone := catalogdefaults.ZoneForModel(name)
		compliant := pe == nil || sovereigntyengine.IsZoneAllowed(*pe, zone)
		out[name] = routingscore.ModelInfo{SovereigntyChecked: true, SovereigntyCompliant: compliant}
	}
	for i := range cat.models {
		m := cat.models[i]
		zone := cat.zoneForModel(m.Spec.ModelName)
		compliant := pe == nil || sovereigntyengine.IsZoneAllowed(*pe, zone)
		out[m.Spec.ModelName] = routingscore.ModelInfo{
			QualityTier:          string(m.Spec.QualityTier),
			SovereigntyChecked:   true,
			SovereigntyCompliant: compliant,
		}
	}
	return out
}

// unknownModels returns the distinct provider-side model ids in samples that are
// priced by neither a user AIModel nor the built-in default catalog — models the
// operator cannot cost. They drive the data-quality recommendation and an auto
// stub AIModel. Returned in first-seen order.
func (cat catalog) unknownModels(samples []collectors.UsageSample) []string {
	userModels := map[string]bool{}
	for i := range cat.models {
		userModels[cat.models[i].Spec.ModelName] = true
	}
	seen := map[string]bool{}
	var out []string
	for _, s := range samples {
		if s.Model == "" || seen[s.Model] {
			continue
		}
		seen[s.Model] = true
		if userModels[s.Model] || catalogdefaults.Known(s.Model) {
			continue
		}
		out = append(out, s.Model)
	}
	return out
}

// modelByName returns the AIModel with the given metadata name.
func (cat catalog) modelByName(name string) (aiopsv1alpha1.AIModel, bool) {
	for i := range cat.models {
		if cat.models[i].Name == name {
			return cat.models[i], true
		}
	}
	return aiopsv1alpha1.AIModel{}, false
}

// zoneForModel returns the normalized sovereignty zone of the provider serving
// the given model name (empty if the model or its provider is unknown).
func (cat catalog) zoneForModel(modelName string) string {
	for i := range cat.models {
		if cat.models[i].Spec.ModelName != modelName {
			continue
		}
		if prov, ok := cat.providers[cat.models[i].Spec.ProviderRef]; ok {
			return sovereigntyengine.NormalizeZone(prov.Spec.DataResidency)
		}
	}
	// No user catalog entry — fall back to the built-in default zone for the model.
	return catalogdefaults.ZoneForModel(modelName)
}

// cheapestCompliantModelName returns the provider-side model name of the cheapest
// catalog model whose provider sits in a sovereignty-allowed zone (empty if none).
// It is the reroute target enforcement proposes when blocking forbidden-zone traffic.
func (cat catalog) cheapestCompliantModelName(pe sovereigntyengine.Policy) string {
	best := ""
	bestPrice := math.MaxFloat64
	for name, p := range cat.priceBook() {
		if !sovereigntyengine.IsZoneAllowed(pe, cat.zoneForModel(name)) {
			continue
		}
		if price := p.InputPerMillion + p.OutputPerMillion; price < bestPrice {
			bestPrice, best = price, name
		}
	}
	return best
}

// providerFixedMonthly returns the provider's flat monthly fee (0 if unset).
func (cat catalog) providerFixedMonthly(providerRef string) float64 {
	if p, ok := cat.providers[providerRef]; ok && p.Spec.Pricing.FixedMonthlyCost != nil {
		return p.Spec.Pricing.FixedMonthlyCost.AsApproximateFloat64()
	}
	return 0
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// collectorFor returns the TelemetryCollector implied by a gateway's telemetry
// mode. There is deliberately NO silent fake fallback: a sovereign-FinOps product
// whose whole value is "real, verifiable numbers" must never serve fabricated
// figures that a reader could mistake for actual spend. The fake collector is
// returned ONLY when a gateway explicitly opts in with mode "fake"; every other
// path that lacks a real source returns an error so the caller can surface a clear
// status condition instead of inventing data.
func collectorFor(c client.Client, namespace string, gw *aiopsv1alpha1.AIGateway) (collectors.TelemetryCollector, error) {
	if gw == nil {
		return nil, fmt.Errorf("no AIGateway / telemetry source configured for namespace %q; "+
			"set spec.gatewayRef or create an AIGateway with a telemetry mode (prometheus|configmap|aigw), "+
			"or mode \"fake\" to explicitly opt into demo data", namespace)
	}
	switch gw.Spec.Telemetry.Mode {
	case aiopsv1alpha1.TelemetryModePrometheus:
		endpoint := gw.Spec.Endpoint + gw.Spec.Telemetry.MetricsEndpoint
		return promcollector.New(endpoint), nil
	case aiopsv1alpha1.TelemetryModeConfigMap:
		name := gw.Spec.Telemetry.SourceConfigMap
		if name == "" {
			return nil, fmt.Errorf("gateway %q uses configmap telemetry but spec.telemetry.sourceConfigMap is empty", gw.Name)
		}
		return cmcollector.New(c, namespace, name), nil
	case aiopsv1alpha1.TelemetryModeAIGW:
		endpoint := gw.Spec.Endpoint + gw.Spec.Telemetry.MetricsEndpoint
		return aigwcollector.New(c, namespace, endpoint), nil
	case aiopsv1alpha1.TelemetryModeFake:
		// Explicit, opt-in demo data only.
		return fake.New(), nil
	default:
		return nil, fmt.Errorf("gateway %q has unknown telemetry mode %q", gw.Name, gw.Spec.Telemetry.Mode)
	}
}

// filterByNamespace keeps only samples for the target namespace (all if empty).
func filterByNamespace(samples []collectors.UsageSample, namespace string) []collectors.UsageSample {
	if namespace == "" {
		return samples
	}
	out := samples[:0:0]
	for _, s := range samples {
		if s.Namespace == namespace {
			out = append(out, s)
		}
	}
	return out
}

// filterByBudgetTarget keeps samples matching every non-empty target dimension.
func filterByBudgetTarget(samples []collectors.UsageSample, t aiopsv1alpha1.BudgetTarget) []collectors.UsageSample {
	out := samples[:0:0]
	for _, s := range samples {
		if t.Namespace != "" && s.Namespace != t.Namespace {
			continue
		}
		if t.Team != "" && s.Team != t.Team {
			continue
		}
		if t.Application != "" && s.Application != t.Application {
			continue
		}
		out = append(out, s)
	}
	return out
}

// modelSensitivityByProviderModel indexes whether each provider-side model name
// is allowed for sensitive data, from the AIModel catalog.
func (cat catalog) modelSensitivity(modelName string) (allowed bool, known bool) {
	for i := range cat.models {
		if cat.models[i].Spec.ModelName == modelName {
			return cat.models[i].Spec.SensitiveDataAllowed, true
		}
	}
	return false, false
}

// flows turns usage samples into sovereignty Flows, resolving each flow's
// provider residency/managed/sensitivity and the model's sensitivity from the
// catalog. This is what makes sovereignty verification per namespace/app rather
// than per provider. Samples without a provider are skipped.
func (cat catalog) flows(samples []collectors.UsageSample) []sovereigntyengine.Flow {
	out := make([]sovereigntyengine.Flow, 0, len(samples))
	for _, s := range samples {
		fl := sovereigntyengine.Flow{
			Namespace:   s.Namespace,
			Application: s.Application,
			Model:       s.Model,
			Provider:    s.Provider,
			Requests:    s.Requests,
		}
		if p, ok := cat.providers[s.Provider]; ok {
			fl.Zone = p.Spec.DataResidency
			fl.Managed = p.Spec.Managed
			fl.ProviderAllowsSensitive = p.Spec.Compliance.AllowedForSensitiveData
		}
		// Autonomy: with no AIProvider CR (or one without a residency), derive the
		// zone from the built-in default catalog by model — so sovereignty works out
		// of the box. Default-catalog models are managed external APIs. This also
		// fixes the case of provider-only telemetry whose provider has no CR.
		if sovereigntyengine.NormalizeZone(fl.Zone) == "" {
			if dz := catalogdefaults.ZoneForModel(s.Model); dz != "" {
				fl.Zone = dz
				fl.Managed = true
				if fl.Provider == "" {
					fl.Provider = catalogdefaults.ProviderForModel(s.Model)
				}
			}
		}
		// Nothing to assert about a flow we can place neither by provider nor by
		// model — skip it rather than emit a spurious "unknown zone" finding.
		if sovereigntyengine.NormalizeZone(fl.Zone) == "" && s.Provider == "" {
			continue
		}
		// A model unknown to the catalog defaults to not allowed for sensitive
		// data (conservative: surface it rather than silently pass).
		modelAllows, _ := cat.modelSensitivity(s.Model)
		fl.ModelAllowsSensitive = modelAllows
		out = append(out, fl)
	}
	return out
}

// avgDaysPerMonth is the run-rate denominator (365.25/12).
const avgDaysPerMonth = 30.4375

// monthlyFactor returns the multiplier that forecasts a full month from spend
// observed over the given period window (run-rate). daily -> ~30.4, weekly ->
// ~4.35, monthly/unknown -> 1 (no extrapolation).
func monthlyFactor(period string) float64 {
	switch period {
	case "daily":
		return avgDaysPerMonth
	case "weekly":
		return avgDaysPerMonth / 7.0
	default:
		return 1.0
	}
}

// firstSovereigntyPolicy returns any AISovereigntyPolicy in the namespace, or nil.
func firstSovereigntyPolicy(ctx context.Context, c client.Client, namespace string) *aiopsv1alpha1.AISovereigntyPolicy {
	var list aiopsv1alpha1.AISovereigntyPolicyList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil || len(list.Items) == 0 {
		return nil
	}
	return &list.Items[0]
}

// firstGateway returns any AIGateway in the namespace, or nil if none exists.
func firstGateway(ctx context.Context, c client.Client, namespace string) *aiopsv1alpha1.AIGateway {
	var list aiopsv1alpha1.AIGatewayList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil || len(list.Items) == 0 {
		return nil
	}
	return &list.Items[0]
}
