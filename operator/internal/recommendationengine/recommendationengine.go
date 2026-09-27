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

// Package recommendationengine turns observed usage, the model catalog and
// sovereignty findings into actionable, quantified recommendations:
//   - cost-saving: route an app's traffic to a cheaper model and how much it saves
//   - sovereignty: requests/cost going to non-compliant providers and the fix
//   - data-quality: usage of models missing a price (unattributable cost)
//
// It is pure (no Kubernetes dependency) and computes EUR figures from the same
// real token volumes and prices the cost engine uses.
package recommendationengine

import (
	"fmt"
	"sort"
)

// Types and severities.
const (
	TypeCostSaving  = "cost-saving"
	TypeSovereignty = "sovereignty"
	TypeDataQuality = "data-quality"

	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

// Usage is one app/model's observed consumption with its computed cost.
type Usage struct {
	Namespace, Application, Model string
	InputTokens, OutputTokens     int64
	Requests                      int64
	CostEUR                       float64
}

// Candidate is an alternative model the router could use, with its prices.
// Compliant reports whether the model's provider sits in a sovereignty-allowed
// zone: a sovereign-FinOps product must never recommend routing to a model that
// violates the active sovereignty policy, however cheap it is.
type Candidate struct {
	Name                              string
	InputPerMillion, OutputPerMillion float64
	Compliant                         bool
}

// Risk is sovereignty exposure for an app: requests/cost sent to a non-compliant
// provider (forbidden or out-of-allowed zone).
type Risk struct {
	Namespace, Application, Provider, Zone string
	Requests                               int64
	CostEUR                                float64
}

// Recommendation is one actionable suggestion. EstimatedSavingsEUR is over the
// same observation window as the input usage (0 when not a saving).
type Recommendation struct {
	Type     string
	Severity string
	Message  string
	// Namespace and Application identify the workload the recommendation targets
	// (empty for catalog-wide recommendations such as data-quality). They let
	// consumers attribute each action to an owner without parsing the message.
	Namespace   string
	Application string
	// CurrentModel and RecommendedModel describe a cost-saving model swap (empty
	// for other types), so a dashboard can show the concrete action.
	CurrentModel        string
	RecommendedModel    string
	EstimatedSavingsEUR float64
}

// minSavingFraction suppresses noise: only suggest a swap that cuts >=20% of cost.
const minSavingFraction = 0.20

// candidateCost prices a usage's token mix under a candidate model (EUR).
func candidateCost(u Usage, c Candidate) float64 {
	return float64(u.InputTokens)/1e6*c.InputPerMillion + float64(u.OutputTokens)/1e6*c.OutputPerMillion
}

// Recommend produces quantified recommendations. hasCompliantModel indicates
// whether the catalog has any model in an allowed sovereignty zone.
func Recommend(usages []Usage, candidates []Candidate, risks []Risk, unpricedModels []string, hasCompliantModel bool) (recs []Recommendation, totalSavings float64) {
	// 1. Cost-saving: for each app/model, find the cheapest alternative.
	for _, u := range usages {
		if u.CostEUR <= 0 {
			continue
		}
		best := ""
		bestCost := u.CostEUR
		for _, c := range candidates {
			if c.Name == u.Model {
				continue
			}
			// Never recommend a swap to a non-compliant (forbidden-zone) model:
			// saving money by breaking sovereignty contradicts the product's purpose.
			if !c.Compliant {
				continue
			}
			if cc := candidateCost(u, c); cc < bestCost {
				bestCost, best = cc, c.Name
			}
		}
		saving := u.CostEUR - bestCost
		if best != "" && saving >= u.CostEUR*minSavingFraction {
			recs = append(recs, Recommendation{
				Type: TypeCostSaving, Severity: SeverityInfo,
				Namespace: u.Namespace, Application: u.Application,
				CurrentModel: u.Model, RecommendedModel: best,
				EstimatedSavingsEUR: saving,
				Message: fmt.Sprintf("%s/%s: routing low-stakes %q traffic to %q would save ~%.4f EUR over the window (-%.0f%%).",
					u.Namespace, u.Application, u.Model, best, saving, saving/u.CostEUR*100),
			})
			totalSavings += saving
		}
	}

	// 2. Sovereignty: requests/cost going to non-compliant providers.
	for _, r := range risks {
		if r.Requests <= 0 {
			continue
		}
		fix := "route this app to an allowed-zone model"
		if !hasCompliantModel {
			fix = "no compliant (allowed-zone) model exists in the catalog — add an EU/allowed provider"
		}
		recs = append(recs, Recommendation{
			Type: TypeSovereignty, Severity: SeverityCritical,
			Namespace: r.Namespace, Application: r.Application,
			Message: fmt.Sprintf("%s/%s: %d request(s) (~%.4f EUR) sent to non-compliant provider %q in zone %s — %s.",
				r.Namespace, r.Application, r.Requests, r.CostEUR, r.Provider, r.Zone, fix),
		})
	}

	// 3. Data quality: unpriced models make cost unattributable.
	sort.Strings(unpricedModels)
	for _, m := range unpricedModels {
		recs = append(recs, Recommendation{
			Type: TypeDataQuality, Severity: SeverityWarning,
			Message: fmt.Sprintf("Model %q has no price; create an AIModel/AIProvider so its cost is attributed.", m),
		})
	}

	// Highest-value first: by severity, then estimated saving.
	sort.SliceStable(recs, func(i, j int) bool {
		ri, rj := sevRank(recs[i].Severity), sevRank(recs[j].Severity)
		if ri != rj {
			return ri > rj
		}
		return recs[i].EstimatedSavingsEUR > recs[j].EstimatedSavingsEUR
	})
	return recs, totalSavings
}

func sevRank(s string) int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	default:
		return 1
	}
}
