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

// Package routingscore computes auditable runtime scores from real usage
// telemetry. It never turns a catalog prior into a measured latency: when no
// observed latency is present, the total score still exists, but the latency
// component is neutral and explicitly marked unavailable.
package routingscore

import (
	"math"
	"sort"

	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
	"github.com/imperium/ai-sovereign-finops-operator/internal/costengine"
)

const (
	NeutralLatencyScore = 0.5

	SourceObserved    = "observed"
	SourceUnavailable = "unavailable"
)

// Weights are higher-is-better score weights. They intentionally sum to one.
// Sovereignty is not a weight: a non-compliant model is hard-blocked (score 0)
// regardless of how attractive its cost/quality/latency/reliability are.
type Weights struct {
	Cost, Quality, Latency, Reliability float64
}

// DefaultWeights balances FinOps pressure with quality and measured latency.
func DefaultWeights() Weights {
	return Weights{Cost: 0.40, Quality: 0.30, Latency: 0.20, Reliability: 0.10}
}

// ModelInfo carries catalog metadata needed for scoring.
type ModelInfo struct {
	QualityTier string
	// SovereigntyChecked is true when SovereigntyCompliant was computed from an
	// active policy/catalog. When false, the routing engine defaults to compliant
	// so callers that only provide quality tiers keep receiving runtime scores.
	SovereigntyChecked bool
	// SovereigntyCompliant is false when the model's provider sits in a
	// zone forbidden by the active AISovereigntyPolicy. A non-compliant model
	// receives a hard score of 0 and is never recommended, regardless of cost.
	SovereigntyCompliant bool
}

// Score is one namespace/application/model runtime score.
type Score struct {
	Namespace                 string
	Application               string
	Model                     string
	Provider                  string
	Requests                  int64
	InputTokens               int64
	OutputTokens              int64
	Errors                    int64
	CostEUR                   float64
	CostPerRequestEUR         float64
	ObservedLatencyMillis     float64
	LatencyTelemetryAvailable bool
	LatencySource             string
	CostScore                 float64
	QualityScore              float64
	LatencyScore              float64
	ReliabilityScore          float64
	// SovereigntyCompliant mirrors the catalog flag. When false the Score field
	// is hard-set to 0 so this model is never chosen by the routing engine.
	SovereigntyCompliant bool
	Score                float64
}

type aggregate struct {
	Score
	latencyWeight float64
}

// Compute returns one score per observed workload/model. A score is always
// emitted for every observed sample group, even when latency telemetry is absent.
func Compute(samples []collectors.UsageSample, prices costengine.PriceBook, models map[string]ModelInfo, weights Weights) []Score {
	if weights == (Weights{}) {
		weights = DefaultWeights()
	}
	groups := aggregateSamples(samples, prices, models)
	if len(groups) == 0 {
		return nil
	}

	minCost, maxCost := math.MaxFloat64, 0.0
	minLatency, maxLatency := math.MaxFloat64, 0.0
	for _, g := range groups {
		if g.CostPerRequestEUR < minCost {
			minCost = g.CostPerRequestEUR
		}
		if g.CostPerRequestEUR > maxCost {
			maxCost = g.CostPerRequestEUR
		}
		if g.LatencyTelemetryAvailable {
			if g.ObservedLatencyMillis < minLatency {
				minLatency = g.ObservedLatencyMillis
			}
			if g.ObservedLatencyMillis > maxLatency {
				maxLatency = g.ObservedLatencyMillis
			}
		}
	}

	out := make([]Score, 0, len(groups))
	for _, g := range groups {
		s := g.Score
		s.CostScore = inverseNormalized(s.CostPerRequestEUR, minCost, maxCost)
		s.ReliabilityScore = reliabilityScore(s.Requests, s.Errors)
		if s.LatencyTelemetryAvailable {
			s.LatencyScore = inverseNormalized(s.ObservedLatencyMillis, minLatency, maxLatency)
			s.LatencySource = SourceObserved
		} else {
			s.LatencyScore = NeutralLatencyScore
			s.LatencySource = SourceUnavailable
		}
		// Sovereignty is a hard gate: a model in a forbidden zone scores 0
		// and must never be recommended, regardless of cost/quality/latency.
		if !s.SovereigntyCompliant {
			s.Score = 0
		} else {
			s.Score = clamp01(weights.Cost*s.CostScore +
				weights.Quality*s.QualityScore +
				weights.Latency*s.LatencyScore +
				weights.Reliability*s.ReliabilityScore)
		}
		out = append(out, s)
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].Application != out[j].Application {
			return out[i].Application < out[j].Application
		}
		return out[i].Model < out[j].Model
	})
	return out
}

func aggregateSamples(samples []collectors.UsageSample, prices costengine.PriceBook, models map[string]ModelInfo) map[string]*aggregate {
	groups := map[string]*aggregate{}
	for _, s := range samples {
		key := s.Namespace + "\x1f" + s.Application + "\x1f" + s.Model + "\x1f" + s.Provider
		g := groups[key]
		if g == nil {
			info, ok := models[s.Model]
			g = &aggregate{}
			g.Namespace = s.Namespace
			g.Application = s.Application
			g.Model = s.Model
			g.Provider = s.Provider
			g.QualityScore = qualityScore(info.QualityTier)
			g.SovereigntyCompliant = true
			if ok && info.SovereigntyChecked {
				g.SovereigntyCompliant = info.SovereigntyCompliant
			}
			groups[key] = g
		}
		g.Requests += s.Requests
		g.InputTokens += s.InputTokens
		g.OutputTokens += s.OutputTokens
		g.Errors += s.Errors
		if s.LatencyMillis > 0 && s.Requests > 0 {
			g.LatencyTelemetryAvailable = true
			g.latencyWeight += float64(s.Requests)
			g.ObservedLatencyMillis += s.LatencyMillis * float64(s.Requests)
		}
	}
	for _, g := range groups {
		if p, ok := prices[g.Model]; ok {
			g.CostEUR = float64(g.InputTokens)/1e6*p.InputPerMillion + float64(g.OutputTokens)/1e6*p.OutputPerMillion
		}
		if g.Requests > 0 {
			g.CostPerRequestEUR = g.CostEUR / float64(g.Requests)
		}
		if g.LatencyTelemetryAvailable && g.latencyWeight > 0 {
			g.ObservedLatencyMillis /= g.latencyWeight
		}
	}
	return groups
}

func inverseNormalized(v, min, max float64) float64 {
	if max <= min {
		return 1
	}
	return clamp01(1 - (v-min)/(max-min))
}

func qualityScore(tier string) float64 {
	switch tier {
	case "high":
		return 1.0
	case "medium":
		return 0.75
	case "low":
		return 0.50
	default:
		return 0.60
	}
}

func reliabilityScore(requests, errors int64) float64 {
	if requests <= 0 {
		return 1
	}
	return clamp01(1 - float64(errors)/float64(requests))
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
