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

// Package costengine computes LLM spend from usage samples and a price book.
// It is pure (no Kubernetes dependency) so it is trivially unit-testable.
package costengine

import (
	"sort"

	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

// TokenPricing is the per-million-token price for a model, in a single currency.
type TokenPricing struct {
	Currency         string
	InputPerMillion  float64
	OutputPerMillion float64
}

// PriceBook maps a model name to its pricing. Models absent from the book are
// counted with zero cost and reported via Breakdown.UnpricedModels.
type PriceBook map[string]TokenPricing

// LineItem is the cost and usage aggregated for one dimension value.
type LineItem struct {
	Key          string
	Requests     int64
	InputTokens  int64
	OutputTokens int64
	CostInput    float64
	CostOutput   float64
	CostTotal    float64
}

// TotalTokens returns input+output tokens for the line.
func (l LineItem) TotalTokens() int64 { return l.InputTokens + l.OutputTokens }

// Breakdown is the full cost decomposition over a set of samples.
type Breakdown struct {
	Currency      string
	Total         LineItem
	ByModel       map[string]LineItem
	ByProvider    map[string]LineItem
	ByNamespace   map[string]LineItem
	ByTeam        map[string]LineItem
	ByApplication map[string]LineItem
	// UnpricedModels lists models seen in samples but missing from the PriceBook.
	UnpricedModels []string
}

// AvgCostPerRequest returns total cost divided by total requests (0 if none).
func (b Breakdown) AvgCostPerRequest() float64 {
	if b.Total.Requests == 0 {
		return 0
	}
	return b.Total.CostTotal / float64(b.Total.Requests)
}

// CostPerToken returns total cost divided by total tokens (0 if none).
func (b Breakdown) CostPerToken() float64 {
	t := b.Total.TotalTokens()
	if t == 0 {
		return 0
	}
	return b.Total.CostTotal / float64(t)
}

// sampleCost returns input, output and total cost for a single sample.
func sampleCost(s collectors.UsageSample, p TokenPricing) (in, out, total float64) {
	in = float64(s.InputTokens) / 1_000_000 * p.InputPerMillion
	out = float64(s.OutputTokens) / 1_000_000 * p.OutputPerMillion
	return in, out, in + out
}

// Compute decomposes spend across all dimensions. Currency is taken from the
// first priced model; mixed currencies are not converted in the MVP (documented).
func Compute(samples []collectors.UsageSample, prices PriceBook) Breakdown {
	b := Breakdown{
		ByModel:       map[string]LineItem{},
		ByProvider:    map[string]LineItem{},
		ByNamespace:   map[string]LineItem{},
		ByTeam:        map[string]LineItem{},
		ByApplication: map[string]LineItem{},
	}
	unpriced := map[string]struct{}{}

	add := func(m map[string]LineItem, key string, s collectors.UsageSample, in, out, total float64) {
		if key == "" {
			return
		}
		li := m[key]
		li.Key = key
		li.Requests += s.Requests
		li.InputTokens += s.InputTokens
		li.OutputTokens += s.OutputTokens
		li.CostInput += in
		li.CostOutput += out
		li.CostTotal += total
		m[key] = li
	}

	for _, s := range samples {
		p, ok := prices[s.Model]
		if !ok {
			unpriced[s.Model] = struct{}{}
		}
		if b.Currency == "" && p.Currency != "" {
			b.Currency = p.Currency
		}
		in, out, total := sampleCost(s, p)

		b.Total.Key = "total"
		b.Total.Requests += s.Requests
		b.Total.InputTokens += s.InputTokens
		b.Total.OutputTokens += s.OutputTokens
		b.Total.CostInput += in
		b.Total.CostOutput += out
		b.Total.CostTotal += total

		add(b.ByModel, s.Model, s, in, out, total)
		add(b.ByProvider, s.Provider, s, in, out, total)
		add(b.ByNamespace, s.Namespace, s, in, out, total)
		add(b.ByTeam, s.Team, s, in, out, total)
		add(b.ByApplication, s.Application, s, in, out, total)
	}

	for m := range unpriced {
		b.UnpricedModels = append(b.UnpricedModels, m)
	}
	sort.Strings(b.UnpricedModels)
	return b
}

// TopByCost returns the line items of a dimension map sorted by descending cost,
// truncated to n (n<=0 returns all).
func TopByCost(m map[string]LineItem, n int) []LineItem {
	items := make([]LineItem, 0, len(m))
	for _, li := range m {
		items = append(items, li)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CostTotal != items[j].CostTotal {
			return items[i].CostTotal > items[j].CostTotal
		}
		return items[i].Key < items[j].Key
	})
	if n > 0 && len(items) > n {
		items = items[:n]
	}
	return items
}
