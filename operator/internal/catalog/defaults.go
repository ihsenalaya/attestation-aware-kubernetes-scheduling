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

// Package catalog holds a BUILT-IN default catalog of well-known LLM providers and
// models so the operator produces meaningful cost + sovereignty signals out of the
// box — before anyone writes a single AIProvider/AIModel CR. It is pure (no K8s
// dependency) and therefore unit-testable.
//
// Two responsibilities:
//
//   - Default pricing & zone per well-known model (DefaultPriceBook / ZoneForModel).
//   - EndpointToZone: derive the sovereignty zone + provider family from a hostname,
//     so a flow can be classified from where it goes even without a catalog entry.
//
// IMPORTANT — these are DEFAULTS, never authority. A user AIProvider/AIModel always
// overrides them; defaults only fill the gaps. The prices below are PUBLIC list
// prices from the providers' own pricing pages, last reviewed PriceDate, converted
// USD→EUR at usdToEUR. They are best-effort reference values for forecasting, NOT a
// billing source — consistent with the project rule of never presenting fabricated
// numbers as measured spend.
package catalog

import (
	"strings"

	"github.com/imperium/ai-sovereign-finops-operator/internal/costengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

const (
	// PriceDate is the month these default list prices were last reviewed. Surface
	// it wherever defaults are used so a stale price is visible, not silent.
	PriceDate = "2026-01"

	// usdToEUR converts USD list prices to EUR (same rate the demo assets use). The
	// real economics depend on contracts and FX; this is a transparent approximation.
	usdToEUR = 0.92
)

// defaultModel is a well-known model's provider family, sovereignty zone and public
// per-million-token list prices in USD (converted to EUR on read).
type defaultModel struct {
	Provider  string  // provider family (matches AIProvider.Type)
	Zone      string  // canonical zone (US, EU, ...) per NormalizeZone
	InputUSD  float64 // USD / 1e6 input tokens (list price)
	OutputUSD float64 // USD / 1e6 output tokens (list price)
}

// defaultModels maps a provider-side model id to its defaults. Only well-established
// public prices are listed; unknown/newer ids are deliberately omitted so they fall
// through to a data-quality recommendation rather than carry an invented price.
var defaultModels = map[string]defaultModel{
	// OpenAI — United States.
	"gpt-4o":        {"openai", "US", 2.50, 10.00},
	"gpt-4o-mini":   {"openai", "US", 0.15, 0.60},
	"gpt-4-turbo":   {"openai", "US", 10.00, 30.00},
	"gpt-3.5-turbo": {"openai", "US", 0.50, 1.50},

	// Anthropic (Claude) — United States.
	"claude-3-5-sonnet": {"anthropic", "US", 3.00, 15.00},
	"claude-3-7-sonnet": {"anthropic", "US", 3.00, 15.00},
	"claude-3-5-haiku":  {"anthropic", "US", 0.80, 4.00},
	"claude-3-haiku":    {"anthropic", "US", 0.25, 1.25},
	"claude-3-opus":     {"anthropic", "US", 15.00, 75.00},

	// Mistral — European Union.
	"mistral-large":        {"mistral", "EU", 2.00, 6.00},
	"mistral-large-latest": {"mistral", "EU", 2.00, 6.00},
	"mistral-small":        {"mistral", "EU", 0.20, 0.60},
	"mistral-small-latest": {"mistral", "EU", 0.20, 0.60},

	// Google (Gemini) — direct API is US/global; Vertex regional zone via endpoint.
	"gemini-1.5-pro":   {"vertex", "US", 1.25, 5.00},
	"gemini-1.5-flash": {"vertex", "US", 0.075, 0.30},
}

// normalizeModel canonicalizes a model id for lookup: lowercase, and strip a leading
// provider prefix and trailing dated/version suffix so "Mistral-Large-2407" or
// "claude-3-5-sonnet-20241022" still resolve to a known base id.
func normalizeModel(model string) string {
	m := strings.ToLower(strings.TrimSpace(model))
	// Drop a date suffix like -20241022 or -2407 (kept simple: trailing -<digits>).
	if i := strings.LastIndex(m, "-"); i > 0 {
		if rest := m[i+1:]; rest != "" && isAllDigits(rest) {
			m = m[:i]
		}
	}
	return m
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0
}

// lookup returns the default entry for a model id (after normalization), trying the
// exact id first then the normalized base id.
func lookup(model string) (defaultModel, bool) {
	if d, ok := defaultModels[strings.ToLower(strings.TrimSpace(model))]; ok {
		return d, true
	}
	d, ok := defaultModels[normalizeModel(model)]
	return d, ok
}

// Known reports whether the default catalog prices the given model id.
func Known(model string) bool {
	_, ok := lookup(model)
	return ok
}

// PriceForModel returns the default EUR pricing for a model id (ok=false if unknown).
func PriceForModel(model string) (costengine.TokenPricing, bool) {
	d, ok := lookup(model)
	if !ok {
		return costengine.TokenPricing{}, false
	}
	return costengine.TokenPricing{
		Currency:         "EUR",
		InputPerMillion:  d.InputUSD * usdToEUR,
		OutputPerMillion: d.OutputUSD * usdToEUR,
	}, true
}

// ZoneForModel returns the default sovereignty zone for a model id ("" if unknown).
func ZoneForModel(model string) string {
	if d, ok := lookup(model); ok {
		return d.Zone
	}
	return ""
}

// ProviderForModel returns the default provider family for a model id ("" if
// unknown). Used to label a flow whose telemetry carried only the model.
func ProviderForModel(model string) string {
	if d, ok := lookup(model); ok {
		return d.Provider
	}
	return ""
}

// AllModelNames returns every provider-side model id in the built-in default
// catalog. Used by callers that need to seed per-model metadata (e.g. routing
// score sovereignty flags) for all well-known models before overlaying user CRs.
func AllModelNames() []string {
	names := make([]string, 0, len(defaultModels))
	for id := range defaultModels {
		names = append(names, id)
	}
	return names
}

// DefaultPriceBook is every default model keyed by model id, ready to seed a
// costengine.PriceBook. Callers overlay user AIModel/AIProvider prices on top.
func DefaultPriceBook() costengine.PriceBook {
	pb := make(costengine.PriceBook, len(defaultModels))
	for id := range defaultModels {
		if p, ok := PriceForModel(id); ok {
			pb[id] = p
		}
	}
	return pb
}

// hostProvider maps a hostname suffix to a provider family and (when the host alone
// determines it) a sovereignty zone. A zone of "" means "derive from the region
// token in the host" (cloud providers) or "unknown — needs AIProvider.Region".
type hostProvider struct {
	provider string
	zone     string // "" => region-derived or unknown
}

// knownHosts matches by hostname suffix (longest match wins). Direct provider APIs
// carry a fixed zone; cloud-hosted ones (Azure/Bedrock/Vertex) leave zone empty so
// it is read from the region token in the host.
var knownHosts = []struct {
	suffix string
	hp     hostProvider
}{
	{"api.openai.com", hostProvider{"openai", "US"}},
	{"api.anthropic.com", hostProvider{"anthropic", "US"}},
	{"api.mistral.ai", hostProvider{"mistral", "EU"}},
	{"api.cohere.ai", hostProvider{"cohere", "US"}},
	{"api.cohere.com", hostProvider{"cohere", "US"}},
	{"api.groq.com", hostProvider{"groq", "US"}},
	{"generativelanguage.googleapis.com", hostProvider{"vertex", "US"}},
	// Cloud-hosted: provider known, zone from region token in host.
	{"openai.azure.com", hostProvider{"azure-openai", ""}},
	{"cognitiveservices.azure.com", hostProvider{"azure-openai", ""}},
	{"services.ai.azure.com", hostProvider{"azure-openai", ""}},
	{"amazonaws.com", hostProvider{"bedrock", ""}},
	{"aiplatform.googleapis.com", hostProvider{"vertex", ""}},
}

// EndpointToZone derives the sovereignty zone and provider family from an endpoint
// host or URL. It returns canonical zone codes (US, EU, FR, ...) via NormalizeZone.
// An unrecognized host yields ("", "") — the caller treats that as a data-quality
// gap rather than guessing. This is what lets sovereignty classify a flow by WHERE
// it goes, even with no catalog entry (and, later, from eBPF-observed SNI).
func EndpointToZone(endpoint string) (zone, provider string) {
	host := hostOf(endpoint)
	if host == "" {
		return "", ""
	}
	best := ""
	var bestHP hostProvider
	for _, kh := range knownHosts {
		// Match at a label boundary. Some providers join the region to the base host
		// with a dash (Vertex: "europe-west4-aiplatform.googleapis.com"), so accept a
		// "-" boundary too, not only ".".
		if host == kh.suffix || strings.HasSuffix(host, "."+kh.suffix) || strings.HasSuffix(host, "-"+kh.suffix) {
			if len(kh.suffix) > len(best) {
				best, bestHP = kh.suffix, kh.hp
			}
		}
	}
	if best == "" {
		return "", ""
	}
	zone = bestHP.zone
	if zone == "" {
		zone = zoneFromRegionToken(host)
	}
	return zone, bestHP.provider
}

// hostOf extracts the bare lowercase host from a URL or host[:port] string.
func hostOf(endpoint string) string {
	h := strings.TrimSpace(strings.ToLower(endpoint))
	if i := strings.Index(h, "://"); i >= 0 {
		h = h[i+3:]
	}
	if i := strings.IndexAny(h, "/?#"); i >= 0 {
		h = h[:i]
	}
	if i := strings.LastIndex(h, "@"); i >= 0 {
		h = h[i+1:]
	}
	if i := strings.LastIndex(h, ":"); i >= 0 {
		h = h[:i]
	}
	return h
}

// regionPrefixes maps a leading cloud-region token to a canonical zone. Matched as a
// "."-delimited label prefix of any host label (so eu-west-1, europe-west4,
// us-east-1, francecentral, eastus, westeurope all resolve).
var regionPrefixes = []struct {
	prefix string
	zone   string
}{
	{"europe", "EU"},
	{"eu-", "EU"},
	{"francecentral", "FR"},
	{"westeurope", "EU"},
	{"northeurope", "EU"},
	{"swedencentral", "EU"},
	{"germanywestcentral", "EU"},
	{"us-", "US"},
	{"eastus", "US"},
	{"westus", "US"},
	{"centralus", "US"},
	{"uksouth", "GB"},
	{"ukwest", "GB"},
	{"uk-", "GB"},
	{"ap-", "AP"},
	{"asia", "AP"},
	{"australia", "AP"},
	{"cn-", "CN"},
	{"china", "CN"},
}

// zoneFromRegionToken scans the host labels for a cloud-region token and returns the
// canonical zone, or "" if none is recognized.
func zoneFromRegionToken(host string) string {
	for _, label := range strings.Split(host, ".") {
		for _, rp := range regionPrefixes {
			if strings.HasPrefix(label, rp.prefix) {
				return sovereigntyengine.NormalizeZone(rp.zone)
			}
		}
	}
	return ""
}
