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

// Package sovereigntyengine evaluates which providers are in use against a
// sovereignty policy (allowed/forbidden zones, sensitive-data rules) and emits
// findings. It is pure (no Kubernetes dependency) and never mutates traffic
// itself; controllers turn its findings into report/warn/enforce actions.
package sovereigntyengine

import (
	"fmt"
	"sort"
	"strings"
)

// Severity levels for findings.
const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

// Finding is a single sovereignty observation. For flow-aware evaluation it is
// attributed to the namespace/application/model whose traffic triggered it and
// carries the number of requests affected; provider-level evaluation leaves
// those fields empty.
type Finding struct {
	Severity    string
	Message     string
	Namespace   string
	Application string
	Model       string
	Provider    string
	Zone        string
	Requests    int64
}

// ProviderInfo is the subset of an AIProvider needed to evaluate sovereignty.
type ProviderInfo struct {
	Name                    string
	Zone                    string // raw dataResidency (e.g. "france", "us")
	Managed                 bool
	AllowedForSensitiveData bool
}

// Flow is one observed traffic flow (a namespace/application calling a model on
// a provider) enriched with the provider/model attributes needed to verify it
// against a sovereignty policy. It is the unit of flow-aware evaluation.
type Flow struct {
	Namespace               string
	Application             string
	Model                   string
	Provider                string
	Zone                    string // raw provider dataResidency
	Managed                 bool
	ProviderAllowsSensitive bool
	ModelAllowsSensitive    bool
	Requests                int64
}

// Policy is the subset of an AISovereigntyPolicy needed for evaluation.
type Policy struct {
	AllowedZones             []string
	ForbiddenZones           []string
	ExternalProvidersAllowed bool
	RequireAnonymization     bool
}

// euCountries are treated as covered when "EU" is an allowed zone.
var euCountries = map[string]bool{
	"FR": true, "DE": true, "ES": true, "IT": true, "NL": true, "BE": true,
	"LU": true, "IE": true, "PT": true, "AT": true, "FI": true, "SE": true,
	"DK": true, "PL": true, "CZ": true, "GR": true, "RO": true, "HU": true,
}

// NormalizeZone maps free-form residency strings to canonical zone codes.
func NormalizeZone(z string) string {
	switch strings.ToLower(strings.TrimSpace(z)) {
	case "fr", "france", "francecentral":
		return "FR"
	case "eu", "europe", "european union":
		return "EU"
	case "us", "usa", "united states", "eastus", "westus":
		return "US"
	case "cn", "china":
		return "CN"
	case "uk", "gb", "united kingdom":
		return "GB"
	case "":
		return ""
	default:
		return strings.ToUpper(strings.TrimSpace(z))
	}
}

func toSet(zones []string) map[string]bool {
	s := make(map[string]bool, len(zones))
	for _, z := range zones {
		s[NormalizeZone(z)] = true
	}
	return s
}

// zoneAllowed reports whether a normalized zone satisfies the allowed set. An EU
// member country is allowed when "EU" is permitted. An empty allowed set imposes
// no positive restriction (only the forbidden set applies).
func zoneAllowed(zone string, allowed map[string]bool) bool {
	if len(allowed) == 0 {
		return true
	}
	if allowed[zone] {
		return true
	}
	if allowed["EU"] && euCountries[zone] {
		return true
	}
	return false
}

// Evaluate returns the findings for the given providers under the policy.
// Findings are sorted by descending severity then provider for stable output.
func Evaluate(policy Policy, providers []ProviderInfo) []Finding {
	allowed := toSet(policy.AllowedZones)
	forbidden := toSet(policy.ForbiddenZones)

	var findings []Finding
	for _, p := range providers {
		zone := NormalizeZone(p.Zone)

		switch {
		case forbidden[zone]:
			findings = append(findings, Finding{
				Severity: SeverityCritical, Provider: p.Name, Zone: zone,
				Message: fmt.Sprintf("Provider %q operates in forbidden zone %s.", p.Name, zone),
			})
		case !zoneAllowed(zone, allowed):
			findings = append(findings, Finding{
				Severity: SeverityWarning, Provider: p.Name, Zone: zone,
				Message: fmt.Sprintf("Provider %q zone %s is outside the allowed zones %v.", p.Name, zone, sortedKeys(allowed)),
			})
		}

		if !policy.ExternalProvidersAllowed && p.Managed {
			findings = append(findings, Finding{
				Severity: SeverityWarning, Provider: p.Name, Zone: zone,
				Message: fmt.Sprintf("External managed provider %q is in use while external providers are disallowed for sensitive data.", p.Name),
			})
		}
	}

	if policy.RequireAnonymization {
		findings = append(findings, Finding{
			Severity: SeverityInfo,
			Message:  "Policy requires prompt anonymization; ensure the gateway redacts sensitive fields before egress.",
		})
	}

	sort.SliceStable(findings, func(i, j int) bool {
		ri, rj := severityRank(findings[i].Severity), severityRank(findings[j].Severity)
		if ri != rj {
			return ri > rj
		}
		return findings[i].Provider < findings[j].Provider
	})
	return findings
}

// EvaluateFlows verifies each observed flow (namespace/application → model →
// provider) against the policy and returns findings attributed to the flow that
// triggered them. Identical findings (same severity/namespace/app/model/provider)
// are aggregated and their request counts summed, so the output is one line per
// distinct violation rather than one per sample. Findings are sorted by
// descending severity, then namespace, application, provider, model.
func EvaluateFlows(policy Policy, flows []Flow) []Finding {
	allowed := toSet(policy.AllowedZones)
	forbidden := toSet(policy.ForbiddenZones)

	agg := map[string]*Finding{}
	var order []string
	add := func(f Finding) {
		key := strings.Join([]string{f.Severity, f.Namespace, f.Application, f.Model, f.Provider, f.Message}, "\x1f")
		if existing, ok := agg[key]; ok {
			existing.Requests += f.Requests
			return
		}
		cp := f
		agg[key] = &cp
		order = append(order, key)
	}

	for _, fl := range flows {
		zone := NormalizeZone(fl.Zone)
		base := Finding{
			Namespace: fl.Namespace, Application: fl.Application,
			Model: fl.Model, Provider: fl.Provider, Zone: zone, Requests: fl.Requests,
		}

		switch {
		case forbidden[zone]:
			f := base
			f.Severity = SeverityCritical
			f.Message = fmt.Sprintf("Namespace %q app %q routed model %q to provider %q in forbidden zone %s.",
				fl.Namespace, fl.Application, fl.Model, fl.Provider, zone)
			add(f)
		case !zoneAllowed(zone, allowed):
			f := base
			f.Severity = SeverityWarning
			f.Message = fmt.Sprintf("Namespace %q app %q routed model %q to provider %q in zone %s, outside allowed zones %v.",
				fl.Namespace, fl.Application, fl.Model, fl.Provider, zone, sortedKeys(allowed))
			add(f)
		}

		// Sensitive-data rule: when the policy forbids external providers for
		// sensitive data, flag flows on a managed external provider that is not
		// cleared for sensitive data (provider or model not allowed).
		if !policy.ExternalProvidersAllowed && fl.Managed && (!fl.ProviderAllowsSensitive || !fl.ModelAllowsSensitive) {
			f := base
			f.Severity = SeverityWarning
			f.Message = fmt.Sprintf("Namespace %q app %q sent traffic (model %q) to external managed provider %q while external providers are disallowed for sensitive data.",
				fl.Namespace, fl.Application, fl.Model, fl.Provider)
			add(f)
		}
	}

	findings := make([]Finding, 0, len(order))
	for _, k := range order {
		findings = append(findings, *agg[k])
	}

	if policy.RequireAnonymization {
		findings = append(findings, Finding{
			Severity: SeverityInfo,
			Message:  "Policy requires prompt anonymization; ensure the gateway redacts sensitive fields before egress.",
		})
	}

	sort.SliceStable(findings, func(i, j int) bool {
		ri, rj := severityRank(findings[i].Severity), severityRank(findings[j].Severity)
		if ri != rj {
			return ri > rj
		}
		if findings[i].Namespace != findings[j].Namespace {
			return findings[i].Namespace < findings[j].Namespace
		}
		if findings[i].Application != findings[j].Application {
			return findings[i].Application < findings[j].Application
		}
		if findings[i].Provider != findings[j].Provider {
			return findings[i].Provider < findings[j].Provider
		}
		return findings[i].Model < findings[j].Model
	})
	return findings
}

// IsZoneAllowed reports whether a provider in the given raw zone is compliant
// under the policy (not forbidden, and within the allowed zones; EU covers EU
// members). Used to decide whether a compliant alternative exists.
func IsZoneAllowed(policy Policy, rawZone string) bool {
	zone := NormalizeZone(rawZone)
	if toSet(policy.ForbiddenZones)[zone] {
		return false
	}
	return zoneAllowed(zone, toSet(policy.AllowedZones))
}

// CountBySeverity tallies findings per severity level.
func CountBySeverity(findings []Finding) map[string]int {
	out := map[string]int{SeverityInfo: 0, SeverityWarning: 0, SeverityCritical: 0}
	for _, f := range findings {
		out[f.Severity]++
	}
	return out
}

func severityRank(s string) int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 1
	default:
		return 0
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
