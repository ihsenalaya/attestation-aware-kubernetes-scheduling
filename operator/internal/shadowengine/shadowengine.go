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

// Package shadowengine is the gateway-INDEPENDENT sovereignty plane. It classifies
// per-workload network egress (captured by eBPF — Tetragon/Hubble — from connect()
// + TLS SNI, or supplied as observed records) against a sovereignty policy, so the
// operator catches "shadow-AI": a pod talking DIRECTLY to a known LLM endpoint in a
// non-compliant zone, bypassing the governed AI gateway entirely.
//
// It is pure (no K8s/eBPF dependency) so the detection logic is unit-testable; the
// live data source (Tetragon on the cluster) and the host→zone catalog are injected.
package shadowengine

import (
	"fmt"

	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

// Egress is one observed per-workload egress to an external host. It is gateway-
// independent: it reflects what a pod actually connected to (e.g. Tetragon-observed
// connect() + SNI), whether or not that traffic went through the AI gateway.
type Egress struct {
	Namespace   string
	Application string
	Host        string // destination hostname / TLS SNI
	Connections int64
}

// Finding is a shadow-AI sovereignty violation: a workload reaching a known LLM
// endpoint in a non-compliant zone outside the governed gateway.
type Finding struct {
	Namespace   string
	Application string
	Host        string
	Provider    string
	Zone        string
	Severity    string
	Message     string
	Connections int64
}

// ZoneResolver maps a host / TLS SNI to a canonical (zone, provider). Pass
// catalog.EndpointToZone. A return of ("","") means "not a recognised LLM endpoint".
type ZoneResolver func(host string) (zone, provider string)

// Detect classifies each egress whose host is a RECOGNISED LLM endpoint against the
// policy. Egress to unknown hosts is ignored — this is AI-egress detection, not a
// firewall. A forbidden zone yields a critical finding; a zone merely outside the
// allowed set yields a warning; a compliant zone yields nothing. Results are in
// input order (the caller aggregates/sorts as needed).
func Detect(policy sovereigntyengine.Policy, egresses []Egress, resolve ZoneResolver) []Finding {
	forbidden := map[string]bool{}
	for _, z := range policy.ForbiddenZones {
		forbidden[sovereigntyengine.NormalizeZone(z)] = true
	}

	var out []Finding
	for _, e := range egresses {
		if resolve == nil {
			break
		}
		zone, provider := resolve(e.Host)
		if zone == "" && provider == "" {
			continue // not a known LLM endpoint — ignore
		}
		nz := sovereigntyengine.NormalizeZone(zone)
		if sovereigntyengine.IsZoneAllowed(policy, nz) {
			continue // compliant — no shadow violation
		}
		f := Finding{
			Namespace: e.Namespace, Application: e.Application,
			Host: e.Host, Provider: provider, Zone: nz, Connections: e.Connections,
		}
		if forbidden[nz] {
			f.Severity = sovereigntyengine.SeverityCritical
			f.Message = fmt.Sprintf("Shadow-AI: %s/%s connected directly to %q (%s) in FORBIDDEN zone %s, bypassing the gateway.",
				e.Namespace, e.Application, e.Host, provider, nz)
		} else {
			f.Severity = sovereigntyengine.SeverityWarning
			f.Message = fmt.Sprintf("Shadow-AI: %s/%s connected directly to %q (%s) in zone %s, outside the allowed zones, bypassing the gateway.",
				e.Namespace, e.Application, e.Host, provider, nz)
		}
		out = append(out, f)
	}
	return out
}
