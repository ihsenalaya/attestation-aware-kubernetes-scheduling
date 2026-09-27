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

package sovereigntyengine

import "testing"

func TestEvaluate(t *testing.T) {
	policy := Policy{
		AllowedZones:             []string{"FR", "EU"},
		ForbiddenZones:           []string{"US", "CN"},
		ExternalProvidersAllowed: false,
		RequireAnonymization:     true,
	}
	providers := []ProviderInfo{
		{Name: "azure-openai-france", Zone: "france", Managed: true, AllowedForSensitiveData: true},
		{Name: "openai-us", Zone: "us", Managed: true, AllowedForSensitiveData: false},
		{Name: "onprem-gpu", Zone: "FR", Managed: false},
	}

	findings := Evaluate(policy, providers)
	counts := CountBySeverity(findings)

	// openai-us in US -> critical (forbidden). Both managed providers -> warning
	// (external disallowed). Anonymization -> info.
	if counts[SeverityCritical] != 1 {
		t.Errorf("critical = %d, want 1 (%+v)", counts[SeverityCritical], findings)
	}
	if counts[SeverityWarning] != 2 {
		t.Errorf("warning = %d, want 2 (%+v)", counts[SeverityWarning], findings)
	}
	if counts[SeverityInfo] != 1 {
		t.Errorf("info = %d, want 1", counts[SeverityInfo])
	}
	// Critical must sort first.
	if findings[0].Severity != SeverityCritical {
		t.Errorf("first finding severity = %s, want critical", findings[0].Severity)
	}
}

func TestEvaluateFlows(t *testing.T) {
	policy := Policy{
		AllowedZones:             []string{"FR", "EU"},
		ForbiddenZones:           []string{"US", "CN"},
		ExternalProvidersAllowed: false,
		RequireAnonymization:     true,
	}
	flows := []Flow{
		// Compliant EU flow, model cleared for sensitive data -> no zone/sensitive finding.
		{Namespace: "rh", Application: "chatbot-rh", Model: "mistral-small", Provider: "azure-fr",
			Zone: "france", Managed: true, ProviderAllowsSensitive: true, ModelAllowsSensitive: true, Requests: 5},
		// Forbidden zone (US) -> critical, attributed to finance/risk-assistant.
		{Namespace: "finance", Application: "risk-assistant", Model: "gpt-4o", Provider: "openai-us",
			Zone: "us", Managed: true, ProviderAllowsSensitive: false, ModelAllowsSensitive: false, Requests: 3},
		// Same forbidden flow again -> aggregated (requests summed), not a second finding.
		{Namespace: "finance", Application: "risk-assistant", Model: "gpt-4o", Provider: "openai-us",
			Zone: "us", Managed: true, ProviderAllowsSensitive: false, ModelAllowsSensitive: false, Requests: 4},
	}

	findings := EvaluateFlows(policy, flows)
	counts := CountBySeverity(findings)

	if counts[SeverityCritical] != 1 {
		t.Errorf("critical = %d, want 1 (%+v)", counts[SeverityCritical], findings)
	}
	if counts[SeverityInfo] != 1 { // anonymization
		t.Errorf("info = %d, want 1", counts[SeverityInfo])
	}
	// Critical sorts first and must carry flow attribution + aggregated requests.
	first := findings[0]
	if first.Severity != SeverityCritical {
		t.Fatalf("first severity = %s, want critical", first.Severity)
	}
	if first.Namespace != "finance" || first.Application != "risk-assistant" || first.Provider != "openai-us" {
		t.Errorf("attribution = %s/%s/%s, want finance/risk-assistant/openai-us", first.Namespace, first.Application, first.Provider)
	}
	if first.Requests != 7 { // 3 + 4 aggregated
		t.Errorf("aggregated requests = %d, want 7", first.Requests)
	}
	// The compliant EU flow must NOT produce a zone/sensitive finding.
	for _, f := range findings {
		if f.Namespace == "rh" {
			t.Errorf("compliant rh flow should not produce a finding: %+v", f)
		}
	}
}

func TestEvaluateFlowsSensitiveExternal(t *testing.T) {
	// External managed provider not cleared for sensitive data, but in an allowed
	// zone: no zone finding, but a sensitive-data warning attributed to the flow.
	policy := Policy{AllowedZones: []string{"EU"}, ExternalProvidersAllowed: false}
	flows := []Flow{
		{Namespace: "rh", Application: "chatbot-rh", Model: "gpt-4o", Provider: "azure-eu",
			Zone: "DE", Managed: true, ProviderAllowsSensitive: false, ModelAllowsSensitive: false, Requests: 2},
	}
	findings := EvaluateFlows(policy, flows)
	if len(findings) != 1 || findings[0].Severity != SeverityWarning {
		t.Fatalf("want 1 warning, got %+v", findings)
	}
	if findings[0].Application != "chatbot-rh" {
		t.Errorf("application = %q, want chatbot-rh", findings[0].Application)
	}
}

func TestZoneAllowedEUCoversFrance(t *testing.T) {
	allowed := toSet([]string{"EU"})
	if !zoneAllowed("FR", allowed) {
		t.Error("FR should be allowed when EU is permitted")
	}
	if zoneAllowed("US", allowed) {
		t.Error("US should not be allowed when only EU is permitted")
	}
}

func TestNormalizeZone(t *testing.T) {
	cases := map[string]string{"france": "FR", "francecentral": "FR", "us": "US", "eastus": "US", "": ""}
	for in, want := range cases {
		if got := NormalizeZone(in); got != want {
			t.Errorf("NormalizeZone(%q) = %q, want %q", in, got, want)
		}
	}
}
