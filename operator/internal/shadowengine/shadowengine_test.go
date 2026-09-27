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

package shadowengine

import (
	"testing"

	"github.com/imperium/ai-sovereign-finops-operator/internal/catalog"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

func TestDetect(t *testing.T) {
	// EU-only policy: US is forbidden, anything outside EU is at least a warning.
	policy := sovereigntyengine.Policy{
		AllowedZones:   []string{"EU"},
		ForbiddenZones: []string{"US"},
	}

	egresses := []Egress{
		{Namespace: "finance", Application: "rogue-script", Host: "api.openai.com", Connections: 12}, // US forbidden → critical
		{Namespace: "marketing", Application: "writer", Host: "api.mistral.ai", Connections: 5},      // EU allowed → none
		{Namespace: "data", Application: "etl", Host: "github.com", Connections: 99},                 // not an LLM endpoint → ignored
	}

	got := Detect(policy, egresses, catalog.EndpointToZone)
	if len(got) != 1 {
		t.Fatalf("expected 1 shadow finding, got %d: %+v", len(got), got)
	}
	f := got[0]
	if f.Severity != sovereigntyengine.SeverityCritical {
		t.Errorf("severity = %q, want critical", f.Severity)
	}
	if f.Zone != "US" || f.Provider != "openai" {
		t.Errorf("zone/provider = %q/%q, want US/openai", f.Zone, f.Provider)
	}
	if f.Namespace != "finance" || f.Application != "rogue-script" || f.Connections != 12 {
		t.Errorf("attribution wrong: %+v", f)
	}
}

func TestDetectWarningOutsideAllowed(t *testing.T) {
	// Allowed FR only, nothing explicitly forbidden: US is outside allowed → warning.
	policy := sovereigntyengine.Policy{AllowedZones: []string{"FR"}}
	got := Detect(policy, []Egress{
		{Namespace: "rh", Application: "bot", Host: "https://api.anthropic.com/v1", Connections: 3},
	}, catalog.EndpointToZone)
	if len(got) != 1 || got[0].Severity != sovereigntyengine.SeverityWarning {
		t.Fatalf("expected one warning, got %+v", got)
	}
}

func TestDetectIgnoresCompliantAndUnknown(t *testing.T) {
	policy := sovereigntyengine.Policy{AllowedZones: []string{"EU", "US"}}
	got := Detect(policy, []Egress{
		{Host: "api.openai.com"}, // US allowed → none
		{Host: "api.mistral.ai"}, // EU allowed → none
		{Host: "example.com"},    // unknown → ignored
	}, catalog.EndpointToZone)
	if len(got) != 0 {
		t.Errorf("expected no findings, got %+v", got)
	}
}
