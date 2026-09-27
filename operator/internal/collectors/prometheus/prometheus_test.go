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

package prometheus

import (
	"strings"
	"testing"
)

const sampleMetrics = `
# HELP ai_finops_requests_total requests
# TYPE ai_finops_requests_total counter
ai_finops_requests_total{namespace="rh",team="rh",application="chatbot",provider="az",model="gpt-4o"} 100
ai_finops_input_tokens_total{namespace="rh",team="rh",application="chatbot",provider="az",model="gpt-4o"} 2000000
ai_finops_output_tokens_total{namespace="rh",team="rh",application="chatbot",provider="az",model="gpt-4o"} 1000000
ai_finops_errors_total{namespace="rh",team="rh",application="chatbot",provider="az",model="gpt-4o"} 3
ai_finops_latency_millis{namespace="rh",team="rh",application="chatbot",provider="az",model="gpt-4o"} 842
ai_finops_requests_total{namespace="finance",team="finance",application="risk",provider="oa",model="mistral-small"} 50
`

func TestParse(t *testing.T) {
	samples, err := Parse(strings.NewReader(sampleMetrics))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2", len(samples))
	}
	for _, s := range samples {
		switch s.Model {
		case "gpt-4o":
			if s.Requests != 100 || s.InputTokens != 2_000_000 || s.OutputTokens != 1_000_000 || s.Errors != 3 || s.LatencyMillis != 842 {
				t.Errorf("gpt-4o sample wrong: %+v", s)
			}
			if s.Namespace != "rh" || s.Team != "rh" || s.Application != "chatbot" || s.Provider != "az" {
				t.Errorf("gpt-4o labels wrong: %+v", s)
			}
		case "mistral-small":
			if s.Requests != 50 {
				t.Errorf("mistral requests = %d, want 50", s.Requests)
			}
		default:
			t.Errorf("unexpected model %q", s.Model)
		}
	}
}
