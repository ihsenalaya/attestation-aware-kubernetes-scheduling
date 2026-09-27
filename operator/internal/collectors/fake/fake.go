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

// Package fake provides a deterministic TelemetryCollector for tests and demos.
// It needs no gateway and returns a realistic, fixed usage profile.
package fake

import (
	"context"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

// Collector returns a fixed set of samples. If Samples is nil, a built-in demo
// profile (RH chatbot across two models/providers) is used.
type Collector struct {
	Samples []collectors.UsageSample
}

// New returns a fake collector using the built-in demo profile.
func New() *Collector { return &Collector{Samples: DemoSamples()} }

// DemoSamples is a small offline fallback dataset used ONLY for unit tests and
// when no real telemetry source is configured. The real demo does NOT use this:
// it reads measured usage from a ConfigMap (telemetry mode "configmap") produced
// by the seed-usage tool from live LLM calls. Volumes here are illustrative; the
// providers/models match config/samples (gpt-4o @ openai-us, mistral-large @
// mistral-eu) so sovereignty zones and prices resolve consistently.
func DemoSamples() []collectors.UsageSample {
	return []collectors.UsageSample{
		{
			Namespace: "rh", Application: "chatbot-rh", Team: "rh",
			Provider: "mistral-eu", Model: "mistral-large",
			Requests: 30000, InputTokens: 3_000_000, OutputTokens: 6_000_000,
			LatencyMillis: 820, Errors: 30,
		},
		{
			Namespace: "finance", Application: "risk-assistant", Team: "finance",
			Provider: "openai-us", Model: "gpt-4o",
			Requests: 8000, InputTokens: 2_000_000, OutputTokens: 1_200_000,
			LatencyMillis: 910, Errors: 40,
		},
	}
}

// Collect implements collectors.TelemetryCollector.
func (c *Collector) Collect(_ context.Context, _ time.Duration) ([]collectors.UsageSample, error) {
	out := make([]collectors.UsageSample, len(c.Samples))
	copy(out, c.Samples)
	return out, nil
}

// Name implements collectors.TelemetryCollector.
func (c *Collector) Name() string { return "fake" }
