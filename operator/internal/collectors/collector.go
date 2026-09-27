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

// Package collectors defines the TelemetryCollector abstraction used to pull
// LLM usage data from an AI gateway. Implementations live in sub-packages:
// fake (deterministic, for tests/demo), prometheus (text exposition format),
// and litellm (LiteLLM admin API — stubbed in the MVP).
package collectors

import (
	"context"
	"time"
)

// UsageSample is one aggregated usage record over the collection window. It is
// intentionally provider-agnostic so engines do not depend on any gateway.
type UsageSample struct {
	Namespace    string
	Application  string
	Team         string
	Provider     string
	Model        string
	Requests     int64
	InputTokens  int64
	OutputTokens int64
	// LatencyMillis is the mean per-request latency over the window.
	LatencyMillis float64
	Errors        int64
}

// TotalTokens returns the sum of input and output tokens.
func (s UsageSample) TotalTokens() int64 { return s.InputTokens + s.OutputTokens }

// TelemetryCollector pulls usage samples from a telemetry source. Collect must
// be read-only and safe to call repeatedly.
type TelemetryCollector interface {
	// Collect returns usage samples covering roughly the given trailing window.
	Collect(ctx context.Context, window time.Duration) ([]UsageSample, error)
	// Name identifies the collector implementation (for logs/metrics).
	Name() string
}
