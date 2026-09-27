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

// Package prometheus implements a TelemetryCollector that scrapes a Prometheus
// text-exposition endpoint. It reads counters following the ai_finops_* naming
// convention, labelled by namespace/application/team/provider/model. Adapting it
// to native LiteLLM/Envoy metric names is a label/name mapping (future work).
package prometheus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

// Metric names consumed by the collector.
const (
	metricRequests     = "ai_finops_requests_total"
	metricInputTokens  = "ai_finops_input_tokens_total"
	metricOutputTokens = "ai_finops_output_tokens_total"
	metricErrors       = "ai_finops_errors_total"
	metricLatencyMS    = "ai_finops_latency_millis"
)

// Collector scrapes a metrics endpoint exposing ai_finops_* counters.
type Collector struct {
	Endpoint string       // full URL, e.g. http://litellm.default:4000/metrics
	Client   *http.Client // optional; defaults to a 10s-timeout client
}

// New returns a Collector for the given endpoint.
func New(endpoint string) *Collector {
	return &Collector{Endpoint: endpoint, Client: &http.Client{Timeout: 10 * time.Second}}
}

// Name implements collectors.TelemetryCollector.
func (c *Collector) Name() string { return "prometheus" }

type sampleKey struct{ namespace, application, team, provider, model string }

// Collect implements collectors.TelemetryCollector.
func (c *Collector) Collect(ctx context.Context, _ time.Duration) ([]collectors.UsageSample, error) {
	client := c.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", c.Endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("scrape %s: status %d: %s", c.Endpoint, resp.StatusCode, body)
	}
	return Parse(resp.Body)
}

// Parse turns a Prometheus text-exposition stream into usage samples. Exported
// so it can be unit-tested without an HTTP server.
func Parse(r io.Reader) ([]collectors.UsageSample, error) {
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}

	byKey := map[sampleKey]*collectors.UsageSample{}
	get := func(k sampleKey) *collectors.UsageSample {
		s, ok := byKey[k]
		if !ok {
			s = &collectors.UsageSample{
				Namespace: k.namespace, Application: k.application, Team: k.team,
				Provider: k.provider, Model: k.model,
			}
			byKey[k] = s
		}
		return s
	}

	for name, fam := range families {
		for _, m := range fam.GetMetric() {
			k := keyFromLabels(m.GetLabel())
			v := counterValue(m)
			s := get(k)
			switch name {
			case metricRequests:
				s.Requests += int64(v)
			case metricInputTokens:
				s.InputTokens += int64(v)
			case metricOutputTokens:
				s.OutputTokens += int64(v)
			case metricErrors:
				s.Errors += int64(v)
			case metricLatencyMS:
				s.LatencyMillis = v
			}
		}
	}

	out := make([]collectors.UsageSample, 0, len(byKey))
	for _, s := range byKey {
		out = append(out, *s)
	}
	return out, nil
}

func keyFromLabels(labels []*dto.LabelPair) sampleKey {
	var k sampleKey
	for _, l := range labels {
		switch l.GetName() {
		case "namespace":
			k.namespace = l.GetValue()
		case "application", "app":
			k.application = l.GetValue()
		case "team":
			k.team = l.GetValue()
		case "provider":
			k.provider = l.GetValue()
		case "model":
			k.model = l.GetValue()
		}
	}
	return k
}

func counterValue(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	if g := m.GetGauge(); g != nil {
		return g.GetValue()
	}
	if u := m.GetUntyped(); u != nil {
		return u.GetValue()
	}
	return 0
}
