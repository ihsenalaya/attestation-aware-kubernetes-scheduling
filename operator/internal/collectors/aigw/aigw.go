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

// Package aigw is a TelemetryCollector for the Envoy AI Gateway. It scrapes the
// gateway's OpenTelemetry Prometheus endpoint and reads the real per-request
// token usage emitted as the `gen_ai_client_token_usage` histogram plus observed
// request duration from `gen_ai_server_request_duration_seconds` when present
// (labels gen_ai_request_model + gen_ai_token_type input/output; the histogram
// _sum is the token total, _count the request count). Token usage is real
// (measured by the gateway from provider responses), and latency is only filled
// from the observed duration histogram. The gateway metric carries only the
// model, so the collector attributes each model's usage to the workload that
// consumes it via the AIModel catalog (providerRef + serves{Namespace,
// Application,Team}).
package aigw

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/common/expfmt"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

const (
	tokenMetric    = "gen_ai_client_token_usage"
	durationMetric = "gen_ai_server_request_duration_seconds"
)

// Collector scrapes an Envoy AI Gateway metrics endpoint.
type Collector struct {
	client    client.Client
	namespace string
	endpoint  string // full URL, e.g. http://<svc>.<ns>:1064/metrics
	http      *http.Client
}

// New returns an Envoy AI Gateway collector. namespace is where the AIModel
// catalog used for attribution lives.
func New(c client.Client, namespace, endpoint string) *Collector {
	return &Collector{client: c, namespace: namespace, endpoint: endpoint, http: &http.Client{Timeout: 10 * time.Second}}
}

// Name implements collectors.TelemetryCollector.
func (c *Collector) Name() string { return "aigw" }

type attribution struct{ provider, namespace, application, team string }

// modelAttribution maps each cataloged model name to its provider + workload.
func (c *Collector) modelAttribution(ctx context.Context) map[string]attribution {
	out := map[string]attribution{}
	var models aiopsv1alpha1.AIModelList
	if err := c.client.List(ctx, &models, client.InNamespace(c.namespace)); err != nil {
		return out
	}
	for i := range models.Items {
		m := models.Items[i].Spec
		out[m.ModelName] = attribution{
			provider:    m.ProviderRef,
			namespace:   m.ServesNamespace,
			application: m.ServesApplication,
			team:        m.ServesTeam,
		}
	}
	return out
}

// Collect implements collectors.TelemetryCollector.
func (c *Collector) Collect(ctx context.Context, _ time.Duration) ([]collectors.UsageSample, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", c.endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("scrape %s: status %d: %s", c.endpoint, resp.StatusCode, body)
	}

	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}
	fam, ok := families[tokenMetric]
	if !ok {
		return nil, fmt.Errorf("metric %s not found at %s (no gateway traffic yet?)", tokenMetric, c.endpoint)
	}

	attr := c.modelAttribution(ctx)
	bySample := map[string]*collectors.UsageSample{}
	getSample := func(ns, app, team, provider, model string) *collectors.UsageSample {
		key := ns + "\x1f" + app + "\x1f" + model
		s, ok := bySample[key]
		if !ok {
			s = &collectors.UsageSample{Namespace: ns, Application: app, Team: team, Provider: provider, Model: model}
			bySample[key] = s
		}
		return s
	}
	for _, m := range fam.GetMetric() {
		h := m.GetHistogram()
		if h == nil {
			continue
		}
		var model, provider, tokenType, hdrNS, hdrApp string
		for _, l := range m.GetLabel() {
			switch l.GetName() {
			case "gen_ai_request_model":
				model = l.GetValue()
			case "gen_ai_provider_name":
				provider = l.GetValue()
			case "gen_ai_token_type":
				tokenType = l.GetValue()
			case "k8s_namespace": // from the request header x-greenops-namespace
				hdrNS = l.GetValue()
			case "k8s_app": // from the request header x-greenops-app
				hdrApp = l.GetValue()
			}
		}
		if model == "" || (tokenType != "input" && tokenType != "output") {
			continue // ignore cache_creation_input/cached_input and non-token metrics
		}
		sum := int64(h.GetSampleSum())
		count := int64(h.GetSampleCount())

		// Attribution: prefer the real namespace/app carried by the request header
		// (so two apps in different namespaces on the SAME model are separated);
		// fall back to the AIModel catalog's serves* defaults when the header is
		// absent (e.g. pre-header traffic).
		a := attr[model]
		ns, app, team := hdrNS, hdrApp, a.team
		if ns == "" {
			ns = a.namespace
		}
		if app == "" {
			app = a.application
		}
		// Header-attributed traffic carries no team header; team follows the real
		// namespace so per-team views match per-namespace (no merge into the model's
		// default serves-team).
		if hdrNS != "" {
			team = hdrNS
		}
		prov := a.provider
		if prov == "" {
			prov = provider
		}
		s := getSample(ns, app, team, prov, model)
		switch tokenType {
		case "input":
			s.InputTokens += sum
			if count > s.Requests {
				s.Requests = count
			}
		case "output":
			s.OutputTokens += sum
		}
	}

	if fam, ok := families[durationMetric]; ok {
		for _, m := range fam.GetMetric() {
			h := m.GetHistogram()
			if h == nil || h.GetSampleCount() == 0 {
				continue
			}
			var model, provider, hdrNS, hdrApp string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "gen_ai_request_model":
					model = l.GetValue()
				case "gen_ai_provider_name":
					provider = l.GetValue()
				case "k8s_namespace":
					hdrNS = l.GetValue()
				case "k8s_app":
					hdrApp = l.GetValue()
				}
			}
			if model == "" {
				continue
			}
			a := attr[model]
			ns, app, team := hdrNS, hdrApp, a.team
			if ns == "" {
				ns = a.namespace
			}
			if app == "" {
				app = a.application
			}
			if hdrNS != "" {
				team = hdrNS
			}
			prov := a.provider
			if prov == "" {
				prov = provider
			}
			s := getSample(ns, app, team, prov, model)
			count := int64(h.GetSampleCount())
			if count > s.Requests {
				s.Requests = count
			}
			s.LatencyMillis = h.GetSampleSum() / float64(h.GetSampleCount()) * 1000
		}
	}

	out := make([]collectors.UsageSample, 0, len(bySample))
	for _, s := range bySample {
		out = append(out, *s)
	}
	return out, nil
}
