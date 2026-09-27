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

// Package configmap provides a TelemetryCollector that reads usage samples from a
// Kubernetes ConfigMap. This lets real, measured telemetry (e.g. produced by the
// seed-usage tool from live LLM calls) be fed to the operator without a live
// gateway scrape: the ConfigMap holds a JSON array of usage records under the
// "usage.json" key.
package configmap

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

// DataKey is the ConfigMap key holding the JSON-encoded usage samples.
const DataKey = "usage.json"

// Collector reads usage samples from a ConfigMap.
type Collector struct {
	client    client.Client
	namespace string
	name      string
}

// New returns a configmap collector reading <namespace>/<name>.
func New(c client.Client, namespace, name string) *Collector {
	return &Collector{client: c, namespace: namespace, name: name}
}

// Name implements collectors.TelemetryCollector.
func (c *Collector) Name() string { return "configmap" }

// record mirrors collectors.UsageSample with explicit JSON tags so the on-disk
// format is stable and human-readable.
type record struct {
	Namespace     string  `json:"namespace"`
	Application   string  `json:"application"`
	Team          string  `json:"team"`
	Provider      string  `json:"provider"`
	Model         string  `json:"model"`
	Requests      int64   `json:"requests"`
	InputTokens   int64   `json:"inputTokens"`
	OutputTokens  int64   `json:"outputTokens"`
	LatencyMillis float64 `json:"latencyMillis"`
	Errors        int64   `json:"errors"`
}

// Collect implements collectors.TelemetryCollector. The window is advisory; the
// ConfigMap is expected to already represent the relevant period.
func (c *Collector) Collect(ctx context.Context, _ time.Duration) ([]collectors.UsageSample, error) {
	var cm corev1.ConfigMap
	if err := c.client.Get(ctx, types.NamespacedName{Namespace: c.namespace, Name: c.name}, &cm); err != nil {
		return nil, fmt.Errorf("read usage ConfigMap %s/%s: %w", c.namespace, c.name, err)
	}
	raw, ok := cm.Data[DataKey]
	if !ok {
		return nil, fmt.Errorf("usage ConfigMap %s/%s missing key %q", c.namespace, c.name, DataKey)
	}
	var recs []record
	if err := json.Unmarshal([]byte(raw), &recs); err != nil {
		return nil, fmt.Errorf("parse usage ConfigMap %s/%s: %w", c.namespace, c.name, err)
	}
	out := make([]collectors.UsageSample, 0, len(recs))
	for _, r := range recs {
		out = append(out, collectors.UsageSample{
			Namespace: r.Namespace, Application: r.Application, Team: r.Team,
			Provider: r.Provider, Model: r.Model,
			Requests: r.Requests, InputTokens: r.InputTokens, OutputTokens: r.OutputTokens,
			LatencyMillis: r.LatencyMillis, Errors: r.Errors,
		})
	}
	return out, nil
}
