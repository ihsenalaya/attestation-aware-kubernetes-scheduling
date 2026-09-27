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

package controller

import (
	"context"
	"encoding/json"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	catalogdefaults "github.com/imperium/ai-sovereign-finops-operator/internal/catalog"
	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
	"github.com/imperium/ai-sovereign-finops-operator/internal/shadowengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

const (
	// shadowEgressConfigMap is the conventional ConfigMap holding gateway-independent
	// egress observations (filled by the Tetragon forwarder on the cluster). Its key
	// shadowEgressKey is a JSON array of egressRecord. Absent → no shadow detection.
	shadowEgressConfigMap = "shadow-egress"
	shadowEgressKey       = "egress.json"
)

// egressRecord is one observed per-workload egress in the shadow-egress ConfigMap.
type egressRecord struct {
	Namespace   string `json:"namespace"`
	Application string `json:"application"`
	Host        string `json:"host"`
	Connections int64  `json:"connections"`
}

// readObservedEgress loads observed egress from the shadow-egress ConfigMap in the
// given namespace. A missing ConfigMap is not an error (returns nil) — shadow
// detection is opt-in via the presence of real eBPF-sourced data.
func readObservedEgress(ctx context.Context, c client.Client, namespace string) ([]shadowengine.Egress, error) {
	var cm corev1.ConfigMap
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: shadowEgressConfigMap}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	raw, ok := cm.Data[shadowEgressKey]
	if !ok || raw == "" {
		return nil, nil
	}
	var recs []egressRecord
	if err := json.Unmarshal([]byte(raw), &recs); err != nil {
		return nil, err
	}
	out := make([]shadowengine.Egress, 0, len(recs))
	for _, r := range recs {
		out = append(out, shadowengine.Egress{
			Namespace: r.Namespace, Application: r.Application,
			Host: r.Host, Connections: r.Connections,
		})
	}
	return out, nil
}

// shadowMetricTracker prunes a policy's own ShadowAIEgress series across reconciles
// (same per-UID rationale as the enforcement/report trackers — no global Reset()).
type shadowMetricTracker struct {
	mu    sync.Mutex
	byUID map[types.UID][][]string
}

var shadowMetrics = &shadowMetricTracker{byUID: map[types.UID][][]string{}}

func (t *shadowMetricTracker) retire(uid types.UID, now [][]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	deleteStale(metrics.ShadowAIEgress, t.byUID[uid], now)
	t.byUID[uid] = now
}

func (t *shadowMetricTracker) forget(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	deleteStale(metrics.ShadowAIEgress, t.byUID[uid], nil)
	delete(t.byUID, uid)
}

// detectShadowAI reads observed egress, classifies it against the policy with the
// default endpoint catalog (gateway-independent), emits ShadowAIEgress for each
// shadow violation, prunes this policy's stale series, and returns the findings so
// the caller can raise Events. A missing source or error yields no findings.
func (r *AISovereigntyPolicyReconciler) detectShadowAI(
	ctx context.Context, uid types.UID, namespace string, policy sovereigntyengine.Policy,
) []shadowengine.Finding {
	egresses, err := readObservedEgress(ctx, r.Client, namespace)
	if err != nil || len(egresses) == 0 {
		shadowMetrics.retire(uid, nil)
		return nil
	}
	findings := shadowengine.Detect(policy, egresses, catalogdefaults.EndpointToZone)

	var series [][]string
	for _, f := range findings {
		labels := []string{f.Namespace, f.Application, f.Zone, f.Provider, f.Severity}
		metrics.ShadowAIEgress.WithLabelValues(labels...).Set(float64(f.Connections))
		series = append(series, labels)
	}
	shadowMetrics.retire(uid, series)
	return findings
}
