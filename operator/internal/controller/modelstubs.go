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
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

// autoStubLabel marks an AIModel created automatically for a model seen in
// telemetry but absent from the catalog, so such stubs are easy to list and prune.
const autoStubLabel = "aiops.imperium.io/auto-stub"

// nonDNS matches characters not allowed in an RFC1123 DNS label.
var nonDNS = regexp.MustCompile(`[^a-z0-9-]+`)

// stubName derives a valid AIModel object name from a provider-side model id
// (e.g. "GPT-4o" -> "auto-gpt-4o", "anthropic/claude-3-5" -> "auto-anthropic-claude-3-5").
func stubName(model string) string {
	n := strings.ToLower(model)
	n = nonDNS.ReplaceAllString(n, "-")
	n = strings.Trim(n, "-")
	if n == "" {
		n = "model"
	}
	name := "auto-" + n
	if len(name) > 253 {
		name = name[:253]
	}
	return name
}

// ensureModelStubs creates a labeled, UNPRICED stub AIModel for each model the
// operator cannot price (unknown to both the user catalog and the defaults). This
// turns the data-quality signal into a concrete object the user can complete
// (`kubectl get aimodel -l aiops.imperium.io/auto-stub=true`). The stub references a
// placeholder provider "unknown", so its own reconcile reports NotReady until the
// user fills in a real AIProvider + pricing. Idempotent and best-effort: an existing
// AIModel of the same name is never overwritten.
func ensureModelStubs(ctx context.Context, c client.Client, namespace string, models []string) error {
	for _, m := range models {
		name := stubName(m)
		var existing aiopsv1alpha1.AIModel
		err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &existing)
		if err == nil {
			continue // already present (stub or user-defined) — leave it alone
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
		stub := &aiopsv1alpha1.AIModel{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels:    map[string]string{autoStubLabel: "true"},
			},
			Spec: aiopsv1alpha1.AIModelSpec{
				ProviderRef: "unknown",
				ModelName:   m,
				Type:        "llm",
			},
		}
		if err := c.Create(ctx, stub); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}
