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
	"sync"

	"k8s.io/apimachinery/pkg/types"

	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
)

// enforcementMetricTracker remembers, per AISovereigntyPolicy, the enforcement
// series it last emitted, so a policy prunes exactly its own stale series instead
// of Reset()-ing the shared vector (same rationale as reportMetricTracker). A
// finalizer calls forget() so a deleted policy leaves no dead enforcement series.
type enforcementMetricTracker struct {
	mu    sync.Mutex
	byUID map[types.UID][][]string // tuple = {policy, namespace, application, mode, action, actuated}
}

var enforcementMetrics = &enforcementMetricTracker{byUID: map[types.UID][][]string{}}

// retire prunes the tuples this policy emitted before but not in `now`, then
// records `now`. Callers must already have Set() every series in `now`.
func (t *enforcementMetricTracker) retire(uid types.UID, now [][]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	deleteStale(metrics.EnforcementActions, t.byUID[uid], now)
	t.byUID[uid] = now
}

// forget deletes all of a policy's enforcement series and drops it (on delete).
func (t *enforcementMetricTracker) forget(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	deleteStale(metrics.EnforcementActions, t.byUID[uid], nil)
	delete(t.byUID, uid)
}
