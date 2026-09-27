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

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"

	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
)

// reportSeriesSet accumulates the exact label-tuples one AIFinOpsReport emits into
// each per-report gauge vector during a single reconcile. The order of values in
// each tuple matches the vector's label names.
type reportSeriesSet struct {
	recommendations  [][]string // {type, namespace, application, severity}
	savingsByApp     [][]string // {namespace, application}
	costSaving       [][]string // {namespace, application, current_model, recommended_model}
	zone             [][]string // {zone}
	measuredLatency  [][]string // {namespace, application, model, source}
	latencyScore     [][]string // {namespace, application, model, telemetry_available}
	routingScore     [][]string // {namespace, application, model, latency_telemetry}
	latencyAvail     [][]string // {namespace, application, model, source}
	costScore        [][]string // {namespace, application, model}
	reliabilityScore [][]string // {namespace, application, model}
	sovereigntyScore [][]string // {namespace, application, model}
}

func (s *reportSeriesSet) addRecommendation(typ, ns, app, sev string) {
	s.recommendations = append(s.recommendations, []string{typ, ns, app, sev})
}
func (s *reportSeriesSet) addSavingsByApp(ns, app string) {
	s.savingsByApp = append(s.savingsByApp, []string{ns, app})
}
func (s *reportSeriesSet) addCostSaving(ns, app, current, recommended string) {
	s.costSaving = append(s.costSaving, []string{ns, app, current, recommended})
}
func (s *reportSeriesSet) addZone(zone string) {
	s.zone = append(s.zone, []string{zone})
}
func (s *reportSeriesSet) addRoutingScore(ns, app, model, telemetry string) {
	s.routingScore = append(s.routingScore, []string{ns, app, model, telemetry})
}
func (s *reportSeriesSet) addLatencyScore(ns, app, model, available string) {
	s.latencyScore = append(s.latencyScore, []string{ns, app, model, available})
}
func (s *reportSeriesSet) addCostScore(ns, app, model string) {
	s.costScore = append(s.costScore, []string{ns, app, model})
}
func (s *reportSeriesSet) addReliabilityScore(ns, app, model string) {
	s.reliabilityScore = append(s.reliabilityScore, []string{ns, app, model})
}
func (s *reportSeriesSet) addSovereigntyScore(ns, app, model string) {
	s.sovereigntyScore = append(s.sovereigntyScore, []string{ns, app, model})
}
func (s *reportSeriesSet) addMeasuredLatency(ns, app, model, source string) {
	s.measuredLatency = append(s.measuredLatency, []string{ns, app, model, source})
}
func (s *reportSeriesSet) addLatencyAvailable(ns, app, model, source string) {
	s.latencyAvail = append(s.latencyAvail, []string{ns, app, model, source})
}

// reportMetricTracker remembers, per report UID, the series each AIFinOpsReport
// last wrote into the per-report gauge vectors it exclusively owns. We use it to
// prune exactly that report's series instead of Reset()-ing the whole vector.
//
// Why not Reset(): a vector-wide Reset() at the start of one report's reconcile
// wipes EVERY report's series, so when two reports overlap (e.g. an all-namespaces
// report plus a namespace-scoped one) they erase each other's series every cycle —
// the dashboard flaps. And nothing cleaned a report's series when it was deleted,
// leaving dead series behind. Tracking per-report tuples fixes both: a report only
// prunes its own disappeared series (a finalizer prunes all of them on delete), and
// persistent series are never deleted so they don't flap.
//
// This tracker covers only the four vectors the report controller is the sole
// writer of. Vectors written by several controllers (e.g. the sovereignty metrics,
// also emitted by the AISovereigntyPolicy controller) are intentionally excluded —
// per-report pruning there would delete another controller's series.
type reportMetricTracker struct {
	mu    sync.Mutex
	byUID map[types.UID]*reportSeriesSet
}

var reportMetrics = &reportMetricTracker{byUID: map[types.UID]*reportSeriesSet{}}

// retire prunes the tuples this report emitted on a previous reconcile but not in
// `now`, then records `now` as the report's current series. Callers must already
// have Set() every series in `now`, so pruning removes only genuinely-disappeared
// series — persistent series are never touched and so never flap.
func (t *reportMetricTracker) retire(uid types.UID, now *reportSeriesSet) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if prev := t.byUID[uid]; prev != nil {
		deleteStale(metrics.Recommendations, prev.recommendations, now.recommendations)
		deleteStale(metrics.PotentialSavingsByAppEUR, prev.savingsByApp, now.savingsByApp)
		deleteStale(metrics.CostSavingEUR, prev.costSaving, now.costSaving)
		deleteStale(metrics.CostByZoneEUR, prev.zone, now.zone)
		deleteStale(metrics.MeasuredLatencyMillis, prev.measuredLatency, now.measuredLatency)
		deleteStale(metrics.LatencyScore, prev.latencyScore, now.latencyScore)
		deleteStale(metrics.RoutingScore, prev.routingScore, now.routingScore)
		deleteStale(metrics.LatencyTelemetryAvailable, prev.latencyAvail, now.latencyAvail)
		deleteStale(metrics.CostScore, prev.costScore, now.costScore)
		deleteStale(metrics.ReliabilityScore, prev.reliabilityScore, now.reliabilityScore)
		deleteStale(metrics.SovereigntyScore, prev.sovereigntyScore, now.sovereigntyScore)
	}
	t.byUID[uid] = now
}

// forget deletes all of a report's series and drops it from the tracker. Called by
// the finalizer when an AIFinOpsReport is deleted so no dead series persist.
func (t *reportMetricTracker) forget(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	prev := t.byUID[uid]
	if prev == nil {
		return
	}
	deleteStale(metrics.Recommendations, prev.recommendations, nil)
	deleteStale(metrics.PotentialSavingsByAppEUR, prev.savingsByApp, nil)
	deleteStale(metrics.CostSavingEUR, prev.costSaving, nil)
	deleteStale(metrics.CostByZoneEUR, prev.zone, nil)
	deleteStale(metrics.MeasuredLatencyMillis, prev.measuredLatency, nil)
	deleteStale(metrics.LatencyScore, prev.latencyScore, nil)
	deleteStale(metrics.RoutingScore, prev.routingScore, nil)
	deleteStale(metrics.LatencyTelemetryAvailable, prev.latencyAvail, nil)
	deleteStale(metrics.CostScore, prev.costScore, nil)
	deleteStale(metrics.ReliabilityScore, prev.reliabilityScore, nil)
	deleteStale(metrics.SovereigntyScore, prev.sovereigntyScore, nil)
	delete(t.byUID, uid)
}

// deleteStale removes every tuple in prev that is not also in now.
func deleteStale(vec *prometheus.GaugeVec, prev, now [][]string) {
	for _, tuple := range prev {
		if containsTuple(now, tuple) {
			continue
		}
		vec.DeleteLabelValues(tuple...)
	}
}

func containsTuple(set [][]string, tuple []string) bool {
	for _, candidate := range set {
		if equalTuple(candidate, tuple) {
			return true
		}
	}
	return false
}

func equalTuple(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
