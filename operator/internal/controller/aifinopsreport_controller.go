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
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
	"github.com/imperium/ai-sovereign-finops-operator/internal/costengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
	"github.com/imperium/ai-sovereign-finops-operator/internal/recommendationengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/reporting"
	"github.com/imperium/ai-sovereign-finops-operator/internal/routingscore"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

// AIFinOpsReportReconciler reconciles a AIFinOpsReport object.
type AIFinOpsReportReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// reportFinalizer guards a report's Prometheus series so they are pruned when the
// report is deleted, instead of lingering as dead data.
const reportFinalizer = "aiops.imperium.io/report-metrics"

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aifinopsreports,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aifinopsreports/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aifinopsreports/finalizers,verbs=update
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aigateways;aimodels;aiproviders,verbs=get;list;watch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aimodels,verbs=create
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch

// periodWindow maps a report period to a collection window.
func periodWindow(period string) time.Duration {
	switch period {
	case "daily":
		return 24 * time.Hour
	case "weekly":
		return 7 * 24 * time.Hour
	default:
		return 30 * 24 * time.Hour
	}
}

// Reconcile collects usage, computes the cost breakdown and writes the report
// results to .status. The MVP is read-only and re-runs periodically.
func (r *AIFinOpsReportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var report aiopsv1alpha1.AIFinOpsReport
	if err := r.Get(ctx, req.NamespacedName, &report); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion: prune this report's Prometheus series, then drop the finalizer.
	if !report.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&report, reportFinalizer) {
			reportMetrics.forget(report.UID)
			controllerutil.RemoveFinalizer(&report, reportFinalizer)
			if err := r.Update(ctx, &report); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	// Ensure the finalizer is present, then continue this same reconcile.
	if controllerutil.AddFinalizer(&report, reportFinalizer) {
		if err := r.Update(ctx, &report); err != nil {
			return ctrl.Result{}, err
		}
	}

	report.Status.ObservedGeneration = report.Generation
	// series accumulates the per-report metric tuples written this reconcile so the
	// tracker can prune exactly this report's stale series (no vector-wide Reset).
	series := &reportSeriesSet{}

	// Resolve the optional gateway to pick the telemetry collector.
	var gw *aiopsv1alpha1.AIGateway
	if report.Spec.GatewayRef != "" {
		var g aiopsv1alpha1.AIGateway
		if err := r.Get(ctx, types.NamespacedName{Namespace: report.Namespace, Name: report.Spec.GatewayRef}, &g); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		} else {
			gw = &g
		}
	}

	cat, err := loadCatalog(ctx, r.Client, report.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Fall back to any gateway in the namespace when no explicit ref is set, so a
	// configured telemetry source (e.g. configmap) is used consistently.
	if gw == nil {
		gw = firstGateway(ctx, r.Client, report.Namespace)
	}
	collector, err := collectorFor(r.Client, report.Namespace, gw)
	if err != nil {
		meta.SetStatusCondition(&report.Status.Conditions,
			readyFalse(report.Generation, aiopsv1alpha1.ReasonNoTelemetry, err.Error()))
		_ = r.Status().Update(ctx, &report)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	samples, err := collector.Collect(ctx, periodWindow(report.Spec.Target.Period))
	if err != nil {
		meta.SetStatusCondition(&report.Status.Conditions,
			readyFalse(report.Generation, aiopsv1alpha1.ReasonReconcileError,
				fmt.Sprintf("telemetry collection failed (%s): %v", collector.Name(), err)))
		_ = r.Status().Update(ctx, &report)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	samples = filterByNamespace(samples, report.Spec.Target.Namespace)

	// Surface models the operator cannot price (unknown to user catalog AND
	// defaults) as labeled stub AIModels, so they appear in `kubectl get aimodel`
	// for the user to complete — the visible counterpart of the data-quality
	// recommendation. Best-effort: a failure here must not block the report.
	if err := ensureModelStubs(ctx, r.Client, report.Namespace, cat.unknownModels(samples)); err != nil {
		logger.Error(err, "creating stub AIModels for unknown models")
	}

	breakdown := costengine.Compute(samples, cat.priceBook())
	r.applyCostToStatus(&report, cat, breakdown, series)
	sovPolicy := firstSovereigntyPolicy(ctx, r.Client, report.Namespace)
	r.applySovereigntyToStatus(ctx, &report, cat, samples, sovPolicy)
	r.applyRecommendations(ctx, &report, cat, samples, breakdown, series)
	r.applyRoutingScores(&report, cat, samples, sovPolicy, series)
	// Prune the series this report emitted before but not now (and never the ones
	// it still emits, so persistent series don't flap). Cleanup on delete is the
	// finalizer's job above.
	reportMetrics.retire(report.UID, series)

	if err := r.writeReportConfigMap(ctx, &report, breakdown, collector.Name()); err != nil {
		logger.Error(err, "failed to write report ConfigMap")
	}

	now := metav1.Now()
	report.Status.GeneratedAt = &now
	meta.SetStatusCondition(&report.Status.Conditions, readyCondition(
		report.Generation, metav1.ConditionTrue, aiopsv1alpha1.ReasonReportGenerated,
		fmt.Sprintf("Report generated from %d usage sample(s) via %s collector", len(samples), collector.Name())))

	if err := r.Status().Update(ctx, &report); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(&report, "Normal", "ReportGenerated",
			"Total cost %.2f %s over %d requests", breakdown.Total.CostTotal, breakdown.Currency, breakdown.Total.Requests)
	}
	logger.V(1).Info("generated AIFinOpsReport",
		"totalCost", breakdown.Total.CostTotal, "samples", len(samples))
	// Refresh periodically so the report tracks ongoing usage.
	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// applyCostToStatus writes the cost breakdown into the report status.
func (r *AIFinOpsReportReconciler) applyCostToStatus(report *aiopsv1alpha1.AIFinOpsReport, cat catalog, b costengine.Breakdown, series *reportSeriesSet) {
	report.Status.TotalCostEUR = moneyQuantityPtr(b.Total.CostTotal)
	report.Status.TotalInputTokens = b.Total.InputTokens
	report.Status.TotalOutputTokens = b.Total.OutputTokens

	// Run-rate forecast: project the observed spend to a full month based on the
	// report's observation window (period). For a monthly period the factor is 1.
	factor := monthlyFactor(report.Spec.Target.Period)
	report.Status.ProjectedMonthlyCostEUR = moneyQuantityPtr(b.Total.CostTotal * factor)

	// Emit usage/cost metrics keyed by the real telemetry namespace (from the
	// cost breakdown), not by the report scope. This keeps sum() correct even
	// when several reports overlap (e.g. an all-namespaces report alongside a
	// namespace-scoped one) — overlapping series resolve to the same per-namespace
	// value (last-writer-wins) instead of double-counting.
	for ns, li := range b.ByNamespace {
		label := ns
		if label == "" {
			label = "(unknown)"
		}
		metrics.RequestsTotal.WithLabelValues(label).Set(float64(li.Requests))
		metrics.InputTokensTotal.WithLabelValues(label).Set(float64(li.InputTokens))
		metrics.OutputTokensTotal.WithLabelValues(label).Set(float64(li.OutputTokens))
		metrics.CostEURTotal.WithLabelValues(label).Set(li.CostTotal)
		metrics.ProjectedMonthlyCostEUR.WithLabelValues(label).Set(li.CostTotal * factor)
	}

	// Spend per sovereignty zone: resolve each model's provider data-residency
	// and sum its cost, so a dashboard can show compliant (EU) vs forbidden (US)
	// spend. Models with no catalog provider fall under "(unknown)". The series are
	// recorded so the tracker prunes only this report's stale zones (no Reset()).
	zoneCost := map[string]float64{}
	for model, li := range b.ByModel {
		zone := cat.zoneForModel(model)
		if zone == "" {
			zone = "(unknown)"
		}
		zoneCost[zone] += li.CostTotal
	}
	for zone, c := range zoneCost {
		metrics.CostByZoneEUR.WithLabelValues(zone).Set(c)
		series.addZone(zone)
	}

	top := costengine.TopByCost(b.ByModel, 5)
	report.Status.TopModels = make([]aiopsv1alpha1.ModelCost, 0, len(top))
	for _, li := range top {
		report.Status.TopModels = append(report.Status.TopModels, aiopsv1alpha1.ModelCost{
			Name:    li.Key,
			CostEUR: moneyQuantity(li.CostTotal),
		})
	}
}

// applySovereigntyToStatus verifies each observed flow (namespace/application →
// model → provider) against the namespace's sovereignty policy and records the
// attributed findings on the report. Findings carry which namespace/app/model/
// provider triggered them and how many requests were affected.
func (r *AIFinOpsReportReconciler) applySovereigntyToStatus(_ context.Context, report *aiopsv1alpha1.AIFinOpsReport, cat catalog, samples []collectors.UsageSample, policy *aiopsv1alpha1.AISovereigntyPolicy) {
	if policy == nil {
		return
	}
	findings := sovereigntyengine.EvaluateFlows(policyToEngine(policy.Spec), cat.flows(samples))
	report.Status.SovereigntyFindings = make([]aiopsv1alpha1.SovereigntyFinding, 0, len(findings))
	for _, f := range findings {
		report.Status.SovereigntyFindings = append(report.Status.SovereigntyFindings, aiopsv1alpha1.SovereigntyFinding{
			Severity:    aiopsv1alpha1.Severity(f.Severity),
			Message:     f.Message,
			Namespace:   f.Namespace,
			Application: f.Application,
			Model:       f.Model,
			Provider:    f.Provider,
			Zone:        f.Zone,
			Requests:    f.Requests,
		})
	}

	// Flow-aware metrics: findings count AND requests-at-risk per
	// namespace/application/severity (the real volume violating the policy).
	type key struct{ ns, app, sev string }
	counts := map[key]int{}
	reqs := map[key]int64{}
	for _, f := range findings {
		k := key{f.Namespace, f.Application, f.Severity}
		counts[k]++
		reqs[k] += f.Requests
	}
	for k, n := range counts {
		metrics.SovereigntyFindings.WithLabelValues(k.ns, k.app, policy.Name, k.sev).Set(float64(n))
		metrics.SovereigntyRequests.WithLabelValues(k.ns, k.app, policy.Name, k.sev).Set(float64(reqs[k]))
	}
}

// applyRecommendations runs the recommendation engine on the observed usage,
// catalog and sovereignty findings, producing quantified, actionable suggestions
// (cost-saving model swaps with EUR savings, sovereignty remediation, data quality).
func (r *AIFinOpsReportReconciler) applyRecommendations(ctx context.Context, report *aiopsv1alpha1.AIFinOpsReport, cat catalog, samples []collectors.UsageSample, b costengine.Breakdown, series *reportSeriesSet) {
	pb := cat.priceBook()

	// Usage per (namespace, application, model) with its computed cost.
	type uk struct{ ns, app, model string }
	um := map[uk]*recommendationengine.Usage{}
	for _, s := range samples {
		k := uk{s.Namespace, s.Application, s.Model}
		u := um[k]
		if u == nil {
			u = &recommendationengine.Usage{Namespace: s.Namespace, Application: s.Application, Model: s.Model}
			um[k] = u
		}
		u.InputTokens += s.InputTokens
		u.OutputTokens += s.OutputTokens
		u.Requests += s.Requests
	}
	var usages []recommendationengine.Usage
	costByApp := map[[2]string]float64{}
	for _, u := range um {
		if p, ok := pb[u.Model]; ok {
			u.CostEUR = float64(u.InputTokens)/1e6*p.InputPerMillion + float64(u.OutputTokens)/1e6*p.OutputPerMillion
		}
		usages = append(usages, *u)
		costByApp[[2]string{u.Namespace, u.Application}] += u.CostEUR
	}

	// The sovereignty policy was already loaded upstream; pass it down so the
	// recommendation engine can flag compliant vs forbidden candidate models.
	var pe sovereigntyengine.Policy
	hasPolicy := false
	if sovPolicy := firstSovereigntyPolicy(ctx, r.Client, report.Namespace); sovPolicy != nil {
		pe = policyToEngine(sovPolicy.Spec)
		hasPolicy = true
	}

	// Candidate models (anything priced in the catalog), each flagged compliant when
	// its provider sits in a sovereignty-allowed zone (always so when no policy is
	// set). The engine refuses to recommend routing to a non-compliant model — so a
	// cost-saving swap can never push traffic into a forbidden zone.
	var candidates []recommendationengine.Candidate
	hasCompliant := false
	for name, p := range pb {
		compliant := !hasPolicy || sovereigntyengine.IsZoneAllowed(pe, cat.zoneForModel(name))
		if compliant {
			hasCompliant = true
		}
		candidates = append(candidates, recommendationengine.Candidate{
			Name: name, InputPerMillion: p.InputPerMillion, OutputPerMillion: p.OutputPerMillion,
			Compliant: compliant,
		})
	}

	// Sovereignty risks from the critical findings already on the report.
	var risks []recommendationengine.Risk
	for _, f := range report.Status.SovereigntyFindings {
		if string(f.Severity) != "critical" {
			continue
		}
		risks = append(risks, recommendationengine.Risk{
			Namespace: f.Namespace, Application: f.Application, Provider: f.Provider, Zone: f.Zone,
			Requests: f.Requests, CostEUR: costByApp[[2]string{f.Namespace, f.Application}],
		})
	}

	recs, total := recommendationengine.Recommend(usages, candidates, risks, b.UnpricedModels, hasCompliant)

	report.Status.Recommendations = make([]aiopsv1alpha1.Recommendation, 0, len(recs))
	// No vector-wide Reset() here: every series is recorded into `series` and the
	// tracker prunes exactly this report's stale series after the reconcile, so two
	// overlapping reports no longer wipe each other (see reportMetricTracker).
	savingsByApp := map[[2]string]float64{}
	for _, rc := range recs {
		item := aiopsv1alpha1.Recommendation{
			Type: rc.Type, Message: rc.Message, Severity: rc.Severity,
			Namespace: rc.Namespace, Application: rc.Application,
		}
		if rc.EstimatedSavingsEUR > 0 {
			item.EstimatedSavingsEUR = moneyQuantityPtr(rc.EstimatedSavingsEUR)
		}
		report.Status.Recommendations = append(report.Status.Recommendations, item)
		// One metric series per recommendation, labelled by its target workload so
		// a dashboard can list each action (app / type / severity) — not just a
		// count per type. Each Recommend() output is already a distinct workload
		// suggestion, so Set(1) per series.
		metrics.Recommendations.WithLabelValues(rc.Type, rc.Namespace, rc.Application, rc.Severity).Set(1)
		series.addRecommendation(rc.Type, rc.Namespace, rc.Application, rc.Severity)
		if rc.EstimatedSavingsEUR > 0 {
			// Accumulate per app locally, then Set() once below — an idempotent value
			// the tracker can prune, unlike a running Add() that would only stay bounded
			// thanks to a Reset().
			savingsByApp[[2]string{rc.Namespace, rc.Application}] += rc.EstimatedSavingsEUR
		}
		// Concrete cost-saving action (current → recommended model) with its gain,
		// so the dashboard shows what to do and what it saves.
		if rc.Type == recommendationengine.TypeCostSaving {
			metrics.CostSavingEUR.WithLabelValues(rc.Namespace, rc.Application, rc.CurrentModel, rc.RecommendedModel).Set(rc.EstimatedSavingsEUR)
			series.addCostSaving(rc.Namespace, rc.Application, rc.CurrentModel, rc.RecommendedModel)
		}
	}
	for app, saving := range savingsByApp {
		metrics.PotentialSavingsByAppEUR.WithLabelValues(app[0], app[1]).Set(saving)
		series.addSavingsByApp(app[0], app[1])
	}
	metrics.PotentialSavingsEUR.Set(total)
}

func (r *AIFinOpsReportReconciler) applyRoutingScores(report *aiopsv1alpha1.AIFinOpsReport, cat catalog, samples []collectors.UsageSample, sovPolicy *aiopsv1alpha1.AISovereigntyPolicy, series *reportSeriesSet) {
	var pe *sovereigntyengine.Policy
	if sovPolicy != nil {
		p := policyToEngine(sovPolicy.Spec)
		pe = &p
	}
	scores := routingscore.Compute(samples, cat.priceBook(), cat.routingModelInfo(pe), routingscore.DefaultWeights())
	report.Status.RoutingScores = make([]aiopsv1alpha1.RoutingScore, 0, len(scores))
	report.Status.LatencyTelemetryAvailable = false

	for _, sc := range scores {
		if sc.LatencyTelemetryAvailable {
			report.Status.LatencyTelemetryAvailable = true
		}
		report.Status.RoutingScores = append(report.Status.RoutingScores, aiopsv1alpha1.RoutingScore{
			Namespace:                 sc.Namespace,
			Application:               sc.Application,
			Provider:                  sc.Provider,
			Model:                     sc.Model,
			Requests:                  sc.Requests,
			Score:                     decimalString(sc.Score, 3),
			CostScore:                 decimalString(sc.CostScore, 3),
			QualityScore:              decimalString(sc.QualityScore, 3),
			LatencyScore:              decimalString(sc.LatencyScore, 3),
			ReliabilityScore:          decimalString(sc.ReliabilityScore, 3),
			CostEUR:                   decimalString(sc.CostEUR, 6),
			CostPerRequestEUR:         decimalString(sc.CostPerRequestEUR, 6),
			ObservedLatencyMillis:     decimalString(sc.ObservedLatencyMillis, 3),
			LatencyTelemetryAvailable: sc.LatencyTelemetryAvailable,
			LatencySource:             sc.LatencySource,
			SovereigntyCompliant:      sc.SovereigntyCompliant,
		})

		available := boolMetricLabel(sc.LatencyTelemetryAvailable)
		metrics.RoutingScore.WithLabelValues(sc.Namespace, sc.Application, sc.Model, available).Set(sc.Score)
		series.addRoutingScore(sc.Namespace, sc.Application, sc.Model, available)
		metrics.LatencyScore.WithLabelValues(sc.Namespace, sc.Application, sc.Model, available).Set(sc.LatencyScore)
		series.addLatencyScore(sc.Namespace, sc.Application, sc.Model, available)

		metrics.CostScore.WithLabelValues(sc.Namespace, sc.Application, sc.Model).Set(sc.CostScore)
		series.addCostScore(sc.Namespace, sc.Application, sc.Model)
		metrics.ReliabilityScore.WithLabelValues(sc.Namespace, sc.Application, sc.Model).Set(sc.ReliabilityScore)
		series.addReliabilityScore(sc.Namespace, sc.Application, sc.Model)
		sovScore := 0.0
		if sc.SovereigntyCompliant {
			sovScore = 1.0
		}
		metrics.SovereigntyScore.WithLabelValues(sc.Namespace, sc.Application, sc.Model).Set(sovScore)
		series.addSovereigntyScore(sc.Namespace, sc.Application, sc.Model)

		latAvail := 0.0
		if sc.LatencyTelemetryAvailable {
			latAvail = 1.0
			metrics.MeasuredLatencyMillis.WithLabelValues(sc.Namespace, sc.Application, sc.Model, sc.LatencySource).Set(sc.ObservedLatencyMillis)
			series.addMeasuredLatency(sc.Namespace, sc.Application, sc.Model, sc.LatencySource)
		}
		metrics.LatencyTelemetryAvailable.WithLabelValues(sc.Namespace, sc.Application, sc.Model, sc.LatencySource).Set(latAvail)
		series.addLatencyAvailable(sc.Namespace, sc.Application, sc.Model, sc.LatencySource)
	}
}

func boolMetricLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func decimalString(v float64, decimals int) string {
	return strconv.FormatFloat(v, 'f', decimals, 64)
}

// writeReportConfigMap renders the report to Markdown + JSON and upserts a
// ConfigMap named "<report>-report" owned by the report (GC'd with it).
func (r *AIFinOpsReportReconciler) writeReportConfigMap(ctx context.Context, report *aiopsv1alpha1.AIFinOpsReport, b costengine.Breakdown, collectorName string) error {
	generatedAt := time.Now()
	if report.Status.GeneratedAt != nil {
		generatedAt = report.Status.GeneratedAt.Time
	}
	data := reporting.Data{
		Name:             report.Name,
		Namespace:        report.Spec.Target.Namespace,
		Period:           report.Spec.Target.Period,
		GeneratedAt:      generatedAt,
		Collector:        collectorName,
		Breakdown:        b,
		ProjectedMonthly: b.Total.CostTotal * monthlyFactor(report.Spec.Target.Period),
		Sovereignty:      report.Status.SovereigntyFindings,
		Recommends:       report.Status.Recommendations,
		RoutingScores:    report.Status.RoutingScores,
	}
	md := reporting.RenderMarkdown(data)
	jsonBytes, err := reporting.RenderJSON(data)
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      report.Name + "-report",
			Namespace: report.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels["app.kubernetes.io/managed-by"] = "ai-sovereign-finops-operator"
		cm.Labels["aiops.imperium.io/report"] = report.Name
		cm.Data = map[string]string{
			"report.md":   md,
			"report.json": string(jsonBytes),
		}
		return controllerutil.SetControllerReference(report, cm, r.Scheme)
	})
	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *AIFinOpsReportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIFinOpsReport{}).
		Complete(r)
}
