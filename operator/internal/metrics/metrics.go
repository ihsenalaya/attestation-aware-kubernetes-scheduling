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

// Package metrics registers the ai_finops_* Prometheus metrics on the
// controller-runtime registry (exposed via the manager's /metrics endpoint).
//
// Every value here is a derived aggregate over a reporting window, set from the
// latest reconcile — i.e. a gauge, not a monotonic counter. The names therefore
// avoid the _total suffix (Prometheus convention: _total == counter); using
// _total on a gauge silently breaks rate()/increase() and confuses tooling. The
// cumulative gateway-side counters keep _total in their own exporters; these
// operator-side snapshots do not.
package metrics

import (
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// RequestsTotal is the request volume attributed to a namespace.
	RequestsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_requests",
		Help: "LLM requests attributed to a namespace over the reporting window.",
	}, []string{"namespace"})

	// InputTokensTotal is the input token volume per namespace.
	InputTokensTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_input_tokens",
		Help: "Input tokens attributed to a namespace over the reporting window.",
	}, []string{"namespace"})

	// OutputTokensTotal is the output token volume per namespace.
	OutputTokensTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_output_tokens",
		Help: "Output tokens attributed to a namespace over the reporting window.",
	}, []string{"namespace"})

	// CostEURTotal is the spend in EUR per namespace.
	CostEURTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_cost_eur",
		Help: "Spend in EUR actually observed for a namespace over the reporting window.",
	}, []string{"namespace"})

	// CostByZoneEUR is the spend in EUR per sovereignty zone (e.g. EU vs US),
	// resolved from each model's provider data residency. It surfaces how much
	// spend stays in a compliant zone versus leaves it.
	CostByZoneEUR = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_cost_by_zone_eur",
		Help: "Spend in EUR per sovereignty zone (provider data-residency) over the reporting window.",
	}, []string{"zone"})

	// PotentialSavingsEUR is the total estimated saving from cost-saving
	// recommendations over the observation window.
	PotentialSavingsEUR = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ai_finops_potential_savings_eur",
		Help: "Total estimated saving from cost-saving recommendations over the observation window.",
	})

	// PotentialSavingsByAppEUR is the estimated cost-saving per namespace/app,
	// so a dashboard can show which workload each saving belongs to.
	PotentialSavingsByAppEUR = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_potential_savings_by_app_eur",
		Help: "Estimated cost-saving in EUR per namespace/application over the observation window.",
	}, []string{"namespace", "application"})

	// CostSavingEUR is the estimated EUR saving of one concrete cost-saving model
	// swap, labelled with the workload and the current/recommended models — so a
	// dashboard can show the actual action ("gpt-4o → gpt-4o-mini") and its gain.
	CostSavingEUR = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_cost_saving_eur",
		Help: "Estimated EUR saving of a cost-saving recommendation, by namespace/application and current/recommended model.",
	}, []string{"namespace", "application", "current_model", "recommended_model"})

	// MeasuredLatencyMillis is the observed mean gateway latency per
	// namespace/application/model over the reporting window. It is set only when
	// the configured telemetry source provides real latency.
	MeasuredLatencyMillis = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_measured_latency_millis",
		Help: "Observed mean LLM gateway latency in milliseconds, by namespace/application/model, over the reporting window.",
	}, []string{"namespace", "application", "model", "source"})

	// LatencyScore is the higher-is-better latency component used by the runtime
	// routing score. When telemetry is unavailable, it is a neutral score and the
	// telemetry_available label is false.
	LatencyScore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_latency_score",
		Help: "Higher-is-better runtime latency score by namespace/application/model; neutral when measured latency is unavailable.",
	}, []string{"namespace", "application", "model", "telemetry_available"})

	// RoutingScore is the final higher-is-better runtime governance score per
	// observed namespace/application/model.
	RoutingScore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_routing_score",
		Help: "Higher-is-better runtime governance score by namespace/application/model over the reporting window.",
	}, []string{"namespace", "application", "model", "latency_telemetry"})

	// CostScore is the cost dimension of the routing score (higher = cheaper).
	CostScore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_cost_score",
		Help: "Higher-is-better cost dimension of the routing score (0=most expensive, 1=cheapest) by namespace/application/model.",
	}, []string{"namespace", "application", "model"})

	// QualityScore is the audited AI Quality Score emitted by AIQualityGate.
	QualityScore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_quality_score",
		Help: "Audited AI quality score in [0,100], by namespace, app, provider, model and score dimension.",
	}, []string{"namespace", "app", "provider", "model", "dimension"})

	// ReliabilityScore is the reliability dimension (1 - error_rate).
	ReliabilityScore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_reliability_score",
		Help: "Reliability dimension of the routing score (1 - error_rate) by namespace/application/model.",
	}, []string{"namespace", "application", "model"})

	// SovereigntyScore is 1 when the model is sovereignty-compliant, 0 when it is not.
	SovereigntyScore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_sovereignty_score",
		Help: "Sovereignty compliance for a model (1=compliant, 0=non-compliant/blocked) by namespace/application/model.",
	}, []string{"namespace", "application", "model"})

	// LatencyTelemetryAvailable is 1 when latency was measured for a
	// namespace/application/model and 0 when the score used a neutral component.
	LatencyTelemetryAvailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_latency_telemetry_available",
		Help: "Whether real latency telemetry was available for a namespace/application/model routing score (1=true, 0=false).",
	}, []string{"namespace", "application", "model", "source"})

	// QualityGatePassed is 1 when an AIQualityGate passes for a candidate model,
	// 0 when it is pending or failed. Labels keep the decision attributable to the
	// protected app and the exact source/candidate model pair.
	QualityGatePassed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_quality_gate_passed",
		Help: "Whether an AIQualityGate passed for a target application and source/candidate model pair (1=true, 0=false).",
	}, []string{"namespace", "quality_gate", "target_namespace", "application", "source_model", "candidate_model"})

	// QualityGateFailedChecks is the number of failed AIQualityGate checks for
	// the latest reconcile. Missing telemetry keeps the gate pending and is
	// explained in status.failureMessages, but does not count as a failed check.
	QualityGateFailedChecks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_quality_gate_failed_checks",
		Help: "Number of failed checks for an AIQualityGate.",
	}, []string{"namespace", "quality_gate", "target_namespace", "application"})

	// QualityGateScore is the audited composite gate score in [0,1] by gate.
	QualityGateScore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_quality_gate_score",
		Help: "Audited composite AIQualityGate score in [0,1] by gate.",
	}, []string{"namespace", "quality_gate", "target_namespace", "application"})

	// SimulatedRuntimeClassInUse indicates a pod/policy is using a simulated runtime class.
	SimulatedRuntimeClassInUse = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_simulated_runtimeclass_in_use",
		Help: "Whether a confidential pod policy is currently using a simulated runtime class mapping.",
	}, []string{"namespace", "policy", "runtimeclass"})

	// ProjectedMonthlyCostEUR is the run-rate forecast of monthly spend per namespace.
	ProjectedMonthlyCostEUR = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_projected_monthly_cost_eur",
		Help: "Run-rate forecast of full-month spend in EUR per namespace (observed x 30.4/window-days).",
	}, []string{"namespace"})

	// BudgetUsagePercent is the percent of budget consumed per policy.
	BudgetUsagePercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_budget_usage_percent",
		Help: "Percent of budget consumed for an AIBudgetPolicy.",
	}, []string{"namespace", "policy"})

	// SovereigntyFindings is the count of findings per policy, namespace,
	// application and severity (flow-aware attribution).
	SovereigntyFindings = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_sovereignty_findings",
		Help: "Sovereignty findings detected for an AISovereigntyPolicy, by namespace, application and severity.",
	}, []string{"namespace", "application", "policy", "severity"})

	// SovereigntyRequests is the number of requests affected by sovereignty
	// findings, by namespace, application and severity (the real volume at risk).
	SovereigntyRequests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_sovereignty_requests",
		Help: "Requests affected by sovereignty findings, by namespace, application and severity.",
	}, []string{"namespace", "application", "policy", "severity"})

	// BreakevenSavingsEUR is the estimated monthly savings of self-hosting.
	BreakevenSavingsEUR = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_breakeven_savings_eur",
		Help: "Estimated monthly EUR savings from self-hosting for an AIBreakEvenAnalysis.",
	}, []string{"namespace", "analysis"})

	// Recommendations is the number of recommendations emitted, by type and (when
	// the recommendation targets a workload) namespace, application and severity.
	// The per-app labels let a dashboard list each actionable recommendation with
	// its owner instead of only a count per type.
	Recommendations = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_recommendations",
		Help: "Recommendations emitted by the operator, by type, namespace, application and severity.",
	}, []string{"type", "namespace", "application", "severity"})

	// EnforcementActions is the enforcement decision taken for a policy violation,
	// by policy, namespace, application, mode (reportOnly/warn/enforce), action
	// (report/warn/block/reroute) and whether it was actually actuated end-to-end
	// (warn/report are immediate; block/reroute reflect gateway mutation success).
	// This turns the operator from an observer into a control plane: a dashboard or
	// alert can see what the operator decided to do about each non-compliant workload.
	EnforcementActions = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_enforcement_actions",
		Help: "Enforcement decisions by policy, namespace, application, mode, action and actuated.",
	}, []string{"policy", "namespace", "application", "mode", "action", "actuated"})

	// ShadowAIEgress is gateway-INDEPENDENT sovereignty: per-workload egress to a
	// known LLM endpoint observed directly (eBPF/Tetragon) in a non-compliant zone,
	// i.e. shadow-AI that bypasses the governed gateway. The value is the observed
	// connection count for that namespace/app/zone/provider at the given severity.
	ShadowAIEgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ai_finops_shadow_ai_egress",
		Help: "Shadow-AI egress (connections) to known LLM endpoints bypassing the gateway, by namespace, application, zone, provider and severity.",
	}, []string{"namespace", "application", "zone", "provider", "severity"})
)

// all is every collector this package owns. init() registers them and Names()
// derives their metric names — so the set of exposed names has a single source of
// truth (used by the dashboard-consistency test to catch metric/dashboard drift).
var all = []prometheus.Collector{
	RequestsTotal,
	InputTokensTotal,
	OutputTokensTotal,
	CostEURTotal,
	CostByZoneEUR,
	PotentialSavingsEUR,
	PotentialSavingsByAppEUR,
	CostSavingEUR,
	MeasuredLatencyMillis,
	LatencyScore,
	RoutingScore,
	CostScore,
	QualityScore,
	ReliabilityScore,
	SovereigntyScore,
	LatencyTelemetryAvailable,
	QualityGatePassed,
	QualityGateFailedChecks,
	QualityGateScore,
	SimulatedRuntimeClassInUse,
	ProjectedMonthlyCostEUR,
	BudgetUsagePercent,
	SovereigntyFindings,
	SovereigntyRequests,
	BreakevenSavingsEUR,
	Recommendations,
	EnforcementActions,
	ShadowAIEgress,
}

func init() {
	crmetrics.Registry.MustRegister(all...)
}

var fqNameRe = regexp.MustCompile(`fqName: "([^"]+)"`)

// Names returns the fully-qualified names of every metric this package registers.
// It Describe()s each collector and extracts the fqName, so it stays correct
// automatically when metrics are added, removed or renamed.
func Names() []string {
	var names []string
	for _, c := range all {
		ch := make(chan *prometheus.Desc, 4)
		c.Describe(ch)
		close(ch)
		for d := range ch {
			if m := fqNameRe.FindStringSubmatch(d.String()); m != nil {
				names = append(names, m[1])
			}
		}
	}
	return names
}
