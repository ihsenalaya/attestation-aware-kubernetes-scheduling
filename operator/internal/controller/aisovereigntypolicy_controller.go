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
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/enforcementengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

// sovereigntyFinalizer guards a policy's enforcement Prometheus series so they are
// pruned when the policy is deleted (no dead series left behind).
const sovereigntyFinalizer = "aiops.imperium.io/enforcement-metrics"

// AISovereigntyPolicyReconciler reconciles a AISovereigntyPolicy object.
type AISovereigntyPolicyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aisovereigntypolicies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aisovereigntypolicies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aisovereigntypolicies/finalizers,verbs=update
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aigateways;aimodels;aiproviders,verbs=get;list;watch
//+kubebuilder:rbac:groups=aigateway.envoyproxy.io,resources=aigatewayroutes,verbs=get;list;watch;update;patch

// policyToEngine maps the CRD spec to the pure engine policy.
func policyToEngine(p aiopsv1alpha1.AISovereigntyPolicySpec) sovereigntyengine.Policy {
	return sovereigntyengine.Policy{
		AllowedZones:             p.DataResidency.AllowedZones,
		ForbiddenZones:           p.DataResidency.ForbiddenZones,
		ExternalProvidersAllowed: p.SensitiveData.ExternalProvidersAllowed,
		RequireAnonymization:     p.SensitiveData.RequireAnonymization,
	}
}

// Reconcile evaluates providers in use against the policy and records findings.
// reportOnly: nothing is ever blocked.
func (r *AISovereigntyPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var policy aiopsv1alpha1.AISovereigntyPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion: revert any gateway reroute this policy applied, prune its enforcement
	// series, then drop the finalizer (so deleting the policy un-enforces cleanly).
	if !policy.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&policy, sovereigntyFinalizer) {
			if _, err := actuateReroutes(ctx, r.Client, policy.Namespace, nil); err != nil {
				logger.Error(err, "reverting gateway enforcement controls on delete")
			}
			enforcementMetrics.forget(policy.UID)
			shadowMetrics.forget(policy.UID)
			controllerutil.RemoveFinalizer(&policy, sovereigntyFinalizer)
			if err := r.Update(ctx, &policy); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	if controllerutil.AddFinalizer(&policy, sovereigntyFinalizer) {
		if err := r.Update(ctx, &policy); err != nil {
			return ctrl.Result{}, err
		}
	}

	policy.Status.ObservedGeneration = policy.Generation

	cat, err := loadCatalog(ctx, r.Client, policy.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Gateway-INDEPENDENT shadow-AI detection runs FIRST and unconditionally — it must
	// work even with NO AIGateway telemetry source, which is exactly the shadow-AI case:
	// traffic that never touches the gateway. It classifies observed egress (eBPF /
	// Tetragon, surfaced via the shadow-egress ConfigMap) by sovereignty zone.
	shadowFindings := r.detectShadowAI(ctx, policy.UID, policy.Namespace, policyToEngine(policy.Spec))
	if r.Recorder != nil {
		for _, f := range shadowFindings {
			r.Recorder.Eventf(&policy, "Warning", "ShadowAI", "%s", f.Message)
		}
	}

	collector, err := collectorFor(r.Client, policy.Namespace, firstGateway(ctx, r.Client, policy.Namespace))
	if err != nil {
		meta.SetStatusCondition(&policy.Status.Conditions,
			readyFalse(policy.Generation, aiopsv1alpha1.ReasonNoTelemetry, err.Error()))
		_ = r.Status().Update(ctx, &policy)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	samples, err := collector.Collect(ctx, 30*24*time.Hour)
	if err != nil {
		meta.SetStatusCondition(&policy.Status.Conditions,
			readyFalse(policy.Generation, aiopsv1alpha1.ReasonReconcileError, err.Error()))
		_ = r.Status().Update(ctx, &policy)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	findings := sovereigntyengine.EvaluateFlows(policyToEngine(policy.Spec), cat.flows(samples))
	counts := sovereigntyengine.CountBySeverity(findings)

	policy.Status.FindingsCount = int32(len(findings))

	// Flow-aware metric: findings per namespace/application/severity.
	type appSev struct{ ns, app, sev string }
	perApp := map[appSev]int{}
	for _, f := range findings {
		perApp[appSev{f.Namespace, f.Application, f.Severity}]++
	}
	for k, n := range perApp {
		metrics.SovereigntyFindings.WithLabelValues(k.ns, k.app, policy.Name, k.sev).Set(float64(n))
	}

	// Enforcement: turn the critical (forbidden-zone) findings into decisions for
	// this policy's mode. reportOnly records only; warn raises alerts; enforce
	// additionally ACTUATES in the real Envoy AI Gateway data plane (slice 2):
	// each forbidden model's route is rerouted to the compliant backend.
	mode := string(policy.Spec.EnforcementMode)
	fallback := cat.cheapestCompliantModelName(policyToEngine(policy.Spec))
	var violations []enforcementengine.Violation
	for _, f := range findings {
		if f.Severity != sovereigntyengine.SeverityCritical {
			continue
		}
		violations = append(violations, enforcementengine.Violation{
			Namespace: f.Namespace, Application: f.Application, Provider: f.Provider,
			Zone: f.Zone, Model: f.Model, Requests: f.Requests,
		})
	}
	decisions := enforcementengine.DecideSovereignty(mode, violations, fallback)

	// Actuate enforcement decisions at the gateway. In enforce mode, reroute each
	// forbidden model to its compliant target when available; otherwise block its
	// model route. In any other mode both desired sets are empty, so the actuator
	// reverts any route mutation this policy previously applied.
	desiredReroutes := map[string]string{}
	desiredBlocks := map[string]bool{}
	if mode == enforcementengine.ModeEnforce {
		for _, d := range decisions {
			if d.Action == enforcementengine.ActionReroute && d.RerouteTo != "" {
				desiredReroutes[d.Model] = d.RerouteTo
			}
			if d.Action == enforcementengine.ActionBlock {
				desiredBlocks[d.Model] = true
			}
		}
	}
	gwActuated, err := actuateSovereigntyControls(ctx, r.Client, policy.Namespace, desiredReroutes, desiredBlocks)
	if err != nil {
		logger.Error(err, "gateway enforcement actuation failed")
	}

	var enfSeries [][]string
	for _, d := range decisions {
		// A reroute is actuated only once the gateway route was actually patched.
		if d.Action == enforcementengine.ActionReroute {
			d.Actuated = gwActuated.rerouted[d.Model]
		}
		if d.Action == enforcementengine.ActionBlock {
			d.Actuated = gwActuated.blocked[d.Model]
		}
		actuated := "false"
		msg := d.Message
		if d.Actuated {
			actuated = "true"
			if d.Action == enforcementengine.ActionReroute {
				msg = fmt.Sprintf("enforce: %s/%s — rerouted %q to compliant %q at the Envoy AI Gateway",
					d.Namespace, d.Application, d.Model, d.RerouteTo)
			}
			if d.Action == enforcementengine.ActionBlock {
				msg = fmt.Sprintf("enforce: %s/%s — blocked model %q at the Envoy AI Gateway (no compliant fallback)",
					d.Namespace, d.Application, d.Model)
			}
		}
		metrics.EnforcementActions.WithLabelValues(policy.Name, d.Namespace, d.Application, d.Mode, string(d.Action), actuated).Set(1)
		enfSeries = append(enfSeries, []string{policy.Name, d.Namespace, d.Application, d.Mode, string(d.Action), actuated})
		if r.Recorder != nil && d.Action != enforcementengine.ActionReport {
			r.Recorder.Eventf(&policy, "Warning", "Enforcement", "%s", msg)
		}
	}
	enforcementMetrics.retire(policy.UID, enfSeries)

	actionCounts := enforcementengine.CountByAction(decisions)
	meta.SetStatusCondition(&policy.Status.Conditions, readyTrue(policy.Generation,
		fmt.Sprintf("Evaluated (%s): %d finding(s) — %d critical, %d warning; enforcement: %d warn, %d reroute, %d block",
			policy.Spec.EnforcementMode, len(findings),
			counts[sovereigntyengine.SeverityCritical], counts[sovereigntyengine.SeverityWarning],
			actionCounts[enforcementengine.ActionWarn], actionCounts[enforcementengine.ActionReroute], actionCounts[enforcementengine.ActionBlock])))

	if err := r.Status().Update(ctx, &policy); err != nil {
		return ctrl.Result{}, err
	}
	logger.V(1).Info("reconciled AISovereigntyPolicy", "findings", len(findings),
		"mode", mode, "decisions", len(decisions))
	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AISovereigntyPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AISovereigntyPolicy{}).
		Complete(r)
}
