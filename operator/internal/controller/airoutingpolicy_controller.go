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
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/routingscore"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

// defaultMinQualityScore is used when the user does not set guardrails.minQualityScore.
const defaultMinQualityScore = 0.70

// AIRoutingPolicyReconciler evaluates observed usage telemetry to find the best
// routing candidate for each app/model, respecting guardrails. It surfaces
// recommendations in status and can create AIChangeRequest objects for human
// approval when actuation is required.
type AIRoutingPolicyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// Now is injectable only for deterministic calibration freshness tests.
	Now func() time.Time
	// allowDiagnosticConfigMapEvidence is test-only. Production leaves it false:
	// self-declared ConfigMap flags do not prove selected-attempt, authoritative
	// finality, or frozen-split membership and therefore cannot enable non-strict
	// admission.
	allowDiagnosticConfigMapEvidence bool
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airoutingpolicies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airoutingpolicies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airoutingpolicies/finalizers,verbs=update
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aichangerequests,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aigateways;aimodels;aiproviders,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

func (r *AIRoutingPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var policy aiopsv1alpha1.AIRoutingPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.GOVAR = r.reconcileGOVAREvidence(ctx, &policy)

	cat, err := loadCatalog(ctx, r.Client, policy.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	gw := firstGateway(ctx, r.Client, policy.Namespace)
	collector, err := collectorFor(r.Client, policy.Namespace, gw)
	if err != nil {
		apimeta.SetStatusCondition(&policy.Status.Conditions,
			readyFalse(policy.Generation, aiopsv1alpha1.ReasonNoTelemetry, err.Error()))
		_ = r.Status().Update(ctx, &policy)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	samples, err := collector.Collect(ctx, periodWindow("monthly"))
	if err != nil {
		apimeta.SetStatusCondition(&policy.Status.Conditions,
			readyFalse(policy.Generation, aiopsv1alpha1.ReasonReconcileError, err.Error()))
		_ = r.Status().Update(ctx, &policy)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	// Resolve sovereignty policy.
	var pe *sovereigntyengine.Policy
	if sovPolicy := firstSovereigntyPolicy(ctx, r.Client, policy.Namespace); sovPolicy != nil {
		p := policyToEngine(sovPolicy.Spec)
		pe = &p
	}

	scores := routingscore.Compute(samples, cat.priceBook(), cat.routingModelInfo(pe), routingscore.DefaultWeights())

	// Group scores by application, pick the current and the best alternative.
	byApp := map[string][]routingscore.Score{}
	for _, sc := range scores {
		byApp[sc.Application] = append(byApp[sc.Application], sc)
	}

	minQuality := policy.Spec.Guardrails.MinQualityScore
	if minQuality == 0 {
		minQuality = defaultMinQualityScore
	}

	var recs []aiopsv1alpha1.AIRoutingPolicyRecommendation
	for app, scs := range byApp {
		// Sort descending by score.
		sort.SliceStable(scs, func(i, j int) bool { return scs[i].Score > scs[j].Score })
		if len(scs) < 2 {
			continue
		}
		best := scs[0]             // candidate (highest score)
		current := scs[len(scs)-1] // current (lowest score = most expensive / worst)
		if best.Model == current.Model {
			continue
		}
		rec := aiopsv1alpha1.AIRoutingPolicyRecommendation{
			Application:    app,
			CurrentModel:   current.Model,
			CandidateModel: best.Model,
			CandidateScore: strconv.FormatFloat(best.Score, 'f', 3, 64),
		}
		// Guardrail checks.
		var blockReason string
		if policy.Spec.Guardrails.RequireSovereigntyCompliance && !best.SovereigntyCompliant {
			blockReason = fmt.Sprintf("candidate %q is not sovereignty-compliant", best.Model)
		} else if best.Score < minQuality {
			blockReason = fmt.Sprintf("candidate score %.3f below minimum %.3f", best.Score, minQuality)
		} else if policy.Spec.Guardrails.MaxLatencyMillis > 0 && best.LatencyTelemetryAvailable &&
			best.ObservedLatencyMillis > float64(policy.Spec.Guardrails.MaxLatencyMillis) {
			blockReason = fmt.Sprintf("candidate latency %.0fms exceeds max %dms", best.ObservedLatencyMillis, policy.Spec.Guardrails.MaxLatencyMillis)
		}
		if blockReason != "" {
			rec.Blocked = true
			rec.BlockReason = blockReason
		}
		recs = append(recs, rec)
	}

	policy.Status.Recommendations = recs
	now := metav1.Now()
	policy.Status.LastEvaluatedAt = &now
	apimeta.SetStatusCondition(&policy.Status.Conditions, readyCondition(
		policy.Generation, metav1.ConditionTrue, aiopsv1alpha1.ReasonReconciled,
		fmt.Sprintf("Evaluated %d application(s), %d recommendation(s)", len(byApp), len(recs))))

	logger.V(1).Info("AIRoutingPolicy evaluated", "recommendations", len(recs))
	return ctrl.Result{RequeueAfter: 60 * time.Second}, r.Status().Update(ctx, &policy)
}

func (r *AIRoutingPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIRoutingPolicy{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.policiesForCalibrationConfigMap)).
		Complete(r)
}

func (r *AIRoutingPolicyReconciler) policiesForCalibrationConfigMap(ctx context.Context, object client.Object) []reconcile.Request {
	var policies aiopsv1alpha1.AIRoutingPolicyList
	if err := r.List(ctx, &policies, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range policies.Items {
		govar := policies.Items[i].Spec.GOVAR
		if govar == nil || govar.Calibration == nil {
			continue
		}
		if govar.Calibration.CalibrationDataRef == object.GetName() || govar.Calibration.MonitoringDataRef == object.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&policies.Items[i])})
		}
	}
	return requests
}
