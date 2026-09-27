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
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/breakevenengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/costengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
)

// AIBreakEvenAnalysisReconciler reconciles a AIBreakEvenAnalysis object.
type AIBreakEvenAnalysisReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aibreakevenanalyses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aibreakevenanalyses/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aibreakevenanalyses/finalizers,verbs=update
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aigateways;aimodels;aiproviders,verbs=get;list;watch

func qtyOrZero(q *resource.Quantity) float64 {
	if q == nil {
		return 0
	}
	return q.AsApproximateFloat64()
}

// Reconcile computes the managed vs self-hosted monthly costs and the payback
// point, then writes the recommendation to status.
func (r *AIBreakEvenAnalysisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var analysis aiopsv1alpha1.AIBreakEvenAnalysis
	if err := r.Get(ctx, req.NamespacedName, &analysis); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	analysis.Status.ObservedGeneration = analysis.Generation

	cat, err := loadCatalog(ctx, r.Client, analysis.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	collector, err := collectorFor(r.Client, analysis.Namespace, firstGateway(ctx, r.Client, analysis.Namespace))
	if err != nil {
		meta.SetStatusCondition(&analysis.Status.Conditions,
			readyFalse(analysis.Generation, aiopsv1alpha1.ReasonNoTelemetry, err.Error()))
		_ = r.Status().Update(ctx, &analysis)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	samples, err := collector.Collect(ctx, periodWindow("monthly"))
	if err != nil {
		meta.SetStatusCondition(&analysis.Status.Conditions,
			readyFalse(analysis.Generation, aiopsv1alpha1.ReasonReconcileError, err.Error()))
		_ = r.Status().Update(ctx, &analysis)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	// Cost of the current model over the analysis window, extrapolated to a month.
	target := aiopsv1alpha1.BudgetTarget{
		Namespace:   analysis.Spec.Target.Namespace,
		Application: analysis.Spec.Target.Application,
	}
	scoped := filterByBudgetTarget(samples, target)
	breakdown := costengine.Compute(scoped, cat.priceBook())

	var windowCost, providerFixed float64
	if model, ok := cat.modelByName(analysis.Spec.CurrentModelRef); ok {
		windowCost = breakdown.ByModel[model.Spec.ModelName].CostTotal
		providerFixed = cat.providerFixedMonthly(model.Spec.ProviderRef)
	}
	managedMonthly := breakevenengine.ExtrapolateMonthly(windowCost, analysis.Spec.AnalysisWindowDays)

	sh := analysis.Spec.AlternativeSelfHosted
	result := breakevenengine.Analyze(breakevenengine.Inputs{
		ManagedTokenCostMonthly: managedMonthly,
		ProviderFixedMonthly:    providerFixed,
		GpuMonthly:              sh.MonthlyGpuCostEUR.AsApproximateFloat64(),
		OpsMonthly:              qtyOrZero(sh.EstimatedOpsCostEUR),
		StorageNetworkMonthly:   qtyOrZero(sh.StorageNetworkCostEUR),
		MigrationCost:           qtyOrZero(sh.MigrationCostEUR),
	}, breakevenengine.DefaultPaybackThresholdMonths)

	analysis.Status.ManagedMonthlyCostEUR = moneyQuantityPtr(result.ManagedMonthly)
	analysis.Status.SelfHostedMonthlyCostEUR = moneyQuantityPtr(result.SelfHostedMonthly)
	analysis.Status.MonthlySavingsEUR = moneyQuantityPtr(result.MonthlySavings)
	if result.HasPayback {
		analysis.Status.PaybackMonths = strconv.FormatFloat(result.PaybackMonths, 'f', 1, 64)
	} else {
		analysis.Status.PaybackMonths = ""
	}
	analysis.Status.Recommendation = aiopsv1alpha1.BreakEvenRecommendation(result.Recommendation)
	meta.SetStatusCondition(&analysis.Status.Conditions, readyTrue(analysis.Generation, result.Message))

	metrics.BreakevenSavingsEUR.WithLabelValues(analysis.Namespace, analysis.Name).Set(result.MonthlySavings)

	if err := r.Status().Update(ctx, &analysis); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(&analysis, "Normal", "BreakEvenComputed",
			"%s (savings %.2f EUR/mo)", result.Recommendation, result.MonthlySavings)
	}
	logger.V(1).Info("reconciled AIBreakEvenAnalysis",
		"recommendation", result.Recommendation, "savings", result.MonthlySavings)
	return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AIBreakEvenAnalysisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIBreakEvenAnalysis{}).
		Complete(r)
}
