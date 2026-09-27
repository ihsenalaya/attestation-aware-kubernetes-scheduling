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
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
)

// aiModelCatalogTracker maps "namespace/cr-name" → modelName so metrics can be
// cleaned up on deletion without re-fetching the (already gone) object.
var aiModelCatalogTracker sync.Map // key string → modelName string

// AIModelReconciler reconciles a AIModel object.
type AIModelReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Now      func() time.Time
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aimodels,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aimodels/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aimodels/finalizers,verbs=update
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiproviders,verbs=get;list;watch

// Reconcile validates that the referenced AIProvider exists and records readiness.
// It also emits catalog-level sovereignty_score Prometheus metrics for every
// registered AIModel so the dashboard can show compliance even before traffic.
func (r *AIModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	trackerKey := req.Namespace + "/" + req.Name

	var model aiopsv1alpha1.AIModel
	if err := r.Get(ctx, req.NamespacedName, &model); err != nil {
		if apierrors.IsNotFound(err) {
			// Clean up catalog metrics for the deleted model.
			if v, ok := aiModelCatalogTracker.LoadAndDelete(trackerKey); ok {
				modelName := v.(string)
				metrics.SovereigntyScore.DeleteLabelValues(req.Namespace, "catalog", modelName)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	model.Status.ObservedGeneration = model.Generation

	var provider aiopsv1alpha1.AIProvider
	provKey := types.NamespacedName{Namespace: model.Namespace, Name: model.Spec.ProviderRef}
	if err := r.Get(ctx, provKey, &provider); err != nil {
		if apierrors.IsNotFound(err) {
			model.Status.ResolvedProvider = ""
			meta.SetStatusCondition(&model.Status.Conditions, readyFalse(
				model.Generation, aiopsv1alpha1.ReasonReferenceNotFound,
				fmt.Sprintf("AIProvider %q not found", model.Spec.ProviderRef)))
			if err := r.Status().Update(ctx, &model); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	model.Status.ResolvedProvider = provider.Spec.Type
	model.Status.GOVAR = nil // stale cap evidence must never survive a failed refresh.
	if model.Spec.GOVAR != nil && model.Spec.GOVAR.Routable {
		capEvidence := model.Spec.GOVAR.OutputCapEvidence
		binding, bindingErr := modelProviderRoute(provider, model.Spec.GOVAR.RouteBindingRef)
		expectedCapParameter, adapterSupported := govarpricing.OutputCapRequestParameter(provider.Spec.Type, binding.PathMode)
		if capEvidence == nil || bindingErr != nil || provider.Status.GOVAR == nil || provider.Status.GOVAR.PricingSnapshot == nil {
			meta.SetStatusCondition(&model.Status.Conditions, readyFalse(model.Generation, "OutputCapEvidenceInvalid", "routable model lacks current provider-bound pricing/cap evidence"))
		} else if !adapterSupported || capEvidence.CapabilityAdapterVersion != govarpricing.CurrentAdapterVersion || !capEvidence.EnforcedByPathAdapter || capEvidence.RequestParameter != expectedCapParameter || !capEvidence.ValidUntil.After(r.evaluationTime()) || capEvidence.Mode != aiopsv1alpha1.ProviderEvidenceAdminAttested || !govarpricing.ValidSHA256(capEvidence.EvidenceSHA256) {
			meta.SetStatusCondition(&model.Status.Conditions, readyFalse(model.Generation, "OutputCapEvidenceInvalid", "output cap is stale, unbound, or not enforced by the selected path adapter"))
		} else if provider.Status.ObservedGeneration != provider.Generation || provider.Status.GOVAR.PricingSnapshot.SpecGeneration != provider.Generation {
			meta.SetStatusCondition(&model.Status.Conditions, readyFalse(model.Generation, "ProviderPricingStale", "provider pricing snapshot does not match current generation"))
		} else {
			observed := metav1.Now()
			model.Status.GOVAR = &aiopsv1alpha1.AIModelGOVARStatus{VerifiedOutputCap: &aiopsv1alpha1.AIModelVerifiedOutputCapStatus{
				Verified: true, MaxOutputTokens: capEvidence.MaxOutputTokens, ObservedAt: observed,
				SourceVersion: capEvidence.SourceVersion, ProviderUID: string(provider.UID), ProviderGeneration: provider.Generation,
				ProviderDeployment: binding.ProviderDeployment, ModelVersion: model.Spec.ModelName,
				CapabilityAdapterVersion: capEvidence.CapabilityAdapterVersion, EvidenceMode: capEvidence.Mode,
				EvidenceSHA256: capEvidence.EvidenceSHA256, PricingSnapshotSHA256: provider.Status.GOVAR.PricingSnapshot.SnapshotSHA256, ValidUntil: capEvidence.ValidUntil,
				RequestParameter: capEvidence.RequestParameter, EnforcedByPathAdapter: capEvidence.EnforcedByPathAdapter,
			}}
			meta.SetStatusCondition(&model.Status.Conditions, readyTrue(model.Generation, "provider-bound output cap evidence is current"))
		}
	} else {
		meta.SetStatusCondition(&model.Status.Conditions, readyTrue(model.Generation, "AIModel catalogued and provider resolved"))
	}

	if err := r.Status().Update(ctx, &model); err != nil {
		return ctrl.Result{}, err
	}

	// Emit catalog-level metrics so every registered model appears in the radar
	// even before it receives any observed traffic.
	r.emitCatalogMetrics(ctx, &model, &provider)
	aiModelCatalogTracker.Store(trackerKey, model.Spec.ModelName)

	logger.V(1).Info("reconciled AIModel", "model", model.Spec.ModelName, "provider", provider.Spec.Type)
	return ctrl.Result{}, nil
}

func modelProviderRoute(provider aiopsv1alpha1.AIProvider, name string) (aiopsv1alpha1.AIProviderGatewayRouteBinding, error) {
	if provider.Spec.GOVAR == nil || strings.TrimSpace(name) == "" {
		return aiopsv1alpha1.AIProviderGatewayRouteBinding{}, fmt.Errorf("route binding is absent")
	}
	var found *aiopsv1alpha1.AIProviderGatewayRouteBinding
	for i := range provider.Spec.GOVAR.GatewayRoutes {
		if provider.Spec.GOVAR.GatewayRoutes[i].Name == name {
			if found != nil {
				return aiopsv1alpha1.AIProviderGatewayRouteBinding{}, fmt.Errorf("duplicate route binding")
			}
			candidate := provider.Spec.GOVAR.GatewayRoutes[i]
			found = &candidate
		}
	}
	if found == nil || strings.TrimSpace(found.ProviderDeployment) == "" {
		return aiopsv1alpha1.AIProviderGatewayRouteBinding{}, fmt.Errorf("route binding is missing")
	}
	return *found, nil
}

// emitCatalogMetrics publishes sovereignty_score for the model.
func (r *AIModelReconciler) emitCatalogMetrics(ctx context.Context, model *aiopsv1alpha1.AIModel, provider *aiopsv1alpha1.AIProvider) {
	ns := model.Namespace
	modelName := model.Spec.ModelName

	// Sovereignty score: check whether the provider zone is allowed by the
	// active AISovereigntyPolicy. Default compliant when no policy is active.
	sovScore := 1.0
	if sov := firstSovereigntyPolicy(ctx, r.Client, ns); sov != nil {
		pe := policyToEngine(sov.Spec)
		zone := strings.ToUpper(strings.TrimSpace(provider.Spec.DataResidency))
		if !sovereigntyengine.IsZoneAllowed(pe, zone) {
			sovScore = 0.0
		}
	}
	metrics.SovereigntyScore.WithLabelValues(ns, "catalog", modelName).Set(sovScore)
}

// modelsForProvider maps an AIProvider event to reconcile requests for every
// AIModel in the same namespace that references it.
func (r *AIModelReconciler) modelsForProvider(ctx context.Context, obj client.Object) []reconcile.Request {
	var models aiopsv1alpha1.AIModelList
	if err := r.List(ctx, &models, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range models.Items {
		if models.Items[i].Spec.ProviderRef == obj.GetName() {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: models.Items[i].Namespace,
				Name:      models.Items[i].Name,
			}})
		}
	}
	return reqs
}

func (r *AIModelReconciler) evaluationTime() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// SetupWithManager sets up the controller with the Manager.
func (r *AIModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIModel{}).
		Watches(&aiopsv1alpha1.AIProvider{}, handler.EnqueueRequestsFromMapFunc(r.modelsForProvider)).
		Complete(r)
}
