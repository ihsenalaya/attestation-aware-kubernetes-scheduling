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
	"sigs.k8s.io/controller-runtime/pkg/log"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
)

// AIProviderReconciler reconciles a AIProvider object.
type AIProviderReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Now      func() time.Time
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiproviders,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiproviders/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiproviders/finalizers,verbs=update

// Reconcile records that the provider definition is registered and valid. The
// CRD schema already enforces structural validity; here we surface readiness.
func (r *AIProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var provider aiopsv1alpha1.AIProvider
	if err := r.Get(ctx, req.NamespacedName, &provider); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	provider.Status.ObservedGeneration = provider.Generation
	adapter, adapterErr := govarpricing.ForProviderType(provider.Spec.Type)
	if adapterErr == nil {
		now := time.Now().UTC()
		if r.Now != nil {
			now = r.Now().UTC()
		}
		snapshot, _, normalizeErr := adapter.Normalize(provider, now)
		adapterErr = normalizeErr
		if normalizeErr == nil {
			provider.Status.GOVAR = &aiopsv1alpha1.AIProviderGOVARStatus{PricingSnapshot: &snapshot}
			meta.SetStatusCondition(&provider.Status.Conditions,
				readyTrue(provider.Generation, "typed pricing snapshot normalized and complete"))
		}
	}
	if adapterErr != nil {
		provider.Status.GOVAR = nil
		meta.SetStatusCondition(&provider.Status.Conditions, readyFalse(provider.Generation,
			"PricingEvidenceInvalid", fmt.Sprintf("GOV-AR pricing is not executable: %v", adapterErr)))
	}

	if err := r.Status().Update(ctx, &provider); err != nil {
		return ctrl.Result{}, err
	}
	logger.V(1).Info("reconciled AIProvider", "type", provider.Spec.Type, "managed", provider.Spec.Managed)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AIProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIProvider{}).
		Complete(r)
}
