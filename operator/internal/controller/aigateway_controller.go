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
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

// AIGatewayReconciler reconciles a AIGateway object.
type AIGatewayReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aigateways,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aigateways/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aigateways/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

// Reconcile resolves the set of governed namespaces from the NamespaceSelector
// and records readiness. The MVP is strictly read-only: it never mutates the
// gateway or cluster workloads.
func (r *AIGatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var gw aiopsv1alpha1.AIGateway
	if err := r.Get(ctx, req.NamespacedName, &gw); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	gw.Status.ObservedGeneration = gw.Generation

	governed, err := r.resolveGovernedNamespaces(ctx, gw.Spec.NamespaceSelector)
	if err != nil {
		meta.SetStatusCondition(&gw.Status.Conditions,
			readyFalse(gw.Generation, aiopsv1alpha1.ReasonReconcileError, err.Error()))
		_ = r.Status().Update(ctx, &gw)
		return ctrl.Result{}, err
	}
	gw.Status.GovernedNamespaces = governed

	meta.SetStatusCondition(&gw.Status.Conditions,
		readyTrue(gw.Generation, "AIGateway registered and namespaces resolved"))

	if err := r.Status().Update(ctx, &gw); err != nil {
		return ctrl.Result{}, err
	}

	if r.Recorder != nil {
		r.Recorder.Eventf(&gw, corev1.EventTypeNormal, "Reconciled",
			"Governs %d namespace(s) via %s telemetry", len(governed), gw.Spec.Telemetry.Mode)
	}
	logger.V(1).Info("reconciled AIGateway", "governedNamespaces", len(governed))
	return ctrl.Result{}, nil
}

// resolveGovernedNamespaces lists namespaces matching the selector. A nil
// selector governs nothing (safe default); an empty selector would match all,
// so we treat nil explicitly.
func (r *AIGatewayReconciler) resolveGovernedNamespaces(ctx context.Context, sel *metav1.LabelSelector) ([]string, error) {
	if sel == nil {
		return nil, nil
	}
	selector, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return nil, err
	}
	var nsList corev1.NamespaceList
	if err := r.List(ctx, &nsList, &client.ListOptions{LabelSelector: selector}); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(nsList.Items))
	for i := range nsList.Items {
		names = append(names, nsList.Items[i].Name)
	}
	sort.Strings(names)
	return names, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AIGatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIGateway{}).
		Complete(r)
}
