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

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

const (
	routeOverrideFinalizer  = "aiops.imperium.io/route-override"
	routeOverrideAnnotation = "aiops.imperium.io/manual-reroutes"
)

// AIRouteOverrideReconciler reconciles AIRouteOverride objects.
type AIRouteOverrideReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airouteoverrides,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airouteoverrides/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airouteoverrides/finalizers,verbs=update
//+kubebuilder:rbac:groups=aigateway.envoyproxy.io,resources=aigatewayroutelists;aigatewayroutelists,verbs=get;list;watch;update;patch

func (r *AIRouteOverrideReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var override aiopsv1alpha1.AIRouteOverride
	if err := r.Get(ctx, req.NamespacedName, &override); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion: revert the reroute then drop the finalizer.
	if !override.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&override, routeOverrideFinalizer) {
			// Revert by sending empty desired-reroutes map.
			if _, err := actuateReroutesWithAnnotation(ctx, r.Client, override.Namespace, nil, routeOverrideAnnotation); err != nil {
				logger.Error(err, "reverting route override on deletion")
			}
			controllerutil.RemoveFinalizer(&override, routeOverrideFinalizer)
			return ctrl.Result{}, r.Update(ctx, &override)
		}
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is present.
	if controllerutil.AddFinalizer(&override, routeOverrideFinalizer) {
		return ctrl.Result{}, r.Update(ctx, &override)
	}

	override.Status.ObservedGeneration = override.Generation

	// Resolve the backend name the target model is served by.
	mb, err := modelBackends(ctx, r.Client, override.Namespace)
	if err != nil {
		if apimeta.IsNoMatchError(err) {
			r.setStatus(&override, aiopsv1alpha1.AIRouteOverrideFailed,
				"Envoy AI Gateway AIGatewayRoute CRD not found in cluster")
			_ = r.Status().Update(ctx, &override)
			return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
		}
		return ctrl.Result{}, err
	}

	targetBackend, ok := mb[override.Spec.TargetModel]
	if !ok {
		msg := fmt.Sprintf("target model %q has no AIGatewayRoute backend in namespace %q", override.Spec.TargetModel, override.Namespace)
		r.setStatus(&override, aiopsv1alpha1.AIRouteOverrideFailed, msg)
		_ = r.Status().Update(ctx, &override)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	desired := map[string]string{override.Spec.SourceModel: targetBackend}
	actuated, err := actuateReroutesWithAnnotation(ctx, r.Client, override.Namespace, desired, routeOverrideAnnotation)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !actuated[override.Spec.SourceModel] {
		msg := fmt.Sprintf("no AIGatewayRoute found routing model %q in namespace %q", override.Spec.SourceModel, override.Namespace)
		r.setStatus(&override, aiopsv1alpha1.AIRouteOverrideFailed, msg)
	} else {
		msg := fmt.Sprintf("rerouted %q → %q (backend: %s)", override.Spec.SourceModel, override.Spec.TargetModel, targetBackend)
		override.Status.ActuatedRoutes = nil
		for m := range actuated {
			override.Status.ActuatedRoutes = append(override.Status.ActuatedRoutes, m)
		}
		r.setStatus(&override, aiopsv1alpha1.AIRouteOverrideActuated, msg)
		if r.Recorder != nil {
			r.Recorder.Eventf(&override, corev1.EventTypeNormal, "RouteActuated",
				"Rerouted %s → %s (backend: %s)", override.Spec.SourceModel, override.Spec.TargetModel, targetBackend)
		}
	}
	logger.V(1).Info("AIRouteOverride reconciled",
		"source", override.Spec.SourceModel, "target", override.Spec.TargetModel)
	return ctrl.Result{RequeueAfter: 60 * time.Second}, r.Status().Update(ctx, &override)
}

func (r *AIRouteOverrideReconciler) setStatus(override *aiopsv1alpha1.AIRouteOverride, phase aiopsv1alpha1.AIRouteOverridePhase, msg string) {
	override.Status.Phase = phase
	override.Status.Message = msg
	condStatus := metav1.ConditionTrue
	if phase != aiopsv1alpha1.AIRouteOverrideActuated {
		condStatus = metav1.ConditionFalse
	}
	reason := aiopsv1alpha1.ReasonReconciled
	if phase == aiopsv1alpha1.AIRouteOverrideFailed {
		reason = aiopsv1alpha1.ReasonReconcileError
	}
	apimeta.SetStatusCondition(&override.Status.Conditions, readyCondition(override.Generation, condStatus, reason, msg))
}

func (r *AIRouteOverrideReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIRouteOverride{}).
		Complete(r)
}
