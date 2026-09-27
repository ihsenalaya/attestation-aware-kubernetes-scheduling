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
	"regexp"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

const (
	changeRequestAnnotation = "aiops.imperium.io/change-request-reroutes"
	defaultExpiresAfter     = 48 * time.Hour
)

// AIChangeRequestReconciler implements the human-in-the-loop change workflow:
//
//	Pending  → wait for spec.approval to be set by a human
//	Approved → actuate the change (reroute) in the gateway
//	Rejected → record and stop
//	Expired  → record and stop (expiresAfter elapsed)
//	Actuated → idempotent; keep the reroute live
type AIChangeRequestReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Now      func() time.Time
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aichangerequests,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aichangerequests/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aichangerequests/finalizers,verbs=update
//+kubebuilder:rbac:groups=aigateway.envoyproxy.io,resources=aigatewayroutelists;aigatewayroutelists,verbs=get;list;watch;update;patch

func (r *AIChangeRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var crq aiopsv1alpha1.AIChangeRequest
	if err := r.Get(ctx, req.NamespacedName, &crq); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	crq.Status.ObservedGeneration = crq.Generation

	// Compute/ensure expiry time. Policy-level route approval has an absolute
	// reviewer-selected expiry; legacy reroutes retain their relative TTL.
	expiresAt := crq.CreationTimestamp.Add(defaultExpiresAfter)
	if crq.Spec.Action == aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute && crq.Spec.GOVARRouteApproval != nil {
		expiresAt = crq.Spec.GOVARRouteApproval.ValidUntil.Time
	} else if dur, err := time.ParseDuration(crq.Spec.ExpiresAfter); err == nil && dur > 0 {
		expiresAt = crq.CreationTimestamp.Add(dur)
	}
	if crq.Status.ExpiresAt == nil || !crq.Status.ExpiresAt.Equal(&metav1.Time{Time: expiresAt}) {
		t := metav1.NewTime(expiresAt)
		crq.Status.ExpiresAt = &t
		if err := r.Status().Update(ctx, &crq); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Terminal states — nothing more to do.
	switch crq.Status.Phase {
	case aiopsv1alpha1.AIChangeRequestPhaseActuated,
		aiopsv1alpha1.AIChangeRequestPhaseRejected,
		aiopsv1alpha1.AIChangeRequestPhaseExpired:
		return ctrl.Result{}, nil
	}

	// Check expiry before looking at approval. Reusable route approvals also
	// become visibly unusable at their absolute ValidUntil boundary.
	if !r.now().Before(crq.Status.ExpiresAt.Time) {
		r.setPhase(&crq, aiopsv1alpha1.AIChangeRequestPhaseExpired,
			fmt.Sprintf("change request authorization expired at %s", crq.Status.ExpiresAt.Format(time.RFC3339)))
		crq.Status.ApprovedAt = nil
		crq.Status.ApprovedScopeDigest = ""
		crq.Status.ApprovedDecisionDigest = ""
		crq.Status.ApprovedBy = ""
		if r.Recorder != nil {
			r.Recorder.Eventf(&crq, corev1.EventTypeWarning, "ChangeRequestExpired",
				"Change request %s/%s expired without approval", crq.Namespace, crq.Name)
		}
		return ctrl.Result{}, r.Status().Update(ctx, &crq)
	}

	switch crq.Spec.Approval {
	case aiopsv1alpha1.AIChangeRequestApprovalRejected:
		if crq.Spec.Action == aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute {
			if err := crq.ValidateGOVARDecision(r.now()); err != nil {
				r.setPhase(&crq, aiopsv1alpha1.AIChangeRequestPhaseFailed, "GOV-AR authenticated rejection decision is invalid: "+err.Error())
				crq.Status.ApprovedAt = nil
				crq.Status.ApprovedScopeDigest = ""
				crq.Status.ApprovedDecisionDigest = ""
				crq.Status.ApprovedBy = ""
				return ctrl.Result{}, r.Status().Update(ctx, &crq)
			}
		}
		r.setPhase(&crq, aiopsv1alpha1.AIChangeRequestPhaseRejected,
			"change request rejected by reviewer")
		crq.Status.ApprovedAt = nil
		crq.Status.ApprovedScopeDigest = ""
		crq.Status.ApprovedDecisionDigest = ""
		crq.Status.ApprovedBy = ""
		if r.Recorder != nil {
			r.Recorder.Eventf(&crq, corev1.EventTypeNormal, "ChangeRequestRejected",
				"Change %s → %s rejected", crq.Spec.SourceModel, crq.Spec.TargetModel)
		}
		return ctrl.Result{}, r.Status().Update(ctx, &crq)

	case aiopsv1alpha1.AIChangeRequestApprovalApproved:
		if crq.Spec.Action == aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute {
			return r.approveGOVARRoute(ctx, &crq)
		}
		return r.actuate(ctx, &crq)

	default:
		// Still pending — update phase and requeue until expiry.
		r.setPhase(&crq, aiopsv1alpha1.AIChangeRequestPhasePending,
			fmt.Sprintf("waiting for approval; expires at %s", crq.Status.ExpiresAt.Format(time.RFC3339)))
		crq.Status.ApprovedAt = nil
		crq.Status.ApprovedScopeDigest = ""
		crq.Status.ApprovedDecisionDigest = ""
		crq.Status.ApprovedBy = ""
		if err := r.Status().Update(ctx, &crq); err != nil {
			return ctrl.Result{}, err
		}
		logger.V(1).Info("AIChangeRequest pending approval", "name", crq.Name)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
}

func (r *AIChangeRequestReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

var sha256Lower = regexp.MustCompile(`^[a-f0-9]{64}$`)

// approveGOVARRoute verifies the bounded policy/model/provider identities and
// writes controller-owned approval evidence. It deliberately performs no route
// actuation and creates no request-level object.
func (r *AIChangeRequestReconciler) approveGOVARRoute(ctx context.Context, crq *aiopsv1alpha1.AIChangeRequest) (ctrl.Result, error) {
	scope := crq.Spec.GOVARRouteApproval
	if scope == nil || !sha256Lower.MatchString(scope.RouteSnapshotDigest) || !sha256Lower.MatchString(scope.ScopeDigest) || scope.ScopeDigest != scope.ComputeDigest() {
		r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseFailed, "GOV-AR route approval scope or digest is invalid")
		crq.Status.ApprovedAt = nil
		crq.Status.ApprovedScopeDigest = ""
		crq.Status.ApprovedDecisionDigest = ""
		crq.Status.ApprovedBy = ""
		return ctrl.Result{}, r.Status().Update(ctx, crq)
	}
	if err := crq.ValidateGOVARDecision(r.now()); err != nil {
		r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseFailed, "GOV-AR authenticated approval decision is invalid: "+err.Error())
		crq.Status.ApprovedAt = nil
		crq.Status.ApprovedScopeDigest = ""
		crq.Status.ApprovedDecisionDigest = ""
		crq.Status.ApprovedBy = ""
		return ctrl.Result{}, r.Status().Update(ctx, crq)
	}
	if !r.now().Before(scope.ValidUntil.Time) {
		r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseExpired, "GOV-AR route approval scope has expired")
		crq.Status.ApprovedAt = nil
		crq.Status.ApprovedScopeDigest = ""
		crq.Status.ApprovedDecisionDigest = ""
		crq.Status.ApprovedBy = ""
		return ctrl.Result{}, r.Status().Update(ctx, crq)
	}
	if err := r.validateGOVARRouteReferences(ctx, crq.Namespace, scope); err != nil {
		r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseFailed, err.Error())
		crq.Status.ApprovedAt = nil
		crq.Status.ApprovedScopeDigest = ""
		crq.Status.ApprovedDecisionDigest = ""
		crq.Status.ApprovedBy = ""
		return ctrl.Result{}, r.Status().Update(ctx, crq)
	}
	approvedAt := metav1.NewTime(r.now())
	decision := crq.Spec.GOVARDecision
	if crq.Status.Phase == aiopsv1alpha1.AIChangeRequestPhaseApproved && crq.Status.ApprovedAt != nil && crq.Status.ObservedGeneration == crq.Generation &&
		crq.Status.ApprovedScopeDigest == scope.ScopeDigest && crq.Status.ApprovedDecisionDigest == decision.DecisionDigest && crq.Status.ApprovedBy == decision.ReviewerIdentity {
		approvedAt = *crq.Status.ApprovedAt
	}
	crq.Status.ApprovedAt = &approvedAt
	crq.Status.ApprovedScopeDigest = scope.ScopeDigest
	crq.Status.ApprovedDecisionDigest = decision.DecisionDigest
	crq.Status.ApprovedBy = decision.ReviewerIdentity
	r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseApproved, "bounded GOV-AR route scope approved; admission revalidates every request without consuming this object")
	if r.Recorder != nil {
		r.Recorder.Eventf(crq, corev1.EventTypeNormal, "GOVARRouteApproved", "Approved policy %s model %s provider %s route snapshot %s until %s",
			scope.RoutingPolicy.Name, scope.Model.Name, scope.Provider.Name, scope.RouteSnapshotDigest, scope.ValidUntil.Format(time.RFC3339))
	}
	remaining := scope.ValidUntil.Sub(r.now())
	if remaining > 30*time.Second {
		remaining = 30 * time.Second
	}
	return ctrl.Result{RequeueAfter: remaining}, r.Status().Update(ctx, crq)
}

func (r *AIChangeRequestReconciler) validateGOVARRouteReferences(ctx context.Context, namespace string, scope *aiopsv1alpha1.GOVARRouteApprovalScope) error {
	validRef := func(ref aiopsv1alpha1.AIWorkloadBindingResolvedReference) bool {
		return ref.Name != "" && ref.UID != "" && ref.Generation > 0
	}
	if !validRef(scope.RoutingPolicy) || !validRef(scope.Model) || !validRef(scope.Provider) {
		return fmt.Errorf("GOV-AR route approval contains an empty or invalid object reference")
	}
	var routing aiopsv1alpha1.AIRoutingPolicy
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: scope.RoutingPolicy.Name}, &routing); err != nil {
		return fmt.Errorf("resolve approved routing policy: %w", err)
	}
	if routing.UID != scope.RoutingPolicy.UID || routing.Generation != scope.RoutingPolicy.Generation {
		return fmt.Errorf("approved routing policy UID/generation is stale")
	}
	var model aiopsv1alpha1.AIModel
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: scope.Model.Name}, &model); err != nil {
		return fmt.Errorf("resolve approved model: %w", err)
	}
	if model.UID != scope.Model.UID || model.Generation != scope.Model.Generation {
		return fmt.Errorf("approved model UID/generation is stale")
	}
	var provider aiopsv1alpha1.AIProvider
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: scope.Provider.Name}, &provider); err != nil {
		return fmt.Errorf("resolve approved provider: %w", err)
	}
	if provider.UID != scope.Provider.UID || provider.Generation != scope.Provider.Generation || model.Spec.ProviderRef != provider.Name {
		return fmt.Errorf("approved provider UID/generation or model ownership is stale")
	}
	return nil
}

func (r *AIChangeRequestReconciler) actuate(ctx context.Context, crq *aiopsv1alpha1.AIChangeRequest) (ctrl.Result, error) {
	if crq.Status.Phase == aiopsv1alpha1.AIChangeRequestPhaseActuated {
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	mb, err := modelBackends(ctx, r.Client, crq.Namespace)
	if err != nil {
		if apimeta.IsNoMatchError(err) {
			r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseFailed,
				"Envoy AI Gateway AIGatewayRoute CRD not found in cluster")
			return ctrl.Result{}, r.Status().Update(ctx, crq)
		}
		return ctrl.Result{}, err
	}

	targetBackend, ok := mb[crq.Spec.TargetModel]
	if !ok {
		r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseFailed,
			fmt.Sprintf("target model %q has no AIGatewayRoute backend", crq.Spec.TargetModel))
		return ctrl.Result{RequeueAfter: 60 * time.Second}, r.Status().Update(ctx, crq)
	}

	desired := map[string]string{crq.Spec.SourceModel: targetBackend}
	actuated, err := actuateReroutesWithAnnotation(ctx, r.Client, crq.Namespace, desired, changeRequestAnnotation)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !actuated[crq.Spec.SourceModel] {
		r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseFailed,
			fmt.Sprintf("no AIGatewayRoute found routing model %q", crq.Spec.SourceModel))
		return ctrl.Result{RequeueAfter: 60 * time.Second}, r.Status().Update(ctx, crq)
	}

	now := metav1.Now()
	crq.Status.ActuatedAt = &now
	crq.Status.ActuatedRoutes = nil
	for m := range actuated {
		crq.Status.ActuatedRoutes = append(crq.Status.ActuatedRoutes, m)
	}
	r.setPhase(crq, aiopsv1alpha1.AIChangeRequestPhaseActuated,
		fmt.Sprintf("rerouted %q → %q (backend: %s) at %s", crq.Spec.SourceModel, crq.Spec.TargetModel, targetBackend, now.Format(time.RFC3339)))
	if r.Recorder != nil {
		r.Recorder.Eventf(crq, corev1.EventTypeNormal, "ChangeActuated",
			"Change %s → %s actuated (saving: %s)", crq.Spec.SourceModel, crq.Spec.TargetModel, crq.Spec.ExpectedSavingEUR)
	}
	return ctrl.Result{RequeueAfter: 60 * time.Second}, r.Status().Update(ctx, crq)
}

func (r *AIChangeRequestReconciler) setPhase(crq *aiopsv1alpha1.AIChangeRequest, phase aiopsv1alpha1.AIChangeRequestPhase, msg string) {
	crq.Status.Phase = phase
	crq.Status.Message = msg
	condStatus := metav1.ConditionTrue
	if phase == aiopsv1alpha1.AIChangeRequestPhaseFailed || phase == aiopsv1alpha1.AIChangeRequestPhaseExpired || phase == aiopsv1alpha1.AIChangeRequestPhaseRejected {
		condStatus = metav1.ConditionFalse
	}
	reason := aiopsv1alpha1.ReasonReconciled
	if phase == aiopsv1alpha1.AIChangeRequestPhaseFailed {
		reason = aiopsv1alpha1.ReasonReconcileError
	}
	apimeta.SetStatusCondition(&crq.Status.Conditions, readyCondition(crq.Generation, condStatus, reason, msg))
}

func (r *AIChangeRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIChangeRequest{}).
		Complete(r)
}
