package controller

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/keyrelease"
)

type AIKeyReleasePolicyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aikeyreleasepolicies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aikeyreleasepolicies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aikeyreleasepolicies/finalizers,verbs=update
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=attestationevidences;aiplacementdecisions,verbs=get;list;watch

func (r *AIKeyReleasePolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var policy aiopsv1alpha1.AIKeyReleasePolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	evidenceVerified, evidenceRevoked, err := r.resolveEvidenceState(ctx, &policy)
	if err != nil {
		return ctrl.Result{}, err
	}
	decision := keyrelease.Evaluate(keyrelease.Request{
		PolicyRequired:   policy.Spec.KeyRelease.Required,
		EvidenceVerified: evidenceVerified,
		EvidenceRevoked:  evidenceRevoked,
		PolicyTTLSeconds: policy.Spec.KeyRelease.TTLSeconds,
	})

	now := metav1.NewTime(time.Now().UTC())
	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.LastDecision = map[bool]string{true: "allow", false: "deny"}[decision.Allowed]
	policy.Status.LastDecisionTime = &now
	if decision.Allowed {
		meta.SetStatusCondition(&policy.Status.Conditions, readyTrue(policy.Generation, string(decision.Reason)))
	} else {
		meta.SetStatusCondition(&policy.Status.Conditions, readyFalse(policy.Generation, aiopsv1alpha1.ReasonValidationFailed, string(decision.Reason)))
	}
	if err := r.Status().Update(ctx, &policy); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *AIKeyReleasePolicyReconciler) resolveEvidenceState(ctx context.Context, policy *aiopsv1alpha1.AIKeyReleasePolicy) (bool, bool, error) {
	if policy.Spec.EvidenceRef == nil {
		return false, false, nil
	}
	namespace := policy.Spec.EvidenceRef.Namespace
	if namespace == "" {
		namespace = policy.Namespace
	}
	var evidence aiopsv1alpha1.AttestationEvidence
	if err := r.Get(ctx, client.ObjectKey{Name: policy.Spec.EvidenceRef.Name, Namespace: namespace}, &evidence); err != nil {
		return false, false, err
	}
	return evidence.Status.Verified, evidence.Status.Revoked, nil
}

func (r *AIKeyReleasePolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIKeyReleasePolicy{}).
		Complete(r)
}
