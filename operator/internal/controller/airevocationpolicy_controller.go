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
)

type AIRevocationPolicyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airevocationpolicies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airevocationpolicies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=airevocationpolicies/finalizers,verbs=update

func (r *AIRevocationPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var policy aiopsv1alpha1.AIRevocationPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	expiresAt := metav1.NewTime(policy.CreationTimestamp.Add(time.Duration(policy.Spec.TTLSeconds) * time.Second))
	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.ExpiresAt = &expiresAt
	policy.Status.Active = time.Now().UTC().Before(expiresAt.Time)
	if policy.Status.Active {
		meta.SetStatusCondition(&policy.Status.Conditions, readyTrue(policy.Generation, "Revocation policy is active"))
	} else {
		meta.SetStatusCondition(&policy.Status.Conditions, readyFalse(policy.Generation, aiopsv1alpha1.ReasonReconciled, "Revocation policy expired"))
	}
	if err := r.Status().Update(ctx, &policy); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Until(expiresAt.Time)}, nil
}

func (r *AIRevocationPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIRevocationPolicy{}).
		Complete(r)
}
