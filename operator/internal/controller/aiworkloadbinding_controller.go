package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

// AIWorkloadBindingReconciler is the sole status producer for the operator-owned
// ServiceAccount-to-governance binding consumed by synchronous admission.
type AIWorkloadBindingReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiworkloadbindings,verbs=get;list;watch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiworkloadbindings/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aibudgetpolicies;airoutingpolicies,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch

func (r *AIWorkloadBindingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var binding aiopsv1alpha1.AIWorkloadBinding
	if err := r.Get(ctx, req.NamespacedName, &binding); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	binding.Status.ObservedGeneration = binding.Generation
	setFalse := func(reason, message string) (ctrl.Result, error) {
		binding.Status.ResolvedServiceAccountUID = ""
		binding.Status.ResolvedBudgetPolicy = nil
		binding.Status.ResolvedRoutingPolicy = nil
		meta.SetStatusCondition(&binding.Status.Conditions, metav1.Condition{Type: aiopsv1alpha1.ConditionValidated, Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: binding.Generation})
		meta.SetStatusCondition(&binding.Status.Conditions, readyFalse(binding.Generation, reason, message))
		return ctrl.Result{}, r.Status().Update(ctx, &binding)
	}
	if errs := binding.Spec.Validate(binding.Name, field.NewPath("spec")); len(errs) != 0 {
		return setFalse(aiopsv1alpha1.ReasonValidationFailed, errs.ToAggregate().Error())
	}
	var serviceAccount corev1.ServiceAccount
	if err := r.Get(ctx, client.ObjectKey{Namespace: binding.Namespace, Name: binding.Spec.ServiceAccountName}, &serviceAccount); err != nil {
		return setFalse(referenceReason(err), fmt.Sprintf("ServiceAccount %q is not resolvable: %v", binding.Spec.ServiceAccountName, err))
	}
	var budget aiopsv1alpha1.AIBudgetPolicy
	if err := r.Get(ctx, client.ObjectKey{Namespace: binding.Namespace, Name: binding.Spec.BudgetPolicyRef}, &budget); err != nil {
		return setFalse(referenceReason(err), fmt.Sprintf("AIBudgetPolicy %q is not resolvable: %v", binding.Spec.BudgetPolicyRef, err))
	}
	var routing aiopsv1alpha1.AIRoutingPolicy
	if err := r.Get(ctx, client.ObjectKey{Namespace: binding.Namespace, Name: binding.Spec.RoutingPolicyRef}, &routing); err != nil {
		return setFalse(referenceReason(err), fmt.Sprintf("AIRoutingPolicy %q is not resolvable: %v", binding.Spec.RoutingPolicyRef, err))
	}
	binding.Status.ResolvedServiceAccountUID = serviceAccount.UID
	binding.Status.ResolvedBudgetPolicy = &aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: budget.Name, UID: budget.UID, Generation: budget.Generation}
	binding.Status.ResolvedRoutingPolicy = &aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: routing.Name, UID: routing.UID, Generation: routing.Generation}
	message := "ServiceAccount and governance policy references resolved"
	meta.SetStatusCondition(&binding.Status.Conditions, metav1.Condition{Type: aiopsv1alpha1.ConditionValidated, Status: metav1.ConditionTrue, Reason: aiopsv1alpha1.ReasonReconciled, Message: message, ObservedGeneration: binding.Generation})
	meta.SetStatusCondition(&binding.Status.Conditions, readyTrue(binding.Generation, message))
	return ctrl.Result{}, r.Status().Update(ctx, &binding)
}

func referenceReason(err error) string {
	if apierrors.IsNotFound(err) {
		return aiopsv1alpha1.ReasonReferenceNotFound
	}
	return aiopsv1alpha1.ReasonReconcileError
}

func (r *AIWorkloadBindingReconciler) mapDependency(ctx context.Context, object client.Object) []reconcile.Request {
	var bindings aiopsv1alpha1.AIWorkloadBindingList
	if err := r.List(ctx, &bindings, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0)
	for i := range bindings.Items {
		binding := &bindings.Items[i]
		matches := false
		switch object.(type) {
		case *corev1.ServiceAccount:
			matches = binding.Spec.ServiceAccountName == object.GetName()
		case *aiopsv1alpha1.AIBudgetPolicy:
			matches = binding.Spec.BudgetPolicyRef == object.GetName()
		case *aiopsv1alpha1.AIRoutingPolicy:
			matches = binding.Spec.RoutingPolicyRef == object.GetName()
		}
		if matches {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(binding)})
		}
	}
	return out
}

func (r *AIWorkloadBindingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mapper := handler.EnqueueRequestsFromMapFunc(r.mapDependency)
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIWorkloadBinding{}).
		Watches(&corev1.ServiceAccount{}, mapper).
		Watches(&aiopsv1alpha1.AIBudgetPolicy{}, mapper).
		Watches(&aiopsv1alpha1.AIRoutingPolicy{}, mapper).
		Complete(r)
}
