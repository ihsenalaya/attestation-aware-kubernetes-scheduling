package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

func TestAIWorkloadBindingReconcilerRequiresAllReferences(t *testing.T) {
	testScheme := runtime.NewScheme()
	if err := corev1.AddToScheme(testScheme); err != nil {
		t.Fatal(err)
	}
	if err := aiopsv1alpha1.AddToScheme(testScheme); err != nil {
		t.Fatal(err)
	}
	binding := &aiopsv1alpha1.AIWorkloadBinding{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "finance", Generation: 3}, Spec: aiopsv1alpha1.AIWorkloadBindingSpec{
		ServiceAccountName: "worker", TenantID: "tenant-a", Team: "treasury", Application: "assistant", BudgetPolicyRef: "budget", RoutingPolicyRef: "routing", Sensitivity: aiopsv1alpha1.TierHigh, AllowedZones: []string{"eu"}, RequireGateway: true,
	}}
	objects := []client.Object{binding, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "finance", UID: types.UID("sa-uid")}},
		&aiopsv1alpha1.AIBudgetPolicy{ObjectMeta: metav1.ObjectMeta{Name: "budget", Namespace: "finance", UID: types.UID("budget-uid"), Generation: 2}},
		&aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "routing", Namespace: "finance", UID: types.UID("routing-uid"), Generation: 4}}}
	c := fakeclient.NewClientBuilder().WithScheme(testScheme).WithStatusSubresource(&aiopsv1alpha1.AIWorkloadBinding{}).WithObjects(objects...).Build()
	r := &AIWorkloadBindingReconciler{Client: c, Scheme: testScheme}
	key := types.NamespacedName{Namespace: "finance", Name: "worker"}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	var got aiopsv1alpha1.AIWorkloadBinding
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ObservedGeneration != 3 || !meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady) || !meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionValidated) {
		t.Fatalf("status=%+v", got.Status)
	}
	if got.Status.ResolvedServiceAccountUID != "sa-uid" || got.Status.ResolvedBudgetPolicy == nil || got.Status.ResolvedBudgetPolicy.UID != "budget-uid" || got.Status.ResolvedBudgetPolicy.Generation != 2 ||
		got.Status.ResolvedRoutingPolicy == nil || got.Status.ResolvedRoutingPolicy.UID != "routing-uid" || got.Status.ResolvedRoutingPolicy.Generation != 4 {
		t.Fatalf("resolved identities=%+v", got.Status)
	}
	if err := c.Delete(context.Background(), &aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "routing", Namespace: "finance"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatal(err)
	}
	if !meta.IsStatusConditionFalse(got.Status.Conditions, aiopsv1alpha1.ConditionReady) || got.Status.ResolvedServiceAccountUID != "" || got.Status.ResolvedBudgetPolicy != nil || got.Status.ResolvedRoutingPolicy != nil {
		t.Fatalf("missing dependency retained ready/stale identity: %+v", got.Status)
	}
}
