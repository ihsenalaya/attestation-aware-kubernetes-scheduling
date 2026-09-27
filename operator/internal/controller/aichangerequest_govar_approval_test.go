package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAIChangeRequestControllerApprovesBoundedGOVARRouteScope(t *testing.T) {
	now := time.Date(2026, 7, 13, 9, 0, 0, 0, time.UTC)
	change, dependencies := govarChangeFixture(now)
	objects := append(dependencies, change)
	client := fakeclient.NewClientBuilder().WithScheme(changeRequestTestScheme(t)).
		WithStatusSubresource(&aiopsv1alpha1.AIChangeRequest{}).WithRuntimeObjects(objects...).Build()
	reconciler := &AIChangeRequestReconciler{Client: client, Now: func() time.Time { return now }}
	key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: change.Namespace, Name: change.Name}}
	for i := 0; i < 2; i++ {
		if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	var got aiopsv1alpha1.AIChangeRequest
	if err := client.Get(context.Background(), key.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != aiopsv1alpha1.AIChangeRequestPhaseApproved || got.Status.ObservedGeneration != got.Generation ||
		got.Status.ApprovedAt == nil || got.Status.ApprovedScopeDigest != change.Spec.GOVARRouteApproval.ScopeDigest ||
		got.Status.ApprovedDecisionDigest != change.Spec.GOVARDecision.DecisionDigest || got.Status.ApprovedBy != change.Spec.GOVARDecision.ReviewerIdentity ||
		got.Status.ExpiresAt == nil || !got.Status.ExpiresAt.Equal(&change.Spec.GOVARRouteApproval.ValidUntil) || len(got.Status.ActuatedRoutes) != 0 {
		t.Fatalf("unexpected bounded approval status: %+v", got.Status)
	}
	// It is reusable state, not a one-use request object: reconciling again
	// retains the same controller-owned approval time and creates no child.
	approvedAt := got.Status.ApprovedAt.DeepCopy()
	if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), key.NamespacedName, &got); err != nil || !got.Status.ApprovedAt.Equal(approvedAt) {
		t.Fatalf("reusable approval changed on replay: status=%+v err=%v", got.Status, err)
	}
}

func TestAIChangeRequestControllerRejectsInvalidGOVARRouteScopes(t *testing.T) {
	now := time.Date(2026, 7, 13, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		mutate    func(*aiopsv1alpha1.AIChangeRequest, []runtime.Object)
		wantPhase aiopsv1alpha1.AIChangeRequestPhase
	}{
		{name: "digest mutation", mutate: func(change *aiopsv1alpha1.AIChangeRequest, _ []runtime.Object) {
			change.Spec.GOVARRouteApproval.RouteSnapshotDigest = strings.Repeat("b", 64)
		}, wantPhase: aiopsv1alpha1.AIChangeRequestPhaseFailed},
		{name: "stale policy generation", mutate: func(change *aiopsv1alpha1.AIChangeRequest, _ []runtime.Object) {
			change.Spec.GOVARRouteApproval.RoutingPolicy.Generation++
			change.Spec.GOVARRouteApproval.ScopeDigest = change.Spec.GOVARRouteApproval.ComputeDigest()
		}, wantPhase: aiopsv1alpha1.AIChangeRequestPhaseFailed},
		{name: "expired", mutate: func(change *aiopsv1alpha1.AIChangeRequest, _ []runtime.Object) {
			change.Spec.GOVARRouteApproval.ValidUntil = metav1.NewTime(now)
			change.Spec.GOVARRouteApproval.ScopeDigest = change.Spec.GOVARRouteApproval.ComputeDigest()
		}, wantPhase: aiopsv1alpha1.AIChangeRequestPhaseExpired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			change, dependencies := govarChangeFixture(now)
			test.mutate(change, dependencies)
			objects := append(dependencies, change)
			client := fakeclient.NewClientBuilder().WithScheme(changeRequestTestScheme(t)).
				WithStatusSubresource(&aiopsv1alpha1.AIChangeRequest{}).WithRuntimeObjects(objects...).Build()
			reconciler := &AIChangeRequestReconciler{Client: client, Now: func() time.Time { return now }}
			key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: change.Namespace, Name: change.Name}}
			for i := 0; i < 2; i++ {
				if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
					t.Fatalf("reconcile %d: %v", i, err)
				}
			}
			var got aiopsv1alpha1.AIChangeRequest
			if err := client.Get(context.Background(), key.NamespacedName, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != test.wantPhase || got.Status.ApprovedAt != nil || got.Status.ApprovedScopeDigest != "" {
				t.Fatalf("phase=%s approval=%+v digest=%q, want phase=%s no approval", got.Status.Phase, got.Status.ApprovedAt, got.Status.ApprovedScopeDigest, test.wantPhase)
			}
		})
	}
}

func govarChangeFixture(now time.Time) (*aiopsv1alpha1.AIChangeRequest, []runtime.Object) {
	routing := &aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "routing", Namespace: "finance", UID: "routing-uid", Generation: 3}}
	provider := &aiopsv1alpha1.AIProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "finance", UID: "provider-uid", Generation: 5}}
	model := &aiopsv1alpha1.AIModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "finance", UID: "model-uid", Generation: 4}, Spec: aiopsv1alpha1.AIModelSpec{ProviderRef: provider.Name}}
	scope := aiopsv1alpha1.GOVARRouteApprovalScope{
		RoutingPolicy:       aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: routing.Name, UID: routing.UID, Generation: routing.Generation},
		Model:               aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: model.Name, UID: model.UID, Generation: model.Generation},
		Provider:            aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: provider.Name, UID: provider.UID, Generation: provider.Generation},
		RouteSnapshotDigest: strings.Repeat("a", 64),
		ValidUntil:          metav1.NewTime(now.Add(time.Hour)),
	}
	scope.ScopeDigest = scope.ComputeDigest()
	change := &aiopsv1alpha1.AIChangeRequest{ObjectMeta: metav1.ObjectMeta{Name: "approve-route", Namespace: "finance", UID: "change-uid", Generation: 1, CreationTimestamp: metav1.NewTime(now.Add(-time.Minute))},
		Spec: aiopsv1alpha1.AIChangeRequestSpec{Action: aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute, Approval: aiopsv1alpha1.AIChangeRequestApprovalApproved, GOVARRouteApproval: &scope, RequestedBy: "system:serviceaccount:operator:manager"}}
	change.Spec.ProposalDigest = change.Spec.ComputeProposalDigest(change.Namespace, change.Name)
	change.Spec.GOVARDecision = &aiopsv1alpha1.GOVARRouteApprovalDecision{
		Outcome: aiopsv1alpha1.AIChangeRequestApprovalApproved, ReviewerIdentity: "reviewer@example.test",
		ReviewerGroup: aiopsv1alpha1.GOVARApprovalReviewerGroup, DecidedAt: metav1.NewTime(now.Add(-10 * time.Second)),
		ValidUntil: scope.ValidUntil, RequestUID: string(change.UID), AdmissionUID: "admission-uid",
		ProposalDigest: change.Spec.ProposalDigest, ScopeDigest: scope.ScopeDigest,
	}
	change.Spec.GOVARDecision.DecisionDigest = change.Spec.GOVARDecision.ComputeDigest(change.Namespace, change.Name, change.Spec.RequestedBy)
	return change, []runtime.Object{routing, model, provider}
}

func changeRequestTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := aiopsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// approvalTestScheme is retained as a package-test helper name for independent
// controller tests that need the full API scheme. It no longer registers any
// request-level approval API because those types have been removed.
func approvalTestScheme(t *testing.T) *runtime.Scheme {
	return changeRequestTestScheme(t)
}
