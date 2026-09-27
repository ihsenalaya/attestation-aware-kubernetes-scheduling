package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
)

func completeProviderFixture(now time.Time) *aiopsv1alpha1.AIProvider {
	observed, valid := metav1.NewTime(now.Add(-time.Hour)), metav1.NewTime(now.Add(time.Hour))
	return &aiopsv1alpha1.AIProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "finance", UID: "provider-uid", Generation: 3}, Spec: aiopsv1alpha1.AIProviderSpec{
		Type: "openai", Pricing: aiopsv1alpha1.ProviderPricing{Currency: "EUR", InputTokenPricePerMillion: resource.MustParse("0.4"), OutputTokenPricePerMillion: resource.MustParse("1.6"), Version: "price-v1", ObservedAt: &observed, Completeness: aiopsv1alpha1.ProviderPricingComplete},
		GOVAR: &aiopsv1alpha1.AIProviderGOVARSpec{GatewayRoutes: []aiopsv1alpha1.AIProviderGatewayRouteBinding{{Name: "primary", ProviderDeployment: "deployment-v1", Cluster: "backend", Authority: "backend.example", PathMode: aiopsv1alpha1.GOVARRouteOpenAIBody}}, Pricing: &aiopsv1alpha1.AIProviderGOVARPricingSpec{
			Evidence:          aiopsv1alpha1.AIProviderPricingEvidenceSpec{Mode: aiopsv1alpha1.ProviderEvidenceAdminAttested, SourceVersion: "catalog-v1", EvidenceSHA256: strings.Repeat("a", 64), ValidUntil: valid, AdapterVersion: govarpricing.CurrentAdapterVersion},
			InapplicableBases: []aiopsv1alpha1.ProviderBillableBasis{aiopsv1alpha1.ProviderBasisCachedInputTokens, aiopsv1alpha1.ProviderBasisReasoningTokens, aiopsv1alpha1.ProviderBasisRequest, aiopsv1alpha1.ProviderBasisToolCall, aiopsv1alpha1.ProviderBasisMediaUnit, aiopsv1alpha1.ProviderBasisBillableSecond, aiopsv1alpha1.ProviderBasisCancellation, aiopsv1alpha1.ProviderBasisRetryAttempt}}},
	}}
}

func TestPricingAndOutputCapControllersProduceBoundStatusAndClearStale(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	provider := completeProviderFixture(now)
	scheme := approvalTestScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&aiopsv1alpha1.AIProvider{}, &aiopsv1alpha1.AIModel{}).WithObjects(provider).Build()
	providerReconciler := &AIProviderReconciler{Client: c, Now: func() time.Time { return now }}
	key := types.NamespacedName{Namespace: provider.Namespace, Name: provider.Name}
	if _, err := providerReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	var produced aiopsv1alpha1.AIProvider
	if err := c.Get(ctx, key, &produced); err != nil {
		t.Fatal(err)
	}
	if produced.Status.GOVAR == nil || produced.Status.GOVAR.PricingSnapshot == nil || !meta.IsStatusConditionTrue(produced.Status.Conditions, aiopsv1alpha1.ConditionReady) {
		t.Fatalf("provider status=%+v", produced.Status)
	}
	snapshot := produced.Status.GOVAR.PricingSnapshot.DeepCopy()

	valid := metav1.NewTime(now.Add(time.Hour))
	model := &aiopsv1alpha1.AIModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "finance", UID: "model-uid", Generation: 4}, Spec: aiopsv1alpha1.AIModelSpec{ProviderRef: provider.Name, ModelName: "model-v1", ContextWindow: 8192, GOVAR: &aiopsv1alpha1.AIModelGOVARSpec{Routable: true, RouteBindingRef: "primary", OutputCapEvidence: &aiopsv1alpha1.AIModelOutputCapEvidenceSpec{MaxOutputTokens: 2048, RequestParameter: "max_output_tokens", EnforcedByPathAdapter: true, Mode: aiopsv1alpha1.ProviderEvidenceAdminAttested, SourceVersion: "cap-v1", EvidenceSHA256: strings.Repeat("b", 64), ValidUntil: valid, CapabilityAdapterVersion: govarpricing.CurrentAdapterVersion}}}}
	if err := c.Create(ctx, model); err != nil {
		t.Fatal(err)
	}
	modelReconciler := &AIModelReconciler{Client: c, Now: func() time.Time { return now }}
	modelKey := types.NamespacedName{Namespace: model.Namespace, Name: model.Name}
	if _, err := modelReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: modelKey}); err != nil {
		t.Fatal(err)
	}
	var got aiopsv1alpha1.AIModel
	if err := c.Get(ctx, modelKey, &got); err != nil {
		t.Fatal(err)
	}
	cap := got.Status.GOVAR.VerifiedOutputCap
	if cap == nil || !cap.Verified || cap.ProviderUID != string(provider.UID) || cap.ProviderGeneration != provider.Generation || cap.ProviderDeployment != "deployment-v1" || cap.PricingSnapshotSHA256 != snapshot.SnapshotSHA256 || !meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady) {
		t.Fatalf("cap status=%+v conditions=%+v", cap, got.Status.Conditions)
	}

	// A provider status generation mismatch invalidates and clears the old cap;
	// controller-owned evidence is never carried through a failed refresh.
	if err := c.Get(ctx, key, &produced); err != nil {
		t.Fatal(err)
	}
	produced.Status.ObservedGeneration = produced.Generation - 1
	if err := c.Status().Update(ctx, &produced); err != nil {
		t.Fatal(err)
	}
	if _, err := modelReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: modelKey}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, modelKey, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.GOVAR != nil || meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady) {
		t.Fatalf("stale cap survived: %+v", got.Status)
	}
}

func TestProviderControllerRejectsIncompleteAndClearsPriorSnapshot(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	provider := completeProviderFixture(now)
	snapshot, _, err := govarpricing.Normalize(*provider, now)
	if err != nil {
		t.Fatal(err)
	}
	provider.Status = aiopsv1alpha1.AIProviderStatus{ObservedGeneration: provider.Generation, GOVAR: &aiopsv1alpha1.AIProviderGOVARStatus{PricingSnapshot: &snapshot}}
	provider.Spec.GOVAR.Pricing.InapplicableBases = provider.Spec.GOVAR.Pricing.InapplicableBases[1:] // cached basis becomes unknown
	c := fakeclient.NewClientBuilder().WithScheme(approvalTestScheme(t)).WithStatusSubresource(&aiopsv1alpha1.AIProvider{}).WithObjects(provider).Build()
	r := &AIProviderReconciler{Client: c, Now: func() time.Time { return now }}
	key := client.ObjectKeyFromObject(provider)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	var got aiopsv1alpha1.AIProvider
	if err := c.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.GOVAR != nil || meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady) {
		t.Fatalf("incomplete pricing remained executable: %+v", got.Status)
	}
}
