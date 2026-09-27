package govar

import (
	"testing"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildCandidatesFiltersByGovernanceAndTarget(t *testing.T) {
	now := metav1.NewTime(time.Now().UTC())
	ready := []metav1.Condition{{Type: aiopsv1alpha1.ConditionReady, Status: metav1.ConditionTrue}}
	models := []aiopsv1alpha1.AIModel{
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "finance", Name: "gpt-fr", UID: "model-fr-uid", Generation: 1, ResourceVersion: "m1"},
			Spec: aiopsv1alpha1.AIModelSpec{
				ProviderRef:          "azure-fr",
				ModelName:            "gpt-4.1-mini",
				ContextWindow:        128000,
				QualityTier:          aiopsv1alpha1.TierHigh,
				CostTier:             aiopsv1alpha1.TierMedium,
				SensitiveDataAllowed: true,
				ServesNamespace:      "finance",
				ServesApplication:    "copilot",
				GOVAR:                &aiopsv1alpha1.AIModelGOVARSpec{Routable: true, RouteBindingRef: "primary"},
			},
			Status: aiopsv1alpha1.AIModelStatus{ObservedGeneration: 1, LastQualityScore: 0.9, LastEvaluatedAt: &now, Conditions: ready,
				GOVAR: &aiopsv1alpha1.AIModelGOVARStatus{VerifiedOutputCap: &aiopsv1alpha1.AIModelVerifiedOutputCapStatus{Verified: true, MaxOutputTokens: 4096, ObservedAt: now, SourceVersion: "cap-v1"}}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "finance", Name: "gpt-us", UID: "model-us-uid", Generation: 1, ResourceVersion: "m2"},
			Spec: aiopsv1alpha1.AIModelSpec{
				ProviderRef:          "azure-us",
				ModelName:            "gpt-4.1-mini",
				ContextWindow:        128000,
				SensitiveDataAllowed: false,
				ServesNamespace:      "finance",
				ServesApplication:    "copilot",
				GOVAR:                &aiopsv1alpha1.AIModelGOVARSpec{Routable: true, RouteBindingRef: "primary"},
			},
			Status: aiopsv1alpha1.AIModelStatus{ObservedGeneration: 1, LastQualityScore: 0.9, LastEvaluatedAt: &now, Conditions: ready,
				GOVAR: &aiopsv1alpha1.AIModelGOVARStatus{VerifiedOutputCap: &aiopsv1alpha1.AIModelVerifiedOutputCapStatus{Verified: true, MaxOutputTokens: 4096, ObservedAt: now, SourceVersion: "cap-v1"}}},
		},
	}
	providers := map[string]aiopsv1alpha1.AIProvider{
		"azure-fr": {
			ObjectMeta: metav1.ObjectMeta{Namespace: "finance", Name: "azure-fr", UID: "provider-fr-uid", Generation: 1, ResourceVersion: "p1"},
			Spec: aiopsv1alpha1.AIProviderSpec{
				Type:          "azure-openai",
				Region:        "francecentral",
				DataResidency: "eu",
				Managed:       true,
				Pricing: aiopsv1alpha1.ProviderPricing{
					Currency:                   "EUR",
					InputTokenPricePerMillion:  resource.MustParse("0.4"),
					OutputTokenPricePerMillion: resource.MustParse("1.6"),
					Version:                    "prices-v1", ObservedAt: &now, Completeness: aiopsv1alpha1.ProviderPricingComplete,
				},
				Compliance: aiopsv1alpha1.ProviderCompliance{
					AllowedForSensitiveData: true,
				},
				GOVAR: &aiopsv1alpha1.AIProviderGOVARSpec{GatewayRoutes: []aiopsv1alpha1.AIProviderGatewayRouteBinding{{Name: "primary", ProviderDeployment: "gpt-fr-deployment", Cluster: "azure-fr", Authority: "fr.example", PathMode: aiopsv1alpha1.GOVARRouteAzureDeploymentPath}}},
			},
			Status: aiopsv1alpha1.AIProviderStatus{ObservedGeneration: 1, Conditions: ready},
		},
		"azure-us": {
			ObjectMeta: metav1.ObjectMeta{Namespace: "finance", Name: "azure-us", UID: "provider-us-uid", Generation: 1, ResourceVersion: "p2"},
			Spec: aiopsv1alpha1.AIProviderSpec{
				Type:          "azure-openai",
				Region:        "eastus",
				DataResidency: "us",
				Managed:       true,
				Pricing: aiopsv1alpha1.ProviderPricing{
					Currency:                   "EUR",
					InputTokenPricePerMillion:  resource.MustParse("0.2"),
					OutputTokenPricePerMillion: resource.MustParse("0.8"),
					Version:                    "prices-v1", ObservedAt: &now, Completeness: aiopsv1alpha1.ProviderPricingComplete,
				},
				GOVAR: &aiopsv1alpha1.AIProviderGOVARSpec{GatewayRoutes: []aiopsv1alpha1.AIProviderGatewayRouteBinding{{Name: "primary", ProviderDeployment: "gpt-us-deployment", Cluster: "azure-us", Authority: "us.example", PathMode: aiopsv1alpha1.GOVARRouteAzureDeploymentPath}}},
			},
			Status: aiopsv1alpha1.AIProviderStatus{ObservedGeneration: 1, Conditions: ready},
		},
	}
	for name, provider := range providers {
		attachTestPricingStatus(t, &provider, now)
		providers[name] = provider
	}
	for i := range models {
		provider := providers[models[i].Spec.ProviderRef]
		binding := provider.Spec.GOVAR.GatewayRoutes[0]
		models[i].Status.GOVAR.VerifiedOutputCap = testCapStatus(provider, binding.ProviderDeployment, models[i].Spec.ModelName, now)
	}

	got := BuildCandidates(RequestContext{
		Namespace:     "finance",
		Application:   "copilot",
		SensitiveData: true,
		AllowedZones:  []string{"eu"},
	}, models, providers)

	var feasible []Candidate
	for _, candidate := range got {
		if candidate.Feasible {
			feasible = append(feasible, candidate)
		}
	}
	if len(feasible) != 1 {
		t.Fatalf("expected 1 feasible candidate, got %+v", got)
	}
	if feasible[0].ProviderRef != "azure-fr" {
		t.Fatalf("expected azure-fr candidate, got %s", feasible[0].ProviderRef)
	}
	if feasible[0].InputPriceMicrosPerMillion != 400_000 || feasible[0].OutputPriceMicrosPerMillion != 1_600_000 {
		t.Fatalf("unexpected pricing snapshot: %+v", feasible[0])
	}
	if err := ValidateRouteSnapshot(feasible[0].RouteSnapshot); err != nil || feasible[0].SnapshotVersion != feasible[0].RouteSnapshot.SnapshotHash {
		t.Fatalf("invalid route snapshot: %+v err=%v", feasible[0].RouteSnapshot, err)
	}
}

func TestBuildCandidatesRejectsFutureQualityTimestamp(t *testing.T) {
	future := metav1.NewTime(time.Now().Add(5 * time.Minute))
	model := aiopsv1alpha1.AIModel{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "future", UID: "future-uid", Generation: 1, ResourceVersion: "m1"}, Spec: aiopsv1alpha1.AIModelSpec{
		ProviderRef: "provider", ModelName: "future", ContextWindow: 1024,
		GOVAR: &aiopsv1alpha1.AIModelGOVARSpec{Routable: true, RouteBindingRef: "primary"},
	}, Status: aiopsv1alpha1.AIModelStatus{ObservedGeneration: 1, LastQualityScore: 1, LastEvaluatedAt: &future, Conditions: []metav1.Condition{{Type: aiopsv1alpha1.ConditionReady, Status: metav1.ConditionTrue}}}}
	provider := aiopsv1alpha1.AIProvider{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "provider", UID: "provider-uid", Generation: 1, ResourceVersion: "p1"}, Spec: aiopsv1alpha1.AIProviderSpec{Type: "openai", Pricing: aiopsv1alpha1.ProviderPricing{
		Currency: "EUR", InputTokenPricePerMillion: resource.MustParse("1"), OutputTokenPricePerMillion: resource.MustParse("1"), Version: "v1", ObservedAt: ptrTime(metav1.Now()), Completeness: aiopsv1alpha1.ProviderPricingComplete,
	}, GOVAR: &aiopsv1alpha1.AIProviderGOVARSpec{GatewayRoutes: []aiopsv1alpha1.AIProviderGatewayRouteBinding{{Name: "primary", ProviderDeployment: "future", Cluster: "provider", Authority: "provider.example", PathMode: aiopsv1alpha1.GOVARRouteOpenAIBody}}}}, Status: aiopsv1alpha1.AIProviderStatus{ObservedGeneration: 1, Conditions: []metav1.Condition{{Type: aiopsv1alpha1.ConditionReady, Status: metav1.ConditionTrue}}}}
	got := BuildCandidates(RequestContext{}, []aiopsv1alpha1.AIModel{model}, map[string]aiopsv1alpha1.AIProvider{"provider": provider})
	if len(got) != 1 || got[0].InfeasibleReason != ReasonQualityStale {
		t.Fatalf("candidates=%+v", got)
	}
}

func TestProviderPathCompatibilityFailsBedrockClosed(t *testing.T) {
	if providerPathCompatible("bedrock", string(aiopsv1alpha1.GOVARRouteAnthropicBody)) {
		t.Fatal("Bedrock was incorrectly authorized through the Anthropic API adapter")
	}
	if providerPathCompatible("custom", string(aiopsv1alpha1.GOVARRouteOpenAIBody)) {
		t.Fatal("custom provider was accepted without a closed authoritative usage adapter")
	}
	if providerPathCompatible("custom", string(aiopsv1alpha1.GOVARRouteAnthropicBody)) {
		t.Fatal("custom provider bypassed the explicit OpenAI-compatible adapter restriction")
	}
}

func ptrTime(value metav1.Time) *metav1.Time { return &value }

func attachTestPricingStatus(t *testing.T, provider *aiopsv1alpha1.AIProvider, now metav1.Time) {
	t.Helper()
	future := metav1.NewTime(now.Add(24 * time.Hour))
	provider.Spec.GOVAR.Pricing = &aiopsv1alpha1.AIProviderGOVARPricingSpec{Evidence: aiopsv1alpha1.AIProviderPricingEvidenceSpec{
		Mode: aiopsv1alpha1.ProviderEvidenceAdminAttested, SourceVersion: "synthetic-v1", EvidenceSHA256: testSHA("provider-pricing-" + provider.Name), ValidUntil: future, AdapterVersion: govarpricing.CurrentAdapterVersion},
		InapplicableBases: []aiopsv1alpha1.ProviderBillableBasis{aiopsv1alpha1.ProviderBasisCachedInputTokens, aiopsv1alpha1.ProviderBasisReasoningTokens, aiopsv1alpha1.ProviderBasisRequest, aiopsv1alpha1.ProviderBasisToolCall, aiopsv1alpha1.ProviderBasisMediaUnit, aiopsv1alpha1.ProviderBasisBillableSecond, aiopsv1alpha1.ProviderBasisCancellation, aiopsv1alpha1.ProviderBasisRetryAttempt}}
	snapshot, _, err := govarpricing.Normalize(*provider, now.Time)
	if err != nil {
		t.Fatalf("normalize test provider: %v", err)
	}
	provider.Status.GOVAR = &aiopsv1alpha1.AIProviderGOVARStatus{PricingSnapshot: &snapshot}
}

func testCapStatus(provider aiopsv1alpha1.AIProvider, deployment, model string, now metav1.Time) *aiopsv1alpha1.AIModelVerifiedOutputCapStatus {
	return &aiopsv1alpha1.AIModelVerifiedOutputCapStatus{Verified: true, MaxOutputTokens: 4096, ObservedAt: now, SourceVersion: "cap-v1",
		ProviderUID: string(provider.UID), ProviderGeneration: provider.Generation, ProviderDeployment: deployment, ModelVersion: model,
		CapabilityAdapterVersion: govarpricing.CurrentAdapterVersion, EvidenceMode: aiopsv1alpha1.ProviderEvidenceAdminAttested,
		EvidenceSHA256: testSHA("cap-" + provider.Name), ValidUntil: metav1.NewTime(now.Add(24 * time.Hour)), RequestParameter: "max_output_tokens", EnforcedByPathAdapter: true}
}

func TestBuildPolicySnapshotCarriesBudgetAndRoutingGuardrails(t *testing.T) {
	budget := aiopsv1alpha1.AIBudgetPolicy{
		Spec: aiopsv1alpha1.AIBudgetPolicySpec{
			Target:           aiopsv1alpha1.BudgetTarget{Namespace: "finance", Team: "treasury"},
			BudgetEUR:        resource.MustParse("125.5"),
			FallbackModelRef: "cheap-model",
			EnforcementMode:  aiopsv1alpha1.EnforcementMode("enforce"),
			FallbackOnPhase:  aiopsv1alpha1.BudgetFallbackOnCritical,
		},
	}
	routing := aiopsv1alpha1.AIRoutingPolicy{
		Spec: aiopsv1alpha1.AIRoutingPolicySpec{
			Objective: "cost",
			Guardrails: aiopsv1alpha1.AIRoutingPolicyGuardrails{
				MinQualityScore:              0.82,
				MaxLatencyMillis:             1200,
				RequireSovereigntyCompliance: true,
			},
		},
	}

	got := BuildPolicySnapshot(budget, routing)
	if got.BudgetMicros != 125_500_000 {
		t.Fatalf("expected exact integer budget snapshot, got %v", got.BudgetMicros)
	}
	if got.Objective != "cost" || got.MinQualityScore != 0.82 || got.MaxLatencyMillis != 1200 {
		t.Fatalf("unexpected policy snapshot: %+v", got)
	}
	if got.BudgetScope.Namespace != "finance" || got.BudgetScope.Team != "treasury" {
		t.Fatalf("unexpected budget scope: %+v", got.BudgetScope)
	}
}
