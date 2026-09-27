package govarpricing

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func pricingProvider(now time.Time) aiopsv1alpha1.AIProvider {
	observed, valid := metav1.NewTime(now.Add(-time.Hour)), metav1.NewTime(now.Add(time.Hour))
	return aiopsv1alpha1.AIProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "synthetic", Generation: 7},
		Spec: aiopsv1alpha1.AIProviderSpec{Type: "openai", Pricing: aiopsv1alpha1.ProviderPricing{
			Currency: "EUR", Version: "synthetic-price-v1", Completeness: aiopsv1alpha1.ProviderPricingComplete, ObservedAt: &observed,
			InputTokenPricePerMillion: resource.MustParse("0.4"), OutputTokenPricePerMillion: resource.MustParse("1.6")},
			GOVAR: &aiopsv1alpha1.AIProviderGOVARSpec{Pricing: &aiopsv1alpha1.AIProviderGOVARPricingSpec{
				Evidence:          aiopsv1alpha1.AIProviderPricingEvidenceSpec{Mode: aiopsv1alpha1.ProviderEvidenceAdminAttested, SourceVersion: "fixture-v1", EvidenceSHA256: strings.Repeat("a", 64), ValidUntil: valid, AdapterVersion: CurrentAdapterVersion},
				InapplicableBases: []aiopsv1alpha1.ProviderBillableBasis{aiopsv1alpha1.ProviderBasisCachedInputTokens, aiopsv1alpha1.ProviderBasisReasoningTokens, aiopsv1alpha1.ProviderBasisRequest, aiopsv1alpha1.ProviderBasisToolCall, aiopsv1alpha1.ProviderBasisMediaUnit, aiopsv1alpha1.ProviderBasisBillableSecond, aiopsv1alpha1.ProviderBasisCancellation, aiopsv1alpha1.ProviderBasisRetryAttempt}}}},
	}
}

func charge(basis aiopsv1alpha1.ProviderBillableBasis, applicability aiopsv1alpha1.ProviderChargeApplicability, field aiopsv1alpha1.ProviderRequestBoundField, price string) aiopsv1alpha1.ProviderBillableCharge {
	return aiopsv1alpha1.ProviderBillableCharge{Basis: basis, Applicability: applicability, Price: resource.MustParse(price), UnitDenominator: 1,
		SettlementUsageField: basis, RequestBoundField: field}
}

func TestNormalizeRequiresClosedPartitionAndDisjointDetailSemantics(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	p := pricingProvider(now)
	p.Spec.GOVAR.Pricing.InapplicableBases = removeBasis(p.Spec.GOVAR.Pricing.InapplicableBases, aiopsv1alpha1.ProviderBasisCachedInputTokens)
	cached := charge(aiopsv1alpha1.ProviderBasisCachedInputTokens, aiopsv1alpha1.ProviderChargeRequestDeclared, "max_cached_input_tokens", "0.2")
	p.Spec.GOVAR.Pricing.Charges = []aiopsv1alpha1.ProviderBillableCharge{cached}
	if _, _, err := Normalize(p, now); err == nil || !strings.Contains(err.Error(), "disjoint") {
		t.Fatalf("overlapping cached charge accepted: %v", err)
	}
	p.Spec.GOVAR.Pricing.Charges[0].DisjointUsage = true
	snapshot, evidence, err := Normalize(p, now)
	if err != nil || evidence.Mode != aiopsv1alpha1.ProviderEvidenceAdminAttested || snapshot.SnapshotSHA256 != SnapshotDigest(snapshot) {
		t.Fatalf("normalize=(%+v,%+v,%v)", snapshot, evidence, err)
	}
	if err := ValidateSnapshot(snapshot, p.Generation, now.Add(2*time.Hour)); err == nil {
		t.Fatal("expired snapshot accepted at explicit evaluation time")
	}
}

func TestIncludedChargesResolveToExistingChargedBaseRoot(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	base := pricingProvider(now)
	base.Spec.GOVAR.Pricing.InapplicableBases = removeBasis(base.Spec.GOVAR.Pricing.InapplicableBases, aiopsv1alpha1.ProviderBasisCachedInputTokens)
	cached := charge(aiopsv1alpha1.ProviderBasisCachedInputTokens, aiopsv1alpha1.ProviderChargeRequestDeclared, "", "0")

	t.Run("legal detail resolves to charged root", func(t *testing.T) {
		provider := base.DeepCopy()
		parent := aiopsv1alpha1.ProviderBasisInputTokens
		cached.IncludedIn = &parent
		provider.Spec.GOVAR.Pricing.Charges = []aiopsv1alpha1.ProviderBillableCharge{cached}
		snapshot, _, err := Normalize(*provider, now)
		if err != nil {
			t.Fatal(err)
		}
		changedGraph := snapshot
		changedGraph.Charges = append([]NormalizedCharge(nil), snapshot.Charges...)
		for i := range changedGraph.Charges {
			if changedGraph.Charges[i].Basis == aiopsv1alpha1.ProviderBasisCachedInputTokens {
				changedGraph.Charges[i].IncludedIn = nil
			}
		}
		if SnapshotDigest(changedGraph) == snapshot.SnapshotSHA256 {
			t.Fatal("included-charge graph change did not alter snapshot digest")
		}
		components, err := ReserveComponents(snapshot, RequestChargeContext{InputTokens: 100, OutputTokens: 20}, now)
		if err != nil || len(components) != 2 {
			t.Fatalf("components=%+v err=%v", components, err)
		}
		for _, component := range components {
			if component.Basis == aiopsv1alpha1.ProviderBasisCachedInputTokens {
				t.Fatal("included detail was charged separately")
			}
		}
	})

	t.Run("inapplicable parent is rejected", func(t *testing.T) {
		provider := base.DeepCopy()
		parent := aiopsv1alpha1.ProviderBasisRequest
		cached.IncludedIn = &parent
		provider.Spec.GOVAR.Pricing.Charges = []aiopsv1alpha1.ProviderBillableCharge{cached}
		if _, _, err := Normalize(*provider, now); err == nil || !strings.Contains(err.Error(), "charged parent") {
			t.Fatalf("inapplicable parent accepted: %v", err)
		}
	})

	t.Run("cycle is rejected by normalization and integrity", func(t *testing.T) {
		provider := pricingProvider(now)
		provider.Spec.GOVAR.Pricing.InapplicableBases = removeBasis(provider.Spec.GOVAR.Pricing.InapplicableBases, aiopsv1alpha1.ProviderBasisToolCall)
		provider.Spec.GOVAR.Pricing.InapplicableBases = removeBasis(provider.Spec.GOVAR.Pricing.InapplicableBases, aiopsv1alpha1.ProviderBasisMediaUnit)
		tool, media := aiopsv1alpha1.ProviderBasisToolCall, aiopsv1alpha1.ProviderBasisMediaUnit
		toolCharge := charge(tool, aiopsv1alpha1.ProviderChargeRequestDeclared, "", "0")
		mediaCharge := charge(media, aiopsv1alpha1.ProviderChargeRequestDeclared, "", "0")
		toolCharge.IncludedIn, mediaCharge.IncludedIn = &media, &tool
		provider.Spec.GOVAR.Pricing.Charges = []aiopsv1alpha1.ProviderBillableCharge{toolCharge, mediaCharge}
		if _, _, err := Normalize(provider, now); err == nil || !strings.Contains(err.Error(), "cycle") {
			t.Fatalf("cycle accepted by normalization: %v", err)
		}

		snapshot, _, err := Normalize(pricingProvider(now), now)
		if err != nil {
			t.Fatal(err)
		}
		snapshot.InapplicableBases = removeBasis(snapshot.InapplicableBases, tool)
		snapshot.InapplicableBases = removeBasis(snapshot.InapplicableBases, media)
		snapshot.Charges = append(snapshot.Charges,
			NormalizedCharge{Basis: tool, Applicability: aiopsv1alpha1.ProviderChargeRequestDeclared, IncludedIn: &media, SettlementUsageField: tool, UnitDenominator: 1},
			NormalizedCharge{Basis: media, Applicability: aiopsv1alpha1.ProviderChargeRequestDeclared, IncludedIn: &tool, SettlementUsageField: media, UnitDenominator: 1},
		)
		snapshot.SnapshotSHA256 = SnapshotDigest(snapshot)
		if err := ValidateSnapshotIntegrity(snapshot); err == nil || !strings.Contains(err.Error(), "cycle") {
			t.Fatalf("cycle accepted by snapshot integrity: %v", err)
		}
		if _, err := ReserveComponents(snapshot, RequestChargeContext{InputTokens: 1, OutputTokens: 1}, now); err == nil {
			t.Fatal("cyclic snapshot reached reservation")
		}
	})
}

func TestProviderAdapterSupportFailsClosed(t *testing.T) {
	for _, providerType := range []string{"anthropic", "vertex", "mistral", "self-hosted", "custom", "bedrock"} {
		if _, err := ForProviderType(providerType); err == nil {
			t.Fatalf("unsupported provider %q received a pricing/usage adapter", providerType)
		}
	}
	if parameter, ok := OutputCapRequestParameter("openai", aiopsv1alpha1.GOVARRouteOpenAIBody); !ok || parameter != "max_output_tokens" {
		t.Fatalf("OpenAI cap adapter=(%q,%v)", parameter, ok)
	}
	if parameter, ok := OutputCapRequestParameter("azure-openai", aiopsv1alpha1.GOVARRouteAzureDeploymentPath); !ok || parameter != "max_output_tokens" {
		t.Fatalf("Azure OpenAI cap adapter=(%q,%v)", parameter, ok)
	}
	if _, ok := OutputCapRequestParameter("anthropic", aiopsv1alpha1.GOVARRouteAnthropicBody); ok {
		t.Fatal("Anthropic cap was accepted without an authoritative response adapter")
	}
}

func TestReserveAllClosedChargeBasesAndRequireDeclaredZero(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	p := pricingProvider(now)
	p.Spec.GOVAR.Pricing.InapplicableBases = nil
	maximum := int64(3)
	p.Spec.GOVAR.Pricing.Charges = []aiopsv1alpha1.ProviderBillableCharge{
		charge(aiopsv1alpha1.ProviderBasisCachedInputTokens, aiopsv1alpha1.ProviderChargeRequestDeclared, "max_cached_input_tokens", "0.2"),
		charge(aiopsv1alpha1.ProviderBasisReasoningTokens, aiopsv1alpha1.ProviderChargeRequestDeclared, "max_reasoning_tokens", "0.3"),
		charge(aiopsv1alpha1.ProviderBasisRequest, aiopsv1alpha1.ProviderChargeAlways, "", "0.000001"),
		charge(aiopsv1alpha1.ProviderBasisToolCall, aiopsv1alpha1.ProviderChargeRequestDeclared, "max_tool_calls", "0.000002"),
		charge(aiopsv1alpha1.ProviderBasisMediaUnit, aiopsv1alpha1.ProviderChargeRequestDeclared, "max_media_units", "0.000003"),
		charge(aiopsv1alpha1.ProviderBasisBillableSecond, aiopsv1alpha1.ProviderChargeRequestDeclared, "timeout_seconds", "0.000004"),
		charge(aiopsv1alpha1.ProviderBasisCancellation, aiopsv1alpha1.ProviderChargeProviderResponse, "", "0.000005"),
		charge(aiopsv1alpha1.ProviderBasisRetryAttempt, aiopsv1alpha1.ProviderChargeRequestDeclared, "max_retry_attempts", "0.000006"),
	}
	p.Spec.GOVAR.Pricing.Charges[0].DisjointUsage, p.Spec.GOVAR.Pricing.Charges[1].DisjointUsage = true, true
	p.Spec.GOVAR.Pricing.Charges[2].MaximumQuantity = ptr64(1)
	p.Spec.GOVAR.Pricing.Charges[6].MaximumQuantity = &maximum
	snapshot, _, err := Normalize(p, now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := RequestChargeContext{InputTokens: 100, OutputTokens: 200, DeclaredBounds: []UsageQuantity{
		{aiopsv1alpha1.ProviderBasisCachedInputTokens, 0}, {aiopsv1alpha1.ProviderBasisReasoningTokens, 10}, {aiopsv1alpha1.ProviderBasisToolCall, 2},
		{aiopsv1alpha1.ProviderBasisMediaUnit, 1}, {aiopsv1alpha1.ProviderBasisBillableSecond, 30}, {aiopsv1alpha1.ProviderBasisRetryAttempt, 2},
	}}
	components, err := ReserveComponents(snapshot, ctx, now)
	if err != nil || len(components) != len(AllBases) {
		t.Fatalf("components=%+v err=%v", components, err)
	}
	ctx.DeclaredBounds = ctx.DeclaredBounds[1:]
	if _, err := ReserveComponents(snapshot, ctx, now); err == nil || !strings.Contains(err.Error(), "cached_input_tokens") {
		t.Fatalf("missing declared zero accepted: %v", err)
	}
}

func TestSettleUsesFrozenVectorReportsMissingUnknownAndExceeded(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	p := pricingProvider(now)
	snapshot, _, err := Normalize(p, now)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := ReserveComponents(snapshot, RequestChargeContext{InputTokens: 100, OutputTokens: 200}, now)
	if err != nil {
		t.Fatal(err)
	}
	actual, missing, exceeded, err := SettleComponents(snapshot, reserved, []UsageQuantity{{aiopsv1alpha1.ProviderBasisInputTokens, 101}})
	if err != nil || !exceeded || !reflect.DeepEqual(missing, []aiopsv1alpha1.ProviderBillableBasis{aiopsv1alpha1.ProviderBasisOutputTokens}) || len(actual) != 1 {
		t.Fatalf("actual=%+v missing=%v exceeded=%v err=%v", actual, missing, exceeded, err)
	}
	changed := snapshot
	changed.Charges = append([]NormalizedCharge(nil), snapshot.Charges...)
	changed.Charges[0].PriceMicrosPerUnit++
	changed.SnapshotSHA256 = SnapshotDigest(changed)
	// Settlement against a current changed snapshot must not be substituted for
	// the frozen one: the reserved vector identity detects the mismatch.
	if _, _, _, err := SettleComponents(changed, reserved, []UsageQuantity{{aiopsv1alpha1.ProviderBasisInputTokens, 1}, {aiopsv1alpha1.ProviderBasisOutputTokens, 1}}); err == nil {
		t.Fatal("changed price accepted against frozen reserved vector")
	}
	if _, _, _, err := SettleComponents(snapshot, reserved, []UsageQuantity{{aiopsv1alpha1.ProviderBasisInputTokens, 1}, {aiopsv1alpha1.ProviderBasisOutputTokens, 1}, {aiopsv1alpha1.ProviderBasisToolCall, 1}}); err == nil {
		t.Fatal("unlisted provider category accepted")
	}
}

func TestCeilingAndOverflow(t *testing.T) {
	if got, err := ceilingProduct(1, 1, 3); err != nil || got != 1 {
		t.Fatalf("ceil=%d err=%v", got, err)
	}
	if _, err := ceilingProduct(math.MaxInt64, math.MaxInt64, 1); err == nil {
		t.Fatal("overflow accepted")
	}
}

func FuzzSnapshotDigestStable(f *testing.F) {
	f.Add(int64(1), int64(2))
	f.Fuzz(func(t *testing.T, input, output int64) {
		if input < 0 || output < 0 {
			t.Skip()
		}
		now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
		p := pricingProvider(now)
		p.Spec.Pricing.InputTokenPricePerMillion = *resource.NewQuantity(input, resource.DecimalSI)
		p.Spec.Pricing.OutputTokenPricePerMillion = *resource.NewQuantity(output, resource.DecimalSI)
		snapshot, _, err := Normalize(p, now)
		if err != nil {
			return
		}
		if snapshot.SnapshotSHA256 != SnapshotDigest(snapshot) {
			t.Fatal("digest is not stable")
		}
	})
}

func removeBasis(in []aiopsv1alpha1.ProviderBillableBasis, target aiopsv1alpha1.ProviderBillableBasis) []aiopsv1alpha1.ProviderBillableBasis {
	var out []aiopsv1alpha1.ProviderBillableBasis
	for _, basis := range in {
		if basis != target {
			out = append(out, basis)
		}
	}
	return out
}
func ptr64(v int64) *int64 { return &v }
