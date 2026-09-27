package govar

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RouteSnapshot = aiopsv1alpha1.GOVARRouteSnapshot

const (
	AnnotationRoutable          = "aiops.imperium.io/routable"
	AnnotationPricingVersion    = "aiops.imperium.io/pricing-version"
	AnnotationPricingObservedAt = "aiops.imperium.io/pricing-observed-at"
	AnnotationOutputCapVerified = "aiops.imperium.io/output-cap-verified"
	AnnotationLatencyMillis     = "aiops.imperium.io/latency-millis"
	AnnotationLatencyObservedAt = "aiops.imperium.io/latency-observed-at"
	observationFreshnessLimit   = 24 * time.Hour
)

type RequestContext struct {
	Namespace     string
	Team          string
	Application   string
	SensitiveData bool
	AllowedZones  []string
}

type Candidate struct {
	ModelRef                    string
	ModelName                   string
	ProviderRef                 string
	ProviderType                string
	Region                      string
	DataResidency               string
	Managed                     bool
	InputPriceMicrosPerMillion  int64
	OutputPriceMicrosPerMillion int64
	QualityTier                 aiopsv1alpha1.Tier
	CostTier                    aiopsv1alpha1.Tier
	SensitiveDataAllowed        bool
	PricingVersion              string
	SnapshotVersion             string
	ContextWindow               int64
	QualityScore                float64
	QualityObservedAt           time.Time
	VerifiedOutputCap           bool
	VerifiedOutputCapTokens     int64
	CapEvidenceDigest           string
	PricingSnapshot             govarpricing.NormalizedPricingSnapshot
	Feasible                    bool
	InfeasibleReason            ReasonCode
	LatencyMillis               int64
	LatencyObservedAt           time.Time
	RouteSnapshot               RouteSnapshot
}

type PolicySnapshot struct {
	BudgetScope                  aiopsv1alpha1.BudgetTarget
	BudgetMicros                 MoneyMicros
	FallbackModelRef             string
	EnforcementMode              aiopsv1alpha1.EnforcementMode
	FallbackOnPhase              aiopsv1alpha1.BudgetFallbackPhase
	Objective                    string
	MinQualityScore              float64
	MaxLatencyMillis             int32
	RequireSovereigntyCompliance bool
}

func BuildPolicySnapshot(budget aiopsv1alpha1.AIBudgetPolicy, routing aiopsv1alpha1.AIRoutingPolicy) PolicySnapshot {
	budgetMicros, _ := quantityToMicros(budget.Spec.BudgetEUR)
	return PolicySnapshot{
		BudgetScope:                  budget.Spec.Target,
		BudgetMicros:                 budgetMicros,
		FallbackModelRef:             budget.Spec.FallbackModelRef,
		EnforcementMode:              budget.Spec.EnforcementMode,
		FallbackOnPhase:              budget.Spec.FallbackOnPhase,
		Objective:                    routing.Spec.Objective,
		MinQualityScore:              routing.Spec.Guardrails.MinQualityScore,
		MaxLatencyMillis:             routing.Spec.Guardrails.MaxLatencyMillis,
		RequireSovereigntyCompliance: routing.Spec.Guardrails.RequireSovereigntyCompliance,
	}
}

func BuildCandidates(req RequestContext, models []aiopsv1alpha1.AIModel, providers map[string]aiopsv1alpha1.AIProvider) []Candidate {
	return BuildCandidatesAt(req, models, providers, time.Now().UTC())
}

func BuildCandidatesAt(req RequestContext, models []aiopsv1alpha1.AIModel, providers map[string]aiopsv1alpha1.AIProvider, evaluatedAt time.Time) []Candidate {
	var out []Candidate
	for i := range models {
		model := models[i]
		provider, ok := providers[model.Spec.ProviderRef]
		if !ok {
			out = append(out, Candidate{ModelRef: model.Name, InfeasibleReason: ReasonProviderUnavailable})
			continue
		}
		candidate := Candidate{ModelRef: model.Name, ModelName: model.Spec.ModelName, ProviderRef: provider.Name,
			ProviderType: provider.Spec.Type, Region: provider.Spec.Region, DataResidency: provider.Spec.DataResidency,
			Managed: provider.Spec.Managed, ContextWindow: int64(model.Spec.ContextWindow), QualityTier: model.Spec.QualityTier,
			CostTier: model.Spec.CostTier, QualityScore: model.Status.LastQualityScore,
			PricingVersion: strings.TrimSpace(provider.Spec.Pricing.Version)}
		if model.Namespace == "" || provider.Namespace != model.Namespace || model.UID == "" || provider.UID == "" || model.ResourceVersion == "" || provider.ResourceVersion == "" || model.Generation <= 0 || provider.Generation <= 0 {
			candidate.InfeasibleReason = ReasonNotRoutable
			out = append(out, candidate)
			continue
		}
		if !matchesTarget(req, model) {
			candidate.InfeasibleReason = ReasonWorkloadTargetMismatch
			out = append(out, candidate)
			continue
		}
		if model.Status.ObservedGeneration != model.Generation || !readyConditionTrue(model.Status.Conditions) {
			candidate.InfeasibleReason = ReasonModelNotReady
			out = append(out, candidate)
			continue
		}
		if provider.Status.ObservedGeneration != provider.Generation || !readyConditionTrue(provider.Status.Conditions) {
			candidate.InfeasibleReason = ReasonProviderUnavailable
			out = append(out, candidate)
			continue
		}
		if model.Spec.GOVAR == nil || !model.Spec.GOVAR.Routable || strings.TrimSpace(model.Spec.GOVAR.RouteBindingRef) == "" {
			candidate.InfeasibleReason = ReasonNotRoutable
			out = append(out, candidate)
			continue
		}
		binding, err := providerRouteBinding(provider, model.Spec.GOVAR.RouteBindingRef)
		if err != nil || !providerPathCompatible(provider.Spec.Type, string(binding.PathMode)) {
			candidate.InfeasibleReason = ReasonNotRoutable
			out = append(out, candidate)
			continue
		}
		if req.SensitiveData && !model.Spec.SensitiveDataAllowed && !provider.Spec.Compliance.AllowedForSensitiveData {
			candidate.InfeasibleReason = ReasonGovernanceInfeasible
			out = append(out, candidate)
			continue
		}
		if len(req.AllowedZones) > 0 && !zoneAllowed(req.AllowedZones, provider.Spec.DataResidency, provider.Spec.Region) {
			candidate.InfeasibleReason = ReasonGovernanceInfeasible
			out = append(out, candidate)
			continue
		}
		if evaluatedAt.IsZero() || model.Status.LastEvaluatedAt == nil || evaluatedAt.Sub(model.Status.LastEvaluatedAt.Time) > observationFreshnessLimit || model.Status.LastEvaluatedAt.Sub(evaluatedAt) > time.Minute {
			candidate.InfeasibleReason = ReasonQualityStale
			out = append(out, candidate)
			continue
		}
		candidate.QualityObservedAt = model.Status.LastEvaluatedAt.Time
		if model.Status.GOVAR != nil && model.Status.GOVAR.Latency != nil {
			latency := model.Status.GOVAR.Latency
			if latency.SampleCount > 0 && evaluatedAt.Sub(latency.ObservedAt.Time) <= observationFreshnessLimit && latency.ObservedAt.Sub(evaluatedAt) <= time.Minute {
				candidate.LatencyMillis, candidate.LatencyObservedAt = latency.MeanMillis, latency.ObservedAt.Time
			}
		}
		if model.Status.GOVAR != nil && model.Status.GOVAR.VerifiedOutputCap != nil {
			cap := model.Status.GOVAR.VerifiedOutputCap
			expectedCapParameter, capAdapterSupported := govarpricing.OutputCapRequestParameter(provider.Spec.Type, binding.PathMode)
			candidate.VerifiedOutputCap = provider.Status.GOVAR != nil && provider.Status.GOVAR.PricingSnapshot != nil && cap.Verified && cap.MaxOutputTokens > 0 && cap.ProviderUID == string(provider.UID) && cap.ProviderGeneration == provider.Generation &&
				cap.ProviderDeployment == binding.ProviderDeployment && cap.ModelVersion == model.Spec.ModelName && cap.CapabilityAdapterVersion == govarpricing.CurrentAdapterVersion &&
				capAdapterSupported && cap.EnforcedByPathAdapter && cap.RequestParameter == expectedCapParameter && len(cap.EvidenceSHA256) == 64 && cap.PricingSnapshotSHA256 == provider.Status.GOVAR.PricingSnapshot.SnapshotSHA256 && strings.TrimSpace(cap.SourceVersion) != "" &&
				cap.ValidUntil.After(evaluatedAt) && evaluatedAt.Sub(cap.ObservedAt.Time) <= observationFreshnessLimit && cap.ObservedAt.Sub(evaluatedAt) <= time.Minute
			candidate.VerifiedOutputCapTokens = cap.MaxOutputTokens
			candidate.CapEvidenceDigest = cap.EvidenceSHA256
		}
		if candidate.ContextWindow <= 0 || candidate.PricingVersion == "" || provider.Status.GOVAR == nil || provider.Status.GOVAR.PricingSnapshot == nil {
			candidate.InfeasibleReason = ReasonPricingIncomplete
			out = append(out, candidate)
			continue
		}
		candidate.PricingSnapshot = *provider.Status.GOVAR.PricingSnapshot.DeepCopy()
		if err := govarpricing.ValidateSnapshot(candidate.PricingSnapshot, provider.Generation, evaluatedAt); err != nil {
			candidate.InfeasibleReason = ReasonPricingStale
			out = append(out, candidate)
			continue
		}
		for _, row := range candidate.PricingSnapshot.Charges {
			if row.Basis == aiopsv1alpha1.ProviderBasisInputTokens {
				candidate.InputPriceMicrosPerMillion = row.PriceMicrosPerUnit
			}
			if row.Basis == aiopsv1alpha1.ProviderBasisOutputTokens {
				candidate.OutputPriceMicrosPerMillion = row.PriceMicrosPerUnit
			}
		}
		candidate.SensitiveDataAllowed = model.Spec.SensitiveDataAllowed || provider.Spec.Compliance.AllowedForSensitiveData
		pricingHash := fixedFieldHash("govar-pricing-compliance-v2", candidate.PricingSnapshot.SnapshotSHA256, candidate.CapEvidenceDigest,
			strings.ToLower(provider.Spec.Type), normalizeZone(provider.Spec.Region), normalizeZone(provider.Spec.DataResidency), fmt.Sprint(provider.Spec.Managed), fmt.Sprint(provider.Spec.Compliance.AllowedForSensitiveData))
		candidate.RouteSnapshot = RouteSnapshot{
			Namespace: model.Namespace, ModelName: model.Name, ModelUID: string(model.UID), ModelGeneration: model.Generation,
			ModelResourceVersion: model.ResourceVersion, ProviderName: provider.Name, ProviderUID: string(provider.UID),
			ProviderGeneration: provider.Generation, ProviderResourceVersion: provider.ResourceVersion, PricingVersion: candidate.PricingVersion,
			PricingComplianceHash: pricingHash, RouteBindingName: binding.Name, ProviderDeployment: binding.ProviderDeployment,
			Cluster: binding.Cluster, Authority: binding.Authority, PathMode: string(binding.PathMode),
		}
		candidate.RouteSnapshot.SnapshotHash = RouteSnapshotHash(candidate.RouteSnapshot)
		candidate.SnapshotVersion = candidate.RouteSnapshot.SnapshotHash
		if err := ValidateRouteSnapshot(candidate.RouteSnapshot); err != nil {
			candidate.InfeasibleReason = ReasonNotRoutable
			out = append(out, candidate)
			continue
		}
		candidate.Feasible = true
		out = append(out, candidate)
	}
	return out
}

var routeToken = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`)
var routeAuthorityToken = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

func providerRouteBinding(provider aiopsv1alpha1.AIProvider, name string) (aiopsv1alpha1.AIProviderGatewayRouteBinding, error) {
	if provider.Spec.GOVAR == nil || strings.TrimSpace(name) == "" {
		return aiopsv1alpha1.AIProviderGatewayRouteBinding{}, errors.New("provider route binding is absent")
	}
	seen := map[string]struct{}{}
	var found aiopsv1alpha1.AIProviderGatewayRouteBinding
	for _, binding := range provider.Spec.GOVAR.GatewayRoutes {
		if _, duplicate := seen[binding.Name]; duplicate {
			return found, errors.New("duplicate provider route binding name")
		}
		seen[binding.Name] = struct{}{}
		if binding.Name == name {
			found = binding
		}
	}
	if found.Name == "" || !routeToken.MatchString(found.Name) || !routeToken.MatchString(found.ProviderDeployment) || !routeAuthorityToken.MatchString(found.Cluster) || !routeAuthorityToken.MatchString(found.Authority) || !validPathMode(string(found.PathMode)) {
		return found, errors.New("provider route binding is missing or malformed")
	}
	return found, nil
}

func providerPathCompatible(providerType, pathMode string) bool {
	_, supported := govarpricing.OutputCapRequestParameter(strings.ToLower(strings.TrimSpace(providerType)), aiopsv1alpha1.GOVARRoutePathMode(pathMode))
	return supported
}

func validPathMode(mode string) bool {
	switch mode {
	case "openai-body", "azure-deployment-path", "anthropic-body", "google-generate-path":
		return true
	default:
		return false
	}
}

// PricingComplianceHash binds every provider input used for monetary or
// governance feasibility to a deterministic fixed-field record.
func PricingComplianceHash(provider aiopsv1alpha1.AIProvider, inputMicros, outputMicros MoneyMicros) (string, error) {
	if provider.Spec.Pricing.ObservedAt == nil {
		return "", errors.New("pricing observation time is required")
	}
	countries := make([]string, 0, len(provider.Spec.Compliance.AllowedCountries))
	seenCountries := map[string]struct{}{}
	for _, raw := range provider.Spec.Compliance.AllowedCountries {
		country := normalizeZone(raw)
		if country == "" {
			return "", errors.New("empty allowed country")
		}
		if _, duplicate := seenCountries[country]; duplicate {
			return "", errors.New("duplicate allowed country")
		}
		seenCountries[country] = struct{}{}
		countries = append(countries, country)
	}
	sort.Strings(countries)
	categories := append([]aiopsv1alpha1.ProviderBillableCategory(nil), provider.Spec.Pricing.BillableCategories...)
	sort.Slice(categories, func(i, j int) bool { return categories[i].Name < categories[j].Name })
	parts := []string{"govar-pricing-compliance-v1", strings.ToLower(provider.Spec.Type), normalizeZone(provider.Spec.Region), normalizeZone(provider.Spec.DataResidency), fmt.Sprint(provider.Spec.Managed),
		strings.ToUpper(strings.TrimSpace(provider.Spec.Pricing.Currency)), strings.TrimSpace(provider.Spec.Pricing.Version), provider.Spec.Pricing.ObservedAt.UTC().Format(time.RFC3339Nano), string(provider.Spec.Pricing.Completeness),
		fmt.Sprint(inputMicros), fmt.Sprint(outputMicros)}
	if provider.Spec.Pricing.FixedMonthlyCost == nil {
		parts = append(parts, "absent")
	} else {
		fixedMicros, err := quantityToExactMicros(*provider.Spec.Pricing.FixedMonthlyCost)
		if err != nil {
			return "", fmt.Errorf("fixed monthly price: %w", err)
		}
		parts = append(parts, fmt.Sprint(fixedMicros))
	}
	lastName := ""
	for _, category := range categories {
		if category.Name == lastName || strings.TrimSpace(category.Name) == "" || category.Price.Sign() < 0 {
			return "", errors.New("duplicate or empty billable category")
		}
		lastName = category.Name
		parts = append(parts, category.Name, string(category.Unit), category.Price.String())
	}
	parts = append(parts, fmt.Sprint(provider.Spec.Compliance.AllowedForSensitiveData))
	parts = append(parts, countries...)
	return fixedFieldHash(parts...), nil
}

func RouteSnapshotHash(snapshot RouteSnapshot) string {
	return fixedFieldHash("govar-route-snapshot-v1", snapshot.Namespace, snapshot.ModelName, snapshot.ModelUID,
		fmt.Sprint(snapshot.ModelGeneration), snapshot.ModelResourceVersion, snapshot.ProviderName, snapshot.ProviderUID,
		fmt.Sprint(snapshot.ProviderGeneration), snapshot.ProviderResourceVersion, snapshot.PricingVersion,
		snapshot.PricingComplianceHash, snapshot.RouteBindingName, snapshot.ProviderDeployment, snapshot.Cluster,
		snapshot.Authority, snapshot.PathMode)
}

// fixedFieldHash length-prefixes every field so embedded separators cannot
// create the same byte stream for different fixed-field records.
func fixedFieldHash(parts ...string) string {
	h := sha256.New()
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(part))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func ValidateRouteSnapshot(snapshot RouteSnapshot) error {
	sha := regexp.MustCompile(`^[0-9a-f]{64}$`)
	if snapshot.Namespace == "" || snapshot.ModelName == "" || snapshot.ModelUID == "" || snapshot.ModelGeneration <= 0 || snapshot.ModelResourceVersion == "" ||
		snapshot.ProviderName == "" || snapshot.ProviderUID == "" || snapshot.ProviderGeneration <= 0 || snapshot.ProviderResourceVersion == "" || snapshot.PricingVersion == "" ||
		!sha.MatchString(snapshot.PricingComplianceHash) || !routeToken.MatchString(snapshot.RouteBindingName) || !routeToken.MatchString(snapshot.ProviderDeployment) ||
		!routeAuthorityToken.MatchString(snapshot.Cluster) || !routeAuthorityToken.MatchString(snapshot.Authority) || !validPathMode(snapshot.PathMode) ||
		!sha.MatchString(snapshot.SnapshotHash) || snapshot.SnapshotHash != RouteSnapshotHash(snapshot) {
		return errors.New("route snapshot is incomplete or its digest is invalid")
	}
	return nil
}

func validateCandidateSnapshot(candidate Candidate) error {
	if err := ValidateRouteSnapshot(candidate.RouteSnapshot); err != nil {
		return err
	}
	if candidate.ModelRef != candidate.RouteSnapshot.ModelName || candidate.ProviderRef != candidate.RouteSnapshot.ProviderName || candidate.PricingVersion != candidate.RouteSnapshot.PricingVersion || candidate.SnapshotVersion != candidate.RouteSnapshot.SnapshotHash || !providerPathCompatible(candidate.ProviderType, candidate.RouteSnapshot.PathMode) {
		return errors.New("candidate and route snapshot identity mismatch")
	}
	return nil
}

func readyConditionTrue(conditions []metav1.Condition) bool {
	for _, condition := range conditions {
		if condition.Type == aiopsv1alpha1.ConditionReady && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func matchesTarget(req RequestContext, model aiopsv1alpha1.AIModel) bool {
	if model.Spec.ServesNamespace != "" && model.Spec.ServesNamespace != req.Namespace {
		return false
	}
	if model.Spec.ServesTeam != "" && model.Spec.ServesTeam != req.Team {
		return false
	}
	if model.Spec.ServesApplication != "" && model.Spec.ServesApplication != req.Application {
		return false
	}
	return true
}

func zoneAllowed(allowed []string, residency, region string) bool {
	normalized := make([]string, 0, len(allowed))
	for _, zone := range allowed {
		normalized = append(normalized, normalizeZone(zone))
	}
	residency = normalizeZone(residency)
	region = normalizeZone(region)
	return slices.Contains(normalized, residency) || slices.Contains(normalized, region)
}

func normalizeZone(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
