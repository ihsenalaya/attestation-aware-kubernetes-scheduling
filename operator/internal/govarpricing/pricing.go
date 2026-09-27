// Package govarpricing provides pure, deterministic normalization and charge
// arithmetic. It deliberately has no Kubernetes client or wall-clock reads.
package govarpricing

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const CurrentAdapterVersion = "govar-pricing-v1"

var AllBases = []aiopsv1alpha1.ProviderBillableBasis{
	aiopsv1alpha1.ProviderBasisInputTokens, aiopsv1alpha1.ProviderBasisCachedInputTokens,
	aiopsv1alpha1.ProviderBasisOutputTokens, aiopsv1alpha1.ProviderBasisReasoningTokens,
	aiopsv1alpha1.ProviderBasisRequest, aiopsv1alpha1.ProviderBasisToolCall,
	aiopsv1alpha1.ProviderBasisMediaUnit, aiopsv1alpha1.ProviderBasisBillableSecond,
	aiopsv1alpha1.ProviderBasisCancellation, aiopsv1alpha1.ProviderBasisRetryAttempt,
}

type NormalizedCharge = aiopsv1alpha1.AIProviderNormalizedChargeStatus
type NormalizedPricingSnapshot = aiopsv1alpha1.AIProviderPricingSnapshotStatus

type Evidence struct {
	Mode          aiopsv1alpha1.ProviderPricingEvidenceMode
	SHA256        string
	SourceVersion string
}

type SnapshotProducer interface {
	Produce(aiopsv1alpha1.AIProvider, time.Time) (NormalizedPricingSnapshot, Evidence, error)
}

type CapabilityVerifier interface {
	VerifyOutputCap(aiopsv1alpha1.AIModel, aiopsv1alpha1.AIProvider, NormalizedPricingSnapshot, time.Time) (aiopsv1alpha1.AIModelVerifiedOutputCapStatus, Evidence, error)
}

type RequestChargeContext struct {
	InputTokens          int64
	OutputTokens         int64
	MaxToolCalls         int64
	MaxMediaUnits        int64
	TimeoutSeconds       int64
	MaxRetryAttempts     int64
	CancellationPossible bool
	// DeclaredBounds is the canonical, non-overlapping per-basis bound supplied
	// by the trusted request adapter. Additional request-declared charges must
	// be present even when their quantity is zero; absence is not interpreted
	// as zero.
	DeclaredBounds []UsageQuantity
}

type UsageQuantity struct {
	Basis    aiopsv1alpha1.ProviderBillableBasis `json:"basis"`
	Quantity int64                               `json:"quantity"`
}

type ChargeComponent struct {
	Basis              aiopsv1alpha1.ProviderBillableBasis `json:"basis"`
	Quantity           int64                               `json:"quantity"`
	PriceMicrosPerUnit int64                               `json:"price_micros_per_unit"`
	UnitDenominator    int64                               `json:"unit_denominator"`
	ReservedMicros     int64                               `json:"reserved_micros"`
}

type ActualChargeComponent struct {
	Basis              aiopsv1alpha1.ProviderBillableBasis `json:"basis"`
	Quantity           int64                               `json:"quantity"`
	PriceMicrosPerUnit int64                               `json:"price_micros_per_unit"`
	UnitDenominator    int64                               `json:"unit_denominator"`
	ActualMicros       int64                               `json:"actual_micros"`
	ReservedQuantity   int64                               `json:"reserved_quantity"`
	ExceededBound      bool                                `json:"exceeded_bound"`
}

type Adapter interface {
	Family() string
	Normalize(aiopsv1alpha1.AIProvider, time.Time) (NormalizedPricingSnapshot, Evidence, error)
	ApplicableCharges(NormalizedPricingSnapshot, RequestChargeContext, time.Time) ([]ChargeComponent, error)
}

type closedAdapter struct{ family string }

func (a closedAdapter) Family() string { return a.family }
func (a closedAdapter) Normalize(p aiopsv1alpha1.AIProvider, now time.Time) (NormalizedPricingSnapshot, Evidence, error) {
	return Normalize(p, now)
}
func (a closedAdapter) ApplicableCharges(s NormalizedPricingSnapshot, c RequestChargeContext, at time.Time) ([]ChargeComponent, error) {
	return ReserveComponents(s, c, at)
}

func ForProviderType(providerType string) (Adapter, error) {
	switch strings.ToLower(strings.TrimSpace(providerType)) {
	case "openai", "azure-openai":
		return closedAdapter{family: strings.ToLower(strings.TrimSpace(providerType))}, nil
	default:
		return nil, fmt.Errorf("no closed GOV-AR pricing and authoritative-usage adapter for provider type %q", providerType)
	}
}

// OutputCapRequestParameter returns the canonical client-facing output bound
// for a provider/path pair with a closed request rewrite and authoritative
// usage mapping. Supporting a provider in the Kubernetes API is deliberately
// not enough to make it GOV-AR feasible.
func OutputCapRequestParameter(providerType string, pathMode aiopsv1alpha1.GOVARRoutePathMode) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(providerType)) {
	case "openai":
		if pathMode == aiopsv1alpha1.GOVARRouteOpenAIBody {
			return "max_output_tokens", true
		}
	case "azure-openai":
		if pathMode == aiopsv1alpha1.GOVARRouteAzureDeploymentPath {
			return "max_output_tokens", true
		}
	}
	return "", false
}

// Normalize requires an explicit partition of every closed billable basis into
// charged or adapter-validated inapplicable. Legacy free-form categories are
// rejected because their settlement semantics cannot be proven.
func Normalize(provider aiopsv1alpha1.AIProvider, now time.Time) (NormalizedPricingSnapshot, Evidence, error) {
	if _, err := ForProviderType(provider.Spec.Type); err != nil {
		return NormalizedPricingSnapshot{}, Evidence{}, err
	}
	if provider.Spec.GOVAR == nil || provider.Spec.GOVAR.Pricing == nil {
		return NormalizedPricingSnapshot{}, Evidence{}, errors.New("typed GOV-AR pricing is absent")
	}
	if len(provider.Spec.Pricing.BillableCategories) != 0 {
		return NormalizedPricingSnapshot{}, Evidence{}, errors.New("legacy free-form billable categories are not GOV-AR authority")
	}
	pricing := provider.Spec.GOVAR.Pricing
	evidence := pricing.Evidence
	if provider.Generation <= 0 || strings.TrimSpace(provider.Spec.Pricing.Version) == "" || provider.Spec.Pricing.Completeness != aiopsv1alpha1.ProviderPricingComplete {
		return NormalizedPricingSnapshot{}, Evidence{}, errors.New("pricing version, generation, or completeness is invalid")
	}
	if !strings.EqualFold(strings.TrimSpace(provider.Spec.Pricing.Currency), "EUR") {
		return NormalizedPricingSnapshot{}, Evidence{}, errors.New("only EUR integer-micro snapshots are supported")
	}
	if evidence.AdapterVersion != CurrentAdapterVersion || !validEvidenceMode(evidence.Mode) || !isSHA256(evidence.EvidenceSHA256) || strings.TrimSpace(evidence.SourceVersion) == "" {
		return NormalizedPricingSnapshot{}, Evidence{}, errors.New("pricing evidence is incomplete")
	}
	if evidence.Mode != aiopsv1alpha1.ProviderEvidenceAdminAttested {
		return NormalizedPricingSnapshot{}, Evidence{}, errors.New("spec-supplied pricing evidence must be admin_attested; provider catalog/API modes require a provider-owned producer")
	}
	if !evidence.ValidUntil.After(now) {
		return NormalizedPricingSnapshot{}, Evidence{}, errors.New("pricing evidence is stale")
	}
	if provider.Spec.Pricing.ObservedAt == nil || provider.Spec.Pricing.ObservedAt.After(now.Add(time.Minute)) || now.Sub(provider.Spec.Pricing.ObservedAt.Time) > 24*time.Hour {
		return NormalizedPricingSnapshot{}, Evidence{}, errors.New("pricing observation is stale or in the future")
	}
	input, err := exactMicros(provider.Spec.Pricing.InputTokenPricePerMillion)
	if err != nil {
		return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("input price: %w", err)
	}
	output, err := exactMicros(provider.Spec.Pricing.OutputTokenPricePerMillion)
	if err != nil {
		return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("output price: %w", err)
	}
	rows := []NormalizedCharge{
		{Basis: aiopsv1alpha1.ProviderBasisInputTokens, Applicability: aiopsv1alpha1.ProviderChargeRequestDeclared, PriceMicrosPerUnit: input, UnitDenominator: 1_000_000, SettlementUsageField: aiopsv1alpha1.ProviderBasisInputTokens, RequestBoundField: "input_tokens"},
		{Basis: aiopsv1alpha1.ProviderBasisOutputTokens, Applicability: aiopsv1alpha1.ProviderChargeRequestDeclared, PriceMicrosPerUnit: output, UnitDenominator: 1_000_000, SettlementUsageField: aiopsv1alpha1.ProviderBasisOutputTokens, RequestBoundField: "max_output_tokens"},
	}
	seen := map[aiopsv1alpha1.ProviderBillableBasis]bool{aiopsv1alpha1.ProviderBasisInputTokens: true, aiopsv1alpha1.ProviderBasisOutputTokens: true}
	for _, charge := range pricing.Charges {
		if !isBasis(charge.Basis) || seen[charge.Basis] || !validApplicability(charge.Applicability) || charge.SettlementUsageField != charge.Basis || charge.UnitDenominator <= 0 {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("invalid or duplicate charge basis %q", charge.Basis)
		}
		price, err := exactMicros(charge.Price)
		if err != nil {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("charge %s: %w", charge.Basis, err)
		}
		if charge.IncludedIn != nil {
			if !isBasis(*charge.IncludedIn) || *charge.IncludedIn == charge.Basis || price != 0 {
				return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("invalid included-in semantics for %s", charge.Basis)
			}
		} else if charge.MaximumQuantity == nil && charge.RequestBoundField == "" {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("charge %s has no pre-dispatch bound", charge.Basis)
		}
		if charge.Applicability == aiopsv1alpha1.ProviderChargeProviderResponse && charge.MaximumQuantity == nil {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("response-only charge %s has no enforced maximum", charge.Basis)
		}
		if (charge.Basis == aiopsv1alpha1.ProviderBasisCachedInputTokens || charge.Basis == aiopsv1alpha1.ProviderBasisReasoningTokens) && charge.IncludedIn == nil && !charge.DisjointUsage {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("separately priced %s must have disjoint usage semantics", charge.Basis)
		}
		if charge.Applicability == aiopsv1alpha1.ProviderChargeRequestDeclared && charge.IncludedIn == nil && charge.RequestBoundField == "" {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("request-declared charge %s has no mapped request bound", charge.Basis)
		}
		row := NormalizedCharge{Basis: charge.Basis, Applicability: charge.Applicability, PriceMicrosPerUnit: price, UnitDenominator: charge.UnitDenominator, SettlementUsageField: charge.SettlementUsageField, IncludedIn: charge.IncludedIn, MaximumQuantity: charge.MaximumQuantity, RequestBoundField: charge.RequestBoundField, DisjointUsage: charge.DisjointUsage}
		rows = append(rows, row)
		seen[charge.Basis] = true
	}
	inapplicable := append([]aiopsv1alpha1.ProviderBillableBasis(nil), pricing.InapplicableBases...)
	for _, basis := range inapplicable {
		if !isBasis(basis) || seen[basis] {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("duplicate/contradictory inapplicable basis %q", basis)
		}
		seen[basis] = true
	}
	for _, basis := range AllBases {
		if !seen[basis] {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("basis %s is neither priced nor explicitly inapplicable", basis)
		}
	}
	if err := validateIncludedChargeGraph(rows, inapplicable); err != nil {
		return NormalizedPricingSnapshot{}, Evidence{}, err
	}
	for _, row := range rows {
		if row.RequestBoundField != "" && !validRequestBound(row.Basis, row.RequestBoundField) {
			return NormalizedPricingSnapshot{}, Evidence{}, fmt.Errorf("request bound %s is invalid for %s", row.RequestBoundField, row.Basis)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Basis < rows[j].Basis })
	sort.Slice(inapplicable, func(i, j int) bool { return inapplicable[i] < inapplicable[j] })
	snapshot := NormalizedPricingSnapshot{SpecGeneration: provider.Generation, Version: strings.TrimSpace(provider.Spec.Pricing.Version), ObservedAt: *provider.Spec.Pricing.ObservedAt, ValidUntil: evidence.ValidUntil, Currency: "EUR", Completeness: aiopsv1alpha1.ProviderPricingComplete, AdapterVersion: CurrentAdapterVersion, EvidenceMode: evidence.Mode, EvidenceSHA256: evidence.EvidenceSHA256, SourceVersion: evidence.SourceVersion, Charges: rows, InapplicableBases: inapplicable}
	snapshot.SnapshotSHA256 = SnapshotDigest(snapshot)
	return snapshot, Evidence{Mode: evidence.Mode, SHA256: evidence.EvidenceSHA256, SourceVersion: evidence.SourceVersion}, nil
}

func SnapshotDigest(snapshot NormalizedPricingSnapshot) string {
	parts := []string{"govar-normalized-pricing-v1", fmt.Sprint(snapshot.SpecGeneration), snapshot.Version, snapshot.ObservedAt.UTC().Format(time.RFC3339Nano), snapshot.ValidUntil.UTC().Format(time.RFC3339Nano), strings.ToUpper(snapshot.Currency), string(snapshot.Completeness), snapshot.AdapterVersion, string(snapshot.EvidenceMode), snapshot.EvidenceSHA256, snapshot.SourceVersion}
	rows := append([]NormalizedCharge(nil), snapshot.Charges...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Basis < rows[j].Basis })
	for _, r := range rows {
		included, maximum := "", ""
		if r.IncludedIn != nil {
			included = string(*r.IncludedIn)
		}
		if r.MaximumQuantity != nil {
			maximum = fmt.Sprint(*r.MaximumQuantity)
		}
		parts = append(parts, string(r.Basis), string(r.Applicability), fmt.Sprint(r.PriceMicrosPerUnit), fmt.Sprint(r.UnitDenominator), string(r.SettlementUsageField), included, maximum, string(r.RequestBoundField), fmt.Sprint(r.DisjointUsage))
	}
	inapplicable := append([]aiopsv1alpha1.ProviderBillableBasis(nil), snapshot.InapplicableBases...)
	sort.Slice(inapplicable, func(i, j int) bool { return inapplicable[i] < inapplicable[j] })
	for _, b := range inapplicable {
		parts = append(parts, "inapplicable", string(b))
	}
	return fixedHash(parts...)
}

func ValidateSnapshot(snapshot NormalizedPricingSnapshot, generation int64, at time.Time) error {
	if err := ValidateSnapshotIntegrity(snapshot); err != nil || snapshot.SpecGeneration != generation || at.IsZero() || !snapshot.ValidUntil.After(at) || snapshot.ObservedAt.After(at.Add(time.Minute)) {
		return errors.New("normalized pricing snapshot is stale, incomplete, or has an invalid digest")
	}
	return nil
}

// ValidateSnapshotIntegrity deliberately does not test ValidUntil. Settlement
// must use the frozen reservation snapshot even when that snapshot expires or
// the provider publishes a new price while the request is in flight.
func ValidateSnapshotIntegrity(snapshot NormalizedPricingSnapshot) error {
	if snapshot.SpecGeneration <= 0 || strings.TrimSpace(snapshot.Version) == "" || snapshot.Completeness != aiopsv1alpha1.ProviderPricingComplete || strings.ToUpper(snapshot.Currency) != "EUR" || snapshot.AdapterVersion != CurrentAdapterVersion || !validEvidenceMode(snapshot.EvidenceMode) || !isSHA256(snapshot.EvidenceSHA256) || strings.TrimSpace(snapshot.SourceVersion) == "" || !snapshot.ValidUntil.After(snapshot.ObservedAt.Time) || !isSHA256(snapshot.SnapshotSHA256) || snapshot.SnapshotSHA256 != SnapshotDigest(snapshot) {
		return errors.New("pricing snapshot digest or completeness is invalid")
	}
	seen := make(map[aiopsv1alpha1.ProviderBillableBasis]bool, len(AllBases))
	for _, row := range snapshot.Charges {
		if !isBasis(row.Basis) || seen[row.Basis] || !validApplicability(row.Applicability) || row.SettlementUsageField != row.Basis || row.PriceMicrosPerUnit < 0 || row.UnitDenominator <= 0 {
			return fmt.Errorf("invalid normalized charge %q", row.Basis)
		}
		if row.IncludedIn != nil {
			if !isBasis(*row.IncludedIn) || *row.IncludedIn == row.Basis || row.PriceMicrosPerUnit != 0 {
				return fmt.Errorf("invalid included charge %q", row.Basis)
			}
		} else {
			if (row.Basis == aiopsv1alpha1.ProviderBasisCachedInputTokens || row.Basis == aiopsv1alpha1.ProviderBasisReasoningTokens) && !row.DisjointUsage {
				return fmt.Errorf("overlapping detail charge %q", row.Basis)
			}
			if row.Applicability == aiopsv1alpha1.ProviderChargeRequestDeclared && !validRequestBound(row.Basis, row.RequestBoundField) {
				return fmt.Errorf("unmapped request charge %q", row.Basis)
			}
			if row.Applicability != aiopsv1alpha1.ProviderChargeRequestDeclared && row.MaximumQuantity == nil {
				return fmt.Errorf("unbounded non-request charge %q", row.Basis)
			}
		}
		seen[row.Basis] = true
	}
	for _, basis := range snapshot.InapplicableBases {
		if !isBasis(basis) || seen[basis] {
			return fmt.Errorf("invalid inapplicable basis %q", basis)
		}
		seen[basis] = true
	}
	for _, basis := range AllBases {
		if !seen[basis] {
			return fmt.Errorf("unpartitioned basis %q", basis)
		}
	}
	if err := validateIncludedChargeGraph(snapshot.Charges, snapshot.InapplicableBases); err != nil {
		return err
	}
	return nil
}

func validateIncludedChargeGraph(rows []NormalizedCharge, inapplicable []aiopsv1alpha1.ProviderBillableBasis) error {
	charged := make(map[aiopsv1alpha1.ProviderBillableBasis]NormalizedCharge, len(rows))
	for _, row := range rows {
		charged[row.Basis] = row
	}
	for _, basis := range inapplicable {
		if _, exists := charged[basis]; exists {
			return fmt.Errorf("included-charge graph contains contradictory inapplicable basis %q", basis)
		}
	}

	for _, row := range rows {
		if row.IncludedIn == nil {
			continue
		}
		visited := map[aiopsv1alpha1.ProviderBillableBasis]bool{row.Basis: true}
		current := row
		for current.IncludedIn != nil {
			parentBasis := *current.IncludedIn
			if visited[parentBasis] {
				return fmt.Errorf("included-charge cycle reaches %q from %q", parentBasis, row.Basis)
			}
			visited[parentBasis] = true
			parent, exists := charged[parentBasis]
			if !exists {
				return fmt.Errorf("included basis %s has no charged parent %s", current.Basis, parentBasis)
			}
			current = parent
		}
		if !legalIncludedPair(row.Basis, current.Basis) {
			return fmt.Errorf("included basis %s cannot resolve to charged root %s", row.Basis, current.Basis)
		}
	}
	return nil
}

func legalIncludedPair(detail, root aiopsv1alpha1.ProviderBillableBasis) bool {
	return (detail == aiopsv1alpha1.ProviderBasisCachedInputTokens && root == aiopsv1alpha1.ProviderBasisInputTokens) ||
		(detail == aiopsv1alpha1.ProviderBasisReasoningTokens && root == aiopsv1alpha1.ProviderBasisOutputTokens)
}

func ReserveComponents(snapshot NormalizedPricingSnapshot, ctx RequestChargeContext, at time.Time) ([]ChargeComponent, error) {
	if err := ValidateSnapshot(snapshot, snapshot.SpecGeneration, at); err != nil {
		return nil, err
	}
	declared, err := canonicalUsage(ctx.DeclaredBounds)
	if err != nil {
		return nil, err
	}
	var out []ChargeComponent
	for _, row := range snapshot.Charges {
		if row.IncludedIn != nil {
			continue
		}
		q, err := requestQuantity(row, ctx, declared)
		if err != nil {
			return nil, err
		}
		if row.MaximumQuantity != nil {
			q = *row.MaximumQuantity
		}
		if q < 0 {
			return nil, fmt.Errorf("negative quantity for %s", row.Basis)
		}
		cost, err := ceilingProduct(row.PriceMicrosPerUnit, q, row.UnitDenominator)
		if err != nil {
			return nil, fmt.Errorf("charge %s: %w", row.Basis, err)
		}
		out = append(out, ChargeComponent{Basis: row.Basis, Quantity: q, PriceMicrosPerUnit: row.PriceMicrosPerUnit, UnitDenominator: row.UnitDenominator, ReservedMicros: cost})
	}
	return out, nil
}

func requestQuantity(row NormalizedCharge, ctx RequestChargeContext, declared map[aiopsv1alpha1.ProviderBillableBasis]int64) (int64, error) {
	if row.Applicability == aiopsv1alpha1.ProviderChargeAlways || row.Applicability == aiopsv1alpha1.ProviderChargeProviderResponse {
		if row.MaximumQuantity == nil {
			return 0, fmt.Errorf("charge %s lacks an enforced maximum", row.Basis)
		}
		return *row.MaximumQuantity, nil
	}
	switch row.RequestBoundField {
	case "input_tokens":
		return ctx.InputTokens, nil
	case "max_cached_input_tokens":
		return requiredDeclared(row.Basis, declared)
	case "max_output_tokens":
		return ctx.OutputTokens, nil
	case "max_reasoning_tokens":
		return requiredDeclared(row.Basis, declared)
	case "max_tool_calls", "max_media_units", "timeout_seconds", "max_retry_attempts", "cancellation_possible":
		return requiredDeclared(row.Basis, declared)
	case "":
		return 0, fmt.Errorf("charge %s has no request-bound mapping", row.Basis)
	default:
		return 0, fmt.Errorf("unknown request bound field %q", row.RequestBoundField)
	}
}

func requiredDeclared(basis aiopsv1alpha1.ProviderBillableBasis, declared map[aiopsv1alpha1.ProviderBillableBasis]int64) (int64, error) {
	q, ok := declared[basis]
	if !ok {
		return 0, fmt.Errorf("request-declared charge %s has no proven bound", basis)
	}
	return q, nil
}

func validRequestBound(basis aiopsv1alpha1.ProviderBillableBasis, field aiopsv1alpha1.ProviderRequestBoundField) bool {
	return (basis == aiopsv1alpha1.ProviderBasisInputTokens && field == "input_tokens") ||
		(basis == aiopsv1alpha1.ProviderBasisCachedInputTokens && field == "max_cached_input_tokens") ||
		(basis == aiopsv1alpha1.ProviderBasisOutputTokens && field == "max_output_tokens") ||
		(basis == aiopsv1alpha1.ProviderBasisReasoningTokens && field == "max_reasoning_tokens") ||
		(basis == aiopsv1alpha1.ProviderBasisToolCall && field == "max_tool_calls") ||
		(basis == aiopsv1alpha1.ProviderBasisMediaUnit && field == "max_media_units") ||
		(basis == aiopsv1alpha1.ProviderBasisBillableSecond && field == "timeout_seconds") ||
		(basis == aiopsv1alpha1.ProviderBasisRetryAttempt && field == "max_retry_attempts") ||
		(basis == aiopsv1alpha1.ProviderBasisCancellation && field == "cancellation_possible")
}

func canonicalUsage(values []UsageQuantity) (map[aiopsv1alpha1.ProviderBillableBasis]int64, error) {
	out := make(map[aiopsv1alpha1.ProviderBillableBasis]int64, len(values))
	for _, value := range values {
		if !isBasis(value.Basis) || value.Quantity < 0 {
			return nil, fmt.Errorf("invalid usage quantity for %q", value.Basis)
		}
		if _, exists := out[value.Basis]; exists {
			return nil, fmt.Errorf("duplicate usage basis %q", value.Basis)
		}
		out[value.Basis] = value.Quantity
	}
	return out, nil
}

// SettleComponents prices a provider-adapter-normalized, non-overlapping usage
// vector against the immutable price rows and reserved bounds. Missing priced
// rows are returned explicitly; callers must not advertise finality or release
// their residual holds until that list is empty.
func SettleComponents(snapshot NormalizedPricingSnapshot, reserved []ChargeComponent, usage []UsageQuantity) ([]ActualChargeComponent, []aiopsv1alpha1.ProviderBillableBasis, bool, error) {
	if err := ValidateSnapshotIntegrity(snapshot); err != nil {
		return nil, nil, false, err
	}
	quantities, err := canonicalUsage(usage)
	if err != nil {
		return nil, nil, false, err
	}
	reservedByBasis := make(map[aiopsv1alpha1.ProviderBillableBasis]ChargeComponent, len(reserved))
	for _, component := range reserved {
		if _, exists := reservedByBasis[component.Basis]; exists {
			return nil, nil, false, fmt.Errorf("duplicate reserved component %s", component.Basis)
		}
		reservedByBasis[component.Basis] = component
	}
	var actual []ActualChargeComponent
	var missing []aiopsv1alpha1.ProviderBillableBasis
	exceeded := false
	for _, row := range snapshot.Charges {
		if row.IncludedIn != nil {
			continue
		}
		bound, ok := reservedByBasis[row.Basis]
		if !ok || bound.PriceMicrosPerUnit != row.PriceMicrosPerUnit || bound.UnitDenominator != row.UnitDenominator {
			return nil, nil, false, fmt.Errorf("reserved component does not match frozen row %s", row.Basis)
		}
		q, ok := quantities[row.SettlementUsageField]
		if !ok {
			missing = append(missing, row.Basis)
			continue
		}
		cost, err := ceilingProduct(row.PriceMicrosPerUnit, q, row.UnitDenominator)
		if err != nil {
			return nil, nil, false, fmt.Errorf("actual charge %s: %w", row.Basis, err)
		}
		over := q > bound.Quantity
		exceeded = exceeded || over
		actual = append(actual, ActualChargeComponent{Basis: row.Basis, Quantity: q, PriceMicrosPerUnit: row.PriceMicrosPerUnit, UnitDenominator: row.UnitDenominator, ActualMicros: cost, ReservedQuantity: bound.Quantity, ExceededBound: over})
		delete(quantities, row.SettlementUsageField)
	}
	if len(quantities) != 0 {
		return nil, nil, false, errors.New("provider usage contains an unlisted billable category")
	}
	sort.Slice(actual, func(i, j int) bool { return actual[i].Basis < actual[j].Basis })
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return actual, missing, exceeded, nil
}

func SumActualComponents(components []ActualChargeComponent) (int64, error) {
	var sum int64
	for _, c := range components {
		if c.ActualMicros < 0 || sum > math.MaxInt64-c.ActualMicros {
			return 0, errors.New("actual component sum overflows int64")
		}
		sum += c.ActualMicros
	}
	return sum, nil
}

func SumComponents(components []ChargeComponent) (int64, error) {
	var sum int64
	for _, c := range components {
		if c.ReservedMicros < 0 || sum > math.MaxInt64-c.ReservedMicros {
			return 0, errors.New("component sum overflows int64")
		}
		sum += c.ReservedMicros
	}
	return sum, nil
}

func ceilingProduct(price, quantity, denominator int64) (int64, error) {
	if price < 0 || quantity < 0 || denominator <= 0 {
		return 0, errors.New("invalid price, quantity, or denominator")
	}
	n := new(big.Int).Mul(big.NewInt(price), big.NewInt(quantity))
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(n, big.NewInt(denominator), r)
	if r.Sign() != 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() {
		return 0, errors.New("charge overflows int64")
	}
	return q.Int64(), nil
}

func exactMicros(q resource.Quantity) (int64, error) {
	r, ok := new(big.Rat).SetString(q.AsDec().String())
	if !ok || r.Sign() < 0 {
		return 0, errors.New("price must be non-negative decimal")
	}
	r.Mul(r, big.NewRat(1_000_000, 1))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(r.Num(), r.Denom(), remainder)
	if remainder.Sign() != 0 {
		return 0, errors.New("price is not exactly representable in micros")
	}
	if !quotient.IsInt64() {
		return 0, errors.New("price exceeds int64")
	}
	return quotient.Int64(), nil
}
func validEvidenceMode(m aiopsv1alpha1.ProviderPricingEvidenceMode) bool {
	return m == aiopsv1alpha1.ProviderEvidenceCatalog || m == aiopsv1alpha1.ProviderEvidenceAPI || m == aiopsv1alpha1.ProviderEvidenceAdminAttested
}
func validApplicability(a aiopsv1alpha1.ProviderChargeApplicability) bool {
	return a == aiopsv1alpha1.ProviderChargeAlways || a == aiopsv1alpha1.ProviderChargeRequestDeclared || a == aiopsv1alpha1.ProviderChargeProviderResponse
}
func isBasis(b aiopsv1alpha1.ProviderBillableBasis) bool {
	for _, candidate := range AllBases {
		if b == candidate {
			return true
		}
	}
	return false
}
func isSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

// ValidSHA256 validates the lowercase immutable-digest encoding used by
// controller-produced pricing and capability evidence.
func ValidSHA256(s string) bool { return isSHA256(s) }
func fixedHash(parts ...string) string {
	h := sha256.New()
	var l [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(l[:], uint64(len(p)))
		_, _ = h.Write(l[:])
		_, _ = h.Write([]byte(p))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
