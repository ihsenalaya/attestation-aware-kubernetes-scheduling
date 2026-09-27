/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProviderPricingCompleteness records whether the versioned price snapshot
// covers every category the provider may bill for.
// +kubebuilder:validation:Enum=unknown;partial;complete
type ProviderPricingCompleteness string

const (
	ProviderPricingUnknown  ProviderPricingCompleteness = "unknown"
	ProviderPricingPartial  ProviderPricingCompleteness = "partial"
	ProviderPricingComplete ProviderPricingCompleteness = "complete"
)

// ProviderBillableUnit defines the denominator for an additional charge.
// +kubebuilder:validation:Enum=per-million-tokens;per-request;per-second;per-unit
type ProviderBillableUnit string

// ProviderBillableBasis is the closed accounting dimension understood by the
// GOV-AR ledger and provider adapters.
// +kubebuilder:validation:Enum=input_tokens;cached_input_tokens;output_tokens;reasoning_tokens;request;tool_call;media_unit;billable_second;cancellation;retry_attempt
type ProviderBillableBasis string

const (
	ProviderBasisInputTokens       ProviderBillableBasis = "input_tokens"
	ProviderBasisCachedInputTokens ProviderBillableBasis = "cached_input_tokens"
	ProviderBasisOutputTokens      ProviderBillableBasis = "output_tokens"
	ProviderBasisReasoningTokens   ProviderBillableBasis = "reasoning_tokens"
	ProviderBasisRequest           ProviderBillableBasis = "request"
	ProviderBasisToolCall          ProviderBillableBasis = "tool_call"
	ProviderBasisMediaUnit         ProviderBillableBasis = "media_unit"
	ProviderBasisBillableSecond    ProviderBillableBasis = "billable_second"
	ProviderBasisCancellation      ProviderBillableBasis = "cancellation"
	ProviderBasisRetryAttempt      ProviderBillableBasis = "retry_attempt"
)

// ProviderChargeApplicability describes when a charge can occur.
// +kubebuilder:validation:Enum=always;request_declared;provider_response
type ProviderChargeApplicability string

const (
	ProviderChargeAlways           ProviderChargeApplicability = "always"
	ProviderChargeRequestDeclared  ProviderChargeApplicability = "request_declared"
	ProviderChargeProviderResponse ProviderChargeApplicability = "provider_response"
)

// ProviderPricingEvidenceMode records who supplied pricing/capability evidence.
// Only provider_catalog and provider_api may be described as provider verified.
// +kubebuilder:validation:Enum=provider_catalog;provider_api;admin_attested
type ProviderPricingEvidenceMode string

const (
	ProviderEvidenceCatalog       ProviderPricingEvidenceMode = "provider_catalog"
	ProviderEvidenceAPI           ProviderPricingEvidenceMode = "provider_api"
	ProviderEvidenceAdminAttested ProviderPricingEvidenceMode = "admin_attested"
)

// ProviderRequestBoundField is a closed request field from which a pre-dispatch
// quantity bound is obtained.
// +kubebuilder:validation:Enum=input_tokens;max_cached_input_tokens;max_output_tokens;max_reasoning_tokens;max_tool_calls;max_media_units;timeout_seconds;max_retry_attempts;cancellation_possible
type ProviderRequestBoundField string

// ProviderBillableCharge is the typed GOV-AR pricing input. Historical
// BillableCategories remain decodable but are never monetary authority.
type ProviderBillableCharge struct {
	Basis         ProviderBillableBasis       `json:"basis"`
	Applicability ProviderChargeApplicability `json:"applicability"`
	// Price is converted exactly to integer micro-currency units by the controller.
	Price resource.Quantity `json:"price"`
	// UnitDenominator is the number of usage units represented by Price.
	// +kubebuilder:validation:Minimum=1
	UnitDenominator      int64                 `json:"unitDenominator"`
	SettlementUsageField ProviderBillableBasis `json:"settlementUsageField"`
	// IncludedIn declares that this quantity is already included in another
	// priced basis and therefore must not be charged separately.
	// +optional
	IncludedIn *ProviderBillableBasis `json:"includedIn,omitempty"`
	// MaximumQuantity is a provider-enforced upper quantity.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaximumQuantity *int64 `json:"maximumQuantity,omitempty"`
	// RequestBoundField names a typed request field that supplies the upper bound.
	// +optional
	RequestBoundField ProviderRequestBoundField `json:"requestBoundField,omitempty"`
	// DisjointUsage asserts that a separately priced cached-input or reasoning
	// quantity is normalized by the adapter so it does not overlap the base
	// input/output quantity. It must be true for those bases unless IncludedIn
	// is used. This prevents silently charging the same token twice.
	// +optional
	DisjointUsage bool `json:"disjointUsage,omitempty"`
}

type AIProviderPricingEvidenceSpec struct {
	Mode ProviderPricingEvidenceMode `json:"mode"`
	// +kubebuilder:validation:MinLength=1
	SourceVersion string `json:"sourceVersion"`
	// EvidenceSHA256 binds the immutable source document or provider response.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	EvidenceSHA256 string      `json:"evidenceSHA256"`
	ValidUntil     metav1.Time `json:"validUntil"`
	// AdapterVersion must match the closed adapter selected for provider type.
	// +kubebuilder:validation:MinLength=1
	AdapterVersion string `json:"adapterVersion"`
}

type AIProviderGOVARPricingSpec struct {
	Evidence AIProviderPricingEvidenceSpec `json:"evidence"`
	// +optional
	// +listType=map
	// +listMapKey=basis
	Charges []ProviderBillableCharge `json:"charges,omitempty"`
	// InapplicableBases are an explicit adapter-validated declaration that the
	// selected deployment/request mode cannot bill these bases.
	// +optional
	// +listType=set
	InapplicableBases []ProviderBillableBasis `json:"inapplicableBases,omitempty"`
}

type AIProviderNormalizedChargeStatus struct {
	Basis                ProviderBillableBasis       `json:"basis"`
	Applicability        ProviderChargeApplicability `json:"applicability"`
	PriceMicrosPerUnit   int64                       `json:"priceMicrosPerUnit"`
	UnitDenominator      int64                       `json:"unitDenominator"`
	SettlementUsageField ProviderBillableBasis       `json:"settlementUsageField"`
	// +optional
	IncludedIn *ProviderBillableBasis `json:"includedIn,omitempty"`
	// +optional
	MaximumQuantity *int64 `json:"maximumQuantity,omitempty"`
	// +optional
	RequestBoundField ProviderRequestBoundField `json:"requestBoundField,omitempty"`
	// +optional
	DisjointUsage bool `json:"disjointUsage,omitempty"`
}

type AIProviderPricingSnapshotStatus struct {
	SpecGeneration int64                       `json:"specGeneration"`
	Version        string                      `json:"version"`
	ObservedAt     metav1.Time                 `json:"observedAt"`
	ValidUntil     metav1.Time                 `json:"validUntil"`
	Currency       string                      `json:"currency"`
	Completeness   ProviderPricingCompleteness `json:"completeness"`
	SnapshotSHA256 string                      `json:"snapshotSHA256"`
	AdapterVersion string                      `json:"adapterVersion"`
	EvidenceMode   ProviderPricingEvidenceMode `json:"evidenceMode"`
	EvidenceSHA256 string                      `json:"evidenceSHA256"`
	SourceVersion  string                      `json:"sourceVersion"`
	// +listType=map
	// +listMapKey=basis
	Charges []AIProviderNormalizedChargeStatus `json:"charges"`
	// +optional
	// +listType=set
	InapplicableBases []ProviderBillableBasis `json:"inapplicableBases,omitempty"`
}

type AIProviderGOVARStatus struct {
	// PricingSnapshot is absent until normalization and evidence validation pass.
	// +optional
	PricingSnapshot *AIProviderPricingSnapshotStatus `json:"pricingSnapshot,omitempty"`
}

// ProviderBillableCategory is a versioned non-base charge that must be included
// in liability calculation when applicable (for example cached input, reasoning,
// tool, media, request, time, cancellation, or retry charges).
type ProviderBillableCategory struct {
	// Name is a stable lower-case category key.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$`
	Name string `json:"name"`

	// Unit specifies the price denominator.
	Unit ProviderBillableUnit `json:"unit"`

	// Price is the decimal price in Pricing.Currency for one Unit.
	Price resource.Quantity `json:"price"`
}

// ProviderPricing captures the unit economics of a provider. Monetary values use
// resource.Quantity so decimal amounts (e.g. "2.5") are stored exactly and are
// idiomatic to the Kubernetes API (no IEEE-754 floats in the schema).
type ProviderPricing struct {
	// Currency is the ISO 4217 code for all prices below.
	// +kubebuilder:default=EUR
	Currency string `json:"currency"`

	// InputTokenPricePerMillion is the price per 1,000,000 input tokens.
	InputTokenPricePerMillion resource.Quantity `json:"inputTokenPricePerMillion"`

	// OutputTokenPricePerMillion is the price per 1,000,000 output tokens.
	OutputTokenPricePerMillion resource.Quantity `json:"outputTokenPricePerMillion"`

	// FixedMonthlyCost is any flat monthly fee for the provider (commitments,
	// reserved capacity, ...). Used by the break-even engine.
	// +optional
	FixedMonthlyCost *resource.Quantity `json:"fixedMonthlyCost,omitempty"`

	// Version is the immutable identifier of this provider price snapshot.
	// Historical objects remain readable when it is absent, but GOV-AR must not
	// treat an absent version as complete pricing evidence.
	// +optional
	Version string `json:"version,omitempty"`

	// ObservedAt is when this price snapshot was verified against its source.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`

	// Completeness states whether all possible billable categories are represented.
	// +optional
	// +kubebuilder:default=unknown
	Completeness ProviderPricingCompleteness `json:"completeness,omitempty"`

	// BillableCategories lists additional charges not represented by the base
	// input/output token prices. Names are unique map keys.
	// +optional
	// +listType=map
	// +listMapKey=name
	BillableCategories []ProviderBillableCategory `json:"billableCategories,omitempty"`
}

// ProviderCompliance describes sovereignty/compliance attributes of a provider.
type ProviderCompliance struct {
	// AllowedForSensitiveData states whether sensitive data may be sent here.
	// +optional
	AllowedForSensitiveData bool `json:"allowedForSensitiveData,omitempty"`

	// AllowedCountries lists ISO country/zone codes (e.g. FR, EU) this provider serves from.
	// +optional
	AllowedCountries []string `json:"allowedCountries,omitempty"`
}

// AIProviderGatewayRouteBinding is a complete provider-owned gateway target.
type AIProviderGatewayRouteBinding struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`
	ProviderDeployment string `json:"providerDeployment"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:-]*$`
	Cluster string `json:"cluster"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:-]*$`
	Authority string             `json:"authority"`
	PathMode  GOVARRoutePathMode `json:"pathMode"`
}

type AIProviderGOVARSpec struct {
	// +optional
	// +listType=map
	// +listMapKey=name
	GatewayRoutes []AIProviderGatewayRouteBinding `json:"gatewayRoutes,omitempty"`

	// Pricing is the only GOV-AR monetary authority. Legacy pricing fields remain
	// readable for older controllers but cannot make a candidate feasible alone.
	// +optional
	Pricing *AIProviderGOVARPricingSpec `json:"pricing,omitempty"`
}

// AIProviderSpec defines the desired state of AIProvider.
type AIProviderSpec struct {
	// Type identifies the provider family.
	// +kubebuilder:validation:Enum=openai;azure-openai;mistral;anthropic;bedrock;vertex;self-hosted;custom
	Type string `json:"type"`

	// Region is the cloud/provider region (e.g. francecentral).
	// +optional
	Region string `json:"region,omitempty"`

	// DataResidency is the country/zone where data is processed (e.g. france, eu, us).
	// +optional
	DataResidency string `json:"dataResidency,omitempty"`

	// Managed distinguishes a managed API provider (true) from self-hosted GPU (false).
	// +optional
	Managed bool `json:"managed,omitempty"`

	// Pricing holds the unit economics for cost calculations.
	Pricing ProviderPricing `json:"pricing"`

	// Compliance describes sovereignty attributes.
	// +optional
	Compliance ProviderCompliance `json:"compliance,omitempty"`

	// GOVAR contains provider-administrator-owned route bindings.
	// +optional
	GOVAR *AIProviderGOVARSpec `json:"govar,omitempty"`
}

// AIProviderStatus defines the observed state of AIProvider.
type AIProviderStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the provider state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// GOVAR contains controller-normalized pricing evidence.
	// +optional
	GOVAR *AIProviderGOVARStatus `json:"govar,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=aiprov
//+kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
//+kubebuilder:printcolumn:name="Residency",type=string,JSONPath=`.spec.dataResidency`
//+kubebuilder:printcolumn:name="Managed",type=boolean,JSONPath=`.spec.managed`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIProvider represents a model provider (managed API or self-hosted GPU) along
// with its pricing and sovereignty attributes.
type AIProvider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIProviderSpec   `json:"spec,omitempty"`
	Status AIProviderStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIProviderList contains a list of AIProvider.
type AIProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIProvider `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIProvider{}, &AIProviderList{})
}
