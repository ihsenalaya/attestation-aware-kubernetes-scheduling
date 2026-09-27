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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Tier is a coarse low/medium/high ranking used for quality and cost.
// +kubebuilder:validation:Enum=low;medium;high
type Tier string

const (
	TierLow    Tier = "low"
	TierMedium Tier = "medium"
	TierHigh   Tier = "high"
)

// GOVARRoutePathMode identifies the provider adapter that must actuate a
// selected route. It is deliberately closed: an unknown adapter is not safe to
// infer from a URL or model name.
// +kubebuilder:validation:Enum=openai-body;azure-deployment-path;anthropic-body;google-generate-path
type GOVARRoutePathMode string

const (
	GOVARRouteOpenAIBody          GOVARRoutePathMode = "openai-body"
	GOVARRouteAzureDeploymentPath GOVARRoutePathMode = "azure-deployment-path"
	GOVARRouteAnthropicBody       GOVARRoutePathMode = "anthropic-body"
	GOVARRouteGoogleGeneratePath  GOVARRoutePathMode = "google-generate-path"
)

// Deprecated compatibility aliases. AIModel route fields are never safety
// authority; provider-owned gateway route bindings are authoritative.
type AIModelRoutePathMode = GOVARRoutePathMode

const (
	AIModelRouteOpenAIBody          = GOVARRouteOpenAIBody
	AIModelRouteAzureDeploymentPath = GOVARRouteAzureDeploymentPath
	AIModelRouteAnthropicBody       = GOVARRouteAnthropicBody
	AIModelRouteGoogleGeneratePath  = GOVARRouteGoogleGeneratePath
)

// AIModelRouteSpec is the complete server-owned route selected by admission.
// The gateway uses the cluster and authority while the adapter uses the
// provider deployment to rewrite the protocol-specific model or path.
type AIModelRouteSpec struct {
	// ProviderDeployment is the provider-side deployment or model identifier.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`
	ProviderDeployment string `json:"providerDeployment"`

	// Cluster is the exact Envoy cluster key configured for this route.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:-]*$`
	Cluster string `json:"cluster"`

	// Authority is the upstream HTTP authority expected by the provider adapter.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:-]*$`
	Authority string `json:"authority"`

	// PathMode selects the explicit provider request adapter.
	PathMode AIModelRoutePathMode `json:"pathMode"`
}

// +kubebuilder:validation:XValidation:rule="!self.routable || (has(self.routeBindingRef) && self.routeBindingRef != \"\")",message="routeBindingRef is required when routable is true"
// AIModelGOVARSpec contains safety-critical routing inputs. These typed fields
// supersede, but do not reinterpret, historical annotations.
type AIModelGOVARSpec struct {
	// Routable explicitly allows this model to enter the hard-feasibility set.
	Routable bool `json:"routable"`

	// RouteBindingRef selects a binding owned by spec.providerRef.
	// +optional
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	RouteBindingRef string `json:"routeBindingRef,omitempty"`

	// Route is deprecated decode compatibility only. GOV-AR ignores it.
	// +optional
	// +deprecated
	Route *AIModelRouteSpec `json:"route,omitempty"`

	// OutputCapEvidence is administrator-attested unless a provider-specific
	// capability producer replaces it with provider catalog/API evidence.
	// Existing objects decode without it but are not GOV-AR feasible.
	// +optional
	OutputCapEvidence *AIModelOutputCapEvidenceSpec `json:"outputCapEvidence,omitempty"`
}

type AIModelOutputCapEvidenceSpec struct {
	// +kubebuilder:validation:Minimum=1
	MaxOutputTokens int64 `json:"maxOutputTokens"`
	// +kubebuilder:validation:MinLength=1
	RequestParameter      string                      `json:"requestParameter"`
	EnforcedByPathAdapter bool                        `json:"enforcedByPathAdapter"`
	Mode                  ProviderPricingEvidenceMode `json:"mode"`
	// +kubebuilder:validation:MinLength=1
	SourceVersion string `json:"sourceVersion"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	EvidenceSHA256 string      `json:"evidenceSHA256"`
	ValidUntil     metav1.Time `json:"validUntil"`
	// +kubebuilder:validation:MinLength=1
	CapabilityAdapterVersion string `json:"capabilityAdapterVersion"`
}

// AIModelVerifiedOutputCapStatus is an observed provider-capability assertion.
// +kubebuilder:validation:XValidation:rule="!self.verified || self.maxOutputTokens > 0",message="maxOutputTokens must be positive when verified is true"
type AIModelVerifiedOutputCapStatus struct {
	// Verified states that the provider enforces this cap for the observed version.
	Verified bool `json:"verified"`

	// MaxOutputTokens is the verified provider-enforced maximum.
	// +kubebuilder:validation:Minimum=0
	MaxOutputTokens int64 `json:"maxOutputTokens"`

	// ObservedAt is when the capability was verified.
	ObservedAt metav1.Time `json:"observedAt"`

	// SourceVersion identifies the immutable provider/model capability source.
	// +kubebuilder:validation:MinLength=1
	SourceVersion string `json:"sourceVersion"`

	ProviderUID              string                      `json:"providerUID"`
	ProviderGeneration       int64                       `json:"providerGeneration"`
	ProviderDeployment       string                      `json:"providerDeployment"`
	ModelVersion             string                      `json:"modelVersion"`
	CapabilityAdapterVersion string                      `json:"capabilityAdapterVersion"`
	EvidenceMode             ProviderPricingEvidenceMode `json:"evidenceMode"`
	EvidenceSHA256           string                      `json:"evidenceSHA256"`
	PricingSnapshotSHA256    string                      `json:"pricingSnapshotSHA256"`
	ValidUntil               metav1.Time                 `json:"validUntil"`
	RequestParameter         string                      `json:"requestParameter"`
	EnforcedByPathAdapter    bool                        `json:"enforcedByPathAdapter"`
}

// AIModelLatencyObservation is a typed, timestamped latency sample summary.
// +kubebuilder:validation:XValidation:rule="self.p99Millis >= self.p95Millis",message="latency quantiles must be ordered p95 <= p99"
type AIModelLatencyObservation struct {
	// MeanMillis is the observed arithmetic mean in milliseconds.
	// +kubebuilder:validation:Minimum=0
	MeanMillis int64 `json:"meanMillis"`

	// P95Millis is the observed 95th percentile in milliseconds.
	// +kubebuilder:validation:Minimum=0
	P95Millis int64 `json:"p95Millis"`

	// P99Millis is the observed 99th percentile in milliseconds.
	// +kubebuilder:validation:Minimum=0
	P99Millis int64 `json:"p99Millis"`

	// SampleCount is the number of observations in the summary.
	// +kubebuilder:validation:Minimum=1
	SampleCount int64 `json:"sampleCount"`

	// ObservedAt is the end of the observation window.
	ObservedAt metav1.Time `json:"observedAt"`
}

// AIModelGOVARStatus contains controller-produced safety observations.
type AIModelGOVARStatus struct {
	// VerifiedOutputCap is absent until capability verification has run.
	// +optional
	VerifiedOutputCap *AIModelVerifiedOutputCapStatus `json:"verifiedOutputCap,omitempty"`

	// Latency is absent until a typed observation is available.
	// +optional
	Latency *AIModelLatencyObservation `json:"latency,omitempty"`
}

// AIModelSpec defines the desired state of AIModel.
type AIModelSpec struct {
	// ProviderRef is the name of the AIProvider serving this model (same namespace).
	// +kubebuilder:validation:MinLength=1
	ProviderRef string `json:"providerRef"`

	// ModelName is the provider-side model identifier (e.g. gpt-4o, mistral-small).
	// +kubebuilder:validation:MinLength=1
	ModelName string `json:"modelName"`

	// Type categorizes the model.
	// +kubebuilder:validation:Enum=llm;embedding;reranker;vision;audio
	// +kubebuilder:default=llm
	Type string `json:"type"`

	// ContextWindow is the maximum context length in tokens.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ContextWindow int32 `json:"contextWindow,omitempty"`

	// QualityTier ranks output quality.
	// +optional
	QualityTier Tier `json:"qualityTier,omitempty"`

	// CostTier ranks relative cost.
	// +optional
	CostTier Tier `json:"costTier,omitempty"`

	// SensitiveDataAllowed states whether this model may process sensitive data.
	// +optional
	SensitiveDataAllowed bool `json:"sensitiveDataAllowed,omitempty"`

	// ServesNamespace/ServesApplication/ServesTeam attribute gateway telemetry that
	// only carries the model (e.g. Envoy AI Gateway gen_ai metrics) to the workload
	// consuming this model, so cost is attributed per namespace/app/team.
	// +optional
	ServesNamespace string `json:"servesNamespace,omitempty"`
	// +optional
	ServesApplication string `json:"servesApplication,omitempty"`
	// +optional
	ServesTeam string `json:"servesTeam,omitempty"`

	// GOVAR contains typed hard-feasibility and route-actuation inputs. When
	// absent, admission must not infer these values from annotations.
	// +optional
	GOVAR *AIModelGOVARSpec `json:"govar,omitempty"`
}

// AIModelStatus defines the observed state of AIModel.
type AIModelStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ResolvedProvider echoes the provider type once ProviderRef is resolved.
	// +optional
	ResolvedProvider string `json:"resolvedProvider,omitempty"`

	// LastQualityScore is the latest AIQualityGate candidate score observed for this model.
	// +optional
	LastQualityScore float64 `json:"lastQualityScore,omitempty"`

	// LastEvaluatedAt is the time of the latest AIQualityGate score update for this model.
	// +optional
	LastEvaluatedAt *metav1.Time `json:"lastEvaluatedAt,omitempty"`

	// Conditions represent the latest available observations of the model state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// GOVAR contains observed, controller-owned cap and latency evidence.
	// +optional
	GOVAR *AIModelGOVARStatus `json:"govar,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.providerRef`
//+kubebuilder:printcolumn:name="Model",type=string,JSONPath=`.spec.modelName`
//+kubebuilder:printcolumn:name="CostTier",type=string,JSONPath=`.spec.costTier`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIModel catalogs an AI model available through a provider.
type AIModel struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIModelSpec   `json:"spec,omitempty"`
	Status AIModelStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIModelList contains a list of AIModel.
type AIModelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIModel `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIModel{}, &AIModelList{})
}
