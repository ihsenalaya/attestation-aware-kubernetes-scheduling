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

// BreakEvenTarget scopes the analysis.
type BreakEvenTarget struct {
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// +optional
	Application string `json:"application,omitempty"`
}

// SelfHostedOption describes the self-hosted GPU alternative being evaluated.
type SelfHostedOption struct {
	// ModelName is the open-weights model to self-host (e.g. llama-3-70b).
	// +kubebuilder:validation:MinLength=1
	ModelName string `json:"modelName"`

	// Runtime is the open-source inference server.
	// +kubebuilder:validation:Enum=vllm;tgi;ollama;sglang
	// +kubebuilder:default=vllm
	Runtime string `json:"runtime"`

	// GpuType is the GPU SKU (e.g. l40s, a100, h100).
	// +optional
	GpuType string `json:"gpuType,omitempty"`

	// GpuCount is how many GPUs the deployment needs.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	GpuCount int32 `json:"gpuCount"`

	// MonthlyGpuCostEUR is the recurring GPU/infra cost per month.
	MonthlyGpuCostEUR resource.Quantity `json:"monthlyGpuCostEUR"`

	// EstimatedOpsCostEUR is the recurring ops/MLOps cost per month.
	// +optional
	EstimatedOpsCostEUR *resource.Quantity `json:"estimatedOpsCostEUR,omitempty"`

	// StorageNetworkCostEUR is the recurring storage/egress cost per month.
	// +optional
	StorageNetworkCostEUR *resource.Quantity `json:"storageNetworkCostEUR,omitempty"`

	// MigrationCostEUR is the one-off cost to migrate to self-hosting.
	// +optional
	MigrationCostEUR *resource.Quantity `json:"migrationCostEUR,omitempty"`
}

// AIBreakEvenAnalysisSpec defines the desired state of AIBreakEvenAnalysis.
type AIBreakEvenAnalysisSpec struct {
	// Target scopes the workload being analyzed.
	Target BreakEvenTarget `json:"target"`

	// CurrentModelRef is the AIModel currently used via a managed API.
	// +kubebuilder:validation:MinLength=1
	CurrentModelRef string `json:"currentModelRef"`

	// AlternativeSelfHosted is the self-hosting option to compare against.
	AlternativeSelfHosted SelfHostedOption `json:"alternativeSelfHosted"`

	// AnalysisWindowDays is the observation window for usage extrapolation.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=30
	AnalysisWindowDays int32 `json:"analysisWindowDays"`
}

// BreakEvenRecommendation is the engine's verdict.
// +kubebuilder:validation:Enum=keep-managed;investigate;self-host
type BreakEvenRecommendation string

const (
	RecommendationKeepManaged BreakEvenRecommendation = "keep-managed"
	RecommendationInvestigate BreakEvenRecommendation = "investigate"
	RecommendationSelfHost    BreakEvenRecommendation = "self-host"
)

// AIBreakEvenAnalysisStatus defines the observed state of AIBreakEvenAnalysis.
type AIBreakEvenAnalysisStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ManagedMonthlyCostEUR is the extrapolated managed-API monthly cost.
	// +optional
	ManagedMonthlyCostEUR *resource.Quantity `json:"managedMonthlyCostEUR,omitempty"`

	// SelfHostedMonthlyCostEUR is the estimated self-hosting monthly cost.
	// +optional
	SelfHostedMonthlyCostEUR *resource.Quantity `json:"selfHostedMonthlyCostEUR,omitempty"`

	// MonthlySavingsEUR is managed minus self-hosted (can be negative).
	// +optional
	MonthlySavingsEUR *resource.Quantity `json:"monthlySavingsEUR,omitempty"`

	// PaybackMonths is migrationCost / monthlySavings, as a decimal string
	// (e.g. "4.2"); empty when savings are non-positive.
	// +optional
	PaybackMonths string `json:"paybackMonths,omitempty"`

	// Recommendation is the engine verdict.
	// +optional
	Recommendation BreakEvenRecommendation `json:"recommendation,omitempty"`

	// Conditions represent the latest available observations of the analysis state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=aibreakeven
//+kubebuilder:printcolumn:name="CurrentModel",type=string,JSONPath=`.spec.currentModelRef`
//+kubebuilder:printcolumn:name="Recommendation",type=string,JSONPath=`.status.recommendation`
//+kubebuilder:printcolumn:name="Payback(mo)",type=string,JSONPath=`.status.paybackMonths`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIBreakEvenAnalysis compares managed-API cost vs self-hosted GPU cost and
// computes a break-even point.
type AIBreakEvenAnalysis struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIBreakEvenAnalysisSpec   `json:"spec,omitempty"`
	Status AIBreakEvenAnalysisStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIBreakEvenAnalysisList contains a list of AIBreakEvenAnalysis.
type AIBreakEvenAnalysisList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIBreakEvenAnalysis `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIBreakEvenAnalysis{}, &AIBreakEvenAnalysisList{})
}
