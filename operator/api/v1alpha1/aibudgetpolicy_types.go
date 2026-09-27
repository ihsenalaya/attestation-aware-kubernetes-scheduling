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

// BudgetTarget scopes a budget to a namespace, team and/or application.
type BudgetTarget struct {
	// Namespace the budget applies to.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Team label value the budget applies to.
	// +optional
	Team string `json:"team,omitempty"`

	// Application label value the budget applies to.
	// +optional
	Application string `json:"application,omitempty"`
}

// BudgetActions lists recommended (MVP: non-enforcing) actions per threshold.
type BudgetActions struct {
	// +optional
	OnWarning []string `json:"onWarning,omitempty"`
	// +optional
	OnCritical []string `json:"onCritical,omitempty"`
	// +optional
	OnHardLimit []string `json:"onHardLimit,omitempty"`
}

// BudgetFallbackPhase decides when a live fallback may be actuated.
// +kubebuilder:validation:Enum=Critical;Exceeded
type BudgetFallbackPhase string

const (
	BudgetFallbackOnCritical BudgetFallbackPhase = "Critical"
	BudgetFallbackOnExceeded BudgetFallbackPhase = "Exceeded"
)

// AIBudgetPolicySpec defines the desired state of AIBudgetPolicy.
type AIBudgetPolicySpec struct {
	// Target scopes the budget.
	Target BudgetTarget `json:"target"`

	// Period is the budget window.
	// +kubebuilder:validation:Enum=daily;weekly;monthly
	// +kubebuilder:default=monthly
	Period string `json:"period"`

	// BudgetEUR is the spend limit for the period, in EUR.
	BudgetEUR resource.Quantity `json:"budgetEUR"`

	// WarningThresholdPercent triggers the warning actions.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=70
	WarningThresholdPercent int32 `json:"warningThresholdPercent"`

	// CriticalThresholdPercent triggers the critical actions.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=90
	CriticalThresholdPercent int32 `json:"criticalThresholdPercent"`

	// HardLimitPercent triggers the hard-limit actions.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=200
	// +kubebuilder:default=100
	HardLimitPercent int32 `json:"hardLimitPercent"`

	// Actions are recommended per threshold. In the MVP these are surfaced as
	// recommendations only and never enforced.
	// +optional
	Actions BudgetActions `json:"actions,omitempty"`

	// FallbackModelRef names a cheaper AIModel to recommend on overspend.
	// +optional
	FallbackModelRef string `json:"fallbackModelRef,omitempty"`

	// EnforcementMode controls whether the fallback stays advisory or may be
	// actuated at the gateway when the budget is under pressure.
	// +kubebuilder:default=reportOnly
	EnforcementMode EnforcementMode `json:"enforcementMode,omitempty"`

	// FallbackOnPhase is the earliest budget phase that may activate the
	// fallback when EnforcementMode=enforce.
	// +kubebuilder:default=Exceeded
	FallbackOnPhase BudgetFallbackPhase `json:"fallbackOnPhase,omitempty"`

	// MaxFallbackLatencyMillis is a guardrail on the fallback model's observed
	// mean latency. 0 disables the latency guardrail.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxFallbackLatencyMillis int32 `json:"maxFallbackLatencyMillis,omitempty"`

	// MaxFallbackErrorPercent is a guardrail on the fallback model's observed
	// error rate, in percent. 0 disables the error-rate guardrail.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MaxFallbackErrorPercent int32 `json:"maxFallbackErrorPercent,omitempty"`

	// MinFallbackQualityTier is the minimum catalog quality tier a fallback
	// model must satisfy to be actuated. Empty disables the quality guardrail.
	// +optional
	MinFallbackQualityTier Tier `json:"minFallbackQualityTier,omitempty"`
}

// AIBudgetPolicyStatus defines the observed state of AIBudgetPolicy.
type AIBudgetPolicyStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// CurrentSpendEUR is the spend observed for the current period.
	// +optional
	CurrentSpendEUR *resource.Quantity `json:"currentSpendEUR,omitempty"`

	// ProjectedMonthlySpendEUR forecasts a full month from the observed spend
	// (run-rate); usage/phase are evaluated against this forecast vs the budget.
	// +optional
	ProjectedMonthlySpendEUR *resource.Quantity `json:"projectedMonthlySpendEUR,omitempty"`

	// UsagePercent is ProjectedMonthlySpend/Budget*100, rounded.
	// +optional
	UsagePercent int32 `json:"usagePercent,omitempty"`

	// Phase summarizes the budget state.
	// +kubebuilder:validation:Enum=Unknown;WithinBudget;Warning;Critical;Exceeded
	// +optional
	Phase string `json:"phase,omitempty"`

	// ActiveFallbackModel is the provider-side model currently selected as the
	// budget fallback target when live actuation is active.
	// +optional
	ActiveFallbackModel string `json:"activeFallbackModel,omitempty"`

	// FallbackActuated reports whether the fallback reroute was applied to the
	// gateway on the last reconcile.
	// +optional
	FallbackActuated bool `json:"fallbackActuated,omitempty"`

	// FallbackReason summarizes why the fallback was or was not actuated.
	// +optional
	FallbackReason string `json:"fallbackReason,omitempty"`

	// Conditions represent the latest available observations of the budget state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=aibudget
//+kubebuilder:printcolumn:name="Budget(EUR)",type=string,JSONPath=`.spec.budgetEUR`
//+kubebuilder:printcolumn:name="Usage%",type=integer,JSONPath=`.status.usagePercent`
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIBudgetPolicy defines an AI spend budget for a namespace/team/application.
type AIBudgetPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIBudgetPolicySpec   `json:"spec,omitempty"`
	Status AIBudgetPolicyStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIBudgetPolicyList contains a list of AIBudgetPolicy.
type AIBudgetPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIBudgetPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIBudgetPolicy{}, &AIBudgetPolicyList{})
}
