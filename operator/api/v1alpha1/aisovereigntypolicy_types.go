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

// DataResidencyRule constrains where data may be processed.
type DataResidencyRule struct {
	// AllowedZones lists permitted country/zone codes (e.g. FR, EU).
	// +optional
	AllowedZones []string `json:"allowedZones,omitempty"`

	// ForbiddenZones lists explicitly forbidden codes (e.g. US, CN).
	// +optional
	ForbiddenZones []string `json:"forbiddenZones,omitempty"`
}

// SensitiveDataRule governs handling of sensitive data.
type SensitiveDataRule struct {
	// ExternalProvidersAllowed permits sending sensitive data to external/managed providers.
	// +optional
	ExternalProvidersAllowed bool `json:"externalProvidersAllowed,omitempty"`

	// RequireAnonymization flags that prompts should be anonymized first.
	// +optional
	RequireAnonymization bool `json:"requireAnonymization,omitempty"`
}

// AuditRule describes audit-trail expectations.
type AuditRule struct {
	// RetainLogsDays is the desired retention window for audit logs.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RetainLogsDays int32 `json:"retainLogsDays,omitempty"`

	// ImmutableLogs flags that audit logs should be tamper-evident.
	// +optional
	ImmutableLogs bool `json:"immutableLogs,omitempty"`
}

// EnforcementMode controls how the sovereignty engine reacts to violations.
// reportOnly records findings only; warn raises differentiated alerts (Kubernetes
// Events + enforcement metric) without blocking; enforce additionally blocks or
// reroutes non-compliant traffic at the Envoy AI Gateway when the route can be
// mutated safely.
// +kubebuilder:validation:Enum=reportOnly;warn;enforce
type EnforcementMode string

const (
	EnforcementReportOnly EnforcementMode = "reportOnly"
	EnforcementWarn       EnforcementMode = "warn"
	EnforcementEnforce    EnforcementMode = "enforce"
)

// AISovereigntyPolicySpec defines the desired state of AISovereigntyPolicy.
type AISovereigntyPolicySpec struct {
	// DataResidency constrains processing zones.
	// +optional
	DataResidency DataResidencyRule `json:"dataResidency,omitempty"`

	// SensitiveData governs sensitive-data handling.
	// +optional
	SensitiveData SensitiveDataRule `json:"sensitiveData,omitempty"`

	// Audit describes audit-trail expectations.
	// +optional
	Audit AuditRule `json:"audit,omitempty"`

	// EnforcementMode controls reactions: reportOnly | warn | enforce.
	// +kubebuilder:default=reportOnly
	EnforcementMode EnforcementMode `json:"enforcementMode"`
}

// AISovereigntyPolicyStatus defines the observed state of AISovereigntyPolicy.
type AISovereigntyPolicyStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// FindingsCount is the number of violations detected on the last evaluation.
	// +optional
	FindingsCount int32 `json:"findingsCount,omitempty"`

	// Conditions represent the latest available observations of the policy state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=aisov
//+kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.enforcementMode`
//+kubebuilder:printcolumn:name="Findings",type=integer,JSONPath=`.status.findingsCount`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AISovereigntyPolicy defines data-residency and sovereignty rules.
type AISovereigntyPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AISovereigntyPolicySpec   `json:"spec,omitempty"`
	Status AISovereigntyPolicyStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AISovereigntyPolicyList contains a list of AISovereigntyPolicy.
type AISovereigntyPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AISovereigntyPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AISovereigntyPolicy{}, &AISovereigntyPolicyList{})
}
