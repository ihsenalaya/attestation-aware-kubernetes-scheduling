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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// AIWorkloadBindingSpec is the operator-owned governance identity for Pods
// authenticated as one ServiceAccount. Admission resolves this object by the
// Pod's live namespace and spec.serviceAccountName; the Pod does not name it.
type AIWorkloadBindingSpec struct {
	// ServiceAccountName is both the authenticated workload principal and the
	// required metadata.name of this binding.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	ServiceAccountName string `json:"serviceAccountName"`

	// TenantID is the immutable financial-isolation identity assigned by the operator.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`
	TenantID string `json:"tenantID"`

	// Team is the operator-assigned organizational identity.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`
	Team string `json:"team"`

	// Application is the operator-assigned application identity.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`
	Application string `json:"application"`

	// BudgetPolicyRef names an AIBudgetPolicy in the same namespace.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	BudgetPolicyRef string `json:"budgetPolicyRef"`

	// RoutingPolicyRef names an AIRoutingPolicy in the same namespace.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	RoutingPolicyRef string `json:"routingPolicyRef"`

	// Sensitivity is the maximum data sensitivity assigned to this workload.
	// +kubebuilder:validation:Enum=low;medium;high
	Sensitivity Tier `json:"sensitivity"`

	// AllowedZones is a non-empty, normalized lower-case set of permitted
	// provider regions or residency zones.
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`
	AllowedZones []string `json:"allowedZones"`

	// RequireGateway makes the authenticated GOV-AR gateway mandatory. It
	// defaults to true; setting it false is an explicit operator decision.
	// +kubebuilder:default=true
	RequireGateway bool `json:"requireGateway"`
}

// AIWorkloadBindingResolvedReference binds a policy name to the exact object
// identity and generation resolved by the controller.
type AIWorkloadBindingResolvedReference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`

	// Generation is the referenced object's observed metadata.generation.
	// +kubebuilder:validation:Minimum=1
	Generation int64 `json:"generation"`
}

// AIWorkloadBindingStatus describes the generation and immutable dependency
// identities consumed by enforcement.
type AIWorkloadBindingStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ResolvedServiceAccountUID is the UID of the live namespaced ServiceAccount.
	// Admission compares it with the authenticated Pod's ServiceAccount.
	// +optional
	ResolvedServiceAccountUID types.UID `json:"resolvedServiceAccountUID,omitempty"`

	// ResolvedBudgetPolicy binds the configured policy name to its UID/generation.
	// +optional
	ResolvedBudgetPolicy *AIWorkloadBindingResolvedReference `json:"resolvedBudgetPolicy,omitempty"`

	// ResolvedRoutingPolicy binds the configured policy name to its UID/generation.
	// +optional
	ResolvedRoutingPolicy *AIWorkloadBindingResolvedReference `json:"resolvedRoutingPolicy,omitempty"`

	// Conditions represent resolution and enforcement readiness.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=aiwb
// +kubebuilder:validation:XValidation:rule="self.metadata.name == self.spec.serviceAccountName",message="metadata.name must equal spec.serviceAccountName"
// +kubebuilder:validation:XValidation:rule="self.spec == oldSelf.spec",message="spec is immutable; replace the binding to change governance identity or policy"
// +kubebuilder:printcolumn:name="ServiceAccount",type=string,JSONPath=`.spec.serviceAccountName`
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantID`
// +kubebuilder:printcolumn:name="Gateway",type=boolean,JSONPath=`.spec.requireGateway`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIWorkloadBinding binds a namespaced ServiceAccount to operator-owned GOV-AR
// identity and policy. Application authors must not receive create, update,
// patch, or delete RBAC for this resource.
type AIWorkloadBinding struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIWorkloadBindingSpec   `json:"spec,omitempty"`
	Status AIWorkloadBindingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AIWorkloadBindingList contains a list of AIWorkloadBinding.
type AIWorkloadBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIWorkloadBinding `json:"items"`
}

// Validate checks the semantic invariants also represented by the CRD schema.
// It is usable by admission and controller tests without requiring an API server.
func (s AIWorkloadBindingSpec) Validate(objectName string, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	required := []struct {
		name  string
		value string
	}{
		{"serviceAccountName", s.ServiceAccountName},
		{"tenantID", s.TenantID},
		{"team", s.Team},
		{"application", s.Application},
		{"budgetPolicyRef", s.BudgetPolicyRef},
		{"routingPolicyRef", s.RoutingPolicyRef},
	}
	for _, item := range required {
		if strings.TrimSpace(item.value) == "" {
			errs = append(errs, field.Required(path.Child(item.name), "must not be empty"))
		}
	}
	if objectName != s.ServiceAccountName {
		errs = append(errs, field.Invalid(path.Child("serviceAccountName"), s.ServiceAccountName, "must equal metadata.name"))
	}
	if len(s.AllowedZones) == 0 {
		errs = append(errs, field.Required(path.Child("allowedZones"), "at least one normalized zone is required"))
	}
	seen := make(map[string]struct{}, len(s.AllowedZones))
	for i, zone := range s.AllowedZones {
		normalized := strings.ToLower(strings.TrimSpace(zone))
		if zone == "" || zone != normalized {
			errs = append(errs, field.Invalid(path.Child("allowedZones").Index(i), zone, "must be non-empty, lower-case, and have no surrounding whitespace"))
		}
		if _, ok := seen[normalized]; ok {
			errs = append(errs, field.Duplicate(path.Child("allowedZones").Index(i), zone))
		}
		seen[normalized] = struct{}{}
	}
	return errs
}

func init() {
	SchemeBuilder.Register(&AIWorkloadBinding{}, &AIWorkloadBindingList{})
}
