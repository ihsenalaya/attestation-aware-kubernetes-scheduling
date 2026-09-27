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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// AIRouteOverrideSpec defines the desired reroute.
type AIRouteOverrideSpec struct {
	// SourceModel is the provider-side model id currently serving traffic.
	// +kubebuilder:validation:MinLength=1
	SourceModel string `json:"sourceModel"`

	// TargetModel is the provider-side model id to route traffic to instead.
	// +kubebuilder:validation:MinLength=1
	TargetModel string `json:"targetModel"`

	// Reason is a human-readable note explaining why this override was created.
	// +optional
	Reason string `json:"reason,omitempty"`
}

// AIRouteOverridePhase captures the lifecycle state of the override.
// +kubebuilder:validation:Enum=Pending;Actuated;Failed;Reverted
type AIRouteOverridePhase string

const (
	AIRouteOverridePending  AIRouteOverridePhase = "Pending"
	AIRouteOverrideActuated AIRouteOverridePhase = "Actuated"
	AIRouteOverrideFailed   AIRouteOverridePhase = "Failed"
	AIRouteOverrideReverted AIRouteOverridePhase = "Reverted"
)

// AIRouteOverrideStatus reflects the observed state of the reroute.
type AIRouteOverrideStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase summarises the actuation lifecycle.
	// +optional
	Phase AIRouteOverridePhase `json:"phase,omitempty"`

	// Message provides a human-readable explanation of the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// ActuatedRoutes lists the AIGatewayRoute names that were modified.
	// +optional
	ActuatedRoutes []string `json:"actuatedRoutes,omitempty"`

	// Conditions represent the latest available observations of the override state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=airoverride
//+kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceModel`
//+kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetModel`
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIRouteOverride manually redirects traffic from one model to another in the
// Envoy AI Gateway data plane. Deleting the resource automatically reverts the
// route to its original backend — no manual cleanup required.
type AIRouteOverride struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIRouteOverrideSpec   `json:"spec,omitempty"`
	Status AIRouteOverrideStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIRouteOverrideList contains a list of AIRouteOverride.
type AIRouteOverrideList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIRouteOverride `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIRouteOverride{}, &AIRouteOverrideList{})
}
