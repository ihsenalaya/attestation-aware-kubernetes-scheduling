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

// TelemetryMode enumerates the supported ways to pull usage telemetry from a gateway.
// (A LiteLLM-native mode was removed: its collector was an unimplemented stub, so
// advertising it sold a non-feature. Use prometheus/configmap/aigw instead.)
// +kubebuilder:validation:Enum=prometheus;fake;configmap;aigw
type TelemetryMode string

const (
	TelemetryModePrometheus TelemetryMode = "prometheus"
	TelemetryModeFake       TelemetryMode = "fake"
	// TelemetryModeAIGW reads real token usage from an Envoy AI Gateway's
	// gen_ai_* OpenTelemetry Prometheus endpoint (Endpoint + MetricsEndpoint).
	TelemetryModeAIGW TelemetryMode = "aigw"
	// TelemetryModeConfigMap reads usage samples from a ConfigMap (key
	// "usage.json" = a JSON array of usage records). This is how measured, real
	// telemetry is fed to the operator without a live gateway scrape.
	TelemetryModeConfigMap TelemetryMode = "configmap"
)

// TelemetrySpec describes how the operator collects usage data from the gateway.
type TelemetrySpec struct {
	// Mode selects the TelemetryCollector implementation.
	// +kubebuilder:default=fake
	Mode TelemetryMode `json:"mode"`

	// MetricsEndpoint is the HTTP path exposing metrics (for the prometheus mode).
	// +kubebuilder:default=/metrics
	// +optional
	MetricsEndpoint string `json:"metricsEndpoint,omitempty"`

	// SourceConfigMap is the name of the ConfigMap holding usage samples (for the
	// configmap mode); read from the gateway's own namespace, key "usage.json".
	// +optional
	SourceConfigMap string `json:"sourceConfigMap,omitempty"`
}

// GatewayAuth holds references to credentials required to query the gateway.
type GatewayAuth struct {
	// SecretRef points to a Secret holding the gateway admin/API token.
	// +optional
	SecretRef *SecretReference `json:"secretRef,omitempty"`
}

// AIGatewaySpec defines the desired state of AIGateway.
type AIGatewaySpec struct {
	// Type identifies the underlying gateway technology.
	// +kubebuilder:validation:Enum=litellm;envoy;kong;gateway-api;custom
	Type string `json:"type"`

	// Endpoint is the base URL of the gateway control/data plane.
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`

	// NamespaceSelector restricts which namespaces are governed by this gateway.
	// An empty selector governs no namespace until explicitly set.
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`

	// Telemetry describes how usage data is collected.
	Telemetry TelemetrySpec `json:"telemetry"`

	// Auth holds credential references for reaching the gateway.
	// +optional
	Auth *GatewayAuth `json:"auth,omitempty"`
}

// AIGatewayStatus defines the observed state of AIGateway.
type AIGatewayStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// GovernedNamespaces lists namespaces currently matched by NamespaceSelector.
	// +optional
	GovernedNamespaces []string `json:"governedNamespaces,omitempty"`

	// Conditions represent the latest available observations of the gateway state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=aigw
//+kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
//+kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.spec.endpoint`
//+kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIGateway represents an existing AI gateway (LiteLLM, Envoy, Kong, ...) that
// the operator observes in read-only mode.
type AIGateway struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIGatewaySpec   `json:"spec,omitempty"`
	Status AIGatewayStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIGatewayList contains a list of AIGateway.
type AIGatewayList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIGateway `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIGateway{}, &AIGatewayList{})
}
