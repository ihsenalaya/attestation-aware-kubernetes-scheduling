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

// RawAttestationReportSpec is written by the node-attestation-agent running on a
// confidential node. It carries ONLY the raw, unverified attestation material.
// The agent has NO permission to write AttestationEvidence — that is exclusively
// the central verifier's job (trust-chain separation, gate G0).
type RawAttestationReportSpec struct {
	// NodeName is the node the agent is running on.
	// +kubebuilder:validation:MinLength=1
	NodeName string `json:"nodeName"`

	// NodeUID is the node object UID as observed by the agent.
	// +optional
	NodeUID string `json:"nodeUID,omitempty"`

	// Provider identifies the attestation source (maa|simulator).
	// +kubebuilder:validation:Enum=maa;simulator
	Provider string `json:"provider"`

	// RawToken is the raw MAA / guest-attestation token (JWT) as collected.
	// It is UNVERIFIED. The verifier is responsible for validating it.
	// +optional
	RawToken string `json:"rawToken,omitempty"`

	// RawTokenHash is the SHA-256 of RawToken, for tamper-evidence in logs.
	// +optional
	RawTokenHash string `json:"rawTokenHash,omitempty"`

	// Nonce is the freshness nonce the agent embedded in the attestation request.
	// +optional
	Nonce string `json:"nonce,omitempty"`

	// CollectedAt is when the agent collected the report.
	CollectedAt metav1.Time `json:"collectedAt"`

	// AgentPodUID / AgentServiceAccount identify the collecting agent for audit.
	// +optional
	AgentPodUID string `json:"agentPodUID,omitempty"`
	// +optional
	AgentServiceAccount string `json:"agentServiceAccount,omitempty"`

	// Simulated marks kind/dev reports that must never become real evidence.
	// +optional
	Simulated bool `json:"simulated,omitempty"`
}

type RawAttestationReportStatus struct {
	// ObservedGeneration is the last generation observed by the verifier.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Processed indicates the verifier has consumed this report.
	// +optional
	Processed bool `json:"processed,omitempty"`

	// EvidenceRef names the AttestationEvidence produced from this report, if any.
	// +optional
	EvidenceRef string `json:"evidenceRef,omitempty"`

	// VerificationStatus mirrors the verifier outcome (Verified|Failed|Unavailable).
	// +optional
	VerificationStatus string `json:"verificationStatus,omitempty"`

	// Conditions captures processing state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rar
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeName`
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.provider`
// +kubebuilder:printcolumn:name="Processed",type=boolean,JSONPath=`.status.processed`

// RawAttestationReport is the untrusted, agent-produced attestation material.
type RawAttestationReport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RawAttestationReportSpec   `json:"spec,omitempty"`
	Status RawAttestationReportStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type RawAttestationReportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RawAttestationReport `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RawAttestationReport{}, &RawAttestationReportList{})
}
