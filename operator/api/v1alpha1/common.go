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

// Shared condition types and reasons used across all aiops CRDs.
//
// Every CRD exposes a standard "Ready" condition plus a resource-specific
// condition. Controllers set these via meta.SetStatusCondition so that
// `kubectl describe` and tooling observe Kubernetes-standard conditions.
const (
	// ConditionReady is the top-level readiness condition present on every CRD.
	ConditionReady = "Ready"

	// ConditionValidated indicates the spec passed semantic validation
	// (referenced objects exist, values are coherent).
	ConditionValidated = "Validated"
)

// Standard condition reasons (PascalCase, as required by the Kubernetes API).
const (
	ReasonReconciled        = "Reconciled"
	ReasonReconcileError    = "ReconcileError"
	ReasonValidationFailed  = "ValidationFailed"
	ReasonReferenceNotFound = "ReferenceNotFound"
	ReasonReportGenerated   = "ReportGenerated"
	// ReasonNoTelemetry signals that no real telemetry source is configured, so the
	// operator refuses to fabricate numbers (no silent fake-data fallback).
	ReasonNoTelemetry = "NoTelemetrySource"
)

// SecretReference points to a Kubernetes Secret holding credentials needed to
// talk to an external system (e.g. a gateway admin API). It intentionally only
// carries a name; the namespace is the owning resource's namespace.
type SecretReference struct {
	// Name of the Secret in the same namespace as the referencing resource.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key within the Secret to read. Optional; defaults are collector-specific.
	// +optional
	Key string `json:"key,omitempty"`
}
