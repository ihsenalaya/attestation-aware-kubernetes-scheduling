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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AIChangeRequestAction describes what action the operator should take once approved.
// +kubebuilder:validation:Enum=reroute;authorize-gov-ar-route
type AIChangeRequestAction string

const (
	// AIChangeRequestActionReroute asks the operator to reroute traffic from
	// SourceModel to TargetModel in the Envoy AI Gateway data plane.
	AIChangeRequestActionReroute AIChangeRequestAction = "reroute"

	// AIChangeRequestActionAuthorizeGOVARRoute approves a bounded, reusable
	// governance scope. It does not actuate a route and is never created per
	// request. Admission still revalidates the complete live candidate.
	AIChangeRequestActionAuthorizeGOVARRoute AIChangeRequestAction = "authorize-gov-ar-route"
)

// AIChangeRequestApproval is the human decision on a change request.
// +kubebuilder:validation:Enum=Pending;Approved;Rejected
type AIChangeRequestApproval string

const (
	AIChangeRequestApprovalPending  AIChangeRequestApproval = "Pending"
	AIChangeRequestApprovalApproved AIChangeRequestApproval = "Approved"
	AIChangeRequestApprovalRejected AIChangeRequestApproval = "Rejected"
)

// AIChangeRequestPhase is the lifecycle state of the change request.
// +kubebuilder:validation:Enum=Pending;Approved;Actuated;Rejected;Expired;Failed
type AIChangeRequestPhase string

const (
	AIChangeRequestPhasePending  AIChangeRequestPhase = "Pending"
	AIChangeRequestPhaseApproved AIChangeRequestPhase = "Approved"
	AIChangeRequestPhaseActuated AIChangeRequestPhase = "Actuated"
	AIChangeRequestPhaseRejected AIChangeRequestPhase = "Rejected"
	AIChangeRequestPhaseExpired  AIChangeRequestPhase = "Expired"
	AIChangeRequestPhaseFailed   AIChangeRequestPhase = "Failed"
)

// GOVARRouteApprovalScope is a bounded policy-level authorization. Its route
// snapshot digest already commits to the model/provider resource versions,
// pricing version, compliance inputs, and provider-owned actuation fields. The
// explicit references make stale UID/generation checks possible without
// trusting the digest alone.
type GOVARRouteApprovalScope struct {
	RoutingPolicy AIWorkloadBindingResolvedReference `json:"routingPolicy"`
	Model         AIWorkloadBindingResolvedReference `json:"model"`
	Provider      AIWorkloadBindingResolvedReference `json:"provider"`

	// RouteSnapshotDigest is the lower-case SHA-256 digest of the exact
	// controller-validated GOVARRouteSnapshot admitted under this scope.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	RouteSnapshotDigest string `json:"routeSnapshotDigest"`

	// ValidUntil is an absolute, reviewer-selected upper bound. An approval is
	// not valid at or after this instant.
	ValidUntil metav1.Time `json:"validUntil"`

	// ScopeDigest is SHA-256 over every other field in this scope. Authors use
	// ComputeDigest to populate it; the controller independently recomputes it.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ScopeDigest string `json:"scopeDigest"`
}

// ComputeDigest returns a canonical digest without trusting ScopeDigest.
func (s GOVARRouteApprovalScope) ComputeDigest() string {
	s.ScopeDigest = ""
	raw, _ := json.Marshal(s)
	digest := sha256.New()
	_, _ = digest.Write([]byte("govar-route-approval-scope-v1\x00"))
	_, _ = digest.Write(raw)
	return hex.EncodeToString(digest.Sum(nil))
}

// GOVARApprovalReviewerGroup is the authenticated Kubernetes group whose
// members may decide a GOV-AR route authorization. Merely granting update RBAC
// is insufficient: the fail-closed admission webhook also verifies this group
// and records the API-server-authenticated subject on the immutable decision.
const GOVARApprovalReviewerGroup = "aiops.imperium.io:gov-ar-approval-reviewers"

// GOVARRouteApprovalDecision is immutable evidence produced by the fail-closed
// AIChangeRequest admission webhook. ReviewerIdentity and ReviewerGroup come
// from AdmissionRequest.UserInfo, not from an untrusted application header.
// AdmissionUID binds a decision to one API-server admission event; RequestUID
// prevents copying it onto another object with the same namespace/name.
type GOVARRouteApprovalDecision struct {
	// Outcome must agree with spec.approval.
	// +kubebuilder:validation:Enum=Approved;Rejected
	Outcome AIChangeRequestApproval `json:"outcome"`

	// ReviewerIdentity is the authenticated Kubernetes username.
	// +kubebuilder:validation:MinLength=1
	ReviewerIdentity string `json:"reviewerIdentity"`

	// ReviewerGroup is the independently administered reviewer group verified
	// by the API server and admission webhook.
	// +kubebuilder:validation:Enum="aiops.imperium.io:gov-ar-approval-reviewers"
	ReviewerGroup string `json:"reviewerGroup"`

	DecidedAt  metav1.Time `json:"decidedAt"`
	ValidUntil metav1.Time `json:"validUntil"`

	// RequestUID is the immutable UID assigned by the Kubernetes API server.
	// +kubebuilder:validation:MinLength=1
	RequestUID string `json:"requestUID"`

	// AdmissionUID is the unique AdmissionReview request UID for this
	// transition. A replay is evaluated under a different UID and fails closed.
	// +kubebuilder:validation:MinLength=1
	AdmissionUID string `json:"admissionUID"`

	// ProposalDigest and ScopeDigest bind the reviewer decision to the exact
	// immutable proposal and route authorization scope.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ProposalDigest string `json:"proposalDigest"`
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ScopeDigest string `json:"scopeDigest"`

	// DecisionDigest is a domain-separated SHA-256 digest over every field
	// above plus object namespace/name and the authenticated requester.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	DecisionDigest string `json:"decisionDigest"`
}

// ComputeDigest returns the canonical decision digest without trusting the
// supplied DecisionDigest. It is tamper evidence; authenticated actor binding
// is supplied by the Kubernetes AdmissionReview UserInfo checks.
func (d GOVARRouteApprovalDecision) ComputeDigest(namespace, name, requestedBy string) string {
	d.DecisionDigest = ""
	raw, _ := json.Marshal(struct {
		Namespace   string                     `json:"namespace"`
		Name        string                     `json:"name"`
		RequestedBy string                     `json:"requestedBy"`
		Decision    GOVARRouteApprovalDecision `json:"decision"`
	}{Namespace: namespace, Name: name, RequestedBy: requestedBy, Decision: d})
	digest := sha256.New()
	_, _ = digest.Write([]byte("govar-route-approval-decision-v1\x00"))
	_, _ = digest.Write(raw)
	return hex.EncodeToString(digest.Sum(nil))
}

// +kubebuilder:validation:XValidation:rule="self.action != 'reroute' || (has(self.sourceModel) && has(self.targetModel))",message="sourceModel and targetModel are required for reroute"
// +kubebuilder:validation:XValidation:rule="self.action != 'authorize-gov-ar-route' || has(self.govarRouteApproval)",message="govarRouteApproval is required for authorize-gov-ar-route"
// +kubebuilder:validation:XValidation:rule="self.action == oldSelf.action",message="action is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.govarRouteApproval) || self.govarRouteApproval == oldSelf.govarRouteApproval",message="govarRouteApproval is immutable; create a new change request for a new scope"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.requestedBy) || self.requestedBy == oldSelf.requestedBy",message="requestedBy is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.proposalDigest) || self.proposalDigest == oldSelf.proposalDigest",message="proposalDigest is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.govarDecision) || self.govarDecision == oldSelf.govarDecision",message="govarDecision is immutable"
// +kubebuilder:validation:XValidation:rule="oldSelf.approval == 'Pending' || self.approval == oldSelf.approval",message="an approval decision is immutable"
// +kubebuilder:validation:XValidation:rule="self.action != 'authorize-gov-ar-route' || self.approval != 'Pending' || !has(self.govarDecision)",message="a Pending GOV-AR authorization cannot contain a decision"
// +kubebuilder:validation:XValidation:rule="self.action != 'authorize-gov-ar-route' || self.approval == 'Pending' || (!has(self.requestedBy) && !has(self.proposalDigest) && !has(self.govarDecision)) || (has(self.requestedBy) && has(self.proposalDigest) && has(self.govarDecision) && self.govarDecision.outcome == self.approval && self.govarDecision.proposalDigest == self.proposalDigest && self.govarDecision.scopeDigest == self.govarRouteApproval.scopeDigest && self.govarDecision.validUntil == self.govarRouteApproval.validUntil && self.govarDecision.reviewerIdentity != self.requestedBy)",message="a non-legacy GOV-AR decision must bind the outcome, proposal, scope, expiry, and a distinct reviewer"
// AIChangeRequestSpec describes a proposed change for human review.
type AIChangeRequestSpec struct {
	// Action is the type of change proposed.
	Action AIChangeRequestAction `json:"action"`

	// SourceModel is the current provider-side model id.
	// +optional
	// +kubebuilder:validation:MinLength=1
	SourceModel string `json:"sourceModel,omitempty"`

	// TargetModel is the proposed replacement model id.
	// +optional
	// +kubebuilder:validation:MinLength=1
	TargetModel string `json:"targetModel,omitempty"`

	// GOVARRouteApproval is present only for authorize-gov-ar-route. One
	// approved object may govern many requests under the exact frozen scope;
	// requests never create or consume Kubernetes objects.
	// +optional
	GOVARRouteApproval *GOVARRouteApprovalScope `json:"govarRouteApproval,omitempty"`

	// RequestedBy is stamped from AdmissionRequest.UserInfo.username when an
	// authorize-gov-ar-route proposal is created. Clients cannot select it.
	// +optional
	// +kubebuilder:validation:MinLength=1
	RequestedBy string `json:"requestedBy,omitempty"`

	// ProposalDigest binds every proposal field, namespace/name, exact route
	// scope, and RequestedBy. The admission webhook computes it on create.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ProposalDigest string `json:"proposalDigest,omitempty"`

	// GOVARDecision is an immutable, API-server-identity-bound reviewer record.
	// It is absent while Pending and is stamped by the admission webhook for the
	// sole Pending -> Approved/Rejected transition.
	// +optional
	GOVARDecision *GOVARRouteApprovalDecision `json:"govarDecision,omitempty"`

	// Reason is a human-readable justification for the change.
	// +optional
	Reason string `json:"reason,omitempty"`

	// ExpectedSavingEUR is the projected saving over the observation window.
	// +optional
	ExpectedSavingEUR string `json:"expectedSavingEUR,omitempty"`

	// QualityScore is the routing score of the target model at proposal time.
	// +optional
	QualityScore string `json:"qualityScore,omitempty"`

	// LatencyImpact is a human-readable estimated latency delta (e.g. "+120ms").
	// +optional
	LatencyImpact string `json:"latencyImpact,omitempty"`

	// RiskLevel is the operator's assessment of the change risk.
	// +kubebuilder:validation:Enum=low;medium;high
	// +optional
	RiskLevel string `json:"riskLevel,omitempty"`

	// ExpiresAfter is how long this request waits for approval before it expires
	// (e.g. "24h"). Defaults to "48h" when not set.
	// +optional
	ExpiresAfter string `json:"expiresAfter,omitempty"`

	// Approval is set by a human reviewer to approve or reject the request.
	// Initially left empty (Pending). The operator acts only when Approved.
	// +kubebuilder:default=Pending
	// +optional
	Approval AIChangeRequestApproval `json:"approval,omitempty"`
}

// ComputeProposalDigest returns a canonical digest over all proposal semantics
// and authenticated requester evidence, excluding only the later approval and
// decision fields.
func (s AIChangeRequestSpec) ComputeProposalDigest(namespace, name string) string {
	raw, _ := json.Marshal(struct {
		Namespace          string                   `json:"namespace"`
		Name               string                   `json:"name"`
		Action             AIChangeRequestAction    `json:"action"`
		SourceModel        string                   `json:"sourceModel,omitempty"`
		TargetModel        string                   `json:"targetModel,omitempty"`
		GOVARRouteApproval *GOVARRouteApprovalScope `json:"govarRouteApproval,omitempty"`
		Reason             string                   `json:"reason,omitempty"`
		ExpectedSavingEUR  string                   `json:"expectedSavingEUR,omitempty"`
		QualityScore       string                   `json:"qualityScore,omitempty"`
		LatencyImpact      string                   `json:"latencyImpact,omitempty"`
		RiskLevel          string                   `json:"riskLevel,omitempty"`
		ExpiresAfter       string                   `json:"expiresAfter,omitempty"`
		RequestedBy        string                   `json:"requestedBy"`
	}{namespace, name, s.Action, s.SourceModel, s.TargetModel, s.GOVARRouteApproval,
		s.Reason, s.ExpectedSavingEUR, s.QualityScore, s.LatencyImpact, s.RiskLevel,
		s.ExpiresAfter, s.RequestedBy})
	digest := sha256.New()
	_, _ = digest.Write([]byte("govar-route-approval-proposal-v1\x00"))
	_, _ = digest.Write(raw)
	return hex.EncodeToString(digest.Sum(nil))
}

// ValidateGOVARDecision verifies the durable decision record that the
// controller and synchronous admission path may trust after webhook admission.
// It deliberately cannot recreate AdmissionRequest.UserInfo; that independent
// authentication check is performed synchronously by the fail-closed webhook.
func (c *AIChangeRequest) ValidateGOVARDecision(now time.Time) error {
	if c == nil || c.Spec.Action != AIChangeRequestActionAuthorizeGOVARRoute || c.Spec.GOVARRouteApproval == nil {
		return fmt.Errorf("GOV-AR route approval scope is absent")
	}
	if strings.TrimSpace(c.Spec.RequestedBy) == "" || c.Spec.ProposalDigest != c.Spec.ComputeProposalDigest(c.Namespace, c.Name) {
		return fmt.Errorf("GOV-AR proposal identity or digest is invalid")
	}
	d := c.Spec.GOVARDecision
	if d == nil || d.Outcome != c.Spec.Approval || (d.Outcome != AIChangeRequestApprovalApproved && d.Outcome != AIChangeRequestApprovalRejected) {
		return fmt.Errorf("GOV-AR decision is absent or inconsistent")
	}
	if strings.TrimSpace(d.ReviewerIdentity) == "" || d.ReviewerIdentity == c.Spec.RequestedBy || d.ReviewerGroup != GOVARApprovalReviewerGroup {
		return fmt.Errorf("GOV-AR requester and authorized reviewer are not distinct")
	}
	if c.UID == "" || d.RequestUID != string(c.UID) || d.AdmissionUID == "" || d.ProposalDigest != c.Spec.ProposalDigest ||
		d.ScopeDigest != c.Spec.GOVARRouteApproval.ScopeDigest || !d.ValidUntil.Equal(&c.Spec.GOVARRouteApproval.ValidUntil) {
		return fmt.Errorf("GOV-AR decision object, proposal, scope, or expiry binding is invalid")
	}
	if d.DecisionDigest != d.ComputeDigest(c.Namespace, c.Name, c.Spec.RequestedBy) {
		return fmt.Errorf("GOV-AR decision digest is invalid")
	}
	now = now.UTC()
	if d.DecidedAt.IsZero() || d.DecidedAt.After(now.Add(5*time.Second)) || !d.DecidedAt.Before(&d.ValidUntil) || !now.Before(d.ValidUntil.Time) {
		return fmt.Errorf("GOV-AR decision time or expiry is invalid")
	}
	return nil
}

// AIChangeRequestStatus reflects the observed state of the change request.
type AIChangeRequestStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase is the current lifecycle state of the request.
	// +optional
	Phase AIChangeRequestPhase `json:"phase,omitempty"`

	// Message is a human-readable description of the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// ActuatedAt is when the operator applied the change.
	// +optional
	ActuatedAt *metav1.Time `json:"actuatedAt,omitempty"`

	// ApprovedAt is when the controller verified a human-approved GOV-AR route scope.
	// +optional
	ApprovedAt *metav1.Time `json:"approvedAt,omitempty"`

	// ApprovedScopeDigest is the controller-recomputed digest of the exact
	// reusable GOV-AR route scope. Admission rejects an absent or stale value.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ApprovedScopeDigest string `json:"approvedScopeDigest,omitempty"`

	// ApprovedDecisionDigest is the controller-recomputed digest of the
	// independently authenticated, immutable reviewer decision.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ApprovedDecisionDigest string `json:"approvedDecisionDigest,omitempty"`

	// ApprovedBy is the API-server-authenticated reviewer recorded in the
	// verified decision. It must differ from spec.requestedBy.
	// +optional
	ApprovedBy string `json:"approvedBy,omitempty"`

	// ExpiresAt is when the request will automatically expire if not acted upon.
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`

	// ActuatedRoutes lists the AIGatewayRoute names modified by this request.
	// +optional
	ActuatedRoutes []string `json:"actuatedRoutes,omitempty"`

	// Conditions represent the latest available observations of the request state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=aicrq
//+kubebuilder:printcolumn:name="Action",type=string,JSONPath=`.spec.action`
//+kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceModel`
//+kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetModel`
//+kubebuilder:printcolumn:name="Approval",type=string,JSONPath=`.spec.approval`
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Risk",type=string,JSONPath=`.spec.riskLevel`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIChangeRequest represents either a proposed model reroute or a bounded
// policy-level GOV-AR route authorization that requires human approval. The
// workflow is:
//
//	Pending → Approved (human sets spec.approval=Approved) → Actuated
//	Pending → Rejected (human sets spec.approval=Rejected)
//	Pending → Expired  (expiresAfter elapsed without decision)
//
// authorize-gov-ar-route ends in Approved without actuation and may be reused
// only inside its exact immutable scope. Every decision is auditable via status
// conditions and Kubernetes Events.
type AIChangeRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIChangeRequestSpec   `json:"spec,omitempty"`
	Status AIChangeRequestStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIChangeRequestList contains a list of AIChangeRequest.
type AIChangeRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIChangeRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIChangeRequest{}, &AIChangeRequestList{})
}
