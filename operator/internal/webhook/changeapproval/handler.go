// Package changeapproval enforces two-person control for reusable GOV-AR
// AIChangeRequest approvals at the Kubernetes authenticated admission boundary.
package changeapproval

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

const (
	group    = "aiops.imperium.io"
	version  = "v1alpha1"
	resource = "aichangerequests"
)

// Matches reports whether a root-resource AdmissionReview concerns an
// AIChangeRequest. Status updates are controller-owned and excluded.
func Matches(req admission.Request) bool {
	return req.Resource.Group == group && req.Resource.Version == version &&
		req.Resource.Resource == resource && req.SubResource == ""
}

// MutationHandler stamps authenticated requester/reviewer evidence. The
// matching webhook configuration uses failurePolicy=Fail.
type MutationHandler struct {
	decoder *admission.Decoder
	Now     func() time.Time
}

func NewMutation(scheme *runtime.Scheme) *MutationHandler {
	return &MutationHandler{decoder: admission.NewDecoder(scheme)}
}

func (h *MutationHandler) now() time.Time {
	if h.Now != nil {
		return h.Now().UTC()
	}
	return time.Now().UTC()
}

func (h *MutationHandler) Handle(_ context.Context, req admission.Request) admission.Response {
	if !Matches(req) {
		return admission.Allowed("not an AIChangeRequest root-resource operation")
	}
	var next aiopsv1alpha1.AIChangeRequest
	if err := h.decoder.Decode(req, &next); err != nil {
		return admission.Errored(400, err)
	}
	username := strings.TrimSpace(req.UserInfo.Username)

	switch req.Operation {
	case admissionv1.Create:
		if next.Spec.Action != aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute {
			return admission.Allowed("legacy non-GOV-AR change workflow is unchanged")
		}
		if username == "" {
			return admission.Denied("GOV-AR change requests require an authenticated Kubernetes username")
		}
		if next.Name == "" || next.Namespace == "" || next.Spec.GOVARRouteApproval == nil {
			return admission.Denied("GOV-AR change request namespace, name, and route scope are required")
		}
		if normalizeApproval(next.Spec.Approval) != aiopsv1alpha1.AIChangeRequestApprovalPending || next.Spec.GOVARDecision != nil {
			return admission.Denied("GOV-AR change requests must be created Pending without a decision")
		}
		if next.Spec.RequestedBy != "" && next.Spec.RequestedBy != username {
			return admission.Denied("requestedBy may only identify the authenticated creator")
		}
		mutated := next.DeepCopy()
		mutated.Spec.RequestedBy = username
		mutated.Spec.Approval = aiopsv1alpha1.AIChangeRequestApprovalPending
		mutated.Spec.ProposalDigest = mutated.Spec.ComputeProposalDigest(mutated.Namespace, mutated.Name)
		return patch(req, mutated)

	case admissionv1.Update:
		var old aiopsv1alpha1.AIChangeRequest
		if len(req.OldObject.Raw) == 0 {
			return admission.Denied("GOV-AR decision update lacks the prior object")
		}
		if err := json.Unmarshal(req.OldObject.Raw, &old); err != nil {
			return admission.Errored(400, fmt.Errorf("decode prior AIChangeRequest: %w", err))
		}
		oldGOVAR := old.Spec.Action == aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute
		newGOVAR := next.Spec.Action == aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute
		if !oldGOVAR && !newGOVAR {
			return admission.Allowed("legacy non-GOV-AR change workflow is unchanged")
		}
		if !oldGOVAR || !newGOVAR {
			return admission.Denied("a GOV-AR change request action is immutable")
		}
		if username == "" {
			return admission.Denied("GOV-AR change requests require an authenticated Kubernetes username")
		}
		if err := validProposal(&old); err != nil {
			return admission.Denied("stored GOV-AR proposal is invalid: " + err.Error())
		}
		if !sameProposal(&old, &next) {
			return admission.Denied("GOV-AR proposal fields are immutable; create a new change request")
		}
		oldApproval := normalizeApproval(old.Spec.Approval)
		newApproval := normalizeApproval(next.Spec.Approval)
		if oldApproval != aiopsv1alpha1.AIChangeRequestApprovalPending {
			if !reflect.DeepEqual(old.Spec, next.Spec) {
				return admission.Denied("GOV-AR decisions are immutable")
			}
			return admission.Allowed("immutable GOV-AR decision unchanged")
		}
		if newApproval == aiopsv1alpha1.AIChangeRequestApprovalPending {
			if next.Spec.GOVARDecision != nil {
				return admission.Denied("a Pending GOV-AR proposal cannot contain a decision")
			}
			return admission.Allowed("GOV-AR proposal remains Pending")
		}
		if newApproval != aiopsv1alpha1.AIChangeRequestApprovalApproved && newApproval != aiopsv1alpha1.AIChangeRequestApprovalRejected {
			return admission.Denied("invalid GOV-AR approval transition")
		}
		if next.Spec.GOVARDecision != nil {
			return admission.Denied("clients cannot supply or replay GOV-AR decision evidence")
		}
		if username == old.Spec.RequestedBy {
			return admission.Denied("GOV-AR self-approval is forbidden")
		}
		if !slices.Contains(req.UserInfo.Groups, aiopsv1alpha1.GOVARApprovalReviewerGroup) {
			return admission.Denied("authenticated user is not in the GOV-AR approval reviewer group")
		}
		if old.UID == "" || req.UID == "" {
			return admission.Denied("GOV-AR decision requires Kubernetes object and AdmissionReview UIDs")
		}
		now := h.now()
		if old.Spec.GOVARRouteApproval == nil || !now.Before(old.Spec.GOVARRouteApproval.ValidUntil.Time) {
			return admission.Denied("GOV-AR approval scope has expired")
		}
		mutated := next.DeepCopy()
		decision := &aiopsv1alpha1.GOVARRouteApprovalDecision{
			Outcome:          newApproval,
			ReviewerIdentity: username,
			ReviewerGroup:    aiopsv1alpha1.GOVARApprovalReviewerGroup,
			DecidedAt:        metav1.NewTime(now),
			ValidUntil:       old.Spec.GOVARRouteApproval.ValidUntil,
			RequestUID:       string(old.UID),
			AdmissionUID:     string(req.UID),
			ProposalDigest:   old.Spec.ProposalDigest,
			ScopeDigest:      old.Spec.GOVARRouteApproval.ScopeDigest,
		}
		decision.DecisionDigest = decision.ComputeDigest(old.Namespace, old.Name, old.Spec.RequestedBy)
		mutated.Spec.GOVARDecision = decision
		return patch(req, mutated)

	default:
		return admission.Allowed("no GOV-AR approval mutation for this operation")
	}
}

// ValidationHandler independently verifies the mutation and authenticated
// actor on the same AdmissionReview before persistence.
type ValidationHandler struct {
	decoder *admission.Decoder
	Now     func() time.Time
}

func NewValidation(scheme *runtime.Scheme) *ValidationHandler {
	return &ValidationHandler{decoder: admission.NewDecoder(scheme)}
}

func (h *ValidationHandler) now() time.Time {
	if h.Now != nil {
		return h.Now().UTC()
	}
	return time.Now().UTC()
}

func (h *ValidationHandler) Handle(_ context.Context, req admission.Request) admission.Response {
	if !Matches(req) {
		return admission.Allowed("not an AIChangeRequest root-resource operation")
	}
	var next aiopsv1alpha1.AIChangeRequest
	if err := h.decoder.Decode(req, &next); err != nil {
		return admission.Errored(400, err)
	}
	username := strings.TrimSpace(req.UserInfo.Username)

	switch req.Operation {
	case admissionv1.Create:
		if next.Spec.Action != aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute {
			return admission.Allowed("legacy non-GOV-AR change workflow is unchanged")
		}
		if username == "" {
			return admission.Denied("GOV-AR change requests require an authenticated Kubernetes username")
		}
		if normalizeApproval(next.Spec.Approval) != aiopsv1alpha1.AIChangeRequestApprovalPending || next.Spec.GOVARDecision != nil {
			return admission.Denied("GOV-AR change requests must be created Pending without a decision")
		}
		if next.Spec.RequestedBy != username {
			return admission.Denied("requestedBy does not match the authenticated creator")
		}
		if err := validProposal(&next); err != nil {
			return admission.Denied("invalid GOV-AR proposal evidence: " + err.Error())
		}
		return admission.Allowed("authenticated GOV-AR requester evidence verified")

	case admissionv1.Update:
		var old aiopsv1alpha1.AIChangeRequest
		if len(req.OldObject.Raw) == 0 {
			return admission.Denied("GOV-AR decision update lacks the prior object")
		}
		if err := json.Unmarshal(req.OldObject.Raw, &old); err != nil {
			return admission.Errored(400, fmt.Errorf("decode prior AIChangeRequest: %w", err))
		}
		oldGOVAR := old.Spec.Action == aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute
		newGOVAR := next.Spec.Action == aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute
		if !oldGOVAR && !newGOVAR {
			return admission.Allowed("legacy non-GOV-AR change workflow is unchanged")
		}
		if !oldGOVAR || !newGOVAR {
			return admission.Denied("a GOV-AR change request action is immutable")
		}
		if username == "" {
			return admission.Denied("GOV-AR change requests require an authenticated Kubernetes username")
		}
		if err := validProposal(&old); err != nil {
			return admission.Denied("stored GOV-AR proposal is invalid: " + err.Error())
		}
		if !sameProposal(&old, &next) {
			return admission.Denied("GOV-AR proposal fields are immutable")
		}
		oldApproval := normalizeApproval(old.Spec.Approval)
		newApproval := normalizeApproval(next.Spec.Approval)
		if oldApproval != aiopsv1alpha1.AIChangeRequestApprovalPending {
			if !reflect.DeepEqual(old.Spec, next.Spec) {
				return admission.Denied("GOV-AR decisions are immutable")
			}
			return admission.Allowed("immutable GOV-AR decision unchanged")
		}
		if newApproval == aiopsv1alpha1.AIChangeRequestApprovalPending {
			if next.Spec.GOVARDecision != nil {
				return admission.Denied("a Pending GOV-AR proposal cannot contain a decision")
			}
			return admission.Allowed("GOV-AR proposal remains Pending")
		}
		if username == old.Spec.RequestedBy || !slices.Contains(req.UserInfo.Groups, aiopsv1alpha1.GOVARApprovalReviewerGroup) {
			return admission.Denied("GOV-AR decision lacks independent authenticated reviewer authority")
		}
		if err := validTransitionDecision(req, &old, &next, h.now()); err != nil {
			return admission.Denied("invalid GOV-AR decision evidence: " + err.Error())
		}
		return admission.Allowed("independent authenticated GOV-AR decision verified")

	default:
		return admission.Allowed("no GOV-AR approval validation for this operation")
	}
}

func normalizeApproval(value aiopsv1alpha1.AIChangeRequestApproval) aiopsv1alpha1.AIChangeRequestApproval {
	if value == "" {
		return aiopsv1alpha1.AIChangeRequestApprovalPending
	}
	return value
}

func validProposal(change *aiopsv1alpha1.AIChangeRequest) error {
	if change == nil || change.Name == "" || change.Namespace == "" || change.Spec.GOVARRouteApproval == nil {
		return fmt.Errorf("object identity or route scope is absent")
	}
	if strings.TrimSpace(change.Spec.RequestedBy) == "" {
		return fmt.Errorf("requester identity is absent")
	}
	scope := change.Spec.GOVARRouteApproval
	if scope.ScopeDigest == "" || scope.ScopeDigest != scope.ComputeDigest() {
		return fmt.Errorf("route scope digest is invalid")
	}
	if change.Spec.ProposalDigest == "" || change.Spec.ProposalDigest != change.Spec.ComputeProposalDigest(change.Namespace, change.Name) {
		return fmt.Errorf("proposal digest is invalid")
	}
	return nil
}

func sameProposal(old, next *aiopsv1alpha1.AIChangeRequest) bool {
	if old == nil || next == nil || old.Namespace != next.Namespace || old.Name != next.Name || old.UID != next.UID {
		return false
	}
	if old.Spec.RequestedBy != next.Spec.RequestedBy || old.Spec.ProposalDigest != next.Spec.ProposalDigest {
		return false
	}
	return old.Spec.ProposalDigest == old.Spec.ComputeProposalDigest(old.Namespace, old.Name) &&
		next.Spec.ProposalDigest == next.Spec.ComputeProposalDigest(next.Namespace, next.Name)
}

func validTransitionDecision(req admission.Request, old, next *aiopsv1alpha1.AIChangeRequest, now time.Time) error {
	d := next.Spec.GOVARDecision
	if d == nil || (next.Spec.Approval != aiopsv1alpha1.AIChangeRequestApprovalApproved && next.Spec.Approval != aiopsv1alpha1.AIChangeRequestApprovalRejected) || d.Outcome != next.Spec.Approval {
		return fmt.Errorf("outcome is absent or inconsistent")
	}
	if d.ReviewerIdentity != req.UserInfo.Username || d.ReviewerIdentity == old.Spec.RequestedBy || d.ReviewerGroup != aiopsv1alpha1.GOVARApprovalReviewerGroup {
		return fmt.Errorf("reviewer identity or group is invalid")
	}
	if d.RequestUID != string(old.UID) || d.AdmissionUID != string(req.UID) || d.ProposalDigest != old.Spec.ProposalDigest ||
		d.ScopeDigest != old.Spec.GOVARRouteApproval.ScopeDigest || !d.ValidUntil.Equal(&old.Spec.GOVARRouteApproval.ValidUntil) {
		return fmt.Errorf("request, admission, proposal, scope, or expiry binding is invalid")
	}
	if d.DecisionDigest != d.ComputeDigest(old.Namespace, old.Name, old.Spec.RequestedBy) {
		return fmt.Errorf("decision digest is invalid")
	}
	now = now.UTC()
	staleCutoff := metav1.NewTime(now.Add(-30 * time.Second))
	if d.DecidedAt.IsZero() || d.DecidedAt.Before(&staleCutoff) || d.DecidedAt.After(now.Add(5*time.Second)) ||
		!d.DecidedAt.Before(&d.ValidUntil) || !now.Before(d.ValidUntil.Time) {
		return fmt.Errorf("decision time is stale, future, or expired")
	}
	return nil
}

func patch(req admission.Request, object *aiopsv1alpha1.AIChangeRequest) admission.Response {
	raw, err := json.Marshal(object)
	if err != nil {
		return admission.Errored(500, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}
