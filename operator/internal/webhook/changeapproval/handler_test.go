package changeapproval

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

func TestAuthenticatedTwoPersonGOVARDecisionLifecycle(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	scheme := approvalScheme(t)
	mutator := NewMutation(scheme)
	mutator.Now = func() time.Time { return now }
	validator := NewValidation(scheme)
	validator.Now = func() time.Time { return now }

	pending := approvalFixture(now)
	create := approvalRequest(t, admissionv1.Create, "create-admission", "alice", nil, pending)
	created := applyMutation(t, create, mutator.Handle(context.Background(), create))
	if created.Spec.RequestedBy != "alice" || created.Spec.ProposalDigest != created.Spec.ComputeProposalDigest(created.Namespace, created.Name) ||
		created.Spec.Approval != aiopsv1alpha1.AIChangeRequestApprovalPending || created.Spec.GOVARDecision != nil {
		t.Fatalf("creation evidence not stamped exactly: %+v", created.Spec)
	}
	create.Object.Raw = mustJSON(t, created)
	assertAllowed(t, validator.Handle(context.Background(), create))

	created.UID = "change-uid"
	created.Generation = 1
	approved := created.DeepCopy()
	approved.Spec.Approval = aiopsv1alpha1.AIChangeRequestApprovalApproved
	update := approvalRequest(t, admissionv1.Update, "decision-admission", "bob", created, approved)
	update.UserInfo.Groups = []string{"system:authenticated", aiopsv1alpha1.GOVARApprovalReviewerGroup}
	decided := applyMutation(t, update, mutator.Handle(context.Background(), update))
	update.Object.Raw = mustJSON(t, decided)
	assertAllowed(t, validator.Handle(context.Background(), update))

	d := decided.Spec.GOVARDecision
	if d == nil || d.ReviewerIdentity != "bob" || d.ReviewerGroup != aiopsv1alpha1.GOVARApprovalReviewerGroup ||
		d.RequestUID != string(created.UID) || d.AdmissionUID != "decision-admission" || d.ProposalDigest != created.Spec.ProposalDigest ||
		d.ScopeDigest != created.Spec.GOVARRouteApproval.ScopeDigest || d.DecisionDigest != d.ComputeDigest(decided.Namespace, decided.Name, "alice") {
		t.Fatalf("decision evidence not bound exactly: %+v", d)
	}
	if err := decided.ValidateGOVARDecision(now); err != nil {
		t.Fatalf("durable controller validation failed: %v", err)
	}
}

func TestGOVARDecisionAdmissionFailsClosed(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	scheme := approvalScheme(t)
	mutator := NewMutation(scheme)
	mutator.Now = func() time.Time { return now }
	validator := NewValidation(scheme)
	validator.Now = func() time.Time { return now }
	pending := stampedPending(t, mutator, now)

	tests := []struct {
		name     string
		username string
		groups   []string
		mutate   func(*aiopsv1alpha1.AIChangeRequest)
		contains string
	}{
		{name: "self approval", username: "alice", groups: []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}, contains: "self-approval"},
		{name: "reviewer group absent", username: "mallory", contains: "reviewer group"},
		{name: "action downgrade bypass", username: "bob", groups: []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}, mutate: func(v *aiopsv1alpha1.AIChangeRequest) { v.Spec.Action = aiopsv1alpha1.AIChangeRequestActionReroute }, contains: "action is immutable"},
		{name: "full proposal patch", username: "bob", groups: []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}, mutate: func(v *aiopsv1alpha1.AIChangeRequest) { v.Spec.Reason = "escalated after proposal" }, contains: "proposal fields are immutable"},
		{name: "scope tamper with recomputed untrusted proposal", username: "bob", groups: []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}, mutate: func(v *aiopsv1alpha1.AIChangeRequest) {
			v.Spec.GOVARRouteApproval.RouteSnapshotDigest = strings.Repeat("b", 64)
			v.Spec.GOVARRouteApproval.ScopeDigest = v.Spec.GOVARRouteApproval.ComputeDigest()
			v.Spec.ProposalDigest = v.Spec.ComputeProposalDigest(v.Namespace, v.Name)
		}, contains: "proposal fields are immutable"},
		{name: "expired scope", username: "bob", groups: []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}, mutate: func(v *aiopsv1alpha1.AIChangeRequest) {
			v.Spec.GOVARRouteApproval.ValidUntil = metav1.NewTime(now.Add(-time.Second))
			v.Spec.GOVARRouteApproval.ScopeDigest = v.Spec.GOVARRouteApproval.ComputeDigest()
			v.Spec.ProposalDigest = v.Spec.ComputeProposalDigest(v.Namespace, v.Name)
		}, contains: "proposal fields are immutable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next := pending.DeepCopy()
			next.Spec.Approval = aiopsv1alpha1.AIChangeRequestApprovalApproved
			if test.mutate != nil {
				test.mutate(next)
			}
			req := approvalRequest(t, admissionv1.Update, "decision-"+strings.ReplaceAll(test.name, " ", "-"), test.username, pending, next)
			req.UserInfo.Groups = test.groups
			assertDeniedContains(t, mutator.Handle(context.Background(), req), test.contains)
		})
	}

	// A client replaying a previously stamped decision is rejected instead of
	// being silently re-bound to a new AdmissionReview UID.
	approved := pending.DeepCopy()
	approved.Spec.Approval = aiopsv1alpha1.AIChangeRequestApprovalApproved
	first := approvalRequest(t, admissionv1.Update, "first-admission", "bob", pending, approved)
	first.UserInfo.Groups = []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}
	decided := applyMutation(t, first, mutator.Handle(context.Background(), first))
	replay := approvalRequest(t, admissionv1.Update, "replay-admission", "bob", pending, decided)
	replay.UserInfo.Groups = []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}
	assertDeniedContains(t, mutator.Handle(context.Background(), replay), "cannot supply or replay")

	// Validator catches tampering even if a mutation webhook were bypassed.
	tampered := decided.DeepCopy()
	tampered.Spec.GOVARDecision.ReviewerIdentity = "carol"
	validate := approvalRequest(t, admissionv1.Update, "first-admission", "bob", pending, tampered)
	validate.UserInfo.Groups = []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}
	assertDeniedContains(t, validator.Handle(context.Background(), validate), "reviewer identity")
}

func TestGOVARDecisionCannotBeCopiedToAnotherObjectOrChangedAfterDecision(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	scheme := approvalScheme(t)
	mutator := NewMutation(scheme)
	mutator.Now = func() time.Time { return now }
	validator := NewValidation(scheme)
	validator.Now = func() time.Time { return now }
	pending := stampedPending(t, mutator, now)
	next := pending.DeepCopy()
	next.Spec.Approval = aiopsv1alpha1.AIChangeRequestApprovalApproved
	req := approvalRequest(t, admissionv1.Update, "decision", "bob", pending, next)
	req.UserInfo.Groups = []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}
	decided := applyMutation(t, req, mutator.Handle(context.Background(), req))

	copied := decided.DeepCopy()
	copied.Name = "copied-name"
	copied.UID = "copied-uid"
	if err := copied.ValidateGOVARDecision(now); err == nil {
		t.Fatal("decision replayed onto another object")
	}

	changed := decided.DeepCopy()
	changed.Spec.Approval = aiopsv1alpha1.AIChangeRequestApprovalRejected
	post := approvalRequest(t, admissionv1.Update, "post-decision", "bob", decided, changed)
	post.UserInfo.Groups = []string{aiopsv1alpha1.GOVARApprovalReviewerGroup}
	assertDeniedContains(t, validator.Handle(context.Background(), post), "decisions are immutable")
}

func stampedPending(t *testing.T, mutator *MutationHandler, now time.Time) *aiopsv1alpha1.AIChangeRequest {
	t.Helper()
	change := approvalFixture(now)
	req := approvalRequest(t, admissionv1.Create, "create", "alice", nil, change)
	change = applyMutation(t, req, mutator.Handle(context.Background(), req))
	change.UID = "change-uid"
	change.Generation = 1
	return change
}

func approvalFixture(now time.Time) *aiopsv1alpha1.AIChangeRequest {
	scope := aiopsv1alpha1.GOVARRouteApprovalScope{
		RoutingPolicy:       aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: "routing", UID: "routing-uid", Generation: 3},
		Model:               aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: "model", UID: "model-uid", Generation: 4},
		Provider:            aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: "provider", UID: "provider-uid", Generation: 5},
		RouteSnapshotDigest: strings.Repeat("a", 64),
		ValidUntil:          metav1.NewTime(now.Add(time.Hour)),
	}
	scope.ScopeDigest = scope.ComputeDigest()
	return &aiopsv1alpha1.AIChangeRequest{
		TypeMeta:   metav1.TypeMeta{APIVersion: "aiops.imperium.io/v1alpha1", Kind: "AIChangeRequest"},
		ObjectMeta: metav1.ObjectMeta{Name: "approve-route", Namespace: "finance", CreationTimestamp: metav1.NewTime(now.Add(-time.Minute))},
		Spec:       aiopsv1alpha1.AIChangeRequestSpec{Action: aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute, Approval: aiopsv1alpha1.AIChangeRequestApprovalPending, GOVARRouteApproval: &scope, Reason: "bounded production route"},
	}
}

func approvalRequest(t *testing.T, operation admissionv1.Operation, uid, username string, old, next *aiopsv1alpha1.AIChangeRequest) admission.Request {
	t.Helper()
	raw := mustJSON(t, next)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		UID: types.UID(uid), Operation: operation,
		Resource:  metav1.GroupVersionResource{Group: group, Version: version, Resource: resource},
		Namespace: next.Namespace, Name: next.Name, Object: runtime.RawExtension{Raw: raw},
		UserInfo: authenticationv1.UserInfo{Username: username},
	}}
	if old != nil {
		req.OldObject.Raw = mustJSON(t, old)
	}
	return req
}

func applyMutation(t *testing.T, req admission.Request, response admission.Response) *aiopsv1alpha1.AIChangeRequest {
	t.Helper()
	assertAllowed(t, response)
	patchRaw, err := json.Marshal(response.Patches)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := jsonpatch.DecodePatch(patchRaw)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := decoded.Apply(req.Object.Raw)
	if err != nil {
		t.Fatal(err)
	}
	var out aiopsv1alpha1.AIChangeRequest
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func assertAllowed(t *testing.T, response admission.Response) {
	t.Helper()
	if !response.Allowed {
		t.Fatalf("admission denied: %+v", response.Result)
	}
}

func assertDeniedContains(t *testing.T, response admission.Response, want string) {
	t.Helper()
	if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, want) {
		t.Fatalf("response allowed=%v message=%q, want denial containing %q", response.Allowed, response.Result.Message, want)
	}
}

func approvalScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := aiopsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
