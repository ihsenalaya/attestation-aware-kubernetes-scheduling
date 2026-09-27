package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/webhook/podinjector"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type tokenReviewClient struct {
	client.Client
	status authenticationv1.TokenReviewStatus
	spec   authenticationv1.TokenReviewSpec
}

func TestTransitionResponseReturnsSelectedFeedbackDispatchOnlyWhenAuthorized(t *testing.T) {
	plain := transitionResponse(govar.Reservation{RequestID: "plain"}, govar.ReasonDispatchDelivered)
	if _, exists := plain["selected_feedback_dispatch_id"]; exists {
		t.Fatal("unbound transition exposed a selected-feedback dispatch identity")
	}
	const dispatchID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bound := transitionResponse(govar.Reservation{RequestID: "bound", SelectedFeedbackDispatchID: dispatchID}, govar.ReasonDispatchDelivered)
	if bound["selected_feedback_dispatch_id"] != dispatchID {
		t.Fatalf("selected-feedback dispatch identity=%v", bound["selected_feedback_dispatch_id"])
	}
}

func TestSelectedFeedbackAuthorityEnvironmentIsAllOrNothing(t *testing.T) {
	for _, name := range []string{"GOV_AR_SELECTED_FEEDBACK_RUN_ID", "GOV_AR_SELECTED_FEEDBACK_SPLIT", "GOV_AR_SELECTED_FEEDBACK_PROTOCOL_SHA256", "GOV_AR_SELECTED_FEEDBACK_ORACLE_SHA256", "GOV_AR_SELECTED_FEEDBACK_CONFIG_SHA256", "GOV_AR_SELECTED_FEEDBACK_MODEL_MAP_SHA256", "GOV_AR_SELECTED_FEEDBACK_MODEL_MAP_JSON"} {
		t.Setenv(name, "")
	}
	if authority, err := selectedFeedbackAuthorityFromEnvironment(strings.Repeat("a", 64)); err != nil || authority != nil {
		t.Fatalf("unconfigured authority=(%+v,%v)", authority, err)
	}
	t.Setenv("GOV_AR_SELECTED_FEEDBACK_RUN_ID", "article3-run")
	if _, err := selectedFeedbackAuthorityFromEnvironment(strings.Repeat("a", 64)); err == nil {
		t.Fatal("partial selected-feedback authority configuration was accepted")
	}
	t.Setenv("GOV_AR_SELECTED_FEEDBACK_SPLIT", "development")
	t.Setenv("GOV_AR_SELECTED_FEEDBACK_PROTOCOL_SHA256", strings.Repeat("b", 64))
	t.Setenv("GOV_AR_SELECTED_FEEDBACK_ORACLE_SHA256", strings.Repeat("c", 64))
	t.Setenv("GOV_AR_SELECTED_FEEDBACK_CONFIG_SHA256", strings.Repeat("d", 64))
	modelID := strings.Repeat("e", 64)
	modelMapJSON := `{"gpt-fr":"` + modelID + `"}`
	modelMapPayload := `{"schema":"govar-selected-feedback-model-map-v1","models":` + modelMapJSON + `}`
	modelMapSHA := sha256.Sum256([]byte(modelMapPayload))
	t.Setenv("GOV_AR_SELECTED_FEEDBACK_MODEL_MAP_JSON", modelMapJSON)
	t.Setenv("GOV_AR_SELECTED_FEEDBACK_MODEL_MAP_SHA256", fmt.Sprintf("%x", modelMapSHA))
	authority, err := selectedFeedbackAuthorityFromEnvironment(strings.Repeat("a", 64))
	if err != nil || authority == nil || authority.RunID != "article3-run" || authority.DatasetID != "routereval_math_outcomes" || authority.SoftwareSHA256 != strings.Repeat("a", 64) || authority.SelectedModelIDs["gpt-fr"] != modelID {
		t.Fatalf("configured authority=(%+v,%v)", authority, err)
	}
}

func (c *tokenReviewClient) Create(ctx context.Context, object client.Object, opts ...client.CreateOption) error {
	if review, ok := object.(*authenticationv1.TokenReview); ok {
		c.spec = review.Spec
		review.Status = c.status
		return nil
	}
	return c.Client.Create(ctx, object, opts...)
}

func validTokenReviewStatus() authenticationv1.TokenReviewStatus {
	return authenticationv1.TokenReviewStatus{Authenticated: true, Audiences: []string{podinjector.GOVARTokenAudience}, User: authenticationv1.UserInfo{
		Username: "system:serviceaccount:finance:worker",
		Extra: map[string]authenticationv1.ExtraValue{
			"authentication.kubernetes.io/pod-name": {"agent"},
			"authentication.kubernetes.io/pod-uid":  {"uid-a"},
		},
	}}
}

func TestTokenReviewRequiresPodBindingAndDedicatedAudience(t *testing.T) {
	base := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	tests := []struct {
		name   string
		mutate func(*authenticationv1.TokenReviewStatus)
		ok     bool
	}{
		{name: "valid", ok: true},
		{name: "unauthenticated", mutate: func(s *authenticationv1.TokenReviewStatus) { s.Authenticated = false }},
		{name: "wrong audience", mutate: func(s *authenticationv1.TokenReviewStatus) { s.Audiences = []string{"kubernetes.default.svc"} }},
		{name: "missing pod uid", mutate: func(s *authenticationv1.TokenReviewStatus) {
			delete(s.User.Extra, "authentication.kubernetes.io/pod-uid")
		}},
		{name: "missing pod name", mutate: func(s *authenticationv1.TokenReviewStatus) {
			delete(s.User.Extra, "authentication.kubernetes.io/pod-name")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := validTokenReviewStatus()
			if test.mutate != nil {
				test.mutate(&status)
			}
			reviewer := &tokenReviewClient{Client: base, status: status}
			principal, err := tokenReviewFunc(reviewer)(context.Background(), "bound-token")
			if (err == nil) != test.ok {
				t.Fatalf("principal=%+v err=%v", principal, err)
			}
			if len(reviewer.spec.Audiences) != 1 || reviewer.spec.Audiences[0] != podinjector.GOVARTokenAudience {
				t.Fatalf("TokenReview audience=%v", reviewer.spec.Audiences)
			}
		})
	}
}

type countingBackend struct {
	*govar.Engine
	dispatchCalls int
	settleCalls   int
}

func (b *countingBackend) Dispatch(req govar.DispatchRequest) (govar.Reservation, govar.ReasonCode, error) {
	b.dispatchCalls++
	return b.Engine.Dispatch(req)
}

func (b *countingBackend) Settle(req govar.SettleRequest) (govar.Reservation, govar.ReasonCode, error) {
	b.settleCalls++
	return b.Engine.Settle(req)
}

func TestWorkloadTokenCannotForgeZeroUsageFinalSettlement(t *testing.T) {
	backend := &countingBackend{Engine: govar.NewEngine()}
	srv := &server{engine: backend, auth: identityAuthenticator{reviewToken: func(context.Context, string) (authenticatedPrincipal, error) {
		return authenticatedPrincipal{namespace: "finance", workloadUID: "uid-a", podName: "agent", serviceAccount: "worker", role: roleAdmissionOnly}, nil
	}}}
	body := []byte(`{"request_id":"r1","settlement_id":"forged-final","tenant_id":"tenant-a","workload_uid":"uid-a","provider_attempt_id":"r1:attempt:1","actual_input_tokens":0,"actual_output_tokens":0,"usage_version":1,"final":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/settle", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer valid-bound-token")
	recorder := httptest.NewRecorder()
	srv.handleSettle(recorder, req)
	if recorder.Code != http.StatusForbidden || backend.settleCalls != 0 {
		t.Fatalf("status=%d settle_calls=%d body=%s", recorder.Code, backend.settleCalls, recorder.Body.String())
	}
	liability, err := backend.LiabilityWithError("tenant-a")
	if err != nil || liability.SettledSpendMicros != 0 || liability.OutstandingLiabilityMicros != 0 {
		t.Fatalf("ledger changed after rejected workload settlement: liability=%+v err=%v", liability, err)
	}
}

func TestAuthenticatedBodyOverOneMiBIsRejectedExplicitly(t *testing.T) {
	srv := &server{engine: govar.NewEngine(), auth: identityAuthenticator{reviewToken: func(context.Context, string) (authenticatedPrincipal, error) {
		return authenticatedPrincipal{role: roleAdmissionOnly}, nil
	}}}
	req := httptest.NewRequest(http.MethodPost, "/v1/admit", bytes.NewReader(bytes.Repeat([]byte("x"), int(maxAuthenticatedBodyBytes)+1)))
	req.Header.Set("Authorization", "Bearer token")
	recorder := httptest.NewRecorder()
	srv.handleAdmit(recorder, req)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestBoundPodCannotMutateAnotherTenantWithForgedHeaderOrBody(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "finance", UID: types.UID("uid-a"),
		Annotations: map[string]string{podinjector.GOVARTenantKey: "tenant-a", podinjector.GOVARBudgetPolicyKey: "budget", podinjector.GOVARRoutingKey: "routing"}}}
	backend := &countingBackend{Engine: govar.NewEngine()}
	srv := &server{k8s: fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build(), engine: backend,
		auth: identityAuthenticator{reviewToken: func(context.Context, string) (authenticatedPrincipal, error) {
			return authenticatedPrincipal{namespace: "finance", workloadUID: "uid-a", podName: "agent", serviceAccount: "worker", role: roleAdmissionOnly}, nil
		}}}
	body := []byte(`{"request_id":"r1","event_id":"e1","tenant_id":"victim","workload_uid":"uid-a","provider_attempt_id":"a1","status":"CLAIMED"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dispatch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer valid-bound-token")
	req.Header.Set("X-GOVAR-Tenant-ID", "victim")
	recorder := httptest.NewRecorder()
	srv.handleDispatch(recorder, req)
	if recorder.Code != http.StatusForbidden || backend.dispatchCalls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", recorder.Code, backend.dispatchCalls, recorder.Body.String())
	}
}

func TestDeletedBoundPodIsRejected(t *testing.T) {
	srv := &server{k8s: fakeclient.NewClientBuilder().WithScheme(scheme).Build()}
	_, err := srv.resolveTrustedWorkload(context.Background(), authenticatedPrincipal{namespace: "finance", workloadUID: "uid-a", podName: "deleted"})
	if err == nil {
		t.Fatal("deleted bound Pod was accepted")
	}
}

func TestIdentityAuthenticatorAcceptsValidBoundBody(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	auth := identityAuthenticator{masterSecret: []byte("0123456789abcdef0123456789abcdef"), now: func() time.Time { return now }}
	body := []byte(`{"tenant_id":"tenant-a","workload_uid":"uid-a"}`)
	req := signedRequest(t, deriveSignerKey(auth.masterSecret, "finance", "tenant-a", "uid-a"), now, "/v1/admit", "tenant-a", "uid-a", body)
	principal, gotBody, err := auth.authenticate(req)
	if err != nil {
		t.Fatal(err)
	}
	if principal.tenantID != "tenant-a" || principal.workloadUID != "uid-a" || !bytes.Equal(gotBody, body) {
		t.Fatalf("principal=%+v body=%q", principal, gotBody)
	}
}

type failingReadyBackend struct{ *govar.Engine }

func (f failingReadyBackend) Ready(context.Context) error { return errors.New("database unavailable") }

type failingWorkerHealth struct{}

func (failingWorkerHealth) Healthy() error { return errors.New("audit worker stale") }

func TestReadyzReportsLedgerFailure(t *testing.T) {
	srv := &server{engine: failingReadyBackend{Engine: govar.NewEngine()}}
	recorder := httptest.NewRecorder()
	srv.handleReadyz(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable || !bytes.Contains(recorder.Body.Bytes(), []byte("ledger not ready")) {
		t.Fatalf("readyz=%d %s", recorder.Code, recorder.Body.String())
	}
}

func TestReadyzReportsDurableWorkerFailure(t *testing.T) {
	srv := &server{engine: govar.NewEngine(), workers: failingWorkerHealth{}}
	recorder := httptest.NewRecorder()
	srv.handleReadyz(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable || !bytes.Contains(recorder.Body.Bytes(), []byte("durable workers not ready")) {
		t.Fatalf("readyz=%d %s", recorder.Code, recorder.Body.String())
	}
}

func TestIdentityAuthenticatorRejectsBodyTamperingAndReplay(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	auth := identityAuthenticator{masterSecret: []byte("0123456789abcdef0123456789abcdef"), now: func() time.Time { return now }}
	signer := deriveSignerKey(auth.masterSecret, "finance", "tenant-a", "uid-a")
	req := signedRequest(t, signer, now, "/v1/settle", "tenant-a", "uid-a", []byte(`{"request_id":"r1"}`))
	req.Body = http.NoBody
	if _, _, err := auth.authenticate(req); err == nil {
		t.Fatal("tampered body was accepted")
	}
	old := signedRequest(t, signer, now.Add(-3*time.Minute), "/v1/admit", "tenant-a", "uid-a", nil)
	if _, _, err := auth.authenticate(old); err == nil {
		t.Fatal("replayed signature was accepted")
	}
}

func TestDerivedSignerCannotImpersonateAnotherPrincipal(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	master := []byte("0123456789abcdef0123456789abcdef")
	auth := identityAuthenticator{masterSecret: master, now: func() time.Time { return now }}
	tenantASigner := deriveSignerKey(master, "finance", "tenant-a", "uid-a")
	req := signedRequest(t, tenantASigner, now, "/v1/admit", "tenant-b", "uid-b", nil)
	if _, _, err := auth.authenticate(req); err == nil {
		t.Fatal("per-principal signer impersonated another principal")
	}
}

func TestPublicGatewayCannotClaimAuthoritativeUnbilled(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	master := []byte("0123456789abcdef0123456789abcdef")
	srv := &server{engine: govar.NewEngine(), auth: identityAuthenticator{masterSecret: master, now: func() time.Time { return now }}}
	body := []byte(`{"request_id":"r1","event_id":"c1","tenant_id":"tenant-a","workload_uid":"uid-a","authoritative_unbilled":true}`)
	req := signedRequest(t, deriveSignerKey(master, "finance", "tenant-a", "uid-a"), now, "/v1/cancel", "tenant-a", "uid-a", body)
	recorder := httptest.NewRecorder()
	srv.handleCancel(recorder, req)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestPublicGatewayCannotPostArbitrarySettlementCost(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	master := []byte("0123456789abcdef0123456789abcdef")
	srv := &server{engine: govar.NewEngine(), auth: identityAuthenticator{masterSecret: master, now: func() time.Time { return now }}}
	body := []byte(`{"request_id":"r1","settlement_id":"s1","tenant_id":"tenant-a","workload_uid":"uid-a","actual_cost_micros":99,"usage_version":1,"final":true}`)
	req := signedRequest(t, deriveSignerKey(master, "finance", "tenant-a", "uid-a"), now, "/v1/settle", "tenant-a", "uid-a", body)
	recorder := httptest.NewRecorder()
	srv.handleSettle(recorder, req)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestResolveTrustedWorkloadUsesActualPodUIDAndMetadata(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "finance", UID: types.UID("uid-a"), ResourceVersion: "7",
		Labels: map[string]string{"aiops.imperium.io/team": "treasury"}, Annotations: map[string]string{
			"aiops.imperium.io/govar-tenant": "tenant-a", "aiops.imperium.io/govar-budget-policy": "budget",
			"aiops.imperium.io/govar-routing-policy": "routing", "aiops.imperium.io/govar-sensitive-data": "true",
			"aiops.imperium.io/govar-allowed-zones": "eu,francecentral"}}, Spec: corev1.PodSpec{ServiceAccountName: "worker"}}
	ready := []metav1.Condition{{Type: aiopsv1alpha1.ConditionReady, Status: metav1.ConditionTrue}}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "finance", UID: types.UID("sa-uid")}}
	budget := &aiopsv1alpha1.AIBudgetPolicy{ObjectMeta: metav1.ObjectMeta{Name: "budget", Namespace: "finance", UID: types.UID("budget-uid"), Generation: 2}, Status: aiopsv1alpha1.AIBudgetPolicyStatus{ObservedGeneration: 2, Conditions: ready}}
	routing := &aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "routing", Namespace: "finance", UID: types.UID("routing-uid"), Generation: 3}, Status: aiopsv1alpha1.AIRoutingPolicyStatus{ObservedGeneration: 3, Conditions: ready}}
	binding := &aiopsv1alpha1.AIWorkloadBinding{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "finance", UID: types.UID("binding-uid"), Generation: 1, ResourceVersion: "8"},
		Spec:   aiopsv1alpha1.AIWorkloadBindingSpec{ServiceAccountName: "worker", TenantID: "tenant-a", Team: "treasury", Application: "assistant", BudgetPolicyRef: "budget", RoutingPolicyRef: "routing", Sensitivity: aiopsv1alpha1.TierHigh, AllowedZones: []string{"eu", "francecentral"}, RequireGateway: true},
		Status: aiopsv1alpha1.AIWorkloadBindingStatus{ObservedGeneration: 1, ResolvedServiceAccountUID: "sa-uid", ResolvedBudgetPolicy: &aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: "budget", UID: "budget-uid", Generation: 2}, ResolvedRoutingPolicy: &aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: "routing", UID: "routing-uid", Generation: 3}, Conditions: ready}}
	srv := &server{k8s: fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(pod, sa, budget, routing, binding).Build()}
	trusted, err := srv.resolveTrustedWorkload(context.Background(), authenticatedPrincipal{namespace: "finance", tenantID: "tenant-a", workloadUID: "uid-a", serviceAccount: "worker"})
	if err != nil || trusted.resourceVersion != "7|binding-uid|1|8" || trusted.team != "treasury" || !trusted.sensitive {
		t.Fatalf("trusted=%+v err=%v", trusted, err)
	}
	req := govar.AdmitRequest{Namespace: "finance", TenantID: "tenant-a", WorkloadUID: "uid-a", Team: "treasury", Application: "assistant", BudgetPolicyName: "budget", RoutingPolicyName: "routing", SensitiveData: true, AllowedZones: []string{"francecentral", "eu"}}
	if err := bindTrustedAdmitRequest(&req, trusted); err != nil {
		t.Fatal(err)
	}
	req.SensitiveData = false
	if err := bindTrustedAdmitRequest(&req, trusted); err == nil {
		t.Fatal("caller relaxed trusted sensitivity")
	}
	binding.Spec.RequireGateway = false
	if err := srv.k8s.Update(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.resolveTrustedWorkload(context.Background(), authenticatedPrincipal{namespace: "finance", workloadUID: "uid-a", serviceAccount: "worker"}); err == nil {
		t.Fatal("binding with requireGateway=false entered synchronous GOV-AR enforcement")
	}
	binding.Spec.RequireGateway = true
	if err := srv.k8s.Update(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	pod.Annotations[podinjector.GOVARTenantKey] = "victim"
	if err := srv.k8s.Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.resolveTrustedWorkload(context.Background(), authenticatedPrincipal{namespace: "finance", workloadUID: "uid-a", serviceAccount: "worker"}); err == nil {
		t.Fatal("cross-tenant Pod annotation was accepted over AIWorkloadBinding")
	}
}

func TestDecodeStrictJSONRejectsUnknownAndMultipleValues(t *testing.T) {
	var dst struct {
		Known string `json:"known"`
	}
	if err := decodeStrictJSON([]byte(`{"known":"yes","unknown":1}`), &dst); err == nil {
		t.Fatal("unknown field accepted")
	}
	if err := decodeStrictJSON([]byte(`{"known":"yes"} {"known":"twice"}`), &dst); err == nil {
		t.Fatal("multiple JSON values accepted")
	}
}

func TestPolicyLevelGOVARRouteApprovalIsReusableWithoutKubernetesWrites(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	routing := aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "routing", Namespace: "finance", UID: "routing-uid", Generation: 3, ResourceVersion: "routing-rv"}}
	candidate := approvalTestCandidate()
	approval := approvedGOVARRouteChange(now, routing, candidate)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(approval).Build()
	srv := &server{k8s: c, auth: identityAuthenticator{now: func() time.Time { return now }}}
	for i, ref := range []string{approval.Name, ""} {
		got, used, err := srv.resolveGOVARRouteApproval(context.Background(), ref, routing, []govar.Candidate{candidate})
		if err != nil || used.UID != approval.UID || got.SnapshotVersion != candidate.SnapshotVersion {
			t.Fatalf("reuse %d failed: candidate=%+v approval=%+v err=%v", i, got, used, err)
		}
	}
	var changes aiopsv1alpha1.AIChangeRequestList
	if err := c.List(context.Background(), &changes); err != nil || len(changes.Items) != 1 {
		t.Fatalf("approval resolution mutated Kubernetes objects: count=%d err=%v", len(changes.Items), err)
	}
}

func TestPolicyLevelGOVARRouteApprovalRejectsMissingStaleAlteredAndExpired(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	routing := aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "routing", Namespace: "finance", UID: "routing-uid", Generation: 3, ResourceVersion: "routing-rv"}}
	candidate := approvalTestCandidate()
	base := approvedGOVARRouteChange(now, routing, candidate)
	tests := []struct {
		name       string
		object     *aiopsv1alpha1.AIChangeRequest
		candidate  govar.Candidate
		routing    aiopsv1alpha1.AIRoutingPolicy
		wantExpiry bool
	}{
		{name: "missing"},
		{name: "stale status", object: func() *aiopsv1alpha1.AIChangeRequest {
			v := base.DeepCopy()
			v.Status.ObservedGeneration = 0
			return v
		}()},
		{name: "tampered approved decision digest", object: func() *aiopsv1alpha1.AIChangeRequest {
			v := base.DeepCopy()
			v.Status.ApprovedDecisionDigest = strings.Repeat("0", 64)
			return v
		}()},
		{name: "self approval with internally consistent digest", object: func() *aiopsv1alpha1.AIChangeRequest {
			v := base.DeepCopy()
			v.Spec.GOVARDecision.ReviewerIdentity = v.Spec.RequestedBy
			v.Spec.GOVARDecision.DecisionDigest = v.Spec.GOVARDecision.ComputeDigest(v.Namespace, v.Name, v.Spec.RequestedBy)
			v.Status.ApprovedDecisionDigest = v.Spec.GOVARDecision.DecisionDigest
			v.Status.ApprovedBy = v.Spec.RequestedBy
			return v
		}()},
		{name: "decision copied from another object UID", object: func() *aiopsv1alpha1.AIChangeRequest {
			v := base.DeepCopy()
			v.Spec.GOVARDecision.RequestUID = "different-object-uid"
			v.Spec.GOVARDecision.DecisionDigest = v.Spec.GOVARDecision.ComputeDigest(v.Namespace, v.Name, v.Spec.RequestedBy)
			v.Status.ApprovedDecisionDigest = v.Spec.GOVARDecision.DecisionDigest
			return v
		}()},
		{name: "altered scope", object: func() *aiopsv1alpha1.AIChangeRequest {
			v := base.DeepCopy()
			v.Spec.GOVARRouteApproval.Provider.Generation++
			return v
		}()},
		{name: "live routing UID changed", object: base.DeepCopy(), routing: func() aiopsv1alpha1.AIRoutingPolicy {
			v := *routing.DeepCopy()
			v.UID = "replacement-routing-uid"
			return v
		}()},
		{name: "live model generation changed", object: base.DeepCopy(), candidate: func() govar.Candidate {
			v := candidate
			v.RouteSnapshot.ModelGeneration++
			v.RouteSnapshot.SnapshotHash = govar.RouteSnapshotHash(v.RouteSnapshot)
			v.SnapshotVersion = v.RouteSnapshot.SnapshotHash
			return v
		}()},
		{name: "live provider generation changed", object: base.DeepCopy(), candidate: func() govar.Candidate {
			v := candidate
			v.RouteSnapshot.ProviderGeneration++
			v.RouteSnapshot.SnapshotHash = govar.RouteSnapshotHash(v.RouteSnapshot)
			v.SnapshotVersion = v.RouteSnapshot.SnapshotHash
			return v
		}()},
		{name: "stale route snapshot", object: base.DeepCopy(), candidate: func() govar.Candidate {
			v := candidate
			v.RouteSnapshot.ProviderResourceVersion = "new-provider-rv"
			v.RouteSnapshot.SnapshotHash = govar.RouteSnapshotHash(v.RouteSnapshot)
			v.SnapshotVersion = v.RouteSnapshot.SnapshotHash
			return v
		}()},
		{name: "expired", object: func() *aiopsv1alpha1.AIChangeRequest {
			v := base.DeepCopy()
			v.Spec.GOVARRouteApproval.ValidUntil = metav1.NewTime(now.Add(-time.Second))
			v.Spec.GOVARRouteApproval.ScopeDigest = v.Spec.GOVARRouteApproval.ComputeDigest()
			v.Status.ApprovedScopeDigest = v.Spec.GOVARRouteApproval.ScopeDigest
			v.Status.ExpiresAt = &v.Spec.GOVARRouteApproval.ValidUntil
			return v
		}(), wantExpiry: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := fakeclient.NewClientBuilder().WithScheme(scheme)
			if test.object != nil {
				builder = builder.WithObjects(test.object)
			}
			srv := &server{k8s: builder.Build(), auth: identityAuthenticator{now: func() time.Time { return now }}}
			actualCandidate := test.candidate
			if actualCandidate.ModelRef == "" {
				actualCandidate = candidate
			}
			actualRouting := test.routing
			if actualRouting.Name == "" {
				actualRouting = routing
			}
			_, _, err := srv.resolveGOVARRouteApproval(context.Background(), base.Name, actualRouting, []govar.Candidate{actualCandidate})
			if err == nil {
				t.Fatal("invalid approval was accepted")
			}
			if test.wantExpiry && !errors.Is(err, errApprovalExpired) {
				t.Fatalf("err=%v, want expiry", err)
			}
		})
	}
}

func approvedGOVARRouteChange(now time.Time, routing aiopsv1alpha1.AIRoutingPolicy, candidate govar.Candidate) *aiopsv1alpha1.AIChangeRequest {
	snapshot := candidate.RouteSnapshot
	scope := aiopsv1alpha1.GOVARRouteApprovalScope{
		RoutingPolicy:       aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: routing.Name, UID: routing.UID, Generation: routing.Generation},
		Model:               aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: snapshot.ModelName, UID: types.UID(snapshot.ModelUID), Generation: snapshot.ModelGeneration},
		Provider:            aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: snapshot.ProviderName, UID: types.UID(snapshot.ProviderUID), Generation: snapshot.ProviderGeneration},
		RouteSnapshotDigest: snapshot.SnapshotHash, ValidUntil: metav1.NewTime(now.Add(time.Hour)),
	}
	scope.ScopeDigest = scope.ComputeDigest()
	change := &aiopsv1alpha1.AIChangeRequest{ObjectMeta: metav1.ObjectMeta{Name: "approve-routing-model-provider", Namespace: routing.Namespace, UID: "change-uid", Generation: 1},
		Spec: aiopsv1alpha1.AIChangeRequestSpec{Action: aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute, Approval: aiopsv1alpha1.AIChangeRequestApprovalApproved,
			GOVARRouteApproval: &scope, RequestedBy: "requester@example.test"}}
	change.Spec.ProposalDigest = change.Spec.ComputeProposalDigest(change.Namespace, change.Name)
	decision := &aiopsv1alpha1.GOVARRouteApprovalDecision{Outcome: aiopsv1alpha1.AIChangeRequestApprovalApproved,
		ReviewerIdentity: "reviewer@example.test", ReviewerGroup: aiopsv1alpha1.GOVARApprovalReviewerGroup,
		DecidedAt: metav1.NewTime(now.Add(-time.Minute)), ValidUntil: scope.ValidUntil, RequestUID: string(change.UID),
		AdmissionUID: "approval-admission-uid", ProposalDigest: change.Spec.ProposalDigest, ScopeDigest: scope.ScopeDigest}
	decision.DecisionDigest = decision.ComputeDigest(change.Namespace, change.Name, change.Spec.RequestedBy)
	change.Spec.GOVARDecision = decision
	change.Status = aiopsv1alpha1.AIChangeRequestStatus{ObservedGeneration: 1, Phase: aiopsv1alpha1.AIChangeRequestPhaseApproved,
		ApprovedAt: &metav1.Time{Time: now}, ApprovedScopeDigest: scope.ScopeDigest, ApprovedDecisionDigest: decision.DecisionDigest,
		ApprovedBy: decision.ReviewerIdentity, ExpiresAt: &scope.ValidUntil,
		Conditions: []metav1.Condition{{Type: aiopsv1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: aiopsv1alpha1.ReasonReconciled, LastTransitionTime: metav1.NewTime(now)}}}
	return change
}

func approvalTestCandidate() govar.Candidate {
	snapshot := govar.RouteSnapshot{Namespace: "finance", ModelName: "model", ModelUID: "model-uid", ModelGeneration: 1, ModelResourceVersion: "model-rv",
		ProviderName: "provider", ProviderUID: "provider-uid", ProviderGeneration: 1, ProviderResourceVersion: "provider-rv", PricingVersion: "pricing-v1",
		PricingComplianceHash: strings.Repeat("a", 64), RouteBindingName: "primary", ProviderDeployment: "provider-model", Cluster: "backend", Authority: "backend.example", PathMode: "openai-body"}
	snapshot.SnapshotHash = govar.RouteSnapshotHash(snapshot)
	return govar.Candidate{ModelRef: "model", ProviderRef: "provider", ProviderType: "openai", PricingVersion: "pricing-v1", SnapshotVersion: snapshot.SnapshotHash, RouteSnapshot: snapshot, Feasible: true}
}

func TestWriteAPIErrorUsesStableReasonCode(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeAPIError(recorder, http.StatusConflict, "invalid_transition", fmt.Errorf("conflict"))
	if recorder.Code != http.StatusConflict || !bytes.Contains(recorder.Body.Bytes(), []byte(`"reason_code":"invalid_transition"`)) {
		t.Fatalf("response=%d %s", recorder.Code, recorder.Body.String())
	}
}

func signedRequest(t *testing.T, secret []byte, timestamp time.Time, path, tenant, workload string, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	ts := fmt.Sprintf("%d", timestamp.Unix())
	digest := sha256.Sum256(body)
	message := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%x", ts, req.Method, req.URL.EscapedPath(), tenant, workload, "finance", digest)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(message))
	req.Header.Set("X-GOVAR-Tenant-ID", tenant)
	req.Header.Set("X-GOVAR-Workload-UID", workload)
	req.Header.Set("X-GOVAR-Namespace", "finance")
	req.Header.Set("X-GOVAR-Timestamp", ts)
	req.Header.Set("X-GOVAR-Signature", fmt.Sprintf("%x", mac.Sum(nil)))
	return req
}
