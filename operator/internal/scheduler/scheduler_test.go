package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
	"github.com/imperium/ai-sovereign-finops-operator/pkg/token"
)

func TestFilterAcceptsValidNode(t *testing.T) {
	s := newTestScheduler(t, baseObjects(t)...)
	policy := basePolicy()
	nodes := []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}}

	candidates, err := s.filter(context.Background(), basePod(), policy, nodes)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, want 1", len(candidates))
	}
	if candidates[0].Node.Name != "node-a" {
		t.Fatalf("selected node = %q", candidates[0].Node.Name)
	}
}

func TestFilterRejectsMissingEvidence(t *testing.T) {
	s := newTestScheduler(t,
		baseNamespace(),
		basePod(),
		basePolicy(),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
	)

	candidates, err := s.filter(context.Background(), basePod(), basePolicy(), []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0", len(candidates))
	}
}

func TestFilterRejectsExpiredEvidence(t *testing.T) {
	objs := baseObjects(t)
	evidence := objs[len(objs)-1].(*aiopsv1alpha1.AttestationEvidence)
	evidence.Status.LastVerifiedTime = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}
	s := newTestScheduler(t, objs...)

	candidates, err := s.filter(context.Background(), basePod(), basePolicy(), []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0", len(candidates))
	}
}

func TestFilterRejectsRevokedEvidence(t *testing.T) {
	objs := baseObjects(t)
	evidence := objs[len(objs)-1].(*aiopsv1alpha1.AttestationEvidence)
	evidence.Status.Revoked = true
	s := newTestScheduler(t, objs...)

	candidates, err := s.filter(context.Background(), basePod(), basePolicy(), []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0", len(candidates))
	}
}

// TestFilterValidEvidenceNotShadowedByInvalid guards against a regression where
// an invalid evidence object (wrong TEE) for the same node caused the scheduler
// to reject the node even though a separate, fully valid evidence existed.
func TestFilterValidEvidenceNotShadowedByInvalid(t *testing.T) {
	objs := baseObjects(t)
	// baseObjects already includes a valid TDX evidence for node-a. Add a second
	// evidence for the same node with a wrong TEE, listed BEFORE the valid one.
	wrongTEE := &aiopsv1alpha1.AttestationEvidence{
		ObjectMeta: metav1.ObjectMeta{Name: "aaa-wrong-tee", Namespace: "finance"},
		Spec: aiopsv1alpha1.AttestationEvidenceSpec{
			SubjectRef:   aiopsv1alpha1.ObjectReference{Name: "node-a"},
			EvidenceType: "cpu",
			TEE:          "SEV-SNP",
			Simulated:    true,
			Freshness:    aiopsv1alpha1.EvidenceFreshness{MaxAgeSeconds: 300, Simulated: true},
		},
		Status: aiopsv1alpha1.AttestationEvidenceStatus{
			Verified:         true,
			LastVerifiedTime: &metav1.Time{Time: time.Now()},
		},
	}
	objs = append(objs, wrongTEE)
	s := newTestScheduler(t, objs...)

	candidates, err := s.filter(context.Background(), basePod(), basePolicy(), []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, want 1 (valid TDX evidence must not be shadowed by wrong-TEE evidence)", len(candidates))
	}
	if candidates[0].Evidence == nil || candidates[0].Evidence.Spec.TEE != "TDX" {
		t.Fatalf("selected evidence TEE = %v, want TDX", candidates[0].Evidence)
	}
}

func TestFilterRejectsWrongTEE(t *testing.T) {
	objs := baseObjects(t)
	evidence := objs[len(objs)-1].(*aiopsv1alpha1.AttestationEvidence)
	evidence.Spec.TEE = "SEV-SNP"
	s := newTestScheduler(t, objs...)

	candidates, err := s.filter(context.Background(), basePod(), basePolicy(), []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0", len(candidates))
	}
}

func TestFilterRespectsNodeSelector(t *testing.T) {
	s := newTestScheduler(t, baseObjects(t)...)
	pod := basePod()
	pod.Spec.NodeSelector = map[string]string{"pool": "confidential"}

	nodes := []corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"pool": "system"}}},
	}
	candidates, err := s.filter(context.Background(), pod, basePolicy(), nodes)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 for nodeSelector mismatch", len(candidates))
	}
}

func TestFilterRespectsRuntimeClassScheduling(t *testing.T) {
	runtimeClassName := "kata-vm-isolation"
	runtimeClass := &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeClassName},
		Handler:    "kata",
		Scheduling: &nodev1.Scheduling{
			NodeSelector: map[string]string{"kubernetes.azure.com/kata-vm-isolation": "true"},
		},
	}
	s := newTestScheduler(t, append(baseObjects(t), runtimeClass)...)
	pod := basePod()
	pod.Spec.RuntimeClassName = &runtimeClassName

	nodes := []corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"kubernetes.azure.com/kata-vm-isolation": "false"}}},
	}
	candidates, err := s.filter(context.Background(), pod, basePolicy(), nodes)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 for RuntimeClass scheduling mismatch", len(candidates))
	}

	nodes[0].Labels["kubernetes.azure.com/kata-vm-isolation"] = "true"
	candidates, err = s.filter(context.Background(), pod, basePolicy(), nodes)
	if err != nil {
		t.Fatalf("filter with matching RuntimeClass scheduling: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, want 1 for RuntimeClass scheduling match", len(candidates))
	}
}

func TestFilterRespectsNoScheduleTaint(t *testing.T) {
	s := newTestScheduler(t, baseObjects(t)...)
	taint := corev1.Taint{Key: "ai.sovereign.io/confidential", Value: "true", Effect: corev1.TaintEffectNoSchedule}
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{taint}},
	}

	candidates, err := s.filter(context.Background(), basePod(), basePolicy(), []corev1.Node{node})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 without toleration", len(candidates))
	}

	pod := basePod()
	pod.Spec.Tolerations = []corev1.Toleration{{
		Key:      taint.Key,
		Operator: corev1.TolerationOpEqual,
		Value:    taint.Value,
		Effect:   taint.Effect,
	}}
	candidates, err = s.filter(context.Background(), pod, basePolicy(), []corev1.Node{node})
	if err != nil {
		t.Fatalf("filter with toleration: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, want 1 with matching toleration", len(candidates))
	}
}

func TestScorePrefersFreshestEvidence(t *testing.T) {
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair: %v", err)
	}
	s := New(newFakeClient(t), priv, pub, time.Minute)
	policy := basePolicy()
	now := time.Now()
	candidates := []NodeCandidate{
		{
			Node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "older"}},
			Evidence: &aiopsv1alpha1.AttestationEvidence{
				Spec:   aiopsv1alpha1.AttestationEvidenceSpec{TEE: "TDX"},
				Status: aiopsv1alpha1.AttestationEvidenceStatus{LastVerifiedTime: &metav1.Time{Time: now.Add(-4 * time.Minute)}},
			},
		},
		{
			Node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "fresher"}},
			Evidence: &aiopsv1alpha1.AttestationEvidence{
				Spec:   aiopsv1alpha1.AttestationEvidenceSpec{TEE: "TDX"},
				Status: aiopsv1alpha1.AttestationEvidenceStatus{LastVerifiedTime: &metav1.Time{Time: now.Add(-30 * time.Second)}},
			},
		},
	}

	s.score(candidates, policy)
	if candidates[1].Score <= candidates[0].Score {
		t.Fatalf("fresh node score = %d, old node score = %d", candidates[1].Score, candidates[0].Score)
	}
}

func TestPreBindDetectsRevokedEvidence(t *testing.T) {
	objs := baseObjects(t)
	evidence := objs[len(objs)-1].(*aiopsv1alpha1.AttestationEvidence)
	s := newTestScheduler(t, objs...)

	selected := NodeCandidate{Node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}, Evidence: evidence.DeepCopy()}
	decision, _, err := s.reserve(context.Background(), basePod(), basePolicy(), selected)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	var current aiopsv1alpha1.AttestationEvidence
	if err := s.client.Get(context.Background(), types.NamespacedName{Name: evidence.Name, Namespace: evidence.Namespace}, &current); err != nil {
		t.Fatalf("get evidence: %v", err)
	}
	current.Status.Revoked = true
	if err := s.client.Update(context.Background(), &current); err != nil {
		t.Fatalf("update evidence: %v", err)
	}

	if err := s.preBind(context.Background(), basePod(), decision, selected); err == nil {
		t.Fatal("expected revoked evidence error")
	}
}

func TestPreBindDetectsPolicyModified(t *testing.T) {
	objs := baseObjects(t)
	policy := objs[2].(*aiopsv1alpha1.ConfidentialInferencePolicy)
	evidence := objs[4].(*aiopsv1alpha1.AttestationEvidence)
	s := newTestScheduler(t, objs...)
	pod := basePod()

	selected := NodeCandidate{Node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}, Evidence: evidence.DeepCopy()}
	decision, _, err := s.reserve(context.Background(), pod, policy, selected)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	var current aiopsv1alpha1.ConfidentialInferencePolicy
	if err := s.client.Get(context.Background(), types.NamespacedName{Name: policy.Name, Namespace: policy.Namespace}, &current); err != nil {
		t.Fatalf("get policy: %v", err)
	}
	current.Spec.MaxEvidenceAgeSeconds = 999
	if err := s.client.Update(context.Background(), &current); err != nil {
		t.Fatalf("update policy: %v", err)
	}

	if err := s.preBind(context.Background(), pod, decision, selected); err == nil {
		t.Fatal("expected policy mismatch error")
	}
}

func TestPreBindFailClosedOnAPIReadError(t *testing.T) {
	objs := baseObjects(t)
	base := newFakeClient(t, objs...)
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair: %v", err)
	}
	s := New(base, priv, pub, time.Minute)
	pod := basePod()
	evidence := objs[4].(*aiopsv1alpha1.AttestationEvidence)
	selected := NodeCandidate{Node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}, Evidence: evidence.DeepCopy()}
	decision, _, err := s.reserve(context.Background(), pod, basePolicy(), selected)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	s.client = &failingClient{
		Client: base,
		getErr: func(key types.NamespacedName) error {
			if key.Name == "evidence-node-a" {
				return errors.New("boom")
			}
			return nil
		},
	}
	if err := s.preBind(context.Background(), pod, decision, selected); err == nil {
		t.Fatal("expected fail-closed API read error")
	}
}

func TestReserveDoesNotPersistAllowTokenWhenPreBindFails(t *testing.T) {
	objs := baseObjects(t)
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair: %v", err)
	}
	s := New(newFakeClient(t, objs...), priv, pub, time.Minute)
	pod := basePod()
	delete(pod.Annotations, "ai.sovereign.io/policy-hash")
	if err := s.client.Update(context.Background(), pod); err != nil {
		t.Fatalf("update pod without policy hash: %v", err)
	}
	evidence := objs[4].(*aiopsv1alpha1.AttestationEvidence)
	selected := NodeCandidate{Node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}, Evidence: evidence.DeepCopy()}

	decision, tokenStr, err := s.reserve(context.Background(), pod, basePolicy(), selected)
	if err == nil {
		t.Fatal("expected reserve to fail closed when policy hash is missing before bind")
	}
	if tokenStr != "" {
		t.Fatal("failed PreBind must not return a placement token")
	}
	if decision == nil {
		t.Fatal("expected deny decision to be persisted for audit")
	}
	var persisted aiopsv1alpha1.AIPlacementDecision
	if err := s.client.Get(context.Background(), types.NamespacedName{Name: decision.Name, Namespace: decision.Namespace}, &persisted); err != nil {
		t.Fatalf("get persisted decision: %v", err)
	}
	if persisted.Status.Decision != "deny" {
		t.Fatalf("decision status = %q, want deny", persisted.Status.Decision)
	}
	if persisted.Annotations[PlacementTokenAnnotation] != "" {
		t.Fatal("failed PreBind must not persist a placement token annotation")
	}
}

func TestPermitTimeout(t *testing.T) {
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair: %v", err)
	}
	s := New(newFakeClient(t, baseNamespace(), basePod(), basePolicy(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}), priv, pub, time.Minute)
	s.permitTimeout = 50 * time.Millisecond
	s.permitPollInterval = 10 * time.Millisecond

	_, err = s.permit(context.Background(), basePod(), basePolicy(), []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}})
	if err == nil {
		t.Fatal("expected permit timeout error")
	}
}

func TestPermitAcceptsEvidenceBecomingValid(t *testing.T) {
	objs := []ctrlclient.Object{
		baseNamespace(),
		basePod(),
		basePolicy(),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&aiopsv1alpha1.AttestationEvidence{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "evidence-node-a",
				Namespace: "finance",
			},
			Spec: aiopsv1alpha1.AttestationEvidenceSpec{
				SubjectRef:   aiopsv1alpha1.ObjectReference{Name: "node-a"},
				EvidenceType: "cpu",
				TEE:          "TDX",
				Simulated:    true,
				Freshness:    aiopsv1alpha1.EvidenceFreshness{MaxAgeSeconds: 300, Simulated: true},
			},
			Status: aiopsv1alpha1.AttestationEvidenceStatus{
				Verified: false,
			},
		},
	}
	s := newTestScheduler(t, objs...)
	s.permitTimeout = 500 * time.Millisecond
	s.permitPollInterval = 25 * time.Millisecond

	go func() {
		time.Sleep(100 * time.Millisecond)
		var evidence aiopsv1alpha1.AttestationEvidence
		if err := s.client.Get(context.Background(), types.NamespacedName{Name: "evidence-node-a", Namespace: "finance"}, &evidence); err != nil {
			return
		}
		evidence.Status.Verified = true
		evidence.Status.LastVerifiedTime = &metav1.Time{Time: time.Now()}
		_ = s.client.Update(context.Background(), &evidence)
	}()

	candidates, err := s.permit(context.Background(), basePod(), basePolicy(), []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}})
	if err != nil {
		t.Fatalf("permit: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, want 1", len(candidates))
	}
}

func TestCreateOrUpdatePlacementDecisionUpdatesSpec(t *testing.T) {
	s := newTestScheduler(t, baseObjects(t)...)
	pod := basePod()
	evidence := baseEvidence()
	selected := NodeCandidate{Node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}, Evidence: evidence}

	first, _, err := s.reserve(context.Background(), pod, basePolicy(), selected)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if first.Spec.SchedulerName != SchedulerName {
		t.Fatalf("schedulerName = %q", first.Spec.SchedulerName)
	}

	second, _, err := s.reserve(context.Background(), pod, basePolicy(), selected)
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if second.Spec.TargetRef.Name != pod.Name {
		t.Fatalf("targetRef.name = %q, want %q", second.Spec.TargetRef.Name, pod.Name)
	}
}

func newTestScheduler(t *testing.T, objs ...ctrlclient.Object) *Scheduler {
	t.Helper()
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair: %v", err)
	}
	return New(newFakeClient(t, objs...), priv, pub, time.Minute)
}

func newFakeClient(t *testing.T, objs ...ctrlclient.Object) ctrlclient.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Add corev1 scheme: %v", err)
	}
	if err := nodev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Add nodev1 scheme: %v", err)
	}
	if err := aiopsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Add aiops scheme: %v", err)
	}
	return fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func baseObjects(t *testing.T) []ctrlclient.Object {
	t.Helper()
	return []ctrlclient.Object{
		baseNamespace(),
		basePod(),
		basePolicy(),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		baseEvidence(),
	}
}

func baseNamespace() *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "finance",
			Labels: map[string]string{"ai.sovereign.io/sensitivity": "high"},
		},
	}
}

func basePod() *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "risk-assistant",
			Namespace: "finance",
			UID:       types.UID("pod-uid-1"),
			Labels:    map[string]string{"app": "risk-assistant"},
			Annotations: map[string]string{
				"ai.sovereign.io/model-digest":                  "sha256:model",
				"ai.sovereign.io/expected-evidence-age-seconds": "300",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: SchedulerName,
			Containers:    []corev1.Container{{Name: "app", Image: "ghcr.io/example/risk-assistant@sha256:abc"}},
		},
	}
	hash, _ := platformcrypto.CanonicalSHA256Hex(basePolicy().Spec)
	pod.Annotations["ai.sovereign.io/policy-hash"] = hash
	return pod
}

func basePolicy() *aiopsv1alpha1.ConfidentialInferencePolicy {
	return &aiopsv1alpha1.ConfidentialInferencePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "finance-conf", Namespace: "finance"},
		Spec: aiopsv1alpha1.ConfidentialInferencePolicySpec{
			Target: aiopsv1alpha1.WorkloadTarget{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"ai.sovereign.io/sensitivity": "high"}},
				WorkloadSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "risk-assistant"}},
			},
			RequiredTEE:                   []string{"TDX"},
			RequireConfidentialContainers: true,
			AllowedRuntimeClasses:         []string{"kata-qemu-tdx"},
			MaxEvidenceAgeSeconds:         300,
			RequireImageDigest:            true,
			RequireModelDigest:            true,
			EnforcementMode:               aiopsv1alpha1.EnforcementModeEnforce,
		},
	}
}

func baseEvidence() *aiopsv1alpha1.AttestationEvidence {
	return &aiopsv1alpha1.AttestationEvidence{
		ObjectMeta: metav1.ObjectMeta{Name: "evidence-node-a", Namespace: "finance"},
		Spec: aiopsv1alpha1.AttestationEvidenceSpec{
			SubjectRef:   aiopsv1alpha1.ObjectReference{Name: "node-a"},
			EvidenceType: "cpu",
			TEE:          "TDX",
			Simulated:    true,
			Runtime:      aiopsv1alpha1.RuntimeExpectation{RuntimeClassName: "simulated-kata-qemu-tdx", Simulated: true},
			Freshness:    aiopsv1alpha1.EvidenceFreshness{MaxAgeSeconds: 300, Simulated: true},
		},
		Status: aiopsv1alpha1.AttestationEvidenceStatus{
			Verified:         true,
			LastVerifiedTime: &metav1.Time{Time: time.Now()},
		},
	}
}

type failingClient struct {
	ctrlclient.Client
	getErr func(types.NamespacedName) error
}

func (c *failingClient) Get(ctx context.Context, key types.NamespacedName, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	if c.getErr != nil {
		if err := c.getErr(key); err != nil {
			return err
		}
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestReserveProducesDeterministicHashes(t *testing.T) {
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair: %v", err)
	}
	s := New(newFakeClient(t, baseObjects(t)...), priv, pub, time.Minute)
	pod := basePod()
	runtimeClass := "simulated-kata-qemu-tdx"
	pod.Spec.RuntimeClassName = &runtimeClass

	selected := NodeCandidate{
		Node:     corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		Evidence: baseEvidence(),
	}
	first, firstToken, err := s.reserve(context.Background(), pod, basePolicy(), selected)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	second, secondToken, err := s.reserve(context.Background(), pod, basePolicy(), selected)
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if firstToken == "" || secondToken == "" {
		t.Fatal("placement token must not be empty")
	}
	if first.Status.NodeName != second.Status.NodeName {
		t.Fatalf("node names differ: %q vs %q", first.Status.NodeName, second.Status.NodeName)
	}
}

// TestReservePersistsIndependentlyVerifiableToken checks that the full signed
// placement token is stored on the AIPlacementDecision and verifies offline
// with only the public key — the verify-placement CLI path.
func TestReservePersistsIndependentlyVerifiableToken(t *testing.T) {
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair: %v", err)
	}
	s := New(newFakeClient(t, baseObjects(t)...), priv, pub, time.Minute)
	pod := basePod()
	runtimeClass := "simulated-kata-qemu-tdx"
	pod.Spec.RuntimeClassName = &runtimeClass

	selected := NodeCandidate{
		Node:     corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		Evidence: baseEvidence(),
	}
	decision, tokenStr, err := s.reserve(context.Background(), pod, basePolicy(), selected)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	stored := decision.Annotations[PlacementTokenAnnotation]
	if stored == "" {
		t.Fatalf("annotation %q must carry the placement token", PlacementTokenAnnotation)
	}
	if stored != tokenStr {
		t.Fatal("stored token differs from minted token")
	}

	tok, err := token.Decode(stored)
	if err != nil {
		t.Fatalf("decode stored token: %v", err)
	}
	if err := token.Verify(pub, tok); err != nil {
		t.Fatalf("stored token must verify with public key alone: %v", err)
	}

	// Identity binding: the token must be rejected for any other pod UID.
	podSpecHash, err := platformcrypto.CanonicalSHA256Hex(pod.Spec)
	if err != nil {
		t.Fatalf("hash pod spec: %v", err)
	}
	if err := token.VerifyForPod(pub, tok, string(pod.UID), podSpecHash, "node-a", tok.Payload.EvidenceHash, tok.Payload.PolicyHash); err != nil {
		t.Fatalf("token must verify for the bound pod: %v", err)
	}
	if err := token.VerifyForPod(pub, tok, "other-pod-uid", podSpecHash, "node-a", tok.Payload.EvidenceHash, tok.Payload.PolicyHash); err == nil {
		t.Fatal("token must NOT verify for a different pod UID")
	}

	// Tamper resistance: altering the signature must break verification.
	tampered := tok
	if len(tampered.Signature) > 1 {
		tampered.Signature = "00" + tampered.Signature[2:]
		if tampered.Signature == tok.Signature {
			tampered.Signature = "ff" + tok.Signature[2:]
		}
	}
	if err := token.Verify(pub, tampered); err == nil {
		t.Fatal("tampered token must NOT verify")
	}
}
