package podinjector

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
	"github.com/imperium/ai-sovereign-finops-operator/internal/webhook/changeapproval"
	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
)

const (
	PolicyHashAnnotation          = "ai.sovereign.io/policy-hash"
	ModelDigestAnnotation         = "ai.sovereign.io/model-digest"
	ExpectedRuntimeAnnotation     = "ai.sovereign.io/expected-runtime-class"
	AppliedRuntimeAnnotation      = "ai.sovereign.io/applied-runtime-class"
	ExpectedEvidenceAgeAnnotation = "ai.sovereign.io/expected-evidence-age-seconds"
	SimulatedExecutionAnnotation  = "ai.sovereign.io/simulated-execution"
	AttestationEvidenceAnnotation = "ai.sovereign.io/attestation-evidence"
	AttestationEvidenceGate       = "aiops.imperium.io/attestation-evidence"
	DefaultSchedulerName          = "ai-attestation-scheduler"
	SimulatedRuntimeClassLabel    = "ai.sovereign.io/simulated"
	PlatformModeEnv               = "AIOPS_PLATFORM_MODE"
	PlatformModeSimulatedKind     = "simulated-kind"
	PlatformModeProduction        = "production"
)

type confidentialMutation struct {
	policy          *aiopsv1alpha1.ConfidentialInferencePolicy
	expectedRuntime string
	appliedRuntime  string
	simulated       bool
}

type ValidationHandler struct {
	client    client.Reader
	decoder   *admission.Decoder
	approvals *changeapproval.ValidationHandler
}

func NewValidation(c client.Reader, scheme *runtime.Scheme) *ValidationHandler {
	return &ValidationHandler{
		client:    c,
		decoder:   admission.NewDecoder(scheme),
		approvals: changeapproval.NewValidation(scheme),
	}
}

func (h *ValidationHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	if changeapproval.Matches(req) {
		return h.approvals.Handle(ctx, req)
	}
	var pod corev1.Pod
	if err := h.decoder.Decode(req, &pod); err != nil {
		return admission.Errored(400, err)
	}
	if req.Operation != "" && req.Operation != admissionv1.Create && req.Operation != admissionv1.Update {
		return admission.Allowed("no validation needed for this operation")
	}

	namespace := req.Namespace
	if namespace == "" {
		namespace = pod.Namespace
	}
	policies, err := matchingPolicies(ctx, h.client, namespace, &pod)
	if err != nil {
		return admission.Errored(500, err)
	}
	for _, policy := range policies {
		if errs := validatePodAgainstPolicy(ctx, h.client, platformMode(), policy, &pod); len(errs) > 0 {
			return admission.Denied(strings.Join(errs, "; "))
		}
	}
	return admission.Allowed("pod satisfies confidential policies")
}

func matchingPolicies(ctx context.Context, c client.Reader, namespace string, pod *corev1.Pod) ([]aiopsv1alpha1.ConfidentialInferencePolicy, error) {
	if c == nil || namespace == "" || pod == nil {
		return nil, nil
	}
	var ns corev1.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: namespace}, &ns); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read namespace %q: %w", namespace, err)
	}
	var list aiopsv1alpha1.ConfidentialInferencePolicyList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list ConfidentialInferencePolicy in %q: %w", namespace, err)
	}
	out := make([]aiopsv1alpha1.ConfidentialInferencePolicy, 0, len(list.Items))
	for i := range list.Items {
		if policyMatches(&list.Items[i], &ns, pod) {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

func policyMatches(policy *aiopsv1alpha1.ConfidentialInferencePolicy, ns *corev1.Namespace, pod *corev1.Pod) bool {
	if policy == nil || ns == nil || pod == nil {
		return false
	}
	if policy.Spec.Target.NamespaceSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(policy.Spec.Target.NamespaceSelector)
		if err != nil || !selector.Matches(labels.Set(ns.Labels)) {
			return false
		}
	}
	if policy.Spec.Target.WorkloadSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(policy.Spec.Target.WorkloadSelector)
		if err != nil || !selector.Matches(labels.Set(pod.Labels)) {
			return false
		}
	}
	return true
}

func mutateForConfidentialPolicy(ctx context.Context, c client.Reader, namespace string, pod *corev1.Pod) (*confidentialMutation, error) {
	policies, err := matchingPolicies(ctx, c, namespace, pod)
	if err != nil || len(policies) == 0 {
		return nil, err
	}
	policy := policies[0]
	expectedRuntime := firstAllowedRuntime(policy.Spec.AllowedRuntimeClasses)
	appliedRuntime := expectedRuntime
	simulated := false
	if policy.Spec.RequireConfidentialContainers {
		appliedRuntime, simulated = mapRuntimeClass(expectedRuntime, platformMode())
	}
	return &confidentialMutation{
		policy:          &policy,
		expectedRuntime: expectedRuntime,
		appliedRuntime:  appliedRuntime,
		simulated:       simulated,
	}, nil
}

func firstAllowedRuntime(runtimeClasses []string) string {
	for _, runtimeClass := range runtimeClasses {
		if trimmed := strings.TrimSpace(runtimeClass); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func mapRuntimeClass(runtimeClass, mode string) (string, bool) {
	switch runtimeClass {
	case "kata-qemu-tdx":
		if mode == PlatformModeSimulatedKind {
			return "simulated-kata-qemu-tdx", true
		}
	case "kata-qemu-snp":
		if mode == PlatformModeSimulatedKind {
			return "simulated-kata-qemu-snp", true
		}
	}
	return runtimeClass, false
}

func validatePodAgainstPolicy(ctx context.Context, c client.Reader, mode string, policy aiopsv1alpha1.ConfidentialInferencePolicy, pod *corev1.Pod) []string {
	var errs []string
	for _, validationErr := range policy.Spec.Validate(field.NewPath("spec")) {
		errs = append(errs, validationErr.Error())
	}
	if policy.Spec.RequireImageDigest {
		for _, container := range pod.Spec.Containers {
			if !strings.Contains(container.Image, "@sha256:") {
				errs = append(errs, fmt.Sprintf("container %q image must be digest-pinned", container.Name))
			}
		}
	}
	if policy.Spec.RequireModelDigest && strings.TrimSpace(pod.Annotations[ModelDigestAnnotation]) == "" {
		errs = append(errs, fmt.Sprintf("annotation %q is required", ModelDigestAnnotation))
	}
	if policy.Spec.RequireConfidentialGPU {
		errs = append(errs, "confidential GPU attestation is not implemented in Article 1; GPU-required workloads fail closed")
	}
	if policy.Spec.RequireConfidentialContainers {
		runtimeClass := firstAllowedRuntime(policy.Spec.AllowedRuntimeClasses)
		if pod.Spec.RuntimeClassName != nil && strings.TrimSpace(*pod.Spec.RuntimeClassName) != "" {
			runtimeClass = *pod.Spec.RuntimeClassName
		}
		if runtimeClass == "" {
			errs = append(errs, "no runtimeClassName could be resolved for confidential containers")
		}
		if !runtimeAllowed(policy, runtimeClass) {
			errs = append(errs, fmt.Sprintf("runtimeClass %q is not allowed by policy %q", runtimeClass, policy.Name))
		}
		if mode == PlatformModeProduction {
			if strings.HasPrefix(runtimeClass, "simulated-") {
				errs = append(errs, fmt.Sprintf("simulated runtimeClass %q is forbidden in production", runtimeClass))
			}
			if c != nil && runtimeClass != "" {
				var rc nodev1.RuntimeClass
				if err := c.Get(ctx, types.NamespacedName{Name: runtimeClass}, &rc); err == nil && rc.Labels[SimulatedRuntimeClassLabel] == "true" {
					errs = append(errs, fmt.Sprintf("runtimeClass %q is labelled simulated and forbidden in production", runtimeClass))
				}
			}
		}
	}
	return errs
}

func annotateConfidentialPod(pod *corev1.Pod, mutation *confidentialMutation) {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	policyHash, err := platformcrypto.CanonicalSHA256Hex(mutation.policy.Spec)
	if err == nil {
		pod.Annotations[PolicyHashAnnotation] = policyHash
	}
	pod.Annotations[ExpectedRuntimeAnnotation] = mutation.expectedRuntime
	pod.Annotations[AppliedRuntimeAnnotation] = mutation.appliedRuntime
	pod.Annotations[ExpectedEvidenceAgeAnnotation] = fmt.Sprintf("%d", mutation.policy.Spec.MaxEvidenceAgeSeconds)
	if mutation.simulated {
		pod.Annotations[SimulatedExecutionAnnotation] = "true"
	} else {
		pod.Annotations[SimulatedExecutionAnnotation] = "false"
	}
}

func ensureSchedulingGate(pod *corev1.Pod) {
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == AttestationEvidenceGate {
			return
		}
	}
	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: AttestationEvidenceGate})
}

func evidencePresent(pod *corev1.Pod) bool {
	return strings.TrimSpace(pod.Annotations[AttestationEvidenceAnnotation]) != ""
}

func applySimulatedRuntimeMetric(namespace, policyName, runtimeClass string, simulated bool) {
	value := 0.0
	if simulated {
		value = 1.0
	}
	metrics.SimulatedRuntimeClassInUse.WithLabelValues(namespace, policyName, runtimeClass).Set(value)
}

func platformMode() string {
	mode := strings.TrimSpace(strings.ToLower(strings.ReplaceAll(os.Getenv(PlatformModeEnv), "_", "-")))
	switch mode {
	case PlatformModeProduction, "prod", "aks", "aks-private", "aks-real-sevsnp":
		return PlatformModeProduction
	case "", "kind", PlatformModeSimulatedKind, "simulated", "dev", "development", "local":
		return PlatformModeSimulatedKind
	default:
		if strings.Contains(mode, "aks") || strings.Contains(mode, "prod") {
			return PlatformModeProduction
		}
		return mode
	}
}

func runtimeAllowed(policy aiopsv1alpha1.ConfidentialInferencePolicy, runtimeClass string) bool {
	if !policy.Spec.RequireConfidentialContainers {
		return true
	}
	return slices.Contains(policy.Spec.AllowedRuntimeClasses, runtimeClass) ||
		slices.Contains(policy.Spec.AllowedRuntimeClasses, strings.TrimPrefix(runtimeClass, "simulated-"))
}
