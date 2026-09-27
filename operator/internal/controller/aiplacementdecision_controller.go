package controller

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/placement"
	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
)

type AIPlacementDecisionReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiplacementdecisions,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiplacementdecisions/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiplacementdecisions/finalizers,verbs=update
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=confidentialinferencepolicies;attestationevidences,verbs=get;list;watch

func (r *AIPlacementDecisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var decision aiopsv1alpha1.AIPlacementDecision
	if err := r.Get(ctx, req.NamespacedName, &decision); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	policy, err := r.fetchConfidentialPolicy(ctx, decision.Namespace, decision.Spec.PolicyRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	evidence, err := r.fetchAttestationEvidence(ctx, decision.Namespace, decision.Spec.EvidenceRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	placementDecision := placement.Evaluate(placement.Policy{
		AllowedTEEs:           append([]string{}, policy.Spec.RequiredTEE...),
		MaxEvidenceAgeSeconds: policy.Spec.MaxEvidenceAgeSeconds,
		RequiredRuntimeClass:  evidence.Spec.Runtime.RuntimeClassName,
	}, placement.Evidence{
		TEE:              evidence.Spec.TEE,
		AgeSeconds:       evidence.Spec.Freshness.MaxAgeSeconds,
		Revoked:          evidence.Status.Revoked,
		RuntimeClassName: evidence.Spec.Runtime.RuntimeClassName,
	})

	decision.Status.ObservedGeneration = decision.Generation
	if placementDecision.Allow {
		decision.Status.Decision = "allow"
		meta.SetStatusCondition(&decision.Status.Conditions, readyTrue(decision.Generation, placementDecision.Reason))
	} else {
		decision.Status.Decision = "deny"
		meta.SetStatusCondition(&decision.Status.Conditions, readyFalse(decision.Generation, aiopsv1alpha1.ReasonValidationFailed, placementDecision.Reason))
	}
	decision.Status.Simulated = evidence.Spec.Simulated || evidence.Spec.Freshness.Simulated
	if decision.Spec.PlacementToken.Required && placementDecision.Allow {
		raw, _ := json.Marshal(map[string]any{
			"target":   decision.Spec.TargetRef,
			"policy":   decision.Spec.PolicyRef,
			"evidence": evidence.Name,
			"runtime":  placementDecision.Runtime,
			"score":    placementDecision.Score,
		})
		decision.Status.PlacementTokenDigest = platformcrypto.SHA256Hex(raw)
	}

	if err := r.Status().Update(ctx, &decision); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *AIPlacementDecisionReconciler) fetchConfidentialPolicy(ctx context.Context, defaultNamespace string, ref aiopsv1alpha1.ObjectReference) (*aiopsv1alpha1.ConfidentialInferencePolicy, error) {
	namespace := ref.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	var policy aiopsv1alpha1.ConfidentialInferencePolicy
	if err := r.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: namespace}, &policy); err != nil {
		return nil, fmt.Errorf("read ConfidentialInferencePolicy %s/%s: %w", namespace, ref.Name, err)
	}
	return &policy, nil
}

func (r *AIPlacementDecisionReconciler) fetchAttestationEvidence(ctx context.Context, defaultNamespace string, ref *aiopsv1alpha1.ObjectReference) (*aiopsv1alpha1.AttestationEvidence, error) {
	if ref == nil {
		return nil, fmt.Errorf("AIPlacementDecision requires spec.evidenceRef")
	}
	namespace := ref.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	var evidence aiopsv1alpha1.AttestationEvidence
	if err := r.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: namespace}, &evidence); err != nil {
		return nil, fmt.Errorf("read AttestationEvidence %s/%s: %w", namespace, ref.Name, err)
	}
	return &evidence, nil
}

func (r *AIPlacementDecisionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIPlacementDecision{}).
		Complete(r)
}
