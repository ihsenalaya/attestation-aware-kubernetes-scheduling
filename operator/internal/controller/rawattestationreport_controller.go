package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/pkg/attestation/maa"
)

// RawAttestationReportReconciler is the CENTRAL VERIFIER. It is the ONLY
// component permitted to create/update AttestationEvidence. It consumes the
// untrusted RawAttestationReport produced by node agents, performs real MAA
// verification (or marks simulated in kind), and emits AttestationEvidence with
// an authoritative evidenceMode. A node agent can never write AttestationEvidence
// directly — RBAC enforces this (gate G0).
type RawAttestationReportReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// VerifierIdentity names this verifier in evidence provenance.
	VerifierIdentity string
	// VerifierPodUID identifies the verifier pod (from env POD_UID).
	VerifierPodUID string
	// ExpectedTEE is the attestation type required for real evidence (sevsnpvm).
	ExpectedTEE string
	// MAAIssuerPrefix guards against issuer spoofing (e.g. "https://").
	MAAIssuerPrefix string
	// MaxTokenAge bounds token freshness.
	MaxTokenAge time.Duration
	// verifyFn is injectable for tests; defaults to maa.VerifyMAAToken.
	verifyFn func(token string, opts maa.Options) maa.Result
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=rawattestationreports,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=rawattestationreports/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=attestationevidences,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=attestationevidences/status,verbs=get;update;patch

func (r *RawAttestationReportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var report aiopsv1alpha1.RawAttestationReport
	if err := r.Get(ctx, req.NamespacedName, &report); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	verify := r.verifyFn
	if verify == nil {
		verify = maa.VerifyMAAToken
	}

	evidenceName := fmt.Sprintf("evidence-%s", report.Spec.NodeName)
	evidence := &aiopsv1alpha1.AttestationEvidence{
		ObjectMeta: metav1.ObjectMeta{Name: evidenceName, Namespace: report.Namespace},
	}

	// Compute the verification outcome.
	var (
		mode       string
		vstatus    string
		failure    string
		verified   bool
		claimsHash string
		tokenHash  string
		issuedAt   *metav1.Time
		expiresAt  *metav1.Time
		freshnessS int64
		attestType string
	)

	switch {
	case report.Spec.Simulated:
		// kind/dev path: honestly simulated, never "real".
		mode = aiopsv1alpha1.EvidenceModeSimulated
		vstatus = aiopsv1alpha1.VerificationStatusVerified
		verified = true
		attestType = "simulated"
	default:
		res := verify(report.Spec.RawToken, maa.Options{
			ExpectedTEE:         r.ExpectedTEE,
			ExpectedNonce:       report.Spec.Nonce,
			MaxAge:              r.MaxTokenAge,
			AllowedIssuerPrefix: r.MAAIssuerPrefix,
		})
		tokenHash = res.TokenHash
		claimsHash = res.ClaimsDigest
		attestType = "guest-attestation"
		if !res.IssuedAt.IsZero() {
			t := metav1.NewTime(res.IssuedAt)
			issuedAt = &t
		}
		if !res.ExpiresAt.IsZero() {
			t := metav1.NewTime(res.ExpiresAt)
			expiresAt = &t
			if r.MaxTokenAge > 0 {
				freshnessS = int64(r.MaxTokenAge.Seconds())
			}
		}
		switch res.Status {
		case maa.StatusVerified:
			mode = aiopsv1alpha1.EvidenceModeReal
			vstatus = aiopsv1alpha1.VerificationStatusVerified
			verified = true
		case maa.StatusUnavailable:
			mode = aiopsv1alpha1.EvidenceModeUnverified
			vstatus = aiopsv1alpha1.VerificationStatusUnavailable
			failure = res.Reason
		default:
			mode = aiopsv1alpha1.EvidenceModeUnverified
			vstatus = aiopsv1alpha1.VerificationStatusFailed
			failure = res.Reason
		}
	}

	// Create or update the AttestationEvidence (spec) — the verifier owns it.
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, evidence, func() error {
		evidence.Spec.SubjectRef = aiopsv1alpha1.ObjectReference{Name: report.Spec.NodeName}
		evidence.Spec.EvidenceType = "cpu"
		if report.Spec.Simulated {
			evidence.Spec.TEE = teeFromAttestationType(r.ExpectedTEE)
			evidence.Spec.Simulated = true
		} else {
			evidence.Spec.TEE = "SEV-SNP"
			evidence.Spec.Simulated = false
		}
		evidence.Spec.Freshness = aiopsv1alpha1.EvidenceFreshness{
			MaxAgeSeconds: maxAgeSecondsOrDefault(freshnessS),
			Simulated:     report.Spec.Simulated,
		}
		if evidence.Labels == nil {
			evidence.Labels = map[string]string{}
		}
		evidence.Labels["ai.sovereign.io/verified-by"] = r.VerifierIdentity
		return nil
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("upsert evidence: %w", err)
	}

	// Write the authoritative status. This is the trust anchor.
	evidence.Status.ObservedGeneration = evidence.Generation
	evidence.Status.Verified = verified
	evidence.Status.EvidenceMode = mode
	evidence.Status.VerificationStatus = vstatus
	evidence.Status.FailureReason = failure
	evidence.Status.VerifiedBy = r.VerifierIdentity
	evidence.Status.VerifierPodUID = r.VerifierPodUID
	evidence.Status.NodeUID = report.Spec.NodeUID
	evidence.Status.Provider = report.Spec.Provider
	evidence.Status.AttestationType = attestType
	evidence.Status.MAATokenHash = tokenHash
	evidence.Status.ClaimsDigest = claimsHash
	evidence.Status.Nonce = report.Spec.Nonce
	evidence.Status.IssuedAt = issuedAt
	evidence.Status.ExpiresAt = expiresAt
	evidence.Status.FreshnessSeconds = freshnessS
	if verified {
		now := metav1.NewTime(time.Now().UTC())
		evidence.Status.LastVerifiedTime = &now
		meta.SetStatusCondition(&evidence.Status.Conditions, readyTrue(evidence.Generation, "attestation verified"))
	} else {
		meta.SetStatusCondition(&evidence.Status.Conditions, readyFalse(evidence.Generation, aiopsv1alpha1.ReasonValidationFailed, failure))
	}
	if err := r.Status().Update(ctx, evidence); err != nil {
		return ctrl.Result{}, fmt.Errorf("update evidence status: %w", err)
	}

	// Mark the report processed.
	report.Status.ObservedGeneration = report.Generation
	report.Status.Processed = true
	report.Status.EvidenceRef = evidenceName
	report.Status.VerificationStatus = vstatus
	if err := r.Status().Update(ctx, &report); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("update report status: %w", err)
	}

	logger.Info("verified raw attestation report",
		"node", report.Spec.NodeName, "mode", mode, "status", vstatus)

	// Re-verify simulated evidence periodically to keep it fresh (kind refresh).
	if report.Spec.Simulated {
		return ctrl.Result{RequeueAfter: simulatedEvidenceRefreshInterval}, nil
	}
	return ctrl.Result{}, nil
}

func teeFromAttestationType(t string) string {
	switch t {
	case "tdxvm":
		return "TDX"
	default:
		return "SEV-SNP"
	}
}

func maxAgeSecondsOrDefault(s int64) int32 {
	if s <= 0 {
		return 300
	}
	return int32(s)
}

func (r *RawAttestationReportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.VerifierIdentity == "" {
		r.VerifierIdentity = "central-verifier"
	}
	if r.VerifierPodUID == "" {
		r.VerifierPodUID = os.Getenv("POD_UID")
	}
	if r.ExpectedTEE == "" {
		r.ExpectedTEE = "sevsnpvm"
	}
	if r.MaxTokenAge == 0 {
		r.MaxTokenAge = 5 * time.Minute
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.RawAttestationReport{}).
		Complete(r)
}
