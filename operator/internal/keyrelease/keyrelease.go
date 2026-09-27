// Package keyrelease implements the conditional key-release decision engine.
// It evaluates a KeyReleaseRequest against placement token, evidence freshness,
// revocation status, policy requirements and pod identity constraints.
package keyrelease

import (
	"crypto/ed25519"
	"fmt"
	"strings"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/pkg/token"
)

// DenyReason enumerates machine-readable denial causes.
type DenyReason string

const (
	ReasonAllowed             DenyReason = "ALLOWED"
	ReasonPolicyNotRequired   DenyReason = "POLICY_NOT_REQUIRED"
	ReasonEvidenceRevoked     DenyReason = "EVIDENCE_REVOKED"
	ReasonEvidenceExpired     DenyReason = "EVIDENCE_EXPIRED"
	ReasonEvidenceNotVerified DenyReason = "EVIDENCE_NOT_VERIFIED"
	ReasonTokenInvalid        DenyReason = "TOKEN_INVALID"
	ReasonTokenExpired        DenyReason = "TOKEN_EXPIRED"
	ReasonPodUIDMismatch      DenyReason = "POD_UID_MISMATCH"
	ReasonModelDigestMismatch DenyReason = "MODEL_DIGEST_MISMATCH"
	ReasonImageDigestMismatch DenyReason = "IMAGE_DIGEST_MISMATCH"
	ReasonPolicyHashMismatch  DenyReason = "POLICY_HASH_MISMATCH"
	ReasonRevocationActive    DenyReason = "REVOCATION_ACTIVE"
	ReasonTTLInvalid          DenyReason = "TTL_INVALID"
	ReasonAuditRequired       DenyReason = "AUDIT_REQUIRED_NOT_MET"
)

// Request contains all inputs needed to evaluate a key release.
type Request struct {
	// Pod identity
	Namespace   string `json:"namespace"`
	PodName     string `json:"podName"`
	PodUID      string `json:"podUID"`
	PodSpecHash string `json:"podSpecHash,omitempty"`

	// Key being requested
	KeyID string `json:"keyID"`

	// Digests for binding
	ModelDigest  string `json:"modelDigest,omitempty"`
	ImageDigest  string `json:"imageDigest,omitempty"`
	PolicyHash   string `json:"policyHash,omitempty"`
	EvidenceHash string `json:"evidenceHash,omitempty"`

	// Placement token (JSON-encoded token.Token)
	PlacementToken string `json:"placementToken,omitempty"`

	// Evidence metadata
	EvidenceVerified   bool      `json:"evidenceVerified"`
	EvidenceRevoked    bool      `json:"evidenceRevoked"`
	EvidenceLastSeen   time.Time `json:"evidenceLastSeen,omitempty"`
	MaxEvidenceAgeSecs int32     `json:"maxEvidenceAgeSecs,omitempty"`

	// Policy flags
	PolicyRequired      bool  `json:"policyRequired"`
	PolicyTTLSeconds    int32 `json:"policyTTLSeconds,omitempty"`
	PolicyAuditRequired bool  `json:"policyAuditRequired,omitempty"`

	// Revocation
	RevocationActive bool `json:"revocationActive,omitempty"`

	// Signing key for token verification (optional)
	TokenPublicKey ed25519.PublicKey `json:"-"`
}

// Response is the key-release decision.
type Response struct {
	Allowed        bool       `json:"allowed"`
	Reason         DenyReason `json:"reason"`
	KeyMaterialRef string     `json:"keyMaterialRef,omitempty"`
	TTLSeconds     int32      `json:"ttlSeconds,omitempty"`
	EvidenceRecord string     `json:"evidenceRecord,omitempty"`
}

// Evaluate decides whether to allow or deny the key release.
func Evaluate(req Request) Response {
	// Policy check
	if !req.PolicyRequired {
		return Response{Allowed: true, Reason: ReasonPolicyNotRequired, TTLSeconds: req.PolicyTTLSeconds}
	}
	if req.PolicyTTLSeconds <= 0 {
		return deny(ReasonTTLInvalid, "policyTTLSeconds must be > 0")
	}

	// Revocation check (highest priority after policy)
	if req.RevocationActive {
		return deny(ReasonRevocationActive, "an active AIRevocationPolicy blocks key release")
	}

	// Evidence checks
	if req.EvidenceRevoked {
		return deny(ReasonEvidenceRevoked, "attestation evidence is revoked")
	}
	if !req.EvidenceVerified {
		return deny(ReasonEvidenceNotVerified, "attestation evidence not verified")
	}
	if req.MaxEvidenceAgeSecs > 0 && !req.EvidenceLastSeen.IsZero() {
		age := time.Since(req.EvidenceLastSeen)
		maxAge := time.Duration(req.MaxEvidenceAgeSecs) * time.Second
		if age > maxAge {
			return deny(ReasonEvidenceExpired, fmt.Sprintf("evidence age %v exceeds max %v", age.Round(time.Second), maxAge))
		}
	}

	// Placement token verification
	if req.PlacementToken != "" {
		if err := verifyToken(req); err != nil {
			if strings.Contains(err.Error(), "expired") {
				return deny(ReasonTokenExpired, err.Error())
			}
			if strings.Contains(err.Error(), "pod UID mismatch") {
				return deny(ReasonPodUIDMismatch, err.Error())
			}
			if strings.Contains(err.Error(), "model") {
				return deny(ReasonModelDigestMismatch, err.Error())
			}
			if strings.Contains(err.Error(), "image") {
				return deny(ReasonImageDigestMismatch, err.Error())
			}
			if strings.Contains(err.Error(), "policy") {
				return deny(ReasonPolicyHashMismatch, err.Error())
			}
			return deny(ReasonTokenInvalid, err.Error())
		}
	}

	// Audit check
	if req.PolicyAuditRequired && strings.TrimSpace(req.EvidenceHash) == "" {
		return deny(ReasonAuditRequired, "policy requires audit evidence hash but none provided")
	}

	return Response{
		Allowed:        true,
		Reason:         ReasonAllowed,
		TTLSeconds:     req.PolicyTTLSeconds,
		KeyMaterialRef: fmt.Sprintf("key://%s", req.KeyID),
	}
}

func deny(reason DenyReason, _ string) Response {
	return Response{Allowed: false, Reason: reason}
}

func verifyToken(req Request) error {
	tok, err := token.Decode(req.PlacementToken)
	if err != nil {
		return fmt.Errorf("decode placement token: %w", err)
	}

	if req.TokenPublicKey != nil {
		if err := token.Verify(req.TokenPublicKey, tok); err != nil {
			return err
		}
	}

	// Pod UID binding
	if req.PodUID != "" && tok.Payload.PodUID != req.PodUID {
		return fmt.Errorf("pod UID mismatch: token=%q request=%q", tok.Payload.PodUID, req.PodUID)
	}

	// Model digest binding
	if req.ModelDigest != "" && tok.Payload.ModelDigest != "" && tok.Payload.ModelDigest != req.ModelDigest {
		return fmt.Errorf("model digest mismatch: token=%q request=%q", tok.Payload.ModelDigest, req.ModelDigest)
	}

	// Image digest binding
	if req.ImageDigest != "" && tok.Payload.ImageDigest != "" && tok.Payload.ImageDigest != req.ImageDigest {
		return fmt.Errorf("image digest mismatch: token=%q request=%q", tok.Payload.ImageDigest, req.ImageDigest)
	}

	// Policy hash binding
	if req.PolicyHash != "" && tok.Payload.PolicyHash != "" && tok.Payload.PolicyHash != req.PolicyHash {
		return fmt.Errorf("policy hash mismatch: token=%q request=%q", tok.Payload.PolicyHash, req.PolicyHash)
	}

	return nil
}
