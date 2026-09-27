package keyrelease

import (
	"testing"
	"time"

	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
	"github.com/imperium/ai-sovereign-finops-operator/pkg/token"
)

func baseReq() Request {
	return Request{
		Namespace:        "test",
		PodName:          "pod-1",
		PodUID:           "uid-1",
		KeyID:            "key-1",
		PolicyRequired:   true,
		PolicyTTLSeconds: 300,
		EvidenceVerified: true,
		EvidenceRevoked:  false,
		RevocationActive: false,
	}
}

func TestAllowed(t *testing.T) {
	resp := Evaluate(baseReq())
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %+v", resp)
	}
	if resp.Reason != ReasonAllowed {
		t.Errorf("expected reason %q, got %q", ReasonAllowed, resp.Reason)
	}
}

func TestPolicyNotRequired(t *testing.T) {
	req := baseReq()
	req.PolicyRequired = false
	resp := Evaluate(req)
	if !resp.Allowed {
		t.Fatalf("expected allowed when policy not required, got %+v", resp)
	}
	if resp.Reason != ReasonPolicyNotRequired {
		t.Errorf("unexpected reason %q", resp.Reason)
	}
}

func TestDenyRevokedEvidence(t *testing.T) {
	req := baseReq()
	req.EvidenceRevoked = true
	resp := Evaluate(req)
	if resp.Allowed {
		t.Error("expected denial for revoked evidence")
	}
	if resp.Reason != ReasonEvidenceRevoked {
		t.Errorf("expected reason %q, got %q", ReasonEvidenceRevoked, resp.Reason)
	}
}

func TestDenyExpiredEvidence(t *testing.T) {
	req := baseReq()
	req.MaxEvidenceAgeSecs = 60
	req.EvidenceLastSeen = time.Now().Add(-2 * time.Minute)
	resp := Evaluate(req)
	if resp.Allowed {
		t.Error("expected denial for expired evidence")
	}
	if resp.Reason != ReasonEvidenceExpired {
		t.Errorf("expected reason %q, got %q", ReasonEvidenceExpired, resp.Reason)
	}
}

func TestDenyUnverifiedEvidence(t *testing.T) {
	req := baseReq()
	req.EvidenceVerified = false
	resp := Evaluate(req)
	if resp.Allowed {
		t.Error("expected denial for unverified evidence")
	}
	if resp.Reason != ReasonEvidenceNotVerified {
		t.Errorf("expected reason %q, got %q", ReasonEvidenceNotVerified, resp.Reason)
	}
}

func TestDenyRevocationActive(t *testing.T) {
	req := baseReq()
	req.RevocationActive = true
	resp := Evaluate(req)
	if resp.Allowed {
		t.Error("expected denial when revocation is active")
	}
	if resp.Reason != ReasonRevocationActive {
		t.Errorf("expected reason %q, got %q", ReasonRevocationActive, resp.Reason)
	}
}

func TestDenyInvalidTTL(t *testing.T) {
	req := baseReq()
	req.PolicyTTLSeconds = 0
	resp := Evaluate(req)
	if resp.Allowed {
		t.Error("expected denial for zero TTL")
	}
	if resp.Reason != ReasonTTLInvalid {
		t.Errorf("expected reason %q, got %q", ReasonTTLInvalid, resp.Reason)
	}
}

func TestAllowWithValidPlacementToken(t *testing.T) {
	pub, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	tok, err := token.Mint(priv,
		"uid-1", "spec-hash", "sha256:img", "sha256:mdl",
		"node-1", "simulated-kata-qemu-tdx", "ev-hash", "pol-hash",
		5*time.Minute, token.MintOptions{},
	)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	encoded, _ := token.Encode(tok)

	req := baseReq()
	req.PlacementToken = encoded
	req.TokenPublicKey = pub
	req.ModelDigest = "sha256:mdl"
	req.ImageDigest = "sha256:img"

	resp := Evaluate(req)
	if !resp.Allowed {
		t.Fatalf("expected allowed with valid token, got reason=%q", resp.Reason)
	}
}

func TestDenyWrongPodUIDInToken(t *testing.T) {
	pub, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	tok, _ := token.Mint(priv,
		"uid-correct", "spec-hash", "sha256:img", "sha256:mdl",
		"node-1", "simulated-kata-qemu-tdx", "ev-hash", "pol-hash",
		5*time.Minute, token.MintOptions{},
	)
	encoded, _ := token.Encode(tok)

	req := baseReq()
	req.PodUID = "uid-wrong"
	req.PlacementToken = encoded
	req.TokenPublicKey = pub

	resp := Evaluate(req)
	if resp.Allowed {
		t.Error("expected denial for pod UID mismatch in token")
	}
	if resp.Reason != ReasonPodUIDMismatch {
		t.Errorf("expected reason %q, got %q", ReasonPodUIDMismatch, resp.Reason)
	}
}

func TestDenyExpiredToken(t *testing.T) {
	pub, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	tok, _ := token.Mint(priv,
		"uid-1", "spec-hash", "sha256:img", "sha256:mdl",
		"node-1", "simulated-kata-qemu-tdx", "ev-hash", "pol-hash",
		-1*time.Second, token.MintOptions{},
	)
	encoded, _ := token.Encode(tok)

	req := baseReq()
	req.PlacementToken = encoded
	req.TokenPublicKey = pub

	resp := Evaluate(req)
	if resp.Allowed {
		t.Error("expected denial for expired token")
	}
	if resp.Reason != ReasonTokenExpired {
		t.Errorf("expected reason %q, got %q", ReasonTokenExpired, resp.Reason)
	}
}

func TestDenyModelDigestMismatch(t *testing.T) {
	pub, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	tok, _ := token.Mint(priv,
		"uid-1", "spec-hash", "sha256:img", "sha256:model-A",
		"node-1", "simulated-kata-qemu-tdx", "ev-hash", "pol-hash",
		5*time.Minute, token.MintOptions{},
	)
	encoded, _ := token.Encode(tok)

	req := baseReq()
	req.PlacementToken = encoded
	req.TokenPublicKey = pub
	req.ModelDigest = "sha256:model-B" // different

	resp := Evaluate(req)
	if resp.Allowed {
		t.Error("expected denial for model digest mismatch")
	}
	if resp.Reason != ReasonModelDigestMismatch {
		t.Errorf("expected reason %q, got %q", ReasonModelDigestMismatch, resp.Reason)
	}
}
