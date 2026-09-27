package token_test

import (
	"testing"
	"time"

	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
	"github.com/imperium/ai-sovereign-finops-operator/pkg/token"
)

func TestMintAndVerify(t *testing.T) {
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}

	tok, err := token.Mint(priv,
		"pod-uid-abc", "spec-hash-1", "sha256:imageabc", "sha256:modelabc",
		"node-1", "simulated-kata-qemu-tdx", "evidence-hash-1", "policy-hash-1",
		5*time.Minute, token.MintOptions{},
	)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	if err := token.Verify(pub, tok); err != nil {
		t.Errorf("Verify valid token: %v", err)
	}
}

func TestVerifyExpiredToken(t *testing.T) {
	pub, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	tok, err := token.Mint(priv,
		"pod-uid-exp", "spec-hash-2", "sha256:img", "sha256:mdl",
		"node-2", "simulated-kata-qemu-tdx", "ev-hash", "pol-hash",
		-1*time.Second, token.MintOptions{},
	)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	if err := token.Verify(pub, tok); err == nil {
		t.Error("expected error for expired token, got nil")
	}
}

func TestVerifyWrongPubKey(t *testing.T) {
	_, priv, _ := platformcrypto.GenerateEd25519KeyPair()
	pub2, _, _ := platformcrypto.GenerateEd25519KeyPair()

	tok, err := token.Mint(priv,
		"pod-uid-x", "spec-hash-3", "sha256:img", "sha256:mdl",
		"node-3", "simulated-kata-qemu-tdx", "ev-hash", "pol-hash",
		5*time.Minute, token.MintOptions{},
	)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	if err := token.Verify(pub2, tok); err == nil {
		t.Error("expected signature mismatch error, got nil")
	}
}

func TestVerifyForPodMismatch(t *testing.T) {
	pub, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	tok, err := token.Mint(priv,
		"pod-uid-correct", "spec-hash", "sha256:img", "sha256:mdl",
		"node-1", "simulated-kata-qemu-tdx", "ev-hash", "pol-hash",
		5*time.Minute, token.MintOptions{},
	)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	if err := token.VerifyForPod(pub, tok, "pod-uid-wrong", "", "", "", ""); err == nil {
		t.Error("expected pod UID mismatch error, got nil")
	}

	if err := token.VerifyForPod(pub, tok, "pod-uid-correct", "", "", "", ""); err != nil {
		t.Errorf("VerifyForPod with correct podUID: %v", err)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	pub, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	tok, _ := token.Mint(priv,
		"pod-uid-rt", "spec-hash", "sha256:img", "sha256:mdl",
		"node-1", "simulated-kata-qemu-tdx", "ev-hash", "pol-hash",
		5*time.Minute, token.MintOptions{GPUIdentity: "gpu-0", KeyIdentifier: "key-1"},
	)

	encoded, err := token.Encode(tok)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := token.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if err := token.Verify(pub, decoded); err != nil {
		t.Errorf("Verify after round-trip: %v", err)
	}

	if decoded.Payload.GPUIdentity != "gpu-0" {
		t.Errorf("GPUIdentity not preserved: got %q", decoded.Payload.GPUIdentity)
	}
}

func TestMintRequiresPodUID(t *testing.T) {
	_, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	_, err := token.Mint(priv, "", "hash", "img", "mdl", "node", "rc", "ev", "pol", time.Minute, token.MintOptions{})
	if err == nil {
		t.Error("expected error for empty podUID, got nil")
	}
}
