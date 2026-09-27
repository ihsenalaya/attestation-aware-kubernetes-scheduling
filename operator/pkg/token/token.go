// Package token implements verifiable placement tokens for attestation-aware scheduling.
// Tokens are Ed25519-signed JSON payloads that bind a pod to a node under a specific
// attestation evidence set and policy.
package token

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
)

// Payload holds all verifiable fields of a placement token.
type Payload struct {
	PodUID        string `json:"pod_uid"`
	PodSpecHash   string `json:"pod_spec_hash"`
	ImageDigest   string `json:"image_digest"`
	ModelDigest   string `json:"model_digest"`
	NodeIdentity  string `json:"node_identity"`
	GPUIdentity   string `json:"gpu_identity,omitempty"`
	RuntimeClass  string `json:"runtime_class"`
	EvidenceHash  string `json:"evidence_hash"`
	PolicyHash    string `json:"policy_hash"`
	KeyIdentifier string `json:"key_identifier,omitempty"`
	IssuedAt      int64  `json:"issued_at"`
	ExpiresAt     int64  `json:"expires_at"`
}

// Token is a signed placement token.
type Token struct {
	Payload   Payload `json:"payload"`
	Signature string  `json:"signature"`
}

// MintOptions contains optional fields for minting a token.
type MintOptions struct {
	GPUIdentity   string
	KeyIdentifier string
}

// Mint creates a new placement token signed with the given Ed25519 private key.
// TTL is the token validity duration from now.
func Mint(
	priv ed25519.PrivateKey,
	podUID, podSpecHash, imageDigest, modelDigest string,
	nodeIdentity, runtimeClass, evidenceHash, policyHash string,
	ttl time.Duration,
	opts MintOptions,
) (Token, error) {
	if priv == nil {
		return Token{}, fmt.Errorf("private key is nil")
	}
	if strings.TrimSpace(podUID) == "" {
		return Token{}, fmt.Errorf("podUID is required")
	}
	now := time.Now().UTC()
	payload := Payload{
		PodUID:        podUID,
		PodSpecHash:   podSpecHash,
		ImageDigest:   imageDigest,
		ModelDigest:   modelDigest,
		NodeIdentity:  nodeIdentity,
		GPUIdentity:   opts.GPUIdentity,
		RuntimeClass:  runtimeClass,
		EvidenceHash:  evidenceHash,
		PolicyHash:    policyHash,
		KeyIdentifier: opts.KeyIdentifier,
		IssuedAt:      now.Unix(),
		ExpiresAt:     now.Add(ttl).Unix(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return Token{}, fmt.Errorf("marshal token payload: %w", err)
	}
	sig := platformcrypto.Ed25519Sign(priv, data)
	return Token{Payload: payload, Signature: sig}, nil
}

// Verify checks the token signature and expiry. It returns an error describing
// why verification failed, or nil if the token is valid.
func Verify(pub ed25519.PublicKey, tok Token) error {
	if pub == nil {
		return fmt.Errorf("public key is nil")
	}
	if time.Now().UTC().Unix() > tok.Payload.ExpiresAt {
		return fmt.Errorf("token expired")
	}
	data, err := json.Marshal(tok.Payload)
	if err != nil {
		return fmt.Errorf("marshal token payload for verification: %w", err)
	}
	ok, err := platformcrypto.Ed25519Verify(pub, data, tok.Signature)
	if err != nil {
		return fmt.Errorf("verify token signature: %w", err)
	}
	if !ok {
		return fmt.Errorf("token signature invalid")
	}
	return nil
}

// VerifyForPod checks the token fields against a specific pod/node/evidence context.
func VerifyForPod(pub ed25519.PublicKey, tok Token, podUID, podSpecHash, nodeIdentity, evidenceHash, policyHash string) error {
	if err := Verify(pub, tok); err != nil {
		return err
	}
	if tok.Payload.PodUID != podUID {
		return fmt.Errorf("pod UID mismatch: token=%q request=%q", tok.Payload.PodUID, podUID)
	}
	if podSpecHash != "" && tok.Payload.PodSpecHash != podSpecHash {
		return fmt.Errorf("pod spec hash mismatch")
	}
	if nodeIdentity != "" && tok.Payload.NodeIdentity != nodeIdentity {
		return fmt.Errorf("node identity mismatch")
	}
	if evidenceHash != "" && tok.Payload.EvidenceHash != evidenceHash {
		return fmt.Errorf("evidence hash mismatch")
	}
	if policyHash != "" && tok.Payload.PolicyHash != policyHash {
		return fmt.Errorf("policy hash mismatch")
	}
	return nil
}

// Encode serialises a token to JSON.
func Encode(tok Token) (string, error) {
	b, err := json.Marshal(tok)
	if err != nil {
		return "", fmt.Errorf("encode token: %w", err)
	}
	return string(b), nil
}

// Decode parses a JSON-encoded token.
func Decode(raw string) (Token, error) {
	var tok Token
	if err := json.Unmarshal([]byte(raw), &tok); err != nil {
		return Token{}, fmt.Errorf("decode token: %w", err)
	}
	return tok, nil
}
