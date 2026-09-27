package audit

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"time"

	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
)

// Checkpoint is a signed anchor of the audit chain at a specific record index.
// Once anchored externally (file, ConfigMap, blob), any rewrite of history
// prior to this checkpoint is detectable by VerifyChainAgainstCheckpoint.
type Checkpoint struct {
	RecordIndex int64     `json:"record_index"`
	RecordHash  string    `json:"record_hash"`
	Timestamp   time.Time `json:"timestamp"`
	SignerID    string    `json:"signer_id"`
	Signature   string    `json:"signature"`
}

// checkpointPayload is the canonical form signed by the Ed25519 key.
type checkpointPayload struct {
	RecordIndex int64     `json:"record_index"`
	RecordHash  string    `json:"record_hash"`
	Timestamp   time.Time `json:"timestamp"`
	SignerID    string    `json:"signer_id"`
}

// CreateCheckpoint signs a checkpoint for the current chain head.
func CreateCheckpoint(c *Chain, priv ed25519.PrivateKey, signerID string) (Checkpoint, error) {
	if c.HeadIndex() < 0 {
		return Checkpoint{}, fmt.Errorf("chain has no sealed records; flush pending entries first")
	}
	if priv == nil {
		return Checkpoint{}, fmt.Errorf("signing key is nil")
	}

	now := time.Now().UTC()
	payload := checkpointPayload{
		RecordIndex: c.HeadIndex(),
		RecordHash:  c.HeadHash(),
		Timestamp:   now,
		SignerID:    signerID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("marshal checkpoint payload: %w", err)
	}

	sig := platformcrypto.Ed25519Sign(priv, data)
	return Checkpoint{
		RecordIndex: payload.RecordIndex,
		RecordHash:  payload.RecordHash,
		Timestamp:   now,
		SignerID:    signerID,
		Signature:   sig,
	}, nil
}

// VerifyCheckpoint verifies the checkpoint signature against the given public key.
func VerifyCheckpoint(cp Checkpoint, pub ed25519.PublicKey) error {
	if pub == nil {
		return fmt.Errorf("public key is nil")
	}
	payload := checkpointPayload{
		RecordIndex: cp.RecordIndex,
		RecordHash:  cp.RecordHash,
		Timestamp:   cp.Timestamp,
		SignerID:    cp.SignerID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal checkpoint payload for verification: %w", err)
	}
	ok, err := platformcrypto.Ed25519Verify(pub, data, cp.Signature)
	if err != nil {
		return fmt.Errorf("verify checkpoint signature: %w", err)
	}
	if !ok {
		return fmt.Errorf("checkpoint signature invalid")
	}
	return nil
}

// EncodeCheckpoint serialises a checkpoint to JSON.
func EncodeCheckpoint(cp Checkpoint) (string, error) {
	b, err := json.Marshal(cp)
	if err != nil {
		return "", fmt.Errorf("encode checkpoint: %w", err)
	}
	return string(b), nil
}

// DecodeCheckpoint parses a JSON-encoded checkpoint.
func DecodeCheckpoint(raw string) (Checkpoint, error) {
	var cp Checkpoint
	if err := json.Unmarshal([]byte(raw), &cp); err != nil {
		return Checkpoint{}, fmt.Errorf("decode checkpoint: %w", err)
	}
	return cp, nil
}
