package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// GenerateEd25519KeyPair generates a new Ed25519 key pair.
func GenerateEd25519KeyPair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate ed25519 key pair: %w", err)
	}
	return pub, priv, nil
}

// Ed25519Sign signs data with the given private key and returns the signature as a hex string.
func Ed25519Sign(priv ed25519.PrivateKey, data []byte) string {
	sig := ed25519.Sign(priv, data)
	return hex.EncodeToString(sig)
}

// Ed25519Verify verifies a hex-encoded Ed25519 signature against data and the given public key.
func Ed25519Verify(pub ed25519.PublicKey, data []byte, sigHex string) (bool, error) {
	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		return false, fmt.Errorf("decode signature hex: %w", err)
	}
	return ed25519.Verify(pub, data, sig), nil
}

// PrivKeyToHex encodes an Ed25519 private key as a hex string (seed only — 32 bytes).
func PrivKeyToHex(priv ed25519.PrivateKey) string {
	return hex.EncodeToString(priv.Seed())
}

// PrivKeyFromHex reconstructs an Ed25519 private key from its hex-encoded seed.
func PrivKeyFromHex(h string) (ed25519.PrivateKey, error) {
	seed, err := hex.DecodeString(h)
	if err != nil {
		return nil, fmt.Errorf("decode private key hex: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("invalid ed25519 seed length: got %d, want %d", len(seed), ed25519.SeedSize)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

// PubKeyToHex encodes an Ed25519 public key as a hex string.
func PubKeyToHex(pub ed25519.PublicKey) string {
	return hex.EncodeToString(pub)
}

// PubKeyFromHex reconstructs an Ed25519 public key from its hex-encoded form.
func PubKeyFromHex(h string) (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(h)
	if err != nil {
		return nil, fmt.Errorf("decode public key hex: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid ed25519 public key length: got %d, want %d", len(b), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(b), nil
}
