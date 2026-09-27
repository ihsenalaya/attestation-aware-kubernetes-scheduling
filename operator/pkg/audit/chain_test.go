package audit_test

import (
	"testing"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/pkg/audit"
	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
)

func sampleEntry(event audit.EventType, pod string) audit.AuditEntry {
	return audit.AuditEntry{
		Timestamp:     time.Now().UTC(),
		EventType:     event,
		Namespace:     "test-ns",
		PodName:       pod,
		PodUID:        "uid-" + pod,
		NodeName:      "node-1",
		RuntimeClass:  "simulated-kata-qemu-tdx",
		PolicyHash:    "pol-hash",
		PodSpecHash:   "spec-hash",
		Decision:      "allow",
		ComponentName: "test",
	}
}

func TestChainFlushAndVerify(t *testing.T) {
	c := audit.NewChain(3)

	for _, ev := range []audit.EventType{
		audit.EventPodAdmitted,
		audit.EventAttestationVerified,
		audit.EventNodeSelected,
	} {
		if err := c.Append(sampleEntry(ev, "pod-1")); err != nil {
			t.Fatalf("Append %q: %v", ev, err)
		}
	}
	// Batch of 3 should have been auto-flushed
	records := c.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 sealed batch, got %d", len(records))
	}
	if anomalies := c.Verify(); len(anomalies) > 0 {
		t.Fatalf("chain verify: unexpected anomalies %+v", anomalies)
	}
}

func TestChainMultipleBatches(t *testing.T) {
	c := audit.NewChain(2)

	for i := 0; i < 6; i++ {
		if err := c.Append(sampleEntry(audit.EventPodAdmitted, "pod")); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := c.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	records := c.Records()
	if len(records) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(records))
	}
	if anomalies := c.Verify(); len(anomalies) > 0 {
		t.Fatalf("chain verify: unexpected anomalies %+v", anomalies)
	}
}

func TestTamperDetection(t *testing.T) {
	c := audit.NewChain(2)

	for i := 0; i < 4; i++ {
		_ = c.Append(sampleEntry(audit.EventPodAdmitted, "pod"))
	}

	records := c.Records()
	if len(records) < 2 {
		t.Fatal("need at least 2 batches for tamper test")
	}

	// Tamper with record[0] hash directly
	tampered := make([]audit.BatchRecord, len(records))
	copy(tampered, records)
	tampered[0].Hash = "tampered-hash"

	anomalies := audit.VerifyChain(tampered)
	if len(anomalies) == 0 {
		t.Fatal("expected tamper to be detected, got no anomalies")
	}
	found := false
	for _, a := range anomalies {
		if a.Kind == audit.AnomalyHashMismatch || a.Kind == audit.AnomalyPreviousHashMismatch {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected hash mismatch anomaly, got %+v", anomalies)
	}
}

func TestCheckpointSignAndVerify(t *testing.T) {
	pub, priv, err := platformcrypto.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}

	c := audit.NewChain(1)
	_ = c.Append(sampleEntry(audit.EventPodAdmitted, "pod-1"))

	cp, err := audit.CreateCheckpoint(c, priv, "test-signer")
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	if err := audit.VerifyCheckpoint(cp, pub); err != nil {
		t.Fatalf("VerifyCheckpoint: %v", err)
	}
}

func TestCheckpointWrongKey(t *testing.T) {
	_, priv, _ := platformcrypto.GenerateEd25519KeyPair()
	pub2, _, _ := platformcrypto.GenerateEd25519KeyPair()

	c := audit.NewChain(1)
	_ = c.Append(sampleEntry(audit.EventPodAdmitted, "pod-1"))

	cp, _ := audit.CreateCheckpoint(c, priv, "signer")

	if err := audit.VerifyCheckpoint(cp, pub2); err == nil {
		t.Error("expected signature mismatch, got nil")
	}
}

func TestRewriteBeforeCheckpointDetected(t *testing.T) {
	pub, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	c := audit.NewChain(1)
	_ = c.Append(sampleEntry(audit.EventPodAdmitted, "pod-1"))
	_ = c.Append(sampleEntry(audit.EventKeyReleased, "pod-1"))

	// Checkpoint after first two records
	cp, err := audit.CreateCheckpoint(c, priv, "signer")
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	if err := audit.VerifyCheckpoint(cp, pub); err != nil {
		t.Fatalf("VerifyCheckpoint: %v", err)
	}

	// Tamper a record before the checkpoint
	records := c.Records()
	tampered := make([]audit.BatchRecord, len(records))
	copy(tampered, records)
	tampered[0].Hash = "evil-hash"

	anomalies := audit.VerifyChainAgainstCheckpoint(tampered, cp)
	found := false
	for _, a := range anomalies {
		if a.Kind == audit.AnomalyRewriteBeforeCheckpoint || a.Kind == audit.AnomalyHashMismatch || a.Kind == audit.AnomalyPreviousHashMismatch {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected rewrite anomaly, got %+v", anomalies)
	}
}

func TestCheckpointRoundTrip(t *testing.T) {
	_, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	c := audit.NewChain(1)
	_ = c.Append(sampleEntry(audit.EventPodAdmitted, "pod-1"))

	cp, _ := audit.CreateCheckpoint(c, priv, "signer")

	encoded, err := audit.EncodeCheckpoint(cp)
	if err != nil {
		t.Fatalf("EncodeCheckpoint: %v", err)
	}
	decoded, err := audit.DecodeCheckpoint(encoded)
	if err != nil {
		t.Fatalf("DecodeCheckpoint: %v", err)
	}
	if decoded.RecordHash != cp.RecordHash {
		t.Errorf("RecordHash mismatch: got %q want %q", decoded.RecordHash, cp.RecordHash)
	}
}

func TestMemoryAnchorBackend(t *testing.T) {
	_, priv, _ := platformcrypto.GenerateEd25519KeyPair()

	c := audit.NewChain(1)
	_ = c.Append(sampleEntry(audit.EventPodAdmitted, "pod-1"))

	cp, _ := audit.CreateCheckpoint(c, priv, "signer")

	backend := &audit.MemoryAnchorBackend{}
	if err := backend.Anchor(cp); err != nil {
		t.Fatalf("Anchor: %v", err)
	}
	latest, err := backend.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.RecordHash != cp.RecordHash {
		t.Errorf("Latest RecordHash mismatch")
	}
}
