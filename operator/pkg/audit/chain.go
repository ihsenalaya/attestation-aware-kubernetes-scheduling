// Package audit implements the append-only audit chain with Ed25519 checkpoints.
//
// Threat model note (honest):
// The immutability enforced by the webhook validation only resists non-admin
// actors. A cluster-admin or direct etcd access can bypass it. The checkpoint
// mechanism bounds the falsification window: once a checkpoint is anchored,
// any rewrite of history prior to that checkpoint is detectable.
//
// Chain structure:
//
//	Record[0] → Record[1] → ... → Record[N]
//	  hash(payload0) = H0
//	  hash(H_{i-1} || payload_i) = H_i   (chained hash)
//
// A batch record contains N events; the global chain links batches.
package audit

import (
	"encoding/json"
	"fmt"
	"time"

	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
)

// EventType is a machine-readable audit event category.
type EventType string

const (
	EventPolicyCreated        EventType = "policy_created"
	EventPodAdmitted          EventType = "pod_admitted"
	EventAttestationVerified  EventType = "attestation_verified"
	EventNodeSelected         EventType = "node_selected"
	EventGPUAllocated         EventType = "gpu_allocated"
	EventPodBound             EventType = "pod_bound"
	EventKeyReleaseRequested  EventType = "key_release_requested"
	EventKeyReleased          EventType = "key_released"
	EventKeyRefused           EventType = "key_refused"
	EventAttestationExpired   EventType = "attestation_expired"
	EventRevocationTriggered  EventType = "revocation_triggered"
	EventPodRescheduled       EventType = "pod_rescheduled"
	EventAuditAnomalyDetected EventType = "audit_anomaly_detected"
)

// AuditEntry is a single event stored in an audit record.
type AuditEntry struct {
	Timestamp     time.Time `json:"timestamp"`
	EventType     EventType `json:"event_type"`
	Namespace     string    `json:"namespace"`
	PodName       string    `json:"pod_name"`
	PodUID        string    `json:"pod_uid"`
	NodeName      string    `json:"node_name"`
	RuntimeClass  string    `json:"runtime_class"`
	GPUIdentity   string    `json:"gpu_identity,omitempty"`
	PolicyHash    string    `json:"policy_hash"`
	PodSpecHash   string    `json:"pod_spec_hash"`
	ImageDigest   string    `json:"image_digest,omitempty"`
	ModelDigest   string    `json:"model_digest,omitempty"`
	EvidenceHash  string    `json:"evidence_hash,omitempty"`
	Decision      string    `json:"decision"`
	Reason        string    `json:"reason,omitempty"`
	ComponentName string    `json:"component_name"`
}

// BatchRecord groups N AuditEntry items into a single chain node.
// The PreviousHash links to the prior BatchRecord's Hash.
// The Hash covers PreviousHash + all entries in this batch.
type BatchRecord struct {
	Index        int64        `json:"index"`
	PreviousHash string       `json:"previous_hash"`
	Entries      []AuditEntry `json:"entries"`
	Hash         string       `json:"hash"`
}

// computeBatchHash computes the chain hash for a BatchRecord.
// It covers: index + previous_hash + all serialised entries.
func computeBatchHash(index int64, previousHash string, entries []AuditEntry) (string, error) {
	type hashInput struct {
		Index        int64        `json:"index"`
		PreviousHash string       `json:"previous_hash"`
		Entries      []AuditEntry `json:"entries"`
	}
	data, err := json.Marshal(hashInput{Index: index, PreviousHash: previousHash, Entries: entries})
	if err != nil {
		return "", fmt.Errorf("marshal batch hash input: %w", err)
	}
	return platformcrypto.SHA256Hex(data), nil
}

// AnomalyKind classifies detected chain integrity anomalies.
type AnomalyKind string

const (
	AnomalyHashMismatch            AnomalyKind = "HASH_MISMATCH"
	AnomalyPreviousHashMismatch    AnomalyKind = "PREVIOUS_HASH_MISMATCH"
	AnomalyRewriteBeforeCheckpoint AnomalyKind = "REWRITE_BEFORE_CHECKPOINT"
	AnomalyMissingEntry            AnomalyKind = "MISSING_ENTRY"
)

// Anomaly describes a detected chain integrity violation.
type Anomaly struct {
	RecordIndex int64       `json:"record_index"`
	Kind        AnomalyKind `json:"kind"`
	Detail      string      `json:"detail"`
}

// VerifyChain verifies the integrity of the chain of BatchRecords from index 0 to the end.
// It returns any detected anomalies. An empty slice means the chain is intact.
func VerifyChain(records []BatchRecord) []Anomaly {
	var anomalies []Anomaly
	prevHash := ""
	for i, rec := range records {
		// Verify the previous hash link
		if rec.PreviousHash != prevHash {
			anomalies = append(anomalies, Anomaly{
				RecordIndex: rec.Index,
				Kind:        AnomalyPreviousHashMismatch,
				Detail:      fmt.Sprintf("record[%d]: previous_hash=%q expected=%q", i, rec.PreviousHash, prevHash),
			})
		}

		// Recompute the hash and compare
		expected, err := computeBatchHash(rec.Index, rec.PreviousHash, rec.Entries)
		if err != nil || expected != rec.Hash {
			detail := fmt.Sprintf("record[%d]: stored hash=%q expected=%q", i, rec.Hash, expected)
			if err != nil {
				detail += ": " + err.Error()
			}
			anomalies = append(anomalies, Anomaly{
				RecordIndex: rec.Index,
				Kind:        AnomalyHashMismatch,
				Detail:      detail,
			})
		}

		prevHash = rec.Hash
	}
	return anomalies
}

// VerifyChainAgainstCheckpoint checks whether the head of the chain matches the checkpoint,
// and detects any rewrite of history prior to the checkpoint anchor point.
func VerifyChainAgainstCheckpoint(records []BatchRecord, cp Checkpoint) []Anomaly {
	var anomalies []Anomaly

	// First verify the chain internally
	anomalies = append(anomalies, VerifyChain(records)...)

	// Find the record that matches the checkpoint
	checkpointFound := false
	for _, rec := range records {
		if rec.Index == cp.RecordIndex {
			checkpointFound = true
			if rec.Hash != cp.RecordHash {
				anomalies = append(anomalies, Anomaly{
					RecordIndex: rec.Index,
					Kind:        AnomalyRewriteBeforeCheckpoint,
					Detail: fmt.Sprintf(
						"record[%d] hash=%q does not match checkpoint anchor hash=%q — history rewrite detected",
						rec.Index, rec.Hash, cp.RecordHash,
					),
				})
			}
			break
		}
	}

	if !checkpointFound && len(records) > 0 {
		anomalies = append(anomalies, Anomaly{
			RecordIndex: cp.RecordIndex,
			Kind:        AnomalyMissingEntry,
			Detail:      fmt.Sprintf("checkpoint references record index %d but it is absent from the chain", cp.RecordIndex),
		})
	}

	return anomalies
}

// Chain is an in-memory append-only audit chain for kind/dev usage.
// It supports batching: events are buffered until Flush() is called.
type Chain struct {
	records   []BatchRecord
	pending   []AuditEntry
	batchSize int
}

// NewChain creates an empty audit chain with the given batch size.
// If batchSize <= 0, a default of 10 is used.
func NewChain(batchSize int) *Chain {
	if batchSize <= 0 {
		batchSize = 10
	}
	return &Chain{batchSize: batchSize}
}

// Append adds an audit entry. When the pending buffer reaches batchSize, it
// is automatically flushed into a new BatchRecord.
func (c *Chain) Append(entry AuditEntry) error {
	c.pending = append(c.pending, entry)
	if len(c.pending) >= c.batchSize {
		return c.Flush()
	}
	return nil
}

// Flush seals the current pending buffer into a new BatchRecord.
// It is a no-op if there are no pending entries.
func (c *Chain) Flush() error {
	if len(c.pending) == 0 {
		return nil
	}
	prevHash := ""
	if len(c.records) > 0 {
		prevHash = c.records[len(c.records)-1].Hash
	}
	idx := int64(len(c.records))
	h, err := computeBatchHash(idx, prevHash, c.pending)
	if err != nil {
		return fmt.Errorf("compute batch hash: %w", err)
	}
	c.records = append(c.records, BatchRecord{
		Index:        idx,
		PreviousHash: prevHash,
		Entries:      c.pending,
		Hash:         h,
	})
	c.pending = nil
	return nil
}

// Records returns all sealed BatchRecords.
func (c *Chain) Records() []BatchRecord {
	out := make([]BatchRecord, len(c.records))
	copy(out, c.records)
	return out
}

// HeadHash returns the hash of the last sealed record, or "" if no records exist.
func (c *Chain) HeadHash() string {
	if len(c.records) == 0 {
		return ""
	}
	return c.records[len(c.records)-1].Hash
}

// HeadIndex returns the index of the last sealed record, or -1 if no records exist.
func (c *Chain) HeadIndex() int64 {
	if len(c.records) == 0 {
		return -1
	}
	return c.records[len(c.records)-1].Index
}

// Verify verifies the integrity of the entire chain.
func (c *Chain) Verify() []Anomaly {
	return VerifyChain(c.records)
}
