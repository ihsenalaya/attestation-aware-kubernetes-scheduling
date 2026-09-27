// Package govaraudit defines the deterministic per-request transition chain
// persisted by the PostgreSQL GOV-AR ledger. It contains no database or wall
// clock access so exported rows can be verified independently.
package govaraudit

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ActorClass string

const (
	ActorAdmission   ActorClass = "ADMISSION"
	ActorGateway     ActorClass = "GATEWAY"
	ActorReconciler  ActorClass = "RECONCILER"
	ActorCorrection  ActorClass = "CORRECTION"
	ActorAuthority   ActorClass = "AUTHORITY"
	ActorRegistry    ActorClass = "REGISTRY"
	ActorCalibration ActorClass = "CALIBRATION"
)

type Entry struct {
	TenantID              string     `json:"tenant_id"`
	Sequence              int64      `json:"sequence"`
	EventID               string     `json:"event_id"`
	EventKind             string     `json:"event_kind"`
	PayloadSHA256         string     `json:"payload_sha256"`
	RequestID             string     `json:"request_id,omitempty"`
	WorkloadUID           string     `json:"workload_uid,omitempty"`
	ProviderAttemptID     string     `json:"provider_attempt_id,omitempty"`
	ActorClass            ActorClass `json:"actor_class"`
	Reason                string     `json:"reason"`
	BeforeStateSHA256     string     `json:"before_state_sha256,omitempty"`
	AfterStateSHA256      string     `json:"after_state_sha256"`
	PolicyVersion         string     `json:"policy_version"`
	PricingSnapshotSHA256 string     `json:"pricing_snapshot_sha256"`
	RouteSnapshotSHA256   string     `json:"route_snapshot_sha256,omitempty"`
	CapEvidenceSHA256     string     `json:"cap_evidence_sha256,omitempty"`
	CohortSHA256          string     `json:"cohort_sha256,omitempty"`
	SoftwareSHA256        string     `json:"software_sha256,omitempty"`
	CalibrationSHA256     string     `json:"calibration_sha256,omitempty"`
	PreviousEntrySHA256   string     `json:"previous_entry_sha256,omitempty"`
	CommittedAt           time.Time  `json:"committed_at"`
	EntrySHA256           string     `json:"entry_sha256"`
}

func BuildEntry(entry Entry) (Entry, error) {
	entry.CommittedAt = entry.CommittedAt.UTC()
	entry.EntrySHA256 = ""
	if err := validateEntry(entry, false); err != nil {
		return Entry{}, err
	}
	entry.EntrySHA256 = hashFields(
		"govar-tenant-audit-v2", entry.TenantID, fmt.Sprint(entry.Sequence), entry.EventID,
		entry.EventKind, entry.PayloadSHA256, entry.RequestID, entry.WorkloadUID, entry.ProviderAttemptID,
		string(entry.ActorClass), entry.Reason, entry.BeforeStateSHA256, entry.AfterStateSHA256,
		entry.PolicyVersion, entry.PricingSnapshotSHA256, entry.RouteSnapshotSHA256,
		entry.CapEvidenceSHA256, entry.CohortSHA256, entry.SoftwareSHA256,
		entry.CalibrationSHA256, entry.PreviousEntrySHA256,
		entry.CommittedAt.Format(time.RFC3339Nano),
	)
	return entry, nil
}

func Verify(entries []Entry) error {
	if len(entries) == 0 {
		return errors.New("audit chain is empty")
	}
	var previous string
	tenant := entries[0].TenantID
	eventIDs := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		if entry.TenantID != tenant {
			return errors.New("audit verification input contains multiple tenants")
		}
		if _, exists := eventIDs[entry.EventID]; exists {
			return errors.New("audit event ID is not unique")
		}
		eventIDs[entry.EventID] = struct{}{}
		if entry.Sequence != int64(index+1) {
			return fmt.Errorf("audit sequence %d is not contiguous", entry.Sequence)
		}
		if entry.PreviousEntrySHA256 != previous {
			return fmt.Errorf("audit sequence %d has wrong predecessor", entry.Sequence)
		}
		if err := validateEntry(entry, true); err != nil {
			return fmt.Errorf("audit sequence %d: %w", entry.Sequence, err)
		}
		rebuilt, err := BuildEntry(entry)
		if err != nil {
			return err
		}
		if rebuilt.EntrySHA256 != entry.EntrySHA256 {
			return fmt.Errorf("audit sequence %d hash mismatch", entry.Sequence)
		}
		previous = entry.EntrySHA256
	}
	return nil
}

func validateEntry(entry Entry, requireHash bool) error {
	if strings.TrimSpace(entry.TenantID) == "" || entry.Sequence <= 0 || strings.TrimSpace(entry.EventID) == "" || strings.TrimSpace(entry.EventKind) == "" {
		return errors.New("tenant, sequence, event ID, and event kind are required")
	}
	if !validKind(entry.EventKind) || len(entry.Reason) == 0 || len(entry.Reason) > 64 || !boundedToken(entry.Reason, false) {
		return errors.New("event kind or reason is outside its closed bounded representation")
	}
	if !validActor(entry.ActorClass) {
		return errors.New("actor class is not closed")
	}
	if entry.CommittedAt.IsZero() || entry.CommittedAt.Location() != time.UTC {
		return errors.New("committed_at must be a nonzero UTC time")
	}
	for name, value := range map[string]string{
		"after state": entry.AfterStateSHA256,
		"payload":     entry.PayloadSHA256,
	} {
		if !isSHA256(value) {
			return fmt.Errorf("%s digest is invalid", name)
		}
	}
	for name, value := range map[string]string{
		"before state":     entry.BeforeStateSHA256,
		"previous entry":   entry.PreviousEntrySHA256,
		"route snapshot":   entry.RouteSnapshotSHA256,
		"cap evidence":     entry.CapEvidenceSHA256,
		"cohort":           entry.CohortSHA256,
		"software":         entry.SoftwareSHA256,
		"calibration":      entry.CalibrationSHA256,
		"pricing snapshot": entry.PricingSnapshotSHA256,
	} {
		if value != "" && !isSHA256(value) {
			return fmt.Errorf("%s digest is invalid", name)
		}
	}
	if requireHash && !isSHA256(entry.EntrySHA256) {
		return errors.New("entry digest is invalid")
	}
	return nil
}

func validKind(kind string) bool {
	switch kind {
	case "RESERVE", "DISPATCH", "SETTLE", "CANCEL", "EXPIRY", "ROLLOVER",
		"BUDGET_ADJUSTMENT", "COHORT_REGISTRATION", "CALIBRATION_PUBLICATION",
		"DRIFT_CHANGE", "RECONCILIATION":
		return true
	default:
		return false
	}
}

func validActor(actor ActorClass) bool {
	switch actor {
	case ActorAdmission, ActorGateway, ActorReconciler, ActorCorrection, ActorAuthority, ActorRegistry, ActorCalibration:
		return true
	default:
		return false
	}
}

func boundedToken(value string, upper bool) bool {
	for _, char := range value {
		if upper {
			if (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' {
				return false
			}
			continue
		}
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' && char != '-' && char != ':' {
			return false
		}
	}
	return true
}

func isSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func hashFields(fields ...string) string {
	hash := sha256.New()
	var length [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(field))
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}
