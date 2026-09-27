package govar

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govaraudit"
)

const reserveAuditEventPrefix = "reserve:"

func canonicalAuditDigest(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal audit state: %w", err)
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:]), nil
}

// reservationAuditDigest binds the complete immutable routing and pricing
// snapshot together with every mutable liability field. The representation is
// private so unrelated response fields cannot change the commitment.
func reservationAuditDigest(r Reservation) (string, error) {
	type auditState struct {
		RequestID                     string
		TenantID                      string
		WorkloadUID                   string
		SelectedDeployment            string
		ProviderAttemptID             string
		OutboxID                      string
		OutboxState                   OutboxState
		State                         ReservationState
		ReservedCostMicros            MoneyMicros
		ReservedComponents            any
		PricingSnapshot               any
		PricingSnapshotSHA256         string
		CapEvidenceSHA256             string
		ActualComponents              any
		MissingUsageBases             any
		ComponentBoundExceeded        bool
		ProvisionalCostMicros         MoneyMicros
		BaseActualMicros              MoneyMicros
		SettledEffectMicros           MoneyMicros
		ResidualHoldMicros            MoneyMicros
		UsageVersion                  int64
		Finalized                     bool
		PolicyVersion                 string
		PricingVersion                string
		ReservationMode               string
		RiskLevel                     string
		AllocatedRiskPPB              int64
		InputPriceMicrosPerMillion    int64
		OutputPriceMicrosPerMillion   int64
		AdmissionFingerprint          string
		CandidateSnapshotVersion      string
		RouteSnapshot                 RouteSnapshot
		CohortID                      string
		CohortIndex                   int64
		CohortRegistryDigest          string
		CalibrationArtifactSHA256     string
		SelectedFeedbackBindingSHA256 string
		OriginWindowID                string
		EnforcementWindowID           string
		RolloverGuardMicros           MoneyMicros
		CarryEffectMicros             MoneyMicros
		HistoricalCreditMicros        MoneyMicros
		Carried                       bool
		LastUsageEventID              string
		ProviderRetryPolicy           string
		LastReasonCode                ReasonCode
		LastTransitionEventID         string
		Expiry                        time.Time
	}
	return canonicalAuditDigest(auditState{
		RequestID: r.RequestID, TenantID: r.TenantID, WorkloadUID: r.WorkloadUID,
		SelectedDeployment: r.SelectedDeployment, ProviderAttemptID: r.ProviderAttemptID,
		OutboxID: r.OutboxID, OutboxState: r.OutboxState, State: r.State,
		ReservedCostMicros: r.ReservedCostMicros, ReservedComponents: r.ReservedComponents,
		PricingSnapshot: r.PricingSnapshot, PricingSnapshotSHA256: r.PricingSnapshotSHA256,
		CapEvidenceSHA256: r.CapEvidenceSHA256, ActualComponents: r.ActualComponents,
		MissingUsageBases: r.MissingUsageBases, ComponentBoundExceeded: r.ComponentBoundExceeded,
		ProvisionalCostMicros: r.ProvisionalCostMicros, BaseActualMicros: r.BaseActualMicros,
		SettledEffectMicros: r.SettledEffectMicros, ResidualHoldMicros: r.ResidualHoldMicros,
		UsageVersion: r.UsageVersion, Finalized: r.Finalized, PolicyVersion: r.PolicyVersion,
		PricingVersion: r.PricingVersion, ReservationMode: r.ReservationMode, RiskLevel: r.RiskLevel,
		AllocatedRiskPPB: r.AllocatedRiskPPB, InputPriceMicrosPerMillion: r.InputPriceMicrosPerMillion,
		OutputPriceMicrosPerMillion: r.OutputPriceMicrosPerMillion,
		AdmissionFingerprint:        r.AdmissionFingerprint, CandidateSnapshotVersion: r.CandidateSnapshotVersion,
		RouteSnapshot: r.RouteSnapshot, CohortID: r.CohortID, CohortIndex: r.CohortIndex,
		CohortRegistryDigest: r.CohortRegistryDigest, OriginWindowID: r.OriginWindowID,
		CalibrationArtifactSHA256:     r.CalibrationArtifactSHA256,
		SelectedFeedbackBindingSHA256: r.SelectedFeedbackBindingSHA256,
		EnforcementWindowID:           r.EnforcementWindowID, RolloverGuardMicros: r.RolloverGuardMicros,
		CarryEffectMicros: r.CarryEffectMicros, HistoricalCreditMicros: r.HistoricalCreditMicros,
		Carried: r.Carried, LastUsageEventID: r.LastUsageEventID,
		ProviderRetryPolicy: r.ProviderRetryPolicy, LastReasonCode: r.LastReasonCode,
		LastTransitionEventID: r.LastTransitionEventID, Expiry: r.Expiry.UTC(),
	})
}

func tenantAuditDigest(t *tenantLedger) (string, error) {
	return canonicalAuditDigest(struct {
		BudgetMicros, SettledMicros, ReservedMicros, CarriedAdjustmentMicros MoneyMicros
		ActiveReservations                                                   int
		BudgetIdentity, CurrentWindowID, WindowPeriod                        string
		HistoricalCreditMicros                                               MoneyMicros
	}{t.BudgetMicros, t.SettledMicros, t.ReservedMicros, t.CarriedAdjustmentMicros,
		t.ActiveReservations, t.BudgetIdentity, t.CurrentWindowID, t.WindowPeriod,
		t.HistoricalCreditMicros})
}

// appendAuditTx allocates a monotone tenant sequence while holding the tenant's
// dedicated sequence row FOR UPDATE. All callers invoke it inside the same
// serializable transaction as the state mutation.
func appendAuditTx(ctx context.Context, tx pgx.Tx, entry govaraudit.Entry) error {
	if _, err := tx.Exec(ctx, `INSERT INTO govar_audit_tenant_sequences(tenant_id,next_sequence) VALUES($1,1) ON CONFLICT(tenant_id) DO NOTHING`, entry.TenantID); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, `SELECT next_sequence FROM govar_audit_tenant_sequences WHERE tenant_id=$1 FOR UPDATE`, entry.TenantID).Scan(&entry.Sequence); err != nil {
		return err
	}
	if entry.Sequence > 1 {
		if err := tx.QueryRow(ctx, `SELECT entry_sha256 FROM govar_audit_events WHERE tenant_id=$1 AND sequence=$2`, entry.TenantID, entry.Sequence-1).Scan(&entry.PreviousEntrySHA256); err != nil {
			return err
		}
	}
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&entry.CommittedAt); err != nil {
		return err
	}
	entry.CommittedAt = entry.CommittedAt.UTC()
	built, err := govaraudit.BuildEntry(entry)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO govar_audit_events(
tenant_id,sequence,event_id,event_kind,payload_sha256,request_id,workload_uid,
provider_attempt_id,actor_class,reason,before_state_sha256,after_state_sha256,
policy_version,pricing_snapshot_sha256,route_snapshot_sha256,cap_evidence_sha256,
cohort_sha256,software_sha256,calibration_sha256,previous_entry_sha256,committed_at,entry_sha256)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)`,
		built.TenantID, built.Sequence, built.EventID, built.EventKind, built.PayloadSHA256,
		built.RequestID, built.WorkloadUID, built.ProviderAttemptID, built.ActorClass, built.Reason,
		built.BeforeStateSHA256, built.AfterStateSHA256, built.PolicyVersion,
		built.PricingSnapshotSHA256, built.RouteSnapshotSHA256, built.CapEvidenceSHA256,
		built.CohortSHA256, built.SoftwareSHA256, built.CalibrationSHA256,
		built.PreviousEntrySHA256, built.CommittedAt, built.EntrySHA256)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE govar_audit_tenant_sequences SET next_sequence=$2 WHERE tenant_id=$1 AND next_sequence=$2-1`, entry.TenantID, entry.Sequence+1)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("tenant audit sequence compare-and-swap failed")
	}
	return nil
}

func appendRequestAuditTx(ctx context.Context, tx pgx.Tx, r Reservation, eventID, eventKind, payloadSHA string, actor govaraudit.ActorClass, reason ReasonCode, beforeDigest, softwareSHA, calibrationSHA string) error {
	afterDigest, err := reservationAuditDigest(r)
	if err != nil {
		return err
	}
	return appendAuditTx(ctx, tx, govaraudit.Entry{
		TenantID: r.TenantID, EventID: eventID, EventKind: eventKind,
		PayloadSHA256: payloadSHA, RequestID: r.RequestID, WorkloadUID: r.WorkloadUID,
		ProviderAttemptID: r.ProviderAttemptID, ActorClass: actor, Reason: string(reason),
		BeforeStateSHA256: beforeDigest, AfterStateSHA256: afterDigest,
		PolicyVersion: r.PolicyVersion, PricingSnapshotSHA256: r.PricingSnapshotSHA256,
		RouteSnapshotSHA256: r.RouteSnapshot.SnapshotHash, CapEvidenceSHA256: r.CapEvidenceSHA256,
		CohortSHA256: r.CohortRegistryDigest, SoftwareSHA256: softwareSHA,
		CalibrationSHA256: calibrationSHA,
	})
}

// ExportTenantAudit returns a complete tenant chain in verification order. It
// contains identifiers and digests only; no endpoint, prompt, credential, or
// provider response body is stored in the audit table.
func (e *PostgresEngine) ExportTenantAudit(ctx context.Context, tenantID string) ([]govaraudit.Entry, error) {
	if tenantID == "" {
		return nil, errors.New("tenant id is required")
	}
	rows, err := e.pool.Query(ctx, `SELECT tenant_id,sequence,event_id,event_kind,payload_sha256,
request_id,workload_uid,provider_attempt_id,actor_class,reason,before_state_sha256,
after_state_sha256,policy_version,pricing_snapshot_sha256,route_snapshot_sha256,
cap_evidence_sha256,cohort_sha256,software_sha256,calibration_sha256,
previous_entry_sha256,committed_at,entry_sha256
FROM govar_audit_events WHERE tenant_id=$1 ORDER BY sequence`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]govaraudit.Entry, 0)
	for rows.Next() {
		var entry govaraudit.Entry
		if err := rows.Scan(&entry.TenantID, &entry.Sequence, &entry.EventID, &entry.EventKind,
			&entry.PayloadSHA256, &entry.RequestID, &entry.WorkloadUID, &entry.ProviderAttemptID,
			&entry.ActorClass, &entry.Reason, &entry.BeforeStateSHA256, &entry.AfterStateSHA256,
			&entry.PolicyVersion, &entry.PricingSnapshotSHA256, &entry.RouteSnapshotSHA256,
			&entry.CapEvidenceSHA256, &entry.CohortSHA256, &entry.SoftwareSHA256,
			&entry.CalibrationSHA256, &entry.PreviousEntrySHA256, &entry.CommittedAt,
			&entry.EntrySHA256); err != nil {
			return nil, err
		}
		entry.CommittedAt = entry.CommittedAt.UTC()
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (e *PostgresEngine) VerifyTenantAudit(ctx context.Context, tenantID string) error {
	entries, err := e.ExportTenantAudit(ctx, tenantID)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return errors.New("tenant audit chain not found")
	}
	return govaraudit.Verify(entries)
}

func (e *PostgresEngine) validateStoredTenantAudits(ctx context.Context) error {
	rows, err := e.pool.Query(ctx, `SELECT DISTINCT tenant_id FROM govar_audit_events ORDER BY tenant_id`)
	if err != nil {
		return err
	}
	var tenants []string
	for rows.Next() {
		var tenant string
		if err := rows.Scan(&tenant); err != nil {
			rows.Close()
			return err
		}
		tenants = append(tenants, tenant)
	}
	rows.Close()
	for _, tenant := range tenants {
		if err := e.VerifyTenantAudit(ctx, tenant); err != nil {
			return fmt.Errorf("stored tenant audit %s: %w", tenant, err)
		}
	}
	return nil
}

// VerifyAllTenantAudits exposes the same fail-closed verification used by
// readiness for the audit-checkpoint worker. An empty ledger is valid; a
// tenant-specific lookup remains available through VerifyTenantAudit.
func (e *PostgresEngine) VerifyAllTenantAudits(ctx context.Context) error {
	return e.validateStoredTenantAudits(ctx)
}
