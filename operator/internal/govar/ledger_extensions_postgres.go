package govar

import (
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govaraudit"
)

func (e *PostgresEngine) RegisterFrozenCohort(ctx context.Context, cohort FrozenCohort) error {
	c, err := normalizeFrozenCohort(cohort)
	if err != nil {
		return err
	}
	if c.FrozenAt.After(e.now().UTC()) {
		return errors.New("frozen_at must precede server registration time")
	}
	if c.SoftwareHash != e.cohortSoftwareHash || c.AuthorityKeyID != e.authorityKeyID || len(e.authorityKey) == 0 || !hmac.Equal([]byte(strings.ToLower(c.AuthorityProof)), []byte(authorityMAC(e.authorityKey, "govar-cohort-authority-v2", c.RegistryDigest))) {
		return errors.New("frozen cohort authority proof is invalid")
	}
	c.RegisteredAt = e.now().UTC()
	for attempt := 0; attempt < 3; attempt++ {
		err = e.registerFrozenCohortOnce(ctx, c)
		var pgErr *pgconn.PgError
		isUniqueViolation := errors.As(err, &pgErr) && pgErr.Code == "23505"
		if err == nil || (!isRetryablePG(err) && !isUniqueViolation) {
			return err
		}
		time.Sleep(time.Duration(1<<attempt) * 10 * time.Millisecond)
	}
	return err
}

func (e *PostgresEngine) registerFrozenCohortOnce(ctx context.Context, c FrozenCohort) error {
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var digest string
	err = tx.QueryRow(ctx, `SELECT registry_digest FROM govar_frozen_cohorts WHERE tenant_id=$1 AND cohort_id=$2 FOR UPDATE`, c.TenantID, c.CohortID).Scan(&digest)
	if err == nil {
		if digest != c.RegistryDigest {
			return errors.New("frozen cohort is immutable and conflicts with existing registration")
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO govar_frozen_cohorts(tenant_id,cohort_id,size,tenant_risk_ppb,data_hash,config_hash,protocol_hash,frozen_at,registered_at,registry_digest,authority_key_id,authority_proof,ledger_layout_id,route_snapshot_schema,software_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, c.TenantID, c.CohortID, c.Size, c.TenantRiskPPB, c.DataHash, c.ConfigHash, c.ProtocolHash, c.FrozenAt, c.RegisteredAt, c.RegistryDigest, c.AuthorityKeyID, c.AuthorityProof, c.LedgerLayoutID, c.RouteSnapshotSchema, c.SoftwareHash); err != nil {
		return err
	}
	for _, s := range c.Slots {
		if _, err = tx.Exec(ctx, `INSERT INTO govar_frozen_cohort_slots(tenant_id,cohort_id,slot_index,request_id,opportunity_digest,weight_ppb) VALUES($1,$2,$3,$4,$5,$6)`, c.TenantID, c.CohortID, s.Index, s.RequestID, s.OpportunityDigest, s.WeightPPB); err != nil {
			return err
		}
	}
	after, err := canonicalAuditDigest(c)
	if err != nil {
		return err
	}
	if err := appendAuditTx(ctx, tx, govaraudit.Entry{TenantID: c.TenantID,
		EventID:   "cohort-registration:" + c.TenantID + ":" + c.CohortID + ":" + c.RegistryDigest,
		EventKind: "COHORT_REGISTRATION", PayloadSHA256: c.RegistryDigest,
		ActorClass: govaraudit.ActorRegistry, Reason: "cohort_registered",
		AfterStateSHA256: after, CohortSHA256: c.RegistryDigest, SoftwareSHA256: c.SoftwareHash}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (e *PostgresEngine) ExportFrozenCohort(ctx context.Context, tenant, cohortID string) (FrozenCohort, error) {
	var c FrozenCohort
	err := e.pool.QueryRow(ctx, `SELECT tenant_id,cohort_id,size,tenant_risk_ppb,data_hash,config_hash,protocol_hash,frozen_at,registered_at,registry_digest,authority_key_id,authority_proof,ledger_layout_id,route_snapshot_schema,software_hash FROM govar_frozen_cohorts WHERE tenant_id=$1 AND cohort_id=$2`, tenant, cohortID).Scan(&c.TenantID, &c.CohortID, &c.Size, &c.TenantRiskPPB, &c.DataHash, &c.ConfigHash, &c.ProtocolHash, &c.FrozenAt, &c.RegisteredAt, &c.RegistryDigest, &c.AuthorityKeyID, &c.AuthorityProof, &c.LedgerLayoutID, &c.RouteSnapshotSchema, &c.SoftwareHash)
	if err != nil {
		return c, err
	}
	rows, err := e.pool.Query(ctx, `SELECT slot_index,request_id,opportunity_digest,weight_ppb FROM govar_frozen_cohort_slots WHERE tenant_id=$1 AND cohort_id=$2 ORDER BY slot_index`, tenant, cohortID)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var s FrozenCohortSlot
		if err := rows.Scan(&s.Index, &s.RequestID, &s.OpportunityDigest, &s.WeightPPB); err != nil {
			return c, err
		}
		c.Slots = append(c.Slots, s)
	}
	return c, rows.Err()
}

func loadFrozenCohortTx(ctx context.Context, tx pgx.Tx, tenant, cohortID string) (FrozenCohort, error) {
	var c FrozenCohort
	err := tx.QueryRow(ctx, `SELECT tenant_id,cohort_id,size,tenant_risk_ppb,data_hash,config_hash,protocol_hash,frozen_at,registered_at,registry_digest,authority_key_id,authority_proof,ledger_layout_id,route_snapshot_schema,software_hash FROM govar_frozen_cohorts WHERE tenant_id=$1 AND cohort_id=$2`, tenant, cohortID).Scan(&c.TenantID, &c.CohortID, &c.Size, &c.TenantRiskPPB, &c.DataHash, &c.ConfigHash, &c.ProtocolHash, &c.FrozenAt, &c.RegisteredAt, &c.RegistryDigest, &c.AuthorityKeyID, &c.AuthorityProof, &c.LedgerLayoutID, &c.RouteSnapshotSchema, &c.SoftwareHash)
	if err != nil {
		return c, err
	}
	rows, err := tx.Query(ctx, `SELECT slot_index,request_id,opportunity_digest,weight_ppb FROM govar_frozen_cohort_slots WHERE tenant_id=$1 AND cohort_id=$2 ORDER BY slot_index`, tenant, cohortID)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var s FrozenCohortSlot
		if err := rows.Scan(&s.Index, &s.RequestID, &s.OpportunityDigest, &s.WeightPPB); err != nil {
			return c, err
		}
		c.Slots = append(c.Slots, s)
	}
	return c, rows.Err()
}

func (e *PostgresEngine) AdjustBudget(ctx context.Context, a BudgetAdjustment) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = e.adjustBudgetOnce(ctx, a)
		if !isRetryablePG(err) && !isUniqueViolationPG(err) {
			return err
		}
		time.Sleep(time.Duration(1<<attempt) * 10 * time.Millisecond)
	}
	return err
}

func (e *PostgresEngine) adjustBudgetOnce(ctx context.Context, a BudgetAdjustment) error {
	if a.AdjustmentID == "" || a.AuthorizedBy == "" || a.Reason == "" || a.NewBudgetMicros < 0 || a.DebtPaymentMicros < 0 {
		return errors.New("authorized non-negative budget adjustment is required")
	}
	if a.AuthorityKeyID != e.authorityKeyID || len(e.authorityKey) == 0 || a.ApprovedAt.After(e.now()) || !a.ExpiresAt.After(e.now()) || !hmac.Equal([]byte(strings.ToLower(a.AuthorityProof)), []byte(authorityMAC(e.authorityKey, "govar-budget-adjustment-authority-v1", a.AdjustmentID, a.TenantID, a.WindowID, fmt.Sprint(a.NewBudgetMicros), fmt.Sprint(a.DebtPaymentMicros), a.AuthorizedBy, a.Reason, a.ApprovedAt.UTC().Format(time.RFC3339Nano), a.ExpiresAt.UTC().Format(time.RFC3339Nano)))) {
		return errors.New("budget adjustment authority proof is invalid or expired")
	}
	payload := eventPayloadHash("budget-adjustment-v1", a.TenantID, a.WindowID, fmt.Sprint(a.NewBudgetMicros), fmt.Sprint(a.DebtPaymentMicros), a.AuthorizedBy, a.Reason)
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var prior string
	err = tx.QueryRow(ctx, `SELECT payload_hash FROM govar_budget_adjustments WHERE adjustment_id=$1`, a.AdjustmentID).Scan(&prior)
	if err == nil {
		if prior != payload {
			return errors.New("adjustment_id replay has conflicting immutable payload")
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	tenant, err := loadTenantTx(ctx, tx, a.TenantID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("tenant/window not found")
		}
		return err
	}
	if err = advanceTenantWindowTx(ctx, tx, a.TenantID, tenant, e.now()); err != nil {
		return err
	}
	before, err := tenantAuditDigest(tenant)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE govar_tenants SET budget_micros=$3,carried_adjustment_micros=carried_adjustment_micros-$4,updated_at=NOW() WHERE tenant_id=$1 AND current_window_id=$2 AND carried_adjustment_micros >= $4`, a.TenantID, a.WindowID, a.NewBudgetMicros, a.DebtPaymentMicros)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("tenant/window not found or debt payment exceeds carried debt")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO govar_budget_adjustments(adjustment_id,tenant_id,window_id,new_budget_micros,debt_payment_micros,authorized_by,reason,payload_hash,authority_key_id,authority_proof,approved_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, a.AdjustmentID, a.TenantID, a.WindowID, a.NewBudgetMicros, a.DebtPaymentMicros, a.AuthorizedBy, a.Reason, payload, a.AuthorityKeyID, a.AuthorityProof, a.ApprovedAt, a.ExpiresAt); err != nil {
		return err
	}
	tenant.BudgetMicros = a.NewBudgetMicros
	tenant.CarriedAdjustmentMicros -= a.DebtPaymentMicros
	after, err := tenantAuditDigest(tenant)
	if err != nil {
		return err
	}
	if err := appendAuditTx(ctx, tx, govaraudit.Entry{TenantID: a.TenantID,
		EventID: "budget-adjustment:" + a.AdjustmentID, EventKind: "BUDGET_ADJUSTMENT",
		PayloadSHA256: payload, ActorClass: govaraudit.ActorAuthority,
		Reason: "budget_adjusted", BeforeStateSHA256: before, AfterStateSHA256: after,
		SoftwareSHA256: e.cohortSoftwareHash}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (e *PostgresEngine) ReconcileExpired(ctx context.Context, at time.Time) ([]ReconciliationRecord, error) {
	var rows []ReconciliationRecord
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		rows, err = e.reconcileExpiredOnce(ctx, at)
		if !isRetryablePG(err) {
			return rows, err
		}
		time.Sleep(time.Duration(1<<attempt) * 10 * time.Millisecond)
	}
	return rows, err
}

func (e *PostgresEngine) reconcileExpiredOnce(ctx context.Context, at time.Time) ([]ReconciliationRecord, error) {
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT request_id FROM govar_reservations WHERE expiry <= $1 AND ((state='RESERVED' AND outbox_state='PENDING') OR state IN ('DISPATCH_PENDING','DISPATCHED')) ORDER BY request_id FOR UPDATE`, at)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []ReconciliationRecord
	for _, id := range ids {
		r, err := loadReservationTx(ctx, tx, id, true)
		if err != nil {
			return nil, err
		}
		previousState := r.State
		before, err := reservationAuditDigest(r)
		if err != nil {
			return nil, err
		}
		t, err := loadTenantTx(ctx, tx, r.TenantID)
		if err != nil {
			return nil, err
		}
		if r.State == StateReserved && r.OutboxState == OutboxPending {
			prev := r.OutboxState
			r.State, r.OutboxState = StateExpiredUndispatched, OutboxCanceled
			r.LastReasonCode = ReasonExpiredUndispatched
			r.LastTransitionEventID = "expiry:" + r.RequestID + ":" + fmt.Sprint(r.Expiry.UnixNano())
			t.ReservedMicros -= r.ResidualHoldMicros + r.RolloverGuardMicros
			r.ResidualHoldMicros, r.RolloverGuardMicros = 0, 0
			t.ActiveReservations--
			if err := updateOutboxState(ctx, tx, r.OutboxID, prev, r.OutboxState); err != nil {
				return nil, err
			}
		} else {
			r.State = StateUnresolved
			r.LastReasonCode = ReasonDispatchUnresolved
			r.LastTransitionEventID = "expiry-unresolved:" + r.RequestID + ":" + fmt.Sprint(r.Expiry.UnixNano())
		}
		payload := eventPayloadHash("reconciliation-expiry-v1", r.RequestID, r.ProviderAttemptID, r.Expiry.UTC().Format(time.RFC3339Nano), string(r.State), string(r.LastReasonCode))
		if _, err := tx.Exec(ctx, `INSERT INTO govar_inbox(event_id,request_id,event_kind,payload_hash) VALUES($1,$2,'reconciliation',$3) ON CONFLICT(event_id) DO NOTHING`, r.LastTransitionEventID, r.RequestID, payload); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO govar_reconciliation_tasks(task_id,request_id,reason_code,state,payload_hash,completed_at) VALUES($1,$2,$3,'COMPLETED',$4,NOW()) ON CONFLICT(task_id) DO NOTHING`, r.LastTransitionEventID, r.RequestID, r.LastReasonCode, payload); err != nil {
			return nil, err
		}
		if err := storeReservationTx(ctx, tx, r); err != nil {
			return nil, err
		}
		if err := appendRequestAuditTx(ctx, tx, r, r.LastTransitionEventID, "EXPIRY", payload, govaraudit.ActorReconciler, r.LastReasonCode, before, e.cohortSoftwareHash, ""); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE govar_tenants SET reserved_micros=$2,active_reservations=$3,updated_at=NOW() WHERE tenant_id=$1`, r.TenantID, t.ReservedMicros, t.ActiveReservations); err != nil {
			return nil, err
		}
		out = append(out, ReconciliationRecord{RequestID: r.RequestID, TenantID: r.TenantID, ProviderAttemptID: r.ProviderAttemptID,
			State: r.State, OutboxState: r.OutboxState, Expiry: r.Expiry, PreviousState: previousState, TransitionEffective: true})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (e *PostgresEngine) PendingReconciliation(ctx context.Context, limit int) ([]ReconciliationRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := e.pool.Query(ctx, `SELECT request_id,tenant_id,provider_attempt_id,state,outbox_state,expiry FROM govar_reservations WHERE outbox_state IN ('PENDING','CLAIMED') OR state='UNRESOLVED' ORDER BY request_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReconciliationRecord
	for rows.Next() {
		var r ReconciliationRecord
		if err := rows.Scan(&r.RequestID, &r.TenantID, &r.ProviderAttemptID, &r.State, &r.OutboxState, &r.Expiry); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
	return out, rows.Err()
}
