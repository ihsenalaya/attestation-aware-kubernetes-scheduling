package govar

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	"github.com/jackc/pgx/v5"
)

func TestPostgresEngineRejectsEmptyURL(t *testing.T) {
	if _, err := NewPostgresEngine(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty database url")
	}
}

func TestPostgresEmptyLegacySchemaIsRefusedAsIncompatible(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS govar_work_items,govar_audit_events,govar_audit_tenant_sequences,govar_frozen_cohort_slots,govar_frozen_cohorts,govar_reconciliation_tasks,govar_budget_adjustments,govar_inbox,govar_outbox,govar_settlements,govar_reservations,govar_tenants,govar_schema_metadata,govar_schema_migrations CASCADE;
CREATE TABLE govar_tenants (tenant_id TEXT PRIMARY KEY,budget_eur DOUBLE PRECISION NOT NULL DEFAULT 0,settled_eur DOUBLE PRECISION NOT NULL DEFAULT 0,reserved_eur DOUBLE PRECISION NOT NULL DEFAULT 0,active_reservations INTEGER NOT NULL DEFAULT 0,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE TABLE govar_reservations(request_id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,selected_deployment TEXT NOT NULL,reserved_cost DOUBLE PRECISION NOT NULL,actual_cost DOUBLE PRECISION NOT NULL DEFAULT 0,policy_version TEXT NOT NULL,pricing_version TEXT NOT NULL,reservation_mode TEXT NOT NULL,risk_level TEXT NOT NULL,expiry TIMESTAMPTZ NOT NULL,settled BOOLEAN NOT NULL DEFAULT FALSE,canceled BOOLEAN NOT NULL DEFAULT FALSE,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());`)
	if err != nil {
		t.Fatal(err)
	}
	if e, err := NewPostgresEngine(ctx, url); err == nil {
		e.Close()
		t.Fatal("empty legacy floating layout was mutated in place")
	}
}

func TestPostgresEngineRejectsUnreconciledLegacyFloatLedger(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS govar_work_items,govar_audit_events,govar_audit_tenant_sequences,govar_frozen_cohort_slots,govar_frozen_cohorts,govar_reconciliation_tasks,govar_budget_adjustments,govar_inbox,govar_outbox,govar_settlements,govar_reservations,govar_tenants,govar_schema_metadata,govar_schema_migrations CASCADE;
CREATE TABLE govar_tenants (tenant_id TEXT PRIMARY KEY,budget_eur DOUBLE PRECISION NOT NULL DEFAULT 0,settled_eur DOUBLE PRECISION NOT NULL DEFAULT 0,reserved_eur DOUBLE PRECISION NOT NULL DEFAULT 0,active_reservations INTEGER NOT NULL DEFAULT 0,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
INSERT INTO govar_tenants(tenant_id,budget_eur) VALUES ('legacy',1.25);`)
	if err != nil {
		t.Fatal(err)
	}
	if engine, err := NewPostgresEngine(ctx, url); err == nil {
		engine.Close()
		t.Fatal("service accepted unreconciled legacy floating-point ledger")
	}
	_, _ = conn.Exec(ctx, `DROP TABLE IF EXISTS govar_work_items,govar_audit_events,govar_audit_tenant_sequences,govar_frozen_cohort_slots,govar_frozen_cohorts,govar_reconciliation_tasks,govar_budget_adjustments,govar_inbox,govar_outbox,govar_settlements,govar_reservations,govar_tenants,govar_schema_metadata,govar_schema_migrations CASCADE`)
}

func TestPostgresEngineRejectsNonemptyPreV3IntegerUpgrade(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS govar_work_items,govar_audit_events,govar_audit_tenant_sequences,govar_frozen_cohort_slots,govar_frozen_cohorts,govar_reconciliation_tasks,govar_budget_adjustments,govar_inbox,govar_outbox,govar_settlements,govar_reservations,govar_tenants,govar_schema_metadata,govar_schema_migrations CASCADE;
CREATE TABLE govar_schema_migrations(version INTEGER PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()); INSERT INTO govar_schema_migrations(version) VALUES(2);
CREATE TABLE govar_tenants(tenant_id TEXT PRIMARY KEY,budget_micros BIGINT NOT NULL DEFAULT 0,settled_micros BIGINT NOT NULL DEFAULT 0,reserved_micros BIGINT NOT NULL DEFAULT 0,carried_adjustment_micros BIGINT NOT NULL DEFAULT 0,active_reservations INTEGER NOT NULL DEFAULT 0,budget_identity TEXT NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE TABLE govar_reservations(request_id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,workload_uid TEXT NOT NULL DEFAULT '',selected_deployment TEXT NOT NULL,provider_attempt_id TEXT,outbox_id TEXT,outbox_state TEXT NOT NULL DEFAULT 'PENDING',state TEXT NOT NULL DEFAULT 'RESERVED',reserved_cost_micros BIGINT NOT NULL DEFAULT 0,provisional_cost_micros BIGINT NOT NULL DEFAULT 0,residual_hold_micros BIGINT NOT NULL DEFAULT 0,usage_version BIGINT NOT NULL DEFAULT 0,finalized BOOLEAN NOT NULL DEFAULT FALSE,policy_version TEXT NOT NULL,pricing_version TEXT NOT NULL,reservation_mode TEXT NOT NULL,risk_level TEXT NOT NULL,allocated_risk_ppb BIGINT NOT NULL DEFAULT 0,input_price_micros_per_million BIGINT NOT NULL DEFAULT 0,output_price_micros_per_million BIGINT NOT NULL DEFAULT 0,admission_fingerprint TEXT NOT NULL,candidate_snapshot_version TEXT NOT NULL,cohort_id TEXT NOT NULL DEFAULT '',cohort_index BIGINT NOT NULL DEFAULT 0,expiry TIMESTAMPTZ NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
	INSERT INTO govar_reservations(request_id,tenant_id,selected_deployment,policy_version,pricing_version,reservation_mode,risk_level,admission_fingerprint,candidate_snapshot_version,expiry) VALUES('pre-v3','t','m','p','price','strict','strict','fp','snap',NOW());`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, `DROP TABLE IF EXISTS govar_work_items,govar_audit_events,govar_audit_tenant_sequences,govar_frozen_cohort_slots,govar_frozen_cohorts,govar_reconciliation_tasks,govar_budget_adjustments,govar_inbox,govar_outbox,govar_settlements,govar_reservations,govar_tenants,govar_schema_metadata,govar_schema_migrations CASCADE`)
	}()
	if e, err := NewPostgresEngine(ctx, url); err == nil {
		e.Close()
		t.Fatal("nonempty pre-v3 integer ledger was silently upgraded")
	}
}

func TestPostgresEngineRejectsEmptyPreV3IntegerUpgrade(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS govar_work_items,govar_audit_events,govar_audit_tenant_sequences,govar_frozen_cohort_slots,govar_frozen_cohorts,govar_reconciliation_tasks,govar_budget_adjustments,govar_inbox,govar_outbox,govar_settlements,govar_reservations,govar_tenants,govar_schema_metadata,govar_schema_migrations CASCADE;
CREATE TABLE govar_schema_migrations(version INTEGER PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
INSERT INTO govar_schema_migrations(version) VALUES(2);`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, `DROP TABLE IF EXISTS govar_work_items,govar_audit_events,govar_audit_tenant_sequences,govar_frozen_cohort_slots,govar_frozen_cohorts,govar_reconciliation_tasks,govar_budget_adjustments,govar_inbox,govar_outbox,govar_settlements,govar_reservations,govar_tenants,govar_schema_metadata,govar_schema_migrations CASCADE`)
	}()
	if e, err := NewPostgresEngine(ctx, url); err == nil {
		e.Close()
		t.Fatal("empty pre-v3 integer layout was silently mutated")
	}
}

func TestPostgresEngineRefusesNewerSchemaRollback(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	err = postgresOwnerExec(ctx, e, `INSERT INTO govar_schema_migrations(version) VALUES(8)`)
	e.Close()
	if err != nil {
		t.Fatal(err)
	}
	if rollback, err := NewPostgresEngine(ctx, url); err == nil {
		rollback.Close()
		t.Fatal("v7 binary accepted newer schema")
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, `DELETE FROM govar_schema_migrations WHERE version=8`); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresEngineLifecycle(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	engine, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if err := resetPostgresTestLedger(ctx, engine); err != nil {
		t.Fatal(err)
	}
	admit, err := engine.Admit(admitRequest("pg-r1", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || admit.Decision != DecisionAdmit {
		t.Fatalf("admit=(%+v,%v)", admit, err)
	}
	duplicate, err := engine.Admit(admitRequest("pg-r1", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || duplicate.ReasonCode != ReasonDuplicateRequest {
		t.Fatalf("exact duplicate=(%+v,%v)", duplicate, err)
	}
	conflict := admitRequest("pg-r1", testTenant, testWorkload)
	for _, mutation := range []func(*AdmitRequest){
		func(r *AdmitRequest) { r.MaxOutputTokens++ },
		func(r *AdmitRequest) { r.InputTokensExact = !r.InputTokensExact },
		func(r *AdmitRequest) { r.MaxToolCalls++ },
		func(r *AdmitRequest) { r.MaxMediaUnits++ },
		func(r *AdmitRequest) { r.TimeoutSeconds++ },
		func(r *AdmitRequest) { r.MaxRetryAttempts++ },
		func(r *AdmitRequest) { r.CancellationPossible = !r.CancellationPossible },
		func(r *AdmitRequest) { r.RequireApproval = !r.RequireApproval },
		func(r *AdmitRequest) {
			r.ChargeBounds = []govarpricing.UsageQuantity{{Basis: aiopsv1alpha1.ProviderBasisToolCall, Quantity: 1}}
		},
	} {
		changed := conflict
		mutation(&changed)
		if _, err := engine.Admit(changed, defaultBudget(), defaultRouting(), defaultCandidates()); err == nil {
			t.Fatal("conflicting PostgreSQL duplicate admission semantics were accepted")
		}
	}
	if _, code, err := engine.Dispatch(dispatchRequest("pg-r1", "pg-d1", admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil || code != ReasonDispatchClaimed {
		t.Fatalf("claim=(%s,%v)", code, err)
	}
	if _, code, err := engine.Settle(settleRequest("pg-r1", "pg-s1", 2_000, 1, false, testTenant, testWorkload)); err != nil || code != ReasonProvisionalSettlement {
		t.Fatalf("settle=(%s,%v)", code, err)
	}
	readTx, err := engine.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := loadReservationTx(ctx, readTx, "pg-r1", false)
	_ = readTx.Rollback(ctx)
	if err != nil || loaded.PricingSnapshotSHA256 == "" || loaded.PricingSnapshot.SnapshotSHA256 != loaded.PricingSnapshotSHA256 || len(loaded.ReservedComponents) != 2 || len(loaded.ActualComponents) != 2 {
		t.Fatalf("component persistence=(%+v,%v)", loaded, err)
	}
	pgFinal := settleRequest("pg-r1", "pg-s2", 2_000, 1, true, testTenant, testWorkload)
	pgFinal.PredecessorEventID = "pg-s1"
	if _, code, err := engine.Settle(pgFinal); err != nil || code != ReasonFinalized {
		t.Fatalf("finality=(%s,%v)", code, err)
	}
	assertLiability(t, engine.Liability(testTenant), 2_000, 0, 99_998_000, 0)
	second, err := engine.Admit(admitRequest("pg-missing-outbox", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	if err := postgresOwnerExec(ctx, engine, `DELETE FROM govar_outbox WHERE request_id='pg-missing-outbox'`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.Dispatch(dispatchRequest("pg-missing-outbox", "pg-missing-outbox-claim", second.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err == nil {
		t.Fatal("dispatch succeeded although outbox compare-and-swap affected no row")
	}
	if err := resetPostgresTestLedger(ctx, engine); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFrozenCohortConcurrentRegistrationAndAdmission(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_ = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey)
	_ = e.ConfigureCohortRuntime(testCohortSoftwareHash)
	err = resetPostgresTestLedger(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	r := admitRequest("pg-cohort", testTenant, testWorkload)
	r.CohortID = "pg-c"
	r.CohortIndex = 0
	c := FrozenCohort{TenantID: testTenant, CohortID: "pg-c", Size: 1, TenantRiskPPB: 123, Slots: []FrozenCohortSlot{{0, r.RequestID, OpportunityDigest(r), 1_000_000_000}}, DataHash: eventPayloadHash("d"), ConfigHash: eventPayloadHash("c"), ProtocolHash: eventPayloadHash("p"), FrozenAt: now.Add(-time.Minute)}
	c = mustSignCohort(t, c)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- e.RegisterFrozenCohort(ctx, c) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("identical concurrent registration: %v", err)
		}
	}
	conflict := c
	conflict.TenantRiskPPB++
	if err := e.RegisterFrozenCohort(ctx, conflict); err == nil {
		t.Fatal("conflicting registration accepted")
	}
	routing := typedAdaptiveRouting(aiopsv1alpha1.GOVARReservationFixedCohort, 1200, now, defaultCandidates()[0])
	bindTypedCohort(&routing, c)
	resp, err := e.Admit(r, defaultBudget(), routing, defaultCandidates())
	if err != nil || resp.Decision != DecisionAdmit || resp.AllocatedRiskPPB != 123 {
		t.Fatalf("admit=(%+v,%v)", resp, err)
	}
	readTx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := loadReservationTx(ctx, readTx, r.RequestID, false)
	_ = readTx.Rollback(ctx)
	wantCalibration := routing.Status.GOVAR.Calibration.ArtifactSHA256
	if err != nil || loaded.CalibrationArtifactSHA256 != wantCalibration {
		t.Fatalf("persisted calibration=%q want=%q err=%v", loaded.CalibrationArtifactSHA256, wantCalibration, err)
	}
	audit, err := e.ExportTenantAudit(ctx, testTenant)
	if err != nil || len(audit) < 2 || audit[len(audit)-1].CalibrationSHA256 != wantCalibration {
		t.Fatalf("audit calibration=(%+v,%v)", audit, err)
	}
}

func TestPostgresRolloverLateCorrectionAndDebtPayoff(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	err = resetPostgresTestLedger(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	b := defaultBudget()
	b.Spec.Period = "daily"
	a, err := e.Admit(admitRequest("pg-late", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.Dispatch(dispatchRequest("pg-late", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	if _, err = e.Admit(admitRequest("pg-new", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.Settle(settleRequest("pg-late", "base", 2_000, 1, false, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	pgDown := settleRequest("pg-late", "down", 1_000, 2, false, testTenant, testWorkload)
	pgDown.PredecessorEventID = "base"
	if _, _, err = e.Settle(pgDown); err != nil {
		t.Fatal(err)
	}
	got, err := e.LiabilityWithError(testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if got.CarriedAdjustmentMicros != 2_000 || got.HistoricalAuditCreditMicros != 1_000 || got.OutstandingLiabilityMicros != 5_200 {
		t.Fatalf("late/down=%+v", got)
	}
	_ = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey)
	adj := BudgetAdjustment{AdjustmentID: "pg-adjust-1", TenantID: testTenant, WindowID: got.CurrentWindowID, NewBudgetMicros: 100_000_000, DebtPaymentMicros: 2_000, AuthorizedBy: "test-admin", Reason: "verified payment", ApprovedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute)}
	adj = SignBudgetAdjustment(adj, "test-authority", testLedgerAuthorityKey)
	if err = e.AdjustBudget(ctx, adj); err != nil {
		t.Fatal(err)
	}
	got, _ = e.LiabilityWithError(testTenant)
	if got.CarriedAdjustmentMicros != 0 {
		t.Fatalf("debt payoff=%+v", got)
	}
}

func TestPostgresUpwardCorrectionAfterLateFinalPreservesFullExternalDebt(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err = resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	b := defaultBudget()
	b.Spec.Period = "daily"
	a, err := e.Admit(admitRequest("pg-late-final-up", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.Dispatch(dispatchRequest("pg-late-final-up", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	if _, err = e.LiabilityWithError(testTenant); err != nil {
		t.Fatal(err)
	}
	if _, code, err := e.Settle(settleRequest("pg-late-final-up", "late-final", 2_000, 1, true, testTenant, testWorkload)); err != nil || code != ReasonLateSettlement {
		t.Fatalf("late final=(%s,%v)", code, err)
	}
	up := settleRequest("pg-late-final-up", "late-up", 2_700, 2, true, testTenant, testWorkload)
	up.PredecessorEventID = "late-final"
	if _, code, err := e.Settle(up); err != nil || code != ReasonCorrection {
		t.Fatalf("up correction=(%s,%v)", code, err)
	}
	got, err := e.LiabilityWithError(testTenant)
	if err != nil || got.CarriedAdjustmentMicros != 2_700 {
		t.Fatalf("late correction reduced external debt: %+v err=%v", got, err)
	}
}

func TestPostgresExpiryScanIsCASConservative(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_ = resetPostgresTestLedger(ctx, e)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	routing := defaultRoutingAt(now)
	pending, err := e.Admit(admitRequest("pg-exp-pending", testTenant, testWorkload), defaultBudget(), routing, defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := e.Admit(admitRequest("pg-exp-claimed", testTenant, testWorkload), defaultBudget(), routing, defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.Dispatch(dispatchRequest("pg-exp-claimed", "claim", claimed.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	_ = pending
	rows, err := e.ReconcileExpired(ctx, now.Add(6*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%+v", rows)
	}
	var tasks, inbox int
	if err := e.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM govar_reconciliation_tasks WHERE state='COMPLETED'),(SELECT count(*) FROM govar_inbox WHERE event_kind='reconciliation')`).Scan(&tasks, &inbox); err != nil || tasks != 2 || inbox != 2 {
		t.Fatalf("durable reconciliation evidence tasks=%d inbox=%d err=%v", tasks, inbox, err)
	}
	again, err := e.ReconcileExpired(ctx, now.Add(6*time.Minute))
	if err != nil || len(again) != 0 {
		t.Fatalf("replayed expiry=(%+v,%v)", again, err)
	}
	got, _ := e.LiabilityWithError(testTenant)
	if got.OutstandingLiabilityMicros != 3_600 || got.ActiveReservations != 1 {
		t.Fatalf("expiry liability=%+v", got)
	}
	pendingRows, err := e.PendingReconciliation(ctx, 10)
	if err != nil || len(pendingRows) != 1 || pendingRows[0].State != StateUnresolved {
		t.Fatalf("pending reconciliation=(%+v,%v)", pendingRows, err)
	}
}
