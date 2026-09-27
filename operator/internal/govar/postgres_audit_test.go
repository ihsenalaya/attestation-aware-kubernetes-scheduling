package govar

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govaraudit"
)

// resetPostgresTestLedger is deliberately test-only. Production has no API or
// database function that suspends the append-only triggers.
func resetPostgresTestLedger(ctx context.Context, e *PostgresEngine) error {
	err := postgresOwnerExec(ctx, e, `
ALTER TABLE govar_audit_events DISABLE TRIGGER govar_audit_no_truncate;
TRUNCATE govar_selected_feedback_observations,govar_selected_feedback_commitments,govar_selected_feedback_dispatches,
 govar_work_items,govar_audit_events,govar_audit_tenant_sequences,govar_reconciliation_tasks,govar_budget_adjustments,
 govar_frozen_cohort_slots,govar_frozen_cohorts,govar_inbox,govar_outbox,
 govar_reservations,govar_tenants CASCADE;
ALTER TABLE govar_audit_events ENABLE TRIGGER govar_audit_no_truncate;`)
	return err
}

func postgresOwnerExec(ctx context.Context, e *PostgresEngine, sql string, args ...any) error {
	conn, err := pgx.Connect(ctx, e.databaseURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, sql, args...)
	return err
}

func TestPostgresTransitionAuditLifecycleReplayAndTamperProtection(t *testing.T) {
	url := testDatabaseURL(t)
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	routing := defaultRoutingAt(now)
	admit, err := e.Admit(admitRequest("audit-lifecycle", testTenant, testWorkload), defaultBudget(), routing, defaultCandidates())
	if err != nil || admit.Decision != DecisionAdmit {
		t.Fatalf("admit=(%+v,%v)", admit, err)
	}
	if _, err := e.Admit(admitRequest("audit-lifecycle", testTenant, testWorkload), defaultBudget(), routing, defaultCandidates()); err != nil {
		t.Fatal(err)
	}
	if _, code, err := e.Dispatch(dispatchRequest("audit-lifecycle", "audit-claim", admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil || code != ReasonDispatchClaimed {
		t.Fatalf("claim=(%s,%v)", code, err)
	}
	if _, code, err := e.Dispatch(dispatchRequest("audit-lifecycle", "audit-claim", admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil || code != ReasonDuplicateEvent {
		t.Fatalf("claim replay=(%s,%v)", code, err)
	}
	if _, code, err := e.Dispatch(dispatchRequest("audit-lifecycle", "audit-deliver", admit.ProviderAttemptID, DispatchDelivered, testTenant, testWorkload)); err != nil || code != ReasonDispatchDelivered {
		t.Fatalf("deliver=(%s,%v)", code, err)
	}
	if _, code, err := e.Settle(settleRequest("audit-lifecycle", "audit-usage", 2_000, 1, false, testTenant, testWorkload)); err != nil || code != ReasonProvisionalSettlement {
		t.Fatalf("settle=(%s,%v)", code, err)
	}
	semanticDuplicate := settleRequest("audit-lifecycle", "audit-usage-duplicate-id", 2_000, 1, false, testTenant, testWorkload)
	if _, code, err := e.Settle(semanticDuplicate); err != nil || code != ReasonSettlementDuplicate {
		t.Fatalf("semantic duplicate=(%s,%v)", code, err)
	}
	final := settleRequest("audit-lifecycle", "audit-final", 2_000, 1, true, testTenant, testWorkload)
	final.PredecessorEventID = "audit-usage"
	if _, code, err := e.Settle(final); err != nil || code != ReasonFinalized {
		t.Fatalf("final=(%s,%v)", code, err)
	}

	entries, err := e.ExportTenantAudit(ctx, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := []string{"RESERVE", "DISPATCH", "DISPATCH", "SETTLE", "SETTLE"}
	if len(entries) != len(wantKinds) {
		t.Fatalf("audit entries=%d want=%d: %+v", len(entries), len(wantKinds), entries)
	}
	for i, want := range wantKinds {
		if entries[i].EventKind != want || entries[i].Sequence != int64(i+1) {
			t.Fatalf("entry[%d]=%+v", i, entries[i])
		}
	}
	if err := govaraudit.Verify(entries); err != nil {
		t.Fatalf("independent verify: %v", err)
	}
	if err := e.VerifyTenantAudit(ctx, testTenant); err != nil {
		t.Fatal(err)
	}
	tampered := append([]govaraudit.Entry(nil), entries...)
	tampered[2].PolicyVersion += "-tampered"
	if err := govaraudit.Verify(tampered); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("tampered export accepted: %v", err)
	}
	for _, query := range []string{
		`UPDATE govar_audit_events SET event_kind='CANCEL' WHERE tenant_id='tenant-a' AND sequence=1`,
		`DELETE FROM govar_audit_events WHERE tenant_id='tenant-a' AND sequence=1`,
		`TRUNCATE govar_audit_events`,
	} {
		if _, err := e.pool.Exec(ctx, query); err == nil || (!strings.Contains(err.Error(), "append-only") && !strings.Contains(err.Error(), "permission denied")) {
			t.Fatalf("audit mutation was not rejected: query=%s err=%v", query, err)
		}
	}
}

func TestPostgresTransitionAuditCancelAndReconciliation(t *testing.T) {
	url := testDatabaseURL(t)
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	routing := defaultRoutingAt(now)
	admit, err := e.Admit(admitRequest("audit-cancel", testTenant, testWorkload), defaultBudget(), routing, defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	if _, code, err := e.Cancel(cancelRequest("audit-cancel", "cancel-once", false, testTenant, testWorkload)); err != nil || code != ReasonCanceled {
		t.Fatalf("cancel=(%s,%v)", code, err)
	}
	if _, code, err := e.Cancel(cancelRequest("audit-cancel", "cancel-semantic-duplicate", false, testTenant, testWorkload)); err != nil || code != ReasonDuplicateEvent {
		t.Fatalf("duplicate cancel=(%s,%v)", code, err)
	}
	entries, err := e.ExportTenantAudit(ctx, testTenant)
	if err != nil || len(entries) != 2 || entries[1].EventKind != "CANCEL" {
		t.Fatalf("cancel audit=(%+v,%v), admit=%+v", entries, err, admit)
	}

	if _, err := e.Admit(admitRequest("audit-expiry", testTenant, testWorkload), defaultBudget(), routing, defaultCandidates()); err != nil {
		t.Fatal(err)
	}
	if rows, err := e.ReconcileExpired(ctx, now.Add(6*time.Minute)); err != nil || len(rows) != 1 {
		t.Fatalf("expiry=(%+v,%v)", rows, err)
	}
	expiry, err := e.ExportTenantAudit(ctx, testTenant)
	if err != nil || len(expiry) != 4 || expiry[3].EventKind != "EXPIRY" || expiry[3].ActorClass != govaraudit.ActorReconciler {
		t.Fatalf("expiry audit=(%+v,%v)", expiry, err)
	}
	if err := govaraudit.Verify(expiry); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTenantAuditCoversCohortRolloverBudgetAndRollback(t *testing.T) {
	url := testDatabaseURL(t)
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	_ = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey)
	_ = e.ConfigureCohortRuntime(testCohortSoftwareHash)
	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	req := admitRequest("audit-admin", testTenant, testWorkload)
	cohort := FrozenCohort{TenantID: testTenant, CohortID: "audit-cohort", Size: 1,
		TenantRiskPPB: 100, Slots: []FrozenCohortSlot{{0, req.RequestID, OpportunityDigest(req), 1_000_000_000}},
		DataHash: eventPayloadHash("audit-data"), ConfigHash: eventPayloadHash("audit-config"),
		ProtocolHash: eventPayloadHash("audit-protocol"), FrozenAt: now.Add(-time.Minute)}
	cohort = mustSignCohort(t, cohort)
	if err := e.RegisterFrozenCohort(ctx, cohort); err != nil {
		t.Fatal(err)
	}
	budget := defaultBudget()
	budget.Spec.Period = "daily"
	admit, err := e.Admit(req, budget, defaultRoutingAt(now), defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt only the sequence allocator to force the audit append to fail.
	// Dispatch state/outbox mutations preceding it must roll back atomically.
	if _, err := e.pool.Exec(ctx, `UPDATE govar_audit_tenant_sequences SET next_sequence=99 WHERE tenant_id=$1`, testTenant); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Dispatch(dispatchRequest(req.RequestID, "audit-forced-rollback", admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err == nil {
		t.Fatal("dispatch committed despite failed audit sequence allocation")
	}
	readTx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := loadReservationTx(ctx, readTx, req.RequestID, false)
	_ = readTx.Rollback(ctx)
	if err != nil || rolledBack.State != StateReserved || rolledBack.OutboxState != OutboxPending {
		t.Fatalf("audit rollback left state mutation: %+v err=%v", rolledBack, err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE govar_audit_tenant_sequences SET next_sequence=3 WHERE tenant_id=$1`, testTenant); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Dispatch(dispatchRequest(req.RequestID, "audit-claim-admin", admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Settle(settleRequest(req.RequestID, "audit-provisional-admin", 2_000, 1, false, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	liability, err := e.LiabilityWithError(testTenant)
	if err != nil {
		t.Fatal(err)
	}
	adjustment := BudgetAdjustment{AdjustmentID: "audit-budget", TenantID: testTenant,
		WindowID: liability.CurrentWindowID, NewBudgetMicros: 100_000_000,
		AuthorizedBy: "test-admin", Reason: "test", ApprovedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute)}
	adjustment = SignBudgetAdjustment(adjustment, "test-authority", testLedgerAuthorityKey)
	if err := e.AdjustBudget(ctx, adjustment); err != nil {
		t.Fatal(err)
	}
	entries, err := e.ExportTenantAudit(ctx, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"COHORT_REGISTRATION", "RESERVE", "DISPATCH", "SETTLE", "ROLLOVER", "BUDGET_ADJUSTMENT"}
	if len(entries) != len(want) {
		t.Fatalf("entries=%+v", entries)
	}
	for i := range want {
		if entries[i].Sequence != int64(i+1) || entries[i].EventKind != want[i] {
			t.Fatalf("entry[%d]=%+v want kind %s", i, entries[i], want[i])
		}
	}
	if err := govaraudit.Verify(entries); err != nil {
		t.Fatal(err)
	}
	var canSelect, canInsert, canUpdate, canDelete bool
	if err := e.pool.QueryRow(ctx, `SELECT
	 has_table_privilege('govar_runtime','govar_audit_events','SELECT'),
	 has_table_privilege('govar_runtime','govar_audit_events','INSERT'),
	 has_table_privilege('govar_runtime','govar_audit_events','UPDATE'),
	 has_table_privilege('govar_runtime','govar_audit_events','DELETE')`).Scan(&canSelect, &canInsert, &canUpdate, &canDelete); err != nil {
		t.Fatal(err)
	}
	if !canSelect || !canInsert || canUpdate || canDelete {
		t.Fatalf("runtime audit grants select=%v insert=%v update=%v delete=%v", canSelect, canInsert, canUpdate, canDelete)
	}
	var currentUser, sessionUser string
	if err := e.pool.QueryRow(ctx, `SELECT current_user,session_user`).Scan(&currentUser, &sessionUser); err != nil {
		t.Fatal(err)
	}
	if currentUser != "govar_runtime" || sessionUser == currentUser {
		t.Fatalf("role separation current_user=%q session_user=%q", currentUser, sessionUser)
	}
}

func TestOpenPostgresEngineRequiresDistinctLeastPrivilegeLogin(t *testing.T) {
	ownerURL := testDatabaseURL(t)
	ctx := context.Background()
	bootstrap, err := NewPostgresEngine(ctx, ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := resetPostgresTestLedger(ctx, bootstrap); err != nil {
		bootstrap.Close()
		t.Fatal(err)
	}
	bootstrap.Close()

	if engine, err := OpenPostgresEngine(ctx, ownerURL, testCohortSoftwareHash); err == nil {
		engine.Close()
		t.Fatal("production runtime accepted migration-owner credentials")
	}

	owner, err := pgx.Connect(ctx, ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	const runtimeLogin = "govar_runtime_open_test"
	const runtimePassword = "govar-runtime-test-password"
	if _, err := owner.Exec(ctx, `DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='govar_runtime_open_test') THEN
   CREATE ROLE govar_runtime_open_test LOGIN PASSWORD 'govar-runtime-test-password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
 END IF;
END $$;
ALTER ROLE govar_runtime_open_test PASSWORD 'govar-runtime-test-password';
GRANT govar_runtime TO govar_runtime_open_test;`); err != nil {
		owner.Close(ctx)
		t.Fatalf("create distinct runtime login: %v", err)
	}
	owner.Close(ctx)

	parsedURL, err := url.Parse(ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	parsedURL.User = url.UserPassword(runtimeLogin, runtimePassword)
	runtimeURL := parsedURL.String()
	runtimeEngine, err := OpenPostgresEngine(ctx, runtimeURL, testCohortSoftwareHash)
	if err != nil {
		t.Fatalf("open least-privilege runtime: %v", err)
	}
	t.Cleanup(func() {
		runtimeEngine.Close()
		cleanup, cleanupErr := pgx.Connect(context.Background(), ownerURL)
		if cleanupErr != nil {
			t.Errorf("connect to remove runtime test login: %v", cleanupErr)
			return
		}
		defer cleanup.Close(context.Background())
		if _, cleanupErr = cleanup.Exec(context.Background(), `DROP ROLE IF EXISTS govar_runtime_open_test`); cleanupErr != nil {
			t.Errorf("remove runtime test login: %v", cleanupErr)
		}
	})
	var currentUser, sessionUser string
	if err := runtimeEngine.pool.QueryRow(ctx, `SELECT current_user,session_user`).Scan(&currentUser, &sessionUser); err != nil {
		t.Fatal(err)
	}
	if currentUser != "govar_runtime" || sessionUser != runtimeLogin {
		t.Fatalf("production role identity current=%q session=%q", currentUser, sessionUser)
	}
	admit, err := runtimeEngine.Admit(admitRequest("runtime-role-audit", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || admit.Decision != DecisionAdmit {
		t.Fatalf("runtime admission=(%+v,%v)", admit, err)
	}
	entries, err := runtimeEngine.ExportTenantAudit(ctx, testTenant)
	if err != nil || len(entries) != 1 || entries[0].SoftwareSHA256 != testCohortSoftwareHash {
		t.Fatalf("runtime audit software binding=(%+v,%v)", entries, err)
	}
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	return url
}
