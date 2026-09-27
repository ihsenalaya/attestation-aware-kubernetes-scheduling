package govar

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govarworker"
)

func workerTestDigest(label string) string {
	return eventPayloadHash("govar-worker-test-v1", label)
}

func TestPostgresV7WorkerSchemaAndLeastPrivilege(t *testing.T) {
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	var version int
	var layout string
	var canSelect, canInsert, canUpdate, canDelete, canTruncate bool
	err = e.pool.QueryRow(ctx, `SELECT (SELECT max(version) FROM govar_schema_migrations),
 (SELECT layout_id FROM govar_schema_metadata WHERE version=7),
 has_table_privilege(current_user,'govar_work_items','SELECT'),
 has_table_privilege(current_user,'govar_work_items','INSERT'),
 has_table_privilege(current_user,'govar_work_items','UPDATE'),
 has_table_privilege(current_user,'govar_work_items','DELETE'),
 has_table_privilege(current_user,'govar_work_items','TRUNCATE')`).Scan(
		&version, &layout, &canSelect, &canInsert, &canUpdate, &canDelete, &canTruncate)
	if err != nil {
		t.Fatal(err)
	}
	if version != 7 || layout != "govar-v7-durable-workers-20260713" || !canSelect || !canInsert || !canUpdate || canDelete || canTruncate {
		t.Fatalf("v7 worker schema/grants mismatch: version=%d layout=%q privileges=%t/%t/%t/%t/%t",
			version, layout, canSelect, canInsert, canUpdate, canDelete, canTruncate)
	}
	for _, constraint := range []string{"govar_work_kind_closed", "govar_work_state_closed", "govar_work_lease_coherent", "govar_work_completion_coherent", "govar_work_liability_disposition_closed", "govar_work_release_implies_liability"} {
		var exists bool
		if err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='govar_work_items'::regclass AND conname=$1)`, constraint).Scan(&exists); err != nil || !exists {
			t.Fatalf("constraint %s: exists=%t err=%v", constraint, exists, err)
		}
	}
}

func TestPostgresWorkerEnqueueCoverageIsIdempotentAndMonetarilyReadOnly(t *testing.T) {
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	admit, err := e.Admit(admitRequest("worker-enqueue", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || admit.Decision != DecisionAdmit {
		t.Fatalf("admit=(%+v,%v)", admit, err)
	}
	if err := postgresOwnerExec(ctx, e, `UPDATE govar_reservations SET expiry=clock_timestamp()-interval '1 second' WHERE request_id='worker-enqueue'`); err != nil {
		t.Fatal(err)
	}
	reconciliationPayload := workerTestDigest("reconciliation")
	if _, err := e.pool.Exec(ctx, `INSERT INTO govar_reconciliation_tasks(task_id,request_id,reason_code,state,payload_hash)
 VALUES('worker-reconciliation','worker-enqueue','missing_delivery','PENDING',$1)`, reconciliationPayload); err != nil {
		t.Fatal(err)
	}
	type monetary struct {
		settled, reserved, carried int64
		active                     int
	}
	readMonetary := func() monetary {
		var value monetary
		if err := e.pool.QueryRow(ctx, `SELECT settled_micros,reserved_micros,carried_adjustment_micros,active_reservations FROM govar_tenants WHERE tenant_id=$1`, testTenant).Scan(
			&value.settled, &value.reserved, &value.carried, &value.active); err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := readMonetary()
	if n, err := e.EnqueueExpiredWork(ctx, 10); err != nil || n != 1 {
		t.Fatalf("expiry enqueue=(%d,%v)", n, err)
	}
	if n, err := e.EnqueueReconciliationWork(ctx, 10); err != nil || n != 1 {
		t.Fatalf("reconciliation enqueue=(%d,%v)", n, err)
	}
	if n, err := e.EnqueueOutboxRepairWork(ctx, 10); err != nil || n != 1 {
		t.Fatalf("outbox enqueue=(%d,%v)", n, err)
	}
	period := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	if inserted, err := e.EnqueuePeriodicWork(ctx, govarworker.KindCalibrationDrift, "tenant-a", period, workerTestDigest("calibration")); err != nil || !inserted {
		t.Fatalf("calibration enqueue=(%t,%v)", inserted, err)
	}
	if inserted, err := e.EnqueuePeriodicWork(ctx, govarworker.KindAuditCheckpoint, "tenant-a", period, workerTestDigest("audit")); err != nil || !inserted {
		t.Fatalf("audit enqueue=(%t,%v)", inserted, err)
	}
	var count, kinds int
	if err := e.pool.QueryRow(ctx, `SELECT count(*),count(DISTINCT kind) FROM govar_work_items`).Scan(&count, &kinds); err != nil {
		t.Fatal(err)
	}
	if count != 5 || kinds != 5 {
		t.Fatalf("work rows=%d kinds=%d, want five closed kinds", count, kinds)
	}
	if n, err := e.EnqueueExpiredWork(ctx, 10); err != nil || n != 0 {
		t.Fatalf("expiry replay=(%d,%v)", n, err)
	}
	if n, err := e.EnqueueReconciliationWork(ctx, 10); err != nil || n != 0 {
		t.Fatalf("reconciliation replay=(%d,%v)", n, err)
	}
	if n, err := e.EnqueueOutboxRepairWork(ctx, 10); err != nil || n != 0 {
		t.Fatalf("outbox replay=(%d,%v)", n, err)
	}
	if inserted, err := e.EnqueuePeriodicWork(ctx, govarworker.KindCalibrationDrift, "tenant-a", period, workerTestDigest("calibration")); err != nil || inserted {
		t.Fatalf("periodic replay=(%t,%v)", inserted, err)
	}
	if after := readMonetary(); after != before {
		t.Fatalf("worker enqueue mutated monetary state: before=%+v after=%+v", before, after)
	}
	conflicting := WorkerEnqueue{Kind: govarworker.KindCalibrationDrift, SourceIdentity: "tenant-a\x00" + period.Format(time.RFC3339Nano), DueAt: period, PayloadSHA256: workerTestDigest("different")}
	if _, err := e.EnqueueWorkerItem(ctx, conflicting); err == nil {
		t.Fatal("conflicting payload reused an existing durable source identity")
	}
}

func TestPostgresWorkerClaimUsesRealSkipLocked(t *testing.T) {
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"locked", "available"} {
		if inserted, err := e.EnqueueWorkerItem(ctx, WorkerEnqueue{Kind: govarworker.KindAuditCheckpoint, SourceIdentity: source, PayloadSHA256: workerTestDigest(source)}); err != nil || !inserted {
			t.Fatalf("enqueue %s=(%t,%v)", source, inserted, err)
		}
	}
	lockedSourceKey := eventPayloadHash("govar-work-source-v1", string(govarworker.KindAuditCheckpoint), "locked")
	lockedID := eventPayloadHash("govar-work-id-v1", string(govarworker.KindAuditCheckpoint), lockedSourceKey)
	owner, err := pgx.Connect(ctx, e.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	tx, err := owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var observed string
	if err := tx.QueryRow(ctx, `SELECT work_id FROM govar_work_items WHERE work_id=$1 FOR UPDATE`, lockedID).Scan(&observed); err != nil {
		t.Fatal(err)
	}
	items, err := e.ClaimWork(ctx, string(govarworker.KindAuditCheckpoint), 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID == lockedID {
		t.Fatalf("SKIP LOCKED claim=%+v", items)
	}
}

func TestPostgresWorkerLeaseTakeoverRetryAndDeadLetterPreserveLiability(t *testing.T) {
	ctx := context.Background()
	url := testDatabaseURL(t)
	e1, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e1.Close()
	if err := resetPostgresTestLedger(ctx, e1); err != nil {
		t.Fatal(err)
	}
	e2, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	e1.workerOwner, e2.workerOwner = "worker-one", "worker-two"
	if inserted, err := e1.EnqueueWorkerItem(ctx, WorkerEnqueue{Kind: govarworker.KindDeliveryReconciliation, SourceIdentity: "lease-takeover", LiabilityHeld: true, PayloadSHA256: workerTestDigest("lease-takeover")}); err != nil || !inserted {
		t.Fatalf("enqueue=(%t,%v)", inserted, err)
	}
	var first, second []govarworker.WorkItem
	var firstErr, secondErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		first, firstErr = e1.ClaimWork(ctx, string(govarworker.KindDeliveryReconciliation), 1, 200*time.Millisecond)
	}()
	go func() {
		defer wg.Done()
		second, secondErr = e2.ClaimWork(ctx, string(govarworker.KindDeliveryReconciliation), 1, 200*time.Millisecond)
	}()
	wg.Wait()
	if firstErr != nil || secondErr != nil || len(first)+len(second) != 1 {
		t.Fatalf("concurrent claims first=%+v/%v second=%+v/%v", first, firstErr, second, secondErr)
	}
	claimant, standby := e1, e2
	claimed := first
	if len(second) == 1 {
		claimant, standby, claimed = e2, e1, second
	}
	item := claimed[0]
	if item.Attempt != 1 || !item.LiabilityHeld || item.AuthoritativeReleaseEligible {
		t.Fatalf("initial claim=%+v", item)
	}
	if err := standby.RenewWork(ctx, item, time.Second); !errors.Is(err, errWorkerLeaseLost) {
		t.Fatalf("non-owner renewed lease: %v", err)
	}
	if err := claimant.RenewWork(ctx, item, 350*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := standby.pool.Exec(ctx, `SELECT pg_sleep(0.22)`); err != nil {
		t.Fatal(err)
	}
	if premature, err := standby.ClaimWork(ctx, string(govarworker.KindDeliveryReconciliation), 1, time.Second); err != nil || len(premature) != 0 {
		t.Fatalf("renewed lease taken early: %+v/%v", premature, err)
	}
	// Simulate the claiming process disappearing without acknowledging. Its
	// lease remains the sole recovery authority.
	claimant.Close()
	if _, err := standby.pool.Exec(ctx, `SELECT pg_sleep(0.18)`); err != nil {
		t.Fatal(err)
	}
	taken, err := standby.ClaimWork(ctx, string(govarworker.KindDeliveryReconciliation), 1, time.Second)
	if err != nil || len(taken) != 1 || taken[0].Attempt != 2 {
		t.Fatalf("expired lease takeover=%+v/%v", taken, err)
	}
	item = taken[0]
	if err := standby.CompleteWork(ctx, item, govarworker.WorkResult{State: govarworker.StateRetry, ReasonCode: "provider_unavailable", NextAttemptAt: time.Now().UTC().Add(150 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if early, err := standby.ClaimWork(ctx, string(govarworker.KindDeliveryReconciliation), 1, time.Second); err != nil || len(early) != 0 {
		t.Fatalf("retry claimed before due: %+v/%v", early, err)
	}
	if _, err := standby.pool.Exec(ctx, `SELECT pg_sleep(0.18)`); err != nil {
		t.Fatal(err)
	}
	retried, err := standby.ClaimWork(ctx, string(govarworker.KindDeliveryReconciliation), 1, time.Second)
	if err != nil || len(retried) != 1 || retried[0].Attempt != 3 {
		t.Fatalf("retry claim=%+v/%v", retried, err)
	}
	dead := govarworker.WorkResult{State: govarworker.StateDeadLetter, ReasonCode: "attempts_exhausted"}
	if err := standby.CompleteWork(ctx, retried[0], dead); err != nil {
		t.Fatal(err)
	}
	if err := standby.CompleteWork(ctx, retried[0], dead); err != nil {
		t.Fatalf("exact completion replay: %v", err)
	}
	var state, disposition string
	var held, release bool
	if err := standby.pool.QueryRow(ctx, `SELECT state,liability_held,authoritative_release_eligible,liability_disposition FROM govar_work_items WHERE work_id=$1`, retried[0].ID).Scan(&state, &held, &release, &disposition); err != nil {
		t.Fatal(err)
	}
	if state != "DEAD_LETTER" || !held || release || disposition != string(govarworker.LiabilityPreserve) {
		t.Fatalf("dead letter released liability: state=%s held=%t release=%t disposition=%s", state, held, release, disposition)
	}
}

func TestPostgresWorkerQueueSnapshotsAreGlobalCommittedAndIdentityFree(t *testing.T) {
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	workID := func(source string) string {
		sourceKey := eventPayloadHash("govar-work-source-v1", string(govarworker.KindAuditCheckpoint), source)
		return eventPayloadHash("govar-work-id-v1", string(govarworker.KindAuditCheckpoint), sourceKey)
	}
	for _, source := range []string{"snapshot-pending", "snapshot-retry", "snapshot-leased", "snapshot-completed", "snapshot-dead"} {
		if inserted, err := e.EnqueueWorkerItem(ctx, WorkerEnqueue{Kind: govarworker.KindAuditCheckpoint, SourceIdentity: source, PayloadSHA256: workerTestDigest(source)}); err != nil || !inserted {
			t.Fatalf("enqueue %s=(%t,%v)", source, inserted, err)
		}
	}
	if _, err := e.pool.Exec(ctx, `UPDATE govar_work_items SET next_attempt_at=clock_timestamp()-interval '5 seconds' WHERE work_id=$1`, workID("snapshot-pending")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE govar_work_items SET state='RETRY',next_attempt_at=clock_timestamp()-interval '2 seconds',reason_code='retry_due' WHERE work_id=$1`, workID("snapshot-retry")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE govar_work_items SET state='LEASED',lease_owner='snapshot-worker',lease_until=clock_timestamp()+interval '1 hour',attempts=1 WHERE work_id=$1`, workID("snapshot-leased")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE govar_work_items SET state='COMPLETED',reason_code='completed',completed_at=clock_timestamp(),completion_sha256=$2 WHERE work_id=$1`, workID("snapshot-completed"), workerTestDigest("completed-result")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE govar_work_items SET state='DEAD_LETTER',reason_code='dead_lettered',completed_at=clock_timestamp(),completion_sha256=$2 WHERE work_id=$1`, workID("snapshot-dead"), workerTestDigest("dead-result")); err != nil {
		t.Fatal(err)
	}
	readAudit := func(rows []WorkerQueueSnapshot) WorkerQueueSnapshot {
		t.Helper()
		if len(rows) != len(govarworker.AllKinds()) {
			t.Fatalf("queue kinds=%d want=%d", len(rows), len(govarworker.AllKinds()))
		}
		for _, row := range rows {
			if len(row.CountsByState) != len(workerPersistedStates) || row.CapturedAt.IsZero() {
				t.Fatalf("incomplete queue snapshot=%+v", row)
			}
			if row.Kind == govarworker.KindAuditCheckpoint {
				return row
			}
		}
		t.Fatal("audit-checkpoint queue snapshot missing")
		return WorkerQueueSnapshot{}
	}
	rows, err := e.WorkerQueueSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	audit := readAudit(rows)
	for _, state := range workerPersistedStates {
		if audit.CountsByState[state] != 1 {
			t.Fatalf("state %s count=%d want=1: %+v", state, audit.CountsByState[state], audit)
		}
	}
	if audit.OldestEligibleAge < 4*time.Second {
		t.Fatalf("oldest eligible age=%s want at least four seconds", audit.OldestEligibleAge)
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "snapshot-pending") || strings.Contains(string(payload), "snapshot-worker") || strings.Contains(string(payload), workerTestDigest("snapshot-pending")) {
		t.Fatalf("queue aggregate leaked identity: %s", payload)
	}

	writer, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback(ctx) }()
	if _, err := writer.Exec(ctx, `UPDATE govar_work_items SET state='LEASED',lease_owner='barrier-worker',lease_until=clock_timestamp()+interval '1 hour',attempts=1 WHERE work_id=$1`, workID("snapshot-pending")); err != nil {
		t.Fatal(err)
	}
	during, err := e.WorkerQueueSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	duringAudit := readAudit(during)
	if duringAudit.CountsByState["PENDING"] != 1 || duringAudit.CountsByState["LEASED"] != 1 {
		t.Fatalf("queue snapshot observed uncommitted writer: %+v", duringAudit)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := e.WorkerQueueSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	afterAudit := readAudit(after)
	if afterAudit.CountsByState["PENDING"] != 0 || afterAudit.CountsByState["LEASED"] != 2 {
		t.Fatalf("queue snapshot did not cross commit barrier: %+v", afterAudit)
	}
}

func TestPostgresVerifyAllTenantAuditsAllowsEmptyAndVerifiesStored(t *testing.T) {
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := e.VerifyAllTenantAudits(ctx); err != nil {
		t.Fatalf("empty ledger verification: %v", err)
	}
	if admitted, err := e.Admit(admitRequest("verify-all-audits", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates()); err != nil || admitted.Decision != DecisionAdmit {
		t.Fatalf("admit=(%+v,%v)", admitted, err)
	}
	if err := e.VerifyAllTenantAudits(ctx); err != nil {
		t.Fatalf("stored audit verification: %v", err)
	}
}
