package govar

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govarworker"
)

var workerTokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)
var workerReasonPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
var workerSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

var errWorkerLeaseLost = errors.New("worker lease is absent, expired, or owned by another worker")

func newWorkerOwner() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate worker identity: %w", err)
	}
	return "worker-" + hex.EncodeToString(bytes), nil
}

// WorkerEnqueue identifies a durable item without persisting the raw source
// identity. SourceIdentity is domain-hashed into source_key and work_id.
type WorkerEnqueue struct {
	Kind                         govarworker.Kind
	SourceIdentity               string
	RequestID                    string
	ProviderAttemptID            string
	DueAt                        time.Time
	LiabilityHeld                bool
	AuthoritativeReleaseEligible bool
	PayloadSHA256                string
}

func (e *PostgresEngine) ClaimWork(ctx context.Context, kind string, limit int, lease time.Duration) ([]govarworker.WorkItem, error) {
	workerKind := govarworker.Kind(kind)
	if !workerKind.Valid() || limit < 1 || limit > 10_000 || lease < time.Millisecond || lease > 24*time.Hour || !workerTokenPattern.MatchString(e.workerOwner) {
		return nil, errors.New("invalid durable worker claim parameters")
	}
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
WITH picked AS (
 SELECT w.work_id,
        CASE WHEN r.request_id IS NULL THEN w.liability_held
             ELSE (r.residual_hold_micros+r.rollover_guard_micros>0 AND r.state NOT IN('CANCELED_UNBILLED','FAILED_UNBILLED','EXPIRED_UNDISPATCHED','FINALIZED','LATE_FINALIZED')) END AS current_liability,
        CASE WHEN w.kind='expiry' AND r.state='RESERVED' AND r.outbox_state='PENDING' THEN TRUE ELSE FALSE END AS current_release_eligible
 FROM govar_work_items w LEFT JOIN govar_reservations r ON r.request_id=w.request_id
 WHERE w.kind=$1 AND ((w.state IN('PENDING','RETRY') AND w.next_attempt_at<=clock_timestamp()) OR (w.state='LEASED' AND w.lease_until<clock_timestamp()))
 ORDER BY CASE WHEN w.state='LEASED' THEN 0 ELSE 1 END,w.next_attempt_at,w.created_at,w.work_id
 FOR UPDATE OF w SKIP LOCKED LIMIT $2
)
UPDATE govar_work_items w SET state='LEASED',lease_owner=$3,
 lease_until=clock_timestamp()+($4::bigint*interval '1 microsecond'),attempts=w.attempts+1,
 liability_held=p.current_liability,authoritative_release_eligible=p.current_release_eligible,
 liability_disposition='PRESERVE',completion_sha256='',completed_at=NULL,updated_at=clock_timestamp()
FROM picked p WHERE w.work_id=p.work_id
RETURNING w.work_id,w.kind,COALESCE(w.request_id,''),w.provider_attempt_id,w.lease_owner,w.lease_until,w.attempts,
 w.liability_held,w.authoritative_release_eligible,w.payload_sha256`, kind, limit, e.workerOwner, lease.Microseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]govarworker.WorkItem, 0)
	for rows.Next() {
		var item govarworker.WorkItem
		if err := rows.Scan(&item.ID, &item.Kind, &item.RequestID, &item.ProviderAttemptID,
			&item.LeaseOwner, &item.LeaseUntil, &item.Attempt, &item.LiabilityHeld,
			&item.AuthoritativeReleaseEligible, &item.PayloadDigest); err != nil {
			return nil, err
		}
		item.LeaseUntil = item.LeaseUntil.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return items, nil
}

func (e *PostgresEngine) RenewWork(ctx context.Context, item govarworker.WorkItem, lease time.Duration) error {
	if item.ID == "" || item.LeaseOwner != e.workerOwner || lease < time.Millisecond || lease > 24*time.Hour {
		return errWorkerLeaseLost
	}
	result, err := e.pool.Exec(ctx, `UPDATE govar_work_items SET
 lease_until=clock_timestamp()+($3::bigint*interval '1 microsecond'),updated_at=clock_timestamp()
 WHERE work_id=$1 AND state='LEASED' AND lease_owner=$2 AND lease_until>=clock_timestamp()`, item.ID, item.LeaseOwner, lease.Microseconds())
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errWorkerLeaseLost
	}
	return nil
}

func (e *PostgresEngine) CompleteWork(ctx context.Context, item govarworker.WorkItem, result govarworker.WorkResult) error {
	if result.LiabilityDisposition == "" {
		result.LiabilityDisposition = govarworker.LiabilityPreserve
	}
	if item.ID == "" || item.LeaseOwner != e.workerOwner || !validWorkerResult(item, result) {
		return errors.New("invalid durable worker completion")
	}
	digest, err := workerCompletionDigest(result)
	if err != nil {
		return err
	}
	var next any
	if result.State == govarworker.StateRetry {
		next = result.NextAttemptAt.UTC()
	}
	command, err := e.pool.Exec(ctx, `UPDATE govar_work_items SET state=$3,lease_owner='',lease_until=NULL,
 next_attempt_at=CASE WHEN $3='RETRY' THEN $4::timestamptz ELSE next_attempt_at END,reason_code=$5,
 liability_disposition=$6,completion_sha256=$7,
 completed_at=CASE WHEN $3 IN('COMPLETED','DEAD_LETTER') THEN clock_timestamp() ELSE NULL END,
 updated_at=clock_timestamp()
 WHERE work_id=$1 AND state='LEASED' AND lease_owner=$2 AND lease_until>=clock_timestamp()
   AND liability_held=$8 AND authoritative_release_eligible=$9`, item.ID, item.LeaseOwner,
		string(result.State), next, result.ReasonCode, string(result.LiabilityDisposition), digest,
		item.LiabilityHeld, item.AuthoritativeReleaseEligible)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	// A response can be lost after PostgreSQL commits. The exact same completion
	// is an idempotent replay; every other stale-owner completion fails closed.
	var state, storedDigest string
	err = e.pool.QueryRow(ctx, `SELECT state,completion_sha256 FROM govar_work_items WHERE work_id=$1`, item.ID).Scan(&state, &storedDigest)
	if err == nil && state == string(result.State) && storedDigest == digest {
		return nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	return errWorkerLeaseLost
}

func validWorkerResult(item govarworker.WorkItem, result govarworker.WorkResult) bool {
	if !workerReasonPattern.MatchString(result.ReasonCode) {
		return false
	}
	switch result.State {
	case govarworker.StateCompleted:
		if !result.NextAttemptAt.IsZero() {
			return false
		}
	case govarworker.StateRetry:
		if result.NextAttemptAt.IsZero() || (item.LiabilityHeld && result.LiabilityDisposition != govarworker.LiabilityPreserve) {
			return false
		}
	case govarworker.StateDeadLetter:
		if !result.NextAttemptAt.IsZero() || (item.LiabilityHeld && result.LiabilityDisposition != govarworker.LiabilityPreserve) {
			return false
		}
	default:
		return false
	}
	if result.LiabilityDisposition == govarworker.LiabilityAuthoritativeRelease {
		return item.LiabilityHeld && item.AuthoritativeReleaseEligible && result.State == govarworker.StateCompleted
	}
	return result.LiabilityDisposition == govarworker.LiabilityPreserve
}

func workerCompletionDigest(result govarworker.WorkResult) (string, error) {
	canonical := struct {
		State                govarworker.WorkState
		ReasonCode           string
		NextAttemptAt        string
		LiabilityDisposition govarworker.LiabilityDisposition
	}{result.State, result.ReasonCode, "", result.LiabilityDisposition}
	if !result.NextAttemptAt.IsZero() {
		canonical.NextAttemptAt = result.NextAttemptAt.UTC().Format(time.RFC3339Nano)
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// EnqueueWorkerItem is idempotent for an exact source identity. A conflicting
// payload or liability binding is rejected instead of silently reused.
func (e *PostgresEngine) EnqueueWorkerItem(ctx context.Context, item WorkerEnqueue) (bool, error) {
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, err := enqueueWorkerItemTx(ctx, tx, item)
	if err != nil {
		return false, err
	}
	return inserted, tx.Commit(ctx)
}

func enqueueWorkerItemTx(ctx context.Context, tx pgx.Tx, item WorkerEnqueue) (bool, error) {
	if !item.Kind.Valid() || strings.TrimSpace(item.SourceIdentity) == "" || !workerSHA256Pattern.MatchString(item.PayloadSHA256) || (item.AuthoritativeReleaseEligible && !item.LiabilityHeld) {
		return false, errors.New("invalid durable worker enqueue")
	}
	sourceKey := eventPayloadHash("govar-work-source-v1", string(item.Kind), item.SourceIdentity)
	workID := eventPayloadHash("govar-work-id-v1", string(item.Kind), sourceKey)
	var request any
	if item.RequestID != "" {
		request = item.RequestID
	}
	var due any
	if !item.DueAt.IsZero() {
		due = item.DueAt.UTC()
	}
	command, err := tx.Exec(ctx, `INSERT INTO govar_work_items(
 work_id,kind,source_key,request_id,provider_attempt_id,next_attempt_at,liability_held,authoritative_release_eligible,payload_sha256)
 VALUES($1,$2,$3,$4,$5,COALESCE($6,clock_timestamp()),$7,$8,$9)
 ON CONFLICT(kind,source_key) DO NOTHING`, workID, string(item.Kind), sourceKey, request,
		item.ProviderAttemptID, due, item.LiabilityHeld, item.AuthoritativeReleaseEligible, item.PayloadSHA256)
	if err != nil {
		return false, err
	}
	if command.RowsAffected() == 1 {
		return true, nil
	}
	var storedRequest *string
	var storedAttempt, storedPayload string
	var storedHeld, storedRelease bool
	err = tx.QueryRow(ctx, `SELECT request_id,provider_attempt_id,payload_sha256,liability_held,authoritative_release_eligible
 FROM govar_work_items WHERE kind=$1 AND source_key=$2`, string(item.Kind), sourceKey).Scan(
		&storedRequest, &storedAttempt, &storedPayload, &storedHeld, &storedRelease)
	if err != nil {
		return false, err
	}
	wantRequest := ""
	if storedRequest != nil {
		wantRequest = *storedRequest
	}
	if wantRequest != item.RequestID || storedAttempt != item.ProviderAttemptID || storedPayload != item.PayloadSHA256 || storedHeld != item.LiabilityHeld || storedRelease != item.AuthoritativeReleaseEligible {
		return false, errors.New("durable worker source identity conflicts with existing item")
	}
	return false, nil
}

func (e *PostgresEngine) EnqueueExpiredWork(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 10_000 {
		return 0, errors.New("expiry enqueue limit must be in [1,10000]")
	}
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT request_id,provider_attempt_id,state,outbox_state,expiry,
 (residual_hold_micros+rollover_guard_micros>0) AS held
 FROM govar_reservations WHERE expiry<=clock_timestamp()
 AND ((state='RESERVED' AND outbox_state='PENDING') OR state IN('DISPATCH_PENDING','DISPATCHED','UNRESOLVED'))
 ORDER BY expiry,request_id FOR SHARE SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		request, attempt, state, outbox string
		expiry                          time.Time
		held                            bool
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.request, &c.attempt, &c.state, &c.outbox, &c.expiry, &c.held); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	inserted := 0
	for _, c := range candidates {
		payload := eventPayloadHash("govar-expiry-work-v1", c.request, c.attempt, c.state, c.outbox, c.expiry.UTC().Format(time.RFC3339Nano))
		created, err := enqueueWorkerItemTx(ctx, tx, WorkerEnqueue{Kind: govarworker.KindExpiry,
			SourceIdentity: c.request + "\x00" + c.expiry.UTC().Format(time.RFC3339Nano), RequestID: c.request,
			ProviderAttemptID: c.attempt, LiabilityHeld: c.held,
			AuthoritativeReleaseEligible: c.state == "RESERVED" && c.outbox == "PENDING", PayloadSHA256: payload})
		if err != nil {
			return 0, err
		}
		if created {
			inserted++
		}
	}
	return inserted, tx.Commit(ctx)
}

func (e *PostgresEngine) EnqueueReconciliationWork(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 10_000 {
		return 0, errors.New("reconciliation enqueue limit must be in [1,10000]")
	}
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT t.task_id,t.request_id,t.payload_hash,r.provider_attempt_id,
 (r.residual_hold_micros+r.rollover_guard_micros>0) AS held
 FROM govar_reconciliation_tasks t JOIN govar_reservations r ON r.request_id=t.request_id
 WHERE t.state='PENDING' ORDER BY t.created_at,t.task_id FOR SHARE OF t,r SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		task, request, payload, attempt string
		held                            bool
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.task, &c.request, &c.payload, &c.attempt, &c.held); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	inserted := 0
	for _, c := range candidates {
		created, err := enqueueWorkerItemTx(ctx, tx, WorkerEnqueue{Kind: govarworker.KindDeliveryReconciliation,
			SourceIdentity: c.task, RequestID: c.request, ProviderAttemptID: c.attempt,
			LiabilityHeld: c.held, PayloadSHA256: c.payload})
		if err != nil {
			return 0, err
		}
		if created {
			inserted++
		}
	}
	return inserted, tx.Commit(ctx)
}

func (e *PostgresEngine) EnqueueOutboxRepairWork(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 10_000 {
		return 0, errors.New("outbox enqueue limit must be in [1,10000]")
	}
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT o.outbox_id,o.version,o.request_id,o.provider_attempt_id,o.state,
 (r.residual_hold_micros+r.rollover_guard_micros>0) AS held
 FROM govar_outbox o JOIN govar_reservations r ON r.request_id=o.request_id
 WHERE o.state IN('PENDING','CLAIMED') ORDER BY o.updated_at,o.outbox_id FOR SHARE OF o,r SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		outbox, request, attempt, state string
		version                         int64
		held                            bool
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.outbox, &c.version, &c.request, &c.attempt, &c.state, &c.held); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	inserted := 0
	for _, c := range candidates {
		payload := eventPayloadHash("govar-outbox-repair-v1", c.outbox, fmt.Sprint(c.version), c.request, c.attempt, c.state)
		created, err := enqueueWorkerItemTx(ctx, tx, WorkerEnqueue{Kind: govarworker.KindOutboxRepair,
			SourceIdentity: c.outbox + "\x00" + fmt.Sprint(c.version), RequestID: c.request,
			ProviderAttemptID: c.attempt, LiabilityHeld: c.held, PayloadSHA256: payload})
		if err != nil {
			return 0, err
		}
		if created {
			inserted++
		}
	}
	return inserted, tx.Commit(ctx)
}

func (e *PostgresEngine) EnqueuePeriodicWork(ctx context.Context, kind govarworker.Kind, scope string, periodStart time.Time, payloadSHA256 string) (bool, error) {
	if (kind != govarworker.KindCalibrationDrift && kind != govarworker.KindAuditCheckpoint) || strings.TrimSpace(scope) == "" || periodStart.IsZero() {
		return false, errors.New("periodic work requires calibration-drift or audit-checkpoint kind, scope, and period")
	}
	return e.EnqueueWorkerItem(ctx, WorkerEnqueue{Kind: kind,
		SourceIdentity: scope + "\x00" + periodStart.UTC().Format(time.RFC3339Nano), DueAt: periodStart,
		PayloadSHA256: payloadSHA256})
}
