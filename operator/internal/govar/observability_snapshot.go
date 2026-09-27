package govar

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarworker"
)

// TenantObservabilitySnapshot is a read-only view of one committed ledger
// snapshot. ReservationComponentMicros and SettlementComponentMicros aggregate
// the current immutable reservation rows for the tenant; they are not an
// allocation of residual liability and therefore need not equal ReservedMicros.
type TenantObservabilitySnapshot struct {
	TenantID                   string                 `json:"tenant_id"`
	ReservedMicros             MoneyMicros            `json:"reserved_micros"`
	SettledMicros              MoneyMicros            `json:"settled_micros"`
	OutstandingLiabilityMicros MoneyMicros            `json:"outstanding_liability_micros"`
	CarriedDebtMicros          MoneyMicros            `json:"carried_debt_micros"`
	ActiveReservations         int                    `json:"active_reservations"`
	CurrentWindowID            string                 `json:"current_window_id,omitempty"`
	ReservationComponentMicros map[string]MoneyMicros `json:"reservation_component_micros"`
	SettlementComponentMicros  map[string]MoneyMicros `json:"settlement_component_micros"`
	CapturedAt                 time.Time              `json:"captured_at"`
}

// ComponentObservabilitySnapshot is global because component metrics have
// only a basis label. Publishing per-tenant component maps would make tenants,
// replicas, and restarts overwrite the same series with partial values.
type ComponentObservabilitySnapshot struct {
	ReservationMicros map[string]MoneyMicros `json:"reservation_micros"`
	SettlementMicros  map[string]MoneyMicros `json:"settlement_micros"`
	CapturedAt        time.Time              `json:"captured_at"`
}

var workerPersistedStates = []string{"PENDING", "LEASED", "RETRY", "COMPLETED", "DEAD_LETTER"}

// WorkerQueueSnapshot contains aggregate durable queue state only. Request,
// tenant, source, payload, and lease-owner identities are never returned.
type WorkerQueueSnapshot struct {
	Kind              govarworker.Kind `json:"kind"`
	CountsByState     map[string]int64 `json:"counts_by_state"`
	OldestEligibleAge time.Duration    `json:"oldest_eligible_age"`
	CapturedAt        time.Time        `json:"captured_at"`
}

func emptyWorkerQueueSnapshots(capturedAt time.Time) []WorkerQueueSnapshot {
	result := make([]WorkerQueueSnapshot, 0, len(govarworker.AllKinds()))
	for _, kind := range govarworker.AllKinds() {
		counts := make(map[string]int64, len(workerPersistedStates))
		for _, state := range workerPersistedStates {
			counts[state] = 0
		}
		result = append(result, WorkerQueueSnapshot{Kind: kind, CountsByState: counts, CapturedAt: capturedAt.UTC()})
	}
	return result
}

func emptyComponentSnapshot() map[string]MoneyMicros {
	components := make(map[string]MoneyMicros, len(govarpricing.AllBases))
	for _, basis := range govarpricing.AllBases {
		components[string(basis)] = 0
	}
	return components
}

func addSnapshotComponent(target map[string]MoneyMicros, basis string, amount int64) error {
	prior, ok := target[basis]
	if !ok || amount < 0 || prior > MoneyMicros(math.MaxInt64)-MoneyMicros(amount) {
		return errors.New("invalid or overflowing observability component aggregate")
	}
	target[basis] = prior + MoneyMicros(amount)
	return nil
}

// ObservabilitySnapshot mirrors the PostgreSQL API for explicitly enabled
// single-process development mode. The engine mutex is the commit barrier.
func (e *Engine) ObservabilitySnapshot(_ context.Context, tenantID string) (TenantObservabilitySnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	snapshot := TenantObservabilitySnapshot{TenantID: tenantID, ReservationComponentMicros: emptyComponentSnapshot(),
		SettlementComponentMicros: emptyComponentSnapshot(), CapturedAt: e.now().UTC()}
	if tenant := e.tenants[tenantID]; tenant != nil {
		snapshot.ReservedMicros = tenant.ReservedMicros
		snapshot.OutstandingLiabilityMicros = tenant.ReservedMicros
		snapshot.SettledMicros = tenant.SettledMicros
		snapshot.CarriedDebtMicros = tenant.CarriedAdjustmentMicros
		snapshot.ActiveReservations = len(tenant.Requests)
		snapshot.CurrentWindowID = tenant.CurrentWindowID
	}
	for _, reservation := range e.reservations {
		if reservation.TenantID != tenantID {
			continue
		}
		for _, component := range reservation.ReservedComponents {
			if err := addSnapshotComponent(snapshot.ReservationComponentMicros, string(component.Basis), component.ReservedMicros); err != nil {
				return TenantObservabilitySnapshot{}, err
			}
		}
		for _, component := range reservation.ActualComponents {
			if err := addSnapshotComponent(snapshot.SettlementComponentMicros, string(component.Basis), component.ActualMicros); err != nil {
				return TenantObservabilitySnapshot{}, err
			}
		}
	}
	return snapshot, nil
}

// ComponentObservabilitySnapshot mirrors the global PostgreSQL component view
// for explicitly enabled single-process development mode.
func (e *Engine) ComponentObservabilitySnapshot(_ context.Context) (ComponentObservabilitySnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	snapshot := ComponentObservabilitySnapshot{ReservationMicros: emptyComponentSnapshot(), SettlementMicros: emptyComponentSnapshot(), CapturedAt: e.now().UTC()}
	for _, reservation := range e.reservations {
		for _, component := range reservation.ReservedComponents {
			if err := addSnapshotComponent(snapshot.ReservationMicros, string(component.Basis), component.ReservedMicros); err != nil {
				return ComponentObservabilitySnapshot{}, err
			}
		}
		for _, component := range reservation.ActualComponents {
			if err := addSnapshotComponent(snapshot.SettlementMicros, string(component.Basis), component.ActualMicros); err != nil {
				return ComponentObservabilitySnapshot{}, err
			}
		}
	}
	return snapshot, nil
}

// WorkerQueueSnapshots returns a closed zero snapshot in development memory
// mode because that mode has no durable queue. Production queue observations
// are available only from PostgreSQL.
func (e *Engine) WorkerQueueSnapshots(_ context.Context) ([]WorkerQueueSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return emptyWorkerQueueSnapshots(e.now()), nil
}

// ObservabilitySnapshot returns tenant totals and component aggregates from one
// committed, repeatable-read, read-only PostgreSQL snapshot. Its successful
// transaction commit is the publication barrier for a caller.
func (e *PostgresEngine) ObservabilitySnapshot(ctx context.Context, tenantID string) (TenantObservabilitySnapshot, error) {
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return TenantObservabilitySnapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	snapshot := TenantObservabilitySnapshot{TenantID: tenantID, ReservationComponentMicros: emptyComponentSnapshot(), SettlementComponentMicros: emptyComponentSnapshot()}
	err = tx.QueryRow(ctx, `SELECT settled_micros,reserved_micros,carried_adjustment_micros,active_reservations,current_window_id,transaction_timestamp()
 FROM govar_tenants WHERE tenant_id=$1`, tenantID).Scan(&snapshot.SettledMicros, &snapshot.ReservedMicros,
		&snapshot.CarriedDebtMicros, &snapshot.ActiveReservations, &snapshot.CurrentWindowID, &snapshot.CapturedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&snapshot.CapturedAt); err != nil {
			return TenantObservabilitySnapshot{}, err
		}
	} else if err != nil {
		return TenantObservabilitySnapshot{}, err
	}
	snapshot.OutstandingLiabilityMicros = snapshot.ReservedMicros
	rows, err := tx.Query(ctx, `
SELECT component_kind,basis,amount FROM (
 SELECT 'reservation'::text AS component_kind,component->>'basis' AS basis,
        SUM((component->>'reserved_micros')::numeric)::bigint AS amount
   FROM govar_reservations r CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(r.reserved_components_json)='array' THEN r.reserved_components_json ELSE '[]'::jsonb END) component
  WHERE r.tenant_id=$1 GROUP BY component->>'basis'
 UNION ALL
 SELECT 'settlement'::text AS component_kind,component->>'basis' AS basis,
        SUM((component->>'actual_micros')::numeric)::bigint AS amount
   FROM govar_reservations r CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(r.actual_components_json)='array' THEN r.actual_components_json ELSE '[]'::jsonb END) component
  WHERE r.tenant_id=$1 GROUP BY component->>'basis'
) aggregates ORDER BY component_kind,basis`, tenantID)
	if err != nil {
		return TenantObservabilitySnapshot{}, err
	}
	for rows.Next() {
		var kind, basis string
		var amount int64
		if err := rows.Scan(&kind, &basis, &amount); err != nil {
			rows.Close()
			return TenantObservabilitySnapshot{}, err
		}
		target := snapshot.ReservationComponentMicros
		if kind == "settlement" {
			target = snapshot.SettlementComponentMicros
		} else if kind != "reservation" {
			rows.Close()
			return TenantObservabilitySnapshot{}, errors.New("unknown observability component aggregate")
		}
		if err := addSnapshotComponent(target, basis, amount); err != nil {
			rows.Close()
			return TenantObservabilitySnapshot{}, err
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return TenantObservabilitySnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TenantObservabilitySnapshot{}, err
	}
	snapshot.CapturedAt = snapshot.CapturedAt.UTC()
	return snapshot, nil
}

// ComponentObservabilitySnapshot returns the global component aggregates from
// one committed, repeatable-read, read-only PostgreSQL snapshot.
func (e *PostgresEngine) ComponentObservabilitySnapshot(ctx context.Context) (ComponentObservabilitySnapshot, error) {
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ComponentObservabilitySnapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	snapshot := ComponentObservabilitySnapshot{ReservationMicros: emptyComponentSnapshot(), SettlementMicros: emptyComponentSnapshot()}
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&snapshot.CapturedAt); err != nil {
		return ComponentObservabilitySnapshot{}, err
	}
	rows, err := tx.Query(ctx, `
SELECT component_kind,basis,amount FROM (
 SELECT 'reservation'::text AS component_kind,component->>'basis' AS basis,
        SUM((component->>'reserved_micros')::numeric)::bigint AS amount
   FROM govar_reservations r CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(r.reserved_components_json)='array' THEN r.reserved_components_json ELSE '[]'::jsonb END) component
  GROUP BY component->>'basis'
 UNION ALL
 SELECT 'settlement'::text AS component_kind,component->>'basis' AS basis,
        SUM((component->>'actual_micros')::numeric)::bigint AS amount
   FROM govar_reservations r CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(r.actual_components_json)='array' THEN r.actual_components_json ELSE '[]'::jsonb END) component
  GROUP BY component->>'basis'
) aggregates ORDER BY component_kind,basis`)
	if err != nil {
		return ComponentObservabilitySnapshot{}, err
	}
	for rows.Next() {
		var kind, basis string
		var amount int64
		if err := rows.Scan(&kind, &basis, &amount); err != nil {
			rows.Close()
			return ComponentObservabilitySnapshot{}, err
		}
		target := snapshot.ReservationMicros
		if kind == "settlement" {
			target = snapshot.SettlementMicros
		} else if kind != "reservation" {
			rows.Close()
			return ComponentObservabilitySnapshot{}, errors.New("unknown observability component aggregate")
		}
		if err := addSnapshotComponent(target, basis, amount); err != nil {
			rows.Close()
			return ComponentObservabilitySnapshot{}, err
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ComponentObservabilitySnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ComponentObservabilitySnapshot{}, err
	}
	snapshot.CapturedAt = snapshot.CapturedAt.UTC()
	return snapshot, nil
}

// WorkerQueueSnapshots returns every closed worker kind and state from one
// committed, repeatable-read, read-only PostgreSQL snapshot. Eligible age uses
// the database transaction clock: ready/retry due time or an expired lease.
func (e *PostgresEngine) WorkerQueueSnapshots(ctx context.Context) ([]WorkerQueueSnapshot, error) {
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var capturedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&capturedAt); err != nil {
		return nil, err
	}
	snapshots := emptyWorkerQueueSnapshots(capturedAt)
	byKind := make(map[govarworker.Kind]*WorkerQueueSnapshot, len(snapshots))
	for i := range snapshots {
		byKind[snapshots[i].Kind] = &snapshots[i]
	}
	rows, err := tx.Query(ctx, `SELECT kind,state,count(*),COALESCE(MAX(
 CASE WHEN state IN('PENDING','RETRY') AND next_attempt_at<=transaction_timestamp()
           THEN GREATEST(0,FLOOR(EXTRACT(EPOCH FROM (transaction_timestamp()-next_attempt_at))*1000000000)::bigint)
      WHEN state='LEASED' AND lease_until<transaction_timestamp()
           THEN GREATEST(0,FLOOR(EXTRACT(EPOCH FROM (transaction_timestamp()-lease_until))*1000000000)::bigint)
      ELSE 0 END),0)
 FROM govar_work_items GROUP BY kind,state ORDER BY kind,state`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var kind govarworker.Kind
		var state string
		var count, oldestNanos int64
		if err := rows.Scan(&kind, &state, &count, &oldestNanos); err != nil {
			rows.Close()
			return nil, err
		}
		snapshot, kindOK := byKind[kind]
		if !kindOK {
			rows.Close()
			return nil, errors.New("unknown or invalid durable worker aggregate")
		}
		_, stateOK := snapshot.CountsByState[state]
		if !stateOK || count < 0 || oldestNanos < 0 {
			rows.Close()
			return nil, errors.New("unknown or invalid durable worker aggregate")
		}
		snapshot.CountsByState[state] = count
		if age := time.Duration(oldestNanos); age > snapshot.OldestEligibleAge {
			snapshot.OldestEligibleAge = age
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return snapshots, nil
}
