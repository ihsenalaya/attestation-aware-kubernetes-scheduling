package govar

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func assertTransitionMetadata(t *testing.T, res Reservation, previous ReservationState, effective bool) {
	t.Helper()
	if res.PreviousState != previous || res.TransitionEffective != effective {
		t.Fatalf("transition metadata previous=%q effective=%t, want %q/%t; reservation=%+v",
			res.PreviousState, res.TransitionEffective, previous, effective, res)
	}
	payload, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "PreviousState") || strings.Contains(string(payload), "TransitionEffective") ||
		strings.Contains(string(payload), "previous_state") || strings.Contains(string(payload), "transition_effective") {
		t.Fatalf("response-only transition metadata leaked into JSON: %s", payload)
	}
}

func TestInMemoryTransitionMetadataDistinguishesCommittedAndDuplicateEvents(t *testing.T) {
	e := NewEngine()
	admit := mustAdmit(t, e, "metadata-dispatch", testTenant, testWorkload, defaultCandidates())
	claim := dispatchRequest("metadata-dispatch", "metadata-claim", admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)
	res, code, err := e.Dispatch(claim)
	if err != nil || code != ReasonDispatchClaimed {
		t.Fatalf("claim=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateReserved, true)
	res, code, err = e.Dispatch(claim)
	if err != nil || code != ReasonDuplicateEvent {
		t.Fatalf("claim replay=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateDispatchPending, false)

	deliver := dispatchRequest("metadata-dispatch", "metadata-deliver", admit.ProviderAttemptID, DispatchDelivered, testTenant, testWorkload)
	if _, _, err := e.Dispatch(deliver); err != nil {
		t.Fatal(err)
	}
	settle := settleRequest("metadata-dispatch", "metadata-settle", 2_000, 1, false, testTenant, testWorkload)
	res, code, err = e.Settle(settle)
	if err != nil || code != ReasonProvisionalSettlement {
		t.Fatalf("settle=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateDispatched, true)
	res, code, err = e.Settle(settle)
	if err != nil || code != ReasonSettlementDuplicate {
		t.Fatalf("settle replay=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateSettledProvisional, false)

	cancelAdmit := mustAdmit(t, e, "metadata-cancel", testTenant, testWorkload, defaultCandidates())
	cancel := cancelRequest("metadata-cancel", "metadata-cancel-event", false, testTenant, testWorkload)
	cancel.ProviderAttemptID = cancelAdmit.ProviderAttemptID
	res, code, err = e.Cancel(cancel)
	if err != nil || code != ReasonCanceled {
		t.Fatalf("cancel=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateReserved, true)
	res, code, err = e.Cancel(cancel)
	if err != nil || code != ReasonDuplicateEvent {
		t.Fatalf("cancel replay=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateCanceledUnbilled, false)

	expiring := mustAdmit(t, e, "metadata-expiry", testTenant, testWorkload, defaultCandidates())
	e.mu.Lock()
	r := e.reservations["metadata-expiry"]
	r.Expiry = time.Now().UTC().Add(-time.Minute)
	e.reservations["metadata-expiry"] = r
	e.mu.Unlock()
	reconciled := e.ReconcileExpired(context.Background(), time.Now().UTC())
	if len(reconciled) != 1 || reconciled[0].RequestID != "metadata-expiry" || reconciled[0].PreviousState != StateReserved || !reconciled[0].TransitionEffective {
		t.Fatalf("reconcile metadata=%+v admit=%+v", reconciled, expiring)
	}
	stored := e.reservations["metadata-expiry"]
	if stored.PreviousState != "" || stored.TransitionEffective {
		t.Fatalf("response metadata persisted in memory ledger: %+v", stored)
	}
}

func TestPostgresTransitionMetadataDistinguishesCommittedAndDuplicateEvents(t *testing.T) {
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	admit, err := e.Admit(admitRequest("pg-metadata", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || admit.Decision != DecisionAdmit {
		t.Fatalf("admit=(%+v,%v)", admit, err)
	}
	claim := dispatchRequest("pg-metadata", "pg-metadata-claim", admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)
	res, code, err := e.Dispatch(claim)
	if err != nil || code != ReasonDispatchClaimed {
		t.Fatalf("claim=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateReserved, true)
	res, code, err = e.Dispatch(claim)
	if err != nil || code != ReasonDuplicateEvent {
		t.Fatalf("claim replay=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateDispatchPending, false)
	if _, _, err := e.Dispatch(dispatchRequest("pg-metadata", "pg-metadata-deliver", admit.ProviderAttemptID, DispatchDelivered, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	settle := settleRequest("pg-metadata", "pg-metadata-settle", 2_000, 1, false, testTenant, testWorkload)
	res, code, err = e.Settle(settle)
	if err != nil || code != ReasonProvisionalSettlement {
		t.Fatalf("settle=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateDispatched, true)
	res, code, err = e.Settle(settle)
	if err != nil || code != ReasonSettlementDuplicate {
		t.Fatalf("settle replay=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateSettledProvisional, false)

	cancelAdmit, err := e.Admit(admitRequest("pg-metadata-cancel", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || cancelAdmit.Decision != DecisionAdmit {
		t.Fatalf("cancel admit=(%+v,%v)", cancelAdmit, err)
	}
	cancel := cancelRequest("pg-metadata-cancel", "pg-metadata-cancel-event", false, testTenant, testWorkload)
	cancel.ProviderAttemptID = cancelAdmit.ProviderAttemptID
	res, code, err = e.Cancel(cancel)
	if err != nil || code != ReasonCanceled {
		t.Fatalf("cancel=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateReserved, true)
	res, code, err = e.Cancel(cancel)
	if err != nil || code != ReasonDuplicateEvent {
		t.Fatalf("cancel replay=(%+v,%s,%v)", res, code, err)
	}
	assertTransitionMetadata(t, res, StateCanceledUnbilled, false)

	expiryAdmit, err := e.Admit(admitRequest("pg-metadata-expiry", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || expiryAdmit.Decision != DecisionAdmit {
		t.Fatalf("expiry admit=(%+v,%v)", expiryAdmit, err)
	}
	if err := postgresOwnerExec(ctx, e, `UPDATE govar_reservations SET expiry=clock_timestamp()-interval '1 second' WHERE request_id='pg-metadata-expiry'`); err != nil {
		t.Fatal(err)
	}
	reconciled, err := e.ReconcileExpired(ctx, time.Now().UTC())
	if err != nil || len(reconciled) != 1 || reconciled[0].RequestID != "pg-metadata-expiry" || reconciled[0].PreviousState != StateReserved || !reconciled[0].TransitionEffective {
		t.Fatalf("reconcile metadata=%+v err=%v", reconciled, err)
	}
	loaded, err := loadReservationTxForMetadataTest(ctx, e, "pg-metadata-expiry")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PreviousState != "" || loaded.TransitionEffective {
		t.Fatalf("response metadata persisted in PostgreSQL ledger: %+v", loaded)
	}
}

func loadReservationTxForMetadataTest(ctx context.Context, e *PostgresEngine, requestID string) (Reservation, error) {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return Reservation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return loadReservationTx(ctx, tx, requestID, false)
}
