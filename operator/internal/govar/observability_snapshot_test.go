package govar

import (
	"context"
	"testing"
)

func TestInMemoryObservabilitySnapshotMirrorsCommittedLedger(t *testing.T) {
	ctx := context.Background()
	e := NewEngine()
	admit := mustAdmit(t, e, "snapshot-memory", testTenant, testWorkload, defaultCandidates())
	snapshot, err := e.ObservabilitySnapshot(ctx, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReservedMicros != admit.ReservedCostMicros || snapshot.OutstandingLiabilityMicros != admit.ReservedCostMicros || snapshot.SettledMicros != 0 || snapshot.ActiveReservations != 1 || snapshot.CapturedAt.IsZero() {
		t.Fatalf("reservation snapshot=%+v", snapshot)
	}
	stored := e.reservations["snapshot-memory"]
	for _, component := range stored.ReservedComponents {
		if got := snapshot.ReservationComponentMicros[string(component.Basis)]; got != MoneyMicros(component.ReservedMicros) {
			t.Fatalf("reserved component %s=%d want=%d", component.Basis, got, component.ReservedMicros)
		}
	}
	if _, _, err := e.Dispatch(dispatchRequest("snapshot-memory", "snapshot-claim", admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Dispatch(dispatchRequest("snapshot-memory", "snapshot-deliver", admit.ProviderAttemptID, DispatchDelivered, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Settle(settleRequest("snapshot-memory", "snapshot-settle", 2_000, 1, false, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = e.ObservabilitySnapshot(ctx, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SettledMicros != 2_000 || snapshot.ReservedMicros != admit.ReservedCostMicros-2_000 {
		t.Fatalf("settlement snapshot=%+v", snapshot)
	}
	stored = e.reservations["snapshot-memory"]
	for _, component := range stored.ActualComponents {
		if got := snapshot.SettlementComponentMicros[string(component.Basis)]; got != MoneyMicros(component.ActualMicros) {
			t.Fatalf("settled component %s=%d want=%d", component.Basis, got, component.ActualMicros)
		}
	}
	if second := mustAdmit(t, e, "snapshot-memory-other", "tenant-b", "workload-b", defaultCandidates()); second.Decision != DecisionAdmit {
		t.Fatalf("second tenant admission=%+v", second)
	}
	global, err := e.ComponentObservabilitySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantReserved := emptyComponentSnapshot()
	wantSettled := emptyComponentSnapshot()
	for _, reservation := range e.reservations {
		for _, component := range reservation.ReservedComponents {
			wantReserved[string(component.Basis)] += MoneyMicros(component.ReservedMicros)
		}
		for _, component := range reservation.ActualComponents {
			wantSettled[string(component.Basis)] += MoneyMicros(component.ActualMicros)
		}
	}
	for basis := range wantReserved {
		if global.ReservationMicros[basis] != wantReserved[basis] || global.SettlementMicros[basis] != wantSettled[basis] {
			t.Fatalf("global component %s=%d/%d want=%d/%d", basis, global.ReservationMicros[basis], global.SettlementMicros[basis], wantReserved[basis], wantSettled[basis])
		}
	}
}

func TestPostgresObservabilitySnapshotHasCommittedMVCCBarrier(t *testing.T) {
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := resetPostgresTestLedger(ctx, e); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resetPostgresTestLedger(ctx, e); err != nil {
			t.Errorf("reset after observability barrier: %v", err)
		}
	}()
	admit, err := e.Admit(admitRequest("snapshot-pg", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || admit.Decision != DecisionAdmit {
		t.Fatalf("admit=(%+v,%v)", admit, err)
	}
	before, err := e.ObservabilitySnapshot(ctx, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if before.ReservedMicros != admit.ReservedCostMicros || before.OutstandingLiabilityMicros != admit.ReservedCostMicros || before.CapturedAt.IsZero() {
		t.Fatalf("initial snapshot=%+v", before)
	}
	other, err := e.Admit(admitRequest("snapshot-pg-other", "tenant-b", "workload-b"), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil || other.Decision != DecisionAdmit {
		t.Fatalf("other tenant admit=(%+v,%v)", other, err)
	}
	beforeComponents, err := e.ComponentObservabilitySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback(ctx) }()
	if _, err := writer.Exec(ctx, `UPDATE govar_tenants SET settled_micros=11,reserved_micros=17,carried_adjustment_micros=3 WHERE tenant_id=$1`, testTenant); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(ctx, `UPDATE govar_reservations SET
 reserved_components_json=jsonb_build_array(jsonb_build_object('basis','input_tokens','quantity',17,'price_micros_per_unit',1,'unit_denominator',1,'reserved_micros',17)),
 actual_components_json=jsonb_build_array(jsonb_build_object('basis','output_tokens','quantity',11,'price_micros_per_unit',1,'unit_denominator',1,'actual_micros',11,'reserved_quantity',11,'exceeded_bound',false))
 WHERE request_id='snapshot-pg'`); err != nil {
		t.Fatal(err)
	}
	// The read-only transaction starts while the writer is uncommitted. MVCC
	// must expose the prior committed row without blocking on the writer.
	during, err := e.ObservabilitySnapshot(ctx, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if during.ReservedMicros != before.ReservedMicros || during.SettledMicros != before.SettledMicros ||
		during.ReservationComponentMicros["input_tokens"] != before.ReservationComponentMicros["input_tokens"] ||
		during.SettlementComponentMicros["output_tokens"] != before.SettlementComponentMicros["output_tokens"] {
		t.Fatalf("snapshot observed uncommitted writer: before=%+v during=%+v", before, during)
	}
	duringComponents, err := e.ComponentObservabilitySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for basis := range beforeComponents.ReservationMicros {
		if duringComponents.ReservationMicros[basis] != beforeComponents.ReservationMicros[basis] || duringComponents.SettlementMicros[basis] != beforeComponents.SettlementMicros[basis] {
			t.Fatalf("global component snapshot observed uncommitted writer for %s: before=%+v during=%+v", basis, beforeComponents, duringComponents)
		}
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := e.ObservabilitySnapshot(ctx, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if after.ReservedMicros != 17 || after.OutstandingLiabilityMicros != 17 || after.SettledMicros != 11 || after.CarriedDebtMicros != 3 ||
		after.ReservationComponentMicros["input_tokens"] != 17 || after.SettlementComponentMicros["output_tokens"] != 11 {
		t.Fatalf("snapshot did not cross committed barrier: %+v", after)
	}
	afterComponents, err := e.ComponentObservabilitySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstReserved := before.ReservationComponentMicros["input_tokens"]
	if afterComponents.ReservationMicros["input_tokens"] != beforeComponents.ReservationMicros["input_tokens"]-firstReserved+17 ||
		afterComponents.SettlementMicros["output_tokens"] != beforeComponents.SettlementMicros["output_tokens"]+11 {
		t.Fatalf("global component snapshot did not cross committed barrier: before=%+v after=%+v", beforeComponents, afterComponents)
	}
}
