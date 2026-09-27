package govar

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/jackc/pgx/v5"
)

func TestWindowBoundarySettlementOrderIsConservative(t *testing.T) {
	for _, rolloverFirst := range []bool{false, true} {
		name := "settlement_first"
		if rolloverFirst {
			name = "rollover_first"
		}
		t.Run(name, func(t *testing.T) {
			e := NewEngine()
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			e.now = func() time.Time { return now }
			b := defaultBudget()
			b.Spec.Period = "daily"
			a, err := e.Admit(admitRequest("clock-late", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates())
			if err != nil {
				t.Fatal(err)
			}
			_, _, _ = e.Dispatch(dispatchRequest("clock-late", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload))
			now = now.Add(24 * time.Hour)
			if rolloverFirst {
				_ = e.Liability(testTenant)
			}
			usage := settleRequest("clock-late", "late-final", 2_000, 1, true, testTenant, testWorkload)
			res, code, err := e.Settle(usage)
			if err != nil || code != ReasonLateSettlement || res.State != StateLateFinalized {
				t.Fatalf("late final=(%+v,%s,%v)", res, code, err)
			}
			got := e.Liability(testTenant)
			if got.CarriedAdjustmentMicros != 2_000 || got.SettledSpendMicros != 0 || got.CurrentWindowID != "daily:2026-01-02" {
				t.Fatalf("clock-derived rollover=%+v", got)
			}
		})
	}
}

func TestFixedCohortFallbackStopsAdvertisingRisk(t *testing.T) {
	e := NewEngine()
	_ = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey)
	_ = e.ConfigureCohortRuntime(testCohortSoftwareHash)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	r := admitRequest("drift-risk", testTenant, testWorkload)
	r.CohortID = "risk"
	c := FrozenCohort{TenantID: testTenant, CohortID: "risk", Size: 1, TenantRiskPPB: 50_000_000, Slots: []FrozenCohortSlot{{0, r.RequestID, OpportunityDigest(r), 1_000_000_000}}, DataHash: eventPayloadHash("d"), ConfigHash: eventPayloadHash("c"), ProtocolHash: eventPayloadHash("p"), FrozenAt: now.Add(-time.Hour)}
	c = mustSignCohort(t, c)
	if err := e.RegisterFrozenCohort(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	routing := typedAdaptiveRouting(aiopsv1alpha1.GOVARReservationFixedCohort, 500, now, defaultCandidates()[0])
	bindTypedCohort(&routing, c)
	routing.Status.GOVAR.Calibration.Support = 1
	resp, err := e.Admit(r, defaultBudget(), routing, defaultCandidates())
	if err != nil || resp.Decision != DecisionAdmit || resp.ReservationMode != "strict_provider_cap" || resp.AllocatedRiskPPB != 0 {
		t.Fatalf("fallback risk=%+v err=%v", resp, err)
	}
}

func TestAuthorityProofsRejectTamperingAndUnsignedCohorts(t *testing.T) {
	e := NewEngine()
	_ = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey)
	_ = e.ConfigureCohortRuntime(testCohortSoftwareHash)
	now := time.Date(2026, 1, 31, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	r := admitRequest("unsigned", testTenant, testWorkload)
	c := FrozenCohort{TenantID: testTenant, CohortID: "u", Size: 1, TenantRiskPPB: 1, Slots: []FrozenCohortSlot{{0, r.RequestID, OpportunityDigest(r), 1_000_000_000}}, DataHash: eventPayloadHash("d"), ConfigHash: eventPayloadHash("c"), ProtocolHash: eventPayloadHash("p"), FrozenAt: now.Add(-time.Hour)}
	if err := e.RegisterFrozenCohort(context.Background(), c); err == nil {
		t.Fatal("unsigned cohort accepted")
	}
	a := BudgetAdjustment{AdjustmentID: "tamper", TenantID: testTenant, WindowID: "daily:2026-01-01", NewBudgetMicros: 10, AuthorizedBy: "admin", Reason: "proof", ApprovedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute)}
	a = SignBudgetAdjustment(a, "test-authority", testLedgerAuthorityKey)
	a.NewBudgetMicros++
	if err := e.AdjustBudget(context.Background(), a); err == nil {
		t.Fatal("tampered adjustment proof accepted")
	}
	if _, err := e.Admit(admitRequest("adjust-window", testTenant, testWorkload), defaultBudget(), defaultRoutingAt(now), defaultCandidates()); err != nil {
		t.Fatal(err)
	}
	oldWindow := e.Liability(testTenant).CurrentWindowID
	old := BudgetAdjustment{AdjustmentID: "old-window", TenantID: testTenant, WindowID: oldWindow, NewBudgetMicros: 100_000_000, AuthorizedBy: "admin", Reason: "window-bound", ApprovedAt: now, ExpiresAt: now.Add(25 * time.Hour)}
	old = SignBudgetAdjustment(old, "test-authority", testLedgerAuthorityKey)
	now = now.Add(24 * time.Hour)
	if err := e.AdjustBudget(context.Background(), old); err == nil {
		t.Fatal("old-window adjustment accepted after server-clock rollover")
	}
}

func TestCorrectionChainAndNoRetryInvariant(t *testing.T) {
	e := NewEngine()
	a := mustAdmit(t, e, "chain", testTenant, testWorkload, defaultCandidates())
	_, _, _ = e.Dispatch(dispatchRequest("chain", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload))
	if _, _, err := e.Settle(settleRequest("chain", "base", 1_000, 1, false, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	gap := settleRequest("chain", "gap", 1_100, 3, false, testTenant, testWorkload)
	gap.PredecessorEventID = "base"
	if _, code, err := e.Settle(gap); err == nil || code != ReasonInvalidTransition {
		t.Fatalf("version gap=(%s,%v)", code, err)
	}
	wrong := settleRequest("chain", "wrong", 1_100, 2, false, testTenant, testWorkload)
	wrong.PredecessorEventID = "other"
	if _, code, err := e.Settle(wrong); err == nil || code != ReasonInvalidTransition {
		t.Fatalf("wrong predecessor=(%s,%v)", code, err)
	}
	if code, err := e.ReserveRetry(context.Background(), "chain"); err == nil || code != ReasonRetriesDisabled {
		t.Fatalf("retry=(%s,%v)", code, err)
	}
	bad := dispatchRequest("chain", "attempt2", "chain:attempt:2", DispatchDelivered, testTenant, testWorkload)
	if _, code, err := e.Dispatch(bad); err == nil || code != ReasonInvalidTransition {
		t.Fatalf("attempt2=(%s,%v)", code, err)
	}
}

func TestPostgresConcurrentExactAdmissionReplaysDeterministically(t *testing.T) {
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
	const n = 20
	var wg sync.WaitGroup
	responses := make(chan AdmitResponse, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.Admit(admitRequest("same", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
			responses <- r
			errs <- err
		}()
	}
	wg.Wait()
	close(responses)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("exact duplicate error: %v", err)
		}
	}
	first, dups := 0, 0
	for r := range responses {
		switch r.ReasonCode {
		case ReasonHighestUtility:
			first++
		case ReasonDuplicateRequest:
			dups++
		}
	}
	if first != 1 || dups != n-1 {
		t.Fatalf("first=%d duplicates=%d", first, dups)
	}
}

func TestPostgresWindowBoundarySettlementOrderIsConservative(t *testing.T) {
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
	for _, rolloverFirst := range []bool{false, true} {
		name := "settlement_first"
		if rolloverFirst {
			name = "rollover_first"
		}
		t.Run(name, func(t *testing.T) {
			_ = resetPostgresTestLedger(ctx, e)
			now := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
			e.now = func() time.Time { return now }
			b := defaultBudget()
			b.Spec.Period = "daily"
			a, err := e.Admit(admitRequest("pg-clock-late", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates())
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = e.Dispatch(dispatchRequest("pg-clock-late", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
				t.Fatal(err)
			}
			now = now.Add(24 * time.Hour)
			if rolloverFirst {
				if _, err = e.LiabilityWithError(testTenant); err != nil {
					t.Fatal(err)
				}
			}
			res, _, err := e.Settle(settleRequest("pg-clock-late", "late", 2_000, 1, true, testTenant, testWorkload))
			if err != nil || res.State != StateLateFinalized {
				t.Fatalf("late=(%+v,%v)", res, err)
			}
			got, err := e.LiabilityWithError(testTenant)
			if err != nil || got.CurrentWindowID != "daily:2026-02-02" || got.CarriedAdjustmentMicros != 2_000 || got.SettledSpendMicros != 0 {
				t.Fatalf("liability=(%+v,%v)", got, err)
			}
		})
	}
}

func TestPostgresCorrectionCohortAndAuthorityCounterexamples(t *testing.T) {
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
	now := time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	if err = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey); err != nil {
		t.Fatal(err)
	}
	if err = e.ConfigureCohortRuntime(testCohortSoftwareHash); err != nil {
		t.Fatal(err)
	}

	r := admitRequest("pg-drift-risk", testTenant, testWorkload)
	r.CohortID = "risk"
	c := FrozenCohort{TenantID: testTenant, CohortID: "risk", Size: 1, TenantRiskPPB: 50_000_000, Slots: []FrozenCohortSlot{{0, r.RequestID, OpportunityDigest(r), 1_000_000_000}}, DataHash: eventPayloadHash("d"), ConfigHash: eventPayloadHash("c"), ProtocolHash: eventPayloadHash("p"), FrozenAt: now.Add(-time.Hour)}
	c = mustSignCohort(t, c)
	if err = e.RegisterFrozenCohort(ctx, c); err != nil {
		t.Fatal(err)
	}
	routing := typedAdaptiveRouting(aiopsv1alpha1.GOVARReservationFixedCohort, 500, now, defaultCandidates()[0])
	bindTypedCohort(&routing, c)
	routing.Status.GOVAR.Calibration.Support = 1
	resp, err := e.Admit(r, defaultBudget(), routing, defaultCandidates())
	if err != nil || resp.ReservationMode != "strict_provider_cap" || resp.AllocatedRiskPPB != 0 {
		t.Fatalf("PostgreSQL fallback risk=%+v err=%v", resp, err)
	}

	if _, _, err = e.Dispatch(dispatchRequest(r.RequestID, "claim", resp.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.Settle(settleRequest(r.RequestID, "base", 1_000, 1, false, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	gap := settleRequest(r.RequestID, "gap", 1_100, 3, false, testTenant, testWorkload)
	gap.PredecessorEventID = "base"
	if _, code, err := e.Settle(gap); err == nil || code != ReasonInvalidTransition {
		t.Fatalf("PostgreSQL version gap=(%s,%v)", code, err)
	}
	wrong := settleRequest(r.RequestID, "wrong", 1_100, 2, false, testTenant, testWorkload)
	wrong.PredecessorEventID = "other"
	if _, code, err := e.Settle(wrong); err == nil || code != ReasonInvalidTransition {
		t.Fatalf("PostgreSQL wrong predecessor=(%s,%v)", code, err)
	}

	unsigned := c
	unsigned.CohortID = "unsigned"
	unsigned.AuthorityKeyID, unsigned.AuthorityProof = "", ""
	if err = e.RegisterFrozenCohort(ctx, unsigned); err == nil {
		t.Fatal("PostgreSQL accepted unsigned cohort")
	}
	liability, err := e.LiabilityWithError(testTenant)
	if err != nil {
		t.Fatal(err)
	}
	adj := BudgetAdjustment{AdjustmentID: "tampered", TenantID: testTenant, WindowID: liability.CurrentWindowID, NewBudgetMicros: 100_000_000, AuthorizedBy: "admin", Reason: "proof", ApprovedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute)}
	adj = SignBudgetAdjustment(adj, "test-authority", testLedgerAuthorityKey)
	adj.NewBudgetMicros++
	if err = e.AdjustBudget(ctx, adj); err == nil {
		t.Fatal("PostgreSQL accepted tampered adjustment proof")
	}
	old := BudgetAdjustment{AdjustmentID: "pg-old-window", TenantID: testTenant, WindowID: liability.CurrentWindowID, NewBudgetMicros: 100_000_000, AuthorizedBy: "admin", Reason: "window-bound", ApprovedAt: now, ExpiresAt: now.Add(25 * time.Hour)}
	old = SignBudgetAdjustment(old, "test-authority", testLedgerAuthorityKey)
	now = now.Add(24 * time.Hour)
	if err = e.AdjustBudget(ctx, old); err == nil {
		t.Fatal("PostgreSQL accepted old-window adjustment after server-clock rollover")
	}
}

func TestPostgresStartupRejectsTamperedAggregates(t *testing.T) {
	url := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	e, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	_ = resetPostgresTestLedger(ctx, e)
	_, err = e.Admit(admitRequest("tamper-aggregate", testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	err = postgresOwnerExec(ctx, e, `UPDATE govar_tenants SET reserved_micros=reserved_micros+1 WHERE tenant_id=$1`, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	if bad, err := NewPostgresEngine(ctx, url); err == nil {
		bad.Close()
		t.Fatal("tampered aggregate passed startup")
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `UPDATE govar_tenants SET reserved_micros=reserved_micros-1 WHERE tenant_id=$1`, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	good, err := NewPostgresEngine(ctx, url)
	if err != nil {
		t.Fatalf("restored aggregate did not restart: %v", err)
	}
	good.Close()
	if _, err = conn.Exec(ctx, `UPDATE govar_outbox SET state='CLAIMED' WHERE request_id='tamper-aggregate'`); err != nil {
		t.Fatal(err)
	}
	if bad, err := NewPostgresEngine(ctx, url); err == nil {
		bad.Close()
		t.Fatal("reservation/outbox state mismatch passed startup")
	}
	if _, err = conn.Exec(ctx, `UPDATE govar_outbox SET state='PENDING' WHERE request_id='tamper-aggregate'`); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `ALTER TABLE govar_outbox DROP CONSTRAINT govar_outbox_request_fk`); err != nil {
		t.Fatal(err)
	}
	if bad, err := NewPostgresEngine(ctx, url); err == nil {
		bad.Close()
		t.Fatal("v5 schema with missing safety foreign key passed startup")
	}
	if _, err = conn.Exec(ctx, `ALTER TABLE govar_outbox ADD CONSTRAINT govar_outbox_request_fk FOREIGN KEY(request_id) REFERENCES govar_reservations(request_id) ON DELETE RESTRICT`); err != nil {
		t.Fatal(err)
	}
}
