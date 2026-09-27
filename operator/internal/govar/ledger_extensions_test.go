package govar

import (
	"context"
	"testing"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var testLedgerAuthorityKey = []byte("0123456789abcdef0123456789abcdef")
var testCohortSoftwareHash = eventPayloadHash("govar-test-software")

func mustSignCohort(t *testing.T, c FrozenCohort) FrozenCohort {
	t.Helper()
	c.LedgerLayoutID = LedgerLayoutID
	c.RouteSnapshotSchema = RouteSnapshotSchemaID
	c.SoftwareHash = testCohortSoftwareHash
	signed, err := SignFrozenCohort(c, "test-authority", testLedgerAuthorityKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func TestFrozenCohortIsPreOutcomeImmutableAndSlotBound(t *testing.T) {
	e := NewEngine()
	_ = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey)
	_ = e.ConfigureCohortRuntime(testCohortSoftwareHash)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	req := admitRequest("cohort-r0", testTenant, testWorkload)
	req.CohortID = "frozen-1"
	req.CohortIndex = 0
	c := FrozenCohort{TenantID: testTenant, CohortID: "frozen-1", Size: 1, TenantRiskPPB: 10_000_000,
		Slots:    []FrozenCohortSlot{{Index: 0, RequestID: req.RequestID, OpportunityDigest: OpportunityDigest(req), WeightPPB: 1_000_000_000}},
		DataHash: eventPayloadHash("data"), ConfigHash: eventPayloadHash("config"), ProtocolHash: eventPayloadHash("protocol"), FrozenAt: now.Add(-time.Hour)}
	c = mustSignCohort(t, c)
	if err := e.RegisterFrozenCohort(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterFrozenCohort(context.Background(), c); err != nil {
		t.Fatalf("exact immutable replay: %v", err)
	}
	changed := c
	changed.TenantRiskPPB++
	if err := e.RegisterFrozenCohort(context.Background(), changed); err == nil {
		t.Fatal("conflicting cohort mutation accepted")
	}
	routing := typedAdaptiveRouting(aiopsv1alpha1.GOVARReservationFixedCohort, 1200, now, defaultCandidates()[0])
	bindTypedCohort(&routing, c)
	resp, err := e.Admit(req, defaultBudget(), routing, defaultCandidates())
	if err != nil || resp.Decision != DecisionAdmit || resp.AllocatedRiskPPB != 10_000_000 {
		t.Fatalf("fixed admission=(%+v,%v)", resp, err)
	}
	exported, err := e.ExportFrozenCohort(context.Background(), testTenant, "frozen-1")
	if err != nil || exported.RegistryDigest == "" || len(exported.Slots) != 1 {
		t.Fatalf("export=(%+v,%v)", exported, err)
	}
}

func TestFrozenCohortRejectsAbsentFutureMismatchedAndAnnotationAuthority(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	newCase := func() (*Engine, AdmitRequest, FrozenCohort) {
		e := NewEngine()
		_ = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey)
		_ = e.ConfigureCohortRuntime(testCohortSoftwareHash)
		e.now = func() time.Time { return now }
		r := admitRequest("c-r", testTenant, testWorkload)
		r.CohortID = "c"
		r.CohortIndex = 0
		c := FrozenCohort{TenantID: testTenant, CohortID: "c", Size: 1, TenantRiskPPB: 1, Slots: []FrozenCohortSlot{{0, r.RequestID, OpportunityDigest(r), 1_000_000_000}}, DataHash: eventPayloadHash("d"), ConfigHash: eventPayloadHash("c"), ProtocolHash: eventPayloadHash("p"), FrozenAt: now.Add(-time.Second)}
		return e, r, mustSignCohort(t, c)
	}
	e, r, absent := newCase()
	routing := typedAdaptiveRouting(aiopsv1alpha1.GOVARReservationFixedCohort, 1000, now, defaultCandidates()[0])
	bindTypedCohort(&routing, absent)
	resp, _ := e.Admit(r, defaultBudget(), routing, defaultCandidates())
	if resp.Decision != DecisionAbstain {
		t.Fatalf("absent cohort=%+v", resp)
	}
	e, r, c := newCase()
	c.FrozenAt = now.Add(time.Second)
	if err := e.RegisterFrozenCohort(context.Background(), c); err == nil {
		t.Fatal("future frozen_at was accepted at server registration")
	}
	e, r, c = newCase()
	if err := e.RegisterFrozenCohort(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	bindTypedCohort(&routing, c)
	r.MaxOutputTokens++
	resp, _ = e.Admit(r, defaultBudget(), routing, defaultCandidates())
	if resp.Decision != DecisionAbstain {
		t.Fatalf("mismatch=%+v", resp)
	}
	e, r, c = newCase()
	if err := e.RegisterFrozenCohort(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	bindTypedCohort(&routing, c)
	routing.Annotations = map[string]string{}
	routing.Annotations[AnnotationCohortSize] = "1"
	resp, _ = e.Admit(r, defaultBudget(), routing, defaultCandidates())
	if resp.Decision != DecisionAdmit {
		t.Fatalf("legacy annotation was not ignored=%+v", resp)
	}
}

func TestBudgetWindowRolloverGuardAndFinality(t *testing.T) {
	e := NewEngine()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	b := defaultBudget()
	b.Spec.Period = "daily"
	b.Spec.BudgetEUR = resource.MustParse("0.02")
	a, err := e.Admit(admitRequest("old", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = e.Dispatch(dispatchRequest("old", "d", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload))
	if _, _, err = e.Settle(settleRequest("old", "s", 2_000, 1, false, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	if _, err = e.Admit(admitRequest("new", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates()); err != nil {
		t.Fatal(err)
	}
	got := e.Liability(testTenant)
	if got.SettledSpendMicros != 0 || got.OutstandingLiabilityMicros != 7_200 || got.AvailableBudgetMicros != 12_800 {
		t.Fatalf("rollover=%+v", got)
	}
	finalReq := settleRequest("old", "f", 2_000, 1, true, testTenant, testWorkload)
	finalReq.PredecessorEventID = "s"
	if _, _, err = e.Settle(finalReq); err != nil {
		t.Fatal(err)
	}
	got = e.Liability(testTenant)
	if got.OutstandingLiabilityMicros != 3_600 || got.AvailableBudgetMicros != 16_400 {
		t.Fatalf("final guard release=%+v", got)
	}
}

func TestBudgetWindowIDsAreDeterministicUTC(t *testing.T) {
	at := time.Date(2026, 7, 12, 23, 30, 0, 0, time.FixedZone("test", 3*60*60)) // 20:30 UTC Sunday
	b := defaultBudget()
	cases := map[string]string{"daily": "daily:2026-07-12", "weekly": "weekly:2026-07-06", "monthly": "monthly:2026-07-01"}
	for period, want := range cases {
		b.Spec.Period = period
		got, err := budgetWindowID(b, at)
		if err != nil || got != want {
			t.Fatalf("%s window=(%q,%v), want %q", period, got, err, want)
		}
	}
	b.Spec.Period = "hourly"
	if _, err := budgetWindowID(b, at); err == nil {
		t.Fatal("unsupported window period accepted")
	}
}

func TestLateSettlementDownCorrectionNoCreditAndDebtPayoff(t *testing.T) {
	e := NewEngine()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	b := defaultBudget()
	b.Spec.Period = "daily"
	b.Spec.BudgetEUR = resource.MustParse("0.02")
	a, err := e.Admit(admitRequest("late", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = e.Dispatch(dispatchRequest("late", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload))
	now = now.Add(24 * time.Hour)
	if _, err = e.Admit(admitRequest("new2", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.Settle(settleRequest("late", "usage", 2_000, 1, false, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	before := e.Liability(testTenant)
	if before.CarriedAdjustmentMicros != 2_000 || before.OutstandingLiabilityMicros != 5_200 {
		t.Fatalf("late=%+v", before)
	}
	down := settleRequest("late", "down", 1_000, 2, false, testTenant, testWorkload)
	down.PredecessorEventID = "usage"
	if _, _, err = e.Settle(down); err != nil {
		t.Fatal(err)
	}
	after := e.Liability(testTenant)
	if after.CarriedAdjustmentMicros != 2_000 || after.OutstandingLiabilityMicros != 5_200 || after.HistoricalAuditCreditMicros != 1_000 || after.AvailableBudgetMicros != before.AvailableBudgetMicros {
		t.Fatalf("downward minted credit: before=%+v after=%+v", before, after)
	}
	upBelow := settleRequest("late", "up-below-base", 1_500, 3, false, testTenant, testWorkload)
	upBelow.PredecessorEventID = "down"
	if _, _, err = e.Settle(upBelow); err != nil {
		t.Fatal(err)
	}
	middle := e.Liability(testTenant)
	if middle.HistoricalAuditCreditMicros != 500 || middle.CarriedAdjustmentMicros != 2_000 || middle.AvailableBudgetMicros != after.AvailableBudgetMicros {
		t.Fatalf("down/up credit reversal=%+v", middle)
	}
	upOver := settleRequest("late", "up-over-base", 2_500, 4, false, testTenant, testWorkload)
	upOver.PredecessorEventID = "up-below-base"
	if _, _, err = e.Settle(upOver); err != nil {
		t.Fatal(err)
	}
	after = e.Liability(testTenant)
	if after.HistoricalAuditCreditMicros != 0 || after.CarriedAdjustmentMicros != 2_500 || after.OutstandingLiabilityMicros != 4_700 || after.AvailableBudgetMicros != before.AvailableBudgetMicros {
		t.Fatalf("upward carried correction=%+v", after)
	}
	_ = e.ConfigureLedgerAuthority("test-authority", testLedgerAuthorityKey)
	adj := BudgetAdjustment{AdjustmentID: "adjust-1", TenantID: testTenant, WindowID: after.CurrentWindowID, NewBudgetMicros: 20_000, DebtPaymentMicros: 2_500, AuthorizedBy: "test-admin", Reason: "verified payment", ApprovedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute)}
	adj = SignBudgetAdjustment(adj, "test-authority", testLedgerAuthorityKey)
	if err = e.AdjustBudget(context.Background(), adj); err != nil {
		t.Fatal(err)
	}
	paid := e.Liability(testTenant)
	if paid.CarriedAdjustmentMicros != 0 || paid.AvailableBudgetMicros != 15_300 {
		t.Fatalf("payoff=%+v", paid)
	}
}

func TestProviderAttemptBindingAndExpiryReconciliation(t *testing.T) {
	e := NewEngine()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	a, err := e.Admit(admitRequest("bound", testTenant, testWorkload), defaultBudget(), defaultRoutingAt(now), defaultCandidates())
	if err != nil || a.Decision != DecisionAdmit {
		t.Fatalf("admit response = %+v err=%v", a, err)
	}
	_, _, _ = e.Dispatch(dispatchRequest("bound", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload))
	bad := settleRequest("bound", "bad", 100, 1, true, testTenant, testWorkload)
	bad.ProviderAttemptID = "wrong"
	if _, code, err := e.Settle(bad); err == nil || code != ReasonInvalidTransition {
		t.Fatalf("mismatched attempt=(%s,%v)", code, err)
	}
	now = now.Add(6 * time.Minute)
	rows := e.ReconcileExpired(context.Background(), now)
	if len(rows) != 1 || rows[0].State != StateUnresolved {
		t.Fatalf("reconcile=%+v", rows)
	}
	if e.Liability(testTenant).OutstandingLiabilityMicros != 3_600 {
		t.Fatal("potentially billable expiry released hold")
	}
}

func TestPostFinalCreditTracksBaseAcrossDownThenUpCorrections(t *testing.T) {
	e := NewEngine()
	a := mustAdmit(t, e, "credit-reversal", testTenant, testWorkload, defaultCandidates())
	_, _, _ = e.Dispatch(dispatchRequest("credit-reversal", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload))
	if _, _, err := e.Settle(settleRequest("credit-reversal", "base", 2_000, 1, true, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	downFinal := settleRequest("credit-reversal", "down", 1_000, 2, true, testTenant, testWorkload)
	downFinal.PredecessorEventID = "base"
	if _, _, err := e.Settle(downFinal); err != nil {
		t.Fatal(err)
	}
	if got := e.Liability(testTenant); got.HistoricalAuditCreditMicros != 1_000 {
		t.Fatalf("down credit=%+v", got)
	}
	upFinal := settleRequest("credit-reversal", "up", 1_500, 3, true, testTenant, testWorkload)
	upFinal.PredecessorEventID = "down"
	if _, _, err := e.Settle(upFinal); err != nil {
		t.Fatal(err)
	}
	if got := e.Liability(testTenant); got.HistoricalAuditCreditMicros != 500 || got.CarriedAdjustmentMicros != 0 {
		t.Fatalf("credit did not reverse to base delta: %+v", got)
	}
}

func TestUpwardCorrectionAfterLateFinalPreservesFullExternalDebt(t *testing.T) {
	e := NewEngine()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return now }
	b := defaultBudget()
	b.Spec.Period = "daily"
	a, err := e.Admit(admitRequest("late-final-up", testTenant, testWorkload), b, defaultRoutingAt(now), defaultCandidates())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.Dispatch(dispatchRequest("late-final-up", "claim", a.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	_ = e.Liability(testTenant)
	if _, code, err := e.Settle(settleRequest("late-final-up", "late-final", 2_000, 1, true, testTenant, testWorkload)); err != nil || code != ReasonLateSettlement {
		t.Fatalf("late final=(%s,%v)", code, err)
	}
	up := settleRequest("late-final-up", "late-up", 2_700, 2, true, testTenant, testWorkload)
	up.PredecessorEventID = "late-final"
	if _, code, err := e.Settle(up); err != nil || code != ReasonCorrection {
		t.Fatalf("up correction=(%s,%v)", code, err)
	}
	if got := e.Liability(testTenant); got.CarriedAdjustmentMicros != 2_700 {
		t.Fatalf("late correction reduced external debt: %+v", got)
	}
}
