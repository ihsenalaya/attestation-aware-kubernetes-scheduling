package govar

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

// FrozenCohort is an administrator-preloaded, immutable theorem cohort. It is
// deliberately separate from routing annotations: callers cannot manufacture
// the common pre-outcome field required by the fixed-cohort argument.
type FrozenCohort struct {
	TenantID            string             `json:"tenant_id"`
	CohortID            string             `json:"cohort_id"`
	Size                int64              `json:"size"`
	TenantRiskPPB       int64              `json:"tenant_risk_ppb"`
	Slots               []FrozenCohortSlot `json:"slots"`
	DataHash            string             `json:"data_hash"`
	ConfigHash          string             `json:"config_hash"`
	ProtocolHash        string             `json:"protocol_hash"`
	FrozenAt            time.Time          `json:"frozen_at"`
	RegisteredAt        time.Time          `json:"registered_at"`
	RegistryDigest      string             `json:"registry_digest"`
	AuthorityKeyID      string             `json:"authority_key_id"`
	AuthorityProof      string             `json:"authority_proof"`
	LedgerLayoutID      string             `json:"ledger_layout_id"`
	RouteSnapshotSchema string             `json:"route_snapshot_schema"`
	SoftwareHash        string             `json:"software_hash"`
}

const LedgerLayoutID = "govar-v5-complete-liability-20260713"
const RouteSnapshotSchemaID = "govar-route-snapshot-v1"

type FrozenCohortSlot struct {
	Index             int64  `json:"index"`
	RequestID         string `json:"request_id"`
	OpportunityDigest string `json:"opportunity_digest"`
	WeightPPB         int64  `json:"weight_ppb"`
}

type ReconciliationRecord struct {
	RequestID           string           `json:"request_id"`
	TenantID            string           `json:"tenant_id"`
	ProviderAttemptID   string           `json:"provider_attempt_id"`
	State               ReservationState `json:"state"`
	OutboxState         OutboxState      `json:"outbox_state"`
	Expiry              time.Time        `json:"expiry"`
	PreviousState       ReservationState `json:"-"`
	TransitionEffective bool             `json:"-"`
}

type BudgetAdjustment struct {
	AdjustmentID      string
	TenantID          string
	WindowID          string
	NewBudgetMicros   MoneyMicros
	DebtPaymentMicros MoneyMicros
	AuthorizedBy      string
	Reason            string
	AuthorityKeyID    string
	ApprovedAt        time.Time
	ExpiresAt         time.Time
	AuthorityProof    string
}

func authorityMAC(key []byte, domain string, parts ...string) string {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(domain))
	for _, p := range parts {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (e *Engine) ConfigureLedgerAuthority(keyID string, key []byte) error {
	if keyID == "" || len(key) < 32 {
		return errors.New("ledger authority requires key id and at least 32 secret bytes")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.authorityKeyID = keyID
	e.authorityKey = append([]byte(nil), key...)
	return nil
}
func (e *PostgresEngine) ConfigureLedgerAuthority(keyID string, key []byte) error {
	if keyID == "" || len(key) < 32 {
		return errors.New("ledger authority requires key id and at least 32 secret bytes")
	}
	e.authorityKeyID = keyID
	e.authorityKey = append([]byte(nil), key...)
	return nil
}

func (e *Engine) ConfigureCohortRuntime(softwareHash string) error {
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(softwareHash) {
		return errors.New("cohort runtime software hash must be SHA-256")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cohortSoftwareHash = softwareHash
	return nil
}

func (e *PostgresEngine) ConfigureCohortRuntime(softwareHash string) error {
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(softwareHash) {
		return errors.New("cohort runtime software hash must be SHA-256")
	}
	e.cohortSoftwareHash = softwareHash
	return nil
}

func SignFrozenCohort(cohort FrozenCohort, keyID string, key []byte) (FrozenCohort, error) {
	c, err := normalizeFrozenCohort(cohort)
	if err != nil {
		return c, err
	}
	c.AuthorityKeyID = keyID
	c.AuthorityProof = authorityMAC(key, "govar-cohort-authority-v2", c.RegistryDigest)
	return c, nil
}
func SignBudgetAdjustment(a BudgetAdjustment, keyID string, key []byte) BudgetAdjustment {
	a.AuthorityKeyID = keyID
	a.AuthorityProof = authorityMAC(key, "govar-budget-adjustment-authority-v1", a.AdjustmentID, a.TenantID, a.WindowID, fmt.Sprint(a.NewBudgetMicros), fmt.Sprint(a.DebtPaymentMicros), a.AuthorizedBy, a.Reason, a.ApprovedAt.UTC().Format(time.RFC3339Nano), a.ExpiresAt.UTC().Format(time.RFC3339Nano))
	return a
}

func OpportunityDigest(req AdmitRequest) string {
	zones := append([]string(nil), req.AllowedZones...)
	sort.Strings(zones)
	return eventPayloadHash("govar-opportunity-v1", req.RequestID, req.Namespace, req.TenantID,
		req.WorkloadUID, req.Team, req.Application, fmt.Sprint(req.SensitiveData), strings.Join(zones, "\x1f"),
		req.BudgetPolicyName, req.RoutingPolicyName, fmt.Sprint(req.InputTokens), fmt.Sprint(req.InputTokensExact),
		fmt.Sprint(req.MaxOutputTokens))
}

func normalizeFrozenCohort(c FrozenCohort) (FrozenCohort, error) {
	proof, keyID := c.AuthorityProof, c.AuthorityKeyID
	if strings.TrimSpace(c.TenantID) == "" || strings.TrimSpace(c.CohortID) == "" || c.Size <= 0 {
		return c, errors.New("tenant_id, cohort_id, and positive size are required")
	}
	if c.TenantRiskPPB < 0 || c.TenantRiskPPB > 1_000_000_000 || c.FrozenAt.IsZero() {
		return c, errors.New("valid tenant risk and frozen_at are required")
	}
	isSHA := regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString
	if int64(len(c.Slots)) != c.Size || !isSHA(c.DataHash) || !isSHA(c.ConfigHash) || !isSHA(c.ProtocolHash) || !isSHA(c.SoftwareHash) || c.LedgerLayoutID != LedgerLayoutID || c.RouteSnapshotSchema != RouteSnapshotSchemaID {
		return c, errors.New("complete slots and immutable data/config/protocol hashes are required")
	}
	slots := append([]FrozenCohortSlot(nil), c.Slots...)
	sort.Slice(slots, func(i, j int) bool { return slots[i].Index < slots[j].Index })
	var weightSum int64
	requests, digests := map[string]struct{}{}, map[string]struct{}{}
	for i, slot := range slots {
		if slot.Index != int64(i) || slot.RequestID == "" || !isSHA(slot.OpportunityDigest) || slot.WeightPPB < 0 || slot.WeightPPB > 1_000_000_000 {
			return c, errors.New("cohort slots must be complete, unique, and indexed from zero")
		}
		if _, ok := requests[slot.RequestID]; ok {
			return c, errors.New("duplicate cohort request_id")
		}
		requests[slot.RequestID] = struct{}{}
		if _, ok := digests[slot.OpportunityDigest]; ok {
			return c, errors.New("duplicate cohort opportunity digest")
		}
		digests[slot.OpportunityDigest] = struct{}{}
		if weightSum > 1_000_000_000-slot.WeightPPB {
			return c, errors.New("cohort weights exceed one")
		}
		weightSum += slot.WeightPPB
	}
	if weightSum != 1_000_000_000 {
		return c, fmt.Errorf("cohort weights sum to %d, want 1000000000", weightSum)
	}
	c.Slots = slots
	c.FrozenAt = c.FrozenAt.UTC()
	c.RegistryDigest = ""
	payload := []string{"govar-frozen-cohort-v2", c.TenantID, c.CohortID, fmt.Sprint(c.Size), fmt.Sprint(c.TenantRiskPPB), c.DataHash, c.ConfigHash, c.ProtocolHash, c.LedgerLayoutID, c.RouteSnapshotSchema, c.SoftwareHash, c.FrozenAt.Format(time.RFC3339Nano)}
	for _, slot := range slots {
		payload = append(payload, fmt.Sprint(slot.Index), slot.RequestID, slot.OpportunityDigest, fmt.Sprint(slot.WeightPPB))
	}
	c.RegistryDigest = eventPayloadHash(payload...)
	c.AuthorityProof, c.AuthorityKeyID = proof, keyID
	return c, nil
}

func cohortKey(tenant, cohort string) string { return tenant + "\x00" + cohort }

func (e *Engine) RegisterFrozenCohort(_ context.Context, cohort FrozenCohort) error {
	normalized, err := normalizeFrozenCohort(cohort)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if normalized.FrozenAt.After(e.now().UTC()) {
		return errors.New("frozen_at must precede server registration time")
	}
	if normalized.SoftwareHash != e.cohortSoftwareHash || normalized.AuthorityKeyID != e.authorityKeyID || len(e.authorityKey) == 0 || !hmac.Equal([]byte(strings.ToLower(normalized.AuthorityProof)), []byte(authorityMAC(e.authorityKey, "govar-cohort-authority-v2", normalized.RegistryDigest))) {
		return errors.New("frozen cohort authority proof is invalid")
	}
	if e.cohorts == nil {
		e.cohorts = map[string]FrozenCohort{}
	}
	key := cohortKey(normalized.TenantID, normalized.CohortID)
	if old, ok := e.cohorts[key]; ok {
		if old.RegistryDigest == normalized.RegistryDigest {
			return nil
		}
		return errors.New("frozen cohort is immutable and conflicts with existing registration")
	}
	normalized.RegisteredAt = e.now().UTC()
	e.cohorts[key] = normalized
	return nil
}

func (e *Engine) ExportFrozenCohort(_ context.Context, tenant, cohortID string) (FrozenCohort, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, ok := e.cohorts[cohortKey(tenant, cohortID)]
	if !ok {
		return FrozenCohort{}, errors.New("frozen cohort not found")
	}
	return c, nil
}

func validateCohortAdmission(req AdmitRequest, routing aiopsv1alpha1.AIRoutingPolicy, c FrozenCohort, now time.Time) (int64, error) {
	if c.RegistryDigest == "" || c.FrozenAt.After(now.UTC()) || c.RegisteredAt.IsZero() || c.RegisteredAt.After(now.UTC()) || req.CohortIndex < 0 || req.CohortIndex >= c.Size {
		return 0, errors.New("cohort is absent, not frozen before admission, or slot is out of range")
	}
	govar := routing.Spec.GOVAR
	if govar == nil || govar.Cohort == nil || govar.Risk == nil || govar.Reservation.Method != aiopsv1alpha1.GOVARReservationFixedCohort {
		return 0, errors.New("typed fixed-cohort policy is required")
	}
	if govar.Cohort.RegistryRef != c.RegistryDigest || govar.Cohort.Size != c.Size || govar.Cohort.OpportunitySetHash != c.DataHash || govar.Cohort.WeightsHash != c.ConfigHash || !govar.Cohort.FrozenAt.Equal(&metav1.Time{Time: c.FrozenAt}) {
		return 0, errors.New("typed cohort identity does not match immutable registry")
	}
	if govar.Risk.TenantRiskPPB != c.TenantRiskPPB {
		return 0, errors.New("typed tenant risk does not match immutable registry")
	}
	if govar.Risk.Allocation == "uniform" {
		for _, candidate := range c.Slots {
			if candidate.WeightPPB != 1_000_000_000/c.Size {
				return 0, errors.New("uniform typed allocation does not match registry weights")
			}
		}
	} else if govar.Risk.Allocation != "fixed-weights" {
		return 0, errors.New("unsupported typed risk allocation")
	}
	slot := c.Slots[req.CohortIndex]
	if slot.RequestID != req.RequestID || slot.OpportunityDigest != OpportunityDigest(req) {
		return 0, errors.New("request does not match immutable frozen cohort opportunity")
	}
	// floor is conservative: the pathwise sum can only decrease.
	return c.TenantRiskPPB * slot.WeightPPB / 1_000_000_000, nil
}

func budgetPolicyIdentity(b aiopsv1alpha1.AIBudgetPolicy) string {
	spec, _ := json.Marshal(b.Spec)
	return eventPayloadHash("budget-policy-v2", string(b.UID), b.Namespace, b.Name,
		fmt.Sprint(b.Generation), string(spec))
}

func budgetWindowID(b aiopsv1alpha1.AIBudgetPolicy, at time.Time) (string, error) {
	return budgetWindowForPeriod(b.Spec.Period, at)
}

func budgetWindowForPeriod(period string, at time.Time) (string, error) {
	u := at.UTC()
	var start time.Time
	switch strings.ToLower(strings.TrimSpace(period)) {
	case "daily":
		start = time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
	case "weekly":
		days := (int(u.Weekday()) + 6) % 7
		start = time.Date(u.Year(), u.Month(), u.Day()-days, 0, 0, 0, 0, time.UTC)
	case "monthly":
		start = time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC)
	default:
		return "", fmt.Errorf("unsupported budget period %q", period)
	}
	return strings.ToLower(period) + ":" + start.Format("2006-01-02"), nil
}

func (e *Engine) advanceTenantWindowLocked(tenantID string, at time.Time) error {
	t := e.tenants[tenantID]
	if t == nil || t.CurrentWindowID == "" || t.WindowPeriod == "" {
		return nil
	}
	next, err := budgetWindowForPeriod(t.WindowPeriod, at)
	if err != nil {
		return err
	}
	if next == t.CurrentWindowID {
		return nil
	}
	if !windowStartsAfter(next, t.CurrentWindowID) {
		return errors.New("trusted clock moved before active budget window")
	}
	for id, r := range e.reservations {
		if r.TenantID == tenantID && (r.State == StateSettledProvisional || r.State == StateCorrectedProvisional) && !r.Carried && r.EnforcementWindowID == t.CurrentWindowID {
			r.RolloverGuardMicros += r.ProvisionalCostMicros
			t.ReservedMicros += r.ProvisionalCostMicros
			e.reservations[id] = r
		}
	}
	t.SettledMicros = 0
	t.CurrentWindowID = next
	return nil
}

func windowStartsAfter(next, current string) bool {
	parse := func(v string) (time.Time, error) {
		parts := strings.SplitN(v, ":", 2)
		if len(parts) != 2 {
			return time.Time{}, errors.New("invalid window id")
		}
		return time.Parse("2006-01-02", parts[1])
	}
	n, errN := parse(next)
	c, errC := parse(current)
	return errN == nil && errC == nil && n.After(c)
}

func (e *Engine) AdjustBudget(_ context.Context, adjustment BudgetAdjustment) error {
	if adjustment.AdjustmentID == "" || adjustment.AuthorizedBy == "" || adjustment.Reason == "" || adjustment.NewBudgetMicros < 0 || adjustment.DebtPaymentMicros < 0 {
		return errors.New("authorized non-negative budget adjustment is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if adjustment.AuthorityKeyID != e.authorityKeyID || len(e.authorityKey) == 0 || adjustment.ApprovedAt.After(e.now()) || !adjustment.ExpiresAt.After(e.now()) || !hmac.Equal([]byte(strings.ToLower(adjustment.AuthorityProof)), []byte(authorityMAC(e.authorityKey, "govar-budget-adjustment-authority-v1", adjustment.AdjustmentID, adjustment.TenantID, adjustment.WindowID, fmt.Sprint(adjustment.NewBudgetMicros), fmt.Sprint(adjustment.DebtPaymentMicros), adjustment.AuthorizedBy, adjustment.Reason, adjustment.ApprovedAt.UTC().Format(time.RFC3339Nano), adjustment.ExpiresAt.UTC().Format(time.RFC3339Nano)))) {
		return errors.New("budget adjustment authority proof is invalid or expired")
	}
	payload := eventPayloadHash("budget-adjustment-v1", adjustment.TenantID, adjustment.WindowID, fmt.Sprint(adjustment.NewBudgetMicros), fmt.Sprint(adjustment.DebtPaymentMicros), adjustment.AuthorizedBy, adjustment.Reason)
	if prior, ok := e.adjustments[adjustment.AdjustmentID]; ok {
		if prior == payload {
			return nil
		}
		return errors.New("adjustment_id replay has conflicting immutable payload")
	}
	t := e.tenants[adjustment.TenantID]
	if t == nil {
		return errors.New("tenant/window not found")
	}
	if err := e.advanceTenantWindowLocked(adjustment.TenantID, e.now()); err != nil {
		return err
	}
	if t.CurrentWindowID != adjustment.WindowID {
		return errors.New("tenant/window not found")
	}
	if adjustment.DebtPaymentMicros > t.CarriedAdjustmentMicros {
		return errors.New("debt payment exceeds carried debt")
	}
	t.CarriedAdjustmentMicros -= adjustment.DebtPaymentMicros
	t.BudgetMicros = adjustment.NewBudgetMicros
	e.adjustments[adjustment.AdjustmentID] = payload
	return nil
}

// ReconcileExpired retains every potentially billable hold. Only a still
// pending outbox is atomically canceled and released.
func (e *Engine) ReconcileExpired(_ context.Context, at time.Time) []ReconciliationRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []ReconciliationRecord
	for id, res := range e.reservations {
		if res.Expiry.After(at) {
			continue
		}
		if validateReservationRoute(res) != nil {
			// Corrupt route evidence can never authorize a release or dispatch.
			continue
		}
		t := e.tenants[res.TenantID]
		previousState := res.State
		if res.State == StateReserved && res.OutboxState == OutboxPending {
			eventID := "expiry:" + res.RequestID + ":" + fmt.Sprint(res.Expiry.UnixNano())
			if _, seen := e.inbox[eventID]; seen {
				continue
			}
			res.State, res.OutboxState = StateExpiredUndispatched, OutboxCanceled
			res.LastReasonCode, res.LastTransitionEventID = ReasonExpiredUndispatched, eventID
			e.inbox[eventID] = inboxEvent{RequestID: res.RequestID, Kind: "expiry", PayloadHash: eventPayloadHash("expiry-v1", res.RequestID, res.ProviderAttemptID, res.Expiry.UTC().Format(time.RFC3339Nano))}
			t.ReservedMicros -= res.ResidualHoldMicros + res.RolloverGuardMicros
			res.ResidualHoldMicros, res.RolloverGuardMicros = 0, 0
			delete(t.Requests, id)
		} else if res.State == StateDispatchPending || res.State == StateDispatched {
			eventID := "expiry-unresolved:" + res.RequestID + ":" + fmt.Sprint(res.Expiry.UnixNano())
			if _, seen := e.inbox[eventID]; seen {
				continue
			}
			res.State = StateUnresolved
			res.LastReasonCode, res.LastTransitionEventID = ReasonDispatchUnresolved, eventID
			e.inbox[eventID] = inboxEvent{RequestID: res.RequestID, Kind: "expiry", PayloadHash: eventPayloadHash("expiry-unresolved-v1", res.RequestID, res.ProviderAttemptID, res.Expiry.UTC().Format(time.RFC3339Nano))}
		} else {
			continue
		}
		e.reservations[id] = withoutTransitionMetadata(res)
		out = append(out, ReconciliationRecord{RequestID: id, TenantID: res.TenantID, ProviderAttemptID: res.ProviderAttemptID,
			State: res.State, OutboxState: res.OutboxState, Expiry: res.Expiry, PreviousState: previousState, TransitionEffective: true})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
	return out
}

func (e *Engine) PendingReconciliation(_ context.Context, limit int) ([]ReconciliationRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []ReconciliationRecord
	for _, r := range e.reservations {
		if r.OutboxState == OutboxPending || r.OutboxState == OutboxClaimed || r.State == StateUnresolved {
			out = append(out, ReconciliationRecord{RequestID: r.RequestID, TenantID: r.TenantID, ProviderAttemptID: r.ProviderAttemptID,
				State: r.State, OutboxState: r.OutboxState, Expiry: r.Expiry, PreviousState: r.State, TransitionEffective: false})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
