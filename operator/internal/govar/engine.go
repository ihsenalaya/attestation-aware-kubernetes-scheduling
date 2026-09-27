package govar

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
)

// MoneyMicros is one millionth of the configured ledger currency. All ledger
// arithmetic uses this integer type; provider prices are converted before a
// reservation transaction begins.
type MoneyMicros int64

type Decision string

const (
	DecisionAdmit           Decision = "ADMIT"
	DecisionQueue           Decision = "QUEUE"
	DecisionReject          Decision = "REJECT"
	DecisionAbstain         Decision = "ABSTAIN"
	DecisionRequireApproval Decision = "REQUIRE_APPROVAL"
)

type ReasonCode string

const (
	ReasonHighestUtility           ReasonCode = "highest_utility_feasible"
	ReasonNoCandidate              ReasonCode = "no_candidate_after_governance"
	ReasonInsufficientEvidence     ReasonCode = "insufficient_reservation_evidence"
	ReasonBudgetUnavailable        ReasonCode = "budget_unavailable"
	ReasonDuplicateRequest         ReasonCode = "duplicate_request"
	ReasonDuplicateConflict        ReasonCode = "duplicate_request_conflict"
	ReasonDuplicateEvent           ReasonCode = "duplicate_event"
	ReasonReservationNotFound      ReasonCode = "reservation_not_found"
	ReasonSettlementDuplicate      ReasonCode = "duplicate_settlement"
	ReasonApprovalRequired         ReasonCode = "approval_required"
	ReasonCanceled                 ReasonCode = "authoritative_unbilled_cancellation"
	ReasonCancellationUnclear      ReasonCode = "cancellation_delivery_ambiguous"
	ReasonDispatchClaimed          ReasonCode = "dispatch_claimed"
	ReasonDispatchDelivered        ReasonCode = "dispatch_delivered"
	ReasonDispatchUnresolved       ReasonCode = "dispatch_delivery_ambiguous"
	ReasonProvisionalSettlement    ReasonCode = "provisional_settlement"
	ReasonLateSettlement           ReasonCode = "late_settlement"
	ReasonCorrection               ReasonCode = "settlement_correction"
	ReasonFinalized                ReasonCode = "settlement_finalized"
	ReasonInvalidTransition        ReasonCode = "invalid_transition"
	ReasonPrincipalMismatch        ReasonCode = "authenticated_principal_mismatch"
	ReasonReservationExceeded      ReasonCode = "reservation_exceeded"
	ReasonPolicyNotReady           ReasonCode = "policy_not_ready"
	ReasonBudgetTargetMismatch     ReasonCode = "budget_target_mismatch"
	ReasonWorkloadTargetMismatch   ReasonCode = "workload_target_mismatch"
	ReasonModelNotReady            ReasonCode = "model_not_ready"
	ReasonProviderUnavailable      ReasonCode = "provider_unavailable"
	ReasonNotRoutable              ReasonCode = "model_not_routable"
	ReasonGovernanceInfeasible     ReasonCode = "governance_infeasible"
	ReasonQualityStale             ReasonCode = "quality_observation_stale"
	ReasonQualityBelowMinimum      ReasonCode = "quality_below_minimum"
	ReasonPricingIncomplete        ReasonCode = "pricing_incomplete"
	ReasonPricingStale             ReasonCode = "pricing_stale"
	ReasonContextLimit             ReasonCode = "context_limit_exceeded"
	ReasonLatencyUnavailable       ReasonCode = "latency_observation_unavailable"
	ReasonLatencyExceeded          ReasonCode = "latency_guardrail_exceeded"
	ReasonStrictCapUnverified      ReasonCode = "strict_cap_unverified"
	ReasonInsufficientCalibration  ReasonCode = "insufficient_calibration"
	ReasonCalibrationMissing       ReasonCode = "calibration_status_missing"
	ReasonCalibrationGeneration    ReasonCode = "calibration_generation_mismatch"
	ReasonCalibrationMismatch      ReasonCode = "calibration_digest_mismatch"
	ReasonCalibrationRegime        ReasonCode = "calibration_regime_mismatch"
	ReasonCalibrationSupport       ReasonCode = "calibration_support_insufficient"
	ReasonCalibrationStale         ReasonCode = "calibration_stale"
	ReasonCalibrationDrift         ReasonCode = "calibration_drift_detected"
	ReasonCalibrationUnsupported   ReasonCode = "calibration_detector_unsupported"
	ReasonInputBoundFallback       ReasonCode = "input_token_bound_uncertain"
	ReasonReservationMethodUnknown ReasonCode = "reservation_method_unknown"
	ReasonBudgetWindowConflict     ReasonCode = "budget_window_conflict"
	ReasonRetriesDisabled          ReasonCode = "provider_retries_disabled_unreserved"
	ReasonExpiredUndispatched      ReasonCode = "expired_undispatched"
	ReasonBudgetWindowRollover     ReasonCode = "budget_window_rollover"
)

type ReservationState string

const (
	StateReserved               ReservationState = "RESERVED"
	StateDispatchPending        ReservationState = "DISPATCH_PENDING"
	StateDispatched             ReservationState = "DISPATCHED"
	StateUnresolved             ReservationState = "UNRESOLVED"
	StateSettledProvisional     ReservationState = "SETTLED_PROVISIONAL"
	StateLateSettledProvisional ReservationState = "LATE_SETTLED_PROVISIONAL"
	StateCorrectedProvisional   ReservationState = "CORRECTED_PROVISIONAL"
	StateFinalized              ReservationState = "FINALIZED"
	StateLateFinalized          ReservationState = "LATE_FINALIZED"
	StateCanceledUnbilled       ReservationState = "CANCELED_UNBILLED"
	StateFailedUnbilled         ReservationState = "FAILED_UNBILLED"
	StateExpiredUndispatched    ReservationState = "EXPIRED_UNDISPATCHED"
)

type OutboxState string

const (
	OutboxPending   OutboxState = "PENDING"
	OutboxClaimed   OutboxState = "CLAIMED"
	OutboxDelivered OutboxState = "DELIVERED"
	OutboxCanceled  OutboxState = "CANCELED"
)

type DispatchStatus string

const (
	DispatchClaimed        DispatchStatus = "CLAIMED"
	DispatchDelivered      DispatchStatus = "DELIVERED"
	DispatchAmbiguous      DispatchStatus = "AMBIGUOUS"
	DispatchFailedUnbilled DispatchStatus = "FAILED_UNBILLED"
)

type AdmitRequest struct {
	RequestID            string   `json:"request_id"`
	Namespace            string   `json:"namespace"`
	TenantID             string   `json:"tenant_id"`
	WorkloadUID          string   `json:"workload_uid"`
	Team                 string   `json:"team,omitempty"`
	Application          string   `json:"application,omitempty"`
	SensitiveData        bool     `json:"sensitive_data,omitempty"`
	AllowedZones         []string `json:"allowed_zones,omitempty"`
	BudgetPolicyName     string   `json:"budget_policy_name"`
	RoutingPolicyName    string   `json:"routing_policy_name"`
	InputTokens          int64    `json:"input_tokens,omitempty"`
	InputTokensExact     bool     `json:"input_tokens_exact,omitempty"`
	MaxOutputTokens      int64    `json:"max_output_tokens,omitempty"`
	MaxToolCalls         int64    `json:"max_tool_calls,omitempty"`
	MaxMediaUnits        int64    `json:"max_media_units,omitempty"`
	TimeoutSeconds       int64    `json:"timeout_seconds,omitempty"`
	MaxRetryAttempts     int64    `json:"max_retry_attempts,omitempty"`
	CancellationPossible bool     `json:"cancellation_possible,omitempty"`
	// ChargeBounds contains adapter-normalized, non-overlapping bounds for every
	// additional request-declared billable basis. A missing basis is unknown,
	// not zero, and makes a candidate infeasible.
	ChargeBounds    []govarpricing.UsageQuantity `json:"charge_bounds,omitempty"`
	RequireApproval bool                         `json:"require_approval,omitempty"`
	CohortID        string                       `json:"cohort_id,omitempty"`
	CohortIndex     int64                        `json:"cohort_index,omitempty"`
	// SelectedFeedback, when present, binds a public benchmark item and all
	// run-level evidence before routing. The selected model is deliberately
	// absent: it is derived from the immutable route snapshot only after an
	// effective dispatch-delivered transition.
	SelectedFeedback *SelectedFeedbackBinding `json:"selected_feedback,omitempty"`

	// Authenticated fields are populated by the trusted HTTP identity boundary,
	// never decoded from the request body.
	AuthenticatedTenantID    string `json:"-"`
	AuthenticatedWorkloadUID string `json:"-"`
	AuthenticatedNamespace   string `json:"-"`
}

type AdmitResponse struct {
	Decision             Decision                              `json:"decision"`
	ReasonCode           ReasonCode                            `json:"reason_code"`
	SelectedDeployment   string                                `json:"selected_deployment,omitempty"`
	ReservationID        string                                `json:"reservation_id,omitempty"`
	ProviderAttemptID    string                                `json:"provider_attempt_id,omitempty"`
	ProviderRetryPolicy  string                                `json:"provider_retry_policy,omitempty"`
	ReservedCostMicros   MoneyMicros                           `json:"reserved_cost_micros,omitempty"`
	ReservationMode      string                                `json:"reservation_method,omitempty"`
	AllocatedRiskPPB     int64                                 `json:"allocated_risk_ppb,omitempty"`
	RiskLevel            string                                `json:"risk_level,omitempty"`
	PolicyVersion        string                                `json:"policy_version,omitempty"`
	PricingVersion       string                                `json:"pricing_version,omitempty"`
	Expiry               string                                `json:"expiry,omitempty"`
	TraceID              string                                `json:"trace_id,omitempty"`
	RouteSnapshot        *RouteSnapshot                        `json:"route_snapshot,omitempty"`
	SettlementUsageBases []aiopsv1alpha1.ProviderBillableBasis `json:"settlement_usage_bases,omitempty"`
}

type DispatchRequest struct {
	RequestID                string         `json:"request_id"`
	EventID                  string         `json:"event_id"`
	TenantID                 string         `json:"tenant_id"`
	WorkloadUID              string         `json:"workload_uid"`
	ProviderAttemptID        string         `json:"provider_attempt_id"`
	RouteSnapshotHash        string         `json:"route_snapshot_hash"`
	Status                   DispatchStatus `json:"status"`
	AuthenticatedTenantID    string         `json:"-"`
	AuthenticatedWorkloadUID string         `json:"-"`
}

type SettleRequest struct {
	RequestID         string          `json:"request_id"`
	SettlementID      string          `json:"settlement_id"`
	ProviderAttemptID string          `json:"provider_attempt_id"`
	TenantID          string          `json:"tenant_id"`
	WorkloadUID       string          `json:"workload_uid"`
	ActualCostMicros  MoneyMicros     `json:"actual_cost_micros"`
	LegacyActualCost  json.RawMessage `json:"actual_cost,omitempty"`
	ActualInput       int64           `json:"actual_input_tokens,omitempty"`
	ActualOutput      int64           `json:"actual_output_tokens,omitempty"`
	// Usage is the authoritative provider-adapter-normalized component vector.
	// It must name every separately priced basis before Final can be effective.
	Usage                    []govarpricing.UsageQuantity `json:"usage,omitempty"`
	UsageVersion             int64                        `json:"usage_version"`
	PredecessorEventID       string                       `json:"predecessor_event_id,omitempty"`
	Final                    bool                         `json:"final"`
	ErrorStatus              string                       `json:"error_status,omitempty"`
	AuthenticatedTenantID    string                       `json:"-"`
	AuthenticatedWorkloadUID string                       `json:"-"`
}

type CancelRequest struct {
	RequestID                string `json:"request_id"`
	EventID                  string `json:"event_id"`
	ProviderAttemptID        string `json:"provider_attempt_id"`
	TenantID                 string `json:"tenant_id"`
	WorkloadUID              string `json:"workload_uid"`
	Reason                   string `json:"reason,omitempty"`
	AuthoritativeUnbilled    bool   `json:"authoritative_unbilled,omitempty"`
	AuthenticatedTenantID    string `json:"-"`
	AuthenticatedWorkloadUID string `json:"-"`
}

type LiabilityResponse struct {
	TenantID                    string      `json:"tenant_id"`
	SettledSpendMicros          MoneyMicros `json:"settled_spend_micros"`
	OutstandingLiabilityMicros  MoneyMicros `json:"outstanding_liability_micros"`
	CarriedAdjustmentMicros     MoneyMicros `json:"carried_adjustment_micros"`
	AvailableBudgetMicros       MoneyMicros `json:"available_budget_micros"`
	ActiveReservations          int         `json:"active_reservations"`
	CurrentWindowID             string      `json:"current_window_id,omitempty"`
	HistoricalAuditCreditMicros MoneyMicros `json:"historical_audit_credit_micros"`
}

type Reservation struct {
	RequestID                     string
	TenantID                      string
	WorkloadUID                   string
	SelectedDeployment            string
	ProviderAttemptID             string
	OutboxID                      string
	OutboxState                   OutboxState
	State                         ReservationState
	ReservedCostMicros            MoneyMicros
	ReservedComponents            []govarpricing.ChargeComponent
	PricingSnapshot               govarpricing.NormalizedPricingSnapshot
	PricingSnapshotSHA256         string
	CapEvidenceSHA256             string
	VerifiedOutputCapTokens       int64
	ActualComponents              []govarpricing.ActualChargeComponent
	MissingUsageBases             []aiopsv1alpha1.ProviderBillableBasis
	ComponentBoundExceeded        bool
	ProvisionalCostMicros         MoneyMicros
	BaseActualMicros              MoneyMicros
	SettledEffectMicros           MoneyMicros
	ResidualHoldMicros            MoneyMicros
	UsageVersion                  int64
	Finalized                     bool
	PolicyVersion                 string
	PricingVersion                string
	ReservationMode               string
	RiskLevel                     string
	AllocatedRiskPPB              int64
	InputPriceMicrosPerMillion    int64
	OutputPriceMicrosPerMillion   int64
	AdmissionFingerprint          string
	CandidateSnapshotVersion      string
	RouteSnapshot                 RouteSnapshot
	CohortID                      string
	CohortIndex                   int64
	CohortRegistryDigest          string
	CalibrationArtifactSHA256     string
	SelectedFeedback              *SelectedFeedbackBinding
	SelectedFeedbackBindingSHA256 string
	OriginWindowID                string
	EnforcementWindowID           string
	RolloverGuardMicros           MoneyMicros
	CarryEffectMicros             MoneyMicros
	HistoricalCreditMicros        MoneyMicros
	Carried                       bool
	LastUsageEventID              string
	ProviderRetryPolicy           string
	LastReasonCode                ReasonCode
	LastTransitionEventID         string
	Expiry                        time.Time
	// PreviousState and TransitionEffective describe only the response from a
	// mutation call. They are never stored in either ledger and are omitted
	// from JSON so persistence, identity hashes, and audit digests cannot depend
	// on transport metadata.
	PreviousState       ReservationState `json:"-"`
	TransitionEffective bool             `json:"-"`
	// SelectedFeedbackDispatchID is response-only transition metadata. It is
	// derived from the immutable pre-admission binding and effective route, and
	// is never persisted in the monetary reservation row.
	SelectedFeedbackDispatchID string `json:"-"`
}

func withTransitionMetadata(res Reservation, previous ReservationState, effective bool) Reservation {
	res.PreviousState = previous
	res.TransitionEffective = effective
	return res
}

func withoutTransitionMetadata(res Reservation) Reservation {
	res.PreviousState = ""
	res.TransitionEffective = false
	res.SelectedFeedbackDispatchID = ""
	return res
}

type inboxEvent struct {
	RequestID   string
	Kind        string
	PayloadHash string
}

type tenantLedger struct {
	BudgetMicros            MoneyMicros
	SettledMicros           MoneyMicros
	ReservedMicros          MoneyMicros
	CarriedAdjustmentMicros MoneyMicros
	ActiveReservations      int
	Requests                map[string]struct{}
	BudgetIdentity          string
	CurrentWindowID         string
	HistoricalCreditMicros  MoneyMicros
	WindowPeriod            string
}

type Engine struct {
	mu                        sync.Mutex
	reservations              map[string]Reservation
	inbox                     map[string]inboxEvent
	tenants                   map[string]*tenantLedger
	cohorts                   map[string]FrozenCohort
	adjustments               map[string]string
	authorityKeyID            string
	authorityKey              []byte
	cohortSoftwareHash        string
	calibrationEvidenceSource string
	now                       func() time.Time
	trustedReplayClock        *TrustedReplayClock
}

// TrustedReplayClock is an explicitly configured offline-replay clock. It is
// shared by all in-memory engines participating in one replay so every policy,
// freshness, expiry, budget-window, and settlement read observes the same
// event time. Advance accepts equal instants for deterministic tie processing
// and rejects any attempt to move the replay backwards.
type TrustedReplayClock struct {
	mu sync.RWMutex
	at time.Time
}

func NewTrustedReplayClock(at time.Time) (*TrustedReplayClock, error) {
	if at.IsZero() {
		return nil, errors.New("trusted replay time is required")
	}
	return &TrustedReplayClock{at: at.UTC()}, nil
}

func (c *TrustedReplayClock) Now() time.Time {
	if c == nil {
		return time.Time{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.at
}

func (c *TrustedReplayClock) Advance(at time.Time) error {
	if c == nil {
		return errors.New("trusted replay clock is required")
	}
	if at.IsZero() {
		return errors.New("trusted replay time is required")
	}
	at = at.UTC()
	c.mu.Lock()
	defer c.mu.Unlock()
	if at.Before(c.at) {
		return fmt.Errorf("trusted replay clock cannot move backwards from %s to %s",
			c.at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano))
	}
	c.at = at
	return nil
}

func NewEngine() *Engine {
	return &Engine{
		reservations:              map[string]Reservation{},
		inbox:                     map[string]inboxEvent{},
		tenants:                   map[string]*tenantLedger{},
		cohorts:                   map[string]FrozenCohort{},
		adjustments:               map[string]string{},
		calibrationEvidenceSource: CalibrationEvidenceSourcePostgreSQLV8,
		now:                       time.Now,
	}
}

// ConfigureInMemoryQualificationEvidence marks this in-memory engine as a
// non-final qualification runtime and permits only the correspondingly honest
// calibration provenance string. It cannot be called after ledger/cohort state
// exists. PostgreSQLEngine deliberately has no equivalent configuration path.
func (e *Engine) ConfigureInMemoryQualificationEvidence() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.reservations) != 0 || len(e.tenants) != 0 || len(e.cohorts) != 0 || len(e.adjustments) != 0 {
		return errors.New("qualification evidence provenance must be configured before ledger state")
	}
	e.calibrationEvidenceSource = CalibrationEvidenceSourceInMemoryQualificationV1
	return nil
}

// ConfigureTrustedTime fixes the authoritative clock before any ledger state
// exists. It is retained for single-instant tests and delegates to the same
// monotonic replay clock used by multi-event replay.
func (e *Engine) ConfigureTrustedTime(at time.Time) error {
	clock, err := NewTrustedReplayClock(at)
	if err != nil {
		return err
	}
	return e.ConfigureTrustedReplayClock(clock)
}

// ConfigureTrustedReplayClock installs one authoritative monotonic clock
// before ledger or cohort state exists. Sharing the clock across engines keeps
// a multi-window replay on one causal timeline without per-engine skew.
func (e *Engine) ConfigureTrustedReplayClock(clock *TrustedReplayClock) error {
	if clock == nil || clock.Now().IsZero() {
		return errors.New("trusted replay clock is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.reservations) != 0 || len(e.tenants) != 0 || len(e.cohorts) != 0 || len(e.adjustments) != 0 {
		return errors.New("trusted replay time must be configured before ledger state")
	}
	e.trustedReplayClock = clock
	e.now = clock.Now
	return nil
}

func (e *Engine) Ready(context.Context) error { return nil }

func (e *Engine) Admit(req AdmitRequest, budget aiopsv1alpha1.AIBudgetPolicy, routing aiopsv1alpha1.AIRoutingPolicy, candidates []Candidate) (AdmitResponse, error) {
	if err := validateAdmitRequest(req); err != nil {
		return AdmitResponse{}, err
	}
	if req.SelectedFeedback != nil && req.SelectedFeedback.SoftwareSHA256 != e.cohortSoftwareHash {
		return AdmitResponse{}, errors.New("selected-feedback software binding differs from the running binary")
	}
	candidates = validatedCandidates(candidates)
	snapshot := BuildPolicySnapshot(budget, routing)
	candidates = rankedCandidates(candidates, req.InputTokens, req.MaxOutputTokens, snapshot.Objective)
	fingerprint := admissionFingerprint(req, budget, routing, candidates)

	e.mu.Lock()
	defer e.mu.Unlock()

	if existing, exists := e.reservations[req.RequestID]; exists {
		if err := validateReservationRoute(existing); err != nil {
			return AdmitResponse{}, err
		}
		if err := matchPrincipal(existing, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
			return AdmitResponse{}, err
		}
		if existing.AdmissionFingerprint != fingerprint {
			return AdmitResponse{}, errors.New("duplicate request_id has conflicting immutable admission payload")
		}
		if !reservationIsActive(existing.State) {
			return AdmitResponse{Decision: DecisionReject, ReasonCode: ReasonInvalidTransition}, nil
		}
		return responseForReservation(existing, ReasonDuplicateRequest), nil
	}
	if reason := validatePolicyAndTarget(req, budget, routing, e.now().UTC()); reason != "" {
		return decisionResponse(req.RequestID, DecisionReject, reason, budget, routing), nil
	}
	if routing.Spec.Canary.Enabled {
		return decisionResponse(req.RequestID, DecisionRequireApproval, ReasonApprovalRequired, budget, routing), nil
	}
	budgetMicros, err := quantityToMicros(budget.Spec.BudgetEUR)
	if err != nil {
		return AdmitResponse{}, fmt.Errorf("budget conversion: %w", err)
	}
	windowID, err := budgetWindowID(budget, e.now())
	if err != nil {
		return AdmitResponse{}, err
	}
	tenant, err := e.bindTenantBudget(req.AuthenticatedTenantID, budgetMicros, budgetPolicyIdentity(budget), windowID, budget.Spec.Period)
	if err != nil {
		return decisionResponse(req.RequestID, DecisionReject, ReasonBudgetWindowConflict, budget, routing), nil
	}
	choice, infeasibleReason, err := chooseAdmissionWithEvidenceSource(req, routing, candidates, tenant.available(), e.now().UTC(), e.calibrationEvidenceSource)
	if err != nil {
		return AdmitResponse{}, err
	}
	if infeasibleReason != "" {
		decision := decisionForAdmissionFailure(routing, infeasibleReason)
		return decisionResponse(req.RequestID, decision, infeasibleReason, budget, routing), nil
	}
	best, reservedCost := choice.Candidate, choice.Reservation
	cohortDigest := ""
	allocatedRiskOverride := int64(-1)
	if choice.Method == string(aiopsv1alpha1.GOVARReservationFixedCohort) {
		cohort, ok := e.cohorts[cohortKey(req.AuthenticatedTenantID, req.CohortID)]
		if !ok {
			return decisionResponse(req.RequestID, DecisionAbstain, ReasonInsufficientCalibration, budget, routing), nil
		}
		allocatedRiskOverride, err = validateCohortAdmission(req, routing, cohort, e.now())
		if err != nil {
			return decisionResponse(req.RequestID, DecisionAbstain, ReasonInsufficientCalibration, budget, routing), nil
		}
		cohortDigest = cohort.RegistryDigest
	}
	if allocatedRiskOverride >= 0 && choice.Method == "govar_fixed_cohort" {
		choice.AllocatedRiskPPB = allocatedRiskOverride
	} else if choice.Method != "govar_fixed_cohort" {
		choice.AllocatedRiskPPB = 0
	}
	if choice.Method == "govar_fixed_cohort" {
		for _, existing := range e.reservations {
			if existing.TenantID == req.AuthenticatedTenantID && existing.CohortID == req.CohortID && existing.CohortIndex == req.CohortIndex {
				return decisionResponse(req.RequestID, DecisionReject, ReasonDuplicateConflict, budget, routing), nil
			}
		}
	}

	expiry := e.now().UTC().Add(5 * time.Minute)
	res := Reservation{
		RequestID: req.RequestID, TenantID: req.AuthenticatedTenantID, WorkloadUID: req.AuthenticatedWorkloadUID,
		SelectedDeployment: best.ModelRef, ProviderAttemptID: req.RequestID + ":attempt:1",
		OutboxID: req.RequestID + ":dispatch:1", OutboxState: OutboxPending, State: StateReserved,
		ReservedCostMicros: reservedCost, ResidualHoldMicros: reservedCost,
		ReservedComponents:    append([]govarpricing.ChargeComponent(nil), choice.Components...),
		PricingSnapshot:       *best.PricingSnapshot.DeepCopy(),
		PricingSnapshotSHA256: best.PricingSnapshot.SnapshotSHA256, CapEvidenceSHA256: best.CapEvidenceDigest, VerifiedOutputCapTokens: best.VerifiedOutputCapTokens,
		PolicyVersion: policyVersion(budget, routing), PricingVersion: best.PricingVersion,
		ReservationMode: choice.Method, RiskLevel: riskLevel(snapshot), AllocatedRiskPPB: choice.AllocatedRiskPPB, Expiry: expiry,
		InputPriceMicrosPerMillion:    best.InputPriceMicrosPerMillion,
		OutputPriceMicrosPerMillion:   best.OutputPriceMicrosPerMillion,
		AdmissionFingerprint:          fingerprint,
		CandidateSnapshotVersion:      best.SnapshotVersion,
		RouteSnapshot:                 best.RouteSnapshot,
		CohortID:                      req.CohortID,
		CohortIndex:                   req.CohortIndex,
		CohortRegistryDigest:          cohortDigest,
		CalibrationArtifactSHA256:     calibrationArtifactForChoice(routing, choice.Method),
		SelectedFeedback:              cloneSelectedFeedbackBinding(req.SelectedFeedback),
		SelectedFeedbackBindingSHA256: selectedFeedbackBindingSHA256(req.SelectedFeedback),
		OriginWindowID:                tenant.CurrentWindowID,
		EnforcementWindowID:           tenant.CurrentWindowID,
		ProviderRetryPolicy:           "NO_PROVIDER_RETRY",
	}
	if choice.FallbackReason != "" {
		res.RiskLevel = "conservative"
	}
	tenant.ReservedMicros += reservedCost
	tenant.Requests[req.RequestID] = struct{}{}
	e.reservations[req.RequestID] = res
	reason := ReasonHighestUtility
	if choice.FallbackReason != "" {
		reason = choice.FallbackReason
	}
	return responseForReservation(res, reason), nil
}

func calibrationArtifactForChoice(routing aiopsv1alpha1.AIRoutingPolicy, method string) string {
	if method != string(aiopsv1alpha1.GOVARReservationAdaptiveQuantile) && method != string(aiopsv1alpha1.GOVARReservationFixedCohort) {
		return ""
	}
	if routing.Status.GOVAR == nil || routing.Status.GOVAR.Calibration == nil {
		return ""
	}
	return routing.Status.GOVAR.Calibration.ArtifactSHA256
}

func (e *Engine) Dispatch(req DispatchRequest) (Reservation, ReasonCode, error) {
	if err := validateEventPrincipal(req.RequestID, req.EventID, req.TenantID, req.WorkloadUID, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
		return Reservation{}, ReasonPrincipalMismatch, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	// The selected-feedback authority is a PostgreSQL transaction boundary.
	// The development in-memory ledger cannot prove atomic dispatch plus
	// authorization and therefore fails closed for bound requests.
	if existing, ok := e.reservations[req.RequestID]; ok && existing.SelectedFeedback != nil && req.Status == DispatchDelivered {
		return withTransitionMetadata(existing, existing.State, false), ReasonInvalidTransition, errors.New("selected-feedback dispatch requires the PostgreSQL authority ledger")
	}
	payloadHash := eventPayloadHash("dispatch", req.RequestID, req.TenantID, req.WorkloadUID, req.ProviderAttemptID, req.RouteSnapshotHash, string(req.Status))
	if prior, ok := e.inbox[req.EventID]; ok {
		if prior.RequestID != req.RequestID {
			return Reservation{}, ReasonDuplicateEvent, errors.New("event_id is already bound to another request")
		}
		if prior.PayloadHash != payloadHash {
			return Reservation{}, ReasonDuplicateEvent, errors.New("event_id replay has conflicting immutable payload")
		}
		res, exists := e.reservations[req.RequestID]
		if !exists {
			return Reservation{}, ReasonDuplicateEvent, errors.New("duplicate event references unknown request")
		}
		if err := matchPrincipal(res, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
			return Reservation{}, ReasonPrincipalMismatch, err
		}
		if err := validateReservationRoute(res); err != nil {
			return withTransitionMetadata(res, res.State, false), ReasonInvalidTransition, err
		}
		return withTransitionMetadata(res, res.State, false), ReasonDuplicateEvent, nil
	}
	res, ok := e.reservations[req.RequestID]
	if !ok {
		return Reservation{}, ReasonReservationNotFound, errors.New("reservation not found")
	}
	previousState := res.State
	if err := matchPrincipal(res, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
		return withTransitionMetadata(res, previousState, false), ReasonPrincipalMismatch, err
	}
	if req.ProviderAttemptID != res.ProviderAttemptID {
		return withTransitionMetadata(res, previousState, false), ReasonInvalidTransition, errors.New("provider_attempt_id does not match the reserved attempt")
	}
	if err := validateReservationRoute(res); err != nil || req.RouteSnapshotHash != res.RouteSnapshot.SnapshotHash {
		return withTransitionMetadata(res, previousState, false), ReasonInvalidTransition, errors.New("dispatch route snapshot does not match reservation")
	}
	if err := e.advanceTenantWindowLocked(res.TenantID, e.now()); err != nil {
		return withTransitionMetadata(res, previousState, false), ReasonBudgetWindowConflict, err
	}
	code, err := applyDispatch(&res, req.Status)
	if err != nil {
		return withTransitionMetadata(res, previousState, false), code, err
	}
	res.LastReasonCode, res.LastTransitionEventID = code, req.EventID
	e.reservations[req.RequestID] = withoutTransitionMetadata(res)
	e.inbox[req.EventID] = inboxEvent{RequestID: req.RequestID, Kind: "dispatch", PayloadHash: payloadHash}
	return withTransitionMetadata(res, previousState, true), code, nil
}

func (e *Engine) Settle(req SettleRequest) (Reservation, ReasonCode, error) {
	if err := validateSettleRequest(req); err != nil {
		return Reservation{}, ReasonPrincipalMismatch, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	payloadHash := settlementPayloadHash(req)
	if prior, ok := e.inbox[req.SettlementID]; ok {
		if prior.RequestID != req.RequestID {
			return Reservation{}, ReasonDuplicateEvent, errors.New("settlement_id is already bound to another request")
		}
		if prior.PayloadHash != payloadHash {
			return Reservation{}, ReasonDuplicateEvent, errors.New("settlement_id replay has conflicting immutable payload")
		}
		res, exists := e.reservations[req.RequestID]
		if !exists {
			return Reservation{}, ReasonSettlementDuplicate, errors.New("duplicate settlement references unknown request")
		}
		if err := matchPrincipal(res, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
			return Reservation{}, ReasonPrincipalMismatch, err
		}
		if err := validateReservationRoute(res); err != nil {
			return withTransitionMetadata(res, res.State, false), ReasonInvalidTransition, err
		}
		return withTransitionMetadata(res, res.State, false), ReasonSettlementDuplicate, nil
	}
	res, ok := e.reservations[req.RequestID]
	if !ok {
		return Reservation{}, ReasonReservationNotFound, errors.New("reservation not found")
	}
	previousState := res.State
	if err := matchPrincipal(res, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
		return withTransitionMetadata(res, previousState, false), ReasonPrincipalMismatch, err
	}
	if req.ProviderAttemptID != res.ProviderAttemptID {
		return withTransitionMetadata(res, previousState, false), ReasonInvalidTransition, errors.New("provider_attempt_id does not match the reserved attempt")
	}
	if err := validateReservationRoute(res); err != nil {
		return withTransitionMetadata(res, previousState, false), ReasonInvalidTransition, err
	}
	if err := e.advanceTenantWindowLocked(res.TenantID, e.now()); err != nil {
		return withTransitionMetadata(res, previousState, false), ReasonBudgetWindowConflict, err
	}
	tenant := e.ensureTenant(res.TenantID, 0)
	code, err := applySettlement(&res, tenant, req)
	if err != nil {
		return withTransitionMetadata(res, previousState, false), code, err
	}
	if code == ReasonSettlementDuplicate || code == ReasonDuplicateEvent {
		return withTransitionMetadata(res, previousState, false), code, nil
	}
	res.LastReasonCode, res.LastTransitionEventID = code, req.SettlementID
	e.reservations[req.RequestID] = withoutTransitionMetadata(res)
	e.inbox[req.SettlementID] = inboxEvent{RequestID: req.RequestID, Kind: "settlement", PayloadHash: payloadHash}
	if !reservationIsActive(res.State) {
		delete(tenant.Requests, req.RequestID)
	}
	return withTransitionMetadata(res, previousState, true), code, nil
}

func (e *Engine) Cancel(req CancelRequest) (Reservation, ReasonCode, error) {
	if err := validateEventPrincipal(req.RequestID, req.EventID, req.TenantID, req.WorkloadUID, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
		return Reservation{}, ReasonPrincipalMismatch, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if strings.TrimSpace(req.ProviderAttemptID) == "" {
		return Reservation{}, ReasonInvalidTransition, errors.New("provider_attempt_id is required")
	}
	payloadHash := eventPayloadHash("cancel", req.RequestID, req.TenantID, req.WorkloadUID, req.ProviderAttemptID, req.Reason, fmt.Sprint(req.AuthoritativeUnbilled))
	if prior, ok := e.inbox[req.EventID]; ok {
		if prior.RequestID != req.RequestID {
			return Reservation{}, ReasonDuplicateEvent, errors.New("event_id is already bound to another request")
		}
		if prior.PayloadHash != payloadHash {
			return Reservation{}, ReasonDuplicateEvent, errors.New("event_id replay has conflicting immutable payload")
		}
		res, exists := e.reservations[req.RequestID]
		if !exists {
			return Reservation{}, ReasonDuplicateEvent, errors.New("duplicate event references unknown request")
		}
		if err := matchPrincipal(res, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
			return Reservation{}, ReasonPrincipalMismatch, err
		}
		if err := validateReservationRoute(res); err != nil {
			return withTransitionMetadata(res, res.State, false), ReasonInvalidTransition, err
		}
		return withTransitionMetadata(res, res.State, false), ReasonDuplicateEvent, nil
	}
	res, ok := e.reservations[req.RequestID]
	if !ok {
		return Reservation{}, ReasonReservationNotFound, errors.New("reservation not found")
	}
	previousState := res.State
	if err := matchPrincipal(res, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID); err != nil {
		return withTransitionMetadata(res, previousState, false), ReasonPrincipalMismatch, err
	}
	if req.ProviderAttemptID != res.ProviderAttemptID {
		return withTransitionMetadata(res, previousState, false), ReasonInvalidTransition, errors.New("provider_attempt_id does not match the reserved attempt")
	}
	if err := validateReservationRoute(res); err != nil {
		return withTransitionMetadata(res, previousState, false), ReasonInvalidTransition, err
	}
	if err := e.advanceTenantWindowLocked(res.TenantID, e.now()); err != nil {
		return withTransitionMetadata(res, previousState, false), ReasonBudgetWindowConflict, err
	}
	tenant := e.ensureTenant(res.TenantID, 0)
	code, err := applyCancel(&res, tenant, req.AuthoritativeUnbilled)
	if err != nil {
		return withTransitionMetadata(res, previousState, false), code, err
	}
	if code == ReasonDuplicateEvent || code == ReasonSettlementDuplicate {
		return withTransitionMetadata(res, previousState, false), code, nil
	}
	res.LastReasonCode, res.LastTransitionEventID = code, req.EventID
	e.reservations[req.RequestID] = withoutTransitionMetadata(res)
	e.inbox[req.EventID] = inboxEvent{RequestID: req.RequestID, Kind: "cancel", PayloadHash: payloadHash}
	if res.State == StateCanceledUnbilled || res.State == StateFailedUnbilled {
		delete(tenant.Requests, req.RequestID)
	}
	return withTransitionMetadata(res, previousState, true), code, nil
}

func (e *Engine) Liability(tenantID string) LiabilityResponse {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.advanceTenantWindowLocked(tenantID, e.now())
	tenant := e.ensureTenant(tenantID, 0)
	return LiabilityResponse{
		TenantID: tenantID, SettledSpendMicros: tenant.SettledMicros,
		OutstandingLiabilityMicros: tenant.ReservedMicros,
		CarriedAdjustmentMicros:    tenant.CarriedAdjustmentMicros,
		AvailableBudgetMicros:      tenant.available(), ActiveReservations: len(tenant.Requests),
		CurrentWindowID: tenant.CurrentWindowID, HistoricalAuditCreditMicros: tenant.HistoricalCreditMicros,
	}
}

func (e *Engine) LiabilityWithError(tenantID string) (LiabilityResponse, error) {
	return e.Liability(tenantID), nil
}

func (e *Engine) ReserveRetry(context.Context, string) (ReasonCode, error) {
	return ReasonRetriesDisabled, errors.New("provider retry/fallback/hedge is disabled unless a separately reserved attempt API is implemented")
}

func applyDispatch(res *Reservation, status DispatchStatus) (ReasonCode, error) {
	switch status {
	case DispatchClaimed:
		if res.State != StateReserved || res.OutboxState != OutboxPending {
			return ReasonInvalidTransition, errors.New("dispatch claim requires a pending reserved outbox")
		}
		res.State, res.OutboxState = StateDispatchPending, OutboxClaimed
		return ReasonDispatchClaimed, nil
	case DispatchDelivered:
		if res.State != StateDispatchPending || res.OutboxState != OutboxClaimed {
			return ReasonInvalidTransition, errors.New("delivery requires a claimed dispatch")
		}
		res.State, res.OutboxState = StateDispatched, OutboxDelivered
		return ReasonDispatchDelivered, nil
	case DispatchAmbiguous:
		if res.State != StateDispatchPending || res.OutboxState != OutboxClaimed {
			return ReasonInvalidTransition, errors.New("ambiguous result requires a claimed dispatch")
		}
		res.State = StateUnresolved
		return ReasonDispatchUnresolved, nil
	case DispatchFailedUnbilled:
		return ReasonInvalidTransition, errors.New("authoritative unbilled failure must use cancel so release is atomic")
	default:
		return ReasonInvalidTransition, fmt.Errorf("unsupported dispatch status %q", status)
	}
}

func applySettlement(res *Reservation, tenant *tenantLedger, req SettleRequest) (ReasonCode, error) {
	usage := append([]govarpricing.UsageQuantity(nil), req.Usage...)
	if len(usage) == 0 && (req.ActualInput > 0 || req.ActualOutput > 0) {
		usage = []govarpricing.UsageQuantity{{Basis: aiopsv1alpha1.ProviderBasisInputTokens, Quantity: req.ActualInput}, {Basis: aiopsv1alpha1.ProviderBasisOutputTokens, Quantity: req.ActualOutput}}
	}
	actualComponents, missing, exceeded, err := govarpricing.SettleComponents(res.PricingSnapshot, res.ReservedComponents, usage)
	if err != nil {
		return ReasonPricingIncomplete, err
	}
	actualMicros, err := govarpricing.SumActualComponents(actualComponents)
	if err != nil {
		return ReasonInvalidTransition, err
	}
	if req.ActualCostMicros != 0 && req.ActualCostMicros != MoneyMicros(actualMicros) {
		return ReasonInvalidTransition, errors.New("client-supplied actual cost does not match the frozen component vector")
	}
	req.ActualCostMicros = MoneyMicros(actualMicros)
	if len(missing) != 0 {
		req.Final = false
		if req.ErrorStatus == "" {
			req.ErrorStatus = "incomplete_authoritative_usage"
		}
	}
	res.ActualComponents = append([]govarpricing.ActualChargeComponent(nil), actualComponents...)
	res.MissingUsageBases = append([]aiopsv1alpha1.ProviderBillableBasis(nil), missing...)
	res.ComponentBoundExceeded = res.ComponentBoundExceeded || exceeded
	if req.ActualCostMicros < 0 || req.UsageVersion <= 0 {
		return ReasonInvalidTransition, errors.New("actual_cost_micros must be non-negative and usage_version positive")
	}
	if res.State == StateCanceledUnbilled || res.State == StateFailedUnbilled {
		return ReasonInvalidTransition, errors.New("cannot settle an authoritative unbilled terminal request")
	}
	if res.State == StateReserved || res.OutboxState == OutboxPending {
		return ReasonInvalidTransition, errors.New("settlement requires a previously claimed provider attempt")
	}
	if res.Finalized {
		if req.UsageVersion != res.UsageVersion+1 || req.PredecessorEventID != res.LastUsageEventID {
			return ReasonInvalidTransition, errors.New("post-finality correction requires exact next version and predecessor event")
		}
		delta := req.ActualCostMicros - res.ProvisionalCostMicros
		res.UsageVersion = req.UsageVersion
		if delta > 0 {
			if req.ActualCostMicros > res.BaseActualMicros {
				targetDebt := req.ActualCostMicros - res.BaseActualMicros
				if res.Carried {
					// A late-final request is entirely external to the active
					// window. Its carry effect is the full authoritative cost,
					// not merely the delta above its first observed actual.
					targetDebt = req.ActualCostMicros
				}
				tenant.CarriedAdjustmentMicros += targetDebt - res.CarryEffectMicros
				res.CarryEffectMicros = targetDebt
			}
			desiredCredit := res.BaseActualMicros - req.ActualCostMicros
			if desiredCredit < 0 {
				desiredCredit = 0
			}
			tenant.HistoricalCreditMicros += desiredCredit - res.HistoricalCreditMicros
			res.HistoricalCreditMicros = desiredCredit
			res.ProvisionalCostMicros = req.ActualCostMicros
			res.LastUsageEventID = req.SettlementID
			if req.ActualCostMicros > res.ReservedCostMicros || exceeded {
				return ReasonReservationExceeded, nil
			}
			return ReasonCorrection, nil
		}
		// Downward post-final corrections are audit-only. They cannot mint an
		// availability credit after the authoritative hold has been released.
		desiredCredit := res.BaseActualMicros - req.ActualCostMicros
		if desiredCredit < 0 {
			desiredCredit = 0
		}
		tenant.HistoricalCreditMicros += desiredCredit - res.HistoricalCreditMicros
		res.HistoricalCreditMicros = desiredCredit
		res.ProvisionalCostMicros = req.ActualCostMicros
		res.LastUsageEventID = req.SettlementID
		return ReasonCorrection, nil
	}
	// Authoritative usage is evidence that the provider attempt was delivered,
	// even when its acknowledgement raced or was lost.
	res.OutboxState = OutboxDelivered
	if res.UsageVersion == 0 && (req.UsageVersion != 1 || req.PredecessorEventID != "") {
		return ReasonInvalidTransition, errors.New("first usage requires version 1 and no predecessor")
	}
	if req.UsageVersion < res.UsageVersion {
		return ReasonInvalidTransition, errors.New("usage_version is stale")
	}
	if req.UsageVersion == res.UsageVersion && res.UsageVersion != 0 {
		if req.ActualCostMicros != res.ProvisionalCostMicros {
			return ReasonInvalidTransition, errors.New("same usage_version has conflicting cost")
		}
		if !req.Final {
			return ReasonSettlementDuplicate, nil
		}
		if req.PredecessorEventID != res.LastUsageEventID {
			return ReasonInvalidTransition, errors.New("finality requires predecessor event")
		}
	} else {
		if res.UsageVersion > 0 && (req.UsageVersion != res.UsageVersion+1 || req.PredecessorEventID != res.LastUsageEventID) {
			return ReasonInvalidTransition, errors.New("correction requires exact next version and predecessor event")
		}
		previous := res.ProvisionalCostMicros
		delta := req.ActualCostMicros - previous
		newResidual := res.ReservedCostMicros - req.ActualCostMicros
		if newResidual < 0 {
			newResidual = 0
		}
		oldHold := res.ResidualHoldMicros
		first := res.UsageVersion == 0
		if first {
			res.BaseActualMicros = req.ActualCostMicros
		}
		late := res.State == StateUnresolved || tenant.CurrentWindowID != res.OriginWindowID
		rolledProvisional := !res.Carried && res.EnforcementWindowID != "" && res.EnforcementWindowID != tenant.CurrentWindowID
		if first && late {
			tenant.CarriedAdjustmentMicros += req.ActualCostMicros
			tenant.ReservedMicros += newResidual - oldHold
			res.Carried = true
			res.CarryEffectMicros = req.ActualCostMicros
			res.EnforcementWindowID = tenant.CurrentWindowID
		} else if res.Carried {
			if req.ActualCostMicros > res.CarryEffectMicros {
				enforcementDelta := req.ActualCostMicros - res.CarryEffectMicros
				tenant.CarriedAdjustmentMicros += enforcementDelta
				targetResidual := res.ReservedCostMicros - req.ActualCostMicros
				if targetResidual < 0 {
					targetResidual = 0
				}
				tenant.ReservedMicros += targetResidual - oldHold
				newResidual = targetResidual
				res.CarryEffectMicros = req.ActualCostMicros
				desiredCredit := res.BaseActualMicros - req.ActualCostMicros
				if desiredCredit < 0 {
					desiredCredit = 0
				}
				tenant.HistoricalCreditMicros += desiredCredit - res.HistoricalCreditMicros
				res.HistoricalCreditMicros = desiredCredit
			} else if req.ActualCostMicros < res.CarryEffectMicros {
				// A downward historical correction is audit-only. Preserve current
				// debt and hold exposure; never mint availability.
				desiredCredit := res.BaseActualMicros - req.ActualCostMicros
				if desiredCredit < 0 {
					desiredCredit = 0
				}
				tenant.HistoricalCreditMicros += desiredCredit - res.HistoricalCreditMicros
				res.HistoricalCreditMicros = desiredCredit
				newResidual = oldHold
			}
		} else if rolledProvisional {
			desiredCredit := res.BaseActualMicros - req.ActualCostMicros
			if desiredCredit < 0 {
				desiredCredit = 0
			}
			tenant.HistoricalCreditMicros += desiredCredit - res.HistoricalCreditMicros
			res.HistoricalCreditMicros = desiredCredit
			// residual and guard move oppositely, so current exposure stays R.
			res.RolloverGuardMicros += delta
			tenant.ReservedMicros += (newResidual - oldHold) + delta
		} else {
			tenant.SettledMicros += delta
			tenant.ReservedMicros += newResidual - oldHold
			res.SettledEffectMicros = req.ActualCostMicros
		}
		res.ProvisionalCostMicros = req.ActualCostMicros
		res.ResidualHoldMicros = newResidual
		res.UsageVersion = req.UsageVersion
		res.LastUsageEventID = req.SettlementID
	}

	late := res.Carried || res.State == StateUnresolved || tenant.CurrentWindowID != res.OriginWindowID
	if req.Final {
		tenant.ReservedMicros -= res.ResidualHoldMicros + res.RolloverGuardMicros
		res.ResidualHoldMicros = 0
		res.RolloverGuardMicros = 0
		res.Finalized = true
		res.LastUsageEventID = req.SettlementID
		finalCode := ReasonFinalized
		if res.Carried {
			res.State = StateLateFinalized
			finalCode = ReasonLateSettlement
		} else {
			res.State = StateFinalized
		}
		if req.ActualCostMicros > res.ReservedCostMicros || exceeded {
			return ReasonReservationExceeded, nil
		}
		return finalCode, nil
	}
	if req.ActualCostMicros > res.ReservedCostMicros || exceeded {
		res.State = StateCorrectedProvisional
		return ReasonReservationExceeded, nil
	}
	if res.UsageVersion > 1 {
		res.State = StateCorrectedProvisional
		return ReasonCorrection, nil
	}
	if late {
		res.State = StateLateSettledProvisional
	} else {
		res.State = StateSettledProvisional
	}
	if late {
		return ReasonLateSettlement, nil
	}
	return ReasonProvisionalSettlement, nil
}

func applyCancel(res *Reservation, tenant *tenantLedger, authoritative bool) (ReasonCode, error) {
	switch res.State {
	case StateReserved:
		if res.OutboxState != OutboxPending {
			return ReasonInvalidTransition, errors.New("reserved request has non-pending outbox")
		}
		res.OutboxState = OutboxCanceled
		res.State = StateCanceledUnbilled
		tenant.ReservedMicros -= res.ResidualHoldMicros + res.RolloverGuardMicros
		res.ResidualHoldMicros = 0
		res.RolloverGuardMicros = 0
		return ReasonCanceled, nil
	case StateDispatchPending, StateDispatched, StateUnresolved:
		if !authoritative {
			res.State = StateUnresolved
			return ReasonCancellationUnclear, nil
		}
		res.OutboxState = OutboxCanceled
		res.State = StateFailedUnbilled
		tenant.ReservedMicros -= res.ResidualHoldMicros + res.RolloverGuardMicros
		res.ResidualHoldMicros = 0
		res.RolloverGuardMicros = 0
		return ReasonCanceled, nil
	case StateSettledProvisional, StateLateSettledProvisional, StateCorrectedProvisional, StateFinalized, StateLateFinalized:
		return ReasonInvalidTransition, errors.New("settled request cannot be canceled")
	case StateCanceledUnbilled, StateFailedUnbilled:
		return ReasonDuplicateEvent, nil
	default:
		return ReasonInvalidTransition, errors.New("unsupported cancellation state")
	}
}

func validateAdmitRequest(req AdmitRequest) error {
	if strings.TrimSpace(req.RequestID) == "" || strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.WorkloadUID) == "" {
		return errors.New("request_id, tenant_id, and workload_uid are required")
	}
	if req.InputTokens < 0 || req.MaxOutputTokens < 0 || req.MaxToolCalls < 0 || req.MaxMediaUnits < 0 || req.TimeoutSeconds < 0 || req.MaxRetryAttempts < 0 {
		return errors.New("charge bounds cannot be negative")
	}
	if req.Namespace == "" || req.Namespace != req.AuthenticatedNamespace {
		return errors.New("request namespace does not match authenticated namespace")
	}
	if req.SelectedFeedback != nil {
		if err := req.SelectedFeedback.Validate(); err != nil {
			return fmt.Errorf("selected-feedback pre-admission binding: %w", err)
		}
	}
	return validatePrincipal(req.TenantID, req.WorkloadUID, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID)
}

func settlementPayloadHash(req SettleRequest) string {
	usage := append([]govarpricing.UsageQuantity(nil), req.Usage...)
	sort.Slice(usage, func(i, j int) bool { return usage[i].Basis < usage[j].Basis })
	parts := []string{"settlement", req.RequestID, req.TenantID, req.WorkloadUID, req.ProviderAttemptID, fmt.Sprint(req.ActualCostMicros), fmt.Sprint(req.ActualInput), fmt.Sprint(req.ActualOutput), fmt.Sprint(req.UsageVersion), req.PredecessorEventID, fmt.Sprint(req.Final), req.ErrorStatus}
	for _, q := range usage {
		parts = append(parts, string(q.Basis), fmt.Sprint(q.Quantity))
	}
	return eventPayloadHash(parts...)
}

func validateSettleRequest(req SettleRequest) error {
	if strings.TrimSpace(req.SettlementID) == "" {
		return errors.New("settlement_id is required")
	}
	if strings.TrimSpace(req.ProviderAttemptID) == "" {
		return errors.New("provider_attempt_id is required")
	}
	if legacy := strings.TrimSpace(string(req.LegacyActualCost)); legacy != "" && legacy != "0" && legacy != "0.0" {
		return errors.New("actual_cost is deprecated; send integer actual_cost_micros")
	}
	return validateEventPrincipal(req.RequestID, req.SettlementID, req.TenantID, req.WorkloadUID, req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID)
}

func validateEventPrincipal(requestID, eventID, tenantID, workloadUID, authenticatedTenantID, authenticatedWorkloadUID string) error {
	if strings.TrimSpace(requestID) == "" || strings.TrimSpace(eventID) == "" {
		return errors.New("request_id and event_id are required")
	}
	return validatePrincipal(tenantID, workloadUID, authenticatedTenantID, authenticatedWorkloadUID)
}

func validatePrincipal(tenantID, workloadUID, authenticatedTenantID, authenticatedWorkloadUID string) error {
	if tenantID == "" || workloadUID == "" || authenticatedTenantID == "" || authenticatedWorkloadUID == "" {
		return errors.New("tenant_id, workload_uid, and authenticated principal are required")
	}
	if tenantID != authenticatedTenantID || workloadUID != authenticatedWorkloadUID {
		return errors.New("request principal does not match authenticated principal")
	}
	return nil
}

func matchPrincipal(res Reservation, tenantID, workloadUID string) error {
	if res.TenantID != tenantID || res.WorkloadUID != workloadUID {
		return errors.New("authenticated principal does not own request")
	}
	return nil
}

func responseForReservation(res Reservation, reason ReasonCode) AdmitResponse {
	route := res.RouteSnapshot
	bases := make([]aiopsv1alpha1.ProviderBillableBasis, 0, len(res.ReservedComponents))
	for _, component := range res.ReservedComponents {
		bases = append(bases, component.Basis)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return AdmitResponse{
		Decision: DecisionAdmit, ReasonCode: reason, SelectedDeployment: res.SelectedDeployment,
		ReservationID: res.RequestID, ProviderAttemptID: res.ProviderAttemptID,
		ReservedCostMicros: res.ReservedCostMicros, ReservationMode: res.ReservationMode,
		AllocatedRiskPPB: res.AllocatedRiskPPB, RiskLevel: res.RiskLevel,
		PolicyVersion: res.PolicyVersion, PricingVersion: res.PricingVersion,
		Expiry:               res.Expiry.UTC().Format(time.RFC3339),
		ProviderRetryPolicy:  res.ProviderRetryPolicy,
		RouteSnapshot:        &route,
		SettlementUsageBases: bases,
	}
}

func validatedCandidates(candidates []Candidate) []Candidate {
	out := append([]Candidate(nil), candidates...)
	for i := range out {
		if out[i].Feasible {
			if err := validateCandidateSnapshot(out[i]); err != nil {
				out[i].Feasible = false
				out[i].InfeasibleReason = ReasonNotRoutable
			}
		}
	}
	return out
}

func validateReservationRoute(res Reservation) error {
	if err := ValidateRouteSnapshot(res.RouteSnapshot); err != nil {
		return err
	}
	if res.SelectedDeployment != res.RouteSnapshot.ModelName || res.PricingVersion != res.RouteSnapshot.PricingVersion || res.CandidateSnapshotVersion != res.RouteSnapshot.SnapshotHash {
		return errors.New("reservation route snapshot identity mismatch")
	}
	if err := govarpricing.ValidateSnapshotIntegrity(res.PricingSnapshot); err != nil {
		return err
	}
	if res.PricingSnapshot.SnapshotSHA256 != res.PricingSnapshotSHA256 {
		return errors.New("reservation pricing snapshot identity mismatch")
	}
	reserved, err := govarpricing.SumComponents(res.ReservedComponents)
	if err != nil || MoneyMicros(reserved) != res.ReservedCostMicros {
		return errors.New("reservation component sum mismatch")
	}
	return nil
}

func decisionResponse(_ string, decision Decision, reason ReasonCode, budget aiopsv1alpha1.AIBudgetPolicy, routing aiopsv1alpha1.AIRoutingPolicy) AdmitResponse {
	// Trace identity is transport context, not a ledger/request identifier. The
	// HTTP boundary fills this field only from an active OpenTelemetry span.
	return AdmitResponse{Decision: decision, ReasonCode: reason, PolicyVersion: policyVersion(budget, routing), PricingVersion: "provider-pricing-live"}
}

func (e *Engine) ensureTenant(tenantID string, budget MoneyMicros) *tenantLedger {
	tenant, ok := e.tenants[tenantID]
	if !ok {
		tenant = &tenantLedger{BudgetMicros: budget, Requests: map[string]struct{}{}}
		e.tenants[tenantID] = tenant
	}
	return tenant
}

func (e *Engine) bindTenantBudget(tenantID string, budget MoneyMicros, identity, windowID, period string) (*tenantLedger, error) {
	tenant := e.ensureTenant(tenantID, budget)
	if tenant.BudgetIdentity != "" && tenant.CurrentWindowID == windowID && tenant.BudgetIdentity != identity {
		return nil, errBudgetWindowChanged
	}
	if tenant.CurrentWindowID != "" && tenant.CurrentWindowID == windowID && tenant.BudgetMicros != budget {
		return nil, errBudgetWindowChanged
	}
	if tenant.CurrentWindowID != "" && tenant.CurrentWindowID != windowID {
		if !windowStartsAfter(windowID, tenant.CurrentWindowID) {
			return nil, errBudgetWindowChanged
		}
		for id, r := range e.reservations {
			if r.TenantID == tenantID && (r.State == StateSettledProvisional || r.State == StateCorrectedProvisional) && !r.Carried && r.EnforcementWindowID == tenant.CurrentWindowID {
				r.RolloverGuardMicros += r.ProvisionalCostMicros
				tenant.ReservedMicros += r.ProvisionalCostMicros
				e.reservations[id] = r
			}
		}
		tenant.SettledMicros = 0
	}
	tenant.BudgetIdentity = identity
	tenant.CurrentWindowID = windowID
	tenant.WindowPeriod = strings.ToLower(period)
	tenant.BudgetMicros = budget
	return tenant, nil
}

func (t *tenantLedger) available() MoneyMicros {
	return t.BudgetMicros - t.SettledMicros - t.ReservedMicros - t.CarriedAdjustmentMicros
}

func rankedCandidates(candidates []Candidate, inputTokens, maxOutputTokens int64, objective string) []Candidate {
	out := append([]Candidate(nil), candidates...)
	sort.Slice(out, func(i, j int) bool {
		left, leftErr := expectedCostMicros(out[i], inputTokens, maxOutputTokens)
		right, rightErr := expectedCostMicros(out[j], inputTokens, maxOutputTokens)
		if leftErr != nil || rightErr != nil {
			return out[i].ModelRef < out[j].ModelRef
		}
		if strings.EqualFold(objective, "quality") && out[i].QualityTier != out[j].QualityTier {
			return qualityRank(out[i].QualityTier) > qualityRank(out[j].QualityTier)
		}
		if left != right {
			return left < right
		}
		return out[i].ModelRef < out[j].ModelRef
	})
	return out
}

func qualityRank(t aiopsv1alpha1.Tier) int {
	switch t {
	case aiopsv1alpha1.TierHigh:
		return 3
	case aiopsv1alpha1.TierMedium:
		return 2
	case aiopsv1alpha1.TierLow:
		return 1
	default:
		return 0
	}
}

func eventPayloadHash(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(part))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func admissionFingerprint(req AdmitRequest, budget aiopsv1alpha1.AIBudgetPolicy, routing aiopsv1alpha1.AIRoutingPolicy, candidates []Candidate) string {
	govarSpec, _ := json.Marshal(routing.Spec.GOVAR)
	govarStatus, _ := json.Marshal(routing.Status.GOVAR)
	parts := []string{"admission-v2", req.Namespace, req.TenantID, req.WorkloadUID, req.Team, req.Application,
		fmt.Sprint(req.SensitiveData), strings.Join(req.AllowedZones, "\x1f"), req.BudgetPolicyName,
		req.RoutingPolicyName, fmt.Sprint(req.InputTokens), fmt.Sprint(req.InputTokensExact), fmt.Sprint(req.MaxOutputTokens),
		fmt.Sprint(req.MaxToolCalls), fmt.Sprint(req.MaxMediaUnits), fmt.Sprint(req.TimeoutSeconds),
		fmt.Sprint(req.MaxRetryAttempts), fmt.Sprint(req.CancellationPossible), fmt.Sprint(req.RequireApproval),
		budget.Spec.BudgetEUR.String(), policyVersion(budget, routing), budgetIdentity(budget),
		string(govarSpec), string(govarStatus)}
	bounds := append([]govarpricing.UsageQuantity(nil), req.ChargeBounds...)
	sort.Slice(bounds, func(i, j int) bool {
		if bounds[i].Basis == bounds[j].Basis {
			return bounds[i].Quantity < bounds[j].Quantity
		}
		return bounds[i].Basis < bounds[j].Basis
	})
	for _, bound := range bounds {
		parts = append(parts, "charge-bound", string(bound.Basis), fmt.Sprint(bound.Quantity))
	}
	parts = append(parts, req.CohortID, fmt.Sprint(req.CohortIndex), selectedFeedbackBindingSHA256(req.SelectedFeedback))
	for _, c := range candidates {
		parts = append(parts, c.ModelRef, c.ProviderRef, c.Region,
			fmt.Sprint(c.InputPriceMicrosPerMillion), fmt.Sprint(c.OutputPriceMicrosPerMillion),
			c.PricingVersion, c.SnapshotVersion, fmt.Sprint(c.ContextWindow), fmt.Sprint(c.QualityScore), fmt.Sprint(c.VerifiedOutputCap))
	}
	return eventPayloadHash(parts...)
}

func budgetIdentity(budget aiopsv1alpha1.AIBudgetPolicy) string {
	return eventPayloadHash("budget-window-v1", string(budget.UID), budget.Namespace, budget.Name,
		fmt.Sprint(budget.Generation), budget.Spec.Period, budget.Spec.BudgetEUR.String(),
		budget.Spec.Target.Namespace, budget.Spec.Target.Team, budget.Spec.Target.Application)
}

func policyVersion(budget aiopsv1alpha1.AIBudgetPolicy, routing aiopsv1alpha1.AIRoutingPolicy) string {
	return fmt.Sprintf("%s:%d@%s|%s:%d@%s", budget.Name, budget.Generation, budget.ResourceVersion, routing.Name, routing.Generation, routing.ResourceVersion)
}

func riskLevel(snapshot PolicySnapshot) string {
	if snapshot.RequireSovereigntyCompliance {
		return "strict"
	}
	return "bounded"
}
