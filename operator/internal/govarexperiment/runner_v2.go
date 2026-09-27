package govarexperiment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	LifecycleSchemaV2       = "govar-e1-lifecycle-v2"
	RuntimeProvenanceV2     = govar.CalibrationEvidenceSourceInMemoryQualificationV1
	RuntimeEvidenceStatusV2 = "development_qualification_not_final_evidence"

	// The qualification runtime is deliberately bounded to one masked P1b cell.
	// These are operational guardrails, not an asymptotic-complexity claim or a
	// substitute for a measured runtime/memory benchmark.
	QualificationOpportunityLimitV2 int64 = 10_000
	QualificationWindowLimitV2      int64 = 400
)

// LifecycleRecordV2 is an explicit usage-only record. Actual-use fields are
// pointers so pre-usage records omit them entirely instead of serializing
// outcome-shaped zeroes that a later pass might accidentally reinterpret.
type LifecycleRecordV2 struct {
	SchemaVersion          string `json:"schema_version"`
	RecordType             string `json:"record_type"`
	RuntimeProvenance      string `json:"runtime_provenance"`
	EvidenceStatus         string `json:"evidence_status"`
	FinalEvidenceEligible  bool   `json:"final_evidence_eligible"`
	EventIndex             int64  `json:"event_index"`
	Timestamp              string `json:"timestamp"`
	LifecycleEvent         string `json:"lifecycle_event"`
	ExperimentID           string `json:"experiment_id"`
	RunID                  string `json:"run_id"`
	CellID                 string `json:"cell_id"`
	Scenario               string `json:"scenario"`
	Seed                   uint64 `json:"seed"`
	ConfigSHA256           string `json:"config_sha256"`
	CanonicalConfigSHA256  string `json:"canonical_config_sha256"`
	StreamSHA256           string `json:"stream_sha256"`
	MatchedStreamKey       string `json:"matched_stream_key"`
	SourceSHA256           string `json:"source_sha256"`
	MethodConfigSHA256     string `json:"method_config_sha256"`
	UsageBindingSHA256     string `json:"usage_binding_sha256"`
	ComparatorMethod       string `json:"comparator_method"`
	ReservationMode        string `json:"reservation_mode"`
	StreamSequence         int64  `json:"stream_sequence"`
	RequestID              string `json:"request_id"`
	TenantID               string `json:"tenant_id"`
	BudgetWindowID         string `json:"budget_window_id"`
	WorkloadUID            string `json:"workload_uid"`
	CohortID               string `json:"cohort_id,omitempty"`
	CohortIndex            int64  `json:"cohort_index,omitempty"`
	UsageItemID            string `json:"usage_item_id"`
	FaultMode              string `json:"fault_mode"`
	BudgetMicros           int64  `json:"budget_micros"`
	InputTokens            int64  `json:"input_tokens"`
	MaxOutputTokens        int64  `json:"max_output_tokens"`
	Decision               string `json:"decision,omitempty"`
	ReasonCode             string `json:"reason_code,omitempty"`
	PreviousState          string `json:"previous_state,omitempty"`
	CurrentState           string `json:"current_state,omitempty"`
	Effective              bool   `json:"effective"`
	Selected               bool   `json:"selected"`
	SelectedDeployment     string `json:"selected_deployment,omitempty"`
	ProviderAttemptID      string `json:"provider_attempt_id,omitempty"`
	DispatchID             string `json:"dispatch_id,omitempty"`
	ReservedMicros         int64  `json:"reserved_micros"`
	SettledMicros          int64  `json:"settled_micros"`
	OutstandingMicros      int64  `json:"outstanding_micros"`
	AvailableMicros        int64  `json:"available_micros"`
	ActiveReservations     int64  `json:"active_reservations"`
	ActiveActualMicros     int64  `json:"active_actual_micros"`
	ActualInputTokens      *int64 `json:"actual_input_tokens,omitempty"`
	ActualOutputTokens     *int64 `json:"actual_output_tokens,omitempty"`
	ActualInputCostMicros  *int64 `json:"actual_input_cost_micros,omitempty"`
	ActualOutputCostMicros *int64 `json:"actual_output_cost_micros,omitempty"`
	ActualCostMicros       *int64 `json:"actual_cost_micros,omitempty"`
	UsageReceiptSHA256     string `json:"usage_receipt_sha256,omitempty"`
}

type RunResultV2 struct {
	Config                ConfigV2
	ConfigSHA256          string
	CanonicalConfigSHA256 string
	StreamSHA256          string
	MatchedStreamKey      string
	StreamFacts           E1StreamFacts
	RuntimeProvenance     string
	FinalEvidenceEligible bool
	Records               []LifecycleRecordV2
	DecisionCounts        map[string]int64
	LifecycleCounts       map[string]int64
	Work                  RuntimeWorkV2
}

// RuntimeWorkV2 reports explicit logical-unit counts for the bounded
// qualification run. The fields describe what was planned or processed; they
// do not prove an algorithmic complexity class or measure wall-clock work.
type RuntimeWorkV2 struct {
	QualificationOpportunityLimit     int64
	QualificationWindowLimit          int64
	OpportunityCount                  int64
	ScheduledEventCount               int64
	ProcessedEventCount               int64
	WindowContextCount                int64
	EngineCount                       int64
	GOVARArtifactIndexBuildCount      int64
	GOVARWindowInitializationCount    int64
	GOVAROpportunityBindingCheckCount int64
}

type runtimeWindowV2 struct {
	ctx              *tenantContext
	adaptive         *AdaptiveEstimator
	budget           int64
	activeActual     int64
	expectedSettled  int64
	expectedReserved int64
}

type runtimeRequestV2 struct {
	opportunity        OpportunityV2
	projected          Opportunity
	window             *runtimeWindowV2
	admitted           bool
	providerAttempt    string
	selectedDeployment string
	selectedCandidate  *govar.Candidate
	dispatchID         string
	reserved           int64
	receipt            *UsageReceiptV2
	actualInputCost    int64
	actualOutputCost   int64
	actualCost         int64
	adaptivePlan       *AdaptivePlan
	govarBinding       *govarRuntimeBindingV2
}

type govarRuntimeBindingV2 struct {
	slot     GOVARSlotBound
	evidence GOVARCalibrationEvidence
}

type govarRuntimeIndexV2 struct {
	bindings      map[string]govarRuntimeBindingV2
	cohorts       map[string][]GOVARSlotBound
	windowCohorts map[string][]string
}

// RunWithUsageIssuerV2 is intentionally an in-memory qualification runner. It
// returns records to the caller and has no artifact writer or CLI entry point;
// consequently its output is never final evidence. Its scheduler is causal:
// usage authority is called only from usage_available events.
func RunWithUsageIssuerV2(configRaw, streamRaw []byte, issuer *DevelopmentUsageIssuerV2) (RunResultV2, error) {
	if issuer == nil {
		return RunResultV2{}, errors.New("E1 v2 usage issuer is required")
	}
	config, canonicalConfigSHA, err := ParseConfigV2(configRaw)
	if err != nil {
		return RunResultV2{}, err
	}
	switch config.ComparatorMethod {
	case "no_budget", "settled_only", "strict_max", "mean", "mean_margin", "fixed_quantile", "fixed_estimate", "adaptive_quantile", "expected_cost_router", "gov_ar":
	case "oracle_future_cost":
		return RunResultV2{}, errors.New("oracle_future_cost is a post-nonoracle evaluator and cannot execute in the online E1 v2 trace runner")
	default:
		return RunResultV2{}, fmt.Errorf("E1 v2 qualification runner does not implement method %q", config.ComparatorMethod)
	}
	binding := issuer.Binding()
	if err := binding.Validate(); err != nil {
		return RunResultV2{}, fmt.Errorf("usage binding: %w", err)
	}
	if binding.DatasetID != config.UsageDatasetID || binding.Split != config.UsageSplit ||
		binding.BindingSHA256 != config.UsageBindingSHA256 || binding.ProvenanceLock != config.ProvenanceLock {
		return RunResultV2{}, errors.New("usage issuer binding does not match ConfigV2")
	}
	// Verify the exact raw stream before parsing any row. This prevents a
	// syntactically valid replacement stream from reaching opportunity logic.
	if SHA256(streamRaw) != config.ProvenanceLock.OpportunityStreamSHA256 {
		return RunResultV2{}, errors.New("raw opportunity stream does not match the E1 provenance lock")
	}
	opportunities, facts, err := ParseStreamV2(streamRaw, config)
	if err != nil {
		return RunResultV2{}, err
	}
	preOutcomeSequenceSHA, err := StreamPreOutcomeSequenceSHA256V2(opportunities)
	if err != nil {
		return RunResultV2{}, err
	}
	if preOutcomeSequenceSHA != config.ProvenanceLock.StreamPreOutcomeSequenceSHA256 {
		return RunResultV2{}, errors.New("stream pre-outcome sequence does not match the E1 provenance lock")
	}
	if err := issuer.validateOpportunityCoverage(opportunities); err != nil {
		return RunResultV2{}, err
	}
	for _, opportunity := range opportunities {
		if opportunity.DuplicateSettlement || opportunity.ConflictingSettlementReplay {
			return RunResultV2{}, errors.New("E1 v2 in-memory qualification slice does not implement settlement replay injection")
		}
	}
	work, err := planRuntimeWorkV2(opportunities, config.ComparatorMethod)
	if err != nil {
		return RunResultV2{}, err
	}

	runtimeConfig := config.runtimeConfig()
	projected := make([]Opportunity, len(opportunities))
	byRequest := make(map[string]OpportunityV2, len(opportunities))
	for index, opportunity := range opportunities {
		projected[index] = projectOpportunityV2(opportunity)
		byRequest[opportunity.RequestID] = opportunity
	}
	configSHA, streamSHA := SHA256(configRaw), SHA256(streamRaw)
	matchedKey := DomainHash("govar-e1-v2-matched-stream-v1", []byte(streamSHA), []byte(strconv.FormatUint(config.Seed, 10)))
	virtualStart, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	replayClock, err := govar.NewTrustedReplayClock(virtualStart)
	if err != nil {
		return RunResultV2{}, err
	}
	var govarIndex *govarRuntimeIndexV2
	if config.ComparatorMethod == "gov_ar" {
		govarIndex, err = buildGOVARRuntimeIndexV2(runtimeConfig, projected)
		if err != nil {
			return RunResultV2{}, err
		}
	}
	enginesByBudgetWindow := map[string]*govar.Engine{}
	engineForBudgetWindow := func(budgetWindowID string) (*govar.Engine, error) {
		if engine := enginesByBudgetWindow[budgetWindowID]; engine != nil {
			return engine, nil
		}
		engine := govar.NewEngine()
		enginesByBudgetWindow[budgetWindowID] = engine
		return engine, nil
	}
	if !nonReservingMethodV2(config.ComparatorMethod) {
		engineForBudgetWindow = func(budgetWindowID string) (*govar.Engine, error) {
			if engine := enginesByBudgetWindow[budgetWindowID]; engine != nil {
				return engine, nil
			}
			engine := govar.NewEngine()
			if err := engine.ConfigureInMemoryQualificationEvidence(); err != nil {
				return nil, err
			}
			if err := engine.ConfigureTrustedReplayClock(replayClock); err != nil {
				return nil, err
			}
			if err := engine.ConfigureCohortRuntime(config.SoftwareSHA256); err != nil {
				return nil, err
			}
			enginesByBudgetWindow[budgetWindowID] = engine
			return engine, nil
		}
	}
	settledLedger := NewSettledOnlyLedger()
	windows := map[string]*runtimeWindowV2{}
	requests := map[string]*runtimeRequestV2{}
	decisionCounts := map[string]int64{}
	lifecycleCounts := map[string]int64{}
	records := []LifecycleRecordV2{}

	baseRecord := func(op OpportunityV2) LifecycleRecordV2 {
		return LifecycleRecordV2{
			SchemaVersion: LifecycleSchemaV2, RecordType: "lifecycle", RuntimeProvenance: RuntimeProvenanceV2,
			EvidenceStatus: RuntimeEvidenceStatusV2, FinalEvidenceEligible: false,
			ExperimentID: config.ExperimentID, RunID: config.RunID, CellID: config.CellID, Scenario: config.Scenario,
			Seed: config.Seed, ConfigSHA256: configSHA, CanonicalConfigSHA256: canonicalConfigSHA,
			StreamSHA256: streamSHA, MatchedStreamKey: matchedKey, SourceSHA256: config.SourceSHA256,
			MethodConfigSHA256: config.MethodConfigSHA256, UsageBindingSHA256: config.UsageBindingSHA256,
			ComparatorMethod: config.ComparatorMethod, ReservationMode: config.ProductionReservationMode,
			StreamSequence: op.Sequence, RequestID: op.RequestID, TenantID: op.TenantID,
			BudgetWindowID: op.BudgetWindowID, WorkloadUID: op.WorkloadUID, CohortID: op.CohortID,
			CohortIndex: op.CohortIndex, UsageItemID: op.UsageItemID, FaultMode: op.FaultMode,
			BudgetMicros: op.BudgetMicros, InputTokens: op.InputTokens, MaxOutputTokens: op.MaxOutputTokens,
		}
	}
	appendRecord := func(record LifecycleRecordV2, at time.Time) LifecycleRecordV2 {
		record.EventIndex = int64(len(records) + 1)
		record.Timestamp = at.UTC().Format(time.RFC3339Nano)
		records = append(records, record)
		lifecycleCounts[record.LifecycleEvent]++
		return record
	}
	withState := func(record LifecycleRecordV2, state *runtimeRequestV2) LifecycleRecordV2 {
		key := state.opportunity.TenantID + "\x00" + state.opportunity.BudgetWindowID
		if nonReservingMethodV2(config.ComparatorMethod) {
			record.SettledMicros = settledLedger.Settled(key)
			record.AvailableMicros = state.opportunity.BudgetMicros - record.SettledMicros
			record.ActiveActualMicros = state.window.activeActual
			return record
		}
		liability := state.window.ctx.engine.Liability(state.opportunity.TenantID)
		record.SettledMicros = int64(liability.SettledSpendMicros)
		record.OutstandingMicros = int64(liability.OutstandingLiabilityMicros)
		record.AvailableMicros = int64(liability.AvailableBudgetMicros)
		record.ActiveReservations = int64(liability.ActiveReservations)
		return record
	}
	fillRoute := func(record *LifecycleRecordV2, state *runtimeRequestV2) {
		record.SelectedDeployment = state.selectedDeployment
		record.ProviderAttemptID = state.providerAttempt
		record.DispatchID = state.dispatchID
		record.ReservedMicros = state.reserved
	}
	fillUsage := func(record *LifecycleRecordV2, state *runtimeRequestV2) {
		inputTokens, outputTokens := state.receipt.ActualInputTokens, state.receipt.ActualOutputTokens
		inputCost, outputCost, totalCost := state.actualInputCost, state.actualOutputCost, state.actualCost
		record.ActualInputTokens, record.ActualOutputTokens = &inputTokens, &outputTokens
		record.ActualInputCostMicros, record.ActualOutputCostMicros = &inputCost, &outputCost
		record.ActualCostMicros = &totalCost
		record.UsageReceiptSHA256 = state.receipt.ReceiptSHA256
	}
	windowFor := func(state *runtimeRequestV2) (*runtimeWindowV2, error) {
		key := state.opportunity.TenantID + "\x00" + state.opportunity.BudgetWindowID
		if existing := windows[key]; existing != nil {
			return existing, nil
		}
		window := &runtimeWindowV2{budget: state.opportunity.BudgetMicros}
		if !nonReservingMethodV2(config.ComparatorMethod) {
			engine, err := engineForBudgetWindow(state.opportunity.BudgetWindowID)
			if err != nil {
				return nil, err
			}
			if config.ComparatorMethod == "gov_ar" {
				if err := prepareGOVAREngineIndexedV2(engine, runtimeConfig, govarIndex, key); err != nil {
					return nil, err
				}
			}
			ctx, err := newTenantContext(engine, runtimeConfig, state.projected)
			if err != nil {
				return nil, err
			}
			window.ctx = ctx
			if config.ComparatorMethod == "adaptive_quantile" {
				estimator, err := NewAdaptiveEstimator(*config.MethodConfig.Adaptive)
				if err != nil {
					return nil, err
				}
				window.adaptive = estimator
			}
		}
		windows[key] = window
		return window, nil
	}

	events := make([]opportunityEventV2, 0, len(opportunities)*4)
	for _, opportunity := range opportunities {
		times, _ := opportunity.times()
		events = append(events,
			opportunityEventV2{at: times.arrival, kind: eventArrivalV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.providerResponse, kind: eventProviderResponseV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.usageAvailable, kind: eventUsageAvailableV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.settlement, kind: eventSettlementV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
		)
	}
	orderedOpportunityEventsV2(events)

	for _, event := range events {
		if err := replayClock.Advance(event.at); err != nil {
			return RunResultV2{}, fmt.Errorf("advance E1 v2 replay clock for request %s: %w", event.requestID, err)
		}
		work.ProcessedEventCount++
		opportunity := byRequest[event.requestID]
		state := requests[event.requestID]
		switch event.kind {
		case eventArrivalV2:
			projectedOpportunity := projectOpportunityV2(opportunity)
			state = &runtimeRequestV2{opportunity: opportunity, projected: projectedOpportunity}
			window, err := windowFor(state)
			if err != nil {
				return RunResultV2{}, err
			}
			state.window = window
			requests[event.requestID] = state

			admission := baseRecord(opportunity)
			admission.LifecycleEvent = "admission"
			if nonReservingMethodV2(config.ComparatorMethod) {
				key := opportunity.TenantID + "\x00" + opportunity.BudgetWindowID
				hardFeasible := hardFeasibleForOpportunityV2(runtimeConfig, projectedOpportunity)
				var decision, reason string
				if config.ComparatorMethod == "no_budget" {
					decision, reason = NoBudgetDecision(hardFeasible)
				} else {
					decision, reason, err = SettledOnlyDecision(AdmissionFacts{
						HardFeasible: hardFeasible, BudgetMicros: opportunity.BudgetMicros, SettledMicros: settledLedger.Settled(key),
					})
					if err != nil {
						return RunResultV2{}, err
					}
				}
				admission.Decision, admission.ReasonCode = decision, reason
				state.admitted = decision == string(govar.DecisionAdmit)
				if state.admitted {
					candidate := experimentCandidate(runtimeConfig)
					state.selectedCandidate = &candidate
					state.providerAttempt = opportunity.RequestID + ":attempt:1"
					state.selectedDeployment = candidate.ModelRef
					admission.CurrentState = "ADMITTED"
					admission.Effective, admission.Selected = true, true
				}
			} else {
				if config.ComparatorMethod == "gov_ar" {
					binding, exists := govarIndex.bindings[govarRuntimeSlotKeyV2(projectedOpportunity)]
					if !exists {
						return RunResultV2{}, fmt.Errorf("GOV-AR opportunity %s is absent from the indexed runtime", opportunity.RequestID)
					}
					state.govarBinding = &binding
					if err := bindGOVARContextIndexedV2(window.ctx, runtimeConfig, projectedOpportunity, binding, event.at); err != nil {
						return RunResultV2{}, fmt.Errorf("bind GOV-AR opportunity %s: %w", opportunity.RequestID, err)
					}
				} else if config.ComparatorMethod == "adaptive_quantile" {
					if window.adaptive == nil {
						return RunResultV2{}, errors.New("adaptive_quantile window lacks an estimator")
					}
					plan, err := window.adaptive.Plan(opportunity.Sequence, config.MethodConfig.Adaptive.CandidateRegimeSHA256, opportunity.MaxOutputTokens)
					if err != nil {
						return RunResultV2{}, fmt.Errorf("plan adaptive_quantile opportunity %s: %w", opportunity.RequestID, err)
					}
					state.adaptivePlan = &plan
					if err := bindAdaptiveContextV2(window.ctx, runtimeConfig, plan, event.at); err != nil {
						return RunResultV2{}, fmt.Errorf("bind adaptive_quantile opportunity %s: %w", opportunity.RequestID, err)
					}
				} else {
					evaluatedAt := metav1.NewTime(event.at.UTC())
					window.ctx.routing.Status.LastEvaluatedAt = &evaluatedAt
				}
				if config.ComparatorMethod == "expected_cost_router" {
					available, err := expectedCostAvailableV2(window)
					if err != nil {
						return RunResultV2{}, fmt.Errorf("expected_cost_router availability for %s: %w", opportunity.RequestID, err)
					}
					if err := bindExpectedCostContextV2(window.ctx, runtimeConfig, projectedOpportunity, available); err != nil {
						return RunResultV2{}, fmt.Errorf("bind expected_cost_router opportunity %s: %w", opportunity.RequestID, err)
					}
				}
				response, err := window.ctx.engine.Admit(govarAdmitRequest(projectedOpportunity, opportunity.TenantID), window.ctx.budget, window.ctx.routing, window.ctx.candidates)
				if err != nil {
					return RunResultV2{}, fmt.Errorf("admit %s: %w", opportunity.RequestID, err)
				}
				admission.Decision, admission.ReasonCode = string(response.Decision), string(response.ReasonCode)
				state.admitted = response.Decision == govar.DecisionAdmit
				state.providerAttempt, state.selectedDeployment = response.ProviderAttemptID, response.SelectedDeployment
				state.reserved = int64(response.ReservedCostMicros)
				if response.SelectedDeployment != "" {
					selected, ok := candidateByModelRefV2(window.ctx.candidates, response.SelectedDeployment)
					if !ok {
						return RunResultV2{}, fmt.Errorf("admit %s selected unknown deployment %q", opportunity.RequestID, response.SelectedDeployment)
					}
					state.selectedCandidate = &selected
				}
				admission.ReservedMicros = state.reserved
				admission.SelectedDeployment, admission.ProviderAttemptID = state.selectedDeployment, state.providerAttempt
				if state.admitted {
					if response.RouteSnapshot == nil {
						return RunResultV2{}, fmt.Errorf("admit %s omitted route snapshot", opportunity.RequestID)
					}
					var expected int64
					if state.govarBinding != nil {
						expected, err = expectedGOVARReservationIndexedV2(runtimeConfig, projectedOpportunity, *state.govarBinding, event.at)
					} else {
						expected, err = expectedReservationV2(config, projectedOpportunity, state, event.at)
					}
					if err != nil || expected != state.reserved {
						return RunResultV2{}, fmt.Errorf("reservation mismatch for %s: got %d want %d: %w", opportunity.RequestID, state.reserved, expected, err)
					}
					if state.selectedCandidate == nil {
						return RunResultV2{}, fmt.Errorf("admit %s did not bind its selected candidate", opportunity.RequestID)
					}
					if config.ComparatorMethod == "expected_cost_router" {
						state.window.expectedReserved, err = checkedAdd(state.window.expectedReserved, state.reserved)
						if err != nil {
							return RunResultV2{}, fmt.Errorf("expected_cost_router outstanding reservation for %s: %w", opportunity.RequestID, err)
						}
					}
					admission.CurrentState = string(govar.StateReserved)
					admission.Effective, admission.Selected = true, true
				}
			}
			decisionCounts[admission.Decision]++
			appendRecord(withState(admission, state), event.at)
			if !state.admitted {
				continue
			}

			state.dispatchID = DomainHash("govar-e1-v2-effective-dispatch-v1", []byte(config.RunID), []byte(opportunity.RequestID), []byte(state.providerAttempt), []byte(configSHA))
			var delivered LifecycleRecordV2
			if nonReservingMethodV2(config.ComparatorMethod) {
				claim := baseRecord(opportunity)
				claim.LifecycleEvent, claim.ReasonCode = "dispatch_claim", "dispatch_claimed"
				claim.PreviousState, claim.CurrentState = "ADMITTED", "DISPATCH_PENDING"
				claim.Effective, claim.Selected = true, true
				fillRoute(&claim, state)
				appendRecord(withState(claim, state), event.at)
				delivered = baseRecord(opportunity)
				delivered.LifecycleEvent, delivered.ReasonCode = "dispatch_delivered", "dispatch_delivered"
				delivered.PreviousState, delivered.CurrentState = "DISPATCH_PENDING", "DISPATCHED"
				delivered.Effective, delivered.Selected = true, true
				fillRoute(&delivered, state)
				delivered = appendRecord(withState(delivered, state), event.at)
			} else {
				if state.selectedCandidate == nil {
					return RunResultV2{}, fmt.Errorf("dispatch %s lacks a selected candidate binding", opportunity.RequestID)
				}
				claimRequest := govar.DispatchRequest{
					RequestID: opportunity.RequestID, EventID: eventID(config.Seed, streamSHA, opportunity.RequestID, "v2-dispatch-claim"),
					TenantID: opportunity.TenantID, WorkloadUID: opportunity.WorkloadUID, ProviderAttemptID: state.providerAttempt,
					RouteSnapshotHash: state.selectedCandidate.RouteSnapshot.SnapshotHash, Status: govar.DispatchClaimed,
					AuthenticatedTenantID: opportunity.TenantID, AuthenticatedWorkloadUID: opportunity.WorkloadUID,
				}
				reservation, code, err := state.window.ctx.engine.Dispatch(claimRequest)
				if err != nil {
					return RunResultV2{}, fmt.Errorf("claim dispatch %s: %w", opportunity.RequestID, err)
				}
				claim := baseRecord(opportunity)
				claim.LifecycleEvent, claim.ReasonCode = "dispatch_claim", string(code)
				claim.PreviousState, claim.CurrentState = string(reservation.PreviousState), string(reservation.State)
				claim.Effective, claim.Selected = reservation.TransitionEffective, true
				fillRoute(&claim, state)
				appendRecord(withState(claim, state), event.at)
				deliveryRequest := claimRequest
				deliveryRequest.EventID = eventID(config.Seed, streamSHA, opportunity.RequestID, "v2-dispatch-delivered")
				deliveryRequest.Status = govar.DispatchDelivered
				reservation, code, err = state.window.ctx.engine.Dispatch(deliveryRequest)
				if err != nil {
					return RunResultV2{}, fmt.Errorf("deliver dispatch %s: %w", opportunity.RequestID, err)
				}
				delivered = baseRecord(opportunity)
				delivered.LifecycleEvent, delivered.ReasonCode = "dispatch_delivered", string(code)
				delivered.PreviousState, delivered.CurrentState = string(reservation.PreviousState), string(reservation.State)
				delivered.Effective, delivered.Selected = reservation.TransitionEffective, true
				fillRoute(&delivered, state)
				delivered = appendRecord(withState(delivered, state), event.at)
			}
			if !delivered.Effective || delivered.CurrentState != "DISPATCHED" {
				return RunResultV2{}, fmt.Errorf("request %s did not produce an effective delivered dispatch", opportunity.RequestID)
			}
			authorization := UsageAuthorizationV2{
				RunID: config.RunID, RequestID: opportunity.RequestID, UsageItemID: opportunity.UsageItemID,
				DispatchID: state.dispatchID, ConfigSHA256: configSHA, UsageAvailableAt: opportunity.UsageAvailableAt,
			}
			proof := sealEffectiveUsageDispatchProofV2(delivered)
			if err := issuer.authorizeUsage(authorization, proof); err != nil {
				return RunResultV2{}, fmt.Errorf("authorize usage %s: %w", opportunity.RequestID, err)
			}

		case eventProviderResponseV2:
			if state == nil {
				return RunResultV2{}, fmt.Errorf("provider response for unknown request %s", opportunity.RequestID)
			}
			if !state.admitted {
				continue
			}
			record := baseRecord(opportunity)
			record.LifecycleEvent, record.ReasonCode = "provider_response", "provider_response_observed"
			record.CurrentState, record.Selected = "DISPATCHED", true
			fillRoute(&record, state)
			appendRecord(withState(record, state), event.at)

		case eventUsageAvailableV2:
			if state == nil {
				return RunResultV2{}, fmt.Errorf("usage event for unknown request %s", opportunity.RequestID)
			}
			if !state.admitted {
				continue
			}
			request := UsageRevealRequestV2{
				RunID: config.RunID, RequestID: opportunity.RequestID, UsageItemID: opportunity.UsageItemID,
				DispatchID: state.dispatchID, ConfigSHA256: configSHA, RevealAt: opportunity.UsageAvailableAt,
			}
			receipt, err := issuer.revealUsage(context.Background(), request)
			if err != nil {
				return RunResultV2{}, fmt.Errorf("reveal usage %s: %w", opportunity.RequestID, err)
			}
			if err := issuer.verifyUsage(request, receipt); err != nil {
				return RunResultV2{}, fmt.Errorf("verify usage %s: %w", opportunity.RequestID, err)
			}
			if err := validateUsageReceiptForRunnerV2(config, configSHA, opportunity, request, receipt); err != nil {
				return RunResultV2{}, fmt.Errorf("usage %s: %w", opportunity.RequestID, err)
			}
			state.receipt = &receipt
			priceCandidate, err := selectedCandidateForCostV2(runtimeConfig, state)
			if err != nil {
				return RunResultV2{}, err
			}
			state.actualInputCost, err = ceilingProduct(priceCandidate.InputPriceMicrosPerMillion, receipt.ActualInputTokens)
			if err != nil {
				return RunResultV2{}, err
			}
			state.actualOutputCost, err = ceilingProduct(priceCandidate.OutputPriceMicrosPerMillion, receipt.ActualOutputTokens)
			if err != nil {
				return RunResultV2{}, err
			}
			state.actualCost, err = checkedAdd(state.actualInputCost, state.actualOutputCost)
			if err != nil {
				return RunResultV2{}, err
			}
			if nonReservingMethodV2(config.ComparatorMethod) {
				state.window.activeActual, err = checkedAdd(state.window.activeActual, state.actualCost)
				if err != nil {
					return RunResultV2{}, err
				}
			}
			record := baseRecord(opportunity)
			record.LifecycleEvent, record.ReasonCode = "usage_available", "usage_revealed_at_authorized_time"
			record.CurrentState, record.Effective, record.Selected = "DISPATCHED", true, true
			fillRoute(&record, state)
			fillUsage(&record, state)
			appendRecord(withState(record, state), event.at)

		case eventSettlementV2:
			if state == nil {
				return RunResultV2{}, fmt.Errorf("settlement for unknown request %s", opportunity.RequestID)
			}
			if !state.admitted {
				continue
			}
			if state.receipt == nil {
				return RunResultV2{}, fmt.Errorf("settlement %s attempted before authorized usage reveal", opportunity.RequestID)
			}
			record := baseRecord(opportunity)
			record.LifecycleEvent, record.Selected = "settlement_final", true
			fillRoute(&record, state)
			fillUsage(&record, state)
			if nonReservingMethodV2(config.ComparatorMethod) {
				key := opportunity.TenantID + "\x00" + opportunity.BudgetWindowID
				effective, err := settledLedger.Settle(key, opportunity.RequestID, state.actualCost)
				if err != nil || !effective {
					return RunResultV2{}, fmt.Errorf("settled-only settlement %s was not effective: %w", opportunity.RequestID, err)
				}
				record.ReasonCode, record.PreviousState, record.CurrentState = "settlement_finalized", "DISPATCHED", "FINALIZED"
				record.Effective = true
				if state.window.activeActual < state.actualCost {
					return RunResultV2{}, fmt.Errorf("settled-only active exposure underflow for %s", opportunity.RequestID)
				}
				state.window.activeActual -= state.actualCost
			} else {
				settleRequest := govar.SettleRequest{
					RequestID: opportunity.RequestID, SettlementID: eventID(config.Seed, streamSHA, opportunity.RequestID, "v2-settlement-final"),
					ProviderAttemptID: state.providerAttempt, TenantID: opportunity.TenantID, WorkloadUID: opportunity.WorkloadUID,
					ActualCostMicros: govar.MoneyMicros(state.actualCost), UsageVersion: 1, Final: true,
					Usage: []govarpricing.UsageQuantity{
						{Basis: aiopsv1alpha1.ProviderBasisInputTokens, Quantity: state.receipt.ActualInputTokens},
						{Basis: aiopsv1alpha1.ProviderBasisOutputTokens, Quantity: state.receipt.ActualOutputTokens},
					},
					AuthenticatedTenantID: opportunity.TenantID, AuthenticatedWorkloadUID: opportunity.WorkloadUID,
				}
				reservation, code, err := state.window.ctx.engine.Settle(settleRequest)
				if err != nil {
					return RunResultV2{}, fmt.Errorf("settle %s: %w", opportunity.RequestID, err)
				}
				if !reservation.Finalized || int64(reservation.BaseActualMicros) != state.actualCost {
					return RunResultV2{}, fmt.Errorf("settlement %s did not preserve authoritative integer usage cost", opportunity.RequestID)
				}
				record.ReasonCode = string(code)
				record.PreviousState, record.CurrentState = string(reservation.PreviousState), string(reservation.State)
				record.Effective = reservation.TransitionEffective
			}
			if config.ComparatorMethod == "expected_cost_router" {
				if state.window.expectedReserved < state.reserved {
					return RunResultV2{}, fmt.Errorf("expected_cost_router reserved ledger underflow for %s", opportunity.RequestID)
				}
				state.window.expectedReserved -= state.reserved
				state.window.expectedSettled, err = checkedAdd(state.window.expectedSettled, state.actualCost)
				if err != nil {
					return RunResultV2{}, fmt.Errorf("expected_cost_router settled ledger overflow for %s: %w", opportunity.RequestID, err)
				}
			}
			if config.ComparatorMethod == "adaptive_quantile" {
				if state.window == nil || state.window.adaptive == nil {
					return RunResultV2{}, fmt.Errorf("adaptive_quantile settlement %s lacks its estimator", opportunity.RequestID)
				}
				if err := state.window.adaptive.Observe(AdaptiveFeedback{
					FinalizedStep: opportunity.Sequence, OutputTokens: state.receipt.ActualOutputTokens,
					Selected: true, Final: true,
				}); err != nil {
					return RunResultV2{}, fmt.Errorf("observe adaptive_quantile feedback %s: %w", opportunity.RequestID, err)
				}
			}
			appendRecord(withState(record, state), event.at)
		}
	}

	return RunResultV2{
		Config: config, ConfigSHA256: configSHA, CanonicalConfigSHA256: canonicalConfigSHA,
		StreamSHA256: streamSHA, MatchedStreamKey: matchedKey, StreamFacts: facts,
		RuntimeProvenance: RuntimeProvenanceV2, FinalEvidenceEligible: false,
		Records: records, DecisionCounts: decisionCounts, LifecycleCounts: lifecycleCounts, Work: work,
	}, nil
}

func planRuntimeWorkV2(opportunities []OpportunityV2, method string) (RuntimeWorkV2, error) {
	if int64(len(opportunities)) > QualificationOpportunityLimitV2 {
		return RuntimeWorkV2{}, fmt.Errorf("E1 v2 qualification runtime is limited to %d opportunities per cell", QualificationOpportunityLimitV2)
	}
	windows := make(map[string]struct{})
	budgetWindows := make(map[string]struct{})
	for _, opportunity := range opportunities {
		windowKey := opportunity.TenantID + "\x00" + opportunity.BudgetWindowID
		windows[windowKey] = struct{}{}
		budgetWindows[opportunity.BudgetWindowID] = struct{}{}
	}
	if int64(len(windows)) > QualificationWindowLimitV2 {
		return RuntimeWorkV2{}, fmt.Errorf("E1 v2 qualification runtime is limited to %d tenant-window contexts per cell", QualificationWindowLimitV2)
	}
	work := RuntimeWorkV2{
		QualificationOpportunityLimit: QualificationOpportunityLimitV2,
		QualificationWindowLimit:      QualificationWindowLimitV2,
		OpportunityCount:              int64(len(opportunities)),
		ScheduledEventCount:           int64(len(opportunities)) * 4,
		WindowContextCount:            int64(len(windows)),
	}
	if nonReservingMethodV2(method) || len(opportunities) == 0 {
		work.EngineCount = 0
	} else {
		work.EngineCount = int64(len(budgetWindows))
	}
	if method == "gov_ar" && len(opportunities) > 0 {
		work.GOVARArtifactIndexBuildCount = 1
		work.GOVARWindowInitializationCount = int64(len(windows))
		work.GOVAROpportunityBindingCheckCount = int64(len(opportunities))
	}
	return work, nil
}

func nonReservingMethodV2(method string) bool {
	return method == "no_budget" || method == "settled_only"
}

type expectedCostRuntimeCandidateV2 struct {
	policy    ExpectedCandidate
	candidate govar.Candidate
}

func expectedCostAvailableV2(window *runtimeWindowV2) (int64, error) {
	if window == nil {
		return 0, errors.New("expected-cost window is missing")
	}
	if window.budget <= 0 || window.expectedSettled < 0 || window.expectedReserved < 0 {
		return 0, errors.New("expected-cost ledger facts are invalid")
	}
	available := window.budget - window.expectedSettled - window.expectedReserved
	if available < 0 {
		return 0, errors.New("expected-cost ledger availability is negative")
	}
	return available, nil
}

func candidateByModelRefV2(candidates []govar.Candidate, modelRef string) (govar.Candidate, bool) {
	for _, candidate := range candidates {
		if candidate.ModelRef == modelRef {
			return candidate, true
		}
	}
	return govar.Candidate{}, false
}

func selectedCandidateForCostV2(_ Config, state *runtimeRequestV2) (govar.Candidate, error) {
	if state == nil || state.selectedCandidate == nil {
		return govar.Candidate{}, errors.New("selected candidate is not bound before usage cost computation")
	}
	return *state.selectedCandidate, nil
}

func bindExpectedCostContextV2(ctx *tenantContext, config Config, op Opportunity, availableMicros int64) error {
	if ctx == nil || config.MethodConfig.ExpectedCost == nil {
		return errors.New("expected_cost_router binding requires context and method config")
	}
	runtimeCandidates, err := expectedCostRuntimeCandidatesV2(config, op)
	if err != nil {
		return err
	}
	allCandidates := make([]govar.Candidate, 0, len(runtimeCandidates))
	policyCandidates := make([]ExpectedCandidate, 0, len(runtimeCandidates))
	byID := make(map[string]govar.Candidate, len(runtimeCandidates))
	fits := false
	for _, runtimeCandidate := range runtimeCandidates {
		allCandidates = append(allCandidates, runtimeCandidate.candidate)
		policyCandidates = append(policyCandidates, runtimeCandidate.policy)
		byID[runtimeCandidate.policy.ID] = runtimeCandidate.candidate
		if runtimeCandidate.policy.HardFeasible && runtimeCandidate.policy.ExpectedReservedMicros <= availableMicros {
			fits = true
		}
	}
	ctx.candidates = allCandidates
	if !fits {
		return nil
	}
	selected, err := SelectExpectedCostCandidate(policyCandidates, availableMicros, config.MethodConfig.ExpectedCost.UtilityCostPenaltyPPBPerMicro)
	if err != nil {
		return err
	}
	candidate, ok := byID[selected.ID]
	if !ok {
		return fmt.Errorf("expected_cost_router selected unknown candidate %q", selected.ID)
	}
	ctx.candidates = []govar.Candidate{candidate}
	return nil
}

func expectedCostRuntimeCandidatesV2(config Config, op Opportunity) ([]expectedCostRuntimeCandidateV2, error) {
	if config.MethodConfig.ExpectedCost == nil || !shaPattern.MatchString(config.MethodConfig.ExpectedCost.PredictorArtifactSHA256) {
		return nil, errors.New("expected_cost_router predictor artifact is not hash-bound")
	}
	premium, err := expectedCostCandidateVariantV2(config, "experiment-model-expected-premium", config.InputPriceMicrosPerMillion, config.OutputPriceMicrosPerMillion, 900_000_000)
	if err != nil {
		return nil, err
	}
	economyInput := expectedCostEconomyPriceV2(config.InputPriceMicrosPerMillion)
	economyOutput := expectedCostEconomyPriceV2(config.OutputPriceMicrosPerMillion)
	economy, err := expectedCostCandidateVariantV2(config, "experiment-model-expected-economy", economyInput, economyOutput, 899_999_990)
	if err != nil {
		return nil, err
	}
	out := []expectedCostRuntimeCandidateV2{}
	for _, candidate := range []govar.Candidate{premium, economy} {
		reserved, err := expectedCostReservationForCandidateV2(config, op, candidate)
		if err != nil {
			return nil, err
		}
		out = append(out, expectedCostRuntimeCandidateV2{
			candidate: candidate,
			policy: ExpectedCandidate{
				ID:                     candidate.ModelRef,
				ExpectedQualityPPB:     int64(candidate.QualityScore * 1_000_000_000),
				ExpectedReservedMicros: reserved,
				HardFeasible:           hardFeasibleForCandidateV2(candidate, config, op),
			},
		})
	}
	return out, nil
}

func expectedCostCandidateVariantV2(config Config, modelRef string, inputPrice, outputPrice, qualityPPB int64) (govar.Candidate, error) {
	if !validIdentity(modelRef) || inputPrice < 0 || outputPrice < 0 || qualityPPB < 0 || qualityPPB > 1_000_000_000 {
		return govar.Candidate{}, errors.New("expected_cost_router candidate variant is invalid")
	}
	if config.MethodConfig.ExpectedCost == nil {
		return govar.Candidate{}, errors.New("expected_cost_router method config is missing")
	}
	candidate := experimentCandidate(config)
	root := DomainHash("govar-e1-v2-expected-cost-candidate-v1",
		[]byte(config.MethodConfig.ExpectedCost.PredictorArtifactSHA256),
		[]byte(config.MethodConfigSHA256), []byte(modelRef),
		[]byte(strconv.FormatInt(inputPrice, 10)), []byte(strconv.FormatInt(outputPrice, 10)),
		[]byte(strconv.FormatInt(qualityPPB, 10)))
	pricing := *candidate.PricingSnapshot.DeepCopy()
	pricing.Version = "experiment-pricing-v1-" + modelRef
	pricing.EvidenceSHA256 = DomainHash("govar-e1-v2-expected-cost-pricing-evidence-v1", []byte(root))
	pricing.SourceVersion = "synthetic-public-expected-cost-v1"
	for index := range pricing.Charges {
		switch pricing.Charges[index].Basis {
		case aiopsv1alpha1.ProviderBasisInputTokens:
			pricing.Charges[index].PriceMicrosPerUnit = inputPrice
		case aiopsv1alpha1.ProviderBasisOutputTokens:
			pricing.Charges[index].PriceMicrosPerUnit = outputPrice
		}
	}
	pricing.SnapshotSHA256 = govarpricing.SnapshotDigest(pricing)
	route := candidate.RouteSnapshot
	route.ModelName = modelRef
	route.ModelUID = modelRef + "-uid"
	route.ModelResourceVersion = modelRef + "-rv"
	route.PricingVersion = pricing.Version
	route.PricingComplianceHash = DomainHash("govar-e1-v2-expected-cost-pricing-compliance-v1", []byte(pricing.SnapshotSHA256), []byte(root))
	route.ProviderDeployment = modelRef
	route.SnapshotHash = govar.RouteSnapshotHash(route)
	candidate.ModelRef = modelRef
	candidate.ModelName = modelRef
	candidate.InputPriceMicrosPerMillion = inputPrice
	candidate.OutputPriceMicrosPerMillion = outputPrice
	candidate.PricingVersion = pricing.Version
	candidate.SnapshotVersion = route.SnapshotHash
	candidate.QualityScore = float64(qualityPPB) / 1_000_000_000
	candidate.CapEvidenceDigest = DomainHash("govar-e1-v2-expected-cost-cap-evidence-v1", []byte(root))
	candidate.PricingSnapshot = pricing
	candidate.RouteSnapshot = route
	return candidate, nil
}

func expectedCostEconomyPriceV2(price int64) int64 {
	if price <= 0 {
		return price
	}
	discounted := price / 10
	if discounted == 0 {
		return 1
	}
	return discounted
}

func expectedCostReservationForCandidateV2(config Config, op Opportunity, candidate govar.Candidate) (int64, error) {
	input, err := ceilingProduct(candidate.InputPriceMicrosPerMillion, op.InputTokens)
	if err != nil {
		return 0, err
	}
	outputTokens := min64(config.EstimateOutputTokens, op.MaxOutputTokens)
	output, err := ceilingProduct(candidate.OutputPriceMicrosPerMillion, outputTokens)
	if err != nil {
		return 0, err
	}
	return checkedAdd(input, output)
}

func hardFeasibleForOpportunityV2(config Config, op Opportunity) bool {
	candidate := experimentCandidate(config)
	return hardFeasibleForCandidateV2(candidate, config, op)
}

func hardFeasibleForCandidateV2(candidate govar.Candidate, config Config, op Opportunity) bool {
	if !candidate.Feasible || candidate.ContextWindow <= 0 || op.InputTokens+op.MaxOutputTokens > candidate.ContextWindow {
		return false
	}
	if !candidate.VerifiedOutputCap || op.MaxOutputTokens > candidate.VerifiedOutputCapTokens {
		return false
	}
	_, err := govarpricing.ReserveComponents(candidate.PricingSnapshot, govarpricing.RequestChargeContext{
		InputTokens: op.InputTokens, OutputTokens: op.MaxOutputTokens,
	}, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	return err == nil
}

func expectedReservationV2(config ConfigV2, op Opportunity, state *runtimeRequestV2, evaluatedAt time.Time) (int64, error) {
	if nonReservingMethodV2(config.ComparatorMethod) {
		return 0, nil
	}
	if evaluatedAt.IsZero() {
		return 0, errors.New("E1 v2 expected reservation requires event time")
	}
	inputPrice, outputPrice := config.InputPriceMicrosPerMillion, config.OutputPriceMicrosPerMillion
	if config.ComparatorMethod == "expected_cost_router" {
		if state == nil || state.selectedCandidate == nil {
			return 0, errors.New("expected_cost_router reservation lacks selected candidate")
		}
		inputPrice, outputPrice = state.selectedCandidate.InputPriceMicrosPerMillion, state.selectedCandidate.OutputPriceMicrosPerMillion
	}
	input, err := ceilingProduct(inputPrice, op.InputTokens)
	if err != nil {
		return 0, err
	}
	tokens := op.MaxOutputTokens
	switch config.ComparatorMethod {
	case "strict_max":
	case "mean", "fixed_quantile", "fixed_estimate", "expected_cost_router":
		tokens = min64(config.EstimateOutputTokens, op.MaxOutputTokens)
	case "mean_margin":
		tokens = min64(config.EstimateOutputTokens+config.MarginOutputTokens, op.MaxOutputTokens)
	case "adaptive_quantile":
		if state == nil || state.adaptivePlan == nil {
			return 0, errors.New("adaptive_quantile reservation was not prospectively planned")
		}
		tokens = min64(state.adaptivePlan.OutputTokens, op.MaxOutputTokens)
	case "gov_ar":
		return 0, errors.New("GOV-AR expected reservation must use the slot-bound indexed verifier")
	default:
		return 0, fmt.Errorf("unsupported E1 v2 reservation method %q", config.ComparatorMethod)
	}
	output, err := ceilingProduct(outputPrice, tokens)
	if err != nil {
		return 0, err
	}
	return checkedAdd(input, output)
}

func govarRuntimeSlotKeyV2(op Opportunity) string {
	return fmt.Sprintf("%s\x00%d", cohortKey(op.TenantID, op.BudgetWindowID, op.CohortID), op.CohortIndex)
}

func bindAdaptiveContextV2(ctx *tenantContext, config Config, plan AdaptivePlan, evaluatedAt time.Time) error {
	if ctx == nil || config.MethodConfig.Adaptive == nil {
		return errors.New("adaptive_quantile binding requires a context and method config")
	}
	evaluatedAt = evaluatedAt.UTC()
	if evaluatedAt.IsZero() {
		return errors.New("adaptive_quantile binding requires event time")
	}
	method := config.MethodConfig.Adaptive
	candidate := experimentCandidate(config)
	priceRegime, err := govarcalibration.CanonicalPriceRegime(
		candidate.PricingSnapshot.SnapshotSHA256,
		candidate.RouteSnapshot.ProviderUID,
		candidate.RouteSnapshot.ProviderDeployment,
	)
	if err != nil {
		return err
	}
	capRegime, err := govar.CanonicalCandidateCapPathAdapterRegime(candidate)
	if err != nil {
		return err
	}
	featureRegime := DomainHash("govar-e1-v2-adaptive-feature-regime-v1", []byte(config.MethodConfigSHA256), []byte(config.SourceSHA256))
	cohortRegime := DomainHash("govar-e1-v2-adaptive-cohort-regime-v1", []byte(config.CellID), []byte(config.Scenario), []byte(strconv.FormatUint(config.Seed, 10)))
	splitRegime := DomainHash("govar-e1-v2-adaptive-split-regime-v1", []byte(config.SplitSHA256), []byte(cohortRegime))
	calibrationInput := DomainHash("govar-e1-v2-adaptive-calibration-input-v1", []byte(config.MethodConfig.ArtifactSHA256), []byte(featureRegime), []byte(priceRegime), []byte(capRegime), []byte(splitRegime), []byte(cohortRegime))
	publication := DomainHash("govar-e1-v2-adaptive-publication-v1", []byte(calibrationInput), []byte(config.MethodConfigSHA256), []byte(strconv.FormatInt(method.CoverageTargetPPB, 10)))
	driftSHA := DomainHash("govar-e1-v2-adaptive-drift-v1", []byte(publication), []byte(plan.EffectiveMode), []byte(plan.FallbackReason), []byte(strconv.FormatInt(plan.Support, 10)))

	support := method.CalibrationSupport
	rank := int64(0)
	lower := int64(0)
	if support > 0 {
		rank = conformalRank(support, method.CoverageTargetPPB)
		lower = rank * 1_000_000_000 / (support + 1)
	}
	windowEnd := evaluatedAt.Add(-time.Second)
	windowStart := windowEnd.Add(-time.Hour)
	driftDetected := plan.EffectiveMode == "strict_provider_cap" && plan.FallbackReason == "coverage_drift_detected"
	if plan.EffectiveMode != "adaptive_quantile" && plan.FallbackReason == "calibration_support_insufficient" {
		support = method.CalibrationSupport
	}
	routing := ctx.routing
	routing.Generation = 1
	routing.Status.ObservedGeneration = 1
	routing.Status.LastEvaluatedAt = &metav1.Time{Time: evaluatedAt}
	routing.Spec.GOVAR.Reservation.Method = aiopsv1alpha1.GOVARReservationAdaptiveQuantile
	routing.Spec.GOVAR.Calibration = &aiopsv1alpha1.GOVARCalibrationPolicy{
		ArtifactRef: "adaptive-qualification", ArtifactSHA256: config.MethodConfig.ArtifactSHA256,
		CalibrationInputSHA256: calibrationInput, RegistryID: "experiment-adaptive-qualification",
		Version: "e1-v2-adaptive-qualification-v1", FeatureSchemaVersion: "e1-v2-usage-only-features-v1",
		PriceRegimeSHA256: priceRegime, CapRegimeSHA256: capRegime,
		ProducerSoftwareSHA256: config.SoftwareSHA256, CoverageTargetPPB: method.CoverageTargetPPB,
		MinimumSupport: method.MinimumSupport, MaxAgeSeconds: 7200,
	}
	routing.Spec.GOVAR.Drift = aiopsv1alpha1.GOVARDriftPolicy{
		Detector: "coverage-gap", ThresholdPPB: method.DriftThresholdPPB,
		Fallback: "strict_provider_cap", RevalidationMinimumSupport: method.MinimumSupport,
	}
	routing.Status.GOVAR = &aiopsv1alpha1.GOVARRoutingPolicyStatus{
		Calibration: &aiopsv1alpha1.GOVARCalibrationStatus{
			EvidenceSource:    govar.CalibrationEvidenceSourceInMemoryQualificationV1,
			PublicationSHA256: publication, SourceResourceVersion: "immutable-e1-v2-method-config",
			ArtifactRef: "adaptive-qualification", Version: "e1-v2-adaptive-qualification-v1",
			ArtifactSHA256: config.MethodConfig.ArtifactSHA256, CalibrationInputSHA256: calibrationInput,
			FeatureSchemaVersion: "e1-v2-usage-only-features-v1", PriceRegimeSHA256: priceRegime,
			CapRegimeSHA256: capRegime, ProducerSoftwareSHA256: config.SoftwareSHA256,
			CoverageTargetPPB: method.CoverageTargetPPB, EmpiricalCoveragePPB: lower,
			CalibrationMethod:                    "split_conformal_order_statistic_v1",
			ExchangeableMarginalCoverageLowerPPB: lower,
			ConformalRank:                        rank, CoverageBoundKind: govarcalibration.ExactMarginalBoundKind,
			CoverageBoundNumerator: rank, CoverageBoundDenominator: support + 1,
			CoverageIntervalLowerPPB: lower, CoverageIntervalUpperPPB: 1_000_000_000,
			CoverageConfidencePPB: 1_000_000_000, FeatureRegimeSHA256: featureRegime,
			SplitOpportunityRegimeSHA256: splitRegime, CohortRegimeSHA256: cohortRegime,
			Support: support, AdaptiveOutputTokens: min64(plan.OutputTokens, config.VerifiedOutputCapTokens),
			Valid: true, CalibrationWindowStart: metav1.NewTime(windowStart),
			CalibrationWindowEnd: metav1.NewTime(windowEnd), ObservedAt: metav1.NewTime(evaluatedAt),
		},
		Drift: &aiopsv1alpha1.GOVARDriftStatus{
			DriftSHA256: driftSHA, Detected: driftDetected, ConservativeMode: driftDetected,
			Detector: "coverage-gap", ThresholdPPB: method.DriftThresholdPPB,
			MonitoringInputSHA256: DomainHash("govar-e1-v2-adaptive-monitoring-input-v1", []byte(publication)),
			Support:               max64(method.MinimumSupport, plan.Support), EmpiricalCoveragePPB: lower,
			ConfidencePPB: 1_000_000_000, IntervalLowerPPB: lower, IntervalUpperPPB: 1_000_000_000,
			MonitoringWindowStart: metav1.NewTime(windowStart), MonitoringWindowEnd: metav1.NewTime(windowEnd),
			ObservedAt: metav1.NewTime(evaluatedAt),
		},
	}
	ctx.routing = routing
	return nil
}

// buildGOVARRuntimeIndexV2 converts the already-validated immutable artifact
// into keyed slot/evidence lookups. The explicit qualification guardrails and
// work counters above bound the supported 10k-row, 400-window pilot cell; this
// comment does not claim a measured complexity class.
func buildGOVARRuntimeIndexV2(config Config, opportunities []Opportunity) (*govarRuntimeIndexV2, error) {
	method := config.MethodConfig.GOVAR
	if method == nil {
		return nil, errors.New("GOV-AR runtime index requires a method artifact")
	}
	evidence := make(map[string]GOVARCalibrationEvidence, len(method.EvidenceRegistry))
	for _, item := range method.EvidenceRegistry {
		evidence[item.PublicationSHA256] = item
	}
	index := &govarRuntimeIndexV2{
		bindings: make(map[string]govarRuntimeBindingV2, len(method.SlotBounds)),
		cohorts:  make(map[string][]GOVARSlotBound), windowCohorts: make(map[string][]string),
	}
	for _, slot := range method.SlotBounds {
		item, exists := evidence[slot.EvidencePublicationSHA256]
		if !exists {
			return nil, fmt.Errorf("GOV-AR slot %s lacks indexed evidence", slot.RequestID)
		}
		key := fmt.Sprintf("%s\x00%d", cohortKey(slot.TenantID, slot.BudgetWindowID, slot.CohortID), slot.CohortIndex)
		if _, duplicate := index.bindings[key]; duplicate {
			return nil, fmt.Errorf("GOV-AR runtime index duplicates slot %s", key)
		}
		index.bindings[key] = govarRuntimeBindingV2{slot: slot, evidence: item}
		cohort := cohortKey(slot.TenantID, slot.BudgetWindowID, slot.CohortID)
		if _, exists := index.cohorts[cohort]; !exists {
			window := slot.TenantID + "\x00" + slot.BudgetWindowID
			index.windowCohorts[window] = append(index.windowCohorts[window], cohort)
		}
		index.cohorts[cohort] = append(index.cohorts[cohort], slot)
	}
	seen := make(map[string]struct{}, len(opportunities))
	for _, opportunity := range opportunities {
		key := govarRuntimeSlotKeyV2(opportunity)
		binding, exists := index.bindings[key]
		if !exists || binding.slot.RequestID != opportunity.RequestID || binding.slot.OpportunityDigest != GOVAROpportunityDigest(config, opportunity) {
			return nil, fmt.Errorf("GOV-AR opportunity %s does not match its indexed immutable slot", opportunity.RequestID)
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("GOV-AR opportunity %s reuses an indexed slot", opportunity.RequestID)
		}
		seen[key] = struct{}{}
	}
	if len(seen) != len(index.bindings) {
		return nil, fmt.Errorf("GOV-AR stream covers %d indexed slots, artifact contains %d", len(seen), len(index.bindings))
	}
	return index, nil
}

func prepareGOVAREngineIndexedV2(engine *govar.Engine, config Config, index *govarRuntimeIndexV2, window string) error {
	method := config.MethodConfig.GOVAR
	authorityKey := []byte(DomainHash("govar-experiment-cohort-authority-key-v1", []byte(config.MethodConfigSHA256), []byte(config.SourceSHA256)))
	if err := engine.ConfigureLedgerAuthority(experimentCohortAuthorityID, authorityKey); err != nil {
		return err
	}
	frozenAt, _ := time.Parse(time.RFC3339Nano, method.FrozenAt)
	keys := append([]string(nil), index.windowCohorts[window]...)
	if len(keys) == 0 {
		return fmt.Errorf("GOV-AR runtime window %q has no indexed cohort", window)
	}
	sort.Strings(keys)
	for _, key := range keys {
		slots := append([]GOVARSlotBound(nil), index.cohorts[key]...)
		sort.Slice(slots, func(i, j int) bool { return slots[i].CohortIndex < slots[j].CohortIndex })
		membershipSHA, weightsSHA, err := govarCohortDigests(slots)
		if err != nil {
			return err
		}
		cohort := govar.FrozenCohort{
			TenantID: slots[0].TenantID, CohortID: slots[0].CohortID, Size: int64(len(slots)),
			TenantRiskPPB: method.TenantRiskPPB, DataHash: membershipSHA, ConfigHash: weightsSHA,
			ProtocolHash: config.ProtocolSHA256, FrozenAt: frozenAt, LedgerLayoutID: govar.LedgerLayoutID,
			RouteSnapshotSchema: govar.RouteSnapshotSchemaID, SoftwareHash: config.SoftwareSHA256,
		}
		for _, slot := range slots {
			cohort.Slots = append(cohort.Slots, govar.FrozenCohortSlot{
				Index: slot.CohortIndex, RequestID: slot.RequestID, OpportunityDigest: slot.OpportunityDigest, WeightPPB: slot.WeightPPB,
			})
		}
		cohort, err = govar.SignFrozenCohort(cohort, experimentCohortAuthorityID, authorityKey)
		if err != nil {
			return err
		}
		if err := engine.RegisterFrozenCohort(context.Background(), cohort); err != nil {
			return err
		}
	}
	return nil
}

func expectedGOVARReservationIndexedV2(config Config, opportunity Opportunity, binding govarRuntimeBindingV2, evaluatedAt time.Time) (int64, error) {
	input, err := ceilingProduct(config.InputPriceMicrosPerMillion, opportunity.InputTokens)
	if err != nil {
		return 0, err
	}
	outputTokens := opportunity.MaxOutputTokens
	if !govarEvidenceFallsBackAtV2(config, binding.evidence, evaluatedAt) {
		outputTokens = min64(binding.evidence.Calibration.UpperOutputTokens, opportunity.MaxOutputTokens)
	}
	output, err := ceilingProduct(config.OutputPriceMicrosPerMillion, outputTokens)
	if err != nil {
		return 0, err
	}
	return checkedAdd(input, output)
}

func govarEvidenceFallsBackAtV2(config Config, evidence GOVARCalibrationEvidence, evaluatedAt time.Time) bool {
	method := config.MethodConfig.GOVAR
	if evidence.Drift.Result.Support < method.RevalidationMinimumSupport {
		return true
	}
	evaluatedAt = evaluatedAt.UTC()
	maxAge := time.Duration(method.MaxAgeSeconds) * time.Second
	if evidence.Calibration.WindowEnd.After(evaluatedAt) || evidence.Drift.Result.WindowEnd.After(evaluatedAt) ||
		evaluatedAt.Sub(evidence.Calibration.WindowEnd) > maxAge || evaluatedAt.Sub(evidence.Drift.Result.WindowEnd) > maxAge {
		return true
	}
	return evidence.Drift.Result.Detected
}

func bindGOVARContextIndexedV2(ctx *tenantContext, config Config, opportunity Opportunity, binding govarRuntimeBindingV2, evaluatedAt time.Time) error {
	method := config.MethodConfig.GOVAR
	slot, evidence := binding.slot, binding.evidence
	cohort, err := ctx.engine.ExportFrozenCohort(context.Background(), opportunity.TenantID, opportunity.CohortID)
	if err != nil {
		return err
	}
	a, d := evidence.Calibration, evidence.Drift
	now, err := time.Parse(time.RFC3339Nano, config.VirtualStart)
	if err != nil {
		return err
	}
	routing := ctx.routing
	routing.Generation = 1
	routing.Status.ObservedGeneration = 1
	routing.Status.LastEvaluatedAt = &metav1.Time{Time: evaluatedAt.UTC()}
	routing.Spec.Objective = method.JointSelection.Objective
	routing.Spec.GOVAR.Reservation.Method = aiopsv1alpha1.GOVARReservationFixedCohort
	routing.Spec.GOVAR.Calibration = &aiopsv1alpha1.GOVARCalibrationPolicy{
		ArtifactRef: a.ArtifactRef, ArtifactSHA256: a.ArtifactSHA256, CalibrationInputSHA256: a.SourceObservationsSHA256,
		RegistryID: "experiment-slot-" + slot.CohortID, Version: a.Version, FeatureSchemaVersion: a.FeatureSchemaVersion,
		PriceRegimeSHA256: a.PriceRegimeSHA256, CapRegimeSHA256: a.CapRegimeSHA256,
		ProducerSoftwareSHA256: a.ProducerSoftwareSHA256, CoverageTargetPPB: a.CoverageTargetPPB,
		MinimumSupport: a.MinimumSupport, MaxAgeSeconds: method.MaxAgeSeconds,
	}
	routing.Spec.GOVAR.Drift = aiopsv1alpha1.GOVARDriftPolicy{
		Detector: "coverage-gap", ThresholdPPB: method.DriftThresholdPPB,
		Fallback: "strict_provider_cap", RevalidationMinimumSupport: method.RevalidationMinimumSupport,
	}
	routing.Spec.GOVAR.Cohort = &aiopsv1alpha1.GOVARCohortPolicy{
		RegistryRef: cohort.RegistryDigest, Size: cohort.Size, OpportunitySetHash: cohort.DataHash,
		WeightsHash: cohort.ConfigHash, FrozenAt: metav1.NewTime(cohort.FrozenAt),
	}
	routing.Spec.GOVAR.Risk = &aiopsv1alpha1.GOVARRiskPolicy{TenantRiskPPB: method.TenantRiskPPB, Allocation: "fixed-weights"}
	routing.Status.GOVAR = &aiopsv1alpha1.GOVARRoutingPolicyStatus{
		Calibration: &aiopsv1alpha1.GOVARCalibrationStatus{
			EvidenceSource: govar.CalibrationEvidenceSourceInMemoryQualificationV1, PublicationSHA256: evidence.PublicationSHA256,
			SourceResourceVersion: "immutable-experiment-slot", ArtifactRef: a.ArtifactRef, Version: a.Version,
			ArtifactSHA256: a.ArtifactSHA256, CalibrationInputSHA256: a.SourceObservationsSHA256,
			FeatureSchemaVersion: a.FeatureSchemaVersion, PriceRegimeSHA256: a.PriceRegimeSHA256,
			CapRegimeSHA256: a.CapRegimeSHA256, ProducerSoftwareSHA256: a.ProducerSoftwareSHA256,
			CoverageTargetPPB: a.CoverageTargetPPB, EmpiricalCoveragePPB: a.EmpiricalCoveragePPB,
			CalibrationMethod: a.CalibrationMethod, ExchangeableMarginalCoverageLowerPPB: a.ExchangeableMarginalCoverageLowerPPB,
			ConformalRank: a.ConformalRank, CoverageBoundKind: a.CoverageBoundKind,
			CoverageBoundNumerator: a.CoverageBoundNumerator, CoverageBoundDenominator: a.CoverageBoundDenominator,
			CoverageIntervalLowerPPB: a.CoverageIntervalLowerPPB, CoverageIntervalUpperPPB: a.CoverageIntervalUpperPPB,
			CoverageConfidencePPB: a.CoverageConfidencePPB, FeatureRegimeSHA256: a.FeatureRegimeSHA256,
			SplitOpportunityRegimeSHA256: a.SplitOpportunityRegimeSHA256, CohortRegimeSHA256: a.CohortRegimeSHA256,
			Support: a.Support, AdaptiveOutputTokens: a.UpperOutputTokens, Valid: true,
			CalibrationWindowStart: metav1.NewTime(a.WindowStart), CalibrationWindowEnd: metav1.NewTime(a.WindowEnd),
			ObservedAt: metav1.NewTime(now),
		},
		Drift: &aiopsv1alpha1.GOVARDriftStatus{
			DriftSHA256: d.DriftSHA256, Detected: d.Result.Detected, ConservativeMode: d.Result.Detected,
			Detector: "coverage-gap", ThresholdPPB: d.Result.ThresholdPPB, MonitoringInputSHA256: d.Result.MonitoringInputSHA256,
			Support: d.Result.Support, EmpiricalCoveragePPB: d.Result.EmpiricalCoveragePPB, ConfidencePPB: d.Result.ConfidencePPB,
			IntervalLowerPPB: d.Result.IntervalLowerPPB, IntervalUpperPPB: d.Result.IntervalUpperPPB,
			MonitoringWindowStart: metav1.NewTime(d.Result.WindowStart), MonitoringWindowEnd: metav1.NewTime(d.Result.WindowEnd),
			ObservedAt: metav1.NewTime(now),
		},
	}
	ctx.routing = routing
	return nil
}

func projectOpportunityV2(op OpportunityV2) Opportunity {
	return Opportunity{
		SchemaVersion: OpportunitySchema, RecordType: op.RecordType, Sequence: op.Sequence,
		RequestID: op.RequestID, TenantID: op.TenantID, BudgetWindowID: op.BudgetWindowID,
		WorkloadUID: op.WorkloadUID, BudgetMicros: op.BudgetMicros, CohortID: op.CohortID,
		CohortIndex: op.CohortIndex, InputTokens: op.InputTokens, MaxOutputTokens: op.MaxOutputTokens,
		FaultMode: op.FaultMode, FeedbackItemID: op.UsageItemID,
	}
}

func lifecycleRecordDigestV2(record LifecycleRecordV2) string {
	raw, _ := json.Marshal(record)
	return DomainHash("govar-e1-lifecycle-record-v2", raw)
}

func validateUsageReceiptForRunnerV2(config ConfigV2, configSHA string, opportunity OpportunityV2, request UsageRevealRequestV2, receipt UsageReceiptV2) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.RunID != config.RunID || receipt.RequestID != opportunity.RequestID || receipt.UsageItemID != opportunity.UsageItemID ||
		receipt.DispatchID != request.DispatchID || receipt.ConfigSHA256 != configSHA ||
		receipt.UsageBindingSHA256 != config.UsageBindingSHA256 || receipt.UsageAvailableAt != opportunity.UsageAvailableAt ||
		receipt.RevealedAt != opportunity.UsageAvailableAt {
		return errors.New("usage receipt is not exactly bound to config, dispatch, item, and causal availability")
	}
	inputMismatch := receipt.ActualInputTokens != opportunity.InputTokens
	capViolation := receipt.ActualOutputTokens > opportunity.MaxOutputTokens || receipt.ActualOutputTokens > config.VerifiedOutputCapTokens
	switch opportunity.FaultMode {
	case FaultNominal:
		if inputMismatch || capViolation {
			return errors.New("nominal usage violates exact input or verified output bound")
		}
	case FaultProviderCapViolation:
		if inputMismatch || !capViolation {
			return errors.New("provider cap fault must contain only an output-bound violation")
		}
	case FaultKnownInputMismatch:
		if !inputMismatch || capViolation {
			return errors.New("known-input fault must contain only an exact-input mismatch")
		}
	}
	return nil
}
