package govarexperiment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
)

type observationalWindow struct {
	budget  int64
	settled int64
	active  map[string]observationalPending
}

type observationalPending struct {
	opportunity      Opportunity
	dueStep          int64
	tieBreaker       uint64
	outcome          DispatchedOutcome
	actualInputCost  int64
	actualOutputCost int64
	actualCost       int64
	attemptID        string
}

// runNonReserving executes the two comparators whose contracts explicitly
// forbid pre-dispatch monetary holds. It is intentionally separate from the
// production reserve ledger: no_budget is observational and settled_only has
// an atomic idempotent settlement ledger but no pending liability field.
func runNonReserving(config Config, configRaw, streamRaw []byte, issuer OutcomeIssuer) (RunResult, error) {
	if config.ComparatorMethod != "no_budget" && config.ComparatorMethod != "settled_only" {
		return RunResult{}, errors.New("non-reserving adapter received a reserving method")
	}
	if config.EvidenceTier == "frozen_authorized" {
		return RunResult{}, errors.New("frozen non-reserving execution requires the PostgreSQL selected-feedback authority ledger")
	}
	opportunities, err := ParseStream(streamRaw, config)
	if err != nil {
		return RunResult{}, err
	}
	configSHA, streamSHA := SHA256(configRaw), SHA256(streamRaw)
	matchedKey := DomainHash("govar-matched-stream-v1", []byte(streamSHA), []byte(strconv.FormatUint(config.Seed, 10)))
	canonical, _ := jsonCanonicalConfig(config)
	virtualStart, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	candidate := experimentCandidate(config)
	policyVersion := DomainHash("govar-experiment-observational-policy-v1", []byte(config.MethodConfigSHA256))
	settlementLedger := NewSettledOnlyLedger()

	windows := map[string]*observationalWindow{}
	windowExamples := map[string]Opportunity{}
	for _, op := range opportunities {
		key := op.TenantID + "\x00" + op.BudgetWindowID
		if windows[key] == nil {
			windows[key] = &observationalWindow{budget: op.BudgetMicros, active: map[string]observationalPending{}}
			windowExamples[key] = op
		}
	}
	windowKeys := sortedWindowKeys(func() map[string]int64 {
		result := map[string]int64{}
		for key, window := range windows {
			result[key] = window.budget
		}
		return result
	}())

	var records []LifecycleRecord
	var pending []observationalPending
	eventIndex := int64(0)
	appendRecord := func(row LifecycleRecord, step int64) {
		eventIndex++
		row.EventIndex = eventIndex
		row.LogicalTimeStep = step
		row.Timestamp = virtualStart.Add(time.Duration(step)*time.Second + time.Duration(eventIndex)*time.Nanosecond).Format(time.RFC3339Nano)
		records = append(records, row)
	}
	baseRecord := func(op Opportunity, tie uint64) LifecycleRecord {
		return LifecycleRecord{SchemaVersion: LifecycleSchema, RecordType: RecordLifecycle, ExperimentID: config.ExperimentID, RunID: config.RunID,
			CellID: config.CellID, Scenario: config.Scenario, Seed: config.Seed, StreamSHA256: streamSHA, ConfigSHA256: configSHA,
			MatchedStreamKey: matchedKey, RegistryID: config.RegistryID, ProtocolMethodID: config.ProtocolMethodID,
			ComparatorMethod: config.ComparatorMethod, ProductionReservationMode: config.ProductionReservationMode, MethodConfigSHA256: config.MethodConfigSHA256,
			ClusterID: config.ClusterID, TrialID: config.TrialID, SourceSHA256: config.SourceSHA256, SoftwareSHA256: config.SoftwareSHA256,
			DataSHA256: config.DataSHA256, ProtocolSHA256: config.ProtocolSHA256, SplitSHA256: config.SplitSHA256,
			FeedbackModelMapSHA256: config.SelectedFeedbackModelMapSHA256,
			StreamSequence:         op.Sequence, ScheduleTieBreaker: tie, RequestID: op.RequestID, TenantID: op.TenantID,
			BudgetWindowID: op.BudgetWindowID, WorkloadUID: op.WorkloadUID, CohortID: op.CohortID, CohortIndex: op.CohortIndex,
			FaultMode: op.FaultMode, InputTokens: op.InputTokens, MaxOutputTokens: op.MaxOutputTokens,
			InputPriceMicrosPerMillion: config.InputPriceMicrosPerMillion, OutputPriceMicrosPerMillion: config.OutputPriceMicrosPerMillion,
			VerifiedOutputCapTokens: config.VerifiedOutputCapTokens, FeedbackItemID: op.FeedbackItemID, Attempted: true, BudgetMicros: op.BudgetMicros}
	}
	withLedger := func(row LifecycleRecord, window *observationalWindow) LifecycleRecord {
		windowKey := row.TenantID + "\x00" + row.BudgetWindowID
		window.settled = settlementLedger.Settled(windowKey)
		row.SettledMicros = window.settled
		row.OutstandingMicros = 0
		row.CarriedMicros = 0
		row.AvailableMicros = window.budget - window.settled
		row.ActiveReservations = 0
		row.EngineWindowID = "observational-" + row.BudgetWindowID
		return row
	}
	applyRoute := func(row *LifecycleRecord, op Opportunity) {
		row.PricingVersion = candidate.PricingVersion
		row.PricingSnapshotSHA256 = candidate.PricingSnapshot.SnapshotSHA256
		row.CapEvidenceSHA256 = candidate.CapEvidenceDigest
		row.PolicyVersion = policyVersion
		row.SelectedDeployment = candidate.ModelRef
		row.SelectedModel = candidate.ModelName
		row.SelectedProvider = candidate.ProviderRef
		row.ProviderAttemptID = op.RequestID + ":attempt:1"
		row.EffectiveMethod = config.ProductionReservationMode
	}
	fillOutcome := func(row *LifecycleRecord, item observationalPending) {
		row.ActualInputTokens = item.outcome.ActualInputTokens
		row.ActualOutputTokens = item.outcome.ActualOutputTokens
		row.ActualInputCostMicros = item.actualInputCost
		row.ActualOutputCostMicros = item.actualOutputCost
		row.ActualCostMicros = item.actualCost
		row.DispatchID = item.outcome.Feedback.DispatchID
		row.FeedbackObservationID = item.outcome.Feedback.ObservationID
		row.FeedbackRequestSHA256 = item.outcome.Feedback.RequestID
		row.FeedbackItemSHA256 = item.outcome.Feedback.ItemID
		row.FeedbackSelectedModelSHA256 = item.outcome.Feedback.SelectedModelID
		row.SelectedScore = item.outcome.Feedback.SelectedScore
		row.FeedbackReplayed = item.outcome.Feedback.Replayed
		row.UsageReceiptSHA256 = item.outcome.UsageReceiptSHA256
		row.FeedbackAuthoritySHA256 = item.outcome.Evidence.AuthoritySHA256
		row.FeedbackModelMapSHA256 = item.outcome.Evidence.ModelMapSHA256
		row.FeedbackArtifactSHA256 = item.outcome.Evidence.FeedbackArtifactSHA256
		row.Analyzed = true
		row.EffectiveMethod = config.ProductionReservationMode
	}

	settle := func(item observationalPending) error {
		op := item.opportunity
		windowKey := op.TenantID + "\x00" + op.BudgetWindowID
		window := windows[windowKey]
		effective, err := settlementLedger.Settle(windowKey, op.RequestID, item.actualCost)
		if err != nil || !effective {
			return fmt.Errorf("first settlement %s was not effective: effective=%v err=%v", op.RequestID, effective, err)
		}
		window.settled = settlementLedger.Settled(windowKey)
		delete(window.active, op.RequestID)
		row := baseRecord(op, item.tieBreaker)
		row.LifecycleEvent = "settlement_final"
		row.ReasonCode = "reservation_exceeded"
		if item.actualCost == 0 {
			row.ReasonCode = "settlement_finalized"
		}
		row.PreviousState = "DISPATCHED"
		row.CurrentState = "FINALIZED"
		row.Effective = true
		row.Selected = true
		applyRoute(&row, op)
		fillOutcome(&row, item)
		appendRecord(withLedger(row, window), item.dueStep)
		if op.DuplicateSettlement {
			effective, err = settlementLedger.Settle(windowKey, op.RequestID, item.actualCost)
			if err != nil || effective {
				return fmt.Errorf("duplicate settlement %s was not an idempotent replay: effective=%v err=%v", op.RequestID, effective, err)
			}
			row = baseRecord(op, item.tieBreaker)
			row.LifecycleEvent = "settlement_duplicate"
			row.ReasonCode = "duplicate_settlement"
			row.PreviousState = "FINALIZED"
			row.CurrentState = "FINALIZED"
			row.Selected = true
			applyRoute(&row, op)
			fillOutcome(&row, item)
			appendRecord(withLedger(row, window), item.dueStep)
		}
		if op.ConflictingSettlementReplay {
			conflictingCost, addErr := checkedAdd(item.actualCost, 1)
			if addErr != nil {
				return addErr
			}
			if effective, err = settlementLedger.Settle(windowKey, op.RequestID, conflictingCost); err == nil || effective {
				return fmt.Errorf("conflicting settlement %s was not rejected: effective=%v err=%v", op.RequestID, effective, err)
			}
			row = baseRecord(op, item.tieBreaker)
			row.LifecycleEvent = "settlement_conflict_rejected"
			row.ReasonCode = "duplicate_event"
			row.PreviousState = "FINALIZED"
			row.CurrentState = "FINALIZED"
			row.Selected = true
			applyRoute(&row, op)
			fillOutcome(&row, item)
			appendRecord(withLedger(row, window), item.dueStep)
		}
		return nil
	}
	flush := func(limit int64, all bool) error {
		sort.Slice(pending, func(i, j int) bool {
			if pending[i].dueStep != pending[j].dueStep {
				return pending[i].dueStep < pending[j].dueStep
			}
			if pending[i].tieBreaker != pending[j].tieBreaker {
				return pending[i].tieBreaker < pending[j].tieBreaker
			}
			return pending[i].opportunity.Sequence < pending[j].opportunity.Sequence
		})
		remaining := pending[:0]
		for _, item := range pending {
			if !all && item.dueStep > limit {
				remaining = append(remaining, item)
				continue
			}
			if err := settle(item); err != nil {
				return err
			}
		}
		pending = remaining
		return nil
	}
	emitSnapshots := func(epoch int64) {
		for _, key := range windowKeys {
			op := windowExamples[key]
			row := baseRecord(op, 0)
			row.RecordType = RecordSnapshot
			row.RequestID, row.WorkloadUID, row.CohortID, row.FeedbackItemID = "", "", "", ""
			row.CohortIndex = 0
			row.Attempted = false
			row.LifecycleEvent = "opportunity_epoch_snapshot"
			row.ReasonCode = "prespecified_opportunity_epoch"
			row.FaultMode = FaultNominal
			row.StreamSequence = epoch
			window := windows[key]
			for _, item := range window.active {
				row.ActiveActualMicros += item.actualCost
			}
			appendRecord(withLedger(row, window), epoch)
		}
	}

	for _, op := range opportunities {
		if err := flush(op.Sequence, false); err != nil {
			return RunResult{}, err
		}
		window := windows[op.TenantID+"\x00"+op.BudgetWindowID]
		tie := RandomUint64("settlement-order", config.Seed, []byte(streamSHA), []byte(op.RequestID))
		decision, reason := NoBudgetDecision(candidate.Feasible)
		if config.ComparatorMethod == "settled_only" {
			decision, reason, err = SettledOnlyDecision(AdmissionFacts{HardFeasible: candidate.Feasible, BudgetMicros: window.budget,
				SettledMicros: settlementLedger.Settled(op.TenantID + "\x00" + op.BudgetWindowID)})
			if err != nil {
				return RunResult{}, err
			}
		}
		row := baseRecord(op, tie)
		row.LifecycleEvent = "admission"
		row.Decision = decision
		row.ReasonCode = reason
		row.Effective = decision == "ADMIT"
		row.Selected = row.Effective
		if row.Effective {
			row.CurrentState = "RESERVED"
			applyRoute(&row, op)
		}
		appendRecord(withLedger(row, window), op.Sequence)
		if !row.Effective {
			emitSnapshots(op.Sequence)
			continue
		}

		row = baseRecord(op, tie)
		row.LifecycleEvent = "dispatch_claim"
		row.ReasonCode = "dispatch_claimed"
		row.PreviousState, row.CurrentState = "RESERVED", "DISPATCH_PENDING"
		row.Effective, row.Selected = true, true
		applyRoute(&row, op)
		appendRecord(withLedger(row, window), op.Sequence)
		row = baseRecord(op, tie)
		row.LifecycleEvent = "dispatch_delivered"
		row.ReasonCode = "dispatch_delivered"
		row.PreviousState, row.CurrentState = "DISPATCH_PENDING", "DISPATCHED"
		row.Effective, row.Selected = true, true
		applyRoute(&row, op)
		appendRecord(withLedger(row, window), op.Sequence)

		dispatchID := DomainHash("govar-development-dispatch-v1", []byte(config.RunID), []byte(op.RequestID), []byte(op.RequestID+":attempt:1"), []byte(configSHA))
		catalogModelID, err := selectedFeedbackCatalogModelID(config, candidate.ModelRef)
		if err != nil {
			return RunResult{}, fmt.Errorf("selected outcome %s: %w", op.RequestID, err)
		}
		outcomeReq := DispatchOutcomeRequest{DispatchID: dispatchID, RunID: config.RunID, RequestID: op.RequestID, FeedbackItemID: op.FeedbackItemID, SelectedModelID: catalogModelID, SelectedDeployment: candidate.ModelRef, ProviderAttemptID: op.RequestID + ":attempt:1", ConfigSHA256: configSHA}
		if err := issuer.AuthorizeDispatchedOutcome(outcomeReq, EffectiveDispatchProof{LifecycleEventIndex: eventIndex,
			DeliveryEventID:     eventID(config.Seed, streamSHA, op.RequestID, "dispatch-delivered"),
			RouteSnapshotSHA256: candidate.RouteSnapshot.SnapshotHash, State: "DISPATCHED", TransitionEffective: true}); err != nil {
			return RunResult{}, fmt.Errorf("authorize selected outcome %s: %w", op.RequestID, err)
		}
		outcome, err := issuer.IssueDispatchedOutcome(context.Background(), outcomeReq)
		if err != nil {
			return RunResult{}, fmt.Errorf("selected outcome %s: %w", op.RequestID, err)
		}
		if err := issuer.VerifyDispatchedOutcome(outcomeReq, outcome); err != nil {
			return RunResult{}, fmt.Errorf("verify selected outcome %s: %w", op.RequestID, err)
		}
		if err := validateOutcomeBounds(config, op, outcome); err != nil {
			return RunResult{}, err
		}
		actualInput, err := ceilingProduct(config.InputPriceMicrosPerMillion, outcome.ActualInputTokens)
		if err != nil {
			return RunResult{}, err
		}
		actualOutput, err := ceilingProduct(config.OutputPriceMicrosPerMillion, outcome.ActualOutputTokens)
		if err != nil {
			return RunResult{}, err
		}
		actual, err := checkedAdd(actualInput, actualOutput)
		if err != nil {
			return RunResult{}, err
		}
		item := observationalPending{opportunity: op, dueStep: op.Sequence + op.SettlementDelaySteps, tieBreaker: tie, outcome: outcome, actualInputCost: actualInput, actualOutputCost: actualOutput, actualCost: actual, attemptID: op.RequestID + ":attempt:1"}
		for index := len(records) - 3; index < len(records); index++ {
			if index >= 0 && records[index].RequestID == op.RequestID {
				fillOutcome(&records[index], item)
			}
		}
		pending = append(pending, item)
		window.active[op.RequestID] = item
		if err := flush(op.Sequence, false); err != nil {
			return RunResult{}, err
		}
		emitSnapshots(op.Sequence)
	}
	if err := flush(0, true); err != nil {
		return RunResult{}, err
	}
	counts, lifecycle, metrics, err := VerifyRecords(records, config, opportunities, issuer)
	if err != nil {
		return RunResult{}, err
	}
	return RunResult{Config: config, ConfigSHA256: configSHA, CanonicalConfigSHA256: canonical, StreamSHA256: streamSHA, MatchedStreamKey: matchedKey, Records: records, DecisionCounts: counts, LifecycleCounts: lifecycle, Metrics: metrics}, nil
}

func jsonCanonicalConfig(config Config) (string, error) {
	raw, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	return SHA256(raw), nil
}
