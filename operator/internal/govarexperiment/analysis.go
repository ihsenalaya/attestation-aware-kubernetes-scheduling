package govarexperiment

import (
	"errors"
	"fmt"
	"time"
)

type verifiedRequest struct {
	op         Opportunity
	stage      int
	admitted   bool
	dispatched bool
	terminal   bool
	reserved   int64
	actual     int64
	outcome    DispatchedOutcome
}
type verifiedWindow struct {
	budget, settled, outstanding, carried, active, peak int64
	peakLatent, peakActualOutstanding                   int64
	engineWindow                                        string
	activeRequests                                      map[string]*verifiedRequest
}
type cohortFact struct{ actual, reserved int64 }

// VerifyRecords is the authoritative scientific recomputer. Unlike the old
// raw-only counter, it consumes the exact parsed config and opportunity stream,
// verifies post-dispatch receipts, executes the declared reservation arithmetic,
// checks every legal transition and ledger delta, and enforces the fixed
// opportunity-epoch snapshot grid.
func VerifyRecords(records []LifecycleRecord, config Config, opportunities []Opportunity, issuer OutcomeIssuer) (map[string]int64, map[string]int64, Metrics, error) {
	if issuer == nil {
		return nil, nil, Metrics{}, errors.New("outcome receipt verifier is required")
	}
	if err := config.Validate(); err != nil {
		return nil, nil, Metrics{}, err
	}
	opByRequest := map[string]Opportunity{}
	windowBudgets := map[string]int64{}
	windowExamples := map[string]Opportunity{}
	for _, op := range opportunities {
		opByRequest[op.RequestID] = op
		key := op.TenantID + "\x00" + op.BudgetWindowID
		windowBudgets[key] = op.BudgetMicros
		if _, ok := windowExamples[key]; !ok {
			windowExamples[key] = op
		}
	}
	windowKeys := sortedWindowKeys(windowBudgets)
	epochAllowed := map[int64]bool{}
	for _, op := range opportunities {
		epochAllowed[op.Sequence] = true
	}
	windows := map[string]*verifiedWindow{}
	for key, budget := range windowBudgets {
		windows[key] = &verifiedWindow{budget: budget, activeRequests: map[string]*verifiedRequest{}}
	}
	requests := map[string]*verifiedRequest{}
	decisions := map[string]int64{}
	lifecycle := map[string]int64{}
	snapshotSeen := map[string]bool{}
	snapshotStarted := map[int64]bool{}
	snapshotOrder := map[int64]int{}
	var metrics Metrics
	metrics.RawRecordCount = int64(len(records))
	virtualStart, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	configSHA := ""
	streamSHA := ""
	matchedKey := ""
	if len(records) > 0 {
		configSHA = records[0].ConfigSHA256
		streamSHA = records[0].StreamSHA256
		matchedKey = records[0].MatchedStreamKey
	}
	var priorTimestamp time.Time
	for index, row := range records {
		position := index + 1
		if row.SchemaVersion != LifecycleSchema || (row.RecordType != RecordLifecycle && row.RecordType != RecordSnapshot) {
			return nil, nil, Metrics{}, fmt.Errorf("record %d has unknown schema/type", position)
		}
		if row.EventIndex != int64(position) || row.LogicalTimeStep < 0 || row.StreamSequence < 0 {
			return nil, nil, Metrics{}, fmt.Errorf("record %d has invalid event/logical index", position)
		}
		expectedTimestamp := virtualStart.Add(time.Duration(row.LogicalTimeStep)*time.Second + time.Duration(row.EventIndex)*time.Nanosecond).Format(time.RFC3339Nano)
		if row.Timestamp != expectedTimestamp {
			return nil, nil, Metrics{}, fmt.Errorf("record %d timestamp is not the prespecified virtual clock", position)
		}
		parsedTimestamp, _ := time.Parse(time.RFC3339Nano, row.Timestamp)
		if !priorTimestamp.IsZero() && parsedTimestamp.Before(priorTimestamp) {
			return nil, nil, Metrics{}, fmt.Errorf("record %d moves virtual time backwards", position)
		}
		priorTimestamp = parsedTimestamp
		if !shaPattern.MatchString(row.StreamSHA256) || !shaPattern.MatchString(row.ConfigSHA256) || !shaPattern.MatchString(row.MatchedStreamKey) {
			return nil, nil, Metrics{}, fmt.Errorf("record %d has invalid input hash", position)
		}
		if row.ExperimentID != config.ExperimentID || row.RunID != config.RunID || row.CellID != config.CellID || row.Scenario != config.Scenario || row.Seed != config.Seed || row.RegistryID != config.RegistryID || row.ProtocolMethodID != config.ProtocolMethodID || row.ComparatorMethod != config.ComparatorMethod || row.ProductionReservationMode != config.ProductionReservationMode || row.MethodConfigSHA256 != config.MethodConfigSHA256 || row.FeedbackModelMapSHA256 != config.SelectedFeedbackModelMapSHA256 || row.ClusterID != config.ClusterID || row.TrialID != config.TrialID || row.SourceSHA256 != config.SourceSHA256 || row.SoftwareSHA256 != config.SoftwareSHA256 || row.DataSHA256 != config.DataSHA256 || row.ProtocolSHA256 != config.ProtocolSHA256 || row.SplitSHA256 != config.SplitSHA256 {
			return nil, nil, Metrics{}, fmt.Errorf("record %d changes immutable config identity", position)
		}
		if row.ConfigSHA256 != configSHA || row.StreamSHA256 != streamSHA || row.MatchedStreamKey != matchedKey {
			return nil, nil, Metrics{}, fmt.Errorf("record %d changes immutable input identity", position)
		}
		if row.BudgetMicros <= 0 || row.Retried < 0 || row.SettledMicros < 0 || row.OutstandingMicros < 0 || row.ActiveReservations < 0 || row.ActiveActualMicros < 0 || row.ActiveReservedMicros < 0 {
			return nil, nil, Metrics{}, fmt.Errorf("record %d contains invalid accounting value", position)
		}
		if row.Failed || row.Excluded || row.Retried != 0 || row.ExclusionReason != "" {
			return nil, nil, Metrics{}, fmt.Errorf("record %d asserts unsupported failure/retry/exclusion", position)
		}
		if row.RecordType == RecordSnapshot {
			if !epochAllowed[row.StreamSequence] {
				return nil, nil, Metrics{}, fmt.Errorf("record %d samples a non-opportunity epoch", position)
			}
			order := snapshotOrder[row.StreamSequence]
			if order >= len(windowKeys) || row.TenantID+"\x00"+row.BudgetWindowID != windowKeys[order] {
				return nil, nil, Metrics{}, fmt.Errorf("record %d snapshot order is not the prespecified tenant-window order", position)
			}
			snapshotOrder[row.StreamSequence] = order + 1
			if err := verifySnapshotRow(row, windowExamples, windows, snapshotSeen); err != nil {
				return nil, nil, Metrics{}, fmt.Errorf("record %d: %w", position, err)
			}
			snapshotStarted[row.LogicalTimeStep] = true
			lifecycle[row.LifecycleEvent]++
			metrics.ActiveLiabilitySnapshots++
			if row.ActiveActualMicros > row.ActiveReservedMicros {
				metrics.InstantaneousActiveLiabilityExceedance.Numerator++
				var err error
				metrics.InstantaneousExceedanceMagnitudeMicros, err = checkedAdd(metrics.InstantaneousExceedanceMagnitudeMicros, row.ActiveActualMicros-row.ActiveReservedMicros)
				if err != nil {
					return nil, nil, Metrics{}, err
				}
			}
			window := windows[row.TenantID+"\x00"+row.BudgetWindowID]
			latentExposure, err := checkedAdd(row.SettledMicros, row.ActiveActualMicros)
			if err == nil {
				latentExposure, err = checkedAdd(latentExposure, row.CarriedMicros)
			}
			if err != nil {
				return nil, nil, Metrics{}, fmt.Errorf("record %d latent financial exposure: %w", position, err)
			}
			if latentExposure > window.peakLatent {
				window.peakLatent = latentExposure
			}
			if latentExposure > metrics.PeakLatentFinancialExposureMicros {
				metrics.PeakLatentFinancialExposureMicros = latentExposure
			}
			if row.ActiveActualMicros > window.peakActualOutstanding {
				window.peakActualOutstanding = row.ActiveActualMicros
			}
			if row.ActiveActualMicros > metrics.PeakActualOutstandingLiabilityMicros {
				metrics.PeakActualOutstandingLiabilityMicros = row.ActiveActualMicros
			}
			continue
		}
		if snapshotStarted[row.LogicalTimeStep] {
			return nil, nil, Metrics{}, fmt.Errorf("record %d occurs after the fixed snapshot for its epoch", position)
		}
		op, ok := opByRequest[row.RequestID]
		if !ok {
			return nil, nil, Metrics{}, fmt.Errorf("record %d references request absent from opportunity stream", position)
		}
		if err := verifyOpportunityFields(row, op, config); err != nil {
			return nil, nil, Metrics{}, fmt.Errorf("record %d: %w", position, err)
		}
		key := op.TenantID + "\x00" + op.BudgetWindowID
		window := windows[key]
		fact := requests[op.RequestID]
		if fact == nil {
			if row.LifecycleEvent != "admission" {
				return nil, nil, Metrics{}, fmt.Errorf("record %d starts request with %q", position, row.LifecycleEvent)
			}
			fact = &verifiedRequest{op: op}
			requests[op.RequestID] = fact
			metrics.AttemptedObservationCount++
		}
		if row.LogicalTimeStep != expectedEventStep(op, row.LifecycleEvent) {
			return nil, nil, Metrics{}, fmt.Errorf("record %d event is at wrong logical step", position)
		}
		if err := verifyOutcomeFields(row, fact, config, configSHA, issuer); err != nil {
			return nil, nil, Metrics{}, fmt.Errorf("record %d: %w", position, err)
		}
		expectedInput, expectedOutput, expectedReserved, err := expectedReservation(config, op)
		if err != nil {
			return nil, nil, Metrics{}, err
		}
		expectedMode, err := expectedReservationMode(config, op)
		if err != nil {
			return nil, nil, Metrics{}, err
		}
		if row.Selected && (row.InputReservedMicros != expectedInput || row.OutputReservedMicros != expectedOutput || row.ReservedMicros != expectedReserved || row.EffectiveMethod != expectedMode) {
			return nil, nil, Metrics{}, fmt.Errorf("record %d reservation components/mode do not recompute", position)
		}
		available := window.budget - window.settled - window.outstanding - window.carried
		switch row.LifecycleEvent {
		case "admission":
			if fact.stage != 0 {
				return nil, nil, Metrics{}, fmt.Errorf("record %d duplicates admission", position)
			}
			admit := expectedReserved <= available
			expectedAdmitReason := "highest_utility_feasible"
			expectedQueueReason := "budget_unavailable"
			switch config.ComparatorMethod {
			case "no_budget":
				admit = true
				expectedAdmitReason = "hard_feasible_budget_disabled"
			case "settled_only":
				admit = window.budget-window.settled-window.carried > 0
				expectedAdmitReason = "settled_only_positive_available"
			case "gov_ar":
				slot, slotErr := config.MethodConfig.GOVAR.Slot(op.TenantID, op.BudgetWindowID, op.CohortID, op.CohortIndex, config.MethodConfig.GOVAR.CandidateSetSHA256)
				if slotErr != nil {
					return nil, nil, Metrics{}, slotErr
				}
				evidence, evidenceErr := config.MethodConfig.GOVAR.Evidence(slot.EvidencePublicationSHA256)
				if evidenceErr != nil {
					return nil, nil, Metrics{}, evidenceErr
				}
				if fallback := govarEvidenceFallbackReason(config, evidence); fallback != "" {
					expectedAdmitReason = fallback
				}
			}
			if admit {
				if row.Decision != "ADMIT" || row.ReasonCode != expectedAdmitReason || !row.Effective || !row.Selected || row.PreviousState != "" || row.CurrentState != "RESERVED" || row.ReservedMicros != expectedReserved || row.InputReservedMicros != expectedInput || row.OutputReservedMicros != expectedOutput {
					return nil, nil, Metrics{}, fmt.Errorf("record %d violates recomputed ADMIT semantics", position)
				}
				fact.admitted = true
				fact.reserved = expectedReserved
				if config.ComparatorMethod != "no_budget" && config.ComparatorMethod != "settled_only" {
					window.outstanding, err = checkedAdd(window.outstanding, expectedReserved)
					if err != nil {
						return nil, nil, Metrics{}, err
					}
					window.active++
				}
				window.activeRequests[op.RequestID] = fact
			} else if row.Decision != "QUEUE" || row.ReasonCode != expectedQueueReason || row.Effective || row.Selected || row.PreviousState != "" || row.CurrentState != "" || row.ReservedMicros != 0 || row.InputReservedMicros != 0 || row.OutputReservedMicros != 0 {
				return nil, nil, Metrics{}, fmt.Errorf("record %d violates recomputed budget rejection semantics", position)
			}
			fact.stage = 1
			decisions[row.Decision]++
		case "dispatch_claim":
			if !fact.admitted || fact.stage != 1 || row.Decision != "" || row.ReasonCode != "dispatch_claimed" || row.PreviousState != "RESERVED" || row.CurrentState != "DISPATCH_PENDING" || !row.Effective || !row.Selected {
				return nil, nil, Metrics{}, fmt.Errorf("record %d violates dispatch-claim transition", position)
			}
			fact.stage = 2
		case "dispatch_delivered":
			if !fact.admitted || fact.stage != 2 || row.Decision != "" || row.ReasonCode != "dispatch_delivered" || row.PreviousState != "DISPATCH_PENDING" || row.CurrentState != "DISPATCHED" || !row.Effective || !row.Selected {
				return nil, nil, Metrics{}, fmt.Errorf("record %d violates dispatch-delivered transition", position)
			}
			fact.stage = 3
			fact.dispatched = true
		case "settlement_final":
			if !fact.dispatched || fact.stage != 3 || row.Decision != "" || !row.Effective || !row.Selected || row.PreviousState != "DISPATCHED" || row.CurrentState != "FINALIZED" || row.ReasonCode != settlementReason(row) {
				return nil, nil, Metrics{}, fmt.Errorf("record %d violates final-settlement transition", position)
			}
			if config.ComparatorMethod != "no_budget" && config.ComparatorMethod != "settled_only" {
				window.outstanding -= fact.reserved
				window.active--
			}
			window.settled, err = checkedAdd(window.settled, fact.actual)
			if err != nil {
				return nil, nil, Metrics{}, err
			}
			delete(window.activeRequests, op.RequestID)
			fact.stage = 4
			fact.terminal = true
			metrics.AnalyzedObservationCount++
		case "settlement_duplicate":
			if !op.DuplicateSettlement || fact.stage != 4 || row.Decision != "" || row.ReasonCode != "duplicate_settlement" || row.PreviousState != "FINALIZED" || row.CurrentState != "FINALIZED" || row.Effective || !row.Selected {
				return nil, nil, Metrics{}, fmt.Errorf("record %d violates duplicate-settlement replay", position)
			}
			fact.stage = 5
		case "settlement_conflict_rejected":
			expectedPrior := 4
			if op.DuplicateSettlement {
				expectedPrior = 5
			}
			if !op.ConflictingSettlementReplay || fact.stage != expectedPrior || row.Decision != "" || row.ReasonCode != "duplicate_event" || row.PreviousState != "FINALIZED" || row.CurrentState != "FINALIZED" || row.Effective || !row.Selected {
				return nil, nil, Metrics{}, fmt.Errorf("record %d violates conflicting-settlement rejection", position)
			}
			fact.stage = 6
		default:
			return nil, nil, Metrics{}, fmt.Errorf("record %d has invented lifecycle event %q", position, row.LifecycleEvent)
		}
		if err := verifyLedgerRow(row, window); err != nil {
			return nil, nil, Metrics{}, fmt.Errorf("record %d: %w", position, err)
		}
		exposure, err := checkedAdd(window.settled, window.outstanding)
		if err == nil {
			exposure, err = checkedAdd(exposure, window.carried)
		}
		if err != nil {
			return nil, nil, Metrics{}, err
		}
		if exposure > window.peak {
			window.peak = exposure
		}
		if exposure > metrics.PeakExposureMicros {
			metrics.PeakExposureMicros = exposure
		}
		// Lifecycle rows do not expose future outcomes before dispatch.  Once
		// actual cost settles, however, final settled spend is an exact lower
		// bound on latent financial exposure and ensures that a last settlement
		// after the final opportunity snapshot remains represented.
		latentSettled, err := checkedAdd(window.settled, window.carried)
		if err != nil {
			return nil, nil, Metrics{}, err
		}
		if latentSettled > window.peakLatent {
			window.peakLatent = latentSettled
		}
		if latentSettled > metrics.PeakLatentFinancialExposureMicros {
			metrics.PeakLatentFinancialExposureMicros = latentSettled
		}
		lifecycle[row.LifecycleEvent]++
	}
	for _, op := range opportunities {
		fact := requests[op.RequestID]
		if fact == nil {
			return nil, nil, Metrics{}, fmt.Errorf("request %s has no admission", op.RequestID)
		}
		if fact.admitted && !fact.terminal {
			return nil, nil, Metrics{}, fmt.Errorf("request %s has no effective terminal settlement", op.RequestID)
		}
		if !fact.admitted && fact.stage != 1 {
			return nil, nil, Metrics{}, fmt.Errorf("rejected request %s has extra lifecycle", op.RequestID)
		}
		expectedStage := 4
		if op.DuplicateSettlement {
			expectedStage = 5
		}
		if op.ConflictingSettlementReplay {
			expectedStage = 6
		}
		if fact.admitted && fact.stage != expectedStage {
			return nil, nil, Metrics{}, fmt.Errorf("request %s is missing declared replay events", op.RequestID)
		}
	}
	for _, epoch := range opportunities {
		for _, key := range windowKeys {
			snapshotKey := fmt.Sprintf("%d\x00%s", epoch.Sequence, key)
			if !snapshotSeen[snapshotKey] {
				return nil, nil, Metrics{}, fmt.Errorf("missing opportunity-epoch snapshot %q", snapshotKey)
			}
		}
	}
	metrics.UniqueRequestCount = int64(len(requests))
	cohorts := map[string]*cohortFact{}
	for _, fact := range requests {
		if fact.admitted {
			metrics.AdmittedRequestCount++
		}
		if fact.dispatched {
			metrics.DispatchedRequestCount++
			metrics.RequestUnderReservation.Denominator++
			if fact.actual > fact.reserved {
				metrics.RequestUnderReservation.Numerator++
				var err error
				metrics.RequestUnderReservationMagnitudeMicros, err = checkedAdd(metrics.RequestUnderReservationMagnitudeMicros, fact.actual-fact.reserved)
				if err != nil {
					return nil, nil, Metrics{}, err
				}
			}
		}
		if fact.op.CohortID != "" {
			key := cohortKey(fact.op.TenantID, fact.op.BudgetWindowID, fact.op.CohortID)
			c := cohorts[key]
			if c == nil {
				c = &cohortFact{}
				cohorts[key] = c
			}
			if fact.dispatched {
				var err error
				c.actual, err = checkedAdd(c.actual, fact.actual)
				if err == nil {
					c.reserved, err = checkedAdd(c.reserved, fact.reserved)
				}
				if err != nil {
					return nil, nil, Metrics{}, err
				}
			}
		}
	}
	metrics.FixedCohortCount = int64(len(cohorts))
	metrics.FixedCohortLiabilityExceedance.Denominator = metrics.FixedCohortCount
	for _, c := range cohorts {
		if c.actual > c.reserved {
			metrics.FixedCohortLiabilityExceedance.Numerator++
			var err error
			metrics.FixedCohortExceedanceMagnitudeMicros, err = checkedAdd(metrics.FixedCohortExceedanceMagnitudeMicros, c.actual-c.reserved)
			if err != nil {
				return nil, nil, Metrics{}, err
			}
		}
	}
	metrics.InstantaneousActiveLiabilityExceedance.Denominator = metrics.ActiveLiabilitySnapshots
	metrics.TenantWindowCount = int64(len(windows))
	metrics.TenantBudgetWindowOvershoot.Denominator = metrics.TenantWindowCount
	for _, key := range windowKeys {
		w := windows[key]
		var err error
		metrics.TotalWindowPeakExposureMicros, err = checkedAdd(metrics.TotalWindowPeakExposureMicros, w.peak)
		if err == nil {
			metrics.TotalWindowPeakLatentExposureMicros, err = checkedAdd(metrics.TotalWindowPeakLatentExposureMicros, w.peakLatent)
		}
		if err == nil {
			metrics.TotalWindowPeakActualOutstandingMicros, err = checkedAdd(metrics.TotalWindowPeakActualOutstandingMicros, w.peakActualOutstanding)
		}
		if err == nil {
			metrics.TotalWindowFinalSettledMicros, err = checkedAdd(metrics.TotalWindowFinalSettledMicros, w.settled)
		}
		if err == nil {
			metrics.TotalWindowBudgetMicros, err = checkedAdd(metrics.TotalWindowBudgetMicros, w.budget)
		}
		if err != nil {
			return nil, nil, Metrics{}, err
		}
		if w.peak > w.budget {
			metrics.TenantBudgetWindowOvershoot.Numerator++
			metrics.TenantWindowOvershootMagnitudeMicros, err = checkedAdd(metrics.TenantWindowOvershootMagnitudeMicros, w.peak-w.budget)
			if err != nil {
				return nil, nil, Metrics{}, err
			}
		}
	}
	for _, rate := range []*ExactRate{&metrics.RequestUnderReservation, &metrics.FixedCohortLiabilityExceedance, &metrics.InstantaneousActiveLiabilityExceedance, &metrics.TenantBudgetWindowOvershoot} {
		value, err := ratePPB(rate.Numerator, rate.Denominator)
		if err != nil {
			return nil, nil, Metrics{}, err
		}
		rate.RatePPB = value
	}
	var err error
	metrics.BudgetUtilizationPPB, err = utilizationPPB(metrics.TotalWindowFinalSettledMicros, metrics.TotalWindowBudgetMicros)
	if err != nil {
		return nil, nil, Metrics{}, err
	}
	metrics.PeakExposureRatioPPB, err = utilizationPPB(metrics.TotalWindowPeakExposureMicros, metrics.TotalWindowBudgetMicros)
	if err != nil {
		return nil, nil, Metrics{}, err
	}
	metrics.PeakLatentExposureRatioPPB, err = utilizationPPB(metrics.TotalWindowPeakLatentExposureMicros, metrics.TotalWindowBudgetMicros)
	if err != nil {
		return nil, nil, Metrics{}, err
	}
	return decisions, lifecycle, metrics, nil
}

func expectedEventStep(op Opportunity, event string) int64 {
	switch event {
	case "admission", "dispatch_claim", "dispatch_delivered":
		return op.Sequence
	default:
		return op.Sequence + op.SettlementDelaySteps
	}
}
func settlementReason(row LifecycleRecord) string {
	if row.ActualCostMicros > row.ReservedMicros || row.ActualInputCostMicros > row.InputReservedMicros || row.ActualOutputCostMicros > row.OutputReservedMicros {
		return "reservation_exceeded"
	}
	return "settlement_finalized"
}

func verifyOpportunityFields(row LifecycleRecord, op Opportunity, config Config) error {
	if row.RequestID != op.RequestID || row.TenantID != op.TenantID || row.BudgetWindowID != op.BudgetWindowID || row.WorkloadUID != op.WorkloadUID || row.BudgetMicros != op.BudgetMicros || row.CohortID != op.CohortID || row.CohortIndex != op.CohortIndex || row.InputTokens != op.InputTokens || row.MaxOutputTokens != op.MaxOutputTokens || row.FaultMode != op.FaultMode || row.FeedbackItemID != op.FeedbackItemID || row.StreamSequence != op.Sequence {
		return errors.New("raw request facts do not match exact opportunity")
	}
	if row.ScheduleTieBreaker != RandomUint64("settlement-order", config.Seed, []byte(row.StreamSHA256), []byte(op.RequestID)) {
		return errors.New("schedule tie breaker does not recompute")
	}
	if row.InputPriceMicrosPerMillion != config.InputPriceMicrosPerMillion || row.OutputPriceMicrosPerMillion != config.OutputPriceMicrosPerMillion || row.VerifiedOutputCapTokens != config.VerifiedOutputCapTokens {
		return errors.New("raw pricing/cap facts do not match config")
	}
	if config.ComparatorMethod == "gov_ar" {
		slot, err := config.MethodConfig.GOVAR.Slot(op.TenantID, op.BudgetWindowID, op.CohortID, op.CohortIndex, config.MethodConfig.GOVAR.CandidateSetSHA256)
		if err != nil {
			return err
		}
		evidence, err := config.MethodConfig.GOVAR.Evidence(slot.EvidencePublicationSHA256)
		if err != nil {
			return err
		}
		expectedRisk := slot.AllocatedRiskPPB
		expectedCalibration := evidence.Calibration.ArtifactSHA256
		expectedState, expectedFallback := "calibrated", ""
		if govarEvidenceFallsBack(config, evidence) {
			expectedRisk, expectedCalibration = 0, ""
			expectedState, expectedFallback = "conservative_fallback", govarEvidenceFallbackReason(config, evidence)
		}
		if row.AllocatedRiskPPB != expectedRisk || row.CalibrationArtifactSHA256 != expectedCalibration ||
			!shaPattern.MatchString(row.CohortRegistrySHA256) || row.CandidateSetSHA256 != config.MethodConfig.GOVAR.CandidateSetSHA256 ||
			row.JointSelectionSHA256 != config.MethodConfig.GOVAR.JointSelection.ArtifactSHA256 || row.TrustedEvaluationTime != config.VirtualStart ||
			row.CalibrationState != expectedState || row.ConservativeFallbackReason != expectedFallback {
			return fmt.Errorf("raw GOV-AR risk/calibration/cohort/candidate/time binding does not recompute: risk=%d/%d calibration=%q/%q cohort=%q candidate=%q joint=%q time=%q state=%q/%q fallback=%q/%q",
				row.AllocatedRiskPPB, expectedRisk, row.CalibrationArtifactSHA256, expectedCalibration, row.CohortRegistrySHA256,
				row.CandidateSetSHA256, row.JointSelectionSHA256, row.TrustedEvaluationTime, row.CalibrationState, expectedState,
				row.ConservativeFallbackReason, expectedFallback)
		}
	} else if row.AllocatedRiskPPB != 0 || row.CalibrationArtifactSHA256 != "" || row.CohortRegistrySHA256 != "" ||
		row.CandidateSetSHA256 != "" || row.JointSelectionSHA256 != "" || row.TrustedEvaluationTime != "" ||
		row.CalibrationState != "" || row.ConservativeFallbackReason != "" {
		return errors.New("non-GOV-AR row asserts GOV-AR evidence")
	}
	return nil
}

func verifyOutcomeFields(row LifecycleRecord, fact *verifiedRequest, config Config, configSHA string, issuer OutcomeIssuer) error {
	if !row.Selected {
		if row.ActualInputTokens != 0 || row.ActualOutputTokens != 0 || row.ActualInputCostMicros != 0 || row.ActualOutputCostMicros != 0 || row.ActualCostMicros != 0 || row.Analyzed || row.DispatchID != "" || row.FeedbackObservationID != "" || row.FeedbackRequestSHA256 != "" || row.FeedbackItemSHA256 != "" || row.FeedbackSelectedModelSHA256 != "" || row.UsageReceiptSHA256 != "" || row.FeedbackAuthoritySHA256 != "" || row.FeedbackArtifactSHA256 != "" {
			return errors.New("non-selected record leaks or asserts an outcome")
		}
		return nil
	}
	outcome := DispatchedOutcome{ActualInputTokens: row.ActualInputTokens, ActualOutputTokens: row.ActualOutputTokens, UsageReceiptSHA256: row.UsageReceiptSHA256, Feedback: SelectedFeedbackReceipt{DispatchID: row.DispatchID, RequestID: row.FeedbackRequestSHA256, ItemID: row.FeedbackItemSHA256, SelectedModelID: row.FeedbackSelectedModelSHA256, SelectedScore: row.SelectedScore, Replayed: row.FeedbackReplayed, ObservationID: row.FeedbackObservationID}, Evidence: FeedbackEvidence{RunID: config.RunID, DatasetID: config.SelectedFeedbackDatasetID, Split: config.SelectedFeedbackSplit, ProtocolSHA256: config.ProtocolSHA256, FeedbackArtifactSHA256: row.FeedbackArtifactSHA256, SoftwareSHA256: config.SoftwareSHA256, ConfigSHA256: configSHA, AuthoritySHA256: row.FeedbackAuthoritySHA256, ModelMapSHA256: row.FeedbackModelMapSHA256}}
	catalogModelID, err := selectedFeedbackCatalogModelID(config, row.SelectedDeployment)
	if err != nil {
		return err
	}
	req := DispatchOutcomeRequest{DispatchID: row.DispatchID, RunID: config.RunID, RequestID: row.RequestID, FeedbackItemID: row.FeedbackItemID, SelectedModelID: catalogModelID, SelectedDeployment: row.SelectedDeployment, ProviderAttemptID: row.ProviderAttemptID, ConfigSHA256: configSHA}
	if err := issuer.VerifyDispatchedOutcome(req, outcome); err != nil {
		return err
	}
	if err := validateOutcomeBounds(config, fact.op, outcome); err != nil {
		return err
	}
	actualInput, err := ceilingProduct(config.InputPriceMicrosPerMillion, row.ActualInputTokens)
	if err != nil {
		return err
	}
	actualOutput, err := ceilingProduct(config.OutputPriceMicrosPerMillion, row.ActualOutputTokens)
	if err != nil {
		return err
	}
	actual, err := checkedAdd(actualInput, actualOutput)
	if err != nil {
		return err
	}
	if row.ActualInputCostMicros != actualInput || row.ActualOutputCostMicros != actualOutput || row.ActualCostMicros != actual || !row.Analyzed {
		return errors.New("actual usage component arithmetic does not recompute")
	}
	if fact.actual == 0 && fact.outcome.UsageReceiptSHA256 == "" {
		fact.actual = actual
		fact.outcome = outcome
	} else if fact.actual != actual || fact.outcome.UsageReceiptSHA256 != outcome.UsageReceiptSHA256 || fact.outcome.Feedback.ObservationID != outcome.Feedback.ObservationID {
		return errors.New("request outcome changes across lifecycle rows")
	}
	if row.PricingVersion != "experiment-pricing-v1" || row.PricingSnapshotSHA256 == "" || row.CapEvidenceSHA256 == "" || row.PolicyVersion == "" || row.SelectedDeployment != "experiment-model" || row.SelectedModel != "experiment-model" || row.SelectedProvider != "experiment-provider" || row.ProviderAttemptID != "" && row.ProviderAttemptID != row.RequestID+":attempt:1" {
		return errors.New("selected route/pricing/policy evidence is incomplete or inconsistent")
	}
	if row.PricingSnapshotSHA256 != experimentCandidate(config).PricingSnapshot.SnapshotSHA256 || row.CapEvidenceSHA256 != experimentCandidate(config).CapEvidenceDigest {
		return errors.New("pricing/cap evidence digest does not recompute")
	}
	return nil
}

func verifyLedgerRow(row LifecycleRecord, w *verifiedWindow) error {
	if row.SettledMicros != w.settled || row.OutstandingMicros != w.outstanding || row.CarriedMicros != w.carried || row.ActiveReservations != w.active {
		return errors.New("ledger snapshot does not equal recomputed transition deltas")
	}
	available := w.budget - w.settled - w.outstanding - w.carried
	if row.AvailableMicros != available {
		return errors.New("available budget does not recompute")
	}
	if row.EngineWindowID == "" {
		return errors.New("production engine window id is missing")
	}
	if w.engineWindow == "" {
		w.engineWindow = row.EngineWindowID
	} else if w.engineWindow != row.EngineWindowID {
		return errors.New("production engine window changed in non-rollover trace core")
	}
	return nil
}

func verifySnapshotRow(row LifecycleRecord, examples map[string]Opportunity, windows map[string]*verifiedWindow, seen map[string]bool) error {
	if row.RequestID != "" || row.WorkloadUID != "" || row.CohortID != "" || row.Attempted || row.Selected || row.Effective || row.Analyzed || row.Decision != "" || row.PreviousState != "" || row.CurrentState != "" || row.LifecycleEvent != "opportunity_epoch_snapshot" || row.ReasonCode != "prespecified_opportunity_epoch" || row.FaultMode != FaultNominal || row.ScheduleTieBreaker != 0 {
		return errors.New("snapshot carries lifecycle/request fields")
	}
	key := row.TenantID + "\x00" + row.BudgetWindowID
	w, ok := windows[key]
	if !ok {
		return errors.New("snapshot references undeclared tenant-window")
	}
	example := examples[key]
	if row.BudgetMicros != example.BudgetMicros {
		return errors.New("snapshot budget mismatch")
	}
	snapshotKey := fmt.Sprintf("%d\x00%s", row.StreamSequence, key)
	if seen[snapshotKey] {
		return errors.New("duplicate opportunity-epoch tenant-window snapshot")
	}
	seen[snapshotKey] = true
	var actual, reserved int64
	for _, fact := range w.activeRequests {
		var err error
		actual, err = checkedAdd(actual, fact.actual)
		if err == nil {
			reserved, err = checkedAdd(reserved, fact.reserved)
		}
		if err != nil {
			return err
		}
	}
	if row.ActiveActualMicros != actual || row.ActiveReservedMicros != reserved {
		return errors.New("snapshot active liability does not recompute")
	}
	if row.SettledMicros != w.settled || row.OutstandingMicros != w.outstanding || row.CarriedMicros != w.carried || row.ActiveReservations != w.active || row.AvailableMicros != w.budget-w.settled-w.outstanding-w.carried {
		return errors.New("snapshot ledger does not recompute")
	}
	if w.engineWindow != "" && row.EngineWindowID != w.engineWindow {
		return errors.New("snapshot engine window mismatch")
	}
	if w.engineWindow == "" && row.EngineWindowID != "" {
		w.engineWindow = row.EngineWindowID
	}
	return nil
}

func equalCounts(left, right map[string]int64) bool {
	if len(left) != len(right) {
		return false
	}
	for k, v := range left {
		if right[k] != v {
			return false
		}
	}
	return true
}
