package govarexperiment

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RunResult struct {
	Config                Config
	ConfigSHA256          string
	CanonicalConfigSHA256 string
	StreamSHA256          string
	MatchedStreamKey      string
	Records               []LifecycleRecord
	DecisionCounts        map[string]int64
	LifecycleCounts       map[string]int64
	Metrics               Metrics
}

type tenantContext struct {
	engine     *govar.Engine
	tenant     string
	window     string
	budget     aiopsv1alpha1.AIBudgetPolicy
	routing    aiopsv1alpha1.AIRoutingPolicy
	candidates []govar.Candidate
}

type pendingSettlement struct {
	opportunity      Opportunity
	ctx              *tenantContext
	dueStep          int64
	tieBreaker       uint64
	outcome          DispatchedOutcome
	actualInputCost  int64
	actualOutputCost int64
	actualCost       int64
	inputReserved    int64
	outputReserved   int64
	reserved         int64
	effectiveMethod  string
	attemptID        string
}

type rawGOVARBinding struct {
	slot           GOVARSlotBound
	evidence       GOVARCalibrationEvidence
	cohortRegistry string
}

func ParseConfig(raw []byte) (Config, string, error) {
	var config Config
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, "", fmt.Errorf("decode config: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Config{}, "", fmt.Errorf("decode config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return Config{}, "", err
	}
	canonical, err := json.Marshal(config)
	if err != nil {
		return Config{}, "", err
	}
	return config, SHA256(canonical), nil
}

func ParseStream(raw []byte, config Config) ([]Opportunity, error) {
	if config.SchemaVersion != ConfigSchema {
		return nil, fmt.Errorf("ParseStream supports only %s; use ParseStreamV2 for %s", ConfigSchema, ConfigSchemaV2)
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var opportunities []Opportunity
	requestIDs := map[string]struct{}{}
	windowBudgets := map[string]int64{}
	tenantWindows := map[string]string{}
	cohortSlots := map[string]map[int64]string{}
	lastSequence := int64(-1)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			return nil, fmt.Errorf("stream line %d is empty", lineNumber)
		}
		var opportunity Opportunity
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&opportunity); err != nil {
			return nil, fmt.Errorf("stream line %d: %w", lineNumber, err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			return nil, fmt.Errorf("stream line %d: %w", lineNumber, err)
		}
		if err := opportunity.Validate(); err != nil {
			return nil, fmt.Errorf("stream line %d: %w", lineNumber, err)
		}
		if opportunity.Sequence <= lastSequence || opportunity.Sequence > 1_000_000_000 || opportunity.SettlementDelaySteps > 1_000_000_000-opportunity.Sequence {
			return nil, fmt.Errorf("stream line %d has non-increasing or unsupported sequence/delay", lineNumber)
		}
		if opportunity.MaxOutputTokens > config.VerifiedOutputCapTokens {
			return nil, fmt.Errorf("stream line %d exceeds configured verified output cap", lineNumber)
		}
		if _, duplicate := requestIDs[opportunity.RequestID]; duplicate {
			return nil, fmt.Errorf("stream line %d duplicates request_id %q", lineNumber, opportunity.RequestID)
		}
		requestIDs[opportunity.RequestID] = struct{}{}
		windowKey := opportunity.TenantID + "\x00" + opportunity.BudgetWindowID
		if prior, ok := windowBudgets[windowKey]; ok && prior != opportunity.BudgetMicros {
			return nil, fmt.Errorf("stream line %d changes immutable tenant-window budget", lineNumber)
		}
		windowBudgets[windowKey] = opportunity.BudgetMicros
		if prior, ok := tenantWindows[opportunity.TenantID]; ok && prior != opportunity.BudgetWindowID {
			return nil, fmt.Errorf("stream line %d requests multiple windows for one tenant; trace core does not simulate rollover", lineNumber)
		}
		tenantWindows[opportunity.TenantID] = opportunity.BudgetWindowID
		if opportunity.CohortID != "" {
			key := cohortKey(opportunity.TenantID, opportunity.BudgetWindowID, opportunity.CohortID)
			slots := cohortSlots[key]
			if slots == nil {
				slots = map[int64]string{}
				cohortSlots[key] = slots
			}
			if prior, duplicate := slots[opportunity.CohortIndex]; duplicate {
				return nil, fmt.Errorf("stream line %d reuses cohort slot from request %q", lineNumber, prior)
			}
			slots[opportunity.CohortIndex] = opportunity.RequestID
		}
		lastSequence = opportunity.Sequence
		opportunities = append(opportunities, opportunity)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read stream: %w", err)
	}
	registry := map[string]int64{}
	for _, cohort := range config.Cohorts {
		registry[cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)] = cohort.Size
	}
	for key, slots := range cohortSlots {
		size, declared := registry[key]
		if !declared {
			return nil, fmt.Errorf("stream cohort %q is absent from config registry", key)
		}
		if int64(len(slots)) != size {
			return nil, fmt.Errorf("stream cohort %q is incomplete: got %d want %d", key, len(slots), size)
		}
		for index := int64(0); index < size; index++ {
			if _, ok := slots[index]; !ok {
				return nil, fmt.Errorf("stream cohort %q is missing slot %d", key, index)
			}
		}
	}
	for key := range registry {
		if _, ok := cohortSlots[key]; !ok {
			return nil, fmt.Errorf("config cohort %q has no stream rows", key)
		}
	}
	return opportunities, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func RunWithIssuer(configRaw, streamRaw []byte, issuer OutcomeIssuer) (RunResult, error) {
	if issuer == nil {
		return RunResult{}, errors.New("post-dispatch outcome issuer is required")
	}
	config, canonicalConfigSHA, err := ParseConfig(configRaw)
	if err != nil {
		return RunResult{}, err
	}
	if config.EvidenceTier != issuer.EvidenceTier() {
		return RunResult{}, errors.New("config evidence tier does not match outcome issuer authority")
	}
	bindingEvidence := issuer.BindingEvidence()
	if err := bindingEvidence.Validate(); err != nil {
		return RunResult{}, err
	}
	if bindingEvidence.RunID != config.RunID || bindingEvidence.DatasetID != config.SelectedFeedbackDatasetID || bindingEvidence.Split != config.SelectedFeedbackSplit || bindingEvidence.ProtocolSHA256 != config.ProtocolSHA256 || bindingEvidence.SoftwareSHA256 != config.SoftwareSHA256 || bindingEvidence.ConfigSHA256 != SHA256(configRaw) || bindingEvidence.ModelMapSHA256 != config.SelectedFeedbackModelMapSHA256 {
		return RunResult{}, errors.New("selected-feedback RunBinding does not match exact config")
	}
	if config.ComparatorMethod == "no_budget" || config.ComparatorMethod == "settled_only" {
		return runNonReserving(config, configRaw, streamRaw, issuer)
	}
	switch config.ComparatorMethod {
	case "adaptive_quantile":
		return RunResult{}, errors.New("adaptive_quantile trace execution requires a PostgreSQL-v8 candidate-regime calibration publisher and is not aliased to a static estimate")
	case "oracle_future_cost":
		return RunResult{}, errors.New("oracle_future_cost must run in the isolated post-nonoracle evaluator and is not exposed to the online trace runner")
	}
	opportunities, err := ParseStream(streamRaw, config)
	if err != nil {
		return RunResult{}, err
	}
	configSHA, streamSHA := SHA256(configRaw), SHA256(streamRaw)
	matchedKey := DomainHash("govar-matched-stream-v1", []byte(streamSHA), []byte(strconv.FormatUint(config.Seed, 10)))
	virtualStart, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	engine := govar.NewEngine()
	if err := engine.ConfigureTrustedTime(virtualStart); err != nil {
		return RunResult{}, err
	}
	if err := engine.ConfigureCohortRuntime(config.SoftwareSHA256); err != nil {
		return RunResult{}, err
	}
	if err := prepareGOVAREngine(engine, config, opportunities); err != nil {
		return RunResult{}, err
	}
	rawGOVAR := map[string]rawGOVARBinding{}
	if config.ComparatorMethod == "gov_ar" {
		for _, op := range opportunities {
			slot, err := config.MethodConfig.GOVAR.Slot(op.TenantID, op.BudgetWindowID, op.CohortID, op.CohortIndex, config.MethodConfig.GOVAR.CandidateSetSHA256)
			if err != nil {
				return RunResult{}, err
			}
			evidence, err := config.MethodConfig.GOVAR.Evidence(slot.EvidencePublicationSHA256)
			if err != nil {
				return RunResult{}, err
			}
			cohort, err := engine.ExportFrozenCohort(context.Background(), op.TenantID, op.CohortID)
			if err != nil {
				return RunResult{}, err
			}
			rawGOVAR[op.RequestID] = rawGOVARBinding{slot: slot, evidence: evidence, cohortRegistry: cohort.RegistryDigest}
		}
	}
	contexts := map[string]*tenantContext{}
	windowBudgets := map[string]int64{}
	windowExamples := map[string]Opportunity{}
	for _, op := range opportunities {
		key := op.TenantID + "\x00" + op.BudgetWindowID
		windowBudgets[key] = op.BudgetMicros
		if _, ok := windowExamples[key]; !ok {
			windowExamples[key] = op
		}
	}
	windowKeys := sortedWindowKeys(windowBudgets)
	active := map[string]map[string]pendingSettlement{}
	var pending []pendingSettlement
	var records []LifecycleRecord
	eventIndex := int64(0)

	appendRecord := func(record LifecycleRecord, logicalStep int64) {
		eventIndex++
		record.EventIndex = eventIndex
		record.LogicalTimeStep = logicalStep
		record.Timestamp = virtualStart.Add(time.Duration(logicalStep)*time.Second + time.Duration(eventIndex)*time.Nanosecond).Format(time.RFC3339Nano)
		records = append(records, record)
	}
	baseRecord := func(op Opportunity, tie uint64) LifecycleRecord {
		record := LifecycleRecord{SchemaVersion: LifecycleSchema, RecordType: RecordLifecycle, ExperimentID: config.ExperimentID, RunID: config.RunID,
			CellID: config.CellID, Scenario: config.Scenario, Seed: config.Seed, StreamSHA256: streamSHA, ConfigSHA256: configSHA,
			MatchedStreamKey: matchedKey, RegistryID: config.RegistryID, ProtocolMethodID: config.ProtocolMethodID, ComparatorMethod: config.ComparatorMethod, ProductionReservationMode: config.ProductionReservationMode, MethodConfigSHA256: config.MethodConfigSHA256,
			ClusterID: config.ClusterID, TrialID: config.TrialID, SourceSHA256: config.SourceSHA256, SoftwareSHA256: config.SoftwareSHA256,
			DataSHA256: config.DataSHA256, ProtocolSHA256: config.ProtocolSHA256, SplitSHA256: config.SplitSHA256,
			FeedbackModelMapSHA256: config.SelectedFeedbackModelMapSHA256,
			StreamSequence:         op.Sequence, ScheduleTieBreaker: tie, RequestID: op.RequestID, TenantID: op.TenantID,
			BudgetWindowID: op.BudgetWindowID, WorkloadUID: op.WorkloadUID, CohortID: op.CohortID, CohortIndex: op.CohortIndex,
			FaultMode: op.FaultMode, InputTokens: op.InputTokens, MaxOutputTokens: op.MaxOutputTokens,
			InputPriceMicrosPerMillion: config.InputPriceMicrosPerMillion, OutputPriceMicrosPerMillion: config.OutputPriceMicrosPerMillion,
			VerifiedOutputCapTokens: config.VerifiedOutputCapTokens, FeedbackItemID: op.FeedbackItemID, Attempted: true, BudgetMicros: op.BudgetMicros}
		if config.ComparatorMethod == "gov_ar" {
			binding := rawGOVAR[op.RequestID]
			slot, evidence := binding.slot, binding.evidence
			if !govarEvidenceFallsBack(config, evidence) {
				record.AllocatedRiskPPB = slot.AllocatedRiskPPB
				record.CalibrationArtifactSHA256 = evidence.Calibration.ArtifactSHA256
				record.CalibrationState = "calibrated"
			} else {
				record.CalibrationState = "conservative_fallback"
				record.ConservativeFallbackReason = govarEvidenceFallbackReason(config, evidence)
			}
			record.CohortRegistrySHA256 = binding.cohortRegistry
			record.CandidateSetSHA256 = config.MethodConfig.GOVAR.CandidateSetSHA256
			record.JointSelectionSHA256 = config.MethodConfig.GOVAR.JointSelection.ArtifactSHA256
			record.TrustedEvaluationTime = config.VirtualStart
		}
		return record
	}
	withLiability := func(record LifecycleRecord, ctx *tenantContext) LifecycleRecord {
		liability := ctx.engine.Liability(ctx.tenant)
		record.SettledMicros = int64(liability.SettledSpendMicros)
		record.OutstandingMicros = int64(liability.OutstandingLiabilityMicros)
		record.CarriedMicros = int64(liability.CarriedAdjustmentMicros)
		record.AvailableMicros = int64(liability.AvailableBudgetMicros)
		record.ActiveReservations = int64(liability.ActiveReservations)
		record.EngineWindowID = liability.CurrentWindowID
		return record
	}
	fillOutcome := func(record *LifecycleRecord, item pendingSettlement) {
		record.ActualInputTokens = item.outcome.ActualInputTokens
		record.ActualOutputTokens = item.outcome.ActualOutputTokens
		record.ActualInputCostMicros = item.actualInputCost
		record.ActualOutputCostMicros = item.actualOutputCost
		record.ActualCostMicros = item.actualCost
		record.InputReservedMicros = item.inputReserved
		record.OutputReservedMicros = item.outputReserved
		record.ReservedMicros = item.reserved
		record.EffectiveMethod = item.effectiveMethod
		record.DispatchID = item.outcome.Feedback.DispatchID
		record.FeedbackObservationID = item.outcome.Feedback.ObservationID
		record.FeedbackRequestSHA256 = item.outcome.Feedback.RequestID
		record.FeedbackItemSHA256 = item.outcome.Feedback.ItemID
		record.FeedbackSelectedModelSHA256 = item.outcome.Feedback.SelectedModelID
		record.SelectedScore = item.outcome.Feedback.SelectedScore
		record.FeedbackReplayed = item.outcome.Feedback.Replayed
		record.UsageReceiptSHA256 = item.outcome.UsageReceiptSHA256
		record.FeedbackAuthoritySHA256 = item.outcome.Evidence.AuthoritySHA256
		record.FeedbackModelMapSHA256 = item.outcome.Evidence.ModelMapSHA256
		record.FeedbackArtifactSHA256 = item.outcome.Evidence.FeedbackArtifactSHA256
		record.Analyzed = true
	}
	settleOne := func(item pendingSettlement) error {
		op := item.opportunity
		request := govar.SettleRequest{RequestID: op.RequestID, SettlementID: eventID(config.Seed, streamSHA, op.RequestID, "settlement-final"), ProviderAttemptID: item.attemptID,
			TenantID: item.ctx.tenant, WorkloadUID: op.WorkloadUID, ActualCostMicros: govar.MoneyMicros(item.actualCost), UsageVersion: 1, Final: true,
			Usage:                 []govarpricing.UsageQuantity{{Basis: aiopsv1alpha1.ProviderBasisInputTokens, Quantity: item.outcome.ActualInputTokens}, {Basis: aiopsv1alpha1.ProviderBasisOutputTokens, Quantity: item.outcome.ActualOutputTokens}},
			AuthenticatedTenantID: item.ctx.tenant, AuthenticatedWorkloadUID: op.WorkloadUID}
		reservation, code, err := item.ctx.engine.Settle(request)
		if err != nil {
			return fmt.Errorf("settle %s: %w", op.RequestID, err)
		}
		if !reservation.Finalized || int64(reservation.BaseActualMicros) != item.actualCost {
			return fmt.Errorf("settle %s did not preserve authoritative integer cost", op.RequestID)
		}
		record := baseRecord(op, item.tieBreaker)
		record.LifecycleEvent = "settlement_final"
		record.ReasonCode = string(code)
		record.PreviousState = string(reservation.PreviousState)
		record.CurrentState = string(reservation.State)
		record.Effective = reservation.TransitionEffective
		record.Selected = true
		record.PricingVersion = reservation.PricingVersion
		record.PricingSnapshotSHA256 = reservation.PricingSnapshotSHA256
		record.CapEvidenceSHA256 = reservation.CapEvidenceSHA256
		record.PolicyVersion = reservation.PolicyVersion
		record.SelectedDeployment = reservation.SelectedDeployment
		record.SelectedModel = reservation.RouteSnapshot.ModelName
		record.SelectedProvider = reservation.RouteSnapshot.ProviderName
		record.ProviderAttemptID = reservation.ProviderAttemptID
		fillOutcome(&record, item)
		appendRecord(withLiability(record, item.ctx), item.dueStep)
		delete(active[op.TenantID+"\x00"+op.BudgetWindowID], op.RequestID)
		if op.DuplicateSettlement {
			duplicate, duplicateCode, duplicateErr := item.ctx.engine.Settle(request)
			if duplicateErr != nil || duplicateCode != govar.ReasonSettlementDuplicate || duplicate.TransitionEffective {
				return fmt.Errorf("duplicate settlement %s was not idempotent", op.RequestID)
			}
			record = baseRecord(op, item.tieBreaker)
			record.LifecycleEvent = "settlement_duplicate"
			record.ReasonCode = string(duplicateCode)
			record.PreviousState = string(duplicate.PreviousState)
			record.CurrentState = string(duplicate.State)
			record.Selected = true
			record.PricingVersion = duplicate.PricingVersion
			record.PricingSnapshotSHA256 = duplicate.PricingSnapshotSHA256
			record.CapEvidenceSHA256 = duplicate.CapEvidenceSHA256
			record.PolicyVersion = duplicate.PolicyVersion
			record.SelectedDeployment = duplicate.SelectedDeployment
			record.SelectedModel = duplicate.RouteSnapshot.ModelName
			record.SelectedProvider = duplicate.RouteSnapshot.ProviderName
			record.ProviderAttemptID = duplicate.ProviderAttemptID
			fillOutcome(&record, item)
			appendRecord(withLiability(record, item.ctx), item.dueStep)
		}
		if op.ConflictingSettlementReplay {
			conflict := request
			conflict.Usage = append([]govarpricing.UsageQuantity(nil), request.Usage...)
			conflict.Usage[1].Quantity++
			conflict.ActualCostMicros = 0
			_, conflictCode, conflictErr := item.ctx.engine.Settle(conflict)
			if conflictErr == nil || conflictCode != govar.ReasonDuplicateEvent {
				return fmt.Errorf("conflicting settlement %s was not rejected", op.RequestID)
			}
			record = baseRecord(op, item.tieBreaker)
			record.LifecycleEvent = "settlement_conflict_rejected"
			record.ReasonCode = string(conflictCode)
			record.PreviousState = string(reservation.State)
			record.CurrentState = string(reservation.State)
			record.Selected = true
			record.PricingVersion = reservation.PricingVersion
			record.PricingSnapshotSHA256 = reservation.PricingSnapshotSHA256
			record.CapEvidenceSHA256 = reservation.CapEvidenceSHA256
			record.PolicyVersion = reservation.PolicyVersion
			record.SelectedDeployment = reservation.SelectedDeployment
			record.SelectedModel = reservation.RouteSnapshot.ModelName
			record.SelectedProvider = reservation.RouteSnapshot.ProviderName
			record.ProviderAttemptID = reservation.ProviderAttemptID
			fillOutcome(&record, item)
			appendRecord(withLiability(record, item.ctx), item.dueStep)
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
			if err := settleOne(item); err != nil {
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
			row.RequestID = ""
			row.WorkloadUID = ""
			row.CohortID = ""
			row.CohortIndex = 0
			row.FeedbackItemID = ""
			row.Attempted = false
			row.LifecycleEvent = "opportunity_epoch_snapshot"
			row.ReasonCode = "prespecified_opportunity_epoch"
			row.FaultMode = FaultNominal
			row.StreamSequence = epoch
			ctx := contexts[key]
			if ctx != nil {
				row = withLiability(row, ctx)
			} else {
				row.AvailableMicros = row.BudgetMicros
			}
			for _, item := range active[key] {
				var addErr error
				row.ActiveActualMicros, addErr = checkedAdd(row.ActiveActualMicros, item.actualCost)
				if addErr != nil {
					panic(addErr)
				}
				row.ActiveReservedMicros, addErr = checkedAdd(row.ActiveReservedMicros, item.reserved)
				if addErr != nil {
					panic(addErr)
				}
			}
			appendRecord(row, epoch)
		}
	}

	for _, op := range opportunities {
		if err := flush(op.Sequence, false); err != nil {
			return RunResult{}, err
		}
		key := op.TenantID + "\x00" + op.BudgetWindowID
		ctx := contexts[key]
		if ctx == nil {
			ctx, err = newTenantContext(engine, config, op)
			if err != nil {
				return RunResult{}, err
			}
			contexts[key] = ctx
			active[key] = map[string]pendingSettlement{}
		}
		var govarSlot *GOVARSlotBound
		if config.ComparatorMethod == "gov_ar" {
			slot, bindErr := bindGOVARContext(ctx, config, op)
			if bindErr != nil {
				return RunResult{}, fmt.Errorf("bind GOV-AR opportunity %s: %w", op.RequestID, bindErr)
			}
			govarSlot = &slot
		}
		tie := RandomUint64("settlement-order", config.Seed, []byte(streamSHA), []byte(op.RequestID))
		request := govarAdmitRequest(op, ctx.tenant)
		// The production in-memory Engine deliberately refuses to mint a trusted
		// selected-feedback dispatch authorization. Frozen authorization therefore
		// binds here and fails closed; only PostgresEngine can execute it. The
		// development fixture remains outcome-isolated but is not frozen evidence.
		if config.EvidenceTier == "frozen_authorized" {
			request.SelectedFeedback = &govar.SelectedFeedbackBinding{RunID: config.RunID, DatasetID: config.SelectedFeedbackDatasetID, Split: config.SelectedFeedbackSplit, ItemID: op.FeedbackItemID, ProtocolSHA256: config.ProtocolSHA256, OracleArtifactSHA256: bindingEvidence.FeedbackArtifactSHA256, SoftwareSHA256: config.SoftwareSHA256, ConfigSHA256: configSHA}
		}
		response, err := ctx.engine.Admit(request, ctx.budget, ctx.routing, ctx.candidates)
		if err != nil {
			return RunResult{}, fmt.Errorf("admit %s: %w", op.RequestID, err)
		}
		record := baseRecord(op, tie)
		record.LifecycleEvent = "admission"
		record.Decision = string(response.Decision)
		record.ReasonCode = string(response.ReasonCode)
		record.Effective = response.Decision == govar.DecisionAdmit
		record.Selected = record.Effective
		record.ReservedMicros = int64(response.ReservedCostMicros)
		record.EffectiveMethod = response.ReservationMode
		if govarSlot == nil || response.AllocatedRiskPPB != 0 {
			record.AllocatedRiskPPB = response.AllocatedRiskPPB
		}
		record.PolicyVersion = response.PolicyVersion
		record.PricingVersion = response.PricingVersion
		record.SelectedDeployment = response.SelectedDeployment
		if govarSlot != nil {
			evidence, evidenceErr := config.MethodConfig.GOVAR.Evidence(govarSlot.EvidencePublicationSHA256)
			if evidenceErr != nil {
				return RunResult{}, evidenceErr
			}
			if !govarEvidenceFallsBack(config, evidence) {
				record.CalibrationArtifactSHA256 = evidence.Calibration.ArtifactSHA256
			}
			record.CandidateSetSHA256 = config.MethodConfig.GOVAR.CandidateSetSHA256
			record.JointSelectionSHA256 = config.MethodConfig.GOVAR.JointSelection.ArtifactSHA256
			record.TrustedEvaluationTime = config.VirtualStart
		}
		if record.Effective {
			record.CurrentState = string(govar.StateReserved)
		}
		appendRecord(withLiability(record, ctx), op.Sequence)
		if !record.Effective {
			if err := flush(op.Sequence, false); err != nil {
				return RunResult{}, err
			}
			emitSnapshots(op.Sequence)
			continue
		}
		if response.RouteSnapshot == nil {
			return RunResult{}, fmt.Errorf("admit %s omitted route snapshot", op.RequestID)
		}
		claim := govar.DispatchRequest{RequestID: op.RequestID, EventID: eventID(config.Seed, streamSHA, op.RequestID, "dispatch-claim"), TenantID: ctx.tenant, WorkloadUID: op.WorkloadUID, ProviderAttemptID: response.ProviderAttemptID, RouteSnapshotHash: response.RouteSnapshot.SnapshotHash, Status: govar.DispatchClaimed, AuthenticatedTenantID: ctx.tenant, AuthenticatedWorkloadUID: op.WorkloadUID}
		reservation, code, err := ctx.engine.Dispatch(claim)
		if err != nil {
			return RunResult{}, fmt.Errorf("claim dispatch %s: %w", op.RequestID, err)
		}
		record = baseRecord(op, tie)
		applyReservationRecord(&record, reservation)
		record.LifecycleEvent = "dispatch_claim"
		record.ReasonCode = string(code)
		record.Selected = true
		appendRecord(withLiability(record, ctx), op.Sequence)
		delivered := claim
		delivered.EventID = eventID(config.Seed, streamSHA, op.RequestID, "dispatch-delivered")
		delivered.Status = govar.DispatchDelivered
		reservation, code, err = ctx.engine.Dispatch(delivered)
		if err != nil {
			return RunResult{}, fmt.Errorf("deliver dispatch %s: %w", op.RequestID, err)
		}
		record = baseRecord(op, tie)
		applyReservationRecord(&record, reservation)
		record.LifecycleEvent = "dispatch_delivered"
		record.ReasonCode = string(code)
		record.Selected = true
		appendRecord(withLiability(record, ctx), op.Sequence)
		dispatchID := reservation.SelectedFeedbackDispatchID
		if dispatchID == "" && config.EvidenceTier == "development_debug" {
			dispatchID = DomainHash("govar-development-dispatch-v1", []byte(config.RunID), []byte(op.RequestID), []byte(response.ProviderAttemptID), []byte(configSHA))
		}
		if !shaPattern.MatchString(dispatchID) {
			return RunResult{}, fmt.Errorf("delivered dispatch %s omitted production selected-feedback authorization", op.RequestID)
		}
		catalogModelID, err := selectedFeedbackCatalogModelID(config, response.SelectedDeployment)
		if err != nil {
			return RunResult{}, fmt.Errorf("selected outcome %s: %w", op.RequestID, err)
		}
		outcomeReq := DispatchOutcomeRequest{DispatchID: dispatchID, RunID: config.RunID, RequestID: op.RequestID, FeedbackItemID: op.FeedbackItemID, SelectedModelID: catalogModelID, SelectedDeployment: response.SelectedDeployment, ProviderAttemptID: response.ProviderAttemptID, ConfigSHA256: configSHA}
		if err := issuer.AuthorizeDispatchedOutcome(outcomeReq, EffectiveDispatchProof{LifecycleEventIndex: eventIndex,
			DeliveryEventID: delivered.EventID, RouteSnapshotSHA256: reservation.RouteSnapshot.SnapshotHash,
			State: string(govar.StateDispatched), TransitionEffective: reservation.TransitionEffective}); err != nil {
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
			return RunResult{}, fmt.Errorf("request %s: %w", op.RequestID, err)
		}
		actualInput, err := ceilingProduct(config.InputPriceMicrosPerMillion, outcome.ActualInputTokens)
		if err != nil {
			return RunResult{}, err
		}
		actualOutput, err := ceilingProduct(config.OutputPriceMicrosPerMillion, outcome.ActualOutputTokens)
		if err != nil {
			return RunResult{}, err
		}
		actualCost, err := checkedAdd(actualInput, actualOutput)
		if err != nil {
			return RunResult{}, err
		}
		inputReserved, outputReserved, reserved, err := expectedReservation(config, op)
		if err != nil {
			return RunResult{}, err
		}
		if reserved != int64(response.ReservedCostMicros) {
			return RunResult{}, fmt.Errorf("production reservation mismatch for %s: got %d want %d", op.RequestID, response.ReservedCostMicros, reserved)
		}
		item := pendingSettlement{opportunity: op, ctx: ctx, dueStep: op.Sequence + op.SettlementDelaySteps, tieBreaker: tie, outcome: outcome, actualInputCost: actualInput, actualOutputCost: actualOutput, actualCost: actualCost, inputReserved: inputReserved, outputReserved: outputReserved, reserved: reserved, effectiveMethod: response.ReservationMode, attemptID: response.ProviderAttemptID}
		for index := len(records) - 3; index < len(records); index++ {
			if index >= 0 && records[index].RequestID == op.RequestID {
				fillOutcome(&records[index], item)
				records[index].PricingVersion = reservation.PricingVersion
				records[index].PricingSnapshotSHA256 = reservation.PricingSnapshotSHA256
				records[index].CapEvidenceSHA256 = reservation.CapEvidenceSHA256
				records[index].PolicyVersion = reservation.PolicyVersion
				records[index].SelectedDeployment = reservation.SelectedDeployment
				records[index].SelectedModel = reservation.RouteSnapshot.ModelName
				records[index].SelectedProvider = reservation.RouteSnapshot.ProviderName
				records[index].ProviderAttemptID = reservation.ProviderAttemptID
			}
		}
		pending = append(pending, item)
		active[key][op.RequestID] = item
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
	return RunResult{Config: config, ConfigSHA256: configSHA, CanonicalConfigSHA256: canonicalConfigSHA, StreamSHA256: streamSHA, MatchedStreamKey: matchedKey, Records: records, DecisionCounts: counts, LifecycleCounts: lifecycle, Metrics: metrics}, nil
}

func applyReservationRecord(record *LifecycleRecord, reservation govar.Reservation) {
	record.PreviousState = string(reservation.PreviousState)
	record.CurrentState = string(reservation.State)
	record.Effective = reservation.TransitionEffective
	record.ReservedMicros = int64(reservation.ReservedCostMicros)
	record.EffectiveMethod = reservation.ReservationMode
	if record.ComparatorMethod != "gov_ar" || reservation.AllocatedRiskPPB != 0 {
		record.AllocatedRiskPPB = reservation.AllocatedRiskPPB
	}
	if reservation.CalibrationArtifactSHA256 != "" {
		record.CalibrationArtifactSHA256 = reservation.CalibrationArtifactSHA256
	}
	if reservation.CohortRegistryDigest != "" {
		record.CohortRegistrySHA256 = reservation.CohortRegistryDigest
	}
	record.PricingVersion = reservation.PricingVersion
	record.PricingSnapshotSHA256 = reservation.PricingSnapshotSHA256
	record.CapEvidenceSHA256 = reservation.CapEvidenceSHA256
	record.PolicyVersion = reservation.PolicyVersion
	record.SelectedDeployment = reservation.SelectedDeployment
	record.SelectedModel = reservation.RouteSnapshot.ModelName
	record.SelectedProvider = reservation.RouteSnapshot.ProviderName
	record.ProviderAttemptID = reservation.ProviderAttemptID
}

func validateOutcomeBounds(config Config, op Opportunity, outcome DispatchedOutcome) error {
	if outcome.Evidence.RunID != config.RunID || outcome.Evidence.DatasetID != config.SelectedFeedbackDatasetID || outcome.Evidence.Split != config.SelectedFeedbackSplit || outcome.Evidence.ProtocolSHA256 != config.ProtocolSHA256 || outcome.Evidence.SoftwareSHA256 != config.SoftwareSHA256 || outcome.Evidence.ModelMapSHA256 != config.SelectedFeedbackModelMapSHA256 {
		return errors.New("selected-feedback authority is not bound to config evidence")
	}
	inputMismatch := outcome.ActualInputTokens != op.InputTokens
	capViolation := outcome.ActualOutputTokens > op.MaxOutputTokens || outcome.ActualOutputTokens > config.VerifiedOutputCapTokens
	switch op.FaultMode {
	case FaultNominal:
		if inputMismatch || capViolation {
			return errors.New("nominal outcome violates exact input or verified output bound")
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

func expectedReservation(config Config, op Opportunity) (int64, int64, int64, error) {
	if config.ProductionReservationMode == "observational" || config.ProductionReservationMode == "settled_only" {
		return 0, 0, 0, nil
	}
	input, err := ceilingProduct(config.InputPriceMicrosPerMillion, op.InputTokens)
	if err != nil {
		return 0, 0, 0, err
	}
	tokens := op.MaxOutputTokens
	switch config.ProductionReservationMode {
	case "mean", "fixed_quantile":
		tokens = min64(config.EstimateOutputTokens, op.MaxOutputTokens)
	case "fixed_margin":
		tokens = min64(config.EstimateOutputTokens+config.MarginOutputTokens, op.MaxOutputTokens)
	case "strict_provider_cap":
	case "govar_fixed_cohort":
		slot, err := config.MethodConfig.GOVAR.Slot(op.TenantID, op.BudgetWindowID, op.CohortID, op.CohortIndex, config.MethodConfig.GOVAR.CandidateSetSHA256)
		if err != nil {
			return 0, 0, 0, err
		}
		evidence, err := config.MethodConfig.GOVAR.Evidence(slot.EvidencePublicationSHA256)
		if err != nil {
			return 0, 0, 0, err
		}
		if !govarEvidenceFallsBack(config, evidence) {
			tokens = min64(evidence.Calibration.UpperOutputTokens, op.MaxOutputTokens)
		}
	default:
		return 0, 0, 0, errors.New("unsupported reservation mode")
	}
	output, err := ceilingProduct(config.OutputPriceMicrosPerMillion, tokens)
	if err != nil {
		return 0, 0, 0, err
	}
	total, err := checkedAdd(input, output)
	return input, output, total, err
}

func expectedReservationMode(config Config, op Opportunity) (string, error) {
	if config.ProductionReservationMode != "govar_fixed_cohort" {
		return config.ProductionReservationMode, nil
	}
	slot, err := config.MethodConfig.GOVAR.Slot(op.TenantID, op.BudgetWindowID, op.CohortID, op.CohortIndex, config.MethodConfig.GOVAR.CandidateSetSHA256)
	if err != nil {
		return "", err
	}
	evidence, err := config.MethodConfig.GOVAR.Evidence(slot.EvidencePublicationSHA256)
	if err != nil {
		return "", err
	}
	if govarEvidenceFallsBack(config, evidence) {
		return "strict_provider_cap", nil
	}
	return "govar_fixed_cohort", nil
}

func govarEvidenceFallsBack(config Config, evidence GOVARCalibrationEvidence) bool {
	return govarEvidenceFallbackReason(config, evidence) != ""
}

func govarEvidenceFallbackReason(config Config, evidence GOVARCalibrationEvidence) string {
	method := config.MethodConfig.GOVAR
	trustedAt, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	if evidence.Drift.Result.Support < method.RevalidationMinimumSupport {
		return string(govar.ReasonCalibrationSupport)
	}
	if trustedAt.Sub(evidence.Calibration.WindowEnd) > time.Duration(method.MaxAgeSeconds)*time.Second ||
		trustedAt.Sub(evidence.Drift.Result.WindowEnd) > time.Duration(method.MaxAgeSeconds)*time.Second {
		return string(govar.ReasonCalibrationStale)
	}
	if evidence.Drift.Result.Detected {
		return string(govar.ReasonCalibrationDrift)
	}
	return ""
}
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func newTenantContext(engine *govar.Engine, config Config, op Opportunity) (*tenantContext, error) {
	budgetQuantity, err := microsQuantity(op.BudgetMicros)
	if err != nil {
		return nil, err
	}
	budget := aiopsv1alpha1.AIBudgetPolicy{ObjectMeta: metav1.ObjectMeta{Name: "experiment-budget-" + op.TenantID, Generation: 1}, Spec: aiopsv1alpha1.AIBudgetPolicySpec{Target: aiopsv1alpha1.BudgetTarget{Namespace: "experiment"}, Period: "monthly", BudgetEUR: budgetQuantity}, Status: aiopsv1alpha1.AIBudgetPolicyStatus{ObservedGeneration: 1, Conditions: []metav1.Condition{{Type: aiopsv1alpha1.ConditionReady, Status: metav1.ConditionTrue}}}}
	trustedAt, err := time.Parse(time.RFC3339Nano, config.VirtualStart)
	if err != nil {
		return nil, err
	}
	now := metav1.NewTime(trustedAt)
	method := aiopsv1alpha1.GOVARReservationMethod(config.ProductionReservationMode)
	reservation := aiopsv1alpha1.GOVARReservationPolicy{Method: method}
	value, margin := config.EstimateOutputTokens, config.MarginOutputTokens
	switch method {
	case aiopsv1alpha1.GOVARReservationMean:
		reservation.MeanOutputTokens = &value
	case aiopsv1alpha1.GOVARReservationFixedMargin:
		reservation.MeanOutputTokens = &value
		reservation.MarginOutputTokens = &margin
	case aiopsv1alpha1.GOVARReservationFixedQuantile:
		reservation.FixedQuantileOutputTokens = &value
	}
	routing := aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "experiment-routing-" + op.TenantID, Generation: 1}, Spec: aiopsv1alpha1.AIRoutingPolicySpec{Objective: "cost", GOVAR: &aiopsv1alpha1.GOVARRoutingPolicySpec{Reservation: reservation, Drift: aiopsv1alpha1.GOVARDriftPolicy{Detector: "coverage-gap", ThresholdPPB: 1, Fallback: "strict_provider_cap", RevalidationMinimumSupport: 1}}}, Status: aiopsv1alpha1.AIRoutingPolicyStatus{ObservedGeneration: 1, LastEvaluatedAt: &now, Conditions: []metav1.Condition{{Type: aiopsv1alpha1.ConditionReady, Status: metav1.ConditionTrue}}}}
	return &tenantContext{engine: engine, tenant: op.TenantID, window: op.BudgetWindowID, budget: budget, routing: routing, candidates: []govar.Candidate{experimentCandidate(config)}}, nil
}

func experimentCandidate(config Config) govar.Candidate {
	candidateRegimeRoot := DomainHash("govar-experiment-outcome-free-candidate-regime-v1",
		[]byte(config.SoftwareSHA256), []byte(config.ProtocolSHA256),
		[]byte(strconv.FormatInt(config.InputPriceMicrosPerMillion, 10)),
		[]byte(strconv.FormatInt(config.OutputPriceMicrosPerMillion, 10)),
		[]byte(strconv.FormatInt(config.VerifiedOutputCapTokens, 10)),
		[]byte("experiment-model"), []byte("experiment-provider-uid"), []byte("experiment-model"),
		[]byte("openai-body"), []byte(govarpricing.CurrentAdapterVersion))
	observed := metav1.NewTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	validUntil := metav1.NewTime(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
	pricing := govarpricing.NormalizedPricingSnapshot{SpecGeneration: 1, Version: "experiment-pricing-v1", ObservedAt: observed, ValidUntil: validUntil, Currency: "EUR", Completeness: aiopsv1alpha1.ProviderPricingComplete, AdapterVersion: govarpricing.CurrentAdapterVersion, EvidenceMode: aiopsv1alpha1.ProviderEvidenceAdminAttested, EvidenceSHA256: DomainHash("govar-experiment-pricing-evidence-v1", []byte(candidateRegimeRoot)), SourceVersion: "synthetic-public-experiment-v1", Charges: []aiopsv1alpha1.AIProviderNormalizedChargeStatus{{Basis: aiopsv1alpha1.ProviderBasisInputTokens, Applicability: aiopsv1alpha1.ProviderChargeRequestDeclared, PriceMicrosPerUnit: config.InputPriceMicrosPerMillion, UnitDenominator: 1_000_000, SettlementUsageField: aiopsv1alpha1.ProviderBasisInputTokens, RequestBoundField: "input_tokens"}, {Basis: aiopsv1alpha1.ProviderBasisOutputTokens, Applicability: aiopsv1alpha1.ProviderChargeRequestDeclared, PriceMicrosPerUnit: config.OutputPriceMicrosPerMillion, UnitDenominator: 1_000_000, SettlementUsageField: aiopsv1alpha1.ProviderBasisOutputTokens, RequestBoundField: "max_output_tokens"}}, InapplicableBases: []aiopsv1alpha1.ProviderBillableBasis{aiopsv1alpha1.ProviderBasisCachedInputTokens, aiopsv1alpha1.ProviderBasisReasoningTokens, aiopsv1alpha1.ProviderBasisRequest, aiopsv1alpha1.ProviderBasisToolCall, aiopsv1alpha1.ProviderBasisMediaUnit, aiopsv1alpha1.ProviderBasisBillableSecond, aiopsv1alpha1.ProviderBasisCancellation, aiopsv1alpha1.ProviderBasisRetryAttempt}}
	pricing.SnapshotSHA256 = govarpricing.SnapshotDigest(pricing)
	route := govar.RouteSnapshot{Namespace: "experiment", ModelName: "experiment-model", ModelUID: "experiment-model-uid", ModelGeneration: 1, ModelResourceVersion: "experiment-model-rv", ProviderName: "experiment-provider", ProviderUID: "experiment-provider-uid", ProviderGeneration: 1, ProviderResourceVersion: "experiment-provider-rv", PricingVersion: pricing.Version, PricingComplianceHash: DomainHash("govar-experiment-pricing-compliance-v1", []byte(pricing.SnapshotSHA256)), RouteBindingName: "experiment-route", ProviderDeployment: "experiment-model", Cluster: "experiment-backend", Authority: "experiment-backend.local", PathMode: "openai-body"}
	route.SnapshotHash = govar.RouteSnapshotHash(route)
	return govar.Candidate{ModelRef: route.ModelName, ModelName: route.ModelName, ProviderRef: route.ProviderName, ProviderType: "openai", InputPriceMicrosPerMillion: config.InputPriceMicrosPerMillion, OutputPriceMicrosPerMillion: config.OutputPriceMicrosPerMillion, PricingVersion: pricing.Version, SnapshotVersion: route.SnapshotHash, ContextWindow: 10_000_000, QualityScore: 1, VerifiedOutputCap: true, VerifiedOutputCapTokens: config.VerifiedOutputCapTokens, CapEvidenceDigest: DomainHash("govar-experiment-cap-evidence-v1", []byte(candidateRegimeRoot)), PricingSnapshot: pricing, Feasible: true, RouteSnapshot: route}
}
func microsQuantity(micros int64) (resource.Quantity, error) {
	if micros <= 0 {
		return resource.Quantity{}, errors.New("budget micros must be positive")
	}
	return resource.ParseQuantity(fmt.Sprintf("%d.%06d", micros/1_000_000, micros%1_000_000))
}
func eventID(seed uint64, streamSHA, requestID, event string) string {
	return "evt-" + DomainHash("govar-experiment-event-v1", []byte(strconv.FormatUint(seed, 10)), []byte(streamSHA), []byte(requestID), []byte(event))[:40]
}
func IsSafeArtifactIdentity(value string) bool {
	return validIdentity(value) && !strings.Contains(value, "..")
}
