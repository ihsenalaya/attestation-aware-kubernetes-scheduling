package govarexperiment

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	// ConfigSchemaV2 and OpportunitySchemaV2 are deliberately separate from
	// their v1 counterparts. The production trace runner remains v1-only until
	// its PostgreSQL scheduler and final-evidence writer are implemented.
	ConfigSchemaV2      = "govar-experiment-config-v2"
	OpportunitySchemaV2 = "govar-opportunity-v2"
	UsageDatasetV2      = "azure_llm_code_2024"
	UsageSplitV2        = "development_pilot"
	EvidenceTierV2      = "development_usage_only"

	// OpportunityTieBreakV2 is the authoritative order for events with equal
	// absolute timestamps. Processing all arrivals before completions makes a
	// simultaneous arrival batch observable when recomputing peak concurrency
	// and prevents same-time response/usage/settlement feedback from affecting
	// those admissions. Sequence and request identity make the remaining order
	// total and stable.
	OpportunityTieBreakV2 = "timestamp_then_arrival_provider_response_usage_available_settlement_then_sequence_request_id_v1"
)

type E1CellType string

const (
	E1CellPrincipal   E1CellType = "principal"
	E1CellSensitivity E1CellType = "sensitivity"
)

type E1Concurrency int64

const (
	E1ConcurrencyOne         E1Concurrency = 1
	E1ConcurrencyFifty       E1Concurrency = 50
	E1ConcurrencyFiveHundred E1Concurrency = 500
)

type E1SettlementDelaySeconds int64

const (
	E1SettlementDelayZero  E1SettlementDelaySeconds = 0
	E1SettlementDelayTen   E1SettlementDelaySeconds = 10
	E1SettlementDelaySixty E1SettlementDelaySeconds = 60
)

type E1BudgetLevel string

const (
	E1BudgetRestrictive   E1BudgetLevel = "restrictive"
	E1BudgetMedium        E1BudgetLevel = "medium"
	E1BudgetUnconstrained E1BudgetLevel = "unconstrained"
)

type E1RiskTargetPPB int64

const (
	E1RiskTargetZero        E1RiskTargetPPB = 0
	E1RiskTargetOnePercent  E1RiskTargetPPB = 10_000_000
	E1RiskTargetFivePercent E1RiskTargetPPB = 50_000_000
)

// E1Factors is embedded in ConfigV2 so the immutable factor coordinates stay
// top-level JSON fields. Numeric zero is meaningful for both delay and risk;
// using a distinct v2 type prevents omitempty from erasing those coordinates
// while preserving byte-for-byte v1 serialization.
type E1Factors struct {
	CellType               E1CellType               `json:"cell_type"`
	Concurrency            E1Concurrency            `json:"concurrency"`
	SettlementDelaySeconds E1SettlementDelaySeconds `json:"settlement_delay_seconds"`
	BudgetLevel            E1BudgetLevel            `json:"budget_level"`
	RiskTargetPPB          E1RiskTargetPPB          `json:"risk_target_ppb"`
}

func (f E1Factors) Validate() error {
	switch f.CellType {
	case E1CellPrincipal, E1CellSensitivity:
	default:
		return fmt.Errorf("unsupported E1 cell_type %q", f.CellType)
	}
	switch f.Concurrency {
	case E1ConcurrencyOne, E1ConcurrencyFifty, E1ConcurrencyFiveHundred:
	default:
		return fmt.Errorf("unsupported E1 concurrency %d", f.Concurrency)
	}
	switch f.SettlementDelaySeconds {
	case E1SettlementDelayZero, E1SettlementDelayTen, E1SettlementDelaySixty:
	default:
		return fmt.Errorf("unsupported E1 settlement delay %d seconds", f.SettlementDelaySeconds)
	}
	switch f.BudgetLevel {
	case E1BudgetRestrictive, E1BudgetMedium, E1BudgetUnconstrained:
	default:
		return fmt.Errorf("unsupported E1 budget_level %q", f.BudgetLevel)
	}
	switch f.RiskTargetPPB {
	case E1RiskTargetZero, E1RiskTargetOnePercent, E1RiskTargetFivePercent:
	default:
		return fmt.Errorf("unsupported E1 risk_target_ppb %d", f.RiskTargetPPB)
	}
	return nil
}

// ConfigV2 is an explicit usage-only wire contract. It intentionally does not
// embed Config: embedding would serialize the v1 selected-feedback fields even
// when empty and would make an Azure usage trace look like quality evidence.
// Config remains byte-for-byte unchanged.
type ConfigV2 struct {
	SchemaVersion               string             `json:"schema_version"`
	RecordType                  string             `json:"record_type"`
	ExperimentID                string             `json:"experiment_id"`
	RunID                       string             `json:"run_id"`
	CellID                      string             `json:"cell_id"`
	Scenario                    string             `json:"scenario"`
	Seed                        uint64             `json:"seed"`
	RegistryID                  string             `json:"registry_id"`
	ProtocolMethodID            string             `json:"protocol_method_id"`
	ComparatorMethod            string             `json:"comparator_method"`
	ProductionReservationMode   string             `json:"production_reservation_mode"`
	MethodConfig                MethodConfig       `json:"method_config"`
	MethodConfigSHA256          string             `json:"method_config_sha256"`
	ClusterID                   string             `json:"cluster_id,omitempty"`
	TrialID                     string             `json:"trial_id,omitempty"`
	VirtualStart                string             `json:"virtual_start"`
	EvidenceTier                string             `json:"evidence_tier"`
	SoftwareSHA256              string             `json:"software_sha256"`
	DataSHA256                  string             `json:"data_sha256"`
	ProtocolSHA256              string             `json:"protocol_sha256"`
	SplitSHA256                 string             `json:"split_sha256"`
	SourceSHA256                string             `json:"source_sha256"`
	UsageDatasetID              string             `json:"usage_dataset_id"`
	UsageSplit                  string             `json:"usage_split"`
	ProvenanceLock              E1ProvenanceLockV2 `json:"provenance_lock"`
	UsageBindingSHA256          string             `json:"usage_binding_sha256"`
	InputPriceMicrosPerMillion  int64              `json:"input_price_micros_per_million"`
	OutputPriceMicrosPerMillion int64              `json:"output_price_micros_per_million"`
	VerifiedOutputCapTokens     int64              `json:"verified_output_cap_tokens"`
	EstimateOutputTokens        int64              `json:"estimate_output_tokens,omitempty"`
	MarginOutputTokens          int64              `json:"margin_output_tokens,omitempty"`
	Cohorts                     []CohortSpec       `json:"cohorts,omitempty"`
	E1Factors
}

func (c ConfigV2) Validate() error {
	if c.SchemaVersion != ConfigSchemaV2 || c.RecordType != "experiment_config" {
		return errors.New("unsupported E1 v2 experiment config schema or record type")
	}
	if c.ExperimentID != "E1" {
		return errors.New("E1 v2 config requires experiment_id E1")
	}
	for name, value := range map[string]string{
		"experiment_id": c.ExperimentID, "run_id": c.RunID, "cell_id": c.CellID, "scenario": c.Scenario,
	} {
		if !identityPattern.MatchString(value) {
			return fmt.Errorf("%s is not a safe immutable identity", name)
		}
	}
	for name, value := range map[string]string{"cluster_id": c.ClusterID, "trial_id": c.TrialID} {
		if value != "" && !identityPattern.MatchString(value) {
			return fmt.Errorf("%s is not a safe immutable identity", name)
		}
	}
	if c.EvidenceTier != EvidenceTierV2 {
		return fmt.Errorf("E1 v2 evidence_tier must be %s", EvidenceTierV2)
	}
	if c.UsageDatasetID != UsageDatasetV2 || c.UsageSplit != UsageSplitV2 {
		return errors.New("E1 v2 accepts only the authorized Azure development_pilot usage split")
	}
	if err := c.ProvenanceLock.Validate(); err != nil {
		return fmt.Errorf("E1 v2 provenance lock: %w", err)
	}
	if c.ProvenanceLock.ProtocolSHA256 != c.ProtocolSHA256 {
		return errors.New("E1 v2 provenance subject protocol does not equal config protocol_sha256")
	}
	for name, value := range map[string]string{
		"software_sha256": c.SoftwareSHA256, "data_sha256": c.DataSHA256,
		"protocol_sha256": c.ProtocolSHA256, "split_sha256": c.SplitSHA256,
		"source_sha256": c.SourceSHA256, "usage_binding_sha256": c.UsageBindingSHA256,
	} {
		if !shaPattern.MatchString(value) {
			return fmt.Errorf("%s must be a lower-case SHA-256", name)
		}
	}
	if c.SourceSHA256 != SourceRoot(c.SoftwareSHA256, c.DataSHA256, c.ProtocolSHA256, c.SplitSHA256) {
		return errors.New("source_sha256 does not recompute from software/data/protocol/split hashes")
	}
	mode, known := productionMode(c.ComparatorMethod)
	if !known || c.RegistryID != registryID(c.ComparatorMethod) || c.ProtocolMethodID != c.ComparatorMethod || c.MethodConfig.ProtocolID != c.ComparatorMethod {
		return errors.New("budget/admission registry, protocol, comparator, and method-config identities do not agree")
	}
	if c.ProductionReservationMode != mode {
		return fmt.Errorf("comparator %s must execute production mode %s", c.ComparatorMethod, mode)
	}
	if c.ComparatorMethod == "gov_ar" && c.RiskTargetPPB == E1RiskTargetZero {
		return errors.New("GOV-AR requires a strictly positive E1 risk_target_ppb")
	}
	methodDigest, err := MethodConfigDigest(c.MethodConfig)
	if err != nil {
		return fmt.Errorf("method_config: %w", err)
	}
	if c.MethodConfigSHA256 != methodDigest {
		return errors.New("method_config_sha256 does not recompute from canonical method config")
	}
	start, err := time.Parse(time.RFC3339Nano, c.VirtualStart)
	if err != nil || start.Format(time.RFC3339Nano) != c.VirtualStart {
		return errors.New("virtual_start must be canonical RFC3339Nano")
	}
	if c.InputPriceMicrosPerMillion < 0 || c.OutputPriceMicrosPerMillion < 0 ||
		c.VerifiedOutputCapTokens <= 0 || c.EstimateOutputTokens < 0 || c.MarginOutputTokens < 0 {
		return errors.New("prices, cap, estimates, and margins must be non-negative with a positive cap")
	}
	if err := validateReservationParametersV2(c); err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for index, cohort := range c.Cohorts {
		if !validIdentity(cohort.TenantID) || !validIdentity(cohort.BudgetWindowID) || !validIdentity(cohort.CohortID) || cohort.Size <= 0 {
			return fmt.Errorf("cohort registry entry %d is invalid", index)
		}
		key := cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("cohort registry entry %d is duplicate", index)
		}
		seen[key] = struct{}{}
	}
	if err := c.E1Factors.Validate(); err != nil {
		return err
	}
	// Risk-aware methods must not carry a factor label different from the
	// immutable method artifact that will actually be executed.
	switch c.ComparatorMethod {
	case "gov_ar":
		if c.MethodConfig.GOVAR == nil || c.MethodConfig.GOVAR.TenantRiskPPB != int64(c.RiskTargetPPB) {
			return errors.New("E1 risk_target_ppb does not match the GOV-AR method artifact")
		}
	case "adaptive_quantile":
		if c.MethodConfig.Adaptive == nil || 1_000_000_000-c.MethodConfig.Adaptive.CoverageTargetPPB != int64(c.RiskTargetPPB) {
			return errors.New("E1 risk_target_ppb does not match the adaptive-quantile coverage target")
		}
	}
	if c.ComparatorMethod == "gov_ar" {
		if err := validateGOVARConfig(c.runtimeConfig()); err != nil {
			return err
		}
	}
	return nil
}

func validateReservationParametersV2(c ConfigV2) error {
	switch c.ComparatorMethod {
	case "strict_max":
		if c.EstimateOutputTokens != 0 || c.MarginOutputTokens != 0 {
			return errors.New("strict_max does not accept estimate or margin")
		}
	case "mean":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens || c.MarginOutputTokens != 0 {
			return errors.New("mean estimate is outside cap or margin is inapplicable")
		}
	case "fixed_quantile":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens || c.MarginOutputTokens != 0 {
			return errors.New("estimate is outside the verified cap or margin is inapplicable")
		}
	case "mean_margin":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.MarginOutputTokens != c.MethodConfig.MeanMarginOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens-c.MarginOutputTokens {
			return errors.New("mean plus margin exceeds the verified cap")
		}
	case "fixed_estimate":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.MarginOutputTokens != 0 || c.EstimateOutputTokens > c.VerifiedOutputCapTokens {
			return errors.New("fixed_estimate must execute the one global method-config estimate")
		}
	case "no_budget", "settled_only":
		if c.EstimateOutputTokens != 0 || c.MarginOutputTokens != 0 {
			return errors.New("non-reserving comparator contains a reservation estimate")
		}
	case "adaptive_quantile":
		if c.MethodConfig.Adaptive == nil || c.EstimateOutputTokens != c.MethodConfig.Adaptive.InitialOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens || c.MarginOutputTokens != 0 {
			return errors.New("adaptive_quantile initial estimate is not bound to method_config/cap")
		}
	case "expected_cost_router":
		if c.EstimateOutputTokens != c.MethodConfig.StaticOutputTokens || c.EstimateOutputTokens > c.VerifiedOutputCapTokens || c.MarginOutputTokens != 0 {
			return errors.New("expected_cost_router trace regime is not bound to its expected output estimate")
		}
	case "oracle_future_cost":
		if c.EstimateOutputTokens != 0 || c.MarginOutputTokens != 0 {
			return errors.New("oracle future cost cannot be prefilled into the online config estimate")
		}
	case "gov_ar":
		if c.EstimateOutputTokens != 0 || c.MarginOutputTokens != 0 {
			return errors.New("GOV-AR must resolve the slot-bound quantile rather than a global scalar")
		}
	}
	return nil
}

// runtimeConfig projects only the common admission parameters required by the
// already-audited method adapters. It is never serialized and deliberately
// leaves every v1 selected-feedback field empty.
func (c ConfigV2) runtimeConfig() Config {
	return Config{
		SchemaVersion: ConfigSchema, RecordType: c.RecordType, ExperimentID: c.ExperimentID,
		RunID: c.RunID, CellID: c.CellID, Scenario: c.Scenario, Seed: c.Seed,
		RegistryID: c.RegistryID, ProtocolMethodID: c.ProtocolMethodID,
		ComparatorMethod: c.ComparatorMethod, ProductionReservationMode: c.ProductionReservationMode,
		MethodConfig: c.MethodConfig, MethodConfigSHA256: c.MethodConfigSHA256,
		ClusterID: c.ClusterID, TrialID: c.TrialID, VirtualStart: c.VirtualStart,
		EvidenceTier: c.EvidenceTier, SoftwareSHA256: c.SoftwareSHA256, DataSHA256: c.DataSHA256,
		ProtocolSHA256: c.ProtocolSHA256, SplitSHA256: c.SplitSHA256, SourceSHA256: c.SourceSHA256,
		InputPriceMicrosPerMillion:  c.InputPriceMicrosPerMillion,
		OutputPriceMicrosPerMillion: c.OutputPriceMicrosPerMillion,
		VerifiedOutputCapTokens:     c.VerifiedOutputCapTokens, EstimateOutputTokens: c.EstimateOutputTokens,
		MarginOutputTokens: c.MarginOutputTokens, Cohorts: append([]CohortSpec(nil), c.Cohorts...),
	}
}

func ParseConfigV2(raw []byte) (ConfigV2, string, error) {
	var config ConfigV2
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return ConfigV2{}, "", fmt.Errorf("decode E1 v2 config: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return ConfigV2{}, "", fmt.Errorf("decode E1 v2 config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return ConfigV2{}, "", err
	}
	canonical, err := json.Marshal(config)
	if err != nil {
		return ConfigV2{}, "", err
	}
	return config, SHA256(canonical), nil
}

// OpportunityV2 separates arrival order (Sequence) from four absolute event
// times. It intentionally has no settlement_delay_steps field, preventing a
// second, conflicting clock from entering a v2 trace.
type OpportunityV2 struct {
	SchemaVersion               string `json:"schema_version"`
	RecordType                  string `json:"record_type"`
	Sequence                    int64  `json:"sequence"`
	RequestID                   string `json:"request_id"`
	TenantID                    string `json:"tenant_id"`
	BudgetWindowID              string `json:"budget_window_id"`
	WorkloadUID                 string `json:"workload_uid"`
	BudgetMicros                int64  `json:"budget_micros"`
	CohortID                    string `json:"cohort_id,omitempty"`
	CohortIndex                 int64  `json:"cohort_index,omitempty"`
	InputTokens                 int64  `json:"input_tokens"`
	MaxOutputTokens             int64  `json:"max_output_tokens"`
	ArrivalAt                   string `json:"arrival_at"`
	ProviderResponseAt          string `json:"provider_response_at"`
	UsageAvailableAt            string `json:"usage_available_at"`
	SettlementAt                string `json:"settlement_at"`
	FaultMode                   string `json:"fault_mode"`
	UsageItemID                 string `json:"usage_item_id"`
	SourceRow                   int64  `json:"source_row"`
	SourceTimestamp             string `json:"source_timestamp"`
	ContextTokens               int64  `json:"context_tokens"`
	PreOutcomeID                string `json:"preoutcome_id"`
	DuplicateSettlement         bool   `json:"duplicate_settlement,omitempty"`
	ConflictingSettlementReplay bool   `json:"conflicting_settlement_replay,omitempty"`
}

type opportunityTimesV2 struct {
	arrival          time.Time
	providerResponse time.Time
	usageAvailable   time.Time
	settlement       time.Time
}

func (o OpportunityV2) Validate() error {
	if o.SchemaVersion != OpportunitySchemaV2 || o.RecordType != "opportunity" {
		return errors.New("unsupported E1 v2 opportunity schema or record type")
	}
	// Project only common, non-temporal fields through the v1 validator. This
	// keeps identity, token, budget, cohort, and typed-fault rules identical.
	common := Opportunity{
		SchemaVersion: OpportunitySchema, RecordType: o.RecordType, Sequence: o.Sequence,
		RequestID: o.RequestID, TenantID: o.TenantID, BudgetWindowID: o.BudgetWindowID,
		WorkloadUID: o.WorkloadUID, BudgetMicros: o.BudgetMicros, CohortID: o.CohortID,
		CohortIndex: o.CohortIndex, InputTokens: o.InputTokens, MaxOutputTokens: o.MaxOutputTokens,
		SettlementDelaySteps: 0, FaultMode: o.FaultMode, FeedbackItemID: o.UsageItemID,
		DuplicateSettlement:         o.DuplicateSettlement,
		ConflictingSettlementReplay: o.ConflictingSettlementReplay,
	}
	if err := common.Validate(); err != nil {
		return err
	}
	if o.InputTokens != o.ContextTokens {
		return errors.New("input_tokens must equal the pre-outcome context_tokens coordinate")
	}
	if err := validatePreOutcomeCoordinatesV2(o.SourceRow, o.SourceTimestamp, o.ContextTokens, o.PreOutcomeID, o.UsageItemID); err != nil {
		return err
	}
	_, err := o.times()
	return err
}

func (o OpportunityV2) times() (opportunityTimesV2, error) {
	arrival, err := parseCanonicalAbsoluteTimeV2("arrival_at", o.ArrivalAt)
	if err != nil {
		return opportunityTimesV2{}, err
	}
	providerResponse, err := parseCanonicalAbsoluteTimeV2("provider_response_at", o.ProviderResponseAt)
	if err != nil {
		return opportunityTimesV2{}, err
	}
	usageAvailable, err := parseCanonicalAbsoluteTimeV2("usage_available_at", o.UsageAvailableAt)
	if err != nil {
		return opportunityTimesV2{}, err
	}
	settlement, err := parseCanonicalAbsoluteTimeV2("settlement_at", o.SettlementAt)
	if err != nil {
		return opportunityTimesV2{}, err
	}
	if providerResponse.Before(arrival) {
		return opportunityTimesV2{}, errors.New("provider_response_at precedes arrival_at")
	}
	if usageAvailable.Before(providerResponse) {
		return opportunityTimesV2{}, errors.New("usage_available_at precedes provider_response_at")
	}
	if settlement.Before(usageAvailable) {
		return opportunityTimesV2{}, errors.New("settlement_at precedes usage_available_at")
	}
	return opportunityTimesV2{arrival: arrival, providerResponse: providerResponse, usageAvailable: usageAvailable, settlement: settlement}, nil
}

func parseCanonicalAbsoluteTimeV2(name, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.UTC().Format(time.RFC3339Nano) != value {
		return time.Time{}, fmt.Errorf("%s must be canonical UTC RFC3339Nano", name)
	}
	return parsed.UTC(), nil
}

// E1StreamFacts contains values recomputed from the immutable opportunity
// stream. They are not caller assertions and can later be copied into a v2
// manifest only after the execution and evidence layers are implemented.
type E1StreamFacts struct {
	RecordCount             int64
	ArrivalBatchCount       int64
	MaximumArrivalBatchSize int64
	ObservedPeakConcurrency E1Concurrency
	FirstArrivalAt          string
	LastSettlementAt        string
	TieBreakPolicy          string
}

type opportunityEventKindV2 uint8

const (
	eventArrivalV2 opportunityEventKindV2 = iota
	eventProviderResponseV2
	eventUsageAvailableV2
	eventSettlementV2
)

type opportunityEventV2 struct {
	at        time.Time
	kind      opportunityEventKindV2
	sequence  int64
	requestID string
}

// ParseStreamV2 is intentionally separate from ParseStream. Returning v2
// opportunities to the v1 executor would silently discard absolute event
// semantics, so runner integration must be an explicit later change.
func ParseStreamV2(raw []byte, config ConfigV2) ([]OpportunityV2, E1StreamFacts, error) {
	var opportunities []OpportunityV2
	err := scanCanonicalJSONLV2(raw, "E1 v2 stream", func(_ int, line []byte) error {
		var opportunity OpportunityV2
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&opportunity); err != nil {
			return err
		}
		if err := requireJSONEOF(decoder); err != nil {
			return err
		}
		if err := opportunity.Validate(); err != nil {
			return err
		}
		opportunities = append(opportunities, opportunity)
		return nil
	})
	if err != nil {
		return nil, E1StreamFacts{}, err
	}
	facts, err := ValidateStreamV2(opportunities, config)
	if err != nil {
		return nil, E1StreamFacts{}, err
	}
	return opportunities, facts, nil
}

// ValidateStreamV2 validates exact factors against raw temporal data. Peak
// concurrency is the maximum number of requests in [arrival,response], using
// OpportunityTieBreakV2 at equal instants. Settlement delay is measured from
// provider response to committed settlement, matching the production metric.
func ValidateStreamV2(opportunities []OpportunityV2, config ConfigV2) (E1StreamFacts, error) {
	if err := config.Validate(); err != nil {
		return E1StreamFacts{}, fmt.Errorf("E1 v2 config: %w", err)
	}
	if len(opportunities) == 0 {
		return E1StreamFacts{}, errors.New("E1 v2 stream is empty")
	}
	virtualStart, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	requestIDs := map[string]struct{}{}
	preOutcomeIDs := map[string]struct{}{}
	sourceRows := map[int64]struct{}{}
	windowBudgets := map[string]int64{}
	cohortSlots := map[string]map[int64]string{}
	batchSizes := map[string]int64{}
	events := make([]opportunityEventV2, 0, len(opportunities)*4)
	var firstArrival, lastArrival, lastSettlement time.Time

	for index, opportunity := range opportunities {
		if err := opportunity.Validate(); err != nil {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d: %w", index+1, err)
		}
		if opportunity.Sequence != int64(index) || opportunity.Sequence > 1_000_000_000 {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d must have contiguous sequence %d", index+1, index)
		}
		if opportunity.MaxOutputTokens > config.VerifiedOutputCapTokens {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d exceeds configured verified output cap", index+1)
		}
		if _, duplicate := requestIDs[opportunity.RequestID]; duplicate {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d duplicates request_id %q", index+1, opportunity.RequestID)
		}
		requestIDs[opportunity.RequestID] = struct{}{}
		if _, duplicate := preOutcomeIDs[opportunity.PreOutcomeID]; duplicate {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d duplicates preoutcome_id %q", index+1, opportunity.PreOutcomeID)
		}
		if _, duplicate := sourceRows[opportunity.SourceRow]; duplicate {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d duplicates source_row %d", index+1, opportunity.SourceRow)
		}
		preOutcomeIDs[opportunity.PreOutcomeID] = struct{}{}
		sourceRows[opportunity.SourceRow] = struct{}{}

		windowKey := opportunity.TenantID + "\x00" + opportunity.BudgetWindowID
		if prior, ok := windowBudgets[windowKey]; ok && prior != opportunity.BudgetMicros {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d changes immutable tenant-window budget", index+1)
		}
		windowBudgets[windowKey] = opportunity.BudgetMicros
		if opportunity.CohortID != "" {
			key := cohortKey(opportunity.TenantID, opportunity.BudgetWindowID, opportunity.CohortID)
			slots := cohortSlots[key]
			if slots == nil {
				slots = map[int64]string{}
				cohortSlots[key] = slots
			}
			if prior, duplicate := slots[opportunity.CohortIndex]; duplicate {
				return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d reuses cohort slot from request %q", index+1, prior)
			}
			slots[opportunity.CohortIndex] = opportunity.RequestID
		}

		times, _ := opportunity.times()
		if times.arrival.Before(virtualStart) {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d arrives before virtual_start", index+1)
		}
		if index > 0 && times.arrival.Before(lastArrival) {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d regresses absolute arrival time", index+1)
		}
		delay := times.settlement.Sub(times.providerResponse)
		wantDelay := time.Duration(config.SettlementDelaySeconds) * time.Second
		if delay != wantDelay {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream row %d settlement delay is %s, factor requires %s", index+1, delay, wantDelay)
		}
		if index == 0 {
			firstArrival = times.arrival
		}
		lastArrival = times.arrival
		if times.settlement.After(lastSettlement) {
			lastSettlement = times.settlement
		}
		batchSizes[opportunity.ArrivalAt]++
		events = append(events,
			opportunityEventV2{at: times.arrival, kind: eventArrivalV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.providerResponse, kind: eventProviderResponseV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.usageAvailable, kind: eventUsageAvailableV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
			opportunityEventV2{at: times.settlement, kind: eventSettlementV2, sequence: opportunity.Sequence, requestID: opportunity.RequestID},
		)
	}

	registry := map[string]int64{}
	for _, cohort := range config.Cohorts {
		registry[cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)] = cohort.Size
	}
	for key, slots := range cohortSlots {
		size, declared := registry[key]
		if !declared {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream cohort %q is absent from config registry", key)
		}
		if int64(len(slots)) != size {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 stream cohort %q is incomplete: got %d want %d", key, len(slots), size)
		}
		for index := int64(0); index < size; index++ {
			if _, ok := slots[index]; !ok {
				return E1StreamFacts{}, fmt.Errorf("E1 v2 stream cohort %q is missing slot %d", key, index)
			}
		}
	}
	for key := range registry {
		if _, ok := cohortSlots[key]; !ok {
			return E1StreamFacts{}, fmt.Errorf("E1 v2 config cohort %q has no stream rows", key)
		}
	}

	orderedOpportunityEventsV2(events)
	active := map[string]struct{}{}
	var activeCount, peak int64
	for _, event := range events {
		switch event.kind {
		case eventArrivalV2:
			if _, duplicate := active[event.requestID]; duplicate {
				return E1StreamFacts{}, fmt.Errorf("E1 v2 request %q has duplicate active arrival", event.requestID)
			}
			active[event.requestID] = struct{}{}
			activeCount++
			if activeCount > peak {
				peak = activeCount
			}
		case eventProviderResponseV2:
			if _, exists := active[event.requestID]; !exists {
				return E1StreamFacts{}, fmt.Errorf("E1 v2 request %q completes outside its active interval", event.requestID)
			}
			delete(active, event.requestID)
			activeCount--
		}
	}
	if activeCount != 0 || len(active) != 0 {
		return E1StreamFacts{}, errors.New("E1 v2 concurrency sweep did not terminate at zero")
	}
	if E1Concurrency(peak) != config.Concurrency {
		return E1StreamFacts{}, fmt.Errorf("E1 v2 observed peak concurrency %d does not match factor %d", peak, config.Concurrency)
	}

	var maximumBatch int64
	for _, size := range batchSizes {
		if size > maximumBatch {
			maximumBatch = size
		}
	}
	return E1StreamFacts{
		RecordCount: int64(len(opportunities)), ArrivalBatchCount: int64(len(batchSizes)),
		MaximumArrivalBatchSize: maximumBatch, ObservedPeakConcurrency: E1Concurrency(peak),
		FirstArrivalAt:   firstArrival.UTC().Format(time.RFC3339Nano),
		LastSettlementAt: lastSettlement.UTC().Format(time.RFC3339Nano), TieBreakPolicy: OpportunityTieBreakV2,
	}, nil
}

func orderedOpportunityEventsV2(events []opportunityEventV2) {
	sort.Slice(events, func(i, j int) bool {
		if !events[i].at.Equal(events[j].at) {
			return events[i].at.Before(events[j].at)
		}
		if events[i].kind != events[j].kind {
			return events[i].kind < events[j].kind
		}
		if events[i].sequence != events[j].sequence {
			return events[i].sequence < events[j].sequence
		}
		return events[i].requestID < events[j].requestID
	})
}
