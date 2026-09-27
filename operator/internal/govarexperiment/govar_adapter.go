package govarexperiment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const experimentCohortAuthorityID = "govar-experiment-authority-v1"

type GOVARProspectiveSample struct {
	RequestID         string    `json:"request_id"`
	ProviderAttemptID string    `json:"provider_attempt_id"`
	OpportunityID     string    `json:"opportunity_id"`
	OutputTokens      int64     `json:"output_tokens"`
	SettledAt         time.Time `json:"settled_at"`
}

type GOVARProspectiveProfileFixture struct {
	ProfileID            string                   `json:"profile_id"`
	FeatureSchemaVersion string                   `json:"feature_schema_version"`
	TaskClass            string                   `json:"task_class"`
	TailClass            string                   `json:"tail_class"`
	AssignmentRule       string                   `json:"assignment_rule"`
	Cohorts              []CohortSpec             `json:"cohorts"`
	ArtifactVersion      string                   `json:"artifact_version"`
	MinimumSupport       int64                    `json:"minimum_support"`
	Calibration          []GOVARProspectiveSample `json:"calibration"`
	Monitoring           []GOVARProspectiveSample `json:"monitoring"`
}

type GOVARProspectiveEvidence struct {
	Profiles []GOVARProspectiveProfileFixture `json:"profiles"`
}

// BuildProspectiveGOVARMethodConfig constructs all identities from the exact
// outcome-free opportunity stream and pre-trace calibration/monitoring samples.
// It never accepts a caller-provided artifact, quantile, candidate digest, or
// publication digest.
func BuildProspectiveGOVARMethodConfig(config Config, opportunities []Opportunity, tenantRiskPPB, driftThresholdPPB,
	maxAgeSeconds, revalidationMinimumSupport int64, frozenAt time.Time, fixture GOVARProspectiveEvidence) (MethodConfig, error) {
	if len(opportunities) == 0 || tenantRiskPPB < 0 || tenantRiskPPB > 1_000_000_000 || driftThresholdPPB < 0 ||
		driftThresholdPPB > 1_000_000_000 || maxAgeSeconds <= 0 || revalidationMinimumSupport <= 0 ||
		frozenAt.IsZero() || len(fixture.Profiles) == 0 {
		return MethodConfig{}, errors.New("prospective GOV-AR builder inputs are incomplete")
	}
	frozenAt = frozenAt.UTC()
	virtualStart, err := time.Parse(time.RFC3339Nano, config.VirtualStart)
	if err != nil || !frozenAt.Before(virtualStart) {
		return MethodConfig{}, errors.New("GOV-AR freeze must precede virtual start")
	}
	runRequests := make(map[string]struct{}, len(opportunities))
	groups := map[string][]Opportunity{}
	for _, op := range opportunities {
		if err := op.Validate(); err != nil {
			return MethodConfig{}, err
		}
		if _, duplicate := runRequests[op.RequestID]; duplicate {
			return MethodConfig{}, errors.New("prospective GOV-AR stream contains a duplicate request")
		}
		runRequests[op.RequestID] = struct{}{}
		groups[cohortKey(op.TenantID, op.BudgetWindowID, op.CohortID)] = append(groups[cohortKey(op.TenantID, op.BudgetWindowID, op.CohortID)], op)
	}
	if len(groups) != len(config.Cohorts) {
		return MethodConfig{}, errors.New("prospective GOV-AR stream does not exactly match the cohort registry")
	}
	candidate, err := experimentCandidateBinding(config)
	if err != nil {
		return MethodConfig{}, err
	}
	candidates := []GOVARCandidateBinding{candidate}
	candidateSetSHA, err := GOVARCandidateSetDigest(candidates)
	if err != nil {
		return MethodConfig{}, err
	}
	joint := GOVARJointSelectionBinding{Objective: "cost", QualityWeightPPB: 0, CostWeightPPB: 1_000_000_000,
		SwitchPenaltyMicros: 0, TieBreak: "reserved_cost_then_model_ref", CandidateSetSHA256: candidateSetSHA,
		SelectedCandidateID: candidate.CandidateID, PriorCandidateID: ""}
	joint.ArtifactSHA256, err = GOVARJointSelectionDigest(joint)
	if err != nil {
		return MethodConfig{}, err
	}
	method := GOVARMethodConfig{TenantRiskPPB: tenantRiskPPB, FrozenAt: frozenAt.Format(time.RFC3339Nano),
		Fallback: "strict_provider_cap", DriftThresholdPPB: driftThresholdPPB, MaxAgeSeconds: maxAgeSeconds,
		RevalidationMinimumSupport: revalidationMinimumSupport, CandidateSet: candidates,
		CandidateSetSHA256: candidateSetSHA, JointSelection: joint}
	registry := map[string]int64{}
	for _, cohort := range config.Cohorts {
		registry[cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)] = cohort.Size
	}
	profileByCohort := map[string]GOVARCalibrationProfile{}
	fixtureByProfile := map[string]GOVARProspectiveProfileFixture{}
	evidenceIdentities := map[string]string{}
	for _, profileFixture := range fixture.Profiles {
		if !validIdentity(profileFixture.ProfileID) || !validIdentity(profileFixture.FeatureSchemaVersion) ||
			!validIdentity(profileFixture.TaskClass) || !validIdentity(profileFixture.TailClass) || !validIdentity(profileFixture.AssignmentRule) ||
			!validIdentity(profileFixture.ArtifactVersion) || profileFixture.MinimumSupport <= 0 || len(profileFixture.Calibration) == 0 ||
			len(profileFixture.Monitoring) == 0 || len(profileFixture.Cohorts) == 0 {
			return MethodConfig{}, errors.New("prospective profile fixture is incomplete")
		}
		definitionRaw, _ := json.Marshal(struct {
			Schema               string `json:"schema"`
			ProfileID            string `json:"profile_id"`
			FeatureSchemaVersion string `json:"feature_schema_version"`
			TaskClass            string `json:"task_class"`
			TailClass            string `json:"tail_class"`
			AssignmentRule       string `json:"assignment_rule"`
		}{"govar-p1-calibration-profile-definition-v1", profileFixture.ProfileID, profileFixture.FeatureSchemaVersion,
			profileFixture.TaskClass, profileFixture.TailClass, profileFixture.AssignmentRule})
		profile := GOVARCalibrationProfile{ProfileID: profileFixture.ProfileID, Scenario: config.Scenario,
			FeatureSchemaVersion: profileFixture.FeatureSchemaVersion, TaskClass: profileFixture.TaskClass,
			TailClass: profileFixture.TailClass, AssignmentRule: profileFixture.AssignmentRule,
			DefinitionSHA256: DomainHash("govar-p1-calibration-profile-definition-v1", definitionRaw), CandidateSetSHA256: candidateSetSHA,
			AssignedCohorts: append([]CohortSpec(nil), profileFixture.Cohorts...)}
		sort.Slice(profile.AssignedCohorts, func(i, j int) bool {
			return cohortKey(profile.AssignedCohorts[i].TenantID, profile.AssignedCohorts[i].BudgetWindowID, profile.AssignedCohorts[i].CohortID) <
				cohortKey(profile.AssignedCohorts[j].TenantID, profile.AssignedCohorts[j].BudgetWindowID, profile.AssignedCohorts[j].CohortID)
		})
		profile.ProfileSHA256, err = GOVARCalibrationProfileDigest(profile)
		if err != nil {
			return MethodConfig{}, err
		}
		if _, duplicate := fixtureByProfile[profile.ProfileSHA256]; duplicate {
			return MethodConfig{}, errors.New("prospective profile fixture is duplicate")
		}
		fixtureByProfile[profile.ProfileSHA256] = profileFixture
		method.CalibrationProfiles = append(method.CalibrationProfiles, profile)
		for _, cohort := range profileFixture.Cohorts {
			key := cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)
			if registry[key] != cohort.Size || cohort.Size <= 0 {
				return MethodConfig{}, fmt.Errorf("profile %s names an absent or size-mismatched cohort", profile.ProfileID)
			}
			if _, duplicate := profileByCohort[key]; duplicate {
				return MethodConfig{}, fmt.Errorf("cohort %q has ambiguous prospective profiles", key)
			}
			profileByCohort[key] = profile
		}
		for _, sample := range append(append([]GOVARProspectiveSample(nil), profileFixture.Calibration...), profileFixture.Monitoring...) {
			key := sample.RequestID + "\x00" + sample.ProviderAttemptID
			if _, duplicate := evidenceIdentities[key]; duplicate {
				return MethodConfig{}, errors.New("prospective evidence identity is reused across profiles or splits")
			}
			evidenceIdentities[key] = profile.ProfileSHA256
		}
	}
	if len(profileByCohort) != len(registry) {
		return MethodConfig{}, errors.New("prospective profile assignment does not cover every cohort exactly once")
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ops := groups[key]
		sort.Slice(ops, func(i, j int) bool { return ops[i].CohortIndex < ops[j].CohortIndex })
		size := int64(len(ops))
		if registry[key] != size || size <= 0 || 1_000_000_000%size != 0 {
			return MethodConfig{}, fmt.Errorf("cohort %q is incomplete or cannot use exact uniform PPB weights", key)
		}
		profile := profileByCohort[key]
		profileFixture := fixtureByProfile[profile.ProfileSHA256]
		weight := 1_000_000_000 / size
		for index, op := range ops {
			if op.CohortIndex != int64(index) {
				return MethodConfig{}, fmt.Errorf("cohort %q is missing slot %d", key, index)
			}
			risk := tenantRiskPPB * weight / 1_000_000_000
			method.SlotBounds = append(method.SlotBounds, GOVARSlotBound{TenantID: op.TenantID, BudgetWindowID: op.BudgetWindowID,
				CohortID: op.CohortID, CohortIndex: op.CohortIndex, RequestID: op.RequestID,
				OpportunityDigest: GOVAROpportunityDigest(config, op), WeightPPB: weight, AllocatedRiskPPB: risk,
				CoverageTargetPPB: 1_000_000_000 - risk, CalibrationProfileSHA256: profile.ProfileSHA256})
		}
		cohortSlots := method.SlotBounds[len(method.SlotBounds)-len(ops):]
		calibrationFixtureSHA, err := prospectiveSampleDigest(profileFixture.Calibration)
		if err != nil {
			return MethodConfig{}, err
		}
		featureRegime, err := govarcalibration.CanonicalFeatureRegime(profile.FeatureSchemaVersion, profile.DefinitionSHA256)
		if err != nil {
			return MethodConfig{}, err
		}
		splitRegime, err := govarcalibration.CanonicalSplitOpportunityRegime(profile.DefinitionSHA256, calibrationFixtureSHA,
			govarcalibration.SplitCalibration, profile.ProfileSHA256)
		if err != nil {
			return MethodConfig{}, err
		}
		regimes := govarcalibration.RegimeDigests{FeatureSHA256: featureRegime, PriceSHA256: candidate.PriceRegimeSHA256,
			CapPathAdapterSHA256: candidate.CapRegimeSHA256, SplitOpportunitySHA256: splitRegime,
			CohortSHA256: profile.ProfileSHA256, ProducerSoftwareSHA256: config.SoftwareSHA256}
		calibrationRows, err := prospectiveRows(profileFixture.Calibration, govarcalibration.SplitCalibration, regimes, frozenAt, runRequests, candidate.VerifiedOutputCapTokens)
		if err != nil {
			return MethodConfig{}, err
		}
		monitoringRows, err := prospectiveRows(profileFixture.Monitoring, govarcalibration.SplitMonitoring, regimes, frozenAt, runRequests, candidate.VerifiedOutputCapTokens)
		if err != nil {
			return MethodConfig{}, err
		}
		target := cohortSlots[0].CoverageTargetPPB
		var evidence GOVARCalibrationEvidence
		found := false
		for _, existing := range method.EvidenceRegistry {
			if existing.CalibrationProfileSHA256 == profile.ProfileSHA256 && existing.Calibration.CoverageTargetPPB == target {
				evidence, found = existing, true
				break
			}
		}
		if !found {
			artifact, buildErr := govarcalibration.BuildAuthoritativeArtifact(govarcalibration.AuthoritativeBuildConfig{
				ArtifactRef: "p1-" + profile.ProfileSHA256[:20] + fmt.Sprintf("-%d", target), Version: profileFixture.ArtifactVersion,
				FeatureSchemaVersion: profileFixture.FeatureSchemaVersion, CoverageTargetPPB: target,
				MinimumSupport: profileFixture.MinimumSupport, Regimes: regimes}, calibrationRows)
			if buildErr != nil {
				return MethodConfig{}, buildErr
			}
			drift, buildErr := govarcalibration.BuildAuthoritativeDriftWindow(artifact, regimes, driftThresholdPPB, monitoringRows)
			if buildErr != nil {
				return MethodConfig{}, buildErr
			}
			evidence = GOVARCalibrationEvidence{CalibrationProfileSHA256: profile.ProfileSHA256, Calibration: artifact,
				CalibrationRows: calibrationRows, Drift: drift, MonitoringRows: monitoringRows}
			evidence.PublicationSHA256, err = GOVAREvidencePublicationDigest(evidence, candidateSetSHA, joint.ArtifactSHA256)
			if err != nil {
				return MethodConfig{}, err
			}
			method.EvidenceRegistry = append(method.EvidenceRegistry, evidence)
		}
		for index := range cohortSlots {
			method.SlotBounds[len(method.SlotBounds)-len(ops)+index].EvidencePublicationSHA256 = evidence.PublicationSHA256
		}
	}
	result := MethodConfig{SchemaVersion: MethodConfigSchema, ProtocolID: "gov_ar", GOVAR: &method}
	if err := result.Validate(); err != nil {
		return MethodConfig{}, err
	}
	return result, nil
}

func prospectiveSampleDigest(samples []GOVARProspectiveSample) (string, error) {
	normalized := append([]GOVARProspectiveSample(nil), samples...)
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].RequestID != normalized[j].RequestID {
			return normalized[i].RequestID < normalized[j].RequestID
		}
		return normalized[i].ProviderAttemptID < normalized[j].ProviderAttemptID
	})
	for index := range normalized {
		normalized[index].SettledAt = normalized[index].SettledAt.UTC()
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return SHA256(raw), nil
}

func prospectiveRows(samples []GOVARProspectiveSample, split string, regimes govarcalibration.RegimeDigests,
	frozenAt time.Time, runRequests map[string]struct{}, verifiedCap int64) ([]govarcalibration.AuthoritativeObservation, error) {
	rows := make([]govarcalibration.AuthoritativeObservation, 0, len(samples))
	seen := map[string]struct{}{}
	for _, sample := range samples {
		if !validIdentity(sample.RequestID) || !validIdentity(sample.ProviderAttemptID) || !validIdentity(sample.OpportunityID) ||
			sample.OutputTokens < 0 || sample.OutputTokens > verifiedCap || sample.SettledAt.IsZero() || !sample.SettledAt.UTC().Before(frozenAt) {
			return nil, errors.New("prospective calibration/monitoring sample is incomplete or not pre-freeze")
		}
		if _, collision := runRequests[sample.RequestID]; collision {
			return nil, errors.New("prospective evidence reuses a run request identity")
		}
		key := sample.RequestID + "\x00" + sample.ProviderAttemptID
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.New("prospective evidence contains a duplicate request/attempt")
		}
		seen[key] = struct{}{}
		rows = append(rows, govarcalibration.AuthoritativeObservation{RequestID: sample.RequestID,
			ProviderAttemptID: sample.ProviderAttemptID, OpportunityID: sample.OpportunityID, Split: split,
			OutputTokens: sample.OutputTokens, SettledAt: sample.SettledAt.UTC(), Regimes: regimes})
	}
	return rows, nil
}

func experimentCandidateBinding(config Config) (GOVARCandidateBinding, error) {
	candidate := experimentCandidate(config)
	priceRegime, err := govarcalibration.CanonicalPriceRegime(candidate.PricingSnapshot.SnapshotSHA256,
		candidate.RouteSnapshot.ProviderUID, candidate.RouteSnapshot.ProviderDeployment)
	if err != nil {
		return GOVARCandidateBinding{}, err
	}
	capRegime, err := govar.CanonicalCandidateCapPathAdapterRegime(candidate)
	if err != nil {
		return GOVARCandidateBinding{}, err
	}
	feasibilityRaw, _ := json.Marshal(struct {
		Schema                  string `json:"schema"`
		CandidateID             string `json:"candidate_id"`
		RouteSnapshotSHA256     string `json:"route_snapshot_sha256"`
		CapEvidenceSHA256       string `json:"cap_evidence_sha256"`
		CapRegimeSHA256         string `json:"cap_regime_sha256"`
		VerifiedOutputCapTokens int64  `json:"verified_output_cap_tokens"`
		ContextWindow           int64  `json:"context_window"`
		HardFeasible            bool   `json:"hard_feasible"`
	}{"govar-p1-hard-feasibility-v1", candidate.ModelRef, candidate.RouteSnapshot.SnapshotHash,
		candidate.CapEvidenceDigest, capRegime, candidate.VerifiedOutputCapTokens, candidate.ContextWindow, candidate.Feasible})
	return GOVARCandidateBinding{
		CandidateID: candidate.ModelRef, RouteSnapshotSHA256: candidate.RouteSnapshot.SnapshotHash,
		PricingSnapshotSHA256: candidate.PricingSnapshot.SnapshotSHA256, PriceRegimeSHA256: priceRegime,
		CapEvidenceSHA256: candidate.CapEvidenceDigest, CapRegimeSHA256: capRegime,
		InputPriceMicrosPerMillion:  candidate.InputPriceMicrosPerMillion,
		OutputPriceMicrosPerMillion: candidate.OutputPriceMicrosPerMillion,
		VerifiedOutputCapTokens:     candidate.VerifiedOutputCapTokens, HardFeasible: candidate.Feasible,
		FeasibilitySHA256: DomainHash("govar-p1-hard-feasibility-v1", feasibilityRaw),
	}, nil
}

// GOVAROpportunityDigest is the one public construction used both by the
// prospective P1 config producer and the runner. It contains no outcome.
func GOVAROpportunityDigest(config Config, op Opportunity) string {
	return govar.OpportunityDigest(govarAdmitRequest(op, op.TenantID))
}

func govarAdmitRequest(op Opportunity, tenant string) govar.AdmitRequest {
	return govar.AdmitRequest{
		RequestID: op.RequestID, Namespace: "experiment", TenantID: tenant, WorkloadUID: op.WorkloadUID,
		AuthenticatedTenantID: tenant, AuthenticatedWorkloadUID: op.WorkloadUID, AuthenticatedNamespace: "experiment",
		BudgetPolicyName: "experiment-budget-" + op.TenantID, RoutingPolicyName: "experiment-routing-" + op.TenantID,
		InputTokens: op.InputTokens, InputTokensExact: true, MaxOutputTokens: op.MaxOutputTokens,
		CohortID: op.CohortID, CohortIndex: op.CohortIndex,
	}
}

func govarCohortDigests(slots []GOVARSlotBound) (string, string, error) {
	normalized := append([]GOVARSlotBound(nil), slots...)
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].CohortIndex < normalized[j].CohortIndex })
	type membership struct {
		Index             int64  `json:"index"`
		RequestID         string `json:"request_id"`
		OpportunityDigest string `json:"opportunity_digest"`
	}
	type weight struct {
		Index     int64 `json:"index"`
		WeightPPB int64 `json:"weight_ppb"`
	}
	members := make([]membership, 0, len(normalized))
	weights := make([]weight, 0, len(normalized))
	for index, slot := range normalized {
		if slot.CohortIndex != int64(index) {
			return "", "", errors.New("cohort slots are not complete")
		}
		members = append(members, membership{slot.CohortIndex, slot.RequestID, slot.OpportunityDigest})
		weights = append(weights, weight{slot.CohortIndex, slot.WeightPPB})
	}
	membershipRaw, _ := json.Marshal(struct {
		Schema string       `json:"schema"`
		Rows   []membership `json:"rows"`
	}{"govar-p1-cohort-membership-v1", members})
	weightsRaw, _ := json.Marshal(struct {
		Schema string   `json:"schema"`
		Rows   []weight `json:"rows"`
	}{"govar-p1-cohort-weights-v1", weights})
	return DomainHash("govar-p1-cohort-membership-v1", membershipRaw), DomainHash("govar-p1-cohort-weights-v1", weightsRaw), nil
}

func validateGOVARConfig(config Config) error {
	if config.MethodConfig.GOVAR == nil {
		return errors.New("GOV-AR method config is absent")
	}
	method := config.MethodConfig.GOVAR
	evidenceByPublication := make(map[string]GOVARCalibrationEvidence, len(method.EvidenceRegistry))
	for _, evidence := range method.EvidenceRegistry {
		evidenceByPublication[evidence.PublicationSHA256] = evidence
	}
	for _, profile := range method.CalibrationProfiles {
		if profile.Scenario != config.Scenario || profile.CandidateSetSHA256 != method.CandidateSetSHA256 {
			return errors.New("GOV-AR calibration profile is not bound to this pre-outcome scenario/candidate set")
		}
	}
	expectedCandidate, err := experimentCandidateBinding(config)
	if err != nil || len(method.CandidateSet) != 1 || !reflect.DeepEqual(method.CandidateSet[0], expectedCandidate) {
		return errors.New("GOV-AR candidate set does not match the exact production candidate price/cap regime")
	}
	virtualStart, _ := time.Parse(time.RFC3339Nano, config.VirtualStart)
	frozenAt, _ := time.Parse(time.RFC3339Nano, method.FrozenAt)
	if !frozenAt.Before(virtualStart) {
		return errors.New("GOV-AR cohort was not frozen before the trace")
	}
	registry := make(map[string]int64, len(config.Cohorts))
	for _, cohort := range config.Cohorts {
		registry[cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)] = cohort.Size
	}
	profileRegistry := make(map[string]int64, len(registry))
	for _, profile := range method.CalibrationProfiles {
		for _, cohort := range profile.AssignedCohorts {
			key := cohortKey(cohort.TenantID, cohort.BudgetWindowID, cohort.CohortID)
			if _, duplicate := profileRegistry[key]; duplicate {
				return fmt.Errorf("GOV-AR calibration profiles duplicate assigned cohort %q", key)
			}
			profileRegistry[key] = cohort.Size
		}
	}
	if len(profileRegistry) != len(registry) {
		return errors.New("GOV-AR calibration-profile cohort assignments do not exactly match the config registry")
	}
	for key, size := range registry {
		if assignedSize, ok := profileRegistry[key]; !ok || assignedSize != size {
			return fmt.Errorf("GOV-AR assigned cohort %q size %d does not match config registry size %d", key, assignedSize, size)
		}
	}
	groups := map[string][]GOVARSlotBound{}
	for _, slot := range method.SlotBounds {
		key := cohortKey(slot.TenantID, slot.BudgetWindowID, slot.CohortID)
		groups[key] = append(groups[key], slot)
		evidence, evidenceExists := evidenceByPublication[slot.EvidencePublicationSHA256]
		if !evidenceExists || evidence.Calibration.ProducerSoftwareSHA256 != config.SoftwareSHA256 ||
			evidence.Calibration.UpperOutputTokens > config.VerifiedOutputCapTokens ||
			!evidence.Calibration.WindowEnd.Before(virtualStart) || !evidence.Drift.Result.WindowEnd.Before(virtualStart) {
			return fmt.Errorf("GOV-AR slot %s/%d has post-trace, wrong-software, or over-cap evidence", key, slot.CohortIndex)
		}
	}
	if len(groups) != len(registry) {
		return errors.New("GOV-AR slot cohorts do not exactly match the config registry")
	}
	for key, size := range registry {
		slots, ok := groups[key]
		if !ok || int64(len(slots)) != size {
			return fmt.Errorf("GOV-AR cohort %q has incomplete slot evidence", key)
		}
		for _, slot := range slots {
			evidence := evidenceByPublication[slot.EvidencePublicationSHA256]
			if evidence.Calibration.CohortRegimeSHA256 != slot.CalibrationProfileSHA256 ||
				evidence.Drift.Regimes.CohortSHA256 != slot.CalibrationProfileSHA256 {
				return fmt.Errorf("GOV-AR cohort %q calibration is not bound to its prospective profile", key)
			}
		}
	}
	return nil
}

func prepareGOVAREngine(engine *govar.Engine, config Config, opportunities []Opportunity) error {
	if config.ComparatorMethod != "gov_ar" {
		return nil
	}
	if err := validateGOVARConfig(config); err != nil {
		return err
	}
	method := config.MethodConfig.GOVAR
	for _, op := range opportunities {
		slot, err := method.Slot(op.TenantID, op.BudgetWindowID, op.CohortID, op.CohortIndex, method.CandidateSetSHA256)
		if err != nil || slot.RequestID != op.RequestID || slot.OpportunityDigest != GOVAROpportunityDigest(config, op) {
			return fmt.Errorf("GOV-AR opportunity %s does not match its immutable slot: %w", op.RequestID, err)
		}
	}
	authorityKey := []byte(DomainHash("govar-experiment-cohort-authority-key-v1", []byte(config.MethodConfigSHA256), []byte(config.SourceSHA256)))
	if err := engine.ConfigureLedgerAuthority(experimentCohortAuthorityID, authorityKey); err != nil {
		return err
	}
	frozenAt, _ := time.Parse(time.RFC3339Nano, method.FrozenAt)
	groups := map[string][]GOVARSlotBound{}
	for _, slot := range method.SlotBounds {
		groups[cohortKey(slot.TenantID, slot.BudgetWindowID, slot.CohortID)] = append(groups[cohortKey(slot.TenantID, slot.BudgetWindowID, slot.CohortID)], slot)
	}
	for _, slots := range groups {
		sort.Slice(slots, func(i, j int) bool { return slots[i].CohortIndex < slots[j].CohortIndex })
		membershipSHA, weightsSHA, err := govarCohortDigests(slots)
		if err != nil {
			return err
		}
		cohort := govar.FrozenCohort{TenantID: slots[0].TenantID, CohortID: slots[0].CohortID, Size: int64(len(slots)),
			TenantRiskPPB: method.TenantRiskPPB, DataHash: membershipSHA, ConfigHash: weightsSHA,
			ProtocolHash: config.ProtocolSHA256, FrozenAt: frozenAt, LedgerLayoutID: govar.LedgerLayoutID,
			RouteSnapshotSchema: govar.RouteSnapshotSchemaID, SoftwareHash: config.SoftwareSHA256}
		for _, slot := range slots {
			cohort.Slots = append(cohort.Slots, govar.FrozenCohortSlot{Index: slot.CohortIndex, RequestID: slot.RequestID,
				OpportunityDigest: slot.OpportunityDigest, WeightPPB: slot.WeightPPB})
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

func bindGOVARContext(ctx *tenantContext, config Config, op Opportunity) (GOVARSlotBound, error) {
	method := config.MethodConfig.GOVAR
	slot, err := method.Slot(op.TenantID, op.BudgetWindowID, op.CohortID, op.CohortIndex, method.CandidateSetSHA256)
	if err != nil {
		return GOVARSlotBound{}, err
	}
	cohort, err := ctx.engine.ExportFrozenCohort(context.Background(), op.TenantID, op.CohortID)
	if err != nil {
		return GOVARSlotBound{}, err
	}
	evidence, err := method.Evidence(slot.EvidencePublicationSHA256)
	if err != nil {
		return GOVARSlotBound{}, err
	}
	a, d := evidence.Calibration, evidence.Drift
	now, err := time.Parse(time.RFC3339Nano, config.VirtualStart)
	if err != nil {
		return GOVARSlotBound{}, err
	}
	routing := ctx.routing
	routing.Generation = 1
	routing.Status.ObservedGeneration = 1
	routing.Status.LastEvaluatedAt = &metav1.Time{Time: now}
	routing.Spec.Objective = method.JointSelection.Objective
	routing.Spec.GOVAR.Reservation.Method = aiopsv1alpha1.GOVARReservationFixedCohort
	routing.Spec.GOVAR.Calibration = &aiopsv1alpha1.GOVARCalibrationPolicy{
		ArtifactRef: a.ArtifactRef, ArtifactSHA256: a.ArtifactSHA256, CalibrationInputSHA256: a.SourceObservationsSHA256,
		RegistryID: "experiment-slot-" + slot.CohortID, Version: a.Version, FeatureSchemaVersion: a.FeatureSchemaVersion,
		PriceRegimeSHA256: a.PriceRegimeSHA256, CapRegimeSHA256: a.CapRegimeSHA256,
		ProducerSoftwareSHA256: a.ProducerSoftwareSHA256, CoverageTargetPPB: a.CoverageTargetPPB,
		MinimumSupport: a.MinimumSupport, MaxAgeSeconds: method.MaxAgeSeconds,
	}
	routing.Spec.GOVAR.Drift = aiopsv1alpha1.GOVARDriftPolicy{Detector: "coverage-gap", ThresholdPPB: method.DriftThresholdPPB,
		Fallback: "strict_provider_cap", RevalidationMinimumSupport: method.RevalidationMinimumSupport}
	routing.Spec.GOVAR.Cohort = &aiopsv1alpha1.GOVARCohortPolicy{RegistryRef: cohort.RegistryDigest, Size: cohort.Size,
		OpportunitySetHash: cohort.DataHash, WeightsHash: cohort.ConfigHash, FrozenAt: metav1.NewTime(cohort.FrozenAt)}
	routing.Spec.GOVAR.Risk = &aiopsv1alpha1.GOVARRiskPolicy{TenantRiskPPB: method.TenantRiskPPB, Allocation: "fixed-weights"}
	routing.Status.GOVAR = &aiopsv1alpha1.GOVARRoutingPolicyStatus{
		Calibration: &aiopsv1alpha1.GOVARCalibrationStatus{EvidenceSource: "postgresql-v8", PublicationSHA256: evidence.PublicationSHA256,
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
			CalibrationWindowStart: metav1.NewTime(a.WindowStart), CalibrationWindowEnd: metav1.NewTime(a.WindowEnd), ObservedAt: metav1.NewTime(now)},
		Drift: &aiopsv1alpha1.GOVARDriftStatus{DriftSHA256: d.DriftSHA256, Detected: d.Result.Detected,
			ConservativeMode: d.Result.Detected, Detector: "coverage-gap", ThresholdPPB: d.Result.ThresholdPPB,
			MonitoringInputSHA256: d.Result.MonitoringInputSHA256, Support: d.Result.Support,
			EmpiricalCoveragePPB: d.Result.EmpiricalCoveragePPB, ConfidencePPB: d.Result.ConfidencePPB,
			IntervalLowerPPB: d.Result.IntervalLowerPPB, IntervalUpperPPB: d.Result.IntervalUpperPPB,
			MonitoringWindowStart: metav1.NewTime(d.Result.WindowStart), MonitoringWindowEnd: metav1.NewTime(d.Result.WindowEnd),
			ObservedAt: metav1.NewTime(now)},
	}
	ctx.routing = routing
	return slot, nil
}
