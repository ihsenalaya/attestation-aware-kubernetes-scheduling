package govarexperiment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

const (
	UsageObservationSchemaV2 = "govar-e1-usage-observation-v2"
	UsageReceiptSchemaV2     = "govar-e1-usage-receipt-v2"
	UsageBindingSchemaV2     = "govar-e1-usage-binding-v2"
	UsageProducerSchemaV2    = "govar-e1-azure-usage-producer-evidence-v2"
	UsageAccessSchemaV2      = "govar-e1-usage-access-v2"
)

// UsageObservationV2 contains only metered token use. In particular, the
// Azure development pilot is not an authority for a quality or reward score.
type UsageObservationV2 struct {
	SchemaVersion      string `json:"schema_version"`
	RecordType         string `json:"record_type"`
	RequestID          string `json:"request_id"`
	UsageItemID        string `json:"usage_item_id"`
	SourceRow          int64  `json:"source_row"`
	SourceTimestamp    string `json:"source_timestamp"`
	ContextTokens      int64  `json:"context_tokens"`
	PreOutcomeID       string `json:"preoutcome_id"`
	ActualInputTokens  int64  `json:"actual_input_tokens"`
	ActualOutputTokens int64  `json:"actual_output_tokens"`
}

func (o UsageObservationV2) Validate() error {
	if o.SchemaVersion != UsageObservationSchemaV2 || o.RecordType != "usage_observation" {
		return errors.New("unsupported E1 v2 usage observation schema or record type")
	}
	if !identityPattern.MatchString(o.RequestID) {
		return errors.New("usage observation request_id is not a safe immutable identity")
	}
	if !shaPattern.MatchString(o.UsageItemID) {
		return errors.New("usage_item_id must be a lower-case SHA-256")
	}
	if o.ActualInputTokens < 0 || o.ActualOutputTokens < 0 {
		return errors.New("usage token quantities must be non-negative")
	}
	if o.ActualInputTokens != o.ContextTokens {
		return errors.New("actual_input_tokens must equal the pre-outcome context_tokens coordinate")
	}
	if err := validatePreOutcomeCoordinatesV2(o.SourceRow, o.SourceTimestamp, o.ContextTokens, o.PreOutcomeID, o.UsageItemID); err != nil {
		return err
	}
	return nil
}

// UsageBindingV2 binds the exact raw usage artifact and its canonical row set.
// BindingSHA256 is the value carried by ConfigV2. Dataset and split are fixed,
// rather than supplied by a runner caller.
type UsageBindingV2 struct {
	SchemaVersion          string             `json:"schema_version"`
	DatasetID              string             `json:"dataset_id"`
	Split                  string             `json:"split"`
	ProvenanceLock         E1ProvenanceLockV2 `json:"provenance_lock"`
	ArtifactSHA256         string             `json:"artifact_sha256"`
	ObservationSetSHA256   string             `json:"observation_set_sha256"`
	MappingSetSHA256       string             `json:"mapping_set_sha256"`
	ProducerSoftwareSHA256 string             `json:"producer_software_sha256"`
	ProducerEvidenceSHA256 string             `json:"producer_evidence_sha256"`
	BindingSHA256          string             `json:"binding_sha256"`
}

func (b UsageBindingV2) Validate() error {
	if b.SchemaVersion != UsageBindingSchemaV2 || b.DatasetID != UsageDatasetV2 || b.Split != UsageSplitV2 {
		return errors.New("unsupported E1 v2 usage binding")
	}
	if err := b.ProvenanceLock.Validate(); err != nil {
		return fmt.Errorf("usage binding provenance lock: %w", err)
	}
	for name, value := range map[string]string{
		"artifact_sha256": b.ArtifactSHA256, "observation_set_sha256": b.ObservationSetSHA256,
		"mapping_set_sha256": b.MappingSetSHA256, "producer_software_sha256": b.ProducerSoftwareSHA256,
		"producer_evidence_sha256": b.ProducerEvidenceSHA256, "binding_sha256": b.BindingSHA256,
	} {
		if !shaPattern.MatchString(value) {
			return fmt.Errorf("%s must be a lower-case SHA-256", name)
		}
	}
	if b.BindingSHA256 != usageBindingDigestV2(b) {
		return errors.New("usage binding digest does not recompute")
	}
	return nil
}

func usageBindingDigestV2(binding UsageBindingV2) string {
	return DomainHash("govar-e1-usage-binding-v2",
		[]byte(binding.SchemaVersion), []byte(binding.DatasetID), []byte(binding.Split),
		[]byte(binding.ArtifactSHA256), []byte(binding.ObservationSetSHA256),
		[]byte(binding.MappingSetSHA256), []byte(binding.ProducerSoftwareSHA256),
		[]byte(binding.ProvenanceLock.SupportMode),
		[]byte(binding.ProvenanceLock.OpportunityStreamSHA256),
		[]byte(binding.ProvenanceLock.SourceCodeBindingSHA256),
		[]byte(binding.ProvenanceLock.SourceManifestSHA256),
		[]byte(binding.ProvenanceLock.PilotUsageBindingSHA256),
		[]byte(binding.ProvenanceLock.RuntimeCapabilitySHA256),
		[]byte(binding.ProvenanceLock.MappingArtifactSHA256),
		[]byte(binding.ProvenanceLock.PilotPreOutcomeSetSHA256),
		[]byte(binding.ProvenanceLock.AssignedPreOutcomeSetSHA256),
		[]byte(binding.ProvenanceLock.StreamPreOutcomeSequenceSHA256),
		[]byte(binding.ProvenanceLock.ProtocolSHA256),
		[]byte(binding.ProvenanceLock.DesignSpecSHA256),
		[]byte(binding.ProvenanceLock.DecisionConfigTemplateSHA256),
		[]byte(binding.ProvenanceLock.DesignLockSHA256),
		[]byte(binding.ProvenanceLock.IndependentDesignReviewSHA256),
		[]byte(binding.ProvenanceLock.DesignManifestSHA256),
		[]byte(binding.ProvenanceLock.CohortRegistrySHA256),
		[]byte(binding.ProvenanceLock.ProfileExecutionContractSHA256),
		[]byte(binding.ProvenanceLock.BudgetCalibrationSHA256),
		[]byte(binding.ProvenanceLock.CalibrationArtifactSHA256),
		[]byte(binding.ProvenanceLock.CandidateSetSHA256),
		[]byte(binding.ProvenanceLock.ProfileRegistrySHA256),
		[]byte(binding.ProvenanceLock.SlotTemplateSHA256),
		[]byte(binding.ProvenanceLock.MethodBuilderBundleSHA256),
		[]byte(binding.ProvenanceLock.ExecutionIntentSHA256),
		[]byte(binding.ProvenanceLock.ExecutionSubjectSHA256),
		[]byte(binding.ProvenanceLock.IndependentAuthorizationSHA256),
		[]byte(binding.ProvenanceLock.ExecutionLockSHA256),
		[]byte(binding.ProducerEvidenceSHA256))
}

// UsageProducerEvidenceV2 is supplied by the pilot producer/controller from
// independently verified upstream artifacts. ParseDevelopmentUsageV2 compares
// the raw JSONL digest with UsageArtifactSHA256, but cannot itself establish
// that a caller-controlled file came from Azure. A production caller must
// compare these digests with the committed source manifest before constructing
// the issuer; all of them are then transitively bound into ConfigV2.
type UsageProducerEvidenceV2 struct {
	SchemaVersion          string             `json:"schema_version"`
	DatasetID              string             `json:"dataset_id"`
	Split                  string             `json:"split"`
	ProvenanceLock         E1ProvenanceLockV2 `json:"provenance_lock"`
	ProducerSoftwareSHA256 string             `json:"producer_software_sha256"`
	UsageArtifactSHA256    string             `json:"usage_artifact_sha256"`
}

func (e UsageProducerEvidenceV2) Validate() error {
	if e.SchemaVersion != UsageProducerSchemaV2 || e.DatasetID != UsageDatasetV2 || e.Split != UsageSplitV2 {
		return errors.New("unsupported E1 v2 usage producer evidence")
	}
	if err := e.ProvenanceLock.Validate(); err != nil {
		return fmt.Errorf("usage producer provenance lock: %w", err)
	}
	for name, value := range map[string]string{
		"producer_software_sha256": e.ProducerSoftwareSHA256,
		"usage_artifact_sha256":    e.UsageArtifactSHA256,
	} {
		if !shaPattern.MatchString(value) {
			return fmt.Errorf("%s must be a lower-case SHA-256", name)
		}
	}
	return nil
}

func usageProducerEvidenceDigestV2(evidence UsageProducerEvidenceV2) string {
	raw, _ := json.Marshal(evidence)
	return DomainHash("govar-e1-azure-usage-producer-evidence-v2", raw)
}

// UsageAuthorizationV2 is created only after an effective delivered dispatch.
// It carries no usage value and therefore cannot reveal a future observation.
type UsageAuthorizationV2 struct {
	RunID            string `json:"run_id"`
	RequestID        string `json:"request_id"`
	UsageItemID      string `json:"usage_item_id"`
	DispatchID       string `json:"dispatch_id"`
	ConfigSHA256     string `json:"config_sha256"`
	UsageAvailableAt string `json:"usage_available_at"`
}

// effectiveUsageDispatchProofV2 is deliberately package-private and all of its
// fields are sealed. An external issuer caller therefore cannot manufacture a
// reduced "dispatch happened" assertion: the proof can only be made here from
// the complete lifecycle record that the qualification runner just emitted.
// The issuer still recomputes the record digest and validates every causal
// identity, so the seal is not trusted as a substitute for verification.
type effectiveUsageDispatchProofV2 struct {
	record             LifecycleRecordV2
	recordDigestSHA256 string
}

func sealEffectiveUsageDispatchProofV2(record LifecycleRecordV2) effectiveUsageDispatchProofV2 {
	return effectiveUsageDispatchProofV2{
		record:             record,
		recordDigestSHA256: lifecycleRecordDigestV2(record),
	}
}

func (p effectiveUsageDispatchProofV2) validate(authorization UsageAuthorizationV2) error {
	record := p.record
	if !shaPattern.MatchString(p.recordDigestSHA256) || p.recordDigestSHA256 != lifecycleRecordDigestV2(record) {
		return errors.New("dispatch proof lifecycle record digest does not recompute")
	}
	if record.SchemaVersion != LifecycleSchemaV2 || record.RecordType != "lifecycle" ||
		record.RuntimeProvenance != RuntimeProvenanceV2 || record.EvidenceStatus != RuntimeEvidenceStatusV2 ||
		record.FinalEvidenceEligible {
		return errors.New("dispatch proof is not an exact non-final qualification lifecycle record")
	}
	if record.RunID != authorization.RunID || record.ConfigSHA256 != authorization.ConfigSHA256 ||
		record.RequestID != authorization.RequestID || record.UsageItemID != authorization.UsageItemID ||
		record.DispatchID != authorization.DispatchID {
		return errors.New("dispatch proof does not match the authorized run, config, request, item, and dispatch")
	}
	if record.EventIndex <= 0 {
		return errors.New("dispatch proof lacks a positive lifecycle event index")
	}
	if record.LifecycleEvent != "dispatch_delivered" || record.CurrentState != "DISPATCHED" || !record.Effective {
		return errors.New("usage authorization requires an effective delivered dispatch transition")
	}
	if record.ActualInputTokens != nil || record.ActualOutputTokens != nil ||
		record.ActualInputCostMicros != nil || record.ActualOutputCostMicros != nil ||
		record.ActualCostMicros != nil || record.UsageReceiptSHA256 != "" {
		return errors.New("dispatch proof lifecycle record already contains actual usage")
	}
	deliveredAt, err := parseCanonicalAbsoluteTimeV2("dispatch lifecycle timestamp", record.Timestamp)
	if err != nil {
		return err
	}
	availableAt, err := parseCanonicalAbsoluteTimeV2("usage_available_at", authorization.UsageAvailableAt)
	if err != nil {
		return err
	}
	if deliveredAt.After(availableAt) {
		return errors.New("dispatch delivery occurs after usage_available_at")
	}
	return nil
}

func (a UsageAuthorizationV2) Validate() error {
	if !identityPattern.MatchString(a.RunID) || !identityPattern.MatchString(a.RequestID) {
		return errors.New("usage authorization contains an unsafe identity")
	}
	for name, value := range map[string]string{
		"usage_item_id": a.UsageItemID, "dispatch_id": a.DispatchID, "config_sha256": a.ConfigSHA256,
	} {
		if !shaPattern.MatchString(value) {
			return fmt.Errorf("%s must be a lower-case SHA-256", name)
		}
	}
	if _, err := parseCanonicalAbsoluteTimeV2("usage_available_at", a.UsageAvailableAt); err != nil {
		return err
	}
	return nil
}

type UsageRevealRequestV2 struct {
	RunID        string `json:"run_id"`
	RequestID    string `json:"request_id"`
	UsageItemID  string `json:"usage_item_id"`
	DispatchID   string `json:"dispatch_id"`
	ConfigSHA256 string `json:"config_sha256"`
	RevealAt     string `json:"reveal_at"`
}

// UsageReceiptV2 deliberately has no selected-feedback, score, reward, or
// quality field. RevealedAt is the causal scheduler time at which use became
// visible, not a timestamp imported from the data file.
type UsageReceiptV2 struct {
	SchemaVersion      string `json:"schema_version"`
	RecordType         string `json:"record_type"`
	RunID              string `json:"run_id"`
	RequestID          string `json:"request_id"`
	UsageItemID        string `json:"usage_item_id"`
	DispatchID         string `json:"dispatch_id"`
	ConfigSHA256       string `json:"config_sha256"`
	UsageBindingSHA256 string `json:"usage_binding_sha256"`
	UsageAvailableAt   string `json:"usage_available_at"`
	RevealedAt         string `json:"revealed_at"`
	ActualInputTokens  int64  `json:"actual_input_tokens"`
	ActualOutputTokens int64  `json:"actual_output_tokens"`
	ReceiptSHA256      string `json:"receipt_sha256"`
}

func usageReceiptDigestV2(receipt UsageReceiptV2) string {
	copy := receipt
	copy.ReceiptSHA256 = ""
	raw, _ := json.Marshal(copy)
	return DomainHash("govar-e1-usage-receipt-v2", raw)
}

func (r UsageReceiptV2) Validate() error {
	if r.SchemaVersion != UsageReceiptSchemaV2 || r.RecordType != "usage_receipt" {
		return errors.New("unsupported E1 v2 usage receipt schema or record type")
	}
	if !identityPattern.MatchString(r.RunID) || !identityPattern.MatchString(r.RequestID) {
		return errors.New("usage receipt contains an unsafe identity")
	}
	for name, value := range map[string]string{
		"usage_item_id": r.UsageItemID, "dispatch_id": r.DispatchID, "config_sha256": r.ConfigSHA256,
		"usage_binding_sha256": r.UsageBindingSHA256, "receipt_sha256": r.ReceiptSHA256,
	} {
		if !shaPattern.MatchString(value) {
			return fmt.Errorf("%s must be a lower-case SHA-256", name)
		}
	}
	available, err := parseCanonicalAbsoluteTimeV2("usage_available_at", r.UsageAvailableAt)
	if err != nil {
		return err
	}
	revealed, err := parseCanonicalAbsoluteTimeV2("revealed_at", r.RevealedAt)
	if err != nil {
		return err
	}
	if revealed.Before(available) {
		return errors.New("usage receipt was revealed before usage_available_at")
	}
	if r.ActualInputTokens < 0 || r.ActualOutputTokens < 0 {
		return errors.New("usage receipt token quantities must be non-negative")
	}
	if r.ReceiptSHA256 != usageReceiptDigestV2(r) {
		return errors.New("usage receipt digest does not recompute")
	}
	return nil
}

// UsageAccessEventV2 contains identities and causal access status only. It
// never duplicates the protected token values.
type UsageAccessEventV2 struct {
	SchemaVersion string `json:"schema_version"`
	EventIndex    int64  `json:"event_index"`
	Kind          string `json:"kind"`
	RequestID     string `json:"request_id"`
	UsageItemID   string `json:"usage_item_id"`
	DispatchID    string `json:"dispatch_id"`
	At            string `json:"at"`
	Effective     bool   `json:"effective"`
	Replayed      bool   `json:"replayed"`
}

type DevelopmentUsageIssuerV2 struct {
	mu             sync.Mutex
	binding        UsageBindingV2
	observations   map[string]UsageObservationV2
	mappings       map[string]PreOutcomeMappingV2
	authorizations map[string]effectiveUsageAuthorizationV2
	receipts       map[string]UsageReceiptV2
	access         []UsageAccessEventV2
}

type effectiveUsageAuthorizationV2 struct {
	Authorization UsageAuthorizationV2
	Proof         effectiveUsageDispatchProofV2
}

// ParseDevelopmentUsageV2 is the sole raw-data entrance for the first E1-v2
// qualification slice. Unknown fields, duplicate rows, blank lines, and
// trailing JSON all fail closed.
func ParseDevelopmentUsageV2(raw, mappingRaw []byte, producerEvidence UsageProducerEvidenceV2) (*DevelopmentUsageIssuerV2, error) {
	if err := producerEvidence.Validate(); err != nil {
		return nil, err
	}
	if producerEvidence.UsageArtifactSHA256 != SHA256(raw) {
		return nil, errors.New("usage artifact does not match independently supplied producer evidence")
	}
	if producerEvidence.ProvenanceLock.MappingArtifactSHA256 != SHA256(mappingRaw) {
		return nil, errors.New("pre-outcome mapping artifact does not match the E1 provenance lock")
	}
	mappingRows, err := ParsePreOutcomeMappingV2(mappingRaw)
	if err != nil {
		return nil, err
	}
	mappings := make(map[string]PreOutcomeMappingV2, len(mappingRows))
	for _, row := range mappingRows {
		mappings[row.RequestID] = row
	}
	rows := make(map[string]UsageObservationV2)
	items := make(map[string]string)
	sourceRows := make(map[int64]string)
	err = scanCanonicalJSONLV2(raw, "usage", func(lineNumber int, line []byte) error {
		var row UsageObservationV2
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&row); err != nil {
			return err
		}
		if err := requireJSONEOF(decoder); err != nil {
			return err
		}
		if err := row.Validate(); err != nil {
			return err
		}
		if _, duplicate := rows[row.RequestID]; duplicate {
			return fmt.Errorf("duplicates request_id %q", row.RequestID)
		}
		if prior, duplicate := items[row.UsageItemID]; duplicate {
			return fmt.Errorf("reuses usage_item_id from request %q", prior)
		}
		if prior, duplicate := sourceRows[row.SourceRow]; duplicate {
			return fmt.Errorf("reuses source_row from request %q", prior)
		}
		rows[row.RequestID] = row
		items[row.UsageItemID] = row.RequestID
		sourceRows[row.SourceRow] = row.RequestID
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("usage artifact is empty")
	}
	if len(rows) != len(mappings) {
		return nil, fmt.Errorf("usage/mapping cardinality mismatch: got %d usage rows for %d mapping rows", len(rows), len(mappings))
	}
	for requestID, row := range rows {
		mapping, ok := mappings[requestID]
		if !ok || row.UsageItemID != mapping.UsageItemID || row.PreOutcomeID != mapping.PreOutcomeID ||
			row.SourceRow != mapping.SourceRow || row.SourceTimestamp != mapping.SourceTimestamp ||
			row.ContextTokens != mapping.ContextTokens || row.ActualInputTokens != mapping.InputTokens {
			return nil, fmt.Errorf("usage observation does not exactly match pre-outcome mapping for request %q", requestID)
		}
	}

	ordered := make([]UsageObservationV2, 0, len(rows))
	for _, row := range rows {
		ordered = append(ordered, row)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].RequestID < ordered[j].RequestID })
	canonical, _ := json.Marshal(struct {
		Schema string               `json:"schema"`
		Rows   []UsageObservationV2 `json:"rows"`
	}{"govar-e1-usage-observation-set-v2", ordered})
	binding := UsageBindingV2{
		SchemaVersion: UsageBindingSchemaV2, DatasetID: UsageDatasetV2, Split: UsageSplitV2,
		ProvenanceLock: producerEvidence.ProvenanceLock,
		ArtifactSHA256: SHA256(raw), ObservationSetSHA256: DomainHash("govar-e1-usage-observation-set-v2", canonical),
		MappingSetSHA256:       canonicalPreOutcomeMappingSetSHA256V2(mappingRows),
		ProducerSoftwareSHA256: producerEvidence.ProducerSoftwareSHA256,
		ProducerEvidenceSHA256: usageProducerEvidenceDigestV2(producerEvidence),
	}
	binding.BindingSHA256 = usageBindingDigestV2(binding)
	return &DevelopmentUsageIssuerV2{
		binding: binding, observations: rows, mappings: mappings, authorizations: map[string]effectiveUsageAuthorizationV2{},
		receipts: map[string]UsageReceiptV2{},
	}, nil
}

func (i *DevelopmentUsageIssuerV2) Binding() UsageBindingV2 { return i.binding }

// validateOpportunityCoverage is package-private so only audited E1-v2 code in
// this package can bind protected usage to an opportunity stream.
func (i *DevelopmentUsageIssuerV2) validateOpportunityCoverage(opportunities []OpportunityV2) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(opportunities) != len(i.observations) || len(opportunities) != len(i.mappings) {
		return fmt.Errorf("usage/opportunity cardinality mismatch: got %d usage rows for %d opportunities", len(i.observations), len(opportunities))
	}
	seenRequests := make(map[string]struct{}, len(opportunities))
	seenPreOutcomes := make(map[string]struct{}, len(opportunities))
	for _, opportunity := range opportunities {
		if _, duplicate := seenRequests[opportunity.RequestID]; duplicate {
			return fmt.Errorf("opportunity coverage duplicates request_id %q", opportunity.RequestID)
		}
		if _, duplicate := seenPreOutcomes[opportunity.PreOutcomeID]; duplicate {
			return fmt.Errorf("opportunity coverage duplicates preoutcome_id %q", opportunity.PreOutcomeID)
		}
		seenRequests[opportunity.RequestID] = struct{}{}
		seenPreOutcomes[opportunity.PreOutcomeID] = struct{}{}
		row, ok := i.observations[opportunity.RequestID]
		mapping, mappingOK := i.mappings[opportunity.RequestID]
		if !ok || !mappingOK || mapping.Opportunity() != opportunity ||
			row.UsageItemID != opportunity.UsageItemID || row.PreOutcomeID != opportunity.PreOutcomeID ||
			row.SourceRow != opportunity.SourceRow || row.SourceTimestamp != opportunity.SourceTimestamp ||
			row.ContextTokens != opportunity.ContextTokens || row.ActualInputTokens != opportunity.InputTokens {
			return fmt.Errorf("usage authority does not exactly cover request %q", opportunity.RequestID)
		}
	}
	return nil
}

func (i *DevelopmentUsageIssuerV2) appendAccess(kind, requestID, itemID, dispatchID, at string, effective, replayed bool) {
	i.access = append(i.access, UsageAccessEventV2{
		SchemaVersion: UsageAccessSchemaV2, EventIndex: int64(len(i.access) + 1), Kind: kind,
		RequestID: requestID, UsageItemID: itemID, DispatchID: dispatchID, At: at,
		Effective: effective, Replayed: replayed,
	})
}

// authorizeUsage, revealUsage, and verifyUsage form a package-private causal
// boundary. External holders may inspect Binding and the outcome-free access
// log, but cannot trigger or substitute protected usage revelation.
func (i *DevelopmentUsageIssuerV2) authorizeUsage(authorization UsageAuthorizationV2, proof effectiveUsageDispatchProofV2) error {
	if err := authorization.Validate(); err != nil {
		return err
	}
	if err := proof.validate(authorization); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	row, ok := i.observations[authorization.RequestID]
	if !ok || row.UsageItemID != authorization.UsageItemID {
		return errors.New("usage authorization is not bound to an exact observation")
	}
	effective := effectiveUsageAuthorizationV2{Authorization: authorization, Proof: proof}
	if prior, duplicate := i.authorizations[authorization.RequestID]; duplicate {
		if prior != effective {
			return errors.New("conflicting usage authorization replay")
		}
		i.appendAccess("authorize", authorization.RequestID, authorization.UsageItemID, authorization.DispatchID, proof.record.Timestamp, false, true)
		return nil
	}
	i.authorizations[authorization.RequestID] = effective
	i.appendAccess("authorize", authorization.RequestID, authorization.UsageItemID, authorization.DispatchID, proof.record.Timestamp, true, false)
	return nil
}

func (i *DevelopmentUsageIssuerV2) revealUsage(_ context.Context, request UsageRevealRequestV2) (UsageReceiptV2, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	effective, ok := i.authorizations[request.RequestID]
	if !ok {
		return UsageReceiptV2{}, errors.New("usage reveal is not authorized by an effective dispatch")
	}
	authorization := effective.Authorization
	if request.RunID != authorization.RunID || request.UsageItemID != authorization.UsageItemID ||
		request.DispatchID != authorization.DispatchID || request.ConfigSHA256 != authorization.ConfigSHA256 {
		return UsageReceiptV2{}, errors.New("usage reveal does not match its dispatch authorization")
	}
	revealAt, err := parseCanonicalAbsoluteTimeV2("reveal_at", request.RevealAt)
	if err != nil {
		return UsageReceiptV2{}, err
	}
	availableAt, err := parseCanonicalAbsoluteTimeV2("usage_available_at", authorization.UsageAvailableAt)
	if err != nil {
		return UsageReceiptV2{}, err
	}
	if revealAt.Before(availableAt) {
		i.appendAccess("reveal_rejected_early", request.RequestID, request.UsageItemID, request.DispatchID, request.RevealAt, false, false)
		return UsageReceiptV2{}, errors.New("usage is not available at the requested causal time")
	}
	if prior, replayed := i.receipts[request.RequestID]; replayed {
		i.appendAccess("reveal", request.RequestID, request.UsageItemID, request.DispatchID, request.RevealAt, false, true)
		return prior, nil
	}
	row := i.observations[request.RequestID]
	receipt := UsageReceiptV2{
		SchemaVersion: UsageReceiptSchemaV2, RecordType: "usage_receipt", RunID: request.RunID,
		RequestID: request.RequestID, UsageItemID: request.UsageItemID, DispatchID: request.DispatchID,
		ConfigSHA256: request.ConfigSHA256, UsageBindingSHA256: i.binding.BindingSHA256,
		UsageAvailableAt: authorization.UsageAvailableAt, RevealedAt: request.RevealAt,
		ActualInputTokens: row.ActualInputTokens, ActualOutputTokens: row.ActualOutputTokens,
	}
	receipt.ReceiptSHA256 = usageReceiptDigestV2(receipt)
	i.receipts[request.RequestID] = receipt
	i.appendAccess("reveal", request.RequestID, request.UsageItemID, request.DispatchID, request.RevealAt, true, false)
	return receipt, nil
}

func (i *DevelopmentUsageIssuerV2) verifyUsage(request UsageRevealRequestV2, receipt UsageReceiptV2) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	effective, ok := i.authorizations[request.RequestID]
	if !ok {
		return errors.New("usage receipt has no dispatch authorization")
	}
	authorization := effective.Authorization
	row, ok := i.observations[request.RequestID]
	if !ok || receipt.RunID != authorization.RunID || receipt.RequestID != authorization.RequestID ||
		receipt.UsageItemID != authorization.UsageItemID || receipt.DispatchID != authorization.DispatchID ||
		receipt.ConfigSHA256 != authorization.ConfigSHA256 || receipt.UsageBindingSHA256 != i.binding.BindingSHA256 ||
		receipt.UsageAvailableAt != authorization.UsageAvailableAt || receipt.RevealedAt != request.RevealAt ||
		receipt.ActualInputTokens != row.ActualInputTokens || receipt.ActualOutputTokens != row.ActualOutputTokens {
		return errors.New("usage receipt does not exactly match authority state")
	}
	return nil
}

func (i *DevelopmentUsageIssuerV2) UsageAccessLog() []UsageAccessEventV2 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]UsageAccessEventV2(nil), i.access...)
}
