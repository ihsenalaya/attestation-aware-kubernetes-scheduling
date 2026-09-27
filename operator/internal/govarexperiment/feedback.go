package govarexperiment

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
)

type SelectedFeedbackRequest struct {
	DispatchID string `json:"dispatch_id"`
}

type DispatchOutcomeRequest struct {
	DispatchID     string
	RunID          string
	RequestID      string
	FeedbackItemID string
	// SelectedModelID is the server-pinned public dataset catalog identity,
	// never a hash derived from the deployment name.
	SelectedModelID    string
	SelectedDeployment string
	ProviderAttemptID  string
	ConfigSHA256       string
}

// EffectiveDispatchProof is supplied by the trace runner only after the
// authoritative decision core has made the DISPATCHED transition effective.
// It contains no outcome. The development evaluator binds this proof before it
// will expose the one selected-model fixture row.
type EffectiveDispatchProof struct {
	LifecycleEventIndex int64
	DeliveryEventID     string
	RouteSnapshotSHA256 string
	State               string
	TransitionEffective bool
}

const FeedbackAccessSchema = "govar-selected-feedback-access-v1"

// FeedbackAccessEvent is an outcome-free, append-only evaluator access
// receipt. It proves the authorize-before-issue order and exact immutable
// dispatch binding without serializing usage or quality values.
type FeedbackAccessEvent struct {
	SchemaVersion       string `json:"schema_version"`
	EventIndex          int64  `json:"event_index"`
	EventType           string `json:"event_type"`
	AuthorizationSHA256 string `json:"authorization_sha256"`
	LifecycleEventIndex int64  `json:"lifecycle_event_index"`
	DeliveryEventID     string `json:"delivery_event_id"`
	RouteSnapshotSHA256 string `json:"route_snapshot_sha256"`
	DispatchID          string `json:"dispatch_id"`
	RunID               string `json:"run_id"`
	RequestID           string `json:"request_id"`
	FeedbackItemID      string `json:"feedback_item_id"`
	SelectedModelID     string `json:"selected_model_id"`
	SelectedDeployment  string `json:"selected_deployment"`
	ProviderAttemptID   string `json:"provider_attempt_id"`
	ConfigSHA256        string `json:"config_sha256"`
	Effective           bool   `json:"effective"`
	Replayed            bool   `json:"replayed"`
}

type effectiveDispatchAuthorization struct {
	Request             DispatchOutcomeRequest
	Proof               EffectiveDispatchProof
	AuthorizationSHA256 string
}

func validateDispatchOutcomeRequest(req DispatchOutcomeRequest) error {
	if !shaPattern.MatchString(req.DispatchID) || !validIdentity(req.RunID) || !validIdentity(req.RequestID) ||
		!shaPattern.MatchString(req.FeedbackItemID) || !shaPattern.MatchString(req.SelectedModelID) ||
		!validIdentity(req.SelectedDeployment) || req.ProviderAttemptID == "" || len(req.ProviderAttemptID) > 256 ||
		!shaPattern.MatchString(req.ConfigSHA256) {
		return errors.New("selected-feedback dispatch binding is incomplete")
	}
	return nil
}

func validateEffectiveDispatchProof(proof EffectiveDispatchProof) error {
	if proof.LifecycleEventIndex <= 0 || !validIdentity(proof.DeliveryEventID) ||
		!shaPattern.MatchString(proof.RouteSnapshotSHA256) || proof.State != "DISPATCHED" || !proof.TransitionEffective {
		return errors.New("selected-feedback effective-delivery proof is invalid")
	}
	return nil
}

func effectiveDispatchAuthorizationSHA(req DispatchOutcomeRequest, proof EffectiveDispatchProof) (string, error) {
	if err := validateDispatchOutcomeRequest(req); err != nil {
		return "", err
	}
	if err := validateEffectiveDispatchProof(proof); err != nil {
		return "", err
	}
	return DomainHash("govar-development-effective-dispatch-v1", []byte(req.DispatchID), []byte(req.RunID),
		[]byte(req.RequestID), []byte(req.FeedbackItemID), []byte(req.SelectedModelID), []byte(req.SelectedDeployment),
		[]byte(req.ProviderAttemptID), []byte(req.ConfigSHA256), []byte(strconv.FormatInt(proof.LifecycleEventIndex, 10)),
		[]byte(proof.DeliveryEventID), []byte(proof.RouteSnapshotSHA256), []byte(proof.State)), nil
}

func SelectedFeedbackModelMapDigest(models map[string]string) (string, error) {
	if len(models) == 0 {
		return "", errors.New("selected-feedback model map is empty")
	}
	deployments := make([]string, 0, len(models))
	seen := map[string]struct{}{}
	for deployment, catalogID := range models {
		if deployment == "" || len(deployment) > 256 || !shaPattern.MatchString(catalogID) {
			return "", errors.New("selected-feedback model map contains an invalid deployment or catalog model identity")
		}
		if _, duplicate := seen[catalogID]; duplicate {
			return "", errors.New("selected-feedback model map contains a duplicate catalog identity")
		}
		seen[catalogID] = struct{}{}
		deployments = append(deployments, deployment)
	}
	// encoding/json sorts string map keys. Sorting explicitly documents that
	// insertion order cannot affect the immutable authority digest.
	sort.Strings(deployments)
	canonical := make(map[string]string, len(deployments))
	for _, deployment := range deployments {
		canonical[deployment] = models[deployment]
	}
	payload, err := json.Marshal(struct {
		Schema string            `json:"schema"`
		Models map[string]string `json:"models"`
	}{Schema: "govar-selected-feedback-model-map-v1", Models: canonical})
	if err != nil {
		return "", err
	}
	return SHA256(payload), nil
}

func selectedFeedbackCatalogModelID(config Config, deployment string) (string, error) {
	catalogID, ok := config.SelectedFeedbackModelIDs[deployment]
	if !ok || !shaPattern.MatchString(catalogID) {
		return "", errors.New("selected deployment is absent from the immutable selected-feedback model map")
	}
	return catalogID, nil
}

// OutcomeIssuer is the post-dispatch boundary. Implementations may combine a
// provider usage adapter with the selected-feedback evaluator, but must not
// reveal any outcome until IssueDispatchedOutcome is called for an effective
// production-core dispatch. VerifyDispatchedOutcome is also used during raw
// verification; a frozen issuer must verify its trusted authority receipt.
type OutcomeIssuer interface {
	AuthorizeDispatchedOutcome(DispatchOutcomeRequest, EffectiveDispatchProof) error
	IssueDispatchedOutcome(context.Context, DispatchOutcomeRequest) (DispatchedOutcome, error)
	VerifyDispatchedOutcome(DispatchOutcomeRequest, DispatchedOutcome) error
	FeedbackAccessLog() []FeedbackAccessEvent
	EvidenceTier() string
	BindingEvidence() FeedbackEvidence
}

// FixtureOutcome is accepted only by the development fixture issuer. It lives
// in a separate file/process input and is never part of the opportunity stream.
type FixtureOutcome struct {
	SchemaVersion      string  `json:"schema_version"`
	RecordType         string  `json:"record_type"`
	RequestID          string  `json:"request_id"`
	FeedbackItemID     string  `json:"feedback_item_id"`
	SelectedModelID    string  `json:"selected_model_id"`
	ActualInputTokens  int64   `json:"actual_input_tokens"`
	ActualOutputTokens int64   `json:"actual_output_tokens"`
	SelectedScore      float64 `json:"selected_score"`
}

// DevelopmentFixtureIssuer is deliberately incapable of frozen authorization.
// It exists for hand-computable regression and development pilot fixtures only.
type DevelopmentFixtureIssuer struct {
	mu             sync.Mutex
	outcomes       map[string]FixtureOutcome
	evidence       FeedbackEvidence
	authorizations map[string]effectiveDispatchAuthorization
	issued         map[string]DispatchedOutcome
	access         []FeedbackAccessEvent
}

func ParseDevelopmentFixtureOutcomes(raw []byte, evidence FeedbackEvidence) (*DevelopmentFixtureIssuer, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	rows := map[string]FixtureOutcome{}
	line := 0
	for scanner.Scan() {
		line++
		body := bytes.TrimSpace(scanner.Bytes())
		if len(body) == 0 {
			return nil, fmt.Errorf("fixture outcome line %d is empty", line)
		}
		var row FixtureOutcome
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("fixture outcome line %d: %w", line, err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			return nil, fmt.Errorf("fixture outcome line %d: %w", line, err)
		}
		if row.SchemaVersion != "govar-development-outcome-v1" || row.RecordType != "development_outcome" ||
			!validIdentity(row.RequestID) || !shaPattern.MatchString(row.FeedbackItemID) || !shaPattern.MatchString(row.SelectedModelID) ||
			row.ActualInputTokens < 0 || row.ActualOutputTokens < 0 || row.SelectedScore != row.SelectedScore || row.SelectedScore < 0 || row.SelectedScore > 1 {
			return nil, fmt.Errorf("fixture outcome line %d is invalid", line)
		}
		if _, duplicate := rows[row.RequestID]; duplicate {
			return nil, fmt.Errorf("fixture outcome line %d duplicates request_id", line)
		}
		rows[row.RequestID] = row
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return &DevelopmentFixtureIssuer{outcomes: rows, evidence: evidence,
		authorizations: map[string]effectiveDispatchAuthorization{}, issued: map[string]DispatchedOutcome{}}, nil
}

// ValidateOpportunityCoverage proves that the development-only fixture contains
// exactly one selected-model row for every immutable opportunity and no extra
// request that the producer could query later. It deliberately performs no
// selection or outcome-based filtering.
func (i *DevelopmentFixtureIssuer) ValidateOpportunityCoverage(opportunities []Opportunity, modelIDs map[string]string) error {
	if len(i.outcomes) != len(opportunities) {
		return fmt.Errorf("development fixture rows=%d opportunities=%d", len(i.outcomes), len(opportunities))
	}
	allowedModels := make(map[string]struct{}, len(modelIDs))
	for _, modelID := range modelIDs {
		allowedModels[modelID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(opportunities))
	for _, opportunity := range opportunities {
		if _, duplicate := seen[opportunity.RequestID]; duplicate {
			return errors.New("opportunity request identity is duplicate")
		}
		seen[opportunity.RequestID] = struct{}{}
		row, ok := i.outcomes[opportunity.RequestID]
		if !ok || row.FeedbackItemID != opportunity.FeedbackItemID {
			return fmt.Errorf("development fixture does not exactly bind opportunity %s", opportunity.RequestID)
		}
		if _, ok := allowedModels[row.SelectedModelID]; !ok {
			return fmt.Errorf("development fixture request %s names a model outside the immutable deployment map", opportunity.RequestID)
		}
	}
	return nil
}

func (i *DevelopmentFixtureIssuer) EvidenceTier() string              { return "development_debug" }
func (i *DevelopmentFixtureIssuer) BindingEvidence() FeedbackEvidence { return i.evidence }

func (i *DevelopmentFixtureIssuer) accessEvent(kind string, authorization effectiveDispatchAuthorization, effective, replayed bool) FeedbackAccessEvent {
	return FeedbackAccessEvent{SchemaVersion: FeedbackAccessSchema, EventIndex: int64(len(i.access) + 1), EventType: kind,
		AuthorizationSHA256: authorization.AuthorizationSHA256, LifecycleEventIndex: authorization.Proof.LifecycleEventIndex,
		DeliveryEventID: authorization.Proof.DeliveryEventID, RouteSnapshotSHA256: authorization.Proof.RouteSnapshotSHA256,
		DispatchID: authorization.Request.DispatchID, RunID: authorization.Request.RunID, RequestID: authorization.Request.RequestID,
		FeedbackItemID: authorization.Request.FeedbackItemID, SelectedModelID: authorization.Request.SelectedModelID,
		SelectedDeployment: authorization.Request.SelectedDeployment, ProviderAttemptID: authorization.Request.ProviderAttemptID,
		ConfigSHA256: authorization.Request.ConfigSHA256, Effective: effective, Replayed: replayed}
}

// AuthorizeDispatchedOutcome establishes the development-only selected-
// feedback boundary. An exact repeated authorization is idempotent; any reuse
// of the dispatch identity with a changed request or delivery proof fails.
func (i *DevelopmentFixtureIssuer) AuthorizeDispatchedOutcome(req DispatchOutcomeRequest, proof EffectiveDispatchProof) error {
	authorizationSHA, err := effectiveDispatchAuthorizationSHA(req, proof)
	if err != nil {
		return err
	}
	authorization := effectiveDispatchAuthorization{Request: req, Proof: proof, AuthorizationSHA256: authorizationSHA}
	i.mu.Lock()
	defer i.mu.Unlock()
	if req.RunID != i.evidence.RunID || req.ConfigSHA256 != i.evidence.ConfigSHA256 {
		return errors.New("selected-feedback authorization differs from the bound run/config")
	}
	if prior, exists := i.authorizations[req.DispatchID]; exists {
		if !reflect.DeepEqual(prior, authorization) {
			return errors.New("conflicting selected-feedback dispatch authorization")
		}
		i.access = append(i.access, i.accessEvent("authorization_replay", prior, false, true))
		return nil
	}
	i.authorizations[req.DispatchID] = authorization
	i.access = append(i.access, i.accessEvent("authorize", authorization, true, false))
	return nil
}

func (i *DevelopmentFixtureIssuer) IssueDispatchedOutcome(_ context.Context, req DispatchOutcomeRequest) (DispatchedOutcome, error) {
	if err := validateDispatchOutcomeRequest(req); err != nil {
		return DispatchedOutcome{}, err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	authorization, authorized := i.authorizations[req.DispatchID]
	if !authorized || !reflect.DeepEqual(authorization.Request, req) {
		return DispatchedOutcome{}, errors.New("selected-feedback outcome requested before exact effective dispatch authorization")
	}
	if prior, replayed := i.issued[req.DispatchID]; replayed {
		prior.Feedback.Replayed = true
		i.access = append(i.access, i.accessEvent("issue_replay", authorization, false, true))
		return prior, nil
	}
	row, ok := i.outcomes[req.RequestID]
	if !ok {
		return DispatchedOutcome{}, errors.New("development outcome not found")
	}
	if row.FeedbackItemID != req.FeedbackItemID || row.SelectedModelID != req.SelectedModelID {
		return DispatchedOutcome{}, errors.New("development selected outcome does not match dispatched item/model")
	}
	receipt := SelectedFeedbackReceipt{DispatchID: req.DispatchID, RequestID: SHA256([]byte(req.RequestID)), ItemID: row.FeedbackItemID,
		SelectedModelID: row.SelectedModelID, SelectedScore: row.SelectedScore,
		ObservationID: DomainHash("govar-development-observation-v1", []byte(req.DispatchID))}
	outcome := DispatchedOutcome{ActualInputTokens: row.ActualInputTokens, ActualOutputTokens: row.ActualOutputTokens,
		UsageReceiptSHA256: DomainHash("govar-development-usage-receipt-v1", []byte(req.DispatchID), []byte(fmt.Sprint(row.ActualInputTokens)), []byte(fmt.Sprint(row.ActualOutputTokens))),
		Feedback:           receipt, Evidence: i.evidence}
	i.issued[req.DispatchID] = outcome
	i.access = append(i.access, i.accessEvent("issue", authorization, true, false))
	return outcome, nil
}

func (i *DevelopmentFixtureIssuer) VerifyDispatchedOutcome(req DispatchOutcomeRequest, outcome DispatchedOutcome) error {
	if outcome.ActualInputTokens < 0 || outcome.ActualOutputTokens < 0 || !shaPattern.MatchString(outcome.UsageReceiptSHA256) {
		return errors.New("provider usage receipt is invalid")
	}
	if err := outcome.Feedback.Validate(); err != nil {
		return err
	}
	if err := outcome.Evidence.Validate(); err != nil {
		return err
	}
	if outcome.Feedback.DispatchID != req.DispatchID || outcome.Feedback.RequestID != SHA256([]byte(req.RequestID)) ||
		outcome.Feedback.ItemID != req.FeedbackItemID || outcome.Feedback.SelectedModelID != req.SelectedModelID {
		return errors.New("selected-feedback receipt does not match authorized dispatch")
	}
	if outcome.Evidence.ConfigSHA256 != req.ConfigSHA256 {
		return errors.New("feedback authority config hash mismatch")
	}
	if outcome.Evidence.ModelMapSHA256 != i.evidence.ModelMapSHA256 {
		return errors.New("feedback authority model-map hash mismatch")
	}
	i.mu.Lock()
	row, exists := i.outcomes[req.RequestID]
	i.mu.Unlock()
	if !exists || row.FeedbackItemID != req.FeedbackItemID || row.SelectedModelID != req.SelectedModelID {
		return errors.New("selected-feedback verification fixture does not match the dispatched identity")
	}
	expected := DispatchedOutcome{ActualInputTokens: row.ActualInputTokens, ActualOutputTokens: row.ActualOutputTokens,
		UsageReceiptSHA256: DomainHash("govar-development-usage-receipt-v1", []byte(req.DispatchID), []byte(fmt.Sprint(row.ActualInputTokens)), []byte(fmt.Sprint(row.ActualOutputTokens))),
		Feedback: SelectedFeedbackReceipt{DispatchID: req.DispatchID, RequestID: SHA256([]byte(req.RequestID)), ItemID: row.FeedbackItemID,
			SelectedModelID: row.SelectedModelID, SelectedScore: row.SelectedScore, Replayed: outcome.Feedback.Replayed,
			ObservationID: DomainHash("govar-development-observation-v1", []byte(req.DispatchID))}, Evidence: i.evidence}
	if expected.ActualInputTokens != outcome.ActualInputTokens || expected.ActualOutputTokens != outcome.ActualOutputTokens ||
		expected.UsageReceiptSHA256 != outcome.UsageReceiptSHA256 || expected.Feedback != outcome.Feedback || expected.Evidence != outcome.Evidence {
		return errors.New("development outcome differs from bound post-dispatch fixture")
	}
	return nil
}

func (i *DevelopmentFixtureIssuer) FeedbackAccessLog() []FeedbackAccessEvent {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]FeedbackAccessEvent(nil), i.access...)
}

// VerifyFeedbackAccessLog reconstructs every authorization from lifecycle
// rows. A normal trace run has exactly one authorize and one issue receipt per
// effective delivery; evaluator replays are tested separately and are not
// accepted as unreported scientific observations.
func VerifyFeedbackAccessLog(events []FeedbackAccessEvent, records []LifecycleRecord, config Config) error {
	deliveries := make([]LifecycleRecord, 0)
	for _, row := range records {
		if row.RecordType == RecordLifecycle && row.LifecycleEvent == "dispatch_delivered" && row.Effective && row.Selected {
			deliveries = append(deliveries, row)
		}
	}
	if len(events) != 2*len(deliveries) {
		return fmt.Errorf("selected-feedback access events=%d, want %d for effective deliveries", len(events), 2*len(deliveries))
	}
	for index, delivery := range deliveries {
		request := DispatchOutcomeRequest{DispatchID: delivery.DispatchID, RunID: config.RunID, RequestID: delivery.RequestID,
			FeedbackItemID: delivery.FeedbackItemSHA256, SelectedModelID: delivery.FeedbackSelectedModelSHA256,
			SelectedDeployment: delivery.SelectedDeployment, ProviderAttemptID: delivery.ProviderAttemptID,
			ConfigSHA256: delivery.ConfigSHA256}
		proof := EffectiveDispatchProof{LifecycleEventIndex: delivery.EventIndex,
			DeliveryEventID:     eventID(config.Seed, delivery.StreamSHA256, delivery.RequestID, "dispatch-delivered"),
			RouteSnapshotSHA256: experimentCandidate(config).RouteSnapshot.SnapshotHash, State: "DISPATCHED", TransitionEffective: true}
		authorizationSHA, err := effectiveDispatchAuthorizationSHA(request, proof)
		if err != nil {
			return err
		}
		for offset, kind := range []string{"authorize", "issue"} {
			event := events[index*2+offset]
			expected := FeedbackAccessEvent{SchemaVersion: FeedbackAccessSchema, EventIndex: int64(index*2 + offset + 1),
				EventType: kind, AuthorizationSHA256: authorizationSHA, LifecycleEventIndex: proof.LifecycleEventIndex,
				DeliveryEventID: proof.DeliveryEventID, RouteSnapshotSHA256: proof.RouteSnapshotSHA256,
				DispatchID: request.DispatchID, RunID: request.RunID, RequestID: request.RequestID,
				FeedbackItemID: request.FeedbackItemID, SelectedModelID: request.SelectedModelID,
				SelectedDeployment: request.SelectedDeployment, ProviderAttemptID: request.ProviderAttemptID,
				ConfigSHA256: request.ConfigSHA256, Effective: true, Replayed: false}
			if !reflect.DeepEqual(event, expected) {
				return fmt.Errorf("selected-feedback access event %d does not match effective delivery", event.EventIndex)
			}
		}
	}
	return nil
}
