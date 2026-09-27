package govar

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const selectedFeedbackDataset = "routereval_math_outcomes"

var selectedFeedbackRunID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$`)

// SelectedFeedbackBinding is outcome-free evidence fixed before admission.
// ItemID is the canonical SHA-256 item identity from the prepared dataset. No
// model identity or outcome is accepted here: the effective route supplies the
// selected model after dispatch.
type SelectedFeedbackBinding struct {
	RunID                string `json:"run_id"`
	DatasetID            string `json:"dataset_id"`
	Split                string `json:"split"`
	ItemID               string `json:"item_id"`
	ProtocolSHA256       string `json:"protocol_sha256"`
	OracleArtifactSHA256 string `json:"oracle_artifact_sha256"`
	SoftwareSHA256       string `json:"software_sha256"`
	ConfigSHA256         string `json:"config_sha256"`
}

// SelectedFeedbackAuthority is process configuration supplied by the
// experiment authority, not by an admitted workload. ItemID is intentionally
// absent because it varies per request; every other run binding must match.
type SelectedFeedbackAuthority struct {
	RunID                string            `json:"run_id"`
	DatasetID            string            `json:"dataset_id"`
	Split                string            `json:"split"`
	ProtocolSHA256       string            `json:"protocol_sha256"`
	OracleArtifactSHA256 string            `json:"oracle_artifact_sha256"`
	SoftwareSHA256       string            `json:"software_sha256"`
	ConfigSHA256         string            `json:"config_sha256"`
	ModelMapSHA256       string            `json:"model_map_sha256"`
	SelectedModelIDs     map[string]string `json:"selected_model_ids"`
}

func (a SelectedFeedbackAuthority) Validate() error {
	if err := (SelectedFeedbackBinding{
		RunID: a.RunID, DatasetID: a.DatasetID, Split: a.Split, ItemID: strings.Repeat("0", 64),
		ProtocolSHA256: a.ProtocolSHA256, OracleArtifactSHA256: a.OracleArtifactSHA256,
		SoftwareSHA256: a.SoftwareSHA256, ConfigSHA256: a.ConfigSHA256,
	}).Validate(); err != nil {
		return err
	}
	digest, err := selectedFeedbackModelMapSHA256(a.SelectedModelIDs)
	if err != nil {
		return err
	}
	if !isSHA(a.ModelMapSHA256) || digest != a.ModelMapSHA256 {
		return errors.New("selected-feedback model map digest differs")
	}
	return nil
}

func (a SelectedFeedbackAuthority) matches(binding *SelectedFeedbackBinding) bool {
	return binding != nil && a.RunID == binding.RunID && a.DatasetID == binding.DatasetID && a.Split == binding.Split &&
		a.ProtocolSHA256 == binding.ProtocolSHA256 && a.OracleArtifactSHA256 == binding.OracleArtifactSHA256 &&
		a.SoftwareSHA256 == binding.SoftwareSHA256 && a.ConfigSHA256 == binding.ConfigSHA256
}

func authorityForSelectedFeedbackBinding(binding *SelectedFeedbackBinding) SelectedFeedbackAuthority {
	if binding == nil {
		return SelectedFeedbackAuthority{}
	}
	// Test helper: the value is the first public RouterEval MATH catalog ID.
	models := map[string]string{"gpt-fr": "4ad7fa76864b11e673d8d28b3a25db3343fb4b2d6f10c5f8f21b1fd24c0ae946"}
	digest, _ := selectedFeedbackModelMapSHA256(models)
	return SelectedFeedbackAuthority{RunID: binding.RunID, DatasetID: binding.DatasetID, Split: binding.Split,
		ProtocolSHA256: binding.ProtocolSHA256, OracleArtifactSHA256: binding.OracleArtifactSHA256,
		SoftwareSHA256: binding.SoftwareSHA256, ConfigSHA256: binding.ConfigSHA256,
		ModelMapSHA256: digest, SelectedModelIDs: models}
}

func selectedFeedbackModelMapSHA256(models map[string]string) (string, error) {
	if len(models) == 0 {
		return "", errors.New("selected-feedback model map is empty")
	}
	seen := make(map[string]struct{}, len(models))
	for deployment, modelID := range models {
		deployment = strings.TrimSpace(deployment)
		if deployment == "" || len(deployment) > 256 || !isSHA(modelID) {
			return "", errors.New("selected-feedback model map contains an invalid deployment or model identity")
		}
		if _, duplicate := seen[modelID]; duplicate {
			return "", errors.New("selected-feedback model map contains a duplicate model identity")
		}
		seen[modelID] = struct{}{}
	}
	payload, err := json.Marshal(struct {
		Schema string            `json:"schema"`
		Models map[string]string `json:"models"`
	}{Schema: "govar-selected-feedback-model-map-v1", Models: models})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (a SelectedFeedbackAuthority) selectedModelID(deployment string) (string, error) {
	modelID, ok := a.SelectedModelIDs[deployment]
	if !ok || !isSHA(modelID) {
		return "", errors.New("selected deployment is absent from the immutable selected-feedback model map")
	}
	return modelID, nil
}

func (a SelectedFeedbackAuthority) equal(other SelectedFeedbackAuthority) bool {
	if a.RunID != other.RunID || a.DatasetID != other.DatasetID || a.Split != other.Split ||
		a.ProtocolSHA256 != other.ProtocolSHA256 || a.OracleArtifactSHA256 != other.OracleArtifactSHA256 ||
		a.SoftwareSHA256 != other.SoftwareSHA256 || a.ConfigSHA256 != other.ConfigSHA256 ||
		a.ModelMapSHA256 != other.ModelMapSHA256 || len(a.SelectedModelIDs) != len(other.SelectedModelIDs) {
		return false
	}
	for deployment, modelID := range a.SelectedModelIDs {
		if other.SelectedModelIDs[deployment] != modelID {
			return false
		}
	}
	return true
}

func (b SelectedFeedbackBinding) Validate() error {
	if !selectedFeedbackRunID.MatchString(b.RunID) {
		return errors.New("run_id has an unsafe identity")
	}
	if b.DatasetID != selectedFeedbackDataset {
		return errors.New("dataset_id is not the selected-feedback dataset")
	}
	switch b.Split {
	case "train", "calibration", "development", "frozen_test":
	default:
		return errors.New("split is outside the closed selected-feedback set")
	}
	for name, value := range map[string]string{
		"item_id": b.ItemID, "protocol_sha256": b.ProtocolSHA256,
		"oracle_artifact_sha256": b.OracleArtifactSHA256,
		"software_sha256":        b.SoftwareSHA256, "config_sha256": b.ConfigSHA256,
	} {
		if !isSHA(value) {
			return fmt.Errorf("%s must be a lowercase SHA-256 digest", name)
		}
	}
	return nil
}

func cloneSelectedFeedbackBinding(binding *SelectedFeedbackBinding) *SelectedFeedbackBinding {
	if binding == nil {
		return nil
	}
	copy := *binding
	return &copy
}

func selectedFeedbackBindingSHA256(binding *SelectedFeedbackBinding) string {
	if binding == nil {
		return ""
	}
	payload, _ := json.Marshal(struct {
		Schema string `json:"schema"`
		SelectedFeedbackBinding
	}{Schema: "govar-selected-feedback-binding-v1", SelectedFeedbackBinding: *binding})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func selectedFeedbackTextSHA256(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func selectedFeedbackDomainHash(domain string, parts ...string) string {
	h := sha256.New()
	var length [8]byte
	write := func(value string) {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	write(domain)
	for _, part := range parts {
		write(part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type selectedFeedbackAuthorization struct {
	DispatchID          string                     `json:"dispatch_id"`
	RequestID           string                     `json:"request_id"`
	ItemID              string                     `json:"item_id"`
	SelectedModelID     string                     `json:"selected_model_id"`
	SelectedDeployment  string                     `json:"selected_deployment"`
	AttemptID           string                     `json:"attempt_id"`
	DecisionSHA256      string                     `json:"decision_sha256"`
	DispatchedAtUTC     string                     `json:"dispatched_at_utc"`
	ExpiresAtUTC        string                     `json:"expires_at_utc"`
	Binding             selectedFeedbackRunBinding `json:"binding"`
	AuthorizationSHA256 string                     `json:"-"`
}

type selectedFeedbackRunBinding struct {
	RunID                string `json:"run_id"`
	DatasetID            string `json:"dataset_id"`
	Split                string `json:"split"`
	ProtocolSHA256       string `json:"protocol_sha256"`
	OracleArtifactSHA256 string `json:"oracle_artifact_sha256"`
	SoftwareSHA256       string `json:"software_sha256"`
	ConfigSHA256         string `json:"config_sha256"`
}

func selectedFeedbackDecisionSHA256(res Reservation) string {
	return selectedFeedbackDomainHash("govar-selected-feedback-decision-v1",
		selectedFeedbackTextSHA256(res.RequestID), res.SelectedFeedbackBindingSHA256,
		selectedFeedbackTextSHA256(res.SelectedDeployment), selectedFeedbackTextSHA256(res.ProviderAttemptID),
		res.RouteSnapshot.SnapshotHash, res.AdmissionFingerprint, res.PolicyVersion,
		res.PricingSnapshotSHA256, res.CapEvidenceSHA256)
}

func selectedFeedbackDispatchID(res Reservation) string {
	if res.SelectedFeedback == nil {
		return ""
	}
	binding := res.SelectedFeedback
	return selectedFeedbackDomainHash("govar-selected-feedback-dispatch-v1",
		binding.RunID, binding.DatasetID, binding.Split, selectedFeedbackTextSHA256(res.RequestID), binding.ItemID,
		selectedFeedbackTextSHA256(res.SelectedDeployment), res.SelectedDeployment,
		selectedFeedbackTextSHA256(res.ProviderAttemptID), selectedFeedbackDecisionSHA256(res),
		binding.ProtocolSHA256, binding.OracleArtifactSHA256, binding.SoftwareSHA256, binding.ConfigSHA256)
}

func selectedFeedbackCanonicalAuthorizationSHA256(auth selectedFeedbackAuthorization) (string, error) {
	auth.AuthorizationSHA256 = ""
	unsorted, err := json.Marshal(struct {
		Schema string `json:"schema"`
		selectedFeedbackAuthorization
	}{Schema: "govar-selected-feedback-dispatch-v1", selectedFeedbackAuthorization: auth})
	if err != nil {
		return "", err
	}
	// Python's independent evidence verifier canonicalizes dictionaries with
	// sort_keys=True. Round-trip through interface maps so encoding/json sorts
	// every object key rather than retaining Go struct declaration order.
	var canonical any
	if err := json.Unmarshal(unsorted, &canonical); err != nil {
		return "", err
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func buildSelectedFeedbackAuthorization(res Reservation, dispatchedAt time.Time, authority SelectedFeedbackAuthority) (selectedFeedbackAuthorization, error) {
	if res.SelectedFeedback == nil {
		return selectedFeedbackAuthorization{}, errors.New("selected-feedback binding is absent")
	}
	binding := *res.SelectedFeedback
	if err := binding.Validate(); err != nil {
		return selectedFeedbackAuthorization{}, err
	}
	if err := authority.Validate(); err != nil || !authority.matches(&binding) {
		return selectedFeedbackAuthorization{}, errors.New("selected-feedback binding differs from the configured run authority")
	}
	if binding.SoftwareSHA256 != authority.SoftwareSHA256 {
		return selectedFeedbackAuthorization{}, errors.New("selected-feedback software binding differs from the running binary")
	}
	if selectedFeedbackBindingSHA256(&binding) != res.SelectedFeedbackBindingSHA256 {
		return selectedFeedbackAuthorization{}, errors.New("selected-feedback binding digest differs from the reserved binding")
	}
	if res.State != StateDispatched || res.OutboxState != OutboxDelivered {
		return selectedFeedbackAuthorization{}, errors.New("selected-feedback authorization requires effective dispatch delivery")
	}
	if strings.TrimSpace(res.SelectedDeployment) == "" || strings.TrimSpace(res.ProviderAttemptID) == "" {
		return selectedFeedbackAuthorization{}, errors.New("selected-feedback route or attempt identity is empty")
	}
	dispatchedAt = dispatchedAt.UTC().Truncate(time.Second)
	expiresAt := res.Expiry.UTC().Truncate(time.Second)
	if !expiresAt.After(dispatchedAt) {
		return selectedFeedbackAuthorization{}, errors.New("selected-feedback reservation expired before delivery")
	}
	selectedModelID, err := authority.selectedModelID(res.SelectedDeployment)
	if err != nil {
		return selectedFeedbackAuthorization{}, err
	}
	auth := selectedFeedbackAuthorization{
		DispatchID: selectedFeedbackDispatchID(res), RequestID: selectedFeedbackTextSHA256(res.RequestID),
		ItemID: binding.ItemID, SelectedModelID: selectedModelID,
		SelectedDeployment: res.SelectedDeployment, AttemptID: selectedFeedbackTextSHA256(res.ProviderAttemptID),
		DecisionSHA256:  selectedFeedbackDecisionSHA256(res),
		DispatchedAtUTC: dispatchedAt.Format(time.RFC3339), ExpiresAtUTC: expiresAt.Format(time.RFC3339),
		Binding: selectedFeedbackRunBinding{RunID: binding.RunID, DatasetID: binding.DatasetID, Split: binding.Split,
			ProtocolSHA256: binding.ProtocolSHA256, OracleArtifactSHA256: binding.OracleArtifactSHA256,
			SoftwareSHA256: binding.SoftwareSHA256, ConfigSHA256: binding.ConfigSHA256},
	}
	auth.AuthorizationSHA256, err = selectedFeedbackCanonicalAuthorizationSHA256(auth)
	return auth, err
}

func insertSelectedFeedbackAuthorizationTx(ctx context.Context, tx pgx.Tx, res *Reservation, dispatchedAt time.Time, authority SelectedFeedbackAuthority) error {
	auth, err := buildSelectedFeedbackAuthorization(*res, dispatchedAt, authority)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO govar_selected_feedback_dispatches(
dispatch_id,run_id,dataset_id,split,request_id,item_id,selected_model_id,selected_deployment,attempt_id,
decision_sha256,protocol_sha256,oracle_artifact_sha256,software_sha256,config_sha256,
dispatched_at_utc,expires_at_utc,authorization_sha256)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		auth.DispatchID, auth.Binding.RunID, auth.Binding.DatasetID, auth.Binding.Split, auth.RequestID,
		auth.ItemID, auth.SelectedModelID, auth.SelectedDeployment, auth.AttemptID, auth.DecisionSHA256,
		auth.Binding.ProtocolSHA256, auth.Binding.OracleArtifactSHA256, auth.Binding.SoftwareSHA256,
		auth.Binding.ConfigSHA256, auth.DispatchedAtUTC, auth.ExpiresAtUTC, auth.AuthorizationSHA256)
	if err != nil {
		return fmt.Errorf("insert selected-feedback dispatch authorization: %w", err)
	}
	res.SelectedFeedbackDispatchID = auth.DispatchID
	return nil
}

// selectedFeedbackBootstrapSchema is used only by NewPostgresEngine, the
// explicit bootstrap/test constructor. Production OpenPostgresEngine never
// migrates and validates the externally applied SQL module instead.
const selectedFeedbackBootstrapSchema = `
ALTER TABLE govar_reservations ADD COLUMN IF NOT EXISTS selected_feedback_binding_json JSONB;
ALTER TABLE govar_reservations ADD COLUMN IF NOT EXISTS selected_feedback_binding_sha256 TEXT NOT NULL DEFAULT '';
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='govar_selected_feedback_binding_coherent') THEN
  ALTER TABLE govar_reservations ADD CONSTRAINT govar_selected_feedback_binding_coherent CHECK(
   (selected_feedback_binding_json IS NULL AND selected_feedback_binding_sha256='') OR
   (selected_feedback_binding_json IS NOT NULL AND selected_feedback_binding_sha256 ~ '^[0-9a-f]{64}$'));
 END IF;
END $$;
CREATE TABLE IF NOT EXISTS govar_selected_feedback_dispatches(
 dispatch_id TEXT PRIMARY KEY CHECK(dispatch_id ~ '^[0-9a-f]{64}$'),
 run_id TEXT NOT NULL CHECK(run_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'),
 dataset_id TEXT NOT NULL CHECK(dataset_id='routereval_math_outcomes'),
 split TEXT NOT NULL CHECK(split IN('train','calibration','development','frozen_test')),
 request_id TEXT NOT NULL CHECK(request_id ~ '^[0-9a-f]{64}$'),item_id TEXT NOT NULL CHECK(item_id ~ '^[0-9a-f]{64}$'),
 selected_model_id TEXT NOT NULL CHECK(selected_model_id ~ '^[0-9a-f]{64}$'),selected_deployment TEXT NOT NULL CHECK(length(selected_deployment) BETWEEN 1 AND 256),
 attempt_id TEXT NOT NULL CHECK(attempt_id ~ '^[0-9a-f]{64}$'),decision_sha256 TEXT NOT NULL CHECK(decision_sha256 ~ '^[0-9a-f]{64}$'),
 protocol_sha256 TEXT NOT NULL CHECK(protocol_sha256 ~ '^[0-9a-f]{64}$'),oracle_artifact_sha256 TEXT NOT NULL CHECK(oracle_artifact_sha256 ~ '^[0-9a-f]{64}$'),
 software_sha256 TEXT NOT NULL CHECK(software_sha256 ~ '^[0-9a-f]{64}$'),config_sha256 TEXT NOT NULL CHECK(config_sha256 ~ '^[0-9a-f]{64}$'),
 dispatched_at_utc TIMESTAMPTZ NOT NULL,expires_at_utc TIMESTAMPTZ NOT NULL CHECK(expires_at_utc>dispatched_at_utc),
 authorization_sha256 TEXT NOT NULL UNIQUE CHECK(authorization_sha256 ~ '^[0-9a-f]{64}$'),authorized_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(run_id,request_id),UNIQUE(run_id,item_id),UNIQUE(run_id,attempt_id));
CREATE TABLE IF NOT EXISTS govar_selected_feedback_commitments(
 dispatch_id TEXT PRIMARY KEY REFERENCES govar_selected_feedback_dispatches(dispatch_id) ON DELETE RESTRICT,run_id TEXT NOT NULL,request_id TEXT NOT NULL,item_id TEXT NOT NULL,
 selected_model_id TEXT NOT NULL,selected_score DOUBLE PRECISION NOT NULL CHECK(selected_score BETWEEN 0 AND 1),authorization_sha256 TEXT NOT NULL,
 committed_at_utc TIMESTAMPTZ NOT NULL,commitment_sha256 TEXT NOT NULL UNIQUE CHECK(commitment_sha256 ~ '^[0-9a-f]{64}$'),UNIQUE(run_id,request_id),UNIQUE(run_id,item_id));
CREATE TABLE IF NOT EXISTS govar_selected_feedback_observations(
 observation_id TEXT PRIMARY KEY CHECK(observation_id ~ '^[0-9a-f]{64}$'),dispatch_id TEXT NOT NULL REFERENCES govar_selected_feedback_commitments(dispatch_id) ON DELETE RESTRICT,
 run_id TEXT NOT NULL,request_id TEXT NOT NULL,item_id TEXT NOT NULL,selected_model_id TEXT NOT NULL,selected_score DOUBLE PRECISION NOT NULL CHECK(selected_score BETWEEN 0 AND 1),
 replayed BOOLEAN NOT NULL,observed_at_utc TIMESTAMPTZ NOT NULL,observation_sha256 TEXT NOT NULL UNIQUE CHECK(observation_sha256 ~ '^[0-9a-f]{64}$'));
CREATE OR REPLACE FUNCTION govar_reject_selected_feedback_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'selected-feedback evidence is append-only'; END $$;
CREATE OR REPLACE FUNCTION govar_validate_selected_feedback_commitment() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE authorized govar_selected_feedback_dispatches%ROWTYPE; BEGIN SELECT * INTO STRICT authorized FROM govar_selected_feedback_dispatches WHERE dispatch_id=NEW.dispatch_id;
 IF NEW.run_id<>authorized.run_id OR NEW.request_id<>authorized.request_id OR NEW.item_id<>authorized.item_id OR NEW.selected_model_id<>authorized.selected_model_id OR NEW.authorization_sha256<>authorized.authorization_sha256 THEN RAISE EXCEPTION 'selected-feedback commitment changed its immutable dispatch authorization'; END IF; RETURN NEW; END $$;
CREATE OR REPLACE FUNCTION govar_validate_selected_feedback_observation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE committed govar_selected_feedback_commitments%ROWTYPE; BEGIN SELECT * INTO STRICT committed FROM govar_selected_feedback_commitments WHERE dispatch_id=NEW.dispatch_id;
 IF NEW.run_id<>committed.run_id OR NEW.request_id<>committed.request_id OR NEW.item_id<>committed.item_id OR NEW.selected_model_id<>committed.selected_model_id OR NEW.selected_score<>committed.selected_score THEN RAISE EXCEPTION 'selected-feedback observation changed its immutable commitment'; END IF; RETURN NEW; END $$;
DROP TRIGGER IF EXISTS govar_selected_feedback_commitment_validate ON govar_selected_feedback_commitments;
CREATE TRIGGER govar_selected_feedback_commitment_validate BEFORE INSERT ON govar_selected_feedback_commitments FOR EACH ROW EXECUTE FUNCTION govar_validate_selected_feedback_commitment();
DROP TRIGGER IF EXISTS govar_selected_feedback_observation_validate ON govar_selected_feedback_observations;
CREATE TRIGGER govar_selected_feedback_observation_validate BEFORE INSERT ON govar_selected_feedback_observations FOR EACH ROW EXECUTE FUNCTION govar_validate_selected_feedback_observation();
DROP TRIGGER IF EXISTS govar_selected_feedback_dispatch_no_mutation ON govar_selected_feedback_dispatches;
CREATE TRIGGER govar_selected_feedback_dispatch_no_mutation BEFORE UPDATE OR DELETE ON govar_selected_feedback_dispatches FOR EACH ROW EXECUTE FUNCTION govar_reject_selected_feedback_mutation();
DROP TRIGGER IF EXISTS govar_selected_feedback_commitment_no_mutation ON govar_selected_feedback_commitments;
CREATE TRIGGER govar_selected_feedback_commitment_no_mutation BEFORE UPDATE OR DELETE ON govar_selected_feedback_commitments FOR EACH ROW EXECUTE FUNCTION govar_reject_selected_feedback_mutation();
DROP TRIGGER IF EXISTS govar_selected_feedback_observation_no_mutation ON govar_selected_feedback_observations;
CREATE TRIGGER govar_selected_feedback_observation_no_mutation BEFORE UPDATE OR DELETE ON govar_selected_feedback_observations FOR EACH ROW EXECUTE FUNCTION govar_reject_selected_feedback_mutation();
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='govar_feedback_issuer') THEN CREATE ROLE govar_feedback_issuer NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS; END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='govar_feedback_evaluator') THEN CREATE ROLE govar_feedback_evaluator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS; END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='govar_feedback_verifier') THEN CREATE ROLE govar_feedback_verifier NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS; END IF;
END $$;
GRANT govar_feedback_issuer,govar_feedback_evaluator,govar_feedback_verifier TO CURRENT_USER;
REVOKE ALL ON govar_selected_feedback_dispatches,govar_selected_feedback_commitments,govar_selected_feedback_observations FROM PUBLIC,govar_runtime;
GRANT INSERT ON govar_selected_feedback_dispatches TO govar_runtime;
GRANT SELECT,INSERT ON govar_selected_feedback_dispatches TO govar_feedback_issuer;
GRANT SELECT ON govar_selected_feedback_dispatches TO govar_feedback_evaluator,govar_feedback_verifier;
GRANT SELECT,INSERT ON govar_selected_feedback_commitments,govar_selected_feedback_observations TO govar_feedback_evaluator;
GRANT SELECT ON govar_selected_feedback_commitments,govar_selected_feedback_observations TO govar_feedback_verifier;
`
