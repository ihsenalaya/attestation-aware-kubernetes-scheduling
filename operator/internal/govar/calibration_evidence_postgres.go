package govar

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govaraudit"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
)

const (
	CalibrationLayoutVersion = 8
	CalibrationLayoutID      = "govar-v8-authoritative-calibration-20260713"
)

type SplitRegistry struct {
	RegistryID             string
	TenantID               string
	DatasetSHA256          string
	SplitProtocolSHA256    string
	OpportunitySetSHA256   string
	CohortSHA256           string
	ProducerSoftwareSHA256 string
	AuthorityKeyID         string
	AuthorityProof         string
	RegistrySHA256         string
	FrozenAt               time.Time
	Assignments            []SplitAssignment
}

type SplitAssignment struct {
	OpportunityID                string
	RequestID                    string
	Split                        string
	FeatureSchemaVersion         string
	CanonicalFeatureSHA256       string
	FeatureRegimeSHA256          string
	AdmissionFingerprintSHA256   string
	OpportunitySHA256            string
	ExpectedPriceRegimeSHA256    string
	ExpectedCapPathAdapterSHA256 string
	SplitOpportunityRegimeSHA256 string
	AssignmentSHA256             string
}

type PostgresSplitAuthority struct {
	pool     *pgxpool.Pool
	keyID    string
	key      []byte
	software string
}

type PostgresCalibrationProducer struct {
	pool           *pgxpool.Pool
	softwareSHA256 string
}

type PolicyEvidencePublication struct {
	PublicationSHA256       string
	TenantID                string
	PolicyNamespace         string
	PolicyName              string
	PolicyUID               string
	PolicyGeneration        int64
	ExpectedResourceVersion string
	ArtifactSHA256          string
	DriftSHA256             string
	StatusPayloadSHA256     string
	ProducerSoftwareSHA256  string
}

func SignSplitRegistry(registry SplitRegistry, keyID string, key []byte) (SplitRegistry, error) {
	normalized, err := normalizeSplitRegistry(registry)
	if err != nil {
		return SplitRegistry{}, err
	}
	if strings.TrimSpace(keyID) == "" || len(key) < 32 {
		return SplitRegistry{}, errors.New("split authority key ID and at least 32 secret bytes are required")
	}
	normalized.AuthorityKeyID = keyID
	normalized.AuthorityProof = authorityMAC(key, "govar-split-registry-authority-v1", normalized.RegistrySHA256)
	return normalized, nil
}

func normalizeSplitRegistry(registry SplitRegistry) (SplitRegistry, error) {
	registry.RegistryID = strings.TrimSpace(registry.RegistryID)
	registry.TenantID = strings.TrimSpace(registry.TenantID)
	registry.FrozenAt = registry.FrozenAt.UTC()
	if registry.RegistryID == "" || registry.TenantID == "" || registry.FrozenAt.IsZero() ||
		!isSHA(registry.DatasetSHA256) || !isSHA(registry.SplitProtocolSHA256) || !isSHA(registry.OpportunitySetSHA256) ||
		!isSHA(registry.CohortSHA256) || !isSHA(registry.ProducerSoftwareSHA256) || len(registry.Assignments) == 0 {
		return SplitRegistry{}, errors.New("split registry identity, frozen time, assignments, and immutable digests are required")
	}
	assignments := append([]SplitAssignment(nil), registry.Assignments...)
	sort.Slice(assignments, func(i, j int) bool { return assignments[i].OpportunityID < assignments[j].OpportunityID })
	requestIDs, opportunities := map[string]struct{}{}, map[string]struct{}{}
	assignmentDigests := make([]string, 0, len(assignments))
	for i := range assignments {
		a := &assignments[i]
		a.OpportunityID, a.RequestID = strings.TrimSpace(a.OpportunityID), strings.TrimSpace(a.RequestID)
		a.Split = strings.ToLower(strings.TrimSpace(a.Split))
		if a.OpportunityID == "" || a.RequestID == "" || !calibrationSplit(a.Split) || strings.TrimSpace(a.FeatureSchemaVersion) == "" ||
			!isSHA(a.CanonicalFeatureSHA256) || !isSHA(a.AdmissionFingerprintSHA256) || !isSHA(a.OpportunitySHA256) ||
			!isSHA(a.ExpectedPriceRegimeSHA256) || !isSHA(a.ExpectedCapPathAdapterSHA256) {
			return SplitRegistry{}, fmt.Errorf("split assignment %d is incomplete", i)
		}
		if _, duplicate := requestIDs[a.RequestID]; duplicate {
			return SplitRegistry{}, fmt.Errorf("duplicate split request %s", a.RequestID)
		}
		if _, duplicate := opportunities[a.OpportunityID]; duplicate {
			return SplitRegistry{}, fmt.Errorf("duplicate opportunity %s", a.OpportunityID)
		}
		requestIDs[a.RequestID], opportunities[a.OpportunityID] = struct{}{}, struct{}{}
		feature, err := govarcalibration.CanonicalFeatureRegime(a.FeatureSchemaVersion, a.CanonicalFeatureSHA256)
		if err != nil {
			return SplitRegistry{}, err
		}
		if a.FeatureRegimeSHA256 != "" && a.FeatureRegimeSHA256 != feature {
			return SplitRegistry{}, fmt.Errorf("opportunity %s feature regime mismatch", a.OpportunityID)
		}
		a.FeatureRegimeSHA256 = feature
		splitRegime, err := govarcalibration.CanonicalSplitOpportunityRegime(registry.RegistrySHA256, registry.OpportunitySetSHA256, a.Split, registry.CohortSHA256)
		if registry.RegistrySHA256 == "" {
			// The registry digest is computed below. Use the immutable registry
			// inputs for the first pass and recompute after it is known.
			splitRegime = ""
			err = nil
		}
		if err != nil {
			return SplitRegistry{}, err
		}
		a.SplitOpportunityRegimeSHA256 = splitRegime
		assignmentDigests = append(assignmentDigests, a.OpportunityID, a.RequestID, a.Split, a.FeatureSchemaVersion,
			a.CanonicalFeatureSHA256, a.FeatureRegimeSHA256, a.AdmissionFingerprintSHA256, a.OpportunitySHA256,
			a.ExpectedPriceRegimeSHA256, a.ExpectedCapPathAdapterSHA256)
	}
	registryDigest, err := govarcalibration.CanonicalDigest("govar-split-registry-v1", append([]string{
		registry.RegistryID, registry.TenantID, registry.DatasetSHA256, registry.SplitProtocolSHA256,
		registry.OpportunitySetSHA256, registry.CohortSHA256, registry.ProducerSoftwareSHA256,
		registry.FrozenAt.Format(time.RFC3339Nano),
	}, assignmentDigests...)...)
	if err != nil {
		return SplitRegistry{}, err
	}
	if registry.RegistrySHA256 != "" && registry.RegistrySHA256 != registryDigest {
		return SplitRegistry{}, errors.New("split registry digest mismatch")
	}
	registry.RegistrySHA256 = registryDigest
	for i := range assignments {
		splitRegime, err := govarcalibration.CanonicalSplitOpportunityRegime(registry.RegistrySHA256, registry.OpportunitySetSHA256, assignments[i].Split, registry.CohortSHA256)
		if err != nil {
			return SplitRegistry{}, err
		}
		assignments[i].SplitOpportunityRegimeSHA256 = splitRegime
		assignment, err := govarcalibration.CanonicalDigest("govar-split-assignment-v1", registry.RegistrySHA256,
			assignments[i].OpportunityID, assignments[i].RequestID, assignments[i].Split,
			assignments[i].FeatureSchemaVersion, assignments[i].CanonicalFeatureSHA256,
			assignments[i].FeatureRegimeSHA256, assignments[i].AdmissionFingerprintSHA256,
			assignments[i].OpportunitySHA256, assignments[i].ExpectedPriceRegimeSHA256,
			assignments[i].ExpectedCapPathAdapterSHA256, assignments[i].SplitOpportunityRegimeSHA256)
		if err != nil {
			return SplitRegistry{}, err
		}
		if assignments[i].AssignmentSHA256 != "" && assignments[i].AssignmentSHA256 != assignment {
			return SplitRegistry{}, fmt.Errorf("opportunity %s assignment digest mismatch", assignments[i].OpportunityID)
		}
		assignments[i].AssignmentSHA256 = assignment
	}
	registry.Assignments = assignments
	return registry, nil
}

func OpenPostgresSplitAuthority(ctx context.Context, databaseURL, keyID string, key []byte, softwareSHA256 string) (*PostgresSplitAuthority, error) {
	if strings.TrimSpace(databaseURL) == "" || strings.TrimSpace(keyID) == "" || len(key) < 32 || !isSHA(softwareSHA256) {
		return nil, errors.New("database URL, split authority, and producer software SHA-256 are required")
	}
	pool, err := openCalibrationRolePool(ctx, databaseURL, "govar_split_authority")
	if err != nil {
		return nil, err
	}
	a := &PostgresSplitAuthority{pool: pool, keyID: keyID, key: append([]byte(nil), key...), software: softwareSHA256}
	if err := validateCalibrationRole(ctx, pool, "govar_split_authority", []string{"govar_split_registries", "govar_split_assignments"}); err != nil {
		pool.Close()
		return nil, err
	}
	return a, nil
}

func OpenPostgresCalibrationProducer(ctx context.Context, databaseURL, softwareSHA256 string) (*PostgresCalibrationProducer, error) {
	if strings.TrimSpace(databaseURL) == "" || !isSHA(softwareSHA256) {
		return nil, errors.New("database URL and producer software SHA-256 are required")
	}
	pool, err := openCalibrationRolePool(ctx, databaseURL, "govar_calibration_producer")
	if err != nil {
		return nil, err
	}
	p := &PostgresCalibrationProducer{pool: pool, softwareSHA256: softwareSHA256}
	if err := validateCalibrationRole(ctx, pool, "govar_calibration_producer", []string{"govar_usage_observations", "govar_calibration_artifacts", "govar_drift_windows", "govar_policy_evidence_publications"}); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

func openCalibrationRolePool(ctx context.Context, databaseURL, role string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	prior := config.AfterConnect
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if prior != nil {
			if err := prior(ctx, conn); err != nil {
				return err
			}
		}
		_, err := conn.Exec(ctx, `SET ROLE `+role)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func validateCalibrationRole(ctx context.Context, pool *pgxpool.Pool, role string, insertTables []string) error {
	var current, session, owner, layout string
	var version int
	var superuser, createRole, createDB, replication, bypassRLS, member bool
	if err := pool.QueryRow(ctx, `SELECT current_user,session_user,pg_get_userbyid(c.relowner),
COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname=session_user),false),
COALESCE((SELECT rolcreaterole FROM pg_roles WHERE rolname=session_user),false),
COALESCE((SELECT rolcreatedb FROM pg_roles WHERE rolname=session_user),false),
COALESCE((SELECT rolreplication FROM pg_roles WHERE rolname=session_user),false),
COALESCE((SELECT rolbypassrls FROM pg_roles WHERE rolname=session_user),false),
pg_has_role(session_user,$1,'MEMBER'),(SELECT max(version) FROM govar_schema_migrations),
(SELECT layout_id FROM govar_schema_metadata WHERE version=8)
FROM pg_class c WHERE c.oid=to_regclass('govar_calibration_artifacts')`, role).Scan(&current, &session, &owner,
		&superuser, &createRole, &createDB, &replication, &bypassRLS, &member, &version, &layout); err != nil {
		return err
	}
	if current != role || !member || session == owner || superuser || createRole || createDB || replication || bypassRLS ||
		version != CalibrationLayoutVersion || layout != CalibrationLayoutID {
		return fmt.Errorf("%s requires exact GOV-AR v8 calibration layout", role)
	}
	for _, table := range insertTables {
		var canInsert, canUpdate, canDelete, canTruncate bool
		if err := pool.QueryRow(ctx, `SELECT has_table_privilege(current_user,$1,'INSERT'),has_table_privilege(current_user,$1,'UPDATE'),has_table_privilege(current_user,$1,'DELETE'),has_table_privilege(current_user,$1,'TRUNCATE')`, table).Scan(&canInsert, &canUpdate, &canDelete, &canTruncate); err != nil {
			return err
		}
		if !canInsert || canUpdate || canDelete || canTruncate {
			return fmt.Errorf("%s least-privilege mismatch on %s", role, table)
		}
	}
	return nil
}

func (a *PostgresSplitAuthority) Close() {
	if a != nil && a.pool != nil {
		a.pool.Close()
	}
}
func (p *PostgresCalibrationProducer) Close() {
	if p != nil && p.pool != nil {
		p.pool.Close()
	}
}

func (p *PostgresCalibrationProducer) SoftwareSHA256() string {
	if p == nil {
		return ""
	}
	return p.softwareSHA256
}

// PendingAuthoritativeRequests returns only selected reservations assigned
// before admission and now authoritative-final. No outcome values are exposed
// by this scheduler query, and train/frozen-test assignments are excluded in
// SQL before they reach producer memory.
func (p *PostgresCalibrationProducer) PendingAuthoritativeRequests(ctx context.Context, tenantID, registryID string, limit int) ([]string, error) {
	if p == nil || limit < 1 || limit > 10_000 {
		return nil, errors.New("producer and bounded query limit are required")
	}
	rows, err := p.pool.Query(ctx, `SELECT r.request_id
FROM govar_reservations r JOIN govar_split_assignments a ON a.request_id=r.request_id
LEFT JOIN govar_usage_observations o ON o.request_id=r.request_id
WHERE r.tenant_id=$1 AND a.registry_id=$2 AND a.split IN('development','calibration','monitoring')
AND r.finalized=TRUE AND r.state IN('FINALIZED','LATE_FINALIZED') AND o.request_id IS NULL
ORDER BY r.request_id LIMIT $3`, tenantID, registryID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

func (p *PostgresCalibrationProducer) RegimesForSplit(ctx context.Context, tenantID, registryID, split string) (govarcalibration.RegimeDigests, int64, error) {
	var regimes govarcalibration.RegimeDigests
	if split != govarcalibration.SplitCalibration && split != govarcalibration.SplitMonitoring {
		return regimes, 0, errors.New("only calibration or monitoring regimes may be queried")
	}
	var support, distinct int64
	err := p.pool.QueryRow(ctx, `SELECT count(*),count(DISTINCT ROW(feature_regime_sha256,price_regime_sha256,cap_path_adapter_sha256,split_opportunity_regime_sha256,cohort_sha256,producer_software_sha256)),
min(feature_regime_sha256),min(price_regime_sha256),min(cap_path_adapter_sha256),min(split_opportunity_regime_sha256),min(cohort_sha256),min(producer_software_sha256)
FROM govar_usage_observations WHERE tenant_id=$1 AND registry_id=$2 AND split=$3`, tenantID, registryID, split).Scan(&support, &distinct,
		&regimes.FeatureSHA256, &regimes.PriceSHA256, &regimes.CapPathAdapterSHA256,
		&regimes.SplitOpportunitySHA256, &regimes.CohortSHA256, &regimes.ProducerSoftwareSHA256)
	if err != nil {
		return regimes, 0, err
	}
	if support == 0 {
		return regimes, 0, errors.New("no authoritative observations for split")
	}
	if distinct != 1 {
		return regimes, support, errors.New("split contains multiple immutable regimes")
	}
	if err := govarcalibration.ValidateRegimes(regimes); err != nil {
		return regimes, support, err
	}
	return regimes, support, nil
}

func (a *PostgresSplitAuthority) Register(ctx context.Context, registry SplitRegistry) error {
	normalized, err := normalizeSplitRegistry(registry)
	if err != nil {
		return err
	}
	if normalized.ProducerSoftwareSHA256 != a.software || normalized.AuthorityKeyID != a.keyID ||
		!hmac.Equal([]byte(strings.ToLower(normalized.AuthorityProof)), []byte(authorityMAC(a.key, "govar-split-registry-authority-v1", normalized.RegistrySHA256))) {
		return errors.New("split registry authority or software identity is invalid")
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, normalized.RegistryID); err != nil {
		return err
	}
	var old string
	err = tx.QueryRow(ctx, `SELECT registry_sha256 FROM govar_split_registries WHERE registry_id=$1`, normalized.RegistryID).Scan(&old)
	if err == nil {
		if old != normalized.RegistrySHA256 {
			return errors.New("split registry replay conflicts with immutable registration")
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	requestIDs := make([]string, len(normalized.Assignments))
	for i := range normalized.Assignments {
		requestIDs[i] = normalized.Assignments[i].RequestID
	}
	var already int64
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM govar_reservations WHERE request_id=ANY($1)`, requestIDs).Scan(&already); err != nil {
		return err
	}
	if already != 0 {
		return errors.New("split registration occurred after an admission/opportunity")
	}
	if normalized.FrozenAt.After(time.Now().UTC()) {
		return errors.New("split registry frozen_at is in the future")
	}
	_, err = tx.Exec(ctx, `INSERT INTO govar_split_registries(registry_id,tenant_id,dataset_sha256,split_protocol_sha256,opportunity_set_sha256,cohort_sha256,producer_software_sha256,authority_key_id,authority_proof,registry_sha256,frozen_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		normalized.RegistryID, normalized.TenantID, normalized.DatasetSHA256, normalized.SplitProtocolSHA256,
		normalized.OpportunitySetSHA256, normalized.CohortSHA256, normalized.ProducerSoftwareSHA256,
		normalized.AuthorityKeyID, normalized.AuthorityProof, normalized.RegistrySHA256, normalized.FrozenAt)
	if err != nil {
		return err
	}
	for _, row := range normalized.Assignments {
		_, err = tx.Exec(ctx, `INSERT INTO govar_split_assignments(registry_id,opportunity_id,request_id,split,feature_schema_version,canonical_feature_sha256,feature_regime_sha256,admission_fingerprint_sha256,opportunity_sha256,expected_price_regime_sha256,expected_cap_path_adapter_sha256,split_opportunity_regime_sha256,assignment_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			normalized.RegistryID, row.OpportunityID, row.RequestID, row.Split, row.FeatureSchemaVersion,
			row.CanonicalFeatureSHA256, row.FeatureRegimeSHA256, row.AdmissionFingerprintSHA256, row.OpportunitySHA256,
			row.ExpectedPriceRegimeSHA256, row.ExpectedCapPathAdapterSHA256, row.SplitOpportunityRegimeSHA256, row.AssignmentSHA256)
		if err != nil {
			return err
		}
	}
	after, err := canonicalAuditDigest(normalized)
	if err != nil {
		return err
	}
	if err := appendAuditTx(ctx, tx, govaraudit.Entry{TenantID: normalized.TenantID,
		EventID: "split-registry:" + normalized.RegistrySHA256, EventKind: "COHORT_REGISTRATION",
		PayloadSHA256: normalized.RegistrySHA256, ActorClass: govaraudit.ActorRegistry,
		Reason: "split_registry_frozen", AfterStateSHA256: after, CohortSHA256: normalized.CohortSHA256,
		SoftwareSHA256: normalized.ProducerSoftwareSHA256}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *PostgresCalibrationProducer) MaterializeAuthoritativeObservation(ctx context.Context, requestID string) (govarcalibration.AuthoritativeObservation, error) {
	var result govarcalibration.AuthoritativeObservation
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return result, errors.New("request ID is required")
	}
	// Serializable isolation and the audit commitment below prevent a producer
	// from persisting an observation from a reservation snapshot that changes
	// concurrently. The advisory lock makes retries for one request deterministic.
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "calibration-observation:"+requestID); err != nil {
		return result, err
	}
	err = tx.QueryRow(ctx, `SELECT request_id,provider_attempt_id,opportunity_id,split,output_tokens,settled_at,
feature_regime_sha256,price_regime_sha256,cap_path_adapter_sha256,split_opportunity_regime_sha256,cohort_sha256,producer_software_sha256
FROM govar_usage_observations WHERE request_id=$1`, requestID).Scan(&result.RequestID, &result.ProviderAttemptID,
		&result.OpportunityID, &result.Split, &result.OutputTokens, &result.SettledAt,
		&result.Regimes.FeatureSHA256, &result.Regimes.PriceSHA256, &result.Regimes.CapPathAdapterSHA256,
		&result.Regimes.SplitOpportunitySHA256, &result.Regimes.CohortSHA256, &result.Regimes.ProducerSoftwareSHA256)
	if err == nil {
		return result, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	// The producer intentionally has no UPDATE privilege on the liability
	// ledger. Serializable isolation makes a concurrent final correction abort
	// one side; the audit commitment below rejects a stale or mutated snapshot.
	r, err := loadReservationTx(ctx, tx, requestID, false)
	if err != nil {
		return result, err
	}
	if !r.Finalized || (r.State != StateFinalized && r.State != StateLateFinalized) || r.LastUsageEventID == "" {
		return result, errors.New("reservation is not authoritative-final")
	}
	output, found := int64(0), false
	for _, component := range r.ActualComponents {
		if component.Basis == "output_tokens" {
			if found {
				return result, errors.New("authoritative output usage is duplicated")
			}
			output, found = component.Quantity, true
		}
	}
	if !found {
		return result, errors.New("authoritative output-token usage is absent")
	}
	for _, basis := range r.MissingUsageBases {
		if basis == "output_tokens" {
			return result, errors.New("output-token usage is still provisional")
		}
	}
	var registryID, opportunityID, split, featureRegime, fingerprint, expectedPrice, expectedCap, splitRegime, cohortSHA, registrySoftware string
	var assignedAt, reservationCreated time.Time
	err = tx.QueryRow(ctx, `SELECT a.registry_id,a.opportunity_id,a.split,a.feature_regime_sha256,a.admission_fingerprint_sha256,a.expected_price_regime_sha256,a.expected_cap_path_adapter_sha256,a.split_opportunity_regime_sha256,g.cohort_sha256,g.producer_software_sha256,a.assigned_at,r.created_at
FROM govar_split_assignments a JOIN govar_split_registries g USING(registry_id) JOIN govar_reservations r ON r.request_id=a.request_id
WHERE a.request_id=$1`, requestID).Scan(&registryID, &opportunityID, &split, &featureRegime, &fingerprint, &expectedPrice, &expectedCap, &splitRegime, &cohortSHA, &registrySoftware, &assignedAt, &reservationCreated)
	if err != nil {
		return result, fmt.Errorf("pre-admission split assignment: %w", err)
	}
	if !assignedAt.Before(reservationCreated) {
		return result, errors.New("split assignment was not registered before admission")
	}
	if split == govarcalibration.SplitFrozenTest || split == govarcalibration.SplitTrain {
		return result, fmt.Errorf("split %q outcomes are structurally unavailable to calibration", split)
	}
	if fingerprint != r.AdmissionFingerprint {
		return result, errors.New("registered feature/opportunity does not match the admitted request")
	}
	priceRegime, err := govarcalibration.CanonicalPriceRegime(r.PricingSnapshotSHA256, r.RouteSnapshot.ProviderUID, r.RouteSnapshot.ProviderDeployment)
	if err != nil {
		return result, err
	}
	providerType := "openai"
	if aiopsv1alpha1.GOVARRoutePathMode(r.RouteSnapshot.PathMode) == aiopsv1alpha1.GOVARRouteAzureDeploymentPath {
		providerType = "azure-openai"
	}
	requestParameter, ok := govarpricing.OutputCapRequestParameter(providerType, aiopsv1alpha1.GOVARRoutePathMode(r.RouteSnapshot.PathMode))
	if !ok {
		return result, errors.New("authoritative reservation path has no supported output-cap request parameter")
	}
	capRegime, err := govarcalibration.CanonicalCapPathAdapterRegime(r.CapEvidenceSHA256,
		r.RouteSnapshot.ProviderUID, r.RouteSnapshot.ProviderGeneration, r.RouteSnapshot.ProviderDeployment,
		r.RouteSnapshot.PathMode, govarpricing.CurrentAdapterVersion, requestParameter, r.VerifiedOutputCapTokens)
	if err != nil {
		return result, err
	}
	if priceRegime != expectedPrice || capRegime != expectedCap {
		return result, errors.New("authoritative reservation price or cap/path-adapter regime mismatch")
	}
	var settledAt time.Time
	var ledgerSoftware, sourceReservationSHA string
	err = tx.QueryRow(ctx, `SELECT committed_at,software_sha256,after_state_sha256 FROM govar_audit_events WHERE tenant_id=$1 AND request_id=$2 AND event_id=$3 AND event_kind='SETTLE'`, r.TenantID, r.RequestID, r.LastUsageEventID).Scan(&settledAt, &ledgerSoftware, &sourceReservationSHA)
	if err != nil || !isSHA(ledgerSoftware) {
		return result, errors.New("authoritative settlement audit identity is absent")
	}
	currentReservationSHA, err := reservationAuditDigest(r)
	if err != nil || currentReservationSHA != sourceReservationSHA {
		return result, errors.New("authoritative reservation no longer matches its settlement audit commitment")
	}
	producerRegime, err := govarcalibration.CanonicalDigest("govar-producer-software-regime-v1", p.softwareSHA256, ledgerSoftware, registrySoftware)
	if err != nil {
		return result, err
	}
	result = govarcalibration.AuthoritativeObservation{RequestID: r.RequestID, ProviderAttemptID: r.ProviderAttemptID,
		OpportunityID: opportunityID, Split: split, OutputTokens: output, SettledAt: settledAt.UTC(),
		Regimes: govarcalibration.RegimeDigests{FeatureSHA256: featureRegime, PriceSHA256: priceRegime,
			CapPathAdapterSHA256: expectedCap, SplitOpportunitySHA256: splitRegime,
			CohortSHA256: cohortSHA, ProducerSoftwareSHA256: producerRegime}}
	observationSHA, err := govarcalibration.AuthoritativeObservationDigest([]govarcalibration.AuthoritativeObservation{result})
	if err != nil {
		return result, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO govar_usage_observations(observation_sha256,request_id,tenant_id,provider_attempt_id,opportunity_id,registry_id,split,output_tokens,authoritative_settlement_event_id,settled_at,price_regime_sha256,cap_path_adapter_sha256,feature_regime_sha256,split_opportunity_regime_sha256,cohort_sha256,producer_software_sha256,source_reservation_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		observationSHA, r.RequestID, r.TenantID, r.ProviderAttemptID, opportunityID, registryID, split, output,
		r.LastUsageEventID, settledAt, priceRegime, expectedCap, featureRegime, splitRegime, cohortSHA, producerRegime, sourceReservationSHA)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func (p *PostgresCalibrationProducer) BuildArtifact(ctx context.Context, tenantID, registryID string, cfg govarcalibration.AuthoritativeBuildConfig) (govarcalibration.Artifact, error) {
	var artifact govarcalibration.Artifact
	// READ COMMITTED is intentional here: a transaction waiting on the advisory
	// lock must observe the immutable artifact committed by the prior producer.
	// SERIALIZABLE would establish a stale snapshot before the lock wait.
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return artifact, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	lockKey := "calibration-artifact:" + tenantID + ":" + cfg.ArtifactRef + ":" + cfg.Version
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return artifact, err
	}
	var existingJSON []byte
	err = tx.QueryRow(ctx, `SELECT artifact_json::text::bytea FROM govar_calibration_artifacts WHERE tenant_id=$1 AND artifact_ref=$2 AND artifact_version=$3`, tenantID, cfg.ArtifactRef, cfg.Version).Scan(&existingJSON)
	if err == nil {
		if err := json.Unmarshal(existingJSON, &artifact); err != nil {
			return artifact, err
		}
		if artifact.ArtifactRef != cfg.ArtifactRef || artifact.Version != cfg.Version || artifact.FeatureSchemaVersion != cfg.FeatureSchemaVersion || artifact.CoverageTargetPPB != cfg.CoverageTargetPPB ||
			artifact.MinimumSupport != cfg.MinimumSupport || artifact.FeatureRegimeSHA256 != cfg.Regimes.FeatureSHA256 ||
			artifact.PriceRegimeSHA256 != cfg.Regimes.PriceSHA256 || artifact.CapRegimeSHA256 != cfg.Regimes.CapPathAdapterSHA256 ||
			artifact.SplitOpportunityRegimeSHA256 != cfg.Regimes.SplitOpportunitySHA256 || artifact.CohortRegimeSHA256 != cfg.Regimes.CohortSHA256 ||
			artifact.ProducerSoftwareSHA256 != cfg.Regimes.ProducerSoftwareSHA256 {
			return artifact, errors.New("artifact identity replay conflicts with immutable build configuration")
		}
		return artifact, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return artifact, err
	}
	rows, err := tx.Query(ctx, `SELECT request_id,provider_attempt_id,opportunity_id,split,output_tokens,settled_at,feature_regime_sha256,price_regime_sha256,cap_path_adapter_sha256,split_opportunity_regime_sha256,cohort_sha256,producer_software_sha256 FROM govar_usage_observations WHERE tenant_id=$1 AND registry_id=$2 AND split='calibration' ORDER BY request_id`, tenantID, registryID)
	if err != nil {
		return artifact, err
	}
	var observations []govarcalibration.AuthoritativeObservation
	for rows.Next() {
		var row govarcalibration.AuthoritativeObservation
		if err := rows.Scan(&row.RequestID, &row.ProviderAttemptID, &row.OpportunityID, &row.Split, &row.OutputTokens, &row.SettledAt,
			&row.Regimes.FeatureSHA256, &row.Regimes.PriceSHA256, &row.Regimes.CapPathAdapterSHA256,
			&row.Regimes.SplitOpportunitySHA256, &row.Regimes.CohortSHA256, &row.Regimes.ProducerSoftwareSHA256); err != nil {
			rows.Close()
			return artifact, err
		}
		observations = append(observations, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return artifact, err
	}
	artifact, err = govarcalibration.BuildAuthoritativeArtifact(cfg, observations)
	if err != nil {
		return artifact, err
	}
	artifactJSON, err := json.Marshal(artifact)
	if err != nil {
		return artifact, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO govar_calibration_artifacts(artifact_sha256,artifact_ref,artifact_version,tenant_id,registry_id,calibration_method,coverage_target_ppb,minimum_support,support,conformal_rank,coverage_bound_numerator,coverage_bound_denominator,coverage_interval_lower_ppb,coverage_interval_upper_ppb,coverage_confidence_ppb,upper_output_tokens,source_observations_sha256,feature_regime_sha256,price_regime_sha256,cap_path_adapter_sha256,split_opportunity_regime_sha256,cohort_sha256,producer_software_sha256,window_start,window_end,artifact_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)`,
		artifact.ArtifactSHA256, artifact.ArtifactRef, artifact.Version, tenantID, registryID, artifact.CalibrationMethod,
		artifact.CoverageTargetPPB, artifact.MinimumSupport, artifact.Support, artifact.ConformalRank, artifact.CoverageBoundNumerator,
		artifact.CoverageBoundDenominator, artifact.CoverageIntervalLowerPPB, artifact.CoverageIntervalUpperPPB,
		artifact.CoverageConfidencePPB, artifact.UpperOutputTokens, artifact.SourceObservationsSHA256,
		artifact.FeatureRegimeSHA256, artifact.PriceRegimeSHA256, artifact.CapRegimeSHA256,
		artifact.SplitOpportunityRegimeSHA256, artifact.CohortRegimeSHA256, artifact.ProducerSoftwareSHA256,
		artifact.WindowStart, artifact.WindowEnd, artifactJSON)
	if err != nil {
		return artifact, err
	}
	after, err := canonicalAuditDigest(artifact)
	if err != nil {
		return artifact, err
	}
	if err := appendAuditTx(ctx, tx, govaraudit.Entry{TenantID: tenantID, EventID: "calibration-publication:" + artifact.ArtifactSHA256,
		EventKind: "CALIBRATION_PUBLICATION", PayloadSHA256: artifact.SourceObservationsSHA256,
		ActorClass: govaraudit.ActorCalibration, Reason: "calibration_artifact_built", AfterStateSHA256: after,
		PricingSnapshotSHA256: artifact.PriceRegimeSHA256, CapEvidenceSHA256: artifact.CapRegimeSHA256,
		CohortSHA256: artifact.CohortRegimeSHA256, SoftwareSHA256: p.softwareSHA256,
		CalibrationSHA256: artifact.ArtifactSHA256}); err != nil {
		return artifact, err
	}
	return artifact, tx.Commit(ctx)
}

func (p *PostgresCalibrationProducer) BuildDriftWindow(ctx context.Context, tenantID, registryID, artifactSHA256 string, thresholdPPB int64, regimes govarcalibration.RegimeDigests) (govarcalibration.AuthoritativeDriftWindow, error) {
	var window govarcalibration.AuthoritativeDriftWindow
	if !isSHA(artifactSHA256) {
		return window, errors.New("artifact SHA-256 is required")
	}
	// As for artifacts, a producer that waited on the advisory lock must see the
	// prior producer's immutable publication before applying idempotent replay.
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return window, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	lockKey := "calibration-drift:" + tenantID + ":" + artifactSHA256 + ":" + fmt.Sprint(thresholdPPB)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return window, err
	}
	var artifactJSON []byte
	if err = tx.QueryRow(ctx, `SELECT artifact_json::text::bytea FROM govar_calibration_artifacts WHERE tenant_id=$1 AND artifact_sha256=$2`, tenantID, artifactSHA256).Scan(&artifactJSON); err != nil {
		return window, err
	}
	var artifact govarcalibration.Artifact
	if err = json.Unmarshal(artifactJSON, &artifact); err != nil {
		return window, err
	}
	rows, err := tx.Query(ctx, `SELECT request_id,provider_attempt_id,opportunity_id,split,output_tokens,settled_at,feature_regime_sha256,price_regime_sha256,cap_path_adapter_sha256,split_opportunity_regime_sha256,cohort_sha256,producer_software_sha256 FROM govar_usage_observations WHERE tenant_id=$1 AND registry_id=$2 AND split='monitoring' ORDER BY request_id`, tenantID, registryID)
	if err != nil {
		return window, err
	}
	var observations []govarcalibration.AuthoritativeObservation
	for rows.Next() {
		var row govarcalibration.AuthoritativeObservation
		if err := rows.Scan(&row.RequestID, &row.ProviderAttemptID, &row.OpportunityID, &row.Split, &row.OutputTokens, &row.SettledAt,
			&row.Regimes.FeatureSHA256, &row.Regimes.PriceSHA256, &row.Regimes.CapPathAdapterSHA256,
			&row.Regimes.SplitOpportunitySHA256, &row.Regimes.CohortSHA256, &row.Regimes.ProducerSoftwareSHA256); err != nil {
			rows.Close()
			return window, err
		}
		observations = append(observations, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return window, err
	}
	window, err = govarcalibration.BuildAuthoritativeDriftWindow(artifact, regimes, thresholdPPB, observations)
	if err != nil {
		return window, err
	}
	result, err := tx.Exec(ctx, `INSERT INTO govar_drift_windows(drift_sha256,artifact_sha256,tenant_id,detector,threshold_ppb,support,covered,empirical_coverage_ppb,confidence_ppb,interval_lower_ppb,interval_upper_ppb,detected,source_observations_sha256,feature_regime_sha256,price_regime_sha256,cap_path_adapter_sha256,split_opportunity_regime_sha256,cohort_sha256,producer_software_sha256,window_start,window_end) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21) ON CONFLICT(drift_sha256) DO NOTHING`,
		window.DriftSHA256, artifactSHA256, tenantID, window.Result.Detector, window.Result.ThresholdPPB,
		window.Result.Support, window.Result.Covered, window.Result.EmpiricalCoveragePPB, window.Result.ConfidencePPB,
		window.Result.IntervalLowerPPB, window.Result.IntervalUpperPPB, window.Result.Detected,
		window.Result.MonitoringInputSHA256, regimes.FeatureSHA256, regimes.PriceSHA256,
		regimes.CapPathAdapterSHA256, regimes.SplitOpportunitySHA256, regimes.CohortSHA256,
		regimes.ProducerSoftwareSHA256, window.Result.WindowStart, window.Result.WindowEnd)
	if err != nil {
		return window, err
	}
	if result.RowsAffected() > 0 {
		after, err := canonicalAuditDigest(window)
		if err != nil {
			return window, err
		}
		if err := appendAuditTx(ctx, tx, govaraudit.Entry{TenantID: tenantID, EventID: "drift-change:" + window.DriftSHA256,
			EventKind: "DRIFT_CHANGE", PayloadSHA256: window.Result.MonitoringInputSHA256,
			ActorClass: govaraudit.ActorCalibration, Reason: "drift_window_built", AfterStateSHA256: after,
			PricingSnapshotSHA256: regimes.PriceSHA256, CapEvidenceSHA256: regimes.CapPathAdapterSHA256,
			CohortSHA256: regimes.CohortSHA256, SoftwareSHA256: p.softwareSHA256,
			CalibrationSHA256: artifactSHA256}); err != nil {
			return window, err
		}
	}
	return window, tx.Commit(ctx)
}

func (p *PostgresCalibrationProducer) PublishPolicyEvidence(ctx context.Context, publication PolicyEvidencePublication) error {
	if publication.PolicyGeneration <= 0 || strings.TrimSpace(publication.ExpectedResourceVersion) == "" ||
		!isSHA(publication.PublicationSHA256) || !isSHA(publication.ArtifactSHA256) || !isSHA(publication.StatusPayloadSHA256) ||
		publication.ProducerSoftwareSHA256 != p.softwareSHA256 || (publication.DriftSHA256 != "" && !isSHA(publication.DriftSHA256)) {
		return errors.New("complete immutable policy publication identity is required")
	}
	expectedPublication, err := govarcalibration.CanonicalDigest("govar-policy-evidence-publication-v1",
		publication.TenantID, publication.PolicyNamespace, publication.PolicyName, publication.PolicyUID,
		fmt.Sprint(publication.PolicyGeneration), publication.ExpectedResourceVersion, publication.ArtifactSHA256,
		publication.DriftSHA256, publication.StatusPayloadSHA256, publication.ProducerSoftwareSHA256)
	if err != nil || expectedPublication != publication.PublicationSHA256 {
		return errors.New("policy publication digest does not match canonical identity")
	}
	// The advisory lock serializes publication identity; READ COMMITTED lets a
	// waiter observe the immutable row just committed by another producer.
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	lock := "policy-evidence:" + publication.PolicyUID
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lock); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `INSERT INTO govar_policy_evidence_publications(publication_sha256,tenant_id,policy_namespace,policy_name,policy_uid,policy_generation,expected_resource_version,artifact_sha256,drift_sha256,status_payload_sha256,producer_software_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),$10,$11) ON CONFLICT(publication_sha256) DO NOTHING`,
		publication.PublicationSHA256, publication.TenantID, publication.PolicyNamespace, publication.PolicyName,
		publication.PolicyUID, publication.PolicyGeneration, publication.ExpectedResourceVersion, publication.ArtifactSHA256,
		publication.DriftSHA256, publication.StatusPayloadSHA256, publication.ProducerSoftwareSHA256)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		var stored string
		if err := tx.QueryRow(ctx, `SELECT status_payload_sha256 FROM govar_policy_evidence_publications WHERE publication_sha256=$1`, publication.PublicationSHA256).Scan(&stored); err != nil {
			return err
		}
		if stored != publication.StatusPayloadSHA256 {
			return errors.New("policy publication replay conflicts with immutable payload")
		}
	} else {
		after, err := canonicalAuditDigest(publication)
		if err != nil {
			return err
		}
		if err := appendAuditTx(ctx, tx, govaraudit.Entry{TenantID: publication.TenantID,
			EventID:   "calibration-publication-status:" + publication.PublicationSHA256,
			EventKind: "CALIBRATION_PUBLICATION", PayloadSHA256: publication.StatusPayloadSHA256,
			ActorClass: govaraudit.ActorCalibration, Reason: "policy_evidence_published", AfterStateSHA256: after,
			SoftwareSHA256: p.softwareSHA256, CalibrationSHA256: publication.ArtifactSHA256}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func calibrationSplit(split string) bool {
	switch split {
	case govarcalibration.SplitTrain, govarcalibration.SplitDevelopment, govarcalibration.SplitCalibration, govarcalibration.SplitMonitoring, govarcalibration.SplitFrozenTest:
		return true
	default:
		return false
	}
}

func isSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
