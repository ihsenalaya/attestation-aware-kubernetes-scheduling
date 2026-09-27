package govar

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
)

type postgresV8Fixture struct {
	t            *testing.T
	ctx          context.Context
	admin        *pgx.Conn
	database     string
	adminURL     string
	logins       []string
	core         *PostgresEngine
	authority    *PostgresSplitAuthority
	producers    []*PostgresCalibrationProducer
	software     string
	authorityKey []byte
	keyID        string
}

func newPostgresV8Fixture(t *testing.T, producerCount int) *postgresV8Fixture {
	t.Helper()
	base := os.Getenv("GOVAR_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("GOVAR_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	suffix := randomTestToken(t, 8)
	database := "govar_v8_" + suffix
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{database}.Sanitize()); err != nil {
		admin.Close(ctx)
		t.Fatal(err)
	}
	dbURL := calibrationTestURL(t, base, database, "", "")
	bootstrap, err := NewPostgresEngine(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap.Close()
	migrationPath := filepath.Join("..", "..", "..", "article3", "infra", "postgres", "migrate_v7_to_v8.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatal(err)
	}
	migrationSQL := strings.Replace(string(migration), "\\set ON_ERROR_STOP on\n", "", 1)
	owner, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, migrationSQL); err != nil {
		owner.Close(ctx)
		t.Fatal(err)
	}
	owner.Close(ctx)
	f := &postgresV8Fixture{t: t, ctx: ctx, admin: admin, database: database, adminURL: base,
		software: eventPayloadHash("v8-producer-software"), authorityKey: []byte(strings.Repeat("split-authority-key-", 2)), keyID: "test-split-authority"}
	runtimeURL := f.createLogin("runtime", "govar_runtime")
	authorityURL := f.createLogin("split", "govar_split_authority")
	core, err := OpenPostgresEngine(ctx, runtimeURL, f.software)
	if err != nil {
		t.Fatal(err)
	}
	f.core = core
	authority, err := OpenPostgresSplitAuthority(ctx, authorityURL, f.keyID, f.authorityKey, f.software)
	if err != nil {
		t.Fatal(err)
	}
	f.authority = authority
	for i := 0; i < producerCount; i++ {
		producerURL := f.createLogin("producer"+string(rune('a'+i)), "govar_calibration_producer")
		producer, err := OpenPostgresCalibrationProducer(ctx, producerURL, f.software)
		if err != nil {
			t.Fatal(err)
		}
		f.producers = append(f.producers, producer)
	}
	t.Cleanup(f.close)
	return f
}

func (f *postgresV8Fixture) createLogin(label, role string) string {
	f.t.Helper()
	login := "govar_" + label + "_" + randomTestToken(f.t, 6)
	password := eventPayloadHash("ephemeral-test-login", login)
	_, err := f.admin.Exec(f.ctx, `CREATE ROLE `+pgx.Identifier{login}.Sanitize()+` LOGIN PASSWORD '`+password+`' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS; GRANT `+pgx.Identifier{role}.Sanitize()+` TO `+pgx.Identifier{login}.Sanitize())
	if err != nil {
		f.t.Fatal(err)
	}
	f.logins = append(f.logins, login)
	return calibrationTestURL(f.t, f.adminURL, f.database, login, password)
}

func (f *postgresV8Fixture) close() {
	for _, p := range f.producers {
		p.Close()
	}
	if f.authority != nil {
		f.authority.Close()
	}
	if f.core != nil {
		f.core.Close()
	}
	if f.admin == nil {
		return
	}
	_, _ = f.admin.Exec(f.ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()`, f.database)
	_, _ = f.admin.Exec(f.ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{f.database}.Sanitize())
	for _, login := range f.logins {
		_, _ = f.admin.Exec(f.ctx, `DROP ROLE IF EXISTS `+pgx.Identifier{login}.Sanitize())
	}
	_ = f.admin.Close(f.ctx)
}

func calibrationTestURL(t *testing.T, raw, database, user, password string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + database
	if user != "" {
		u.User = url.UserPassword(user, password)
	}
	return u.String()
}

func randomTestToken(t *testing.T, bytes int) string {
	t.Helper()
	b := make([]byte, bytes)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func (f *postgresV8Fixture) registerRequests(splits map[string]string, mutate func(string, *SplitAssignment)) SplitRegistry {
	f.t.Helper()
	candidate := defaultCandidates()[0]
	price, err := govarcalibration.CanonicalPriceRegime(candidate.PricingSnapshot.SnapshotSHA256, candidate.RouteSnapshot.ProviderUID, candidate.RouteSnapshot.ProviderDeployment)
	if err != nil {
		f.t.Fatal(err)
	}
	capRegime, err := CanonicalCandidateCapPathAdapterRegime(candidate)
	if err != nil {
		f.t.Fatal(err)
	}
	ids := make([]string, 0, len(splits))
	for id := range splits {
		ids = append(ids, id)
	}
	sortStrings(ids)
	assignments := make([]SplitAssignment, 0, len(ids))
	for _, id := range ids {
		req := admitRequest(id, testTenant, testWorkload)
		a := SplitAssignment{OpportunityID: "op-" + id, RequestID: id, Split: splits[id], FeatureSchemaVersion: "features-v1",
			CanonicalFeatureSHA256: eventPayloadHash("shared-feature"), AdmissionFingerprintSHA256: admissionFingerprint(req, defaultBudget(), defaultRouting(), defaultCandidates()),
			OpportunitySHA256: OpportunityDigest(req), ExpectedPriceRegimeSHA256: price, ExpectedCapPathAdapterSHA256: capRegime}
		if mutate != nil {
			mutate(id, &a)
		}
		assignments = append(assignments, a)
	}
	registry := SplitRegistry{RegistryID: "registry-" + randomTestToken(f.t, 5), TenantID: testTenant,
		DatasetSHA256: eventPayloadHash("dataset"), SplitProtocolSHA256: eventPayloadHash("split-protocol"),
		OpportunitySetSHA256: eventPayloadHash("opportunity-set"), CohortSHA256: eventPayloadHash("cohort"),
		ProducerSoftwareSHA256: f.software, FrozenAt: time.Now().UTC().Add(-time.Minute), Assignments: assignments}
	registry, err = SignSplitRegistry(registry, f.keyID, f.authorityKey)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.authority.Register(f.ctx, registry); err != nil {
		f.t.Fatal(err)
	}
	return registry
}

func sortStrings(values []string) {
	for i := range values {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}

func (f *postgresV8Fixture) settleRequests(ids ...string) {
	f.t.Helper()
	for i, id := range ids {
		admit, err := f.core.Admit(admitRequest(id, testTenant, testWorkload), defaultBudget(), defaultRouting(), defaultCandidates())
		if err != nil || admit.Decision != DecisionAdmit {
			f.t.Fatalf("admit %s=(%+v,%v)", id, admit, err)
		}
		if _, _, err := f.core.Dispatch(dispatchRequest(id, "dispatch-"+id, admit.ProviderAttemptID, DispatchClaimed, testTenant, testWorkload)); err != nil {
			f.t.Fatal(err)
		}
		settle := SettleRequest{RequestID: id, SettlementID: "settle-" + id, ProviderAttemptID: admit.ProviderAttemptID,
			TenantID: testTenant, WorkloadUID: testWorkload,
			Usage:        []govarpricing.UsageQuantity{{Basis: aiopsv1alpha1.ProviderBasisInputTokens, Quantity: 1}, {Basis: aiopsv1alpha1.ProviderBasisOutputTokens, Quantity: int64(i + 1)}},
			UsageVersion: 1, Final: true, AuthenticatedTenantID: testTenant, AuthenticatedWorkloadUID: testWorkload}
		if _, code, err := f.core.Settle(settle); err != nil || code != ReasonFinalized {
			f.t.Fatalf("settle %s=(%s,%v)", id, code, err)
		}
	}
}

func TestPostgresV8SplitRegistryPreAdmissionImmutable(t *testing.T) {
	f := newPostgresV8Fixture(t, 1)
	registry := f.registerRequests(map[string]string{"v8-split-a": govarcalibration.SplitCalibration}, nil)
	if err := f.authority.Register(f.ctx, registry); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if _, err := f.authority.pool.Exec(f.ctx, `UPDATE govar_split_registries SET dataset_sha256=$2 WHERE registry_id=$1`, registry.RegistryID, eventPayloadHash("mutated")); err == nil {
		t.Fatal("immutable split registry update succeeded")
	}
	if _, err := f.authority.pool.Exec(f.ctx, `INSERT INTO govar_split_assignments(registry_id,opportunity_id,request_id,split,feature_schema_version,canonical_feature_sha256,feature_regime_sha256,admission_fingerprint_sha256,opportunity_sha256,expected_price_regime_sha256,expected_cap_path_adapter_sha256,split_opportunity_regime_sha256,assignment_sha256) SELECT registry_id,'late','late','calibration','v1',dataset_sha256,dataset_sha256,dataset_sha256,dataset_sha256,dataset_sha256,dataset_sha256,dataset_sha256,dataset_sha256 FROM govar_split_registries WHERE registry_id=$1`, registry.RegistryID); err == nil {
		t.Fatal("post-registration assignment insert succeeded")
	}
	f.settleRequests("v8-split-a")
	late := registry
	late.RegistryID, late.RegistrySHA256, late.AuthorityProof = "late-registry", "", ""
	for i := range late.Assignments {
		late.Assignments[i].AssignmentSHA256 = ""
		late.Assignments[i].SplitOpportunityRegimeSHA256 = ""
	}
	late, err := SignSplitRegistry(late, f.keyID, f.authorityKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.authority.Register(f.ctx, late); err == nil || !strings.Contains(err.Error(), "after an admission") {
		t.Fatalf("future registration accepted: %v", err)
	}
}

func TestPostgresV8AuthoritativeObservationRejectsLeakageAndHiddenOutcomes(t *testing.T) {
	f := newPostgresV8Fixture(t, 1)
	f.registerRequests(map[string]string{"v8-cal": govarcalibration.SplitCalibration, "v8-monitor": govarcalibration.SplitMonitoring, "v8-frozen": govarcalibration.SplitFrozenTest}, nil)
	f.settleRequests("v8-cal", "v8-monitor", "v8-frozen")
	if row, err := f.producers[0].MaterializeAuthoritativeObservation(f.ctx, "v8-cal"); err != nil || row.Split != govarcalibration.SplitCalibration || row.OutputTokens < 0 {
		t.Fatalf("calibration row=(%+v,%v)", row, err)
	}
	if _, err := f.producers[0].MaterializeAuthoritativeObservation(f.ctx, "v8-monitor"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.producers[0].MaterializeAuthoritativeObservation(f.ctx, "v8-frozen"); err == nil || !strings.Contains(err.Error(), "structurally unavailable") {
		t.Fatalf("frozen outcome exposed: %v", err)
	}
	var frozen, flags int
	if err := f.producers[0].pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM govar_usage_observations WHERE split='frozen_test'),(SELECT count(*) FROM information_schema.columns WHERE table_name='govar_usage_observations' AND column_name IN('selected','authoritative_final','excluded'))`).Scan(&frozen, &flags); err != nil {
		t.Fatal(err)
	}
	if frozen != 0 || flags != 0 {
		t.Fatalf("hidden/flag authority leaked: frozen=%d flags=%d", frozen, flags)
	}
	if _, err := f.producers[0].pool.Exec(f.ctx, `UPDATE govar_usage_observations SET output_tokens=999 WHERE request_id='v8-cal'`); err == nil {
		t.Fatal("observation mutation succeeded")
	}
}

func TestPostgresV8RegimeMismatchFailsClosed(t *testing.T) {
	f := newPostgresV8Fixture(t, 1)
	f.registerRequests(map[string]string{"v8-price-mismatch": govarcalibration.SplitCalibration}, func(_ string, a *SplitAssignment) { a.ExpectedPriceRegimeSHA256 = eventPayloadHash("wrong-price") })
	f.settleRequests("v8-price-mismatch")
	if _, err := f.producers[0].MaterializeAuthoritativeObservation(f.ctx, "v8-price-mismatch"); err == nil || !strings.Contains(err.Error(), "regime mismatch") {
		t.Fatalf("price mismatch accepted: %v", err)
	}
}

func TestPostgresV8TwoProducerArtifactIdempotence(t *testing.T) {
	f := newPostgresV8Fixture(t, 2)
	splits := map[string]string{}
	ids := []string{}
	for i := 0; i < 5; i++ {
		id := "v8-artifact-" + string(rune('a'+i))
		ids = append(ids, id)
		splits[id] = govarcalibration.SplitCalibration
	}
	registry := f.registerRequests(splits, nil)
	f.settleRequests(ids...)
	for _, id := range ids {
		if _, err := f.producers[0].MaterializeAuthoritativeObservation(f.ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	regimes, support, err := f.producers[0].RegimesForSplit(f.ctx, testTenant, registry.RegistryID, govarcalibration.SplitCalibration)
	if err != nil || support != 5 {
		t.Fatalf("regimes support=(%d,%v)", support, err)
	}
	cfg := govarcalibration.AuthoritativeBuildConfig{ArtifactRef: "artifact-v8", Version: "v1", FeatureSchemaVersion: "features-v1", CoverageTargetPPB: 800_000_000, MinimumSupport: 5, Regimes: regimes}
	results := make([]govarcalibration.Artifact, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range f.producers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = f.producers[i].BuildArtifact(f.ctx, testTenant, registry.RegistryID, cfg)
		}(i)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || results[0].ArtifactSHA256 == "" || results[0].ArtifactSHA256 != results[1].ArtifactSHA256 {
		t.Fatalf("producer results=(%+v,%+v) errors=(%v,%v)", results[0], results[1], errs[0], errs[1])
	}
	var artifacts, audits int
	if err := f.producers[0].pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM govar_calibration_artifacts),(SELECT count(*) FROM govar_audit_events WHERE event_kind='CALIBRATION_PUBLICATION' AND calibration_sha256=$1)`, results[0].ArtifactSHA256).Scan(&artifacts, &audits); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 || audits != 1 {
		t.Fatalf("idempotence artifacts=%d audits=%d", artifacts, audits)
	}
	conflict := cfg
	conflict.CoverageTargetPPB--
	if _, err := f.producers[0].BuildArtifact(f.ctx, testTenant, registry.RegistryID, conflict); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting replay accepted: %v", err)
	}
}

func TestPostgresV8DriftWindowUsesMonitoringOnlyAndIntervals(t *testing.T) {
	f := newPostgresV8Fixture(t, 1)
	splits := map[string]string{}
	calibrationIDs, monitoringIDs := []string{}, []string{}
	for i := 0; i < 5; i++ {
		calibrationID := "v8-drift-cal-" + string(rune('a'+i))
		monitoringID := "v8-drift-monitor-" + string(rune('a'+i))
		calibrationIDs = append(calibrationIDs, calibrationID)
		monitoringIDs = append(monitoringIDs, monitoringID)
		splits[calibrationID] = govarcalibration.SplitCalibration
		splits[monitoringID] = govarcalibration.SplitMonitoring
	}
	registry := f.registerRequests(splits, nil)
	allIDs := append(append([]string{}, calibrationIDs...), monitoringIDs...)
	f.settleRequests(allIDs...)
	for _, id := range allIDs {
		if _, err := f.producers[0].MaterializeAuthoritativeObservation(f.ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	calibrationRegimes, calibrationSupport, err := f.producers[0].RegimesForSplit(f.ctx, testTenant, registry.RegistryID, govarcalibration.SplitCalibration)
	if err != nil || calibrationSupport != 5 {
		t.Fatalf("calibration regimes support=(%d,%v)", calibrationSupport, err)
	}
	artifact, err := f.producers[0].BuildArtifact(f.ctx, testTenant, registry.RegistryID, govarcalibration.AuthoritativeBuildConfig{
		ArtifactRef: "artifact-v8-drift", Version: "v1", FeatureSchemaVersion: "features-v1",
		CoverageTargetPPB: 800_000_000, MinimumSupport: 5, Regimes: calibrationRegimes,
	})
	if err != nil {
		t.Fatal(err)
	}
	monitoringRegimes, monitoringSupport, err := f.producers[0].RegimesForSplit(f.ctx, testTenant, registry.RegistryID, govarcalibration.SplitMonitoring)
	if err != nil || monitoringSupport != 5 {
		t.Fatalf("monitoring regimes support=(%d,%v)", monitoringSupport, err)
	}
	window, err := f.producers[0].BuildDriftWindow(f.ctx, testTenant, registry.RegistryID, artifact.ArtifactSHA256, 50_000_000, monitoringRegimes)
	if err != nil {
		t.Fatal(err)
	}
	if window.Result.Support != 5 || window.Result.Covered != 0 || !window.Result.Detected ||
		window.Result.ConfidencePPB != 950_000_000 || window.Result.IntervalLowerPPB < 0 ||
		window.Result.IntervalUpperPPB <= window.Result.IntervalLowerPPB || window.DriftSHA256 == "" {
		t.Fatalf("invalid drift window: %+v", window)
	}
	var rows, audits int
	if err := f.producers[0].pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM govar_drift_windows WHERE drift_sha256=$1),(SELECT count(*) FROM govar_audit_events WHERE event_kind='DRIFT_CHANGE' AND event_id=$2)`, window.DriftSHA256, "drift-change:"+window.DriftSHA256).Scan(&rows, &audits); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || audits != 1 {
		t.Fatalf("drift publication rows=%d audits=%d", rows, audits)
	}
	// Exact replay is immutable and does not append another audit event.
	if replay, err := f.producers[0].BuildDriftWindow(f.ctx, testTenant, registry.RegistryID, artifact.ArtifactSHA256, 50_000_000, monitoringRegimes); err != nil || replay.DriftSHA256 != window.DriftSHA256 {
		t.Fatalf("drift replay=(%+v,%v)", replay, err)
	}
	if _, err := f.producers[0].BuildDriftWindow(f.ctx, testTenant, registry.RegistryID, artifact.ArtifactSHA256, 50_000_000, calibrationRegimes); err == nil || !strings.Contains(err.Error(), "regime") {
		t.Fatalf("calibration-split regime accepted as monitoring evidence: %v", err)
	}
}

func TestPostgresV8PolicyPublicationCASIdentity(t *testing.T) {
	f := newPostgresV8Fixture(t, 1)
	// The database layer commits the exact expected resourceVersion and rejects
	// a different payload for the same canonical publication identity. The
	// Kubernetes publisher separately performs the API-server resourceVersion CAS.
	artifactSHA := eventPayloadHash("artifact-placeholder")
	// Insert a minimal constraint-valid artifact through the owner solely to
	// isolate publication idempotence from the artifact-builder test above.
	owner, err := pgx.Connect(f.ctx, calibrationTestURL(t, f.adminURL, f.database, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.Exec(f.ctx, `INSERT INTO govar_split_registries(registry_id,tenant_id,dataset_sha256,split_protocol_sha256,opportunity_set_sha256,cohort_sha256,producer_software_sha256,authority_key_id,authority_proof,registry_sha256,frozen_at) VALUES('pub-reg',$1,$2,$2,$2,$2,$2,'key',$2,$2,clock_timestamp())`, testTenant, eventPayloadHash("pub"))
	if err == nil {
		_, err = owner.Exec(f.ctx, `INSERT INTO govar_calibration_artifacts(artifact_sha256,artifact_ref,artifact_version,tenant_id,registry_id,calibration_method,coverage_target_ppb,minimum_support,support,conformal_rank,coverage_bound_numerator,coverage_bound_denominator,coverage_interval_lower_ppb,coverage_interval_upper_ppb,coverage_confidence_ppb,upper_output_tokens,source_observations_sha256,feature_regime_sha256,price_regime_sha256,cap_path_adapter_sha256,split_opportunity_regime_sha256,cohort_sha256,producer_software_sha256,window_start,window_end,artifact_json) VALUES($1,'a','v',$2,'pub-reg','split_conformal_order_statistic_v1',500000000,1,1,1,1,2,500000000,1000000000,1000000000,1,$3,$3,$3,$3,$3,$3,$3,clock_timestamp(),clock_timestamp(),'{}')`, artifactSHA, testTenant, eventPayloadHash("pub-fields"))
	}
	owner.Close(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := PolicyEvidencePublication{TenantID: testTenant, PolicyNamespace: "ns", PolicyName: "policy", PolicyUID: "uid", PolicyGeneration: 3, ExpectedResourceVersion: "19", ArtifactSHA256: artifactSHA, StatusPayloadSHA256: eventPayloadHash("status"), ProducerSoftwareSHA256: f.software}
	p.PublicationSHA256, _ = govarcalibration.CanonicalDigest("govar-policy-evidence-publication-v1", p.TenantID, p.PolicyNamespace, p.PolicyName, p.PolicyUID, "3", p.ExpectedResourceVersion, p.ArtifactSHA256, p.DriftSHA256, p.StatusPayloadSHA256, p.ProducerSoftwareSHA256)
	if err := f.producers[0].PublishPolicyEvidence(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := f.producers[0].PublishPolicyEvidence(f.ctx, p); err != nil {
		t.Fatalf("exact publication replay: %v", err)
	}
	p.StatusPayloadSHA256 = eventPayloadHash("changed-status")
	if err := f.producers[0].PublishPolicyEvidence(f.ctx, p); err == nil {
		t.Fatal("changed publication payload was accepted")
	}
}

func TestPostgresV7ToV8MigrationReplayAndRollbackRefusal(t *testing.T) {
	f := newPostgresV8Fixture(t, 1)
	var version int
	var layout string
	if err := f.producers[0].pool.QueryRow(f.ctx, `SELECT max(version),(SELECT layout_id FROM govar_schema_metadata WHERE version=8) FROM govar_schema_migrations`).Scan(&version, &layout); err != nil {
		t.Fatal(err)
	}
	if version != 8 || layout != CalibrationLayoutID {
		t.Fatalf("layout=(%d,%s)", version, layout)
	}
	owner, err := pgx.Connect(f.ctx, calibrationTestURL(t, f.adminURL, f.database, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	migration, _ := os.ReadFile(filepath.Join("..", "..", "..", "article3", "infra", "postgres", "migrate_v7_to_v8.sql"))
	if _, err := owner.Exec(f.ctx, strings.Replace(string(migration), "\\set ON_ERROR_STOP on\n", "", 1)); err != nil {
		t.Fatalf("migration replay: %v", err)
	}
	if _, err := owner.Exec(f.ctx, `INSERT INTO govar_schema_migrations(version) VALUES(9)`); err != nil {
		t.Fatal(err)
	}
	owner.Close(f.ctx)
	runtimeURL := f.createLogin("rollback", "govar_runtime")
	if engine, err := OpenPostgresEngine(f.ctx, runtimeURL, f.software); err == nil {
		engine.Close()
		t.Fatal("v8 binary accepted newer v9 schema")
	}
}
