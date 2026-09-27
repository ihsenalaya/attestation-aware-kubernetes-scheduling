package govarcalibration

import (
	"strings"
	"testing"
	"time"
)

func authDigest(label string) string {
	value, err := CanonicalDigest("test", label)
	if err != nil {
		panic(err)
	}
	return value
}

func authoritativeConfig(n int64) AuthoritativeBuildConfig {
	return AuthoritativeBuildConfig{
		ArtifactRef: "artifact-a", Version: "v1", FeatureSchemaVersion: "features-v1", CoverageTargetPPB: 800_000_000, MinimumSupport: n,
		Regimes: RegimeDigests{
			FeatureSHA256: authDigest("feature"), PriceSHA256: authDigest("price"),
			CapPathAdapterSHA256: authDigest("cap"), SplitOpportunitySHA256: authDigest("split"),
			CohortSHA256: authDigest("cohort"), ProducerSoftwareSHA256: authDigest("software"),
		},
	}
}

func authoritativeRows(cfg AuthoritativeBuildConfig, n int) []AuthoritativeObservation {
	rows := make([]AuthoritativeObservation, n)
	for i := range rows {
		rows[i] = AuthoritativeObservation{
			RequestID: "request-" + string(rune('a'+i)), ProviderAttemptID: "attempt-" + string(rune('a'+i)),
			OpportunityID: "opportunity-" + string(rune('a'+i)), Split: SplitCalibration,
			OutputTokens: int64(i + 1), SettledAt: time.Unix(int64(i+1), 0).UTC(), Regimes: cfg.Regimes,
		}
	}
	return rows
}

func TestCanonicalCapPathAdapterRegimeBindsEverySemanticField(t *testing.T) {
	base, err := CanonicalCapPathAdapterRegime(authDigest("evidence"), "provider-uid", 3, "deployment", "openai-body", "adapter-v2", "max_output_tokens", 4096)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := CanonicalCapPathAdapterRegime(authDigest("evidence"), "provider-uid", 3, "deployment", "openai-body", "adapter-v2", "max_output_tokens", 4097)
	if err != nil || changed == base {
		t.Fatalf("effective cap was not digest-bound: changed=%q err=%v", changed, err)
	}
	changed, err = CanonicalCapPathAdapterRegime(authDigest("evidence"), "provider-uid", 3, "deployment", "azure-deployment-path", "adapter-v2", "max_output_tokens", 4096)
	if err != nil || changed == base {
		t.Fatalf("path adapter was not digest-bound: changed=%q err=%v", changed, err)
	}
}

func TestBuildAuthoritativeArtifactExactRankAndRegimes(t *testing.T) {
	cfg := authoritativeConfig(5)
	a, err := BuildAuthoritativeArtifact(cfg, authoritativeRows(cfg, 5))
	if err != nil {
		t.Fatal(err)
	}
	if a.SchemaVersion != AuthoritativeArtifactSchema || a.CoverageBoundKind != ExactMarginalBoundKind ||
		a.ConformalRank != 5 || a.CoverageBoundNumerator != 5 || a.CoverageBoundDenominator != 6 ||
		a.ExchangeableMarginalCoverageLowerPPB != 833_333_333 || a.UpperOutputTokens != 5 ||
		a.FeatureRegimeSHA256 != cfg.Regimes.FeatureSHA256 || a.ArtifactSHA256 == "" {
		t.Fatalf("unexpected artifact: %+v", a)
	}
	rows := authoritativeRows(cfg, 5)
	rows[0].Regimes.CapPathAdapterSHA256 = authDigest("changed-cap")
	if _, err := BuildAuthoritativeArtifact(cfg, rows); err == nil || !strings.Contains(err.Error(), "regime mismatch") {
		t.Fatalf("regime mismatch accepted: %v", err)
	}
}

func TestBuildAuthoritativeArtifactFailsClosedForSmallNAndForbiddenSplit(t *testing.T) {
	cfg := authoritativeConfig(1)
	cfg.CoverageTargetPPB = 990_000_000
	if _, err := BuildAuthoritativeArtifact(cfg, authoritativeRows(cfg, 5)); err == nil || !strings.Contains(err.Error(), "cannot establish") {
		t.Fatalf("small-N 0.99 evidence was advertised: %v", err)
	}
	cfg.CoverageTargetPPB = 500_000_000
	rows := authoritativeRows(cfg, 2)
	for _, forbidden := range []string{SplitDevelopment, SplitMonitoring, SplitFrozenTest} {
		rows[0].Split = forbidden
		if _, err := BuildAuthoritativeArtifact(cfg, rows); err == nil || !strings.Contains(err.Error(), "structurally forbidden") {
			t.Fatalf("split %q accepted: %v", forbidden, err)
		}
	}
}

func TestAuthoritativeObservationDigestRejectsDuplicateRequest(t *testing.T) {
	cfg := authoritativeConfig(1)
	rows := authoritativeRows(cfg, 2)
	rows[1].RequestID = rows[0].RequestID
	if _, err := AuthoritativeObservationDigest(rows); err == nil {
		t.Fatal("duplicate selected-attempt request was accepted")
	}
}
