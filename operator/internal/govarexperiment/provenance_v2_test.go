package govarexperiment

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestE1ProvenanceLockV2IsComparableAndEveryHashIsSealed(t *testing.T) {
	valid := testProvenanceLockV2()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	// Compilation of this map is itself a guard that the shared lock remains
	// directly comparable for exact config/evidence/binding equality checks.
	set := map[E1ProvenanceLockV2]struct{}{valid: {}}
	if _, ok := set[valid]; !ok {
		t.Fatal("comparable provenance lock was not retrievable")
	}

	mutations := []struct {
		name   string
		mutate func(*E1ProvenanceLockV2)
	}{
		{"support-mode", func(l *E1ProvenanceLockV2) { l.SupportMode = E1SupportModeP1bExact }},
		{"opportunity-stream", func(l *E1ProvenanceLockV2) { l.OpportunityStreamSHA256 = strings.Repeat("a", 64) }},
		{"source-code-binding", func(l *E1ProvenanceLockV2) { l.SourceCodeBindingSHA256 = strings.Repeat("a", 64) }},
		{"source-manifest", func(l *E1ProvenanceLockV2) { l.SourceManifestSHA256 = strings.Repeat("a", 64) }},
		{"pilot-usage-binding", func(l *E1ProvenanceLockV2) { l.PilotUsageBindingSHA256 = strings.Repeat("a", 64) }},
		{"runtime-capability", func(l *E1ProvenanceLockV2) { l.RuntimeCapabilitySHA256 = strings.Repeat("a", 64) }},
		{"mapping-artifact", func(l *E1ProvenanceLockV2) { l.MappingArtifactSHA256 = strings.Repeat("a", 64) }},
		{"pilot-preoutcome-set", func(l *E1ProvenanceLockV2) { l.PilotPreOutcomeSetSHA256 = strings.Repeat("a", 64) }},
		{"assigned-preoutcome-set", func(l *E1ProvenanceLockV2) { l.AssignedPreOutcomeSetSHA256 = strings.Repeat("a", 64) }},
		{"stream-preoutcome-sequence", func(l *E1ProvenanceLockV2) { l.StreamPreOutcomeSequenceSHA256 = strings.Repeat("a", 64) }},
		{"protocol", func(l *E1ProvenanceLockV2) { l.ProtocolSHA256 = strings.Repeat("a", 64) }},
		{"design-spec", func(l *E1ProvenanceLockV2) { l.DesignSpecSHA256 = strings.Repeat("1", 64) }},
		{"decision-template", func(l *E1ProvenanceLockV2) { l.DecisionConfigTemplateSHA256 = strings.Repeat("1", 64) }},
		{"design-lock", func(l *E1ProvenanceLockV2) { l.DesignLockSHA256 = strings.Repeat("a", 64) }},
		{"independent-design-review", func(l *E1ProvenanceLockV2) { l.IndependentDesignReviewSHA256 = strings.Repeat("1", 64) }},
		{"design-manifest", func(l *E1ProvenanceLockV2) { l.DesignManifestSHA256 = strings.Repeat("1", 64) }},
		{"cohort-registry", func(l *E1ProvenanceLockV2) { l.CohortRegistrySHA256 = strings.Repeat("1", 64) }},
		{"profile-execution-contract", func(l *E1ProvenanceLockV2) { l.ProfileExecutionContractSHA256 = strings.Repeat("1", 64) }},
		{"budget-calibration", func(l *E1ProvenanceLockV2) { l.BudgetCalibrationSHA256 = strings.Repeat("1", 64) }},
		{"calibration-artifact", func(l *E1ProvenanceLockV2) { l.CalibrationArtifactSHA256 = strings.Repeat("1", 64) }},
		{"candidate-set", func(l *E1ProvenanceLockV2) { l.CandidateSetSHA256 = strings.Repeat("1", 64) }},
		{"profile-registry", func(l *E1ProvenanceLockV2) { l.ProfileRegistrySHA256 = strings.Repeat("1", 64) }},
		{"slot-template", func(l *E1ProvenanceLockV2) { l.SlotTemplateSHA256 = strings.Repeat("1", 64) }},
		{"method-builder-bundle", func(l *E1ProvenanceLockV2) { l.MethodBuilderBundleSHA256 = strings.Repeat("3", 64) }},
		{"execution-intent", func(l *E1ProvenanceLockV2) { l.ExecutionIntentSHA256 = strings.Repeat("2", 64) }},
		{"execution-subject", func(l *E1ProvenanceLockV2) { l.ExecutionSubjectSHA256 = strings.Repeat("1", 64) }},
		{"independent-authorization", func(l *E1ProvenanceLockV2) { l.IndependentAuthorizationSHA256 = strings.Repeat("a", 64) }},
		{"execution-lock", func(l *E1ProvenanceLockV2) { l.ExecutionLockSHA256 = strings.Repeat("a", 64) }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := valid
			mutation.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("mutated provenance lock was accepted without resealing")
			}
		})
	}
}

func TestE1ProvenanceSubjectIsPreOutcomeAndAuthorizationCycleFree(t *testing.T) {
	base := testProvenanceLockV2()
	subject := base.ExecutionSubjectSHA256
	final := base.ExecutionLockSHA256

	changedAuthorization := base
	changedAuthorization.IndependentAuthorizationSHA256 = SHA256([]byte("different external authorization bytes"))
	changedAuthorization.ExecutionLockSHA256 = changedAuthorization.executionDigest()
	if changedAuthorization.ExecutionSubjectSHA256 != subject {
		t.Fatal("authorization hash fed back into the pre-outcome execution subject")
	}
	if changedAuthorization.ExecutionLockSHA256 == final || changedAuthorization.Validate() != nil {
		t.Fatal("final execution lock did not bind the changed external authorization")
	}

	changedDesign := base
	changedDesign.CohortRegistrySHA256 = SHA256([]byte("different prospective cohort registry"))
	changedDesign = sealTestProvenanceLockV2(changedDesign)
	if changedDesign.ExecutionSubjectSHA256 == subject || changedDesign.ExecutionLockSHA256 == final {
		t.Fatal("prospective design mutation did not change subject and final lock")
	}

	typeOfLock := reflect.TypeOf(E1ProvenanceLockV2{})
	forbiddenFields := map[string]struct{}{
		"DevelopmentUsageSHA256": {}, "ProducerEvidenceSHA256": {}, "UsageBindingSHA256": {},
		"LifecycleSHA256": {}, "UsageReceiptSHA256": {}, "ActualUsageSHA256": {},
	}
	for index := 0; index < typeOfLock.NumField(); index++ {
		name := typeOfLock.Field(index).Name
		if _, forbidden := forbiddenFields[name]; forbidden {
			t.Fatalf("post-authorization/outcome field %q leaked into the pre-outcome lock", name)
		}
	}
}

func TestAzurePreOutcomeIDV2MatchesPythonProducerFormulaWithoutTimestampNormalization(t *testing.T) {
	got, err := AzurePreOutcomeIDV2(123, "2024-02-03 04:05:06.123456Z", 456)
	if err != nil {
		t.Fatal(err)
	}
	const want = "7817085546afdbcdddef77e501cf53c2f882ac37c36c7d590d9152fff3299440"
	if got != want {
		t.Fatalf("Go/Python pre-outcome formula differs: got %s want %s", got, want)
	}
	withT, err := AzurePreOutcomeIDV2(123, "2024-02-03T04:05:06.123456Z", 456)
	if err != nil {
		t.Fatal(err)
	}
	if withT == got {
		t.Fatal("source timestamp was normalized before hashing")
	}
	for name, timestamp := range map[string]string{
		"timezone-missing": "2024-02-03T04:05:06.123456",
		"impossible-date":  "2024-02-30T04:05:06.123456Z",
		"impossible-hour":  "2024-02-03T25:05:06.123456Z",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := AzurePreOutcomeIDV2(123, timestamp, 456); err == nil {
				t.Fatalf("invalid source timestamp %q was accepted", timestamp)
			}
		})
	}
}

func TestPreOutcomeMappingV2IsStrictOutcomeFreeAndBijective(t *testing.T) {
	first := testUsageObservationV2("mapping-first", 7, 11)
	second := testUsageObservationV2("mapping-second", 9, 13)
	raw := testUsageMappingRawV2(first, second)
	rows, err := ParsePreOutcomeMappingV2(raw)
	if err != nil || len(rows) != 2 {
		t.Fatalf("valid strict mapping was rejected: rows=%d err=%v", len(rows), err)
	}

	firstLine := bytes.Split(raw, []byte{'\n'})[0]
	for _, forbidden := range []string{"event_id", "generated_tokens"} {
		forged := append(append([]byte(nil), firstLine[:len(firstLine)-1]...), []byte(`,"`+forbidden+`":"forbidden"}`+"\n")...)
		if _, err := ParsePreOutcomeMappingV2(forged); err == nil {
			t.Fatalf("strict mapping accepted forbidden outcome-linked field %q", forbidden)
		}
	}
	duplicateKey := append(append([]byte(nil), firstLine[:len(firstLine)-1]...), []byte(`,"request_id":"replacement"}`+"\n")...)
	if _, err := ParsePreOutcomeMappingV2(duplicateKey); err == nil || !strings.Contains(err.Error(), "duplicates key") {
		t.Fatalf("strict mapping accepted a duplicate JSON key: %v", err)
	}

	reordered := append(append([]byte(nil), bytes.Split(raw, []byte{'\n'})[1]...), '\n')
	reordered = append(reordered, firstLine...)
	reordered = append(reordered, '\n')
	if _, err := ParsePreOutcomeMappingV2(reordered); err == nil {
		t.Fatal("mapping parser accepted reordered non-contiguous sequence")
	}

	duplicate := rows[1]
	duplicate.PreOutcomeID = rows[0].PreOutcomeID
	duplicate.UsageItemID = duplicate.PreOutcomeID
	var duplicateRaw bytes.Buffer
	encoder := json.NewEncoder(&duplicateRaw)
	_ = encoder.Encode(rows[0])
	_ = encoder.Encode(duplicate)
	if _, err := ParsePreOutcomeMappingV2(duplicateRaw.Bytes()); err == nil {
		t.Fatal("mapping parser accepted duplicate preoutcome_id")
	}
}

func TestPreOutcomeMappingV2RejectsNonCanonicalJSONLEnvelopesAndForms(t *testing.T) {
	row := testUsageObservationV2("mapping-canonical-jsonl", 7, 11)
	raw := testUsageMappingRawV2(row)
	if _, err := ParsePreOutcomeMappingV2(raw); err != nil {
		t.Fatalf("canonical mapping fixture was rejected: %v", err)
	}

	withoutLF := append([]byte(nil), raw[:len(raw)-1]...)
	crlf := bytes.ReplaceAll(raw, []byte{'\n'}, []byte{'\r', '\n'})
	leadingSpace := append([]byte{' '}, raw...)
	interiorSpace := bytes.Replace(raw, []byte(`":"`), []byte(`": "`), 1)
	blankLine := append(append([]byte(nil), raw...), '\n')
	whitespaceLine := append(append([]byte(nil), raw...), '\t', '\n')
	escapedIdentity := bytes.Replace(raw, []byte("mapping-canonical-jsonl"), []byte(`\u006dapping-canonical-jsonl`), 1)

	for name, candidate := range map[string][]byte{
		"missing-final-lf": withoutLF,
		"crlf":             crlf,
		"leading-space":    leadingSpace,
		"interior-space":   interiorSpace,
		"blank-line":       blankLine,
		"whitespace-line":  whitespaceLine,
		"escaped-form":     escapedIdentity,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParsePreOutcomeMappingV2(candidate); err == nil {
				t.Fatal("non-canonical pre-outcome mapping JSONL was accepted")
			}
		})
	}
}

func TestCanonicalJSONLV2MatchesPythonInputKeyOrderContract(t *testing.T) {
	// Python json.loads preserves source order and _compact does not sort keys.
	// Both orders are consequently canonical, while whitespace and escaped
	// forms are not. This regression test prevents an accidental Go-only sort.
	for _, raw := range [][]byte{
		[]byte("{\"a\":1,\"b\":2}\n"),
		[]byte("{\"b\":2,\"a\":1}\n"),
	} {
		rows := 0
		if err := scanCanonicalJSONLV2(raw, "ordered rows", func(_ int, _ []byte) error {
			rows++
			return nil
		}); err != nil || rows != 1 {
			t.Fatalf("Python-canonical key order was rejected: rows=%d err=%v", rows, err)
		}
	}
}

func TestUsageV2RejectsCoordinateAndInputContextMutations(t *testing.T) {
	row := testUsageObservationV2("usage-coordinates", 17, 19)
	for name, mutate := range map[string]func(*UsageObservationV2){
		"source-row":       func(candidate *UsageObservationV2) { candidate.SourceRow++ },
		"source-timestamp": func(candidate *UsageObservationV2) { candidate.SourceTimestamp = "2024-01-01T00:00:00" },
		"context":          func(candidate *UsageObservationV2) { candidate.ContextTokens++ },
		"preoutcome":       func(candidate *UsageObservationV2) { candidate.PreOutcomeID = strings.Repeat("a", 64) },
		"usage-item":       func(candidate *UsageObservationV2) { candidate.UsageItemID = strings.Repeat("b", 64) },
		"actual-input":     func(candidate *UsageObservationV2) { candidate.ActualInputTokens++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := row
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("coordinate/input mutation was accepted")
			}
		})
	}
}

func TestRunnerV2ChecksRawStreamAndSharedLockBeforeParsingThenSequenceDigest(t *testing.T) {
	base := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	first, firstRow := oneRunnerOpportunityV2("provenance-first", base, 0)
	factors := E1Factors{
		CellType: E1CellPrincipal, Concurrency: E1ConcurrencyOne,
		SettlementDelaySeconds: E1SettlementDelayZero, BudgetLevel: E1BudgetMedium,
		RiskTargetPPB: E1RiskTargetOnePercent,
	}
	configRaw, streamRaw, issuer := runnerV2Fixture(t, "fixed_quantile", factors, []OpportunityV2{first}, []UsageObservationV2{firstRow})

	mutatedRaw := append(append([]byte(nil), streamRaw...), ' ')
	if _, err := RunWithUsageIssuerV2(configRaw, mutatedRaw, issuer); err == nil || !strings.Contains(err.Error(), "raw opportunity stream") {
		t.Fatalf("raw stream mutation was not rejected before parsing: %v", err)
	}

	config, _, err := ParseConfigV2(configRaw)
	if err != nil {
		t.Fatal(err)
	}
	config.ProvenanceLock.DesignLockSHA256 = strings.Repeat("a", 64)
	config.ProvenanceLock = sealTestProvenanceLockV2(config.ProvenanceLock)
	if _, err := RunWithUsageIssuerV2(marshalConfigV2(t, config), streamRaw, issuer); err == nil || !strings.Contains(err.Error(), "binding does not match") {
		t.Fatalf("config/binding lock mismatch was not rejected before parsing: %v", err)
	}

	binding := issuer.Binding()
	binding.ProvenanceLock.StreamPreOutcomeSequenceSHA256 = strings.Repeat("a", 64)
	binding.ProvenanceLock = sealTestProvenanceLockV2(binding.ProvenanceLock)
	binding.BindingSHA256 = usageBindingDigestV2(binding)
	config.ProvenanceLock = binding.ProvenanceLock
	config.UsageBindingSHA256 = binding.BindingSHA256
	issuer.binding = binding
	if _, err := RunWithUsageIssuerV2(marshalConfigV2(t, config), streamRaw, issuer); err == nil || !strings.Contains(err.Error(), "pre-outcome sequence") {
		t.Fatalf("stream pre-outcome sequence mutation was not rejected: %v", err)
	}
}
