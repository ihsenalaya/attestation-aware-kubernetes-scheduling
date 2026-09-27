package govarexperiment

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func marshalUsageV2(t *testing.T, rows ...UsageObservationV2) []byte {
	t.Helper()
	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	return raw.Bytes()
}

func testUsageObservationV2(requestID string, input, output int64) UsageObservationV2 {
	sourceRow, err := strconv.ParseInt(SHA256([]byte("source-row-" + requestID))[:15], 16, 64)
	if err != nil {
		panic(err)
	}
	sourceTimestamp := "2024-01-01 00:00:00Z"
	preOutcomeID, err := AzurePreOutcomeIDV2(sourceRow, sourceTimestamp, input)
	if err != nil {
		panic(err)
	}
	return UsageObservationV2{
		SchemaVersion: UsageObservationSchemaV2, RecordType: "usage_observation",
		RequestID: requestID, UsageItemID: preOutcomeID, SourceRow: sourceRow,
		SourceTimestamp: sourceTimestamp, ContextTokens: input, PreOutcomeID: preOutcomeID,
		ActualInputTokens: input, ActualOutputTokens: output,
	}
}

func testUsageObservationForOpportunityV2(opportunity OpportunityV2, output int64) UsageObservationV2 {
	return UsageObservationV2{
		SchemaVersion: UsageObservationSchemaV2, RecordType: "usage_observation",
		RequestID: opportunity.RequestID, UsageItemID: opportunity.UsageItemID,
		SourceRow: opportunity.SourceRow, SourceTimestamp: opportunity.SourceTimestamp,
		ContextTokens: opportunity.ContextTokens, PreOutcomeID: opportunity.PreOutcomeID,
		ActualInputTokens: opportunity.InputTokens, ActualOutputTokens: output,
	}
}

func testUsageMappingRawV2(rows ...UsageObservationV2) []byte {
	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	base := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	for index, row := range rows {
		at := base.Add(time.Duration(index) * time.Second).Format(time.RFC3339Nano)
		mapping := PreOutcomeMappingV2{
			SchemaVersion: PreOutcomeMappingSchemaV2, RecordType: "preoutcome_mapping", Sequence: int64(index),
			RequestID: row.RequestID, TenantID: "tenant", BudgetWindowID: "window", WorkloadUID: "workload-1",
			BudgetMicros: 100, InputTokens: row.ContextTokens, MaxOutputTokens: 100,
			ArrivalAt: at, ProviderResponseAt: at, UsageAvailableAt: at, SettlementAt: at,
			FaultMode: FaultNominal, UsageItemID: row.UsageItemID, SourceRow: row.SourceRow,
			SourceTimestamp: row.SourceTimestamp, ContextTokens: row.ContextTokens, PreOutcomeID: row.PreOutcomeID,
		}
		if err := encoder.Encode(mapping); err != nil {
			panic(err)
		}
	}
	return raw.Bytes()
}

func testUsageAuthorizationV2(row UsageObservationV2, available time.Time) UsageAuthorizationV2 {
	return UsageAuthorizationV2{
		RunID: "run-001", RequestID: row.RequestID, UsageItemID: row.UsageItemID,
		DispatchID: SHA256([]byte("dispatch-" + row.RequestID)), ConfigSHA256: strings.Repeat("c", 64),
		UsageAvailableAt: available.UTC().Format(time.RFC3339Nano),
	}
}

func testUsageProducerEvidenceV2(raw, mappingRaw []byte) UsageProducerEvidenceV2 {
	mappings, err := ParsePreOutcomeMappingV2(mappingRaw)
	if err != nil {
		panic(err)
	}
	opportunities := make([]OpportunityV2, len(mappings))
	for index, mapping := range mappings {
		opportunities[index] = mapping.Opportunity()
	}
	sequenceSHA, err := StreamPreOutcomeSequenceSHA256V2(opportunities)
	if err != nil {
		panic(err)
	}
	lock := testProvenanceLockV2()
	lock.MappingArtifactSHA256 = SHA256(mappingRaw)
	lock.StreamPreOutcomeSequenceSHA256 = sequenceSHA
	lock = sealTestProvenanceLockV2(lock)
	return UsageProducerEvidenceV2{
		SchemaVersion: UsageProducerSchemaV2, DatasetID: UsageDatasetV2, Split: UsageSplitV2,
		ProvenanceLock: lock, ProducerSoftwareSHA256: strings.Repeat("4", 64),
		UsageArtifactSHA256: SHA256(raw),
	}
}

func testUsageDispatchRecordV2(authorization UsageAuthorizationV2, delivered time.Time) LifecycleRecordV2 {
	return LifecycleRecordV2{
		SchemaVersion: LifecycleSchemaV2, RecordType: "lifecycle",
		RuntimeProvenance: RuntimeProvenanceV2, EvidenceStatus: RuntimeEvidenceStatusV2,
		FinalEvidenceEligible: false, EventIndex: 3, Timestamp: delivered.UTC().Format(time.RFC3339Nano),
		LifecycleEvent: "dispatch_delivered", ExperimentID: "E1", RunID: authorization.RunID,
		ConfigSHA256: authorization.ConfigSHA256, RequestID: authorization.RequestID,
		UsageItemID: authorization.UsageItemID, DispatchID: authorization.DispatchID,
		CurrentState: "DISPATCHED", Effective: true, Selected: true,
	}
}

func testUsageDispatchProofV2(authorization UsageAuthorizationV2, delivered time.Time) effectiveUsageDispatchProofV2 {
	return sealEffectiveUsageDispatchProofV2(testUsageDispatchRecordV2(authorization, delivered))
}

func revealRequestV2(authorization UsageAuthorizationV2, at time.Time) UsageRevealRequestV2 {
	return UsageRevealRequestV2{
		RunID: authorization.RunID, RequestID: authorization.RequestID, UsageItemID: authorization.UsageItemID,
		DispatchID: authorization.DispatchID, ConfigSHA256: authorization.ConfigSHA256,
		RevealAt: at.UTC().Format(time.RFC3339Nano),
	}
}

func TestUsageV2HasNoQualityOrSelectedFeedbackWireFields(t *testing.T) {
	row := testUsageObservationV2("request-1", 12, 34)
	raw := marshalUsageV2(t, row)
	mappingRaw := testUsageMappingRawV2(row)
	issuer, err := ParseDevelopmentUsageV2(raw, mappingRaw, testUsageProducerEvidenceV2(raw, mappingRaw))
	if err != nil {
		t.Fatal(err)
	}
	available := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	authorization := testUsageAuthorizationV2(row, available)
	if err := issuer.authorizeUsage(authorization, testUsageDispatchProofV2(authorization, available.Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	receipt, err := issuer.revealUsage(context.Background(), revealRequestV2(authorization, available))
	if err != nil {
		t.Fatal(err)
	}
	receiptRaw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	configRaw, err := json.Marshal(testE1ConfigV2(E1ConcurrencyOne, E1SettlementDelayZero))
	if err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string][]byte{"observation": raw, "receipt": receiptRaw, "config": configRaw} {
		lower := bytes.ToLower(candidate)
		for _, forbidden := range [][]byte{[]byte("selected_feedback"), []byte("selected_score"), []byte(`"score`), []byte(`"quality`)} {
			if bytes.Contains(lower, forbidden) {
				t.Fatalf("%s wire value contains forbidden usage-only field %q: %s", name, forbidden, candidate)
			}
		}
	}
}

func TestDevelopmentUsageV2RejectsNonCanonicalJSONLEnvelopesAndForms(t *testing.T) {
	row := testUsageObservationV2("usage-canonical-jsonl", 12, 34)
	raw := marshalUsageV2(t, row)
	mappingRaw := testUsageMappingRawV2(row)
	parse := func(candidate []byte) error {
		_, err := ParseDevelopmentUsageV2(candidate, mappingRaw, testUsageProducerEvidenceV2(candidate, mappingRaw))
		return err
	}
	if err := parse(raw); err != nil {
		t.Fatalf("canonical development-usage fixture was rejected: %v", err)
	}

	withoutLF := append([]byte(nil), raw[:len(raw)-1]...)
	crlf := bytes.ReplaceAll(raw, []byte{'\n'}, []byte{'\r', '\n'})
	leadingSpace := append([]byte{' '}, raw...)
	interiorSpace := bytes.Replace(raw, []byte(`":"`), []byte(`": "`), 1)
	blankLine := append(append([]byte(nil), raw...), '\n')
	whitespaceLine := append(append([]byte(nil), raw...), ' ', '\n')
	escapedIdentity := bytes.Replace(raw, []byte("usage-canonical-jsonl"), []byte(`\u0075sage-canonical-jsonl`), 1)

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
			if err := parse(candidate); err == nil {
				t.Fatal("non-canonical development-usage JSONL was accepted")
			}
		})
	}
}

func TestUsageV2RejectsEarlyRevealAndAllowsExactAvailability(t *testing.T) {
	row := testUsageObservationV2("request-early", 7, 11)
	raw := marshalUsageV2(t, row)
	mappingRaw := testUsageMappingRawV2(row)
	issuer, err := ParseDevelopmentUsageV2(raw, mappingRaw, testUsageProducerEvidenceV2(raw, mappingRaw))
	if err != nil {
		t.Fatal(err)
	}
	available := time.Date(2026, 7, 14, 12, 0, 10, 0, time.UTC)
	authorization := testUsageAuthorizationV2(row, available)
	proof := testUsageDispatchProofV2(authorization, available.Add(-time.Second))
	if err := issuer.authorizeUsage(authorization, proof); err != nil {
		t.Fatal(err)
	}
	if receipt, err := issuer.revealUsage(context.Background(), revealRequestV2(authorization, available.Add(-time.Nanosecond))); err == nil || receipt != (UsageReceiptV2{}) {
		t.Fatalf("future usage leaked before availability: receipt=%+v err=%v", receipt, err)
	}
	receipt, err := issuer.revealUsage(context.Background(), revealRequestV2(authorization, available))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ActualInputTokens != 7 || receipt.ActualOutputTokens != 11 || receipt.RevealedAt != authorization.UsageAvailableAt {
		t.Fatalf("exact-time receipt is wrong: %+v", receipt)
	}
	if err := issuer.verifyUsage(revealRequestV2(authorization, available), receipt); err != nil {
		t.Fatal(err)
	}
	access := issuer.UsageAccessLog()
	if len(access) != 3 || access[1].Kind != "reveal_rejected_early" || access[1].Effective || access[2].Kind != "reveal" || !access[2].Effective {
		t.Fatalf("causal access log is wrong: %+v", access)
	}
	if access[0].At != proof.record.Timestamp {
		t.Fatalf("authorization was not logged at delivery time: %+v", access[0])
	}
}

func TestUsageV2FailsClosedOnUnauthorizedForgedAndFutureUse(t *testing.T) {
	row := testUsageObservationV2("request-fail-closed", 3, 5)
	raw := marshalUsageV2(t, row)
	mappingRaw := testUsageMappingRawV2(row)
	issuer, err := ParseDevelopmentUsageV2(raw, mappingRaw, testUsageProducerEvidenceV2(raw, mappingRaw))
	if err != nil {
		t.Fatal(err)
	}
	available := time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)
	authorization := testUsageAuthorizationV2(row, available)
	request := revealRequestV2(authorization, available)
	if _, err := issuer.revealUsage(context.Background(), request); err == nil {
		t.Fatal("usage was revealed before dispatch authorization")
	}
	if err := issuer.authorizeUsage(authorization, testUsageDispatchProofV2(authorization, available.Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	forgedRequest := request
	forgedRequest.DispatchID = strings.Repeat("f", 64)
	if _, err := issuer.revealUsage(context.Background(), forgedRequest); err == nil {
		t.Fatal("usage was revealed under a forged dispatch")
	}
	receipt, err := issuer.revealUsage(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	forgedReceipt := receipt
	forgedReceipt.ActualOutputTokens++
	if err := issuer.verifyUsage(request, forgedReceipt); err == nil {
		t.Fatal("forged usage receipt was accepted")
	}
}

func TestUsageV2StrictParsingCoverageAndDeterministicBinding(t *testing.T) {
	first := testUsageObservationV2("request-b", 2, 4)
	second := testUsageObservationV2("request-a", 1, 3)
	raw := marshalUsageV2(t, first, second)
	mappingRaw := testUsageMappingRawV2(first, second)
	one, err := ParseDevelopmentUsageV2(raw, mappingRaw, testUsageProducerEvidenceV2(raw, mappingRaw))
	if err != nil {
		t.Fatal(err)
	}
	two, err := ParseDevelopmentUsageV2(raw, mappingRaw, testUsageProducerEvidenceV2(raw, mappingRaw))
	if err != nil {
		t.Fatal(err)
	}
	if one.Binding() != two.Binding() || one.Binding().ArtifactSHA256 != SHA256(raw) {
		t.Fatalf("usage binding is not deterministic or raw-bound: one=%+v two=%+v", one.Binding(), two.Binding())
	}
	if err := one.Binding().Validate(); err != nil {
		t.Fatal(err)
	}
	mappings, err := ParsePreOutcomeMappingV2(mappingRaw)
	if err != nil {
		t.Fatal(err)
	}
	opportunities := []OpportunityV2{mappings[0].Opportunity(), mappings[1].Opportunity()}
	if err := one.validateOpportunityCoverage(opportunities); err != nil {
		t.Fatal(err)
	}
	opportunities[1].UsageItemID = strings.Repeat("0", 64)
	if err := one.validateOpportunityCoverage(opportunities); err == nil {
		t.Fatal("usage coverage accepted a mismatched item identity")
	}

	line, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	forged := append(append([]byte(nil), line[:len(line)-1]...), []byte(`,"selected_score":1}`+"\n")...)
	if _, err := ParseDevelopmentUsageV2(forged, mappingRaw, testUsageProducerEvidenceV2(forged, mappingRaw)); err == nil {
		t.Fatal("usage parser accepted an outcome-quality field")
	}
}

func TestUsageV2DispatchProofFailsClosed(t *testing.T) {
	row := testUsageObservationV2("request-proof", 1, 2)
	raw := marshalUsageV2(t, row)
	mappingRaw := testUsageMappingRawV2(row)
	issuer, err := ParseDevelopmentUsageV2(raw, mappingRaw, testUsageProducerEvidenceV2(raw, mappingRaw))
	if err != nil {
		t.Fatal(err)
	}
	available := time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC)
	authorization := testUsageAuthorizationV2(row, available)
	valid := testUsageDispatchProofV2(authorization, available.Add(-time.Second))
	tests := []struct {
		name   string
		mutate func(*LifecycleRecordV2)
	}{
		{"non-effective", func(record *LifecycleRecordV2) { record.Effective = false }},
		{"wrong-state", func(record *LifecycleRecordV2) { record.CurrentState = "DISPATCH_PENDING" }},
		{"wrong-lifecycle", func(record *LifecycleRecordV2) { record.LifecycleEvent = "dispatch_claim" }},
		{"wrong-run", func(record *LifecycleRecordV2) { record.RunID = "different-run" }},
		{"wrong-config", func(record *LifecycleRecordV2) { record.ConfigSHA256 = strings.Repeat("f", 64) }},
		{"wrong-request", func(record *LifecycleRecordV2) { record.RequestID = "different-request" }},
		{"wrong-item", func(record *LifecycleRecordV2) { record.UsageItemID = strings.Repeat("a", 64) }},
		{"wrong-dispatch", func(record *LifecycleRecordV2) { record.DispatchID = strings.Repeat("b", 64) }},
		{"missing-event-index", func(record *LifecycleRecordV2) { record.EventIndex = 0 }},
		{"delivered-after-availability", func(record *LifecycleRecordV2) {
			record.Timestamp = available.Add(time.Nanosecond).Format(time.RFC3339Nano)
		}},
		{"noncanonical-timestamp", func(record *LifecycleRecordV2) { record.Timestamp = "not-a-time" }},
		{"final-evidence", func(record *LifecycleRecordV2) { record.FinalEvidenceEligible = true }},
		{"wrong-runtime-provenance", func(record *LifecycleRecordV2) { record.RuntimeProvenance = "other" }},
		{"wrong-evidence-status", func(record *LifecycleRecordV2) { record.EvidenceStatus = "other" }},
		{"contains-actual-usage", func(record *LifecycleRecordV2) {
			actual := int64(0)
			record.ActualOutputTokens = &actual
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := valid.record
			test.mutate(&record)
			candidate := sealEffectiveUsageDispatchProofV2(record)
			if err := issuer.authorizeUsage(authorization, candidate); err == nil {
				t.Fatal("invalid delivered-dispatch proof was accepted")
			}
		})
	}
	t.Run("digest-does-not-recompute", func(t *testing.T) {
		candidate := valid
		candidate.recordDigestSHA256 = strings.Repeat("0", 64)
		if err := issuer.authorizeUsage(authorization, candidate); err == nil {
			t.Fatal("dispatch proof with a forged record digest was accepted")
		}
	})
	if len(issuer.UsageAccessLog()) != 0 {
		t.Fatal("failed dispatch proofs mutated the usage authority log")
	}
	if err := issuer.authorizeUsage(authorization, valid); err != nil {
		t.Fatal(err)
	}
}

func TestUsageV2RequiresExternalProducerArtifactBinding(t *testing.T) {
	row := testUsageObservationV2("request-upstream", 1, 2)
	raw := marshalUsageV2(t, row)
	mappingRaw := testUsageMappingRawV2(row)
	evidence := testUsageProducerEvidenceV2(raw, mappingRaw)
	evidence.UsageArtifactSHA256 = strings.Repeat("f", 64)
	if _, err := ParseDevelopmentUsageV2(raw, mappingRaw, evidence); err == nil {
		t.Fatal("usage bytes were accepted against a different upstream artifact digest")
	}
	evidence = testUsageProducerEvidenceV2(raw, mappingRaw)
	evidence.ProvenanceLock.SourceManifestSHA256 = ""
	if _, err := ParseDevelopmentUsageV2(raw, mappingRaw, evidence); err == nil {
		t.Fatal("usage bytes were accepted without upstream source-manifest provenance")
	}
}

func TestUsageV2CausalTimeParsingIsAutonomouslyFailClosed(t *testing.T) {
	row := testUsageObservationV2("request-bad-causal-time", 1, 2)
	raw := marshalUsageV2(t, row)
	mappingRaw := testUsageMappingRawV2(row)
	issuer, err := ParseDevelopmentUsageV2(raw, mappingRaw, testUsageProducerEvidenceV2(raw, mappingRaw))
	if err != nil {
		t.Fatal(err)
	}
	available := time.Date(2026, 7, 14, 15, 0, 0, 0, time.UTC)
	authorization := testUsageAuthorizationV2(row, available)
	proof := testUsageDispatchProofV2(authorization, available.Add(-time.Second))
	invalid := authorization
	invalid.UsageAvailableAt = "not-a-time"
	if err := proof.validate(invalid); err == nil {
		t.Fatal("dispatch proof treated an invalid availability time as zero time")
	}
	if err := issuer.authorizeUsage(authorization, proof); err != nil {
		t.Fatal(err)
	}
	issuer.mu.Lock()
	stored := issuer.authorizations[row.RequestID]
	stored.Authorization.UsageAvailableAt = "not-a-time"
	issuer.authorizations[row.RequestID] = stored
	issuer.mu.Unlock()
	if _, err := issuer.revealUsage(context.Background(), revealRequestV2(authorization, available)); err == nil {
		t.Fatal("reveal treated a corrupted availability time as zero time")
	}
}
