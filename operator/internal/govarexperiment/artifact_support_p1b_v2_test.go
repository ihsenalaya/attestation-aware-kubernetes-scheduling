package govarexperiment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func p1bDesignMatrixFixtureV2(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	encode := func(value any) json.RawMessage {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	return map[string]json.RawMessage{
		"seeds":              encode([]uint64{91001, 91002, 91003, 91004, 91005}),
		"scenarios":          encode([]string{"S0_reference", "S1_liability", "S2_tail", "S3_isolation"}),
		"selection_contract": encode(expectedP1bSelectionContractV2()),
	}
}

func TestP1bDesignMatrixAndSelectionContractRejectMutations(t *testing.T) {
	if err := validateP1bDesignMatrixAndSelectionV2(p1bDesignMatrixFixtureV2(t)); err != nil {
		t.Fatalf("exact P1b matrix/selection contract was rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(map[string]json.RawMessage)
		want   string
	}{
		{
			name: "seed-matrix",
			mutate: func(value map[string]json.RawMessage) {
				value["seeds"] = json.RawMessage(`[91001,91002,91003,91004,99999]`)
			},
			want: "five-seed matrix",
		},
		{
			name: "scenario-matrix",
			mutate: func(value map[string]json.RawMessage) {
				value["scenarios"] = json.RawMessage(`["S0_reference","S1_liability","S3_isolation","S2_tail"]`)
			},
			want: "four-scenario matrix",
		},
		{
			name: "selection-source-unit-domain",
			mutate: func(value map[string]json.RawMessage) {
				contract := expectedP1bSelectionContractV2()
				contract.SourceAssignmentIDDomain = "outcome-bound-domain"
				raw, _ := json.Marshal(contract)
				value["selection_contract"] = raw
			},
			want: "source-assignment v2",
		},
		{
			name: "selection-strict-json-type",
			mutate: func(value map[string]json.RawMessage) {
				var contract map[string]any
				_ = json.Unmarshal(value["selection_contract"], &contract)
				contract["paired_across_scenarios"] = "true"
				raw, _ := json.Marshal(contract)
				value["selection_contract"] = raw
			},
			want: "selection contract",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := p1bDesignMatrixFixtureV2(t)
			test.mutate(value)
			err := validateP1bDesignMatrixAndSelectionV2(value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("mutation was not rejected precisely: %v", err)
			}
		})
	}
}

// TestP1bExactReleasePreconditions pins the conditions under which the
// p1b_exact release latch may be open. The latch was opened for the P1b full
// development pilot; these assertions keep the release honest rather than
// merely keeping it shut.
func TestP1bExactReleasePreconditions(t *testing.T) {
	// 1. Closing the latch must still fail closed, so the guard is real.
	if _, err := preflightP1bExactArtifactInputsWithReleaseV2(
		ArtifactPreflightInputsV2{SupportMode: E1SupportModeP1bExact}, false,
	); err == nil || !strings.Contains(err.Error(), "production release latch is closed") {
		t.Fatalf("closed p1b_exact release latch is not fail-closed: %v", err)
	}
	// 2. An open latch must never admit anything but p1b_exact.
	if _, err := preflightP1bExactArtifactInputsWithReleaseV2(
		ArtifactPreflightInputsV2{SupportMode: E1SupportModeFixture}, true,
	); err == nil || !strings.Contains(err.Error(), "requires explicit p1b_exact mode") {
		t.Fatalf("open latch accepted a non-p1b_exact mode: %v", err)
	}
	// 3. The frozen pretrace generator stays pinned: an open latch must not
	// silently accept a different design producer.
	if len(p1bPretraceGeneratorSHA256V2) != 64 {
		t.Fatal("pretrace generator pin is not a SHA-256")
	}
	// 4. Whatever the latch admits stays development-tier, never final.
	if p1bEvidenceLabelV2 != "development_non_citable" {
		t.Fatalf("p1b_exact evidence label escaped development tier: %s", p1bEvidenceLabelV2)
	}
}

func p1bExactCrossLanguageInputsV2(t *testing.T, directory string) ArtifactPreflightInputsV2 {
	t.Helper()
	read := func(name string) []byte {
		raw, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatalf("read P1b cross-language fixture %s: %v", name, err)
		}
		return raw
	}
	return ArtifactPreflightInputsV2{
		SupportMode:                 E1SupportModeP1bExact,
		StreamRaw:                   read("stream_input"),
		PreOutcomeMappingRaw:        read("preoutcome_mapping_input"),
		ProtocolRaw:                 read("protocol_input"),
		SourceCodeBindingRaw:        read("source_code_binding_input"),
		SourceManifestRaw:           read("source_manifest_input"),
		PilotUsageBindingRaw:        read("pilot_usage_binding_input"),
		RuntimeCapabilityRaw:        read("runtime_capability_input"),
		DesignSpecRaw:               read("design_spec_input"),
		DecisionConfigTemplateRaw:   read("decision_config_template_input"),
		IndependentDesignReviewRaw:  read("independent_design_review_input"),
		DesignManifestRaw:           read("design_manifest_input"),
		CohortRegistryRaw:           read("cohort_registry_input"),
		ProfileExecutionContractRaw: read("profile_execution_contract_input"),
		BudgetCalibrationRaw:        read("budget_calibration_input"),
		CalibrationArtifactRaw:      read("calibration_artifact_input"),
		CandidateSetRaw:             read("candidate_set_input"),
		ProfileRegistryRaw:          read("profile_registry_input"),
		SlotTemplateRaw:             read("slot_template_input"),
		MethodBuilderBundleRaw:      read("method_builder_bundle_input"),
		ExecutionIntentRaw:          read("execution_intent_input"),
		IndependentAuthorizationRaw: read("independent_authorization_input"),
	}
}

func mutateP1bExactObjectV2(t *testing.T, raw []byte, field string, value any) []byte {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	if _, exists := object[field]; !exists {
		t.Fatalf("P1b mutation fixture lacks field %s", field)
	}
	object[field] = value
	mutated, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return mutated
}

// TestP1bExactCrossLanguageFixtureV2 is deliberately opt-in: the Python
// verifier builds the same full-size, outcome-free 4x5 pretrace and supplies
// one exact 10,000-opportunity cell. The environment variable is read only by
// this test file; production code has no environment-controlled latch path.
func TestP1bExactCrossLanguageFixtureV2(t *testing.T) {
	directory := os.Getenv("ARTICLE3_P1B_EXACT_FIXTURE_DIR")
	if directory == "" {
		t.Skip("cross-language P1b exact fixture was not supplied")
	}
	inputs := p1bExactCrossLanguageInputsV2(t, directory)

	// The latch, when explicitly closed, must still reject every request
	// before any protected read, proving the guard is real.
	if _, err := preflightP1bExactArtifactInputsWithReleaseV2(inputs, false); err == nil ||
		!strings.Contains(err.Error(), "production release latch is closed") {
		t.Fatalf("closed latch is not fail-closed on the shared fixture: %v", err)
	}

	// The latch is open for the P1b development pilot, so the production entry
	// point must accept a valid shared fixture end to end.
	result, err := preflightP1bExactArtifactInputsV2(inputs)
	if err != nil {
		t.Fatalf("complete P1b exact pre-release chain rejected the shared fixture: %v", err)
	}
	if result.config.ProvenanceLock.SupportMode != E1SupportModeP1bExact ||
		len(result.opportunities) != 10_000 || len(result.mappings) != 10_000 ||
		result.support.evidenceLabel != p1bEvidenceLabelV2 {
		t.Fatalf("P1b exact pre-release result has unexpected dimensions or support mode")
	}

	t.Run("authorization-mutation", func(t *testing.T) {
		mutated := inputs
		mutated.IndependentAuthorizationRaw = mutateP1bExactObjectV2(
			t, inputs.IndependentAuthorizationRaw, "fresh_authorization", false,
		)
		if _, err := preflightP1bExactArtifactInputsWithReleaseV2(mutated, true); err == nil ||
			!strings.Contains(err.Error(), "independent authorization") {
			t.Fatalf("stale authorization mutation was not rejected: %v", err)
		}
	})

	t.Run("support-byte-mutation", func(t *testing.T) {
		mutated := inputs
		mutated.BudgetCalibrationRaw = mutateP1bExactObjectV2(
			t, inputs.BudgetCalibrationRaw, "budget_micros", float64(1),
		)
		if _, err := preflightP1bExactArtifactInputsWithReleaseV2(mutated, true); err == nil {
			t.Fatal("unbound budget-calibration mutation was accepted")
		}
	})

	t.Run("pretrace-product-mutation", func(t *testing.T) {
		manifest, err := decodeJSONObjectV2(inputs.DesignManifestRaw)
		if err != nil {
			t.Fatal(err)
		}
		var streams []json.RawMessage
		if err := json.Unmarshal(manifest["streams"], &streams); err != nil {
			t.Fatal(err)
		}
		manifest["streams"], err = json.Marshal(streams[:len(streams)-1])
		if err != nil {
			t.Fatal(err)
		}
		if err := validateP1bPretraceArtifactRootV2(manifest); err == nil ||
			!strings.Contains(err.Error(), "exactly 20 streams") {
			t.Fatalf("incomplete 4x5 pretrace product was not rejected: %v", err)
		}
	})
}
