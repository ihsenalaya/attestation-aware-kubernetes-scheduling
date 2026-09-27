package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govarexperiment"
)

type cliPathsV2 struct {
	supportMode                                          string
	config, stream, mapping, usage, evidence             string
	protocol, sourceCodeBinding, sourceManifest          string
	pilotBinding, runtimeCapability, designSpec          string
	decisionTemplate, designReview, designManifest       string
	cohortRegistry, profileExecutionContract             string
	budgetCalibration, calibrationArtifact, candidateSet string
	profileRegistry                                      string
	slotTemplate, methodBuilderBundle, executionIntent   string
	authority, output                                    string
}

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func runCLI(arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("gov-ar-experiment-v2", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var paths cliPathsV2
	flags.StringVar(&paths.supportMode, "support-mode", "", "explicit support mode: fixture or p1b_exact")
	flags.StringVar(&paths.config, "config", "", "fixture-only final E1-v2 experiment config JSON")
	flags.StringVar(&paths.stream, "stream", "", "outcome-free E1-v2 opportunity JSONL")
	flags.StringVar(&paths.mapping, "preoutcome-mapping", "", "outcome-free pre-admission mapping JSONL")
	flags.StringVar(&paths.usage, "development-usage", "", "Azure development-pilot usage-only JSONL")
	flags.StringVar(&paths.evidence, "producer-evidence", "", "fixture-only usage producer evidence JSON")
	flags.StringVar(&paths.protocol, "protocol", "", "exact protocol input JSON")
	flags.StringVar(&paths.sourceCodeBinding, "source-code-binding", "", "reviewed source-code binding JSON")
	flags.StringVar(&paths.sourceManifest, "source-manifest", "", "locked source manifest JSON")
	flags.StringVar(&paths.pilotBinding, "pilot-usage-binding", "", "authorized pilot usage binding JSON")
	flags.StringVar(&paths.runtimeCapability, "runtime-capability", "", "validated runtime capability JSON")
	flags.StringVar(&paths.designSpec, "design-spec", "", "reviewed outcome-free design specification JSON")
	flags.StringVar(&paths.decisionTemplate, "decision-config-template", "", "prospective decision-config template JSON")
	flags.StringVar(&paths.designReview, "independent-design-review", "", "independent pretrace design review JSON")
	flags.StringVar(&paths.designManifest, "design-manifest", "", "outcome-free pretrace design manifest JSON")
	flags.StringVar(&paths.cohortRegistry, "cohort-registry", "", "selected stream cohort registry JSON")
	flags.StringVar(&paths.profileExecutionContract, "profile-execution-contract", "", "selected stream prospective profile-execution contract JSON")
	flags.StringVar(&paths.budgetCalibration, "budget-calibration", "", "raw prospective scenario-budget calibration support JSON")
	flags.StringVar(&paths.calibrationArtifact, "calibration-artifact", "", "raw prospective calibration support JSON")
	flags.StringVar(&paths.candidateSet, "candidate-set", "", "raw prospective candidate-set support JSON")
	flags.StringVar(&paths.profileRegistry, "profile-registry", "", "raw prospective profile-registry support JSON")
	flags.StringVar(&paths.slotTemplate, "slot-template", "", "raw prospective slot-template support JSON")
	flags.StringVar(&paths.methodBuilderBundle, "method-builder-bundle", "", "raw exact method-builder bundle JSON")
	flags.StringVar(&paths.executionIntent, "execution-intent", "", "authorized pre-outcome per-cell execution intent JSON")
	flags.StringVar(&paths.authority, "independent-authorization", "", "independent execution authorization JSON")
	flags.StringVar(&paths.output, "output-dir", "", "create-once single-cell artifact directory")
	buildMethod := flags.Bool("build-govar-method-config", false,
		"build a canonical prospective GOV-AR method config from a v2 stream and exit")
	prospectiveEvidence := flags.String("prospective-evidence", "", "raw prospective GOV-AR evidence JSON")
	methodConfigOutput := flags.String("method-config-output", "", "create-once built method-config JSON")
	tenantRisk := flags.Int64("tenant-risk-ppb", 0, "tenant risk budget in parts per billion")
	driftThreshold := flags.Int64("drift-threshold-ppb", 0, "drift detector threshold in parts per billion")
	maxAge := flags.Int64("max-age-seconds", 0, "maximum accepted calibration age in seconds")
	revalidation := flags.Int64("revalidation-minimum-support", 0, "support required before calibrated claims resume")
	frozenAtRaw := flags.String("frozen-at", "", "canonical RFC3339Nano freeze instant")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	if *buildMethod {
		return runBuildGOVARMethodConfigV2(buildRequestV2{
			config: paths.config, stream: paths.stream, evidence: *prospectiveEvidence,
			output: *methodConfigOutput, tenantRisk: *tenantRisk,
			driftThreshold: *driftThreshold, maxAge: *maxAge,
			revalidation: *revalidation, frozenAt: *frozenAtRaw,
		}, stderr)
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "gov-ar-experiment-v2: positional arguments are forbidden")
		return 2
	}
	if paths.supportMode == "" {
		fmt.Fprintln(stderr, "gov-ar-experiment-v2: --support-mode is required")
		return 2
	}
	mode := govarexperiment.E1SupportModeV2(paths.supportMode)
	if mode != govarexperiment.E1SupportModeFixture && mode != govarexperiment.E1SupportModeP1bExact {
		fmt.Fprintln(stderr, "gov-ar-experiment-v2: --support-mode must be fixture or p1b_exact")
		return 2
	}
	required := []struct {
		flag  string
		value string
	}{
		{"--support-mode", paths.supportMode}, {"--stream", paths.stream},
		{"--preoutcome-mapping", paths.mapping}, {"--development-usage", paths.usage},
		{"--protocol", paths.protocol},
		{"--source-code-binding", paths.sourceCodeBinding}, {"--source-manifest", paths.sourceManifest},
		{"--pilot-usage-binding", paths.pilotBinding}, {"--runtime-capability", paths.runtimeCapability},
		{"--design-spec", paths.designSpec}, {"--decision-config-template", paths.decisionTemplate},
		{"--independent-design-review", paths.designReview}, {"--design-manifest", paths.designManifest},
		{"--cohort-registry", paths.cohortRegistry}, {"--profile-execution-contract", paths.profileExecutionContract},
		{"--budget-calibration", paths.budgetCalibration}, {"--calibration-artifact", paths.calibrationArtifact},
		{"--candidate-set", paths.candidateSet},
		{"--profile-registry", paths.profileRegistry}, {"--slot-template", paths.slotTemplate},
		{"--method-builder-bundle", paths.methodBuilderBundle}, {"--execution-intent", paths.executionIntent},
		{"--independent-authorization", paths.authority}, {"--output-dir", paths.output},
	}
	if mode == govarexperiment.E1SupportModeFixture {
		required = append(required,
			struct {
				flag  string
				value string
			}{"--config", paths.config},
			struct {
				flag  string
				value string
			}{"--producer-evidence", paths.evidence},
		)
	} else if paths.config != "" || paths.evidence != "" {
		fmt.Fprintln(stderr, "gov-ar-experiment-v2: p1b_exact forbids --config and --producer-evidence; both are materialized internally after usage read")
		return 2
	}
	for _, item := range required {
		if item.value == "" {
			fmt.Fprintf(stderr, "gov-ar-experiment-v2: %s is required\n", item.flag)
			return 2
		}
	}
	read := func(label, path string) ([]byte, error) {
		raw, err := govarexperiment.ReadRegularFileNoSymlinkV2(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", label, err)
		}
		return raw, nil
	}
	var configRaw []byte
	if mode == govarexperiment.E1SupportModeFixture {
		var err error
		configRaw, err = read("config", paths.config)
		if err != nil {
			return cliFailureV2(stderr, err)
		}
	}
	streamRaw, err := read("stream", paths.stream)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	mappingRaw, err := read("pre-outcome mapping", paths.mapping)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	protocolRaw, err := read("protocol", paths.protocol)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	sourceCodeBindingRaw, err := read("source-code binding", paths.sourceCodeBinding)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	sourceManifestRaw, err := read("source manifest", paths.sourceManifest)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	pilotBindingRaw, err := read("pilot usage binding", paths.pilotBinding)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	runtimeCapabilityRaw, err := read("runtime capability", paths.runtimeCapability)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	designSpecRaw, err := read("design specification", paths.designSpec)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	decisionTemplateRaw, err := read("decision-config template", paths.decisionTemplate)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	designReviewRaw, err := read("independent design review", paths.designReview)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	designManifestRaw, err := read("design manifest", paths.designManifest)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	cohortRegistryRaw, err := read("cohort registry", paths.cohortRegistry)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	profileExecutionContractRaw, err := read("profile-execution contract", paths.profileExecutionContract)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	budgetCalibrationRaw, err := read("budget calibration", paths.budgetCalibration)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	calibrationArtifactRaw, err := read("calibration artifact", paths.calibrationArtifact)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	candidateSetRaw, err := read("candidate set", paths.candidateSet)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	profileRegistryRaw, err := read("profile registry", paths.profileRegistry)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	slotTemplateRaw, err := read("slot template", paths.slotTemplate)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	methodBuilderBundleRaw, err := read("method-builder bundle", paths.methodBuilderBundle)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	executionIntentRaw, err := read("execution intent", paths.executionIntent)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	authorityRaw, err := read("independent authorization", paths.authority)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	preflight := govarexperiment.ArtifactPreflightInputsV2{
		SupportMode: mode, ConfigRaw: configRaw, StreamRaw: streamRaw, PreOutcomeMappingRaw: mappingRaw,
		ProtocolRaw: protocolRaw, SourceCodeBindingRaw: sourceCodeBindingRaw,
		SourceManifestRaw: sourceManifestRaw, PilotUsageBindingRaw: pilotBindingRaw,
		RuntimeCapabilityRaw: runtimeCapabilityRaw, DesignSpecRaw: designSpecRaw,
		DecisionConfigTemplateRaw: decisionTemplateRaw, IndependentDesignReviewRaw: designReviewRaw,
		DesignManifestRaw: designManifestRaw, CohortRegistryRaw: cohortRegistryRaw,
		ProfileExecutionContractRaw: profileExecutionContractRaw, BudgetCalibrationRaw: budgetCalibrationRaw,
		CalibrationArtifactRaw: calibrationArtifactRaw,
		CandidateSetRaw:        candidateSetRaw, ProfileRegistryRaw: profileRegistryRaw, SlotTemplateRaw: slotTemplateRaw,
		MethodBuilderBundleRaw: methodBuilderBundleRaw, ExecutionIntentRaw: executionIntentRaw,
		IndependentAuthorizationRaw: authorityRaw,
	}
	result, err := govarexperiment.WriteArtifactsV2AfterPreflight(paths.output, preflight, func() (govarexperiment.ArtifactProtectedInputsV2, error) {
		var evidenceRaw []byte
		if mode == govarexperiment.E1SupportModeFixture {
			var err error
			evidenceRaw, err = read("producer evidence", paths.evidence)
			if err != nil {
				return govarexperiment.ArtifactProtectedInputsV2{}, err
			}
		}
		usageRaw, err := read("development usage", paths.usage)
		if err != nil {
			return govarexperiment.ArtifactProtectedInputsV2{}, err
		}
		return govarexperiment.ArtifactProtectedInputsV2{DevelopmentUsageRaw: usageRaw, ProducerEvidenceRaw: evidenceRaw}, nil
	})
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	response := struct {
		ManifestPath          string `json:"manifest_path"`
		CompletionPath        string `json:"completion_path"`
		ArtifactSetRootSHA256 string `json:"artifact_set_root_sha256"`
		EvidenceLabel         string `json:"evidence_label"`
		FinalEvidenceEligible bool   `json:"final_evidence_eligible"`
		LifecycleRecordCount  int64  `json:"lifecycle_record_count"`
	}{
		ManifestPath: result.ManifestPath, CompletionPath: result.CompletionPath,
		ArtifactSetRootSHA256: result.Manifest.ArtifactSetRootSHA256,
		EvidenceLabel:         result.Manifest.EvidenceLabel, FinalEvidenceEligible: false,
		LifecycleRecordCount: result.Manifest.LifecycleRecordCount,
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return cliFailureV2(stderr, err)
	}
	fmt.Fprintln(stdout, string(encoded))
	return 0
}

func cliFailureV2(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "gov-ar-experiment-v2: %v\n", err)
	return 1
}


type buildRequestV2 struct {
	config, stream, evidence, output, frozenAt              string
	tenantRisk, driftThreshold, maxAge, revalidation        int64
}

// runBuildGOVARMethodConfigV2 mirrors the v1 builder mode for a P1b v2 stream.
// It never reads usage: only the outcome-free stream, the prospective evidence
// and the pre-outcome config projection.
func runBuildGOVARMethodConfigV2(request buildRequestV2, stderr io.Writer) int {
	fail := func(err error) int {
		fmt.Fprintf(stderr, "gov-ar-experiment-v2: %v\n", err)
		return 2
	}
	for flagName, value := range map[string]string{
		"--config": request.config, "--stream": request.stream,
		"--prospective-evidence": request.evidence,
		"--method-config-output": request.output, "--frozen-at": request.frozenAt,
	} {
		if value == "" {
			return fail(fmt.Errorf("%s is required with --build-govar-method-config", flagName))
		}
	}
	decode := func(path string, target any) error {
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(target); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		var extra json.RawMessage
		if err := decoder.Decode(&extra); err != io.EOF {
			return fmt.Errorf("%s: trailing content", path)
		}
		return nil
	}
	var config govarexperiment.Config
	if err := decode(request.config, &config); err != nil {
		return fail(err)
	}
	var evidence govarexperiment.GOVARProspectiveEvidence
	if err := decode(request.evidence, &evidence); err != nil {
		return fail(err)
	}
	streamRaw, err := os.ReadFile(request.stream)
	if err != nil {
		return fail(err)
	}
	var opportunities []govarexperiment.OpportunityV2
	for number, line := range bytes.Split(bytes.TrimRight(streamRaw, "\n"), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var opportunity govarexperiment.OpportunityV2
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&opportunity); err != nil {
			return fail(fmt.Errorf("stream line %d: %w", number+1, err))
		}
		opportunities = append(opportunities, opportunity)
	}
	frozenAt, err := time.Parse(time.RFC3339Nano, request.frozenAt)
	if err != nil || frozenAt.Format(time.RFC3339Nano) != request.frozenAt {
		return fail(errors.New("--frozen-at must be canonical RFC3339Nano"))
	}
	method, err := govarexperiment.BuildProspectiveGOVARMethodConfigV2(
		config, opportunities, request.tenantRisk, request.driftThreshold,
		request.maxAge, request.revalidation, frozenAt, evidence)
	if err != nil {
		return fail(err)
	}
	digest, err := govarexperiment.MethodConfigDigest(method)
	if err != nil {
		return fail(err)
	}
	payload, err := json.MarshalIndent(struct {
		MethodConfig       govarexperiment.MethodConfig `json:"method_config"`
		MethodConfigSHA256 string                       `json:"method_config_sha256"`
	}{method, digest}, "", "  ")
	if err != nil {
		return fail(err)
	}
	file, err := os.OpenFile(request.output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
	if err != nil {
		return fail(err)
	}
	defer file.Close()
	if _, err := file.Write(append(payload, '\n')); err != nil {
		return fail(err)
	}
	return 0
}
