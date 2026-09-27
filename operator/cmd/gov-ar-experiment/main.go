package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govarexperiment"
)

func main() {
	configPath := flag.String("config", "", "immutable experiment config JSON")
	streamPath := flag.String("stream", "", "deterministic matched opportunity JSONL")
	outcomesPath := flag.String("development-outcomes", "", "separate post-dispatch development outcome JSONL (development_debug only)")
	fixtureArtifactSHA := flag.String("fixture-artifact-sha256", "", "development-only selected-outcome fixture artifact SHA-256")
	authoritySHA := flag.String("authority-sha256", "", "development dispatch authority SHA-256")
	outputDirectory := flag.String("output-dir", "", "create-once raw/manifest directory")
	buildMethod := flag.Bool("build-govar-method-config", false, "build a canonical prospective GOV-AR method config and exit")
	prospectiveEvidencePath := flag.String("prospective-evidence", "", "pre-trace profile/calibration/monitoring fixture JSON")
	methodOutputPath := flag.String("method-config-output", "", "create-once canonical method config JSON output")
	tenantRiskPPB := flag.Int64("tenant-risk-ppb", -1, "prospective tenant risk in PPB")
	driftThresholdPPB := flag.Int64("drift-threshold-ppb", -1, "prospective coverage-gap threshold in PPB")
	maxAgeSeconds := flag.Int64("max-age-seconds", 0, "bound calibration/drift maximum age")
	revalidationSupport := flag.Int64("revalidation-minimum-support", 0, "bound monitoring revalidation support")
	frozenAtRaw := flag.String("frozen-at", "", "canonical pre-trace freeze time")
	flag.Parse()
	if *buildMethod {
		buildProspectiveMethod(*configPath, *streamPath, *prospectiveEvidencePath, *methodOutputPath,
			*tenantRiskPPB, *driftThresholdPPB, *maxAgeSeconds, *revalidationSupport, *frozenAtRaw)
		return
	}
	if *configPath == "" || *streamPath == "" || *outcomesPath == "" || *fixtureArtifactSHA == "" || *authoritySHA == "" || *outputDirectory == "" {
		fmt.Fprintln(os.Stderr, "--config, --stream, --development-outcomes, --fixture-artifact-sha256, --authority-sha256, and --output-dir are required")
		os.Exit(2)
	}
	configRaw, err := os.ReadFile(*configPath)
	if err != nil {
		fatal(err)
	}
	streamRaw, err := os.ReadFile(*streamPath)
	if err != nil {
		fatal(err)
	}
	outcomesRaw, err := os.ReadFile(*outcomesPath)
	if err != nil {
		fatal(err)
	}
	config, _, err := govarexperiment.ParseConfig(configRaw)
	if err != nil {
		fatal(err)
	}
	if config.EvidenceTier != "development_debug" {
		fatal(fmt.Errorf("file-backed development outcomes cannot authorize frozen evidence"))
	}
	if govarexperiment.SHA256(outcomesRaw) != *fixtureArtifactSHA {
		fatal(fmt.Errorf("--fixture-artifact-sha256 does not match the exact development outcome bytes"))
	}
	evidence := govarexperiment.FeedbackEvidence{RunID: config.RunID, DatasetID: config.SelectedFeedbackDatasetID, Split: config.SelectedFeedbackSplit,
		ProtocolSHA256: config.ProtocolSHA256, FeedbackArtifactSHA256: *fixtureArtifactSHA, SoftwareSHA256: config.SoftwareSHA256,
		ConfigSHA256: govarexperiment.SHA256(configRaw), AuthoritySHA256: *authoritySHA, ModelMapSHA256: config.SelectedFeedbackModelMapSHA256}
	issuer, err := govarexperiment.ParseDevelopmentFixtureOutcomes(outcomesRaw, evidence)
	if err != nil {
		fatal(err)
	}
	opportunities, err := govarexperiment.ParseStream(streamRaw, config)
	if err != nil {
		fatal(err)
	}
	if err := issuer.ValidateOpportunityCoverage(opportunities, config.SelectedFeedbackModelIDs); err != nil {
		fatal(err)
	}
	result, err := govarexperiment.WriteArtifacts(*outputDirectory, configRaw, streamRaw, outcomesRaw, issuer)
	if err != nil {
		fatal(err)
	}
	response := struct {
		ManifestPath string `json:"manifest_path"`
		RawPath      string `json:"raw_path"`
		RawSHA256    string `json:"raw_sha256"`
		RecordCount  int64  `json:"record_count"`
	}{ManifestPath: result.ManifestPath, RawPath: result.RawPath,
		RawSHA256:   result.Manifest.RawFiles[0].SHA256,
		RecordCount: result.Manifest.RawFiles[0].ExpectedRecordCount}
	encoded, _ := json.Marshal(response)
	fmt.Println(string(encoded))
}

func buildProspectiveMethod(configPath, streamPath, evidencePath, outputPath string, tenantRisk, driftThreshold,
	maxAge, revalidation int64, frozenAtRaw string) {
	if configPath == "" || streamPath == "" || evidencePath == "" || outputPath == "" || frozenAtRaw == "" {
		fatal(fmt.Errorf("prospective builder requires --config, --stream, --prospective-evidence, --method-config-output, and --frozen-at"))
	}
	configRaw, err := os.ReadFile(configPath)
	if err != nil {
		fatal(err)
	}
	var config govarexperiment.Config
	decoder := json.NewDecoder(bytes.NewReader(configRaw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		fatal(err)
	}
	if err := requireEOF(decoder); err != nil {
		fatal(err)
	}
	streamRaw, err := os.ReadFile(streamPath)
	if err != nil {
		fatal(err)
	}
	opportunities, err := govarexperiment.ParseStream(streamRaw, config)
	if err != nil {
		fatal(err)
	}
	evidenceRaw, err := os.ReadFile(evidencePath)
	if err != nil {
		fatal(err)
	}
	var evidence govarexperiment.GOVARProspectiveEvidence
	decoder = json.NewDecoder(bytes.NewReader(evidenceRaw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		fatal(err)
	}
	if err := requireEOF(decoder); err != nil {
		fatal(err)
	}
	frozenAt, err := time.Parse(time.RFC3339Nano, frozenAtRaw)
	if err != nil || frozenAt.Format(time.RFC3339Nano) != frozenAtRaw {
		fatal(fmt.Errorf("--frozen-at must be canonical RFC3339Nano"))
	}
	method, err := govarexperiment.BuildProspectiveGOVARMethodConfig(config, opportunities, tenantRisk, driftThreshold,
		maxAge, revalidation, frozenAt, evidence)
	if err != nil {
		fatal(err)
	}
	digest, err := govarexperiment.MethodConfigDigest(method)
	if err != nil {
		fatal(err)
	}
	payload, _ := json.MarshalIndent(struct {
		MethodConfig       govarexperiment.MethodConfig `json:"method_config"`
		MethodConfigSHA256 string                       `json:"method_config_sha256"`
	}{method, digest}, "", "  ")
	payload = append(payload, '\n')
	file, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
	if err != nil {
		fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		fatal(err)
	}
	if err := file.Close(); err != nil {
		fatal(err)
	}
	fmt.Printf("{\"method_config_sha256\":%q,\"path\":%q}\n", digest, outputPath)
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values are forbidden")
		}
		return err
	}
	return nil
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "gov-ar-experiment: %v\n", err)
	os.Exit(1)
}
