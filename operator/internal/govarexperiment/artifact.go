package govarexperiment

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

type ArtifactResult struct {
	RawPath            string
	ManifestPath       string
	ConfigPath         string
	StreamPath         string
	FeedbackPath       string
	FeedbackAccessPath string
	Manifest           Manifest
}

func EncodeJSONL(records []LifecycleRecord) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func encodeFeedbackAccessJSONL(events []FeedbackAccessEvent) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func decodeFeedbackAccessJSONL(raw []byte) ([]FeedbackAccessEvent, error) {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	events := make([]FeedbackAccessEvent, 0)
	for line := 1; scanner.Scan(); line++ {
		body := bytes.TrimSpace(scanner.Bytes())
		if len(body) == 0 {
			return nil, fmt.Errorf("selected-feedback access line %d is empty", line)
		}
		var event FeedbackAccessEvent
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return nil, fmt.Errorf("selected-feedback access line %d: %w", line, err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			return nil, fmt.Errorf("selected-feedback access line %d: %w", line, err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
func deterministicGzip(raw []byte) ([]byte, error) {
	var output bytes.Buffer
	writer, err := gzip.NewWriterLevel(&output, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	writer.ModTime = time.Unix(0, 0).UTC()
	writer.OS = 255
	if _, err := writer.Write(raw); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func DecodeGzipJSONL(compressed []byte) ([]LifecycleRecord, string, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, "", fmt.Errorf("open gzip raw file: %w", err)
	}
	uncompressed, err := io.ReadAll(reader)
	if err != nil {
		_ = reader.Close()
		return nil, "", fmt.Errorf("read gzip raw file: %w", err)
	}
	if err := reader.Close(); err != nil {
		return nil, "", err
	}
	scanner := bufio.NewScanner(bytes.NewReader(uncompressed))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var records []LifecycleRecord
	line := 0
	for scanner.Scan() {
		line++
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			return nil, "", fmt.Errorf("raw line %d is empty", line)
		}
		var record LifecycleRecord
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, "", fmt.Errorf("raw line %d: %w", line, err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			return nil, "", fmt.Errorf("raw line %d: %w", line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, "", err
	}
	return records, SHA256(uncompressed), nil
}

// VerifyRaw consumes exact immutable input bytes, not caller-provided labels.
func VerifyRaw(compressed, configRaw, streamRaw []byte, issuer OutcomeIssuer) ([]LifecycleRecord, map[string]int64, map[string]int64, Metrics, string, error) {
	config, _, err := ParseConfig(configRaw)
	if err != nil {
		return nil, nil, nil, Metrics{}, "", err
	}
	opportunities, err := ParseStream(streamRaw, config)
	if err != nil {
		return nil, nil, nil, Metrics{}, "", err
	}
	records, outputSHA, err := DecodeGzipJSONL(compressed)
	if err != nil {
		return nil, nil, nil, Metrics{}, "", err
	}
	configSHA, streamSHA := SHA256(configRaw), SHA256(streamRaw)
	matched := DomainHash("govar-matched-stream-v1", []byte(streamSHA), []byte(fmt.Sprint(config.Seed)))
	for index, row := range records {
		if row.ConfigSHA256 != configSHA || row.StreamSHA256 != streamSHA || row.MatchedStreamKey != matched {
			return nil, nil, nil, Metrics{}, "", fmt.Errorf("raw record %d is not bound to exact config/stream bytes", index+1)
		}
	}
	decisions, lifecycle, metrics, err := VerifyRecords(records, config, opportunities, issuer)
	if err != nil {
		return nil, nil, nil, Metrics{}, "", err
	}
	return records, decisions, lifecycle, metrics, outputSHA, nil
}

func WriteArtifacts(outputDirectory string, configRaw, streamRaw, feedbackRaw []byte, issuer OutcomeIssuer) (ArtifactResult, error) {
	run, err := RunWithIssuer(configRaw, streamRaw, issuer)
	if err != nil {
		return ArtifactResult{}, err
	}
	if !IsSafeArtifactIdentity(run.Config.RunID) {
		return ArtifactResult{}, errors.New("run_id is unsafe for artifact path")
	}
	if err := os.MkdirAll(outputDirectory, 0o755); err != nil {
		return ArtifactResult{}, err
	}
	base := run.Config.RunID
	configPath := filepath.Join(outputDirectory, base+".config.json")
	streamPath := filepath.Join(outputDirectory, base+".opportunities.jsonl")
	feedbackPath := filepath.Join(outputDirectory, base+".development-outcomes.jsonl")
	feedbackAccessPath := filepath.Join(outputDirectory, base+".selected-feedback-access.jsonl")
	rawPath := filepath.Join(outputDirectory, base+".lifecycle.jsonl.gz")
	manifestPath := filepath.Join(outputDirectory, base+".manifest.json")
	for _, path := range []string{configPath, streamPath, feedbackPath, feedbackAccessPath, rawPath, manifestPath} {
		if pathExists(path) {
			return ArtifactResult{}, fmt.Errorf("create-once artifact path already exists: %s", filepath.Base(path))
		}
	}
	jsonl, err := EncodeJSONL(run.Records)
	if err != nil {
		return ArtifactResult{}, err
	}
	compressed, err := deterministicGzip(jsonl)
	if err != nil {
		return ArtifactResult{}, err
	}
	if err := createOnce(configPath, configRaw); err != nil {
		return ArtifactResult{}, fmt.Errorf("publish config input: %w", err)
	}
	if err := createOnce(streamPath, streamRaw); err != nil {
		return ArtifactResult{}, fmt.Errorf("publish stream input: %w", err)
	}
	if SHA256(feedbackRaw) != issuer.BindingEvidence().FeedbackArtifactSHA256 {
		return ArtifactResult{}, errors.New("development feedback bytes differ from the issuer artifact binding")
	}
	if err := createOnce(feedbackPath, feedbackRaw); err != nil {
		return ArtifactResult{}, fmt.Errorf("publish feedback input: %w", err)
	}
	accessEvents := issuer.FeedbackAccessLog()
	if err := VerifyFeedbackAccessLog(accessEvents, run.Records, run.Config); err != nil {
		return ArtifactResult{}, fmt.Errorf("verify selected-feedback access log: %w", err)
	}
	accessRaw, err := encodeFeedbackAccessJSONL(accessEvents)
	if err != nil {
		return ArtifactResult{}, err
	}
	if err := createOnce(feedbackAccessPath, accessRaw); err != nil {
		return ArtifactResult{}, fmt.Errorf("publish selected-feedback access log: %w", err)
	}
	if err := createOnce(rawPath, compressed); err != nil {
		return ArtifactResult{}, fmt.Errorf("publish raw: %w", err)
	}
	publishedConfig, _ := os.ReadFile(configPath)
	publishedStream, _ := os.ReadFile(streamPath)
	publishedFeedback, _ := os.ReadFile(feedbackPath)
	publishedFeedbackAccess, _ := os.ReadFile(feedbackAccessPath)
	publishedRaw, _ := os.ReadFile(rawPath)
	verified, decisions, lifecycle, metrics, outputSHA, err := VerifyRaw(publishedRaw, publishedConfig, publishedStream, issuer)
	if err != nil {
		return ArtifactResult{}, fmt.Errorf("verify published evidence: %w", err)
	}
	if len(verified) != len(run.Records) || !equalCounts(decisions, run.DecisionCounts) || !equalCounts(lifecycle, run.LifecycleCounts) || !reflect.DeepEqual(metrics, run.Metrics) {
		return ArtifactResult{}, errors.New("published raw recomputation disagrees with execution")
	}
	publishedAccessEvents, err := decodeFeedbackAccessJSONL(publishedFeedbackAccess)
	if err != nil || !reflect.DeepEqual(publishedAccessEvents, accessEvents) {
		return ArtifactResult{}, errors.New("published selected-feedback access log differs from execution")
	}
	if err := VerifyFeedbackAccessLog(publishedAccessEvents, verified, run.Config); err != nil {
		return ArtifactResult{}, fmt.Errorf("verify published selected-feedback access log: %w", err)
	}
	opps, _ := ParseStream(publishedStream, run.Config)
	manifest := Manifest{SchemaVersion: ManifestSchema, RecordType: "run_manifest", ExperimentID: run.Config.ExperimentID, RunID: run.Config.RunID, CellID: run.Config.CellID, Scenario: run.Config.Scenario, Seed: run.Config.Seed, RegistryID: run.Config.RegistryID, ProtocolMethodID: run.Config.ProtocolMethodID, ComparatorMethod: run.Config.ComparatorMethod, ProductionReservationMode: run.Config.ProductionReservationMode, MethodConfigSHA256: run.Config.MethodConfigSHA256, ClusterID: run.Config.ClusterID, TrialID: run.Config.TrialID, EvidenceTier: run.Config.EvidenceTier, EvidenceLabel: "development_non_citable", SoftwareSHA256: run.Config.SoftwareSHA256, DataSHA256: run.Config.DataSHA256, ProtocolSHA256: run.Config.ProtocolSHA256, SplitSHA256: run.Config.SplitSHA256, SelectedFeedbackModelMapSHA256: run.Config.SelectedFeedbackModelMapSHA256, SourceSHA256: run.Config.SourceSHA256, ConfigSHA256: run.ConfigSHA256, CanonicalConfigSHA256: run.CanonicalConfigSHA256, StreamSHA256: run.StreamSHA256, MatchedStreamKey: run.MatchedStreamKey, OutputSHA256: outputSHA, ConfigInput: ArtifactFileSpec{Path: filepath.Base(configPath), SHA256: SHA256(publishedConfig), Schema: ConfigSchema}, StreamInput: ArtifactFileSpec{Path: filepath.Base(streamPath), SHA256: SHA256(publishedStream), Schema: OpportunitySchema, ExpectedRecordCount: int64(len(opps))}, FeedbackInput: ArtifactFileSpec{Path: filepath.Base(feedbackPath), SHA256: SHA256(publishedFeedback), Schema: "govar-development-outcome-v1", ExpectedRecordCount: int64(len(opps))}, FeedbackAccessInput: ArtifactFileSpec{Path: filepath.Base(feedbackAccessPath), SHA256: SHA256(publishedFeedbackAccess), Schema: FeedbackAccessSchema, ExpectedRecordCount: int64(len(publishedAccessEvents))}, RawFiles: []RawFileSpec{{Path: filepath.Base(rawPath), SHA256: SHA256(publishedRaw), UncompressedSHA256: outputSHA, Schema: LifecycleSchema, ExpectedRecordCount: int64(len(verified))}}, DecisionCounts: decisions, LifecycleCounts: lifecycle, Metrics: metrics}
	manifestRaw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return ArtifactResult{}, err
	}
	manifestRaw = append(manifestRaw, '\n')
	if err := createOnce(manifestPath, manifestRaw); err != nil {
		return ArtifactResult{}, fmt.Errorf("publish manifest: %w", err)
	}
	return ArtifactResult{RawPath: rawPath, ManifestPath: manifestPath, ConfigPath: configPath, StreamPath: streamPath, FeedbackPath: feedbackPath, FeedbackAccessPath: feedbackAccessPath, Manifest: manifest}, nil
}

func VerifyManifestDirectory(manifestPath string, issuer OutcomeIssuer) (Manifest, error) {
	body, err := os.ReadFile(manifestPath)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Manifest{}, err
	}
	if manifest.SchemaVersion != ManifestSchema || manifest.RecordType != "run_manifest" || len(manifest.RawFiles) != 1 || manifest.EvidenceLabel != "development_non_citable" {
		return Manifest{}, errors.New("unsupported manifest")
	}
	directory := filepath.Dir(manifestPath)
	for _, name := range []string{manifest.ConfigInput.Path, manifest.StreamInput.Path, manifest.FeedbackInput.Path, manifest.FeedbackAccessInput.Path, manifest.RawFiles[0].Path} {
		if filepath.Base(name) != name || !safeFileName(name) {
			return Manifest{}, errors.New("manifest contains unsafe artifact path")
		}
	}
	configRaw, err := os.ReadFile(filepath.Join(directory, manifest.ConfigInput.Path))
	if err != nil {
		return Manifest{}, err
	}
	streamRaw, err := os.ReadFile(filepath.Join(directory, manifest.StreamInput.Path))
	if err != nil {
		return Manifest{}, err
	}
	feedbackRaw, err := os.ReadFile(filepath.Join(directory, manifest.FeedbackInput.Path))
	if err != nil {
		return Manifest{}, err
	}
	feedbackAccessRaw, err := os.ReadFile(filepath.Join(directory, manifest.FeedbackAccessInput.Path))
	if err != nil {
		return Manifest{}, err
	}
	raw, err := os.ReadFile(filepath.Join(directory, manifest.RawFiles[0].Path))
	if err != nil {
		return Manifest{}, err
	}
	if SHA256(configRaw) != manifest.ConfigInput.SHA256 || SHA256(streamRaw) != manifest.StreamInput.SHA256 || SHA256(feedbackRaw) != manifest.FeedbackInput.SHA256 || SHA256(feedbackAccessRaw) != manifest.FeedbackAccessInput.SHA256 || SHA256(raw) != manifest.RawFiles[0].SHA256 {
		return Manifest{}, errors.New("manifest input/raw digest mismatch")
	}
	config, canonical, err := ParseConfig(configRaw)
	if err != nil {
		return Manifest{}, err
	}
	opps, err := ParseStream(streamRaw, config)
	if err != nil {
		return Manifest{}, err
	}
	if SHA256(feedbackRaw) != issuer.BindingEvidence().FeedbackArtifactSHA256 || manifest.FeedbackInput.Schema != "govar-development-outcome-v1" || manifest.FeedbackInput.ExpectedRecordCount != int64(len(opps)) {
		return Manifest{}, errors.New("manifest feedback input does not equal the development fixture binding")
	}
	if fixture, ok := issuer.(*DevelopmentFixtureIssuer); ok {
		if err := fixture.ValidateOpportunityCoverage(opps, config.SelectedFeedbackModelIDs); err != nil {
			return Manifest{}, err
		}
	}
	records, decisions, lifecycle, metrics, outputSHA, err := VerifyRaw(raw, configRaw, streamRaw, issuer)
	if err != nil {
		return Manifest{}, err
	}
	accessEvents, err := decodeFeedbackAccessJSONL(feedbackAccessRaw)
	if err != nil || manifest.FeedbackAccessInput.Schema != FeedbackAccessSchema || manifest.FeedbackAccessInput.ExpectedRecordCount != int64(len(accessEvents)) {
		return Manifest{}, errors.New("manifest selected-feedback access input is invalid")
	}
	if err := VerifyFeedbackAccessLog(accessEvents, records, config); err != nil {
		return Manifest{}, err
	}
	if manifest.ConfigSHA256 != SHA256(configRaw) || manifest.CanonicalConfigSHA256 != canonical || manifest.StreamSHA256 != SHA256(streamRaw) || manifest.OutputSHA256 != outputSHA || manifest.RawFiles[0].UncompressedSHA256 != outputSHA || manifest.RawFiles[0].ExpectedRecordCount != int64(len(records)) || manifest.StreamInput.ExpectedRecordCount != int64(len(opps)) || !equalCounts(manifest.DecisionCounts, decisions) || !equalCounts(manifest.LifecycleCounts, lifecycle) || !reflect.DeepEqual(manifest.Metrics, metrics) {
		return Manifest{}, errors.New("manifest does not equal full recomputation")
	}
	expectedMatched := DomainHash("govar-matched-stream-v1", []byte(manifest.StreamSHA256), []byte(fmt.Sprint(config.Seed)))
	if manifest.MatchedStreamKey != expectedMatched || manifest.ConfigInput.Schema != ConfigSchema || manifest.StreamInput.Schema != OpportunitySchema || manifest.RawFiles[0].Schema != LifecycleSchema {
		return Manifest{}, errors.New("manifest schema or matched-stream binding does not recompute")
	}
	if manifest.ExperimentID != config.ExperimentID || manifest.RunID != config.RunID || manifest.CellID != config.CellID || manifest.Scenario != config.Scenario || manifest.Seed != config.Seed || manifest.RegistryID != config.RegistryID || manifest.ProtocolMethodID != config.ProtocolMethodID || manifest.ComparatorMethod != config.ComparatorMethod || manifest.ProductionReservationMode != config.ProductionReservationMode || manifest.MethodConfigSHA256 != config.MethodConfigSHA256 || manifest.SelectedFeedbackModelMapSHA256 != config.SelectedFeedbackModelMapSHA256 || manifest.ClusterID != config.ClusterID || manifest.TrialID != config.TrialID || manifest.EvidenceTier != config.EvidenceTier || manifest.SourceSHA256 != config.SourceSHA256 || manifest.SoftwareSHA256 != config.SoftwareSHA256 || manifest.DataSHA256 != config.DataSHA256 || manifest.ProtocolSHA256 != config.ProtocolSHA256 || manifest.SplitSHA256 != config.SplitSHA256 {
		return Manifest{}, errors.New("manifest identity does not match exact config")
	}
	return manifest, nil
}

func safeFileName(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 255 {
		return false
	}
	for _, r := range name {
		if (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}
func createOnce(path string, body []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(body); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}
func pathExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil || !errors.Is(err, os.ErrNotExist)
}
