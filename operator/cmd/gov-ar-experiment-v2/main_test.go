package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIV2RequiresEveryExplicitInput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runCLI(nil, &stdout, &stderr); code != 2 {
		t.Fatalf("missing inputs returned %d, stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "--support-mode is required") {
		t.Fatalf("unexpected missing-input response: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestCLIV2RejectsSymlinkInputBeforeParsing(t *testing.T) {
	directory := t.TempDir()
	realPath := filepath.Join(directory, "input.json")
	if err := os.WriteFile(realPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath := realPath + ".link"
	if err := os.Symlink(realPath, linkPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	arguments := []string{
		"--support-mode", "fixture", "--config", linkPath, "--stream", realPath, "--preoutcome-mapping", realPath,
		"--development-usage", realPath, "--producer-evidence", realPath,
		"--protocol", realPath, "--source-code-binding", realPath, "--source-manifest", realPath,
		"--pilot-usage-binding", realPath, "--runtime-capability", realPath, "--design-spec", realPath,
		"--decision-config-template", realPath, "--independent-design-review", realPath,
		"--design-manifest", realPath, "--cohort-registry", realPath, "--profile-execution-contract", realPath,
		"--budget-calibration", realPath, "--calibration-artifact", realPath, "--candidate-set", realPath, "--profile-registry", realPath,
		"--slot-template", realPath, "--method-builder-bundle", realPath, "--execution-intent", realPath,
		"--independent-authorization", realPath,
		"--output-dir", filepath.Join(directory, "output"),
	}
	var stdout, stderr bytes.Buffer
	if code := runCLI(arguments, &stdout, &stderr); code != 1 {
		t.Fatalf("symlink input returned %d, stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "symlink") {
		t.Fatalf("unexpected symlink response: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestCLIV2P1bExactForbidsExternalConfigAndProducerEvidence(t *testing.T) {
	arguments := []string{
		"--support-mode", "p1b_exact", "--config", "external-config.json", "--producer-evidence", "external-evidence.json",
		"--stream", "stream.jsonl", "--preoutcome-mapping", "mapping.jsonl", "--development-usage", "usage.jsonl",
		"--protocol", "protocol.json", "--source-code-binding", "source-code.json", "--source-manifest", "source.json",
		"--pilot-usage-binding", "pilot.json", "--runtime-capability", "runtime.json", "--design-spec", "design.json",
		"--decision-config-template", "decision.json", "--independent-design-review", "review.json",
		"--design-manifest", "manifest.json", "--cohort-registry", "cohorts.json",
		"--profile-execution-contract", "profile-contract.json", "--budget-calibration", "budget.json",
		"--calibration-artifact", "calibration.json", "--candidate-set", "candidates.json",
		"--profile-registry", "profiles.json", "--slot-template", "slots.json",
		"--method-builder-bundle", "builder.json", "--execution-intent", "intent.json",
		"--independent-authorization", "authorization.json", "--output-dir", "output",
	}
	var stdout, stderr bytes.Buffer
	if code := runCLI(arguments, &stdout, &stderr); code != 2 {
		t.Fatalf("p1b_exact external post-reveal inputs returned %d, stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "forbids --config and --producer-evidence") {
		t.Fatalf("unexpected p1b_exact external-input response: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}
