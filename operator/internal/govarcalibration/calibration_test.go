package govarcalibration

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

var testSHA = strings.Repeat("a", 64)
var testCapSHA = strings.Repeat("b", 64)
var testSoftwareSHA = strings.Repeat("c", 64)

func testConfig() BuildConfig {
	return BuildConfig{ArtifactRef: "calibration-2026-07", Version: "v1", FeatureSchemaVersion: "features-v1",
		PriceRegimeSHA256: testSHA, CapRegimeSHA256: testCapSHA,
		ProducerSoftwareSHA256: testSoftwareSHA, CoverageTargetPPB: 800_000_000}
}

func rows(split string, values ...int64) []Observation {
	result := make([]Observation, len(values))
	for i, value := range values {
		result[i] = Observation{RequestID: fmt.Sprintf("r-%02d", i), ProviderAttemptID: fmt.Sprintf("r-%02d:a1", i),
			Split: split, Selected: true, AuthoritativeFinal: true, OutputTokens: value,
			FeatureSchemaVersion: "features-v1", PriceRegimeSHA256: testSHA, CapRegimeSHA256: testCapSHA,
			SettledAt: time.Date(2026, 7, 1, 0, i, 0, 0, time.UTC)}
	}
	return result
}

func TestGoldenArtifactIsOrderIndependentAndReproducible(t *testing.T) {
	input := rows(SplitCalibration, 5, 1, 3, 2, 4)
	a, err := BuildArtifact(testConfig(), input)
	if err != nil {
		t.Fatal(err)
	}
	if a.UpperOutputTokens != 5 || a.Support != 5 || a.EmpiricalCoveragePPB != 1_000_000_000 ||
		a.ConformalRank != 5 || a.ExchangeableMarginalCoverageLowerPPB != 833_333_333 {
		t.Fatalf("artifact=%+v", a)
	}
	reversed := append([]Observation(nil), input...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	b, err := BuildArtifact(testConfig(), reversed)
	if err != nil || a != b {
		t.Fatalf("second producer mismatch: first=%+v second=%+v err=%v", a, b, err)
	}
	const golden = "665d2e7ab857a6be543ffce13063f8e7c2778684e4ec8eea2a60e9a2b9a12e10"
	if a.ArtifactSHA256 != golden {
		t.Fatalf("artifact digest=%s, want %s", a.ArtifactSHA256, golden)
	}
}

func TestCalibrationRejectsLeakageAndNonFinalInputs(t *testing.T) {
	base := rows(SplitCalibration, 10)[0]
	cases := map[string]func(*Observation){
		"frozen-test":    func(v *Observation) { v.Split = SplitFrozenTest },
		"monitoring":     func(v *Observation) { v.Split = SplitMonitoring },
		"counterfactual": func(v *Observation) { v.Selected = false },
		"provisional":    func(v *Observation) { v.AuthoritativeFinal = false },
		"excluded":       func(v *Observation) { v.Excluded = true },
		"price-regime":   func(v *Observation) { v.PriceRegimeSHA256 = strings.Repeat("d", 64) },
		"cap-regime":     func(v *Observation) { v.CapRegimeSHA256 = strings.Repeat("d", 64) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			row := base
			mutate(&row)
			if _, err := BuildArtifact(testConfig(), []Observation{row}); err == nil {
				t.Fatal("forbidden row accepted")
			}
		})
	}
	if _, err := BuildArtifact(testConfig(), rows(SplitDevelopment, 1, 2)); err == nil {
		t.Fatal("development observations entered the split-conformal calibration set")
	}
}

func TestFiniteSampleTargetRejectsUnsupportedRank(t *testing.T) {
	cfg := testConfig()
	cfg.CoverageTargetPPB = 999_000_000
	if _, err := BuildArtifact(cfg, rows(SplitCalibration, 1, 2, 3, 4, 5)); err == nil {
		t.Fatal("finite calibration set advertised an unsupported 0.999 target")
	}
}

func TestCoverageGapNominalShiftsHeavyTailAndRecovery(t *testing.T) {
	artifact, err := BuildArtifact(testConfig(), rows(SplitCalibration, 1, 2, 3, 4, 5))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		values []int64
		want   bool
	}{
		{"nominal", []int64{1, 2, 3, 4, 4}, false},
		{"plus-50", []int64{2, 3, 5, 6, 6}, true},
		{"plus-100", []int64{2, 4, 6, 8, 10}, true},
		{"plus-200", []int64{3, 6, 9, 12, 15}, true},
		{"heavy-tail", []int64{1, 2, 30, 60, 120}, true},
		{"revalidated", []int64{1, 2, 3, 3, 4}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DetectCoverageGap(artifact, 50_000_000, rows(SplitMonitoring, tt.values...))
			if err != nil || result.Detected != tt.want {
				t.Fatalf("result=%+v err=%v want detected=%v", result, err, tt.want)
			}
		})
	}
	provisional := rows(SplitMonitoring, 1)[0]
	provisional.AuthoritativeFinal = false
	if _, err := DetectCoverageGap(artifact, 0, []Observation{provisional}); err == nil {
		t.Fatal("delayed/provisional settlement entered monitoring window")
	}
}

func TestConcurrentProducersPublishOneDigest(t *testing.T) {
	input := rows(SplitCalibration, 4, 1, 5, 2, 3)
	const workers = 16
	results := make(chan Artifact, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, err := BuildArtifact(testConfig(), input)
			results <- a
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var digest string
	for result := range results {
		if digest == "" {
			digest = result.ArtifactSHA256
		}
		if result.ArtifactSHA256 != digest {
			t.Fatalf("non-deterministic digest %s != %s", result.ArtifactSHA256, digest)
		}
	}
}
