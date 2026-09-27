package bootstrap

import "testing"

func TestPlatformModePreservesProductionModes(t *testing.T) {
	t.Setenv(platformModeEnv, "aks-private")
	if got := platformMode(); got != "aks-private" {
		t.Fatalf("platformMode() = %q, want aks-private", got)
	}

	t.Setenv(platformModeEnv, "production")
	if got := platformMode(); got != "production" {
		t.Fatalf("platformMode() = %q, want production", got)
	}
}

func TestPlatformModeKeepsKindSimulationDefault(t *testing.T) {
	t.Setenv(platformModeEnv, "")
	if got := platformMode(); got != platformModeSimulatedKind {
		t.Fatalf("platformMode() = %q, want %q", got, platformModeSimulatedKind)
	}

	t.Setenv(platformModeEnv, "kind")
	if got := platformMode(); got != platformModeSimulatedKind {
		t.Fatalf("platformMode() = %q, want %q", got, platformModeSimulatedKind)
	}
}
