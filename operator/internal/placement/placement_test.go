package placement

import "testing"

func TestEvaluatePlacement(t *testing.T) {
	decision := Evaluate(Policy{
		AllowedTEEs:           []string{"TDX", "SEV-SNP"},
		MaxEvidenceAgeSeconds: 300,
		RequiredRuntimeClass:  "simulated-kata-qemu-tdx",
	}, Evidence{
		TEE:              "TDX",
		AgeSeconds:       30,
		RuntimeClassName: "simulated-kata-qemu-tdx",
	})
	if !decision.Allow {
		t.Fatalf("expected allowed decision, got %+v", decision)
	}
}

func TestRejectsRevokedEvidence(t *testing.T) {
	decision := Evaluate(Policy{AllowedTEEs: []string{"TDX"}}, Evidence{TEE: "TDX", Revoked: true})
	if decision.Allow || decision.Reason != "evidence revoked" {
		t.Fatalf("unexpected decision %+v", decision)
	}
}
