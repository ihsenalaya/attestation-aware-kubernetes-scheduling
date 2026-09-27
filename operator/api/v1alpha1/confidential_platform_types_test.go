package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestConfidentialInferencePolicySpecValidate(t *testing.T) {
	errs := (ConfidentialInferencePolicySpec{
		RequireConfidentialGPU:        true,
		RequireConfidentialContainers: true,
		KeyRelease:                    PolicyKeyReleaseSpec{Required: true},
	}).Validate(field.NewPath("spec"))
	if len(errs) != 3 {
		t.Fatalf("errors = %d, want 3", len(errs))
	}
}

func TestAIKeyReleasePolicySpecValidate(t *testing.T) {
	errs := (AIKeyReleasePolicySpec{
		RequireAttestedEvidence: true,
		KeyRelease:              PolicyKeyReleaseSpec{Required: true},
	}).Validate(field.NewPath("spec"))
	if len(errs) != 2 {
		t.Fatalf("errors = %d, want 2", len(errs))
	}
}

func TestAIRevocationPolicySpecValidate(t *testing.T) {
	errs := (AIRevocationPolicySpec{
		Reasons: []string{"", "  "},
	}).Validate(field.NewPath("spec"))
	if len(errs) != 2 {
		t.Fatalf("errors = %d, want 2", len(errs))
	}
}
