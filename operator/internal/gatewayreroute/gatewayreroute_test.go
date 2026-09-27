/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gatewayreroute

import "testing"

// rule builds an AIGatewayRoute rule (the decoded-unstructured shape) that matches
// the given model header and routes to the given backend name.
func rule(model, backend string) map[string]any {
	return map[string]any{
		"matches": []any{
			map[string]any{
				"headers": []any{
					map[string]any{"name": ModelHeaderName, "value": model},
				},
			},
		},
		"backendRefs": []any{
			map[string]any{"name": backend},
		},
	}
}

// backendOf reads the first backendRef name of a rule.
func backendOf(t *testing.T, r map[string]any) string {
	t.Helper()
	brs := r["backendRefs"].([]any)
	return brs[0].(map[string]any)["name"].(string)
}

// hasBodyMutation reports whether the rule's first backendRef rewrites the model.
func hasBodyMutation(r map[string]any) bool {
	brs, _ := r["backendRefs"].([]any)
	if len(brs) == 0 {
		return false
	}
	b0, _ := brs[0].(map[string]any)
	_, ok := b0["bodyMutation"]
	return ok
}

func TestRuleModel(t *testing.T) {
	if got := RuleModel(rule("gpt-4o", "openai-backend")); got != "gpt-4o" {
		t.Fatalf("RuleModel = %q, want gpt-4o", got)
	}
	if got := RuleModel(map[string]any{}); got != "" {
		t.Fatalf("RuleModel of model-less rule = %q, want empty", got)
	}
}

func TestApply_Reroute(t *testing.T) {
	rules := []any{
		rule("gpt-4o", "openai-backend"),
		rule("mistral-large", "mistral-backend"),
	}
	saved := map[string]string{}
	backends := map[string]string{"gpt-4o": "openai-backend", "mistral-large": "mistral-backend"}
	desired := map[string]string{"gpt-4o": "mistral-large"} // forbidden gpt-4o -> compliant mistral

	actuated, changed := Apply(rules, saved, desired, backends)
	if !changed {
		t.Fatal("expected changed=true")
	}
	if !actuated["gpt-4o"] {
		t.Fatalf("expected gpt-4o actuated, got %v", actuated)
	}
	r0 := rules[0].(map[string]any)
	if got := backendOf(t, r0); got != "mistral-backend" {
		t.Fatalf("gpt-4o backend = %q, want mistral-backend", got)
	}
	if !hasBodyMutation(r0) {
		t.Fatal("expected bodyMutation rewriting model on the rerouted rule")
	}
	if saved["gpt-4o"] != "openai-backend" {
		t.Fatalf("saved original = %q, want openai-backend", saved["gpt-4o"])
	}
	// The compliant rule must be untouched.
	if got := backendOf(t, rules[1].(map[string]any)); got != "mistral-backend" {
		t.Fatalf("mistral rule changed unexpectedly: %q", got)
	}
}

func TestApply_RevertWhenNoLongerDesired(t *testing.T) {
	rules := []any{rule("gpt-4o", "openai-backend")}
	backends := map[string]string{"gpt-4o": "openai-backend", "mistral-large": "mistral-backend"}

	saved := map[string]string{}
	Apply(rules, saved, map[string]string{"gpt-4o": "mistral-large"}, backends)

	// Now revert: desired is empty.
	actuated, changed := Apply(rules, saved, map[string]string{}, backends)
	if !changed {
		t.Fatal("expected changed=true on revert")
	}
	if len(actuated) != 0 {
		t.Fatalf("expected nothing actuated on revert, got %v", actuated)
	}
	r0 := rules[0].(map[string]any)
	if got := backendOf(t, r0); got != "openai-backend" {
		t.Fatalf("reverted backend = %q, want openai-backend", got)
	}
	if hasBodyMutation(r0) {
		t.Fatal("expected bodyMutation removed after revert")
	}
	if _, ok := saved["gpt-4o"]; ok {
		t.Fatal("expected saved entry cleared after revert")
	}
}

func TestApply_UnknownTargetBackendSkipped(t *testing.T) {
	rules := []any{rule("gpt-4o", "openai-backend")}
	saved := map[string]string{}
	backends := map[string]string{"gpt-4o": "openai-backend"} // no backend for the target
	desired := map[string]string{"gpt-4o": "mistral-large"}

	actuated, changed := Apply(rules, saved, desired, backends)
	if changed {
		t.Fatal("expected changed=false when target backend unknown")
	}
	if len(actuated) != 0 {
		t.Fatalf("expected nothing actuated, got %v", actuated)
	}
	if got := backendOf(t, rules[0].(map[string]any)); got != "openai-backend" {
		t.Fatalf("backend changed despite unknown target: %q", got)
	}
}

func TestApplyControls_Block(t *testing.T) {
	rules := []any{rule("gpt-4o", "openai-backend")}
	saved := map[string]string{}

	res := ApplyControls(rules, saved, nil, map[string]bool{"gpt-4o": true}, nil)
	if !res.Changed {
		t.Fatal("expected changed=true")
	}
	if !res.Blocked["gpt-4o"] {
		t.Fatalf("expected gpt-4o blocked, got %+v", res)
	}
	if len(res.Rerouted) != 0 {
		t.Fatalf("expected no reroute actuation, got %+v", res.Rerouted)
	}
	r0 := rules[0].(map[string]any)
	if got := backendOf(t, r0); got != BlockBackendName {
		t.Fatalf("blocked backend = %q, want %s", got, BlockBackendName)
	}
	if hasBodyMutation(r0) {
		t.Fatal("block must remove any bodyMutation")
	}
	if saved["gpt-4o"] != "openai-backend" {
		t.Fatalf("saved original = %q, want openai-backend", saved["gpt-4o"])
	}
}

func TestApplyControls_RevertBlock(t *testing.T) {
	rules := []any{rule("gpt-4o", "openai-backend")}
	saved := map[string]string{}

	ApplyControls(rules, saved, nil, map[string]bool{"gpt-4o": true}, nil)
	res := ApplyControls(rules, saved, nil, nil, nil)
	if !res.Changed {
		t.Fatal("expected changed=true on block revert")
	}
	if got := backendOf(t, rules[0].(map[string]any)); got != "openai-backend" {
		t.Fatalf("reverted backend = %q, want openai-backend", got)
	}
	if _, ok := saved["gpt-4o"]; ok {
		t.Fatal("expected saved entry cleared after block revert")
	}
}

func TestApplyControls_SwitchRerouteToBlock(t *testing.T) {
	rules := []any{rule("gpt-4o", "openai-backend")}
	saved := map[string]string{}
	backends := map[string]string{"gpt-4o": "openai-backend", "mistral-large": "mistral-backend"}

	ApplyControls(rules, saved, map[string]string{"gpt-4o": "mistral-large"}, nil, backends)
	res := ApplyControls(rules, saved, nil, map[string]bool{"gpt-4o": true}, backends)
	if !res.Blocked["gpt-4o"] {
		t.Fatalf("expected block after reroute, got %+v", res)
	}
	r0 := rules[0].(map[string]any)
	if got := backendOf(t, r0); got != BlockBackendName {
		t.Fatalf("backend = %q, want %s", got, BlockBackendName)
	}
	if hasBodyMutation(r0) {
		t.Fatal("expected bodyMutation removed when switching to block")
	}
	if saved["gpt-4o"] != "openai-backend" {
		t.Fatalf("saved original corrupted: %q", saved["gpt-4o"])
	}
}

func TestApply_Idempotent(t *testing.T) {
	rules := []any{rule("gpt-4o", "openai-backend")}
	saved := map[string]string{}
	backends := map[string]string{"gpt-4o": "openai-backend", "mistral-large": "mistral-backend"}
	desired := map[string]string{"gpt-4o": "mistral-large"}

	Apply(rules, saved, desired, backends)
	// Applying the same desired again still reports the model actuated, and the
	// saved original must remain the genuine first backend (not the rerouted one).
	_, _ = Apply(rules, saved, desired, backends)
	if saved["gpt-4o"] != "openai-backend" {
		t.Fatalf("saved original corrupted on second apply: %q", saved["gpt-4o"])
	}
	if got := backendOf(t, rules[0].(map[string]any)); got != "mistral-backend" {
		t.Fatalf("backend = %q, want mistral-backend", got)
	}
}
