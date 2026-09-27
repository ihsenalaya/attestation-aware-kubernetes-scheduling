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

// Package gatewayreroute holds the pure, dependency-free logic that mutates an
// Envoy AIGatewayRoute's rules to enforce sovereignty controls. It operates on the
// already-decoded `[]any` rules slice (as produced by the unstructured client) so
// it can be unit-tested without a live API server. The controller wires it up in
// gatewayactuator.go.
package gatewayreroute

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// ModelHeaderName is the request header an AIGatewayRoute rule matches on to pick
// a model/backend.
const ModelHeaderName = "x-ai-eg-model"

// BlockBackendName is a deliberately absent backend used to make the route reject
// matched model traffic when no compliant reroute target exists. The original
// backend is kept in the caller's saved map so the mutation is reversible.
const BlockBackendName = "aiops-blocked"

// RuleModel returns the x-ai-eg-model Exact match value of a route rule (empty if
// the rule does not match on a model).
func RuleModel(rule map[string]any) string {
	matches, _, _ := unstructured.NestedSlice(rule, "matches")
	for _, m := range matches {
		mm, ok := m.(map[string]any)
		if !ok {
			continue
		}
		headers, _, _ := unstructured.NestedSlice(mm, "headers")
		for _, h := range headers {
			hh, ok := h.(map[string]any)
			if !ok {
				continue
			}
			if name, _, _ := unstructured.NestedString(hh, "name"); name == ModelHeaderName {
				val, _, _ := unstructured.NestedString(hh, "value")
				return val
			}
		}
	}
	return ""
}

// firstBackend returns the first backendRef map of a rule (nil if none).
func firstBackend(rule map[string]any) (brs []any, b0 map[string]any, ok bool) {
	brs, _, _ = unstructured.NestedSlice(rule, "backendRefs")
	if len(brs) == 0 {
		return nil, nil, false
	}
	b0, ok = brs[0].(map[string]any)
	if !ok {
		return nil, nil, false
	}
	return brs, b0, true
}

// Result describes which controls were applied to route rules.
type Result struct {
	Rerouted map[string]bool
	Blocked  map[string]bool
	Changed  bool
}

// ApplyControls mutates `rules` in place to reflect the desired controls:
//
//   - For each rule whose model is a key of `desired`, its first backendRef is
//     pointed at backends[desired[model]] and a bodyMutation rewrites the request
//     `model` field to the desired target. The original backend name is recorded in
//     `saved` (once) so the change can be reverted. A desired target whose backend
//     is unknown (not in `backends`) is skipped — never reroute somewhere unsafe.
//   - For each rule whose model is in `blocked`, its first backendRef is pointed at
//     BlockBackendName and any bodyMutation is removed. Envoy AI Gateway cannot
//     resolve that backend, so the matched model is denied instead of leaving the
//     cluster for a forbidden provider.
//   - For each rule whose model is in `saved` but no longer in either desired
//     control set, the rule is reverted to its original backend and the
//     bodyMutation is removed.
//
// `saved` is updated to match the new state (entries added on reroute, removed on
// revert). Returns the set of models actually actuated and whether any rule changed.
// Passing empty/nil `desired` and `blocked` reverts everything previously saved.
func ApplyControls(rules []any, saved, desired map[string]string, blocked map[string]bool, backends map[string]string) Result {
	res := Result{Rerouted: map[string]bool{}, Blocked: map[string]bool{}}
	for ri := range rules {
		rule, ok := rules[ri].(map[string]any)
		if !ok {
			continue
		}
		model := RuleModel(rule)
		if model == "" {
			continue
		}
		brs, b0, ok := firstBackend(rule)
		if !ok {
			continue
		}
		if target, want := desired[model]; want {
			targetBackend := backends[target]
			if targetBackend == "" {
				continue // unknown target backend — cannot reroute safely
			}
			if _, already := saved[model]; !already {
				orig, _, _ := unstructured.NestedString(b0, "name")
				saved[model] = orig
			}
			b0["name"] = targetBackend
			b0["bodyMutation"] = map[string]any{
				"set": []any{map[string]any{"path": "model", "value": target}},
			}
			brs[0] = b0
			_ = unstructured.SetNestedSlice(rule, brs, "backendRefs")
			rules[ri] = rule
			res.Changed = true
			res.Rerouted[model] = true
		} else if blocked[model] {
			if _, already := saved[model]; !already {
				orig, _, _ := unstructured.NestedString(b0, "name")
				saved[model] = orig
			}
			b0["name"] = BlockBackendName
			delete(b0, "bodyMutation")
			brs[0] = b0
			_ = unstructured.SetNestedSlice(rule, brs, "backendRefs")
			rules[ri] = rule
			res.Changed = true
			res.Blocked[model] = true
		} else if orig, ok := saved[model]; ok {
			// No longer desired — revert this model's rule to its original backend.
			b0["name"] = orig
			delete(b0, "bodyMutation")
			brs[0] = b0
			_ = unstructured.SetNestedSlice(rule, brs, "backendRefs")
			rules[ri] = rule
			delete(saved, model)
			res.Changed = true
		}
	}
	return res
}

// Apply preserves the original reroute-only API used by budget fallback code.
func Apply(rules []any, saved, desired, backends map[string]string) (actuated map[string]bool, changed bool) {
	res := ApplyControls(rules, saved, desired, nil, backends)
	return res.Rerouted, res.Changed
}
