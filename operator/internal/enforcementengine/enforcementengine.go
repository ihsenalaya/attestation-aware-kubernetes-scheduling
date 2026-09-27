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

// Package enforcementengine turns a policy's enforcement mode and the observed
// violations into concrete enforcement Decisions. It is pure (no Kubernetes
// dependency) and is the single place that decides WHAT should happen for a
// violation; controllers then carry the decision out and replace Actuated with
// the real data-plane outcome.
//
// Modes (sovereignty): reportOnly (record only), warn (raise an alert — fully
// actuated immediately), enforce (block, or reroute to a compliant model through
// the gateway actuator).
package enforcementengine

import "fmt"

// Action is the enforcement action chosen for a violation.
type Action string

const (
	// ActionReport records the violation without acting (reportOnly).
	ActionReport Action = "report"
	// ActionWarn raises an alert (event/condition/metric) without blocking.
	ActionWarn Action = "warn"
	// ActionBlock denies the non-compliant traffic (enforce, no compliant fallback).
	ActionBlock Action = "block"
	// ActionReroute sends the traffic to a compliant model (enforce + fallback).
	ActionReroute Action = "reroute"
)

// Violation is one workload's exposure to a forbidden sovereignty zone.
type Violation struct {
	Namespace, Application, Provider, Zone, Model string
	Requests                                      int64
}

// Decision is the enforcement action chosen for a single violation. The pure
// engine marks report/warn as actuated immediately and leaves gateway-dependent
// actions false; the controller flips block/reroute to true only after the route
// mutation succeeds.
type Decision struct {
	Mode        string
	Action      Action
	Namespace   string
	Application string
	Provider    string
	Zone        string
	Model       string
	Requests    int64
	RerouteTo   string // compliant target model when Action == ActionReroute
	Message     string
	Actuated    bool
}

// Mode constants mirror the CRD's EnforcementMode values (kept as strings so the
// engine stays free of the API package).
const (
	ModeReportOnly = "reportOnly"
	ModeWarn       = "warn"
	ModeEnforce    = "enforce"
)

// DecideSovereignty maps each violation to an enforcement Decision under the given
// mode. fallbackModel, when non-empty, is a compliant (allowed-zone) model the
// traffic could be rerouted to in enforce mode; when empty, enforce blocks instead.
func DecideSovereignty(mode string, violations []Violation, fallbackModel string) []Decision {
	out := make([]Decision, 0, len(violations))
	for _, v := range violations {
		d := Decision{
			Mode: mode, Namespace: v.Namespace, Application: v.Application,
			Provider: v.Provider, Zone: v.Zone, Model: v.Model, Requests: v.Requests,
		}
		switch mode {
		case ModeWarn:
			d.Action = ActionWarn
			d.Actuated = true
			d.Message = fmt.Sprintf("warn: %s/%s sent %d request(s) to forbidden zone %s via %q — alert raised",
				v.Namespace, v.Application, v.Requests, v.Zone, v.Provider)
		case ModeEnforce:
			if fallbackModel != "" {
				d.Action = ActionReroute
				d.RerouteTo = fallbackModel
				d.Message = fmt.Sprintf("enforce: %s/%s — reroute %q to compliant %q",
					v.Namespace, v.Application, v.Model, fallbackModel)
			} else {
				d.Action = ActionBlock
				d.Message = fmt.Sprintf("enforce: %s/%s — block traffic to forbidden zone %s, no compliant fallback",
					v.Namespace, v.Application, v.Zone)
			}
			d.Actuated = false // controller sets true only after the gateway route is mutated
		default: // reportOnly (or unknown) — never blocks
			d.Action = ActionReport
			d.Actuated = true
			d.Message = fmt.Sprintf("reportOnly: %s/%s exposure to forbidden zone %s recorded (no action)",
				v.Namespace, v.Application, v.Zone)
		}
		out = append(out, d)
	}
	return out
}

// CountByAction tallies decisions per action, for a compact status/metric summary.
func CountByAction(decisions []Decision) map[Action]int {
	m := map[Action]int{}
	for _, d := range decisions {
		m[d.Action]++
	}
	return m
}
