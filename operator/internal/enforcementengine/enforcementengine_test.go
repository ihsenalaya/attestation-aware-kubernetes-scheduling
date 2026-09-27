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

package enforcementengine

import "testing"

func violation() Violation {
	return Violation{Namespace: "finance", Application: "risk-assistant", Provider: "openai-us", Zone: "US", Model: "gpt-4o", Requests: 132}
}

func TestReportOnlyNeverActs(t *testing.T) {
	d := DecideSovereignty(ModeReportOnly, []Violation{violation()}, "mistral-large")
	if len(d) != 1 || d[0].Action != ActionReport {
		t.Fatalf("reportOnly action = %+v, want report", d)
	}
	if !d[0].Actuated {
		t.Error("report should be fully actuated (it only records)")
	}
}

func TestWarnRaisesAlert(t *testing.T) {
	d := DecideSovereignty(ModeWarn, []Violation{violation()}, "")
	if d[0].Action != ActionWarn || !d[0].Actuated {
		t.Errorf("warn = %+v, want actuated warn", d[0])
	}
}

func TestEnforceReroutesWhenFallbackExists(t *testing.T) {
	d := DecideSovereignty(ModeEnforce, []Violation{violation()}, "mistral-large")
	if d[0].Action != ActionReroute {
		t.Fatalf("enforce+fallback action = %s, want reroute", d[0].Action)
	}
	if d[0].RerouteTo != "mistral-large" {
		t.Errorf("reroute target = %q, want mistral-large", d[0].RerouteTo)
	}
	if d[0].Actuated {
		t.Error("pure engine must leave reroute unactuated until the controller mutates the gateway")
	}
}

func TestEnforceBlocksWithoutFallback(t *testing.T) {
	d := DecideSovereignty(ModeEnforce, []Violation{violation()}, "")
	if d[0].Action != ActionBlock {
		t.Fatalf("enforce without fallback action = %s, want block", d[0].Action)
	}
	if d[0].Actuated {
		t.Error("pure engine must leave block unactuated until the controller mutates the gateway")
	}
}

func TestCountByAction(t *testing.T) {
	vs := []Violation{violation(), {Namespace: "legal", Application: "contract-review", Zone: "US", Requests: 5}}
	d := DecideSovereignty(ModeEnforce, vs, "mistral-large")
	if c := CountByAction(d); c[ActionReroute] != 2 {
		t.Errorf("reroute count = %d, want 2", c[ActionReroute])
	}
}
