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

package budgetengine

import "testing"

func TestEvaluatePhases(t *testing.T) {
	th := Thresholds{WarningPct: 70, CriticalPct: 90, HardLimitPct: 100}
	act := Actions{
		OnWarning:   []string{"alert"},
		OnCritical:  []string{"enableCache", "routeToCheaperModel"},
		OnHardLimit: []string{"blockOrRequireApproval"},
	}
	cases := []struct {
		spend     float64
		wantPhase string
		wantPct   int32
		wantActs  int
	}{
		{100, PhaseWithinBudget, 20, 0},
		{350, PhaseWarning, 70, 1},
		{460, PhaseCritical, 92, 2},
		{520, PhaseExceeded, 104, 1},
	}
	for _, c := range cases {
		r := Evaluate(c.spend, 500, th, act)
		if r.Phase != c.wantPhase || r.UsagePercent != c.wantPct || len(r.TriggeredActions) != c.wantActs {
			t.Errorf("spend %.0f: got phase=%s pct=%d acts=%d; want %s/%d/%d",
				c.spend, r.Phase, r.UsagePercent, len(r.TriggeredActions), c.wantPhase, c.wantPct, c.wantActs)
		}
	}
}

func TestEvaluateZeroBudget(t *testing.T) {
	r := Evaluate(100, 0, Thresholds{}, Actions{})
	if r.Phase != PhaseUnknown {
		t.Errorf("phase = %s, want Unknown", r.Phase)
	}
}
