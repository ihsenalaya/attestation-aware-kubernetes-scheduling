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

package breakevenengine

import (
	"math"
	"testing"
)

func TestAnalyzeSelfHost(t *testing.T) {
	// managed 5000/mo, self-hosted 2500/mo, migration 5000 -> savings 2500, payback 2mo.
	r := Analyze(Inputs{ManagedTokenCostMonthly: 5000, GpuMonthly: 1800, OpsMonthly: 700, MigrationCost: 5000}, 6)
	if r.Recommendation != RecSelfHost {
		t.Errorf("rec = %s, want self-host (%+v)", r.Recommendation, r)
	}
	if r.MonthlySavings != 2500 {
		t.Errorf("savings = %.2f, want 2500", r.MonthlySavings)
	}
	if r.PaybackMonths != 2 {
		t.Errorf("payback = %.1f, want 2", r.PaybackMonths)
	}
}

func TestAnalyzeKeepManaged(t *testing.T) {
	// self-hosted more expensive than managed.
	r := Analyze(Inputs{ManagedTokenCostMonthly: 1000, GpuMonthly: 1800, OpsMonthly: 700}, 6)
	if r.Recommendation != RecKeepManaged {
		t.Errorf("rec = %s, want keep-managed", r.Recommendation)
	}
	if r.HasPayback {
		t.Error("should have no payback when savings <= 0")
	}
}

func TestAnalyzeInvestigate(t *testing.T) {
	// savings small vs migration -> long payback.
	r := Analyze(Inputs{ManagedTokenCostMonthly: 2600, GpuMonthly: 1800, OpsMonthly: 700, MigrationCost: 5000}, 6)
	if r.Recommendation != RecInvestigate {
		t.Errorf("rec = %s, want investigate (payback %.1f)", r.Recommendation, r.PaybackMonths)
	}
}

func TestExtrapolateMonthly(t *testing.T) {
	if got := ExtrapolateMonthly(100, 30); got != 100 {
		t.Errorf("30d: got %.2f, want 100", got)
	}
	if got := ExtrapolateMonthly(100, 15); math.Abs(got-200) > 1e-9 {
		t.Errorf("15d: got %.2f, want 200", got)
	}
}
