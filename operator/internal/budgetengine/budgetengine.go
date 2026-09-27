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

// Package budgetengine compares spend against a budget and derives a phase plus
// recommended (non-enforcing) actions. It is pure (no Kubernetes dependency).
package budgetengine

import (
	"fmt"
	"math"
)

// Phase values mirror AIBudgetPolicyStatus.Phase.
const (
	PhaseUnknown      = "Unknown"
	PhaseWithinBudget = "WithinBudget"
	PhaseWarning      = "Warning"
	PhaseCritical     = "Critical"
	PhaseExceeded     = "Exceeded"
)

// Thresholds are the percent-of-budget trip points.
type Thresholds struct {
	WarningPct   int32
	CriticalPct  int32
	HardLimitPct int32
}

// Actions are the recommended actions per tier (recommendation-only in the MVP).
type Actions struct {
	OnWarning   []string
	OnCritical  []string
	OnHardLimit []string
}

// Result is the budget evaluation outcome.
type Result struct {
	Phase            string
	UsagePercent     int32
	SpendEUR         float64
	BudgetEUR        float64
	TriggeredActions []string
	Message          string
}

// Evaluate compares spend to budget and returns the phase, usage percent and the
// recommended actions for the highest tripped tier.
func Evaluate(spend, budget float64, t Thresholds, a Actions) Result {
	if budget <= 0 {
		return Result{
			Phase: PhaseUnknown, SpendEUR: spend, BudgetEUR: budget,
			Message: "budget not set or non-positive; cannot evaluate usage",
		}
	}

	pctF := spend / budget * 100
	pct := int32(math.Round(pctF))

	res := Result{UsagePercent: pct, SpendEUR: spend, BudgetEUR: budget}
	switch {
	case pct >= t.HardLimitPct:
		res.Phase = PhaseExceeded
		res.TriggeredActions = a.OnHardLimit
	case pct >= t.CriticalPct:
		res.Phase = PhaseCritical
		res.TriggeredActions = a.OnCritical
	case pct >= t.WarningPct:
		res.Phase = PhaseWarning
		res.TriggeredActions = a.OnWarning
	default:
		res.Phase = PhaseWithinBudget
	}
	res.Message = fmt.Sprintf("%.2f / %.2f EUR consumed (%d%%) — %s", spend, budget, pct, res.Phase)
	return res
}
