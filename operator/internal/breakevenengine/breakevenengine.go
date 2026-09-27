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

// Package breakevenengine compares the monthly cost of a managed API model against
// a self-hosted GPU alternative and computes the break-even (payback) point. Pure,
// no Kubernetes dependency.
package breakevenengine

import (
	"fmt"
	"math"
)

// Recommendation values mirror v1alpha1.BreakEvenRecommendation.
const (
	RecKeepManaged = "keep-managed"
	RecInvestigate = "investigate"
	RecSelfHost    = "self-host"
)

// DefaultPaybackThresholdMonths is the cutoff under which self-hosting is advised.
const DefaultPaybackThresholdMonths = 6.0

// Inputs are the monthly cost components (all in the same currency).
type Inputs struct {
	ManagedTokenCostMonthly float64
	ProviderFixedMonthly    float64
	GpuMonthly              float64
	OpsMonthly              float64
	StorageNetworkMonthly   float64
	MigrationCost           float64
}

// Result is the break-even analysis outcome.
type Result struct {
	ManagedMonthly    float64
	SelfHostedMonthly float64
	MonthlySavings    float64
	PaybackMonths     float64
	HasPayback        bool
	Recommendation    string
	Message           string
}

// Analyze applies the MVP break-even model. paybackThreshold<=0 falls back to the
// default cutoff.
func Analyze(in Inputs, paybackThreshold float64) Result {
	if paybackThreshold <= 0 {
		paybackThreshold = DefaultPaybackThresholdMonths
	}

	managed := in.ManagedTokenCostMonthly + in.ProviderFixedMonthly
	self := in.GpuMonthly + in.OpsMonthly + in.StorageNetworkMonthly
	savings := managed - self

	res := Result{ManagedMonthly: managed, SelfHostedMonthly: self, MonthlySavings: savings}

	if savings <= 0 {
		res.Recommendation = RecKeepManaged
		res.Message = fmt.Sprintf("Self-hosting costs %.2f/mo vs managed %.2f/mo — no monthly saving; keep managed.", self, managed)
		return res
	}

	res.HasPayback = true
	if in.MigrationCost <= 0 {
		res.PaybackMonths = 0
	} else {
		res.PaybackMonths = math.Round(in.MigrationCost/savings*10) / 10 // 1 decimal
	}

	if res.PaybackMonths <= paybackThreshold {
		res.Recommendation = RecSelfHost
		res.Message = fmt.Sprintf("Self-hosting saves %.2f/mo; migration pays back in %.1f months — recommend self-host.", savings, res.PaybackMonths)
	} else {
		res.Recommendation = RecInvestigate
		res.Message = fmt.Sprintf("Self-hosting saves %.2f/mo but payback is %.1f months (> %.0f) — investigate.", savings, res.PaybackMonths, paybackThreshold)
	}
	return res
}

// ExtrapolateMonthly scales a cost observed over windowDays to a 30-day month.
func ExtrapolateMonthly(windowCost float64, windowDays int32) float64 {
	if windowDays <= 0 {
		return windowCost
	}
	return windowCost * 30.0 / float64(windowDays)
}
