package controller

import (
	"math"
	"testing"
)

func TestMoneyQuantityKeepsSubCentSpend(t *testing.T) {
	q := moneyQuantity(0.00456547)
	if q.IsZero() {
		t.Fatalf("moneyQuantity rounded a real sub-cent spend to zero")
	}
	if got := q.AsApproximateFloat64(); math.Abs(got-0.004565) > 0.000001 {
		t.Fatalf("moneyQuantity = %v (%f), want about 0.004565", q.String(), got)
	}
}
