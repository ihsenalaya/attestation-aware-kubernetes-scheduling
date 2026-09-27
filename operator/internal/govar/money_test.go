package govar

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestQuantityToMicrosIsIntegerAndConservativeForBudget(t *testing.T) {
	got, err := quantityToMicros(resource.MustParse("125.1234569"))
	if err != nil {
		t.Fatal(err)
	}
	if got != 125_123_456 {
		t.Fatalf("micros=%d, want floor 125123456", got)
	}
}

func TestExpectedCostMicrosRoundsReservationUp(t *testing.T) {
	got, err := expectedCostMicros(Candidate{InputPriceMicrosPerMillion: 333_333}, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("micros=%d, want conservative one-micro reservation", got)
	}
}
