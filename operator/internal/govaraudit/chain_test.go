package govaraudit

import (
	"strings"
	"testing"
	"time"
)

const digestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const digestB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestBuildAndVerifyTransitionChain(t *testing.T) {
	first, err := BuildEntry(Entry{TenantID: "tenant", RequestID: "synthetic-request", Sequence: 1, EventID: "reserve-1", EventKind: "RESERVE", PayloadSHA256: digestA, ActorClass: ActorAdmission, Reason: "reserved", AfterStateSHA256: digestA, PolicyVersion: "policy-v1", PricingSnapshotSHA256: digestB, CommittedAt: time.Unix(100, 1).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildEntry(Entry{TenantID: first.TenantID, RequestID: first.RequestID, Sequence: 2, EventID: "dispatch-1", EventKind: "DISPATCH", PayloadSHA256: digestA, ActorClass: ActorGateway, Reason: "dispatched", BeforeStateSHA256: digestA, AfterStateSHA256: digestB, PolicyVersion: "policy-v1", PricingSnapshotSHA256: digestB, PreviousEntrySHA256: first.EntrySHA256, CommittedAt: time.Unix(101, 2).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify([]Entry{first, second}); err != nil {
		t.Fatal(err)
	}
	second.EventKind = "TAMPERED"
	if err := Verify([]Entry{first, second}); err == nil || (!strings.Contains(err.Error(), "hash mismatch") && !strings.Contains(err.Error(), "closed bounded")) {
		t.Fatalf("tampering was not rejected: %v", err)
	}
}

func TestVerifyRejectsReorderingAndBrokenPredecessor(t *testing.T) {
	entry, err := BuildEntry(Entry{TenantID: "tenant", RequestID: "synthetic", Sequence: 2, EventID: "event", EventKind: "SETTLE", PayloadSHA256: digestA, ActorClass: ActorGateway, Reason: "settled", AfterStateSHA256: digestA, PolicyVersion: "policy", PricingSnapshotSHA256: digestB, CommittedAt: time.Unix(1, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify([]Entry{entry}); err == nil || !strings.Contains(err.Error(), "contiguous") {
		t.Fatalf("reordered chain was not rejected: %v", err)
	}
}

func TestBuildRejectsUnknownActorAndMalformedDigest(t *testing.T) {
	_, err := BuildEntry(Entry{TenantID: "tenant", RequestID: "synthetic", Sequence: 1, EventID: "event", EventKind: "RESERVE", PayloadSHA256: digestA, ActorClass: "CLIENT", Reason: "reserved", AfterStateSHA256: "not-a-hash", PricingSnapshotSHA256: digestB, CommittedAt: time.Unix(1, 0).UTC()})
	if err == nil {
		t.Fatal("invalid actor and digest were accepted")
	}
}

func TestBuildRejectsUnknownEventKind(t *testing.T) {
	_, err := BuildEntry(Entry{TenantID: "tenant", RequestID: "synthetic", Sequence: 1, EventID: "event", EventKind: "UNKNOWN", PayloadSHA256: digestA, ActorClass: ActorAdmission, Reason: "reserved", AfterStateSHA256: digestA, PricingSnapshotSHA256: digestB, CommittedAt: time.Unix(1, 0).UTC()})
	if err == nil {
		t.Fatal("unknown event kind was accepted")
	}
}
