package govarexperiment

import (
	"context"
	"strings"
	"testing"
)

func developmentAccessIssuer(t *testing.T) (*DevelopmentFixtureIssuer, DispatchOutcomeRequest, EffectiveDispatchProof) {
	t.Helper()
	model := strings.Repeat("a", 64)
	evidence := FeedbackEvidence{RunID: "access-run", DatasetID: "synthetic_p0_outcomes", Split: "development",
		ProtocolSHA256: strings.Repeat("b", 64), FeedbackArtifactSHA256: strings.Repeat("c", 64),
		SoftwareSHA256: strings.Repeat("d", 64), ConfigSHA256: strings.Repeat("e", 64),
		AuthoritySHA256: strings.Repeat("f", 64), ModelMapSHA256: strings.Repeat("1", 64)}
	raw := []byte(`{"schema_version":"govar-development-outcome-v1","record_type":"development_outcome","request_id":"request-1","feedback_item_id":"` + strings.Repeat("2", 64) + `","selected_model_id":"` + model + `","actual_input_tokens":12,"actual_output_tokens":34,"selected_score":0.75}` + "\n")
	issuer, err := ParseDevelopmentFixtureOutcomes(raw, evidence)
	if err != nil {
		t.Fatal(err)
	}
	req := DispatchOutcomeRequest{DispatchID: strings.Repeat("3", 64), RunID: evidence.RunID, RequestID: "request-1",
		FeedbackItemID: strings.Repeat("2", 64), SelectedModelID: model, SelectedDeployment: "experiment-model",
		ProviderAttemptID: "request-1:attempt:1", ConfigSHA256: evidence.ConfigSHA256}
	proof := EffectiveDispatchProof{LifecycleEventIndex: 7, DeliveryEventID: "evt-delivered-1",
		RouteSnapshotSHA256: strings.Repeat("4", 64), State: "DISPATCHED", TransitionEffective: true}
	return issuer, req, proof
}

func TestDevelopmentIssuerRequiresEffectiveAuthorizationAndLogsReplay(t *testing.T) {
	issuer, req, proof := developmentAccessIssuer(t)
	if _, err := issuer.IssueDispatchedOutcome(context.Background(), req); err == nil || !strings.Contains(err.Error(), "before exact effective dispatch") {
		t.Fatalf("pre-dispatch outcome access did not fail closed: %v", err)
	}
	if len(issuer.FeedbackAccessLog()) != 0 {
		t.Fatal("rejected pre-dispatch access was recorded as an effective access")
	}
	if err := issuer.AuthorizeDispatchedOutcome(req, proof); err != nil {
		t.Fatal(err)
	}
	first, err := issuer.IssueDispatchedOutcome(context.Background(), req)
	if err != nil || first.Feedback.Replayed {
		t.Fatalf("first authorized issue failed or claimed replay: %v %+v", err, first.Feedback)
	}
	beforeVerify := len(issuer.FeedbackAccessLog())
	if err := issuer.VerifyDispatchedOutcome(req, first); err != nil {
		t.Fatal(err)
	}
	if len(issuer.FeedbackAccessLog()) != beforeVerify {
		t.Fatal("pure outcome verification generated an evaluator access")
	}
	replay, err := issuer.IssueDispatchedOutcome(context.Background(), req)
	if err != nil || !replay.Feedback.Replayed || replay.ActualOutputTokens != first.ActualOutputTokens {
		t.Fatalf("exact evaluator replay was not idempotent: %v %+v", err, replay)
	}
	events := issuer.FeedbackAccessLog()
	if len(events) != 3 || events[0].EventType != "authorize" || events[1].EventType != "issue" ||
		events[2].EventType != "issue_replay" || !events[2].Replayed || events[2].Effective {
		t.Fatalf("unexpected access event sequence: %+v", events)
	}
}

func TestDevelopmentIssuerRejectsConflictingAuthorization(t *testing.T) {
	issuer, req, proof := developmentAccessIssuer(t)
	if err := issuer.AuthorizeDispatchedOutcome(req, proof); err != nil {
		t.Fatal(err)
	}
	conflict := req
	conflict.RequestID = "request-2"
	if err := issuer.AuthorizeDispatchedOutcome(conflict, proof); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("conflicting dispatch authorization did not fail closed: %v", err)
	}
	if got := len(issuer.FeedbackAccessLog()); got != 1 {
		t.Fatalf("rejected conflict changed the effective access log: %d", got)
	}
	if err := issuer.AuthorizeDispatchedOutcome(req, proof); err != nil {
		t.Fatal(err)
	}
	events := issuer.FeedbackAccessLog()
	if len(events) != 2 || events[1].EventType != "authorization_replay" || !events[1].Replayed || events[1].Effective {
		t.Fatalf("exact authorization replay was not recorded idempotently: %+v", events)
	}
}
