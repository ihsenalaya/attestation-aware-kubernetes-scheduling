package main

import (
	"os"
	"strings"
	"testing"
)

func TestAdmissionServiceAccountCanOnlyReadPolicyLevelApproval(t *testing.T) {
	raw, err := os.ReadFile("../../charts/ai-sovereign-finops-operator/templates/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	roleStart := strings.Index(text, "name: {{ include \"operator.fullname\" . }}-gov-ar-admission\n")
	if roleStart < 0 {
		t.Fatal("admission ClusterRole missing")
	}
	roleEndOffset := strings.Index(text[roleStart:], "kind: ClusterRoleBinding")
	if roleEndOffset < 0 {
		t.Fatal("admission ClusterRoleBinding boundary missing")
	}
	role := text[roleStart : roleStart+roleEndOffset]
	if !strings.Contains(role, "- aichangerequests") || !strings.Contains(role, "verbs: [\"get\", \"list\"]") {
		t.Fatal("admission role lacks read-only AIChangeRequest permission")
	}
	for _, forbidden := range []string{"aiadmissionapprovals", "aiadmissionapprovaldecisions", "coordination.k8s.io", `"update"`, `"patch"`, `"delete"`} {
		if strings.Contains(role, forbidden) {
			t.Fatalf("admission role contains forbidden authority %q", forbidden)
		}
	}
	reviewerRole := "name: {{ include \"operator.fullname\" . }}-gov-ar-approval-reviewer"
	if strings.Count(text, reviewerRole) != 1 {
		t.Fatalf("reviewer role occurrences=%d; expected one unbound role", strings.Count(text, reviewerRole))
	}
	reviewerStart := strings.Index(text, reviewerRole)
	if !strings.Contains(text[reviewerStart:], "resources: [\"aichangerequests\"]") || !strings.Contains(text[reviewerStart:], `"update"`) {
		t.Fatal("unbound reviewer role lacks AIChangeRequest review authority")
	}
}

func TestAdmissionSourceHasNoRequestLevelApprovalObjectOrLeasePath(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, forbidden := range []string{"AIAdmissionApproval", "AdmissionApprovalConsumptionName", "request-level approval", "coordinationv1.Lease", "ensurePendingAdmissionApproval", "validateAndConsumeAdmissionApproval"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("admission source retains obsolete per-request approval path %q", forbidden)
		}
	}
	for _, required := range []string{"resolveGOVARRouteApproval", "validateGOVARRouteApproval", "AIChangeRequestActionAuthorizeGOVARRoute", "ApprovedScopeDigest"} {
		if !strings.Contains(text, required) {
			t.Fatalf("admission source lacks bounded policy-level approval check %q", required)
		}
	}
}
