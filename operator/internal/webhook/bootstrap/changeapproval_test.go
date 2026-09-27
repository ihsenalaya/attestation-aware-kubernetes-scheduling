package bootstrap

import (
	"context"
	"testing"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestApprovalWebhooksAreRegisteredFailClosedForRootResource(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := admissionregv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	k8sClient := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	dir := t.TempDir()
	ctx := context.Background()
	if err := Ensure(ctx, Options{Client: k8sClient, Name: "mutating", ServiceName: "webhook", ServiceNamespace: "operator", CertDir: dir}); err != nil {
		t.Fatal(err)
	}
	if err := EnsureValidation(ctx, Options{Client: k8sClient, Name: "validating", ServiceName: "webhook", ServiceNamespace: "operator", CertDir: dir}); err != nil {
		t.Fatal(err)
	}

	var mutating admissionregv1.MutatingWebhookConfiguration
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: "mutating"}, &mutating); err != nil {
		t.Fatal(err)
	}
	if len(mutating.Webhooks) != 2 {
		t.Fatalf("mutating webhook count=%d, want pod plus approval", len(mutating.Webhooks))
	}
	assertApprovalRule(t, mutating.Webhooks[1].Name, mutating.Webhooks[1].FailurePolicy, mutating.Webhooks[1].Rules, mutating.Webhooks[1].NamespaceSelector)

	var validating admissionregv1.ValidatingWebhookConfiguration
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: "validating"}, &validating); err != nil {
		t.Fatal(err)
	}
	if len(validating.Webhooks) != 2 {
		t.Fatalf("validating webhook count=%d, want pod plus approval", len(validating.Webhooks))
	}
	assertApprovalRule(t, validating.Webhooks[1].Name, validating.Webhooks[1].FailurePolicy, validating.Webhooks[1].Rules, validating.Webhooks[1].NamespaceSelector)
}

func assertApprovalRule(t *testing.T, name string, failure *admissionregv1.FailurePolicyType, rules []admissionregv1.RuleWithOperations, selector *metav1.LabelSelector) {
	t.Helper()
	if failure == nil || *failure != admissionregv1.Fail {
		t.Fatalf("%s failurePolicy=%v, want Fail", name, failure)
	}
	if selector != nil {
		t.Fatalf("%s unexpectedly excludes namespaces: %+v", name, selector)
	}
	if len(rules) != 1 || len(rules[0].APIGroups) != 1 || rules[0].APIGroups[0] != "aiops.imperium.io" ||
		len(rules[0].APIVersions) != 1 || rules[0].APIVersions[0] != "v1alpha1" ||
		len(rules[0].Resources) != 1 || rules[0].Resources[0] != "aichangerequests" {
		t.Fatalf("%s rule not scoped exactly to AIChangeRequest root resource: %+v", name, rules)
	}
	wantOps := map[admissionregv1.OperationType]bool{admissionregv1.Create: true, admissionregv1.Update: true}
	for _, op := range rules[0].Operations {
		delete(wantOps, op)
	}
	if len(wantOps) != 0 || len(rules[0].Operations) != 2 {
		t.Fatalf("%s operations=%v, want create/update", name, rules[0].Operations)
	}
}
