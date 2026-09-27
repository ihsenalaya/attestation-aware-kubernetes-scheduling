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

package controller

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
)

func TestStubName(t *testing.T) {
	cases := map[string]string{
		"gpt-4o":               "auto-gpt-4o",
		"GPT-4o":               "auto-gpt-4o",
		"anthropic/claude-3-5": "auto-anthropic-claude-3-5",
		"  ":                   "auto-model",
	}
	for in, want := range cases {
		if got := stubName(in); got != want {
			t.Errorf("stubName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestUnknownModels(t *testing.T) {
	userModel := aiopsv1alpha1.AIModel{}
	userModel.Spec.ModelName = "my-private-llm"
	cat := catalog{models: []aiopsv1alpha1.AIModel{userModel}}

	got := cat.unknownModels([]collectors.UsageSample{
		{Model: "gpt-4o"},         // known default → skip
		{Model: "my-private-llm"}, // user CR → skip
		{Model: "mystery-1"},      // unknown → keep
		{Model: "mystery-1"},      // duplicate → once
		{Model: ""},               // empty → skip
	})
	if len(got) != 1 || got[0] != "mystery-1" {
		t.Fatalf("unknownModels = %v, want [mystery-1]", got)
	}
}

func TestEnsureModelStubs(t *testing.T) {
	sch := runtime.NewScheme()
	if err := aiopsv1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	c := fakeclient.NewClientBuilder().WithScheme(sch).Build()
	ctx := context.Background()

	if err := ensureModelStubs(ctx, c, "default", []string{"mystery-1"}); err != nil {
		t.Fatalf("ensureModelStubs: %v", err)
	}
	var got aiopsv1alpha1.AIModel
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "auto-mystery-1"}, &got); err != nil {
		t.Fatalf("stub not created: %v", err)
	}
	if got.Spec.ModelName != "mystery-1" || got.Spec.ProviderRef != "unknown" {
		t.Errorf("stub spec wrong: %+v", got.Spec)
	}
	if got.Labels[autoStubLabel] != "true" {
		t.Errorf("stub missing auto-stub label: %v", got.Labels)
	}

	// Idempotent: a second call neither errors nor duplicates.
	if err := ensureModelStubs(ctx, c, "default", []string{"mystery-1"}); err != nil {
		t.Fatalf("second ensureModelStubs: %v", err)
	}
	var list aiopsv1alpha1.AIModelList
	if err := c.List(ctx, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Errorf("expected 1 stub, got %d", len(list.Items))
	}
}
