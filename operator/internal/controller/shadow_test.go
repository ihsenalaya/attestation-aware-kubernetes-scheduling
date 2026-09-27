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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReadObservedEgress(t *testing.T) {
	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// No ConfigMap → nil, no error (shadow detection is opt-in).
	c := fakeclient.NewClientBuilder().WithScheme(sch).Build()
	got, err := readObservedEgress(ctx, c, "default")
	if err != nil || got != nil {
		t.Fatalf("absent CM: got %v, err %v; want nil,nil", got, err)
	}

	// Present ConfigMap with real records → parsed.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: shadowEgressConfigMap, Namespace: "default"},
		Data: map[string]string{
			shadowEgressKey: `[{"namespace":"finance","application":"rogue","host":"api.openai.com","connections":7}]`,
		},
	}
	c = fakeclient.NewClientBuilder().WithScheme(sch).WithObjects(cm).Build()
	got, err = readObservedEgress(ctx, c, "default")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(got) != 1 || got[0].Host != "api.openai.com" || got[0].Connections != 7 || got[0].Namespace != "finance" {
		t.Fatalf("parsed egress wrong: %+v", got)
	}
}
