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

package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Run e2e tests using the Ginkgo runner.
func TestE2E(t *testing.T) {
	if os.Getenv("ARTICLE3_RUN_KIND_E2E") != "1" {
		t.Skip("Kind E2E requires the explicit Article 3 isolated-cluster gate")
	}
	cluster := os.Getenv("KIND_CLUSTER")
	if !strings.HasPrefix(cluster, "article3-") || os.Getenv("KUBECONFIG") == "" {
		t.Fatal("Kind E2E requires an exact article3-* cluster and isolated KUBECONFIG")
	}
	RegisterFailHandler(Fail)
	fmt.Fprintf(GinkgoWriter, "Starting ai-sovereign-finops-operator suite\n")
	RunSpecs(t, "e2e suite")
}
