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

package metrics

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

// metricRefRe matches every ai_finops_* token used in a Grafana dashboard JSON.
var metricRefRe = regexp.MustCompile(`ai_finops_[a-z_]+`)

// TestDashboardMetricsAreDefined guards against the exact failure we hit once: a
// metric is renamed in the operator (e.g. dropping a _total suffix) but a Grafana
// panel keeps querying the old name, so the panel silently shows no data with no
// error anywhere. This test fails the build/CI if any ai_finops_* metric a shipped
// dashboard references is not actually registered by this package.
func TestDashboardMetricsAreDefined(t *testing.T) {
	defined := map[string]bool{}
	for _, n := range Names() {
		defined[n] = true
	}
	if len(defined) == 0 {
		t.Fatal("metrics.Names() returned no metrics; the registry wiring is broken")
	}

	dashboards, err := filepath.Glob("../../dashboards/*.json")
	if err != nil {
		t.Fatalf("globbing dashboards: %v", err)
	}
	if len(dashboards) == 0 {
		t.Skip("no dashboards/*.json found")
	}

	for _, path := range dashboards {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		missing := map[string]bool{}
		for _, ref := range metricRefRe.FindAllString(string(data), -1) {
			if !defined[ref] {
				missing[ref] = true
			}
		}
		if len(missing) > 0 {
			names := make([]string, 0, len(missing))
			for n := range missing {
				names = append(names, n)
			}
			sort.Strings(names)
			t.Errorf("%s references metric(s) not registered by the operator: %v\n"+
				"either the metric was renamed/removed (update the dashboard) or it was never defined (fix the query).",
				filepath.Base(path), names)
		}
	}
}
