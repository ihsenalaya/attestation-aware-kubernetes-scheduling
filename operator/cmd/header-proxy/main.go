package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/imperium/ai-sovereign-finops-operator/internal/sidecarproxy"
)

func main() {
	var listen string
	flag.StringVar(&listen, "listen", ":15088", "listen address for the sidecar HTTP proxy")
	flag.Parse()

	handler := sidecarproxy.New(sidecarproxy.Config{
		GOVAREnabled:      strings.EqualFold(strings.TrimSpace(getenv("GOVAR_ENABLED", "false")), "true"),
		Namespace:         strings.TrimSpace(getenv("GREENOPS_NAMESPACE", "")),
		Application:       strings.TrimSpace(getenv("GREENOPS_APPLICATION", "")),
		Targets:           splitCSV(getenv("GREENOPS_TARGET_HOSTS", "")),
		GOVAREndpoint:     strings.TrimSpace(getenv("GOVAR_ENDPOINT", "")),
		TenantID:          strings.TrimSpace(getenv("GOVAR_TENANT_ID", "")),
		WorkloadUID:       strings.TrimSpace(getenv("GOVAR_WORKLOAD_UID", "")),
		TokenFile:         getenv("GOVAR_TOKEN_FILE", "/var/run/secrets/govar/token"),
		BudgetPolicyName:  strings.TrimSpace(getenv("GOVAR_BUDGET_POLICY", "")),
		RoutingPolicyName: strings.TrimSpace(getenv("GOVAR_ROUTING_POLICY", "")),
		AllowedZones:      splitCSV(getenv("GOVAR_ALLOWED_ZONES", "")),
		SensitiveData:     strings.EqualFold(strings.TrimSpace(getenv("GOVAR_SENSITIVE_DATA", "false")), "true"),
	})

	log.Printf("starting greenops header proxy on %s", listen)
	if err := http.ListenAndServe(listen, handler); err != nil {
		log.Fatal(err)
	}
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func splitCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
