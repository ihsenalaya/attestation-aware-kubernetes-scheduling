package sidecarproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestProxyInjectsHeadersViaHTTPProxy(t *testing.T) {
	got := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Clone()
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	proxy := httptest.NewServer(New(Config{
		Namespace:   "finance",
		Application: "risk-assistant",
	}))
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}

	req, err := http.NewRequest(http.MethodGet, upstream.URL+"/v1/chat/completions", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("x-original", "keep-me")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client do: %v", err)
	}
	defer resp.Body.Close()

	headers := <-got
	if headers.Get(HeaderNamespace) != "finance" {
		t.Fatalf("%s = %q, want finance", HeaderNamespace, headers.Get(HeaderNamespace))
	}
	if headers.Get(HeaderApp) != "risk-assistant" {
		t.Fatalf("%s = %q, want risk-assistant", HeaderApp, headers.Get(HeaderApp))
	}
	if headers.Get("x-original") != "keep-me" {
		t.Fatalf("x-original = %q, want keep-me", headers.Get("x-original"))
	}
}

func TestProxyCallsGOVARAdmitAndSettle(t *testing.T) {
	var gotAdmit map[string]any
	var gotSettle map[string]any
	var dispatchStatuses []string
	govar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/admit":
			_ = json.NewDecoder(r.Body).Decode(&gotAdmit)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"decision":            "ADMIT",
				"reason_code":         "highest_utility_feasible",
				"selected_deployment": "gpt-fr",
				"provider_attempt_id": "attempt-1",
			})
		case "/v1/dispatch":
			var payload map[string]any
			_ = json.NewDecoder(r.Body).Decode(&payload)
			dispatchStatuses = append(dispatchStatuses, payload["status"].(string))
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/settle":
			_ = json.NewDecoder(r.Body).Decode(&gotSettle)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer govar.Close()

	upstreamCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		if got := r.Header.Get("x-ai-eg-model"); got != "gpt-fr" {
			t.Fatalf("x-ai-eg-model = %q, want gpt-fr", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"usage": map[string]any{
				"prompt_tokens":     10,
				"completion_tokens": 20,
			},
		})
	}))
	defer upstream.Close()

	proxy := httptest.NewServer(New(Config{
		GOVAREnabled:      true,
		Namespace:         "finance",
		Application:       "risk-assistant",
		TenantID:          "tenant-finance",
		WorkloadUID:       "pod-uid-finance",
		BearerToken:       "projected-bound-token",
		BudgetPolicyName:  "budget-finance",
		RoutingPolicyName: "routing-finance",
		GOVAREndpoint:     govar.URL,
	}))
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	reqBody := `{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hello"}],"max_tokens":64}`
	req, err := http.NewRequest(http.MethodPost, upstream.URL+"/v1/chat/completions", strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want native-gateway-required 421", resp.StatusCode)
	}
	if upstreamCalled || len(gotAdmit) != 0 || len(gotSettle) != 0 || len(dispatchStatuses) != 0 {
		t.Fatalf("admission-only sidecar performed authoritative/direct work: upstream=%v admit=%v settle=%v dispatch=%v", upstreamCalled, gotAdmit, gotSettle, dispatchStatuses)
	}
}

func TestProxyCancelsOnUpstreamFailure(t *testing.T) {
	cancelCalled := false
	var dispatchStatuses []string
	govar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/admit":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"decision":            "ADMIT",
				"reason_code":         "highest_utility_feasible",
				"selected_deployment": "gpt-fr",
				"provider_attempt_id": "attempt-1",
			})
		case "/v1/dispatch":
			var payload map[string]any
			_ = json.NewDecoder(r.Body).Decode(&payload)
			dispatchStatuses = append(dispatchStatuses, payload["status"].(string))
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/cancel":
			cancelCalled = true
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/settle":
			t.Fatal("settle should not be called on upstream failure")
		default:
			http.NotFound(w, r)
		}
	}))
	defer govar.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend failure", http.StatusBadGateway)
	}))
	defer upstream.Close()

	proxy := httptest.NewServer(New(Config{
		GOVAREnabled:      true,
		Namespace:         "finance",
		Application:       "risk-assistant",
		TenantID:          "tenant-finance",
		WorkloadUID:       "pod-uid-finance",
		BearerToken:       "projected-bound-token",
		BudgetPolicyName:  "budget-finance",
		RoutingPolicyName: "routing-finance",
		GOVAREndpoint:     govar.URL,
	}))
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	req, err := http.NewRequest(http.MethodPost, upstream.URL+"/v1/chat/completions", strings.NewReader(`{"max_tokens":32}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest || cancelCalled || len(dispatchStatuses) != 0 {
		t.Fatalf("status=%d cancel=%v dispatch=%v", resp.StatusCode, cancelCalled, dispatchStatuses)
	}
}

func TestProxyHonorsTargetFilter(t *testing.T) {
	got := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}

	proxy := httptest.NewServer(New(Config{
		Namespace:   "legal",
		Application: "contract-review",
		Targets:     []string{"not-" + upstreamURL.Hostname()},
	}))
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}

	resp, err := client.Get(upstream.URL)
	if err != nil {
		t.Fatalf("client get: %v", err)
	}
	defer resp.Body.Close()

	headers := <-got
	if headers.Get(HeaderNamespace) != "" {
		t.Fatalf("%s = %q, want empty", HeaderNamespace, headers.Get(HeaderNamespace))
	}
	if headers.Get(HeaderApp) != "" {
		t.Fatalf("%s = %q, want empty", HeaderApp, headers.Get(HeaderApp))
	}
}

func TestGovernedCONNECTFailsClosedEvenWhenTargetFilterExcludesHost(t *testing.T) {
	handler := New(Config{GOVAREnabled: true, Targets: []string{"different.example.test"}, GOVAREndpoint: "http://govar.invalid"})
	req := httptest.NewRequest(http.MethodConnect, "http://api.example.test:443", nil)
	req.Host = "api.example.test:443"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), "native synchronous gateway") {
		t.Fatalf("CONNECT response=%d %s", recorder.Code, recorder.Body.String())
	}
}

func TestEnabledIncompleteConfigFailsClosedForAllGovernedShapes(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/embeddings", "/v1/rerank"} {
		handler := New(Config{GOVAREnabled: true, Targets: []string{"different.example.test"}})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "http://provider.example"+path, strings.NewReader(`{}`)))
		if recorder.Code != http.StatusMisdirectedRequest {
			t.Fatalf("path=%s status=%d", path, recorder.Code)
		}
	}
}

func TestEnabledModeIgnoresPodTargetExclusionForLLMAdmission(t *testing.T) {
	admitCalled := false
	govar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/admit" {
			admitCalled = true
			_ = json.NewEncoder(w).Encode(map[string]any{"decision": "ADMIT", "reason_code": "duplicate_request"})
			return
		}
		http.NotFound(w, r)
	}))
	defer govar.Close()
	cfg := readyGOVARConfig(govar.URL)
	cfg.Targets = []string{"different.example.test"}
	handler := New(cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "http://provider.example/v1/chat/completions", strings.NewReader(`{"max_tokens":8}`)))
	if admitCalled || recorder.Code != http.StatusMisdirectedRequest {
		t.Fatalf("admit=%v status=%d", admitCalled, recorder.Code)
	}
}

func TestParseUsageRequiresPresentValidUsage(t *testing.T) {
	invalid := [][]byte{nil, []byte(`{}`), []byte(`{"usage":null}`), []byte(`{"usage":{}}`), []byte(`{"usage":{"prompt_tokens":1,"completion_tokens":-1}}`), []byte(`not-json`)}
	for _, body := range invalid {
		if _, ok := parseUsage(body); ok {
			t.Fatalf("invalid usage accepted: %s", body)
		}
	}
	got, ok := parseUsage([]byte(`{"usage":{"input_tokens":0,"output_tokens":0}}`))
	if !ok || got.PromptTokens != 0 || got.CompletionTokens != 0 {
		t.Fatalf("valid zero usage rejected: %+v %v", got, ok)
	}
}

func TestDuplicateAdmissionStopsBeforeProviderDispatch(t *testing.T) {
	providerCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { providerCalled = true }))
	defer upstream.Close()
	govar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/admit" {
			t.Fatalf("unexpected GOV-AR call after duplicate admission: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"decision": "ADMIT", "reason_code": "duplicate_request", "provider_attempt_id": "attempt-1"})
	}))
	defer govar.Close()
	handler := New(readyGOVARConfig(govar.URL))
	req := httptest.NewRequest(http.MethodPost, upstream.URL+"/v1/chat/completions", strings.NewReader(`{"max_tokens":8}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusMisdirectedRequest || providerCalled {
		t.Fatalf("duplicate response=%d providerCalled=%v", recorder.Code, providerCalled)
	}
}

func TestDuplicateClaimIsNotProviderAuthorization(t *testing.T) {
	govar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"reason_code": "duplicate_event"})
	}))
	defer govar.Close()
	p := New(readyGOVARConfig(govar.URL)).(*proxy)
	if err := p.callDispatch(context.Background(), "r1", "a1", "claim", "CLAIMED"); err == nil {
		t.Fatal("duplicate claim was treated as provider authorization")
	}
}

func TestMissingUsageMarksDeliveredRequestUnresolvedWithoutSettle(t *testing.T) {
	cancelCalled, settleCalled := false, false
	govar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/admit":
			_ = json.NewEncoder(w).Encode(map[string]any{"decision": "ADMIT", "reason_code": "highest_utility_feasible", "provider_attempt_id": "attempt-1"})
		case "/v1/dispatch":
			_ = json.NewEncoder(w).Encode(map[string]any{"reason_code": "dispatch_delivered"})
		case "/v1/cancel":
			cancelCalled = true
			_ = json.NewEncoder(w).Encode(map[string]any{"reason_code": "cancellation_delivery_ambiguous"})
		case "/v1/settle":
			settleCalled = true
		}
	}))
	defer govar.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{}`) }))
	defer upstream.Close()
	handler := New(readyGOVARConfig(govar.URL))
	req := httptest.NewRequest(http.MethodPost, upstream.URL+"/v1/chat/completions", strings.NewReader(`{"max_tokens":8}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusMisdirectedRequest || cancelCalled || settleCalled {
		t.Fatalf("missing usage cancel=%v settle=%v", cancelCalled, settleCalled)
	}
}

func TestGovernedSidecarRejectsStreamingAndOversizeBodies(t *testing.T) {
	handler := New(readyGOVARConfig("http://gov-ar.invalid"))
	stream := httptest.NewRecorder()
	handler.ServeHTTP(stream, httptest.NewRequest(http.MethodPost, "http://provider.example/v1/messages", strings.NewReader(`{"stream":true}`)))
	if stream.Code != http.StatusBadRequest || !strings.Contains(stream.Body.String(), "stream=true") {
		t.Fatalf("stream response=%d %s", stream.Code, stream.Body.String())
	}
	oversize := httptest.NewRecorder()
	handler.ServeHTTP(oversize, httptest.NewRequest(http.MethodPost, "http://provider.example/v1/chat/completions", bytes.NewReader(bytes.Repeat([]byte("x"), int(maxProxyBodyBytes)+1))))
	if oversize.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize response=%d", oversize.Code)
	}
	unknown := httptest.NewRecorder()
	handler.ServeHTTP(unknown, httptest.NewRequest(http.MethodPost, "http://provider.example/custom/generate", strings.NewReader(`{}`)))
	if unknown.Code != http.StatusForbidden {
		t.Fatalf("unknown shape response=%d", unknown.Code)
	}
}

func readyGOVARConfig(endpoint string) Config {
	return Config{GOVAREnabled: true, Namespace: "finance", Application: "app", GOVAREndpoint: endpoint,
		TenantID: "tenant-a", WorkloadUID: "uid-a", BudgetPolicyName: "budget", RoutingPolicyName: "routing",
		BearerToken: "projected-bound-token"}
}
