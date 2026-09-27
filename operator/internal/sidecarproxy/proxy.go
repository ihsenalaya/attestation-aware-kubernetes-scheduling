package sidecarproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	HeaderNamespace         = "x-greenops-namespace"
	HeaderApp               = "x-greenops-app"
	HeaderRequestID         = "x-govar-request-id"
	maxProxyBodyBytes int64 = 1 << 20
)

var errProxyBodyTooLarge = errors.New("proxy body exceeds 1 MiB")

// Config defines how the sidecar proxy enriches outbound HTTP requests.
type Config struct {
	GOVAREnabled      bool
	Namespace         string
	Application       string
	Targets           []string
	Transport         http.RoundTripper
	DialContext       func(context.Context, string, string) (net.Conn, error)
	GOVAREndpoint     string
	TenantID          string
	WorkloadUID       string
	TokenFile         string
	BearerToken       string
	BudgetPolicyName  string
	RoutingPolicyName string
	AllowedZones      []string
	SensitiveData     bool
	HTTPClient        *http.Client
}

// New returns a forward proxy that injects namespace/app headers on matching
// outbound HTTP requests.
func New(cfg Config) http.Handler {
	normalizedTargets := make([]string, 0, len(cfg.Targets))
	for _, t := range cfg.Targets {
		t = strings.ToLower(strings.TrimSpace(t))
		if t != "" {
			normalizedTargets = append(normalizedTargets, t)
		}
	}
	transport := cfg.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	dialContext := cfg.DialContext
	if dialContext == nil {
		dialer := &net.Dialer{Timeout: 30 * time.Second}
		dialContext = dialer.DialContext
	}
	return &proxy{
		govarEnabled:      cfg.GOVAREnabled,
		namespace:         cfg.Namespace,
		application:       cfg.Application,
		targets:           normalizedTargets,
		transport:         transport,
		dialContext:       dialContext,
		govarEndpoint:     strings.TrimRight(strings.TrimSpace(cfg.GOVAREndpoint), "/"),
		tenantID:          strings.TrimSpace(cfg.TenantID),
		workloadUID:       strings.TrimSpace(cfg.WorkloadUID),
		tokenFile:         strings.TrimSpace(cfg.TokenFile),
		bearerToken:       strings.TrimSpace(cfg.BearerToken),
		budgetPolicyName:  strings.TrimSpace(cfg.BudgetPolicyName),
		routingPolicyName: strings.TrimSpace(cfg.RoutingPolicyName),
		allowedZones:      cfg.AllowedZones,
		sensitiveData:     cfg.SensitiveData,
		httpClient:        orDefaultClient(cfg.HTTPClient),
	}
}

type proxy struct {
	govarEnabled      bool
	namespace         string
	application       string
	targets           []string
	transport         http.RoundTripper
	dialContext       func(context.Context, string, string) (net.Conn, error)
	govarEndpoint     string
	tenantID          string
	workloadUID       string
	tokenFile         string
	bearerToken       string
	budgetPolicyName  string
	routingPolicyName string
	allowedZones      []string
	sensitiveData     bool
	httpClient        *http.Client
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		if p.govarEnabled {
			http.Error(w, "governed HTTPS CONNECT is denied: use the native synchronous gateway path for HTTPS LLM traffic", http.StatusForbidden)
			return
		}
		p.handleConnect(w, r)
		return
	}
	requestBody, err := readProxyBody(r.Body)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errProxyBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(requestBody))
	if p.govarEnabled {
		if !isSupportedGovernedShape(r.URL.Path) {
			http.Error(w, "unknown governed outbound shape denied; use a configured native gateway route", http.StatusForbidden)
			return
		}
		var payload map[string]any
		if len(requestBody) != 0 && json.Unmarshal(requestBody, &payload) != nil {
			http.Error(w, "malformed governed request body", http.StatusBadRequest)
			return
		}
		if streaming, _ := payload["stream"].(bool); streaming {
			http.Error(w, "stream=true is unsupported until authoritative streaming settlement is implemented", http.StatusBadRequest)
			return
		}
		http.Error(w, "governed direct-proxy dispatch denied: send the request through the authenticated native Envoy gateway", http.StatusMisdirectedRequest)
		return
	}

	outReq := r.Clone(r.Context())
	outReq.RequestURI = ""
	outReq.URL = cloneURL(r.URL)
	if outReq.URL == nil {
		http.Error(w, "missing request URL", http.StatusBadRequest)
		return
	}
	if outReq.URL.Scheme == "" {
		outReq.URL.Scheme = "http"
	}
	if outReq.URL.Host == "" {
		outReq.URL.Host = r.Host
	}

	outReq.Header = r.Header.Clone()
	outReq.Header.Del("Proxy-Connection")
	outReq.Header.Del("Connection")

	var requestID string
	var providerAttemptID string
	var reserve reservationIntent
	if p.shouldInject(outReq.URL.Hostname()) || (p.govarEnabled && isGOVARRequest(r)) {
		requestID = requestIDFor(r)
		outReq.Header.Set(HeaderRequestID, requestID)
		reserve = p.extractReservationIntent(r)
		if reserve.body != nil {
			outReq.Body = io.NopCloser(bytes.NewReader(reserve.body))
			outReq.ContentLength = int64(len(reserve.body))
		}
		if p.shouldCallGOVAR(r) {
			admitResp, err := p.callAdmit(r.Context(), requestID, reserve)
			if err != nil {
				http.Error(w, fmt.Sprintf("gov-ar admit error: %v", err), http.StatusBadGateway)
				return
			}
			if admitResp.Decision != "ADMIT" {
				http.Error(w, fmt.Sprintf("gov-ar decision=%s reason=%s", admitResp.Decision, admitResp.ReasonCode), httpStatusForDecision(admitResp.Decision))
				return
			}
			if admitResp.ReasonCode == "duplicate_request" {
				http.Error(w, "duplicate active request acknowledged without provider redispatch", http.StatusConflict)
				return
			}
			if admitResp.SelectedDeployment != "" {
				outReq.Header.Set("x-ai-eg-model", admitResp.SelectedDeployment)
			}
			providerAttemptID = admitResp.ProviderAttemptID
			if err := p.callDispatch(r.Context(), requestID, admitResp.ProviderAttemptID, "claim", "CLAIMED"); err != nil {
				_ = p.callCancel(r.Context(), requestID, "dispatch_claim_failed")
				http.Error(w, fmt.Sprintf("gov-ar dispatch claim error: %v", err), http.StatusBadGateway)
				return
			}
		}
	}
	if p.shouldInject(outReq.URL.Hostname()) {
		if p.namespace != "" {
			outReq.Header.Set(HeaderNamespace, p.namespace)
		}
		if p.application != "" {
			outReq.Header.Set(HeaderApp, p.application)
		}
	}

	resp, err := p.transport.RoundTrip(outReq)
	if err != nil {
		if requestID != "" && p.shouldCallGOVAR(r) {
			_ = p.callDispatch(r.Context(), requestID, providerAttemptID, "ambiguous", "AMBIGUOUS")
			_ = p.callCancel(r.Context(), requestID, "upstream_transport_error")
		}
		http.Error(w, fmt.Sprintf("proxy upstream error: %v", err), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if requestID != "" && p.shouldCallGOVAR(r) {
		_ = p.callDispatch(r.Context(), requestID, providerAttemptID, "delivered", "DELIVERED")
	}

	bodyBytes, readErr := readProxyBody(resp.Body)
	if readErr != nil {
		if requestID != "" && p.shouldCallGOVAR(r) {
			_ = p.callCancel(r.Context(), requestID, "upstream_body_read_error")
		}
		http.Error(w, fmt.Sprintf("proxy upstream read error: %v", readErr), http.StatusBadGateway)
		return
	}

	if requestID != "" && p.shouldCallGOVAR(r) {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			usage, valid := parseUsage(bodyBytes)
			if !valid {
				_ = p.callCancel(r.Context(), requestID, "missing_or_invalid_usage")
			} else {
				_ = p.callSettle(r.Context(), requestID, usage, "")
			}
		} else {
			_ = p.callCancel(r.Context(), requestID, "upstream_http_error")
		}
	}

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(bodyBytes)
}

func readProxyBody(body io.Reader) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	value, err := io.ReadAll(io.LimitReader(body, maxProxyBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(value)) > maxProxyBodyBytes {
		return nil, errProxyBodyTooLarge
	}
	return value, nil
}

func (p *proxy) handleConnect(w http.ResponseWriter, r *http.Request) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "proxy does not support hijacking", http.StatusInternalServerError)
		return
	}
	targetConn, err := p.dialContext(r.Context(), "tcp", r.Host)
	if err != nil {
		http.Error(w, fmt.Sprintf("proxy connect error: %v", err), http.StatusBadGateway)
		return
	}

	clientConn, buf, err := hijacker.Hijack()
	if err != nil {
		_ = targetConn.Close()
		http.Error(w, "proxy hijack error", http.StatusInternalServerError)
		return
	}
	defer func() { _ = buf.Flush() }()

	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = clientConn.Close()
		_ = targetConn.Close()
		return
	}

	var once sync.Once
	closeBoth := func() {
		once.Do(func() {
			_ = clientConn.Close()
			_ = targetConn.Close()
		})
	}
	go func() {
		_, _ = io.Copy(targetConn, clientConn)
		closeBoth()
	}()
	go func() {
		_, _ = io.Copy(clientConn, targetConn)
		closeBoth()
	}()
}

func (p *proxy) shouldInject(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	if len(p.targets) == 0 {
		return true
	}
	for _, target := range p.targets {
		if host == target || strings.HasSuffix(host, "."+target) {
			return true
		}
	}
	return false
}

func cloneURL(in *url.URL) *url.URL {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func copyHeader(dst, src http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

type reservationIntent struct {
	InputTokens     int64
	MaxOutputTokens int64
	body            []byte
}

type admitResponse struct {
	Decision           string `json:"decision"`
	ReasonCode         string `json:"reason_code"`
	SelectedDeployment string `json:"selected_deployment"`
	ProviderAttemptID  string `json:"provider_attempt_id"`
}

type usageSummary struct {
	PromptTokens     int64
	CompletionTokens int64
}

func orDefaultClient(c *http.Client) *http.Client {
	if c != nil {
		return c
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func requestIDFor(r *http.Request) string {
	if existing := strings.TrimSpace(r.Header.Get(HeaderRequestID)); existing != "" {
		return existing
	}
	if existing := strings.TrimSpace(r.Header.Get("x-request-id")); existing != "" {
		return existing
	}
	return fmt.Sprintf("govar-%d", time.Now().UTC().UnixNano())
}

func (p *proxy) shouldCallGOVAR(r *http.Request) bool {
	return p.govarEnabled && p.govarReady() && isGOVARRequest(r)
}

func (p *proxy) govarReady() bool {
	return p.govarEndpoint != "" && p.tenantID != "" && p.workloadUID != "" && (p.bearerToken != "" || p.tokenFile != "") && p.budgetPolicyName != "" && p.routingPolicyName != ""
}

func isGOVARRequest(r *http.Request) bool {
	return isSupportedGovernedShape(r.URL.Path)
}

func isSupportedGovernedShape(rawPath string) bool {
	path := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(rawPath), "/"))
	switch path {
	case "/v1/chat/completions", "/v1/responses", "/v1/completions", "/v1/embeddings", "/v1/rerank", "/v1/messages":
		return true
	}
	return strings.HasSuffix(path, "/chat/completions") && strings.Contains(path, "/openai/deployments/") ||
		strings.HasSuffix(path, ":generatecontent")
}

func (p *proxy) extractReservationIntent(r *http.Request) reservationIntent {
	if r.Body == nil {
		return reservationIntent{}
	}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return reservationIntent{}
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	var payload map[string]any
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		return reservationIntent{InputTokens: roughInputTokens(bodyBytes), body: bodyBytes}
	}
	intent := reservationIntent{InputTokens: roughInputTokens(bodyBytes), body: bodyBytes}
	if v, ok := payload["max_tokens"]; ok {
		intent.MaxOutputTokens = anyToInt64(v)
	}
	if intent.MaxOutputTokens == 0 {
		if v, ok := payload["max_output_tokens"]; ok {
			intent.MaxOutputTokens = anyToInt64(v)
		}
	}
	return intent
}

func roughInputTokens(body []byte) int64 {
	if len(body) == 0 {
		return 0
	}
	return int64(len(body) / 4)
}

func anyToInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n
	default:
		return 0
	}
}

func parseUsage(body []byte) (usageSummary, bool) {
	var payload struct {
		Usage *struct {
			PromptTokens     *int64 `json:"prompt_tokens"`
			InputTokens      *int64 `json:"input_tokens"`
			CompletionTokens *int64 `json:"completion_tokens"`
			OutputTokens     *int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Usage == nil {
		return usageSummary{}, false
	}
	prompt := payload.Usage.PromptTokens
	if prompt == nil {
		prompt = payload.Usage.InputTokens
	}
	completion := payload.Usage.CompletionTokens
	if completion == nil {
		completion = payload.Usage.OutputTokens
	}
	if prompt == nil || completion == nil || *prompt < 0 || *completion < 0 {
		return usageSummary{}, false
	}
	return usageSummary{PromptTokens: *prompt, CompletionTokens: *completion}, true
}

func (p *proxy) callAdmit(ctx context.Context, requestID string, intent reservationIntent) (admitResponse, error) {
	payload := map[string]any{
		"request_id":          requestID,
		"namespace":           p.namespace,
		"tenant_id":           p.tenantID,
		"workload_uid":        p.workloadUID,
		"application":         p.application,
		"budget_policy_name":  p.budgetPolicyName,
		"routing_policy_name": p.routingPolicyName,
		"input_tokens":        intent.InputTokens,
		"input_tokens_exact":  false,
		"max_output_tokens":   intent.MaxOutputTokens,
		"sensitive_data":      p.sensitiveData,
		"allowed_zones":       p.allowedZones,
	}
	var resp admitResponse
	if err := p.postJSON(ctx, p.govarEndpoint+"/v1/admit", payload, &resp); err != nil {
		return admitResponse{}, err
	}
	return resp, nil
}

func (p *proxy) callDispatch(ctx context.Context, requestID, attemptID, eventSuffix, status string) error {
	if attemptID == "" {
		attemptID = requestID + ":attempt:1"
	}
	var response struct {
		ReasonCode string `json:"reason_code"`
	}
	if err := p.postJSON(ctx, p.govarEndpoint+"/v1/dispatch", map[string]any{
		"request_id": requestID, "event_id": requestID + ":dispatch:" + eventSuffix,
		"tenant_id": p.tenantID, "workload_uid": p.workloadUID,
		"provider_attempt_id": attemptID, "status": status,
	}, &response); err != nil {
		return err
	}
	if response.ReasonCode == "duplicate_event" || response.ReasonCode == "duplicate_settlement" {
		return errors.New("duplicate dispatch event is not provider authorization")
	}
	return nil
}

func (p *proxy) callSettle(ctx context.Context, requestID string, usage usageSummary, errorStatus string) error {
	return p.postJSON(ctx, p.govarEndpoint+"/v1/settle", map[string]any{
		"request_id":           requestID,
		"settlement_id":        requestID + "-settlement",
		"tenant_id":            p.tenantID,
		"workload_uid":         p.workloadUID,
		"actual_input_tokens":  usage.PromptTokens,
		"actual_output_tokens": usage.CompletionTokens,
		"actual_cost_micros":   0,
		"usage_version":        1,
		"final":                true,
		"error_status":         errorStatus,
	}, nil)
}

func (p *proxy) callCancel(ctx context.Context, requestID, reason string) error {
	return p.postJSON(ctx, p.govarEndpoint+"/v1/cancel", map[string]any{
		"request_id": requestID, "event_id": requestID + ":cancel:" + reason,
		"tenant_id": p.tenantID, "workload_uid": p.workloadUID, "reason": reason,
	}, nil)
}

func (p *proxy) postJSON(ctx context.Context, endpoint string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := p.authenticateRequest(req); err != nil {
		return err
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("gov-ar endpoint %s returned %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (p *proxy) authenticateRequest(req *http.Request) error {
	token := p.bearerToken
	if token == "" {
		raw, err := os.ReadFile(p.tokenFile)
		if err != nil {
			return fmt.Errorf("read projected GOV-AR token: %w", err)
		}
		token = strings.TrimSpace(string(raw))
	}
	if token == "" {
		return errors.New("projected GOV-AR token is empty")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-GOVAR-Tenant-ID", p.tenantID)
	return nil
}

func httpStatusForDecision(decision string) int {
	switch decision {
	case "QUEUE":
		return http.StatusTooManyRequests
	case "REQUIRE_APPROVAL":
		return http.StatusAccepted
	case "ABSTAIN":
		return http.StatusServiceUnavailable
	case "REJECT":
		return http.StatusForbidden
	default:
		return http.StatusForbidden
	}
}
