package govarextproc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
)

// Server is the synchronous Envoy ext_proc trust boundary. Envoy terminates
// downstream TLS and sends buffered request/response bodies on this stream.
type Server struct {
	extprocv3.UnimplementedExternalProcessorServer
	AdmissionURL       string
	MasterSecret       []byte
	HTTPClient         *http.Client
	PrincipalRegistry  map[string]RouteBinding
	ResolvePrincipal   func(context.Context, string) (RouteBinding, error)
	AllowedGatewayURIs map[string]struct{}
	AllowInsecureDev   bool
}

type RouteBinding struct {
	Namespace, TenantID, WorkloadUID, Team, Application string
	BudgetPolicy, RoutingPolicy                         string
	Sensitive                                           bool
	AllowedZones                                        []string
}

type RouteTarget struct {
	SelectedModel      string
	ProviderDeployment string
	Cluster            string
	Authority          string
	PathMode           string
	Path               string
	SnapshotHash       string
}

type streamState struct {
	headers                   map[string]string
	requestBody, responseBody []byte
	requestID, attemptID      string
	binding                   RouteBinding
	route                     RouteTarget
	routeSnapshotHash         string
	traceContext              context.Context
	traceSpan                 trace.Span
	providerSpan              trace.Span
	settlementBases           []aiopsv1alpha1.ProviderBillableBasis
	requestStartedAt          time.Time
}

const maxExtProcBodyBytes = 1 << 20

func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	gatewayURI, err := s.authorizeGatewayTransport(stream.Context())
	if err != nil {
		return err
	}
	state := streamState{headers: map[string]string{}}
	defer func() {
		if state.providerSpan != nil {
			state.providerSpan.End()
		}
		if state.traceSpan != nil {
			state.traceSpan.End()
		}
	}()
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch {
		case request.GetRequestHeaders() != nil:
			if state.traceSpan != nil {
				return errors.New("duplicate ext_proc request headers")
			}
			state.headers = headerMap(request.GetRequestHeaders().GetHeaders())
			parent := otel.GetTextMapPropagator().Extract(stream.Context(), propagation.MapCarrier(state.headers))
			state.traceContext, state.traceSpan = otel.Tracer("github.com/imperium/ai-sovereign-finops-operator/gov-ar-ext-proc").Start(
				parent, "govar.ext_proc.request", trace.WithSpanKind(trace.SpanKindServer))
			binding, err := s.resolvePrincipal(state.traceContext, gatewayURI, state.headers["x-forwarded-client-cert"])
			if err != nil {
				return stream.Send(immediate(http.StatusForbidden, err.Error()))
			}
			state.binding = binding
			if err := stream.Send(headerContinue(true, state.traceContext)); err != nil {
				return err
			}
		case request.GetRequestBody() != nil:
			body := request.GetRequestBody()
			if len(state.requestBody)+len(body.GetBody()) > maxExtProcBodyBytes {
				return stream.Send(immediate(http.StatusRequestEntityTooLarge, "governed request body exceeds 1 MiB"))
			}
			state.requestBody = append(state.requestBody, body.GetBody()...)
			if !body.GetEndOfStream() {
				if err := stream.Send(bodyContinue(true, RouteTarget{}, nil, state.context(stream.Context()))); err != nil {
					return err
				}
				continue
			}
			if err := s.admitAndClaim(state.context(stream.Context()), &state); err != nil {
				return stream.Send(immediate(http.StatusForbidden, err.Error()))
			}
			if err := stream.Send(bodyContinue(true, state.route, state.requestBody, state.context(stream.Context()))); err != nil {
				return err
			}
		case request.GetResponseHeaders() != nil:
			if state.requestID != "" {
				if err := s.dispatch(state.context(stream.Context()), state, "delivered", "DELIVERED"); err != nil {
					return err
				}
			}
			if err := stream.Send(responseHeaderContinue()); err != nil {
				return err
			}
		case request.GetResponseBody() != nil:
			body := request.GetResponseBody()
			if len(state.responseBody)+len(body.GetBody()) > maxExtProcBodyBytes {
				return errors.New("provider response body exceeds 1 MiB; liability remains unresolved")
			}
			state.responseBody = append(state.responseBody, body.GetBody()...)
			if body.GetEndOfStream() && state.requestID != "" {
				usage, validEnvelope := parseUsage(state.responseBody, state.settlementBases, state.route)
				if !validEnvelope {
					usage = usageSummary{}
				}
				if err := s.settle(state.context(stream.Context()), state, usage); err != nil {
					return err
				}
				if state.providerSpan != nil {
					state.providerSpan.End()
					state.providerSpan = nil
				}
			}
			if err := stream.Send(responseBodyContinue()); err != nil {
				return err
			}
		default:
			return errors.New("unsupported ext_proc message")
		}
	}
}

func (s streamState) context(fallback context.Context) context.Context {
	if s.traceContext != nil {
		return s.traceContext
	}
	return fallback
}

func (s *Server) authorizeGatewayTransport(ctx context.Context) (string, error) {
	if s.AllowInsecureDev {
		return "insecure-dev", nil
	}
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("authenticated Envoy mTLS transport is required")
	}
	tlsInfo, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 || len(tlsInfo.State.VerifiedChains) == 0 {
		return "", errors.New("verified Envoy client certificate is required")
	}
	for _, uri := range tlsInfo.State.PeerCertificates[0].URIs {
		if _, allowed := s.AllowedGatewayURIs[uri.String()]; allowed {
			return uri.String(), nil
		}
	}
	return "", errors.New("envoy client certificate SPIFFE identity is not allowed")
}

type admitResult struct {
	Decision             string                                `json:"decision"`
	ReasonCode           string                                `json:"reason_code"`
	SelectedDeployment   string                                `json:"selected_deployment"`
	ProviderAttemptID    string                                `json:"provider_attempt_id"`
	ApprovalRef          string                                `json:"approval_ref,omitempty"`
	PricingVersion       string                                `json:"pricing_version"`
	RouteSnapshot        *govar.RouteSnapshot                  `json:"route_snapshot"`
	SettlementUsageBases []aiopsv1alpha1.ProviderBillableBasis `json:"settlement_usage_bases"`
}

func (s *Server) admitAndClaim(ctx context.Context, state *streamState) error {
	payload, err := decodeJSONObject(state.requestBody)
	if err != nil {
		return errors.New("malformed LLM request body")
	}
	if raw, exists := payload["stream"]; exists {
		var streaming bool
		if err := json.Unmarshal(raw, &streaming); err != nil {
			return errors.New("stream must be boolean")
		}
		if streaming {
			return errors.New("stream=true is unsupported until authoritative streaming settlement is implemented")
		}
	}
	if _, exists := payload["max_tokens"]; exists {
		return errors.New("max_tokens is provider-native; governed requests must declare exact max_output_tokens for adapter rewriting")
	}
	maxOutput, err := exactNonNegativeInt64Field(payload, "max_output_tokens", true)
	if err != nil || maxOutput == 0 {
		return errors.New("max_output_tokens must be a positive exact JSON integer")
	}
	state.requestID = first(state.headers["x-request-id"], state.headers["x-govar-request-id"])
	if state.requestID == "" {
		return errors.New("trusted Envoy route must provide x-request-id")
	}
	binding := state.binding
	request := map[string]any{"request_id": state.requestID, "namespace": binding.Namespace, "tenant_id": binding.TenantID,
		"workload_uid": binding.WorkloadUID, "team": binding.Team, "application": binding.Application,
		"budget_policy_name": binding.BudgetPolicy, "routing_policy_name": binding.RoutingPolicy,
		"sensitive_data": binding.Sensitive, "allowed_zones": binding.AllowedZones,
		"input_tokens": int64(0), "input_tokens_exact": false, "max_output_tokens": maxOutput}
	var chargeBounds []govarpricing.UsageQuantity
	for _, mapping := range []struct {
		field string
		basis aiopsv1alpha1.ProviderBillableBasis
	}{
		{"max_cached_input_tokens", aiopsv1alpha1.ProviderBasisCachedInputTokens}, {"max_reasoning_tokens", aiopsv1alpha1.ProviderBasisReasoningTokens},
		{"max_tool_calls", aiopsv1alpha1.ProviderBasisToolCall}, {"max_media_units", aiopsv1alpha1.ProviderBasisMediaUnit},
		{"timeout_seconds", aiopsv1alpha1.ProviderBasisBillableSecond}, {"max_retry_attempts", aiopsv1alpha1.ProviderBasisRetryAttempt},
	} {
		if _, exists := payload[mapping.field]; exists {
			quantity, err := exactNonNegativeInt64Field(payload, mapping.field, false)
			if err != nil {
				return err
			}
			chargeBounds = append(chargeBounds, govarpricing.UsageQuantity{Basis: mapping.basis, Quantity: quantity})
		}
	}
	if raw, exists := payload["cancellation_possible"]; exists {
		var possible bool
		if err := json.Unmarshal(raw, &possible); err != nil {
			return errors.New("cancellation_possible must be boolean")
		}
		q := int64(0)
		if possible {
			q = 1
		}
		chargeBounds = append(chargeBounds, govarpricing.UsageQuantity{Basis: aiopsv1alpha1.ProviderBasisCancellation, Quantity: q})
	}
	if len(chargeBounds) != 0 {
		request["charge_bounds"] = chargeBounds
	}
	if approvalRef := strings.TrimSpace(state.headers["x-govar-approval-ref"]); approvalRef != "" {
		request["approval_ref"] = approvalRef
	}
	var result admitResult
	if err := s.post(ctx, *state, "/v1/admit", request, &result); err != nil {
		return err
	}
	if result.Decision != "ADMIT" || result.ReasonCode == "duplicate_request" {
		return fmt.Errorf("admission decision=%s reason=%s approval_ref=%s", result.Decision, result.ReasonCode, result.ApprovalRef)
	}
	state.attemptID = result.ProviderAttemptID
	state.settlementBases = append([]aiopsv1alpha1.ProviderBillableBasis(nil), result.SettlementUsageBases...)
	state.requestStartedAt = time.Now().UTC()
	if result.RouteSnapshot == nil || result.SelectedDeployment != result.RouteSnapshot.ModelName || result.PricingVersion != result.RouteSnapshot.PricingVersion {
		return errors.New("admission response route snapshot conflicts with top-level identity")
	}
	if err := govar.ValidateRouteSnapshot(*result.RouteSnapshot); err != nil {
		return err
	}
	actuationContext, actuationSpan := otel.Tracer("github.com/imperium/ai-sovereign-finops-operator/gov-ar-ext-proc").Start(
		ctx, "govar.route_actuation", trace.WithAttributes(attribute.String("govar.route_snapshot_hash", result.RouteSnapshot.SnapshotHash)))
	defer actuationSpan.End()
	route := RouteTarget{SelectedModel: result.RouteSnapshot.ModelName, ProviderDeployment: result.RouteSnapshot.ProviderDeployment,
		Cluster: result.RouteSnapshot.Cluster, Authority: result.RouteSnapshot.Authority, PathMode: result.RouteSnapshot.PathMode,
		SnapshotHash: result.RouteSnapshot.SnapshotHash}
	state.routeSnapshotHash = result.RouteSnapshot.SnapshotHash
	rewritten, err := rewriteRequestForRoute(state.requestBody, state.headers[":path"], route)
	if err != nil {
		return err
	}
	route.Path, err = routePath(state.headers[":path"], route)
	if err != nil {
		return err
	}
	state.route, state.requestBody = route, rewritten
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("govar.decision", result.Decision),
		attribute.String("govar.reason_code", result.ReasonCode),
		attribute.String("govar.route_snapshot_hash", state.routeSnapshotHash),
	)
	if err := s.dispatch(actuationContext, *state, "claim", "CLAIMED"); err != nil {
		return err
	}
	state.traceContext, state.providerSpan = otel.Tracer("github.com/imperium/ai-sovereign-finops-operator/gov-ar-ext-proc").Start(
		ctx, "govar.provider_attempt", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("govar.route_snapshot_hash", state.routeSnapshotHash)))
	return nil
}

func rewriteRequestForRoute(body []byte, originalPath string, target RouteTarget) ([]byte, error) {
	payload, err := decodeJSONObject(body)
	if err != nil {
		return nil, errors.New("malformed LLM request body")
	}
	capValue, exists := payload["max_output_tokens"]
	if !exists {
		return nil, errors.New("canonical max_output_tokens is absent before provider rewrite")
	}
	if _, err := exactNonNegativeInt64Field(payload, "max_output_tokens", true); err != nil {
		return nil, err
	}
	providerCapField, err := providerOutputCapField(target.PathMode, originalPath)
	if err != nil {
		return nil, err
	}
	switch target.PathMode {
	case "openai-body":
		model, _ := json.Marshal(target.ProviderDeployment)
		payload["model"] = model
	case "azure-deployment-path":
		delete(payload, "model")
	default:
		return nil, fmt.Errorf("route path mode %q has no authoritative GOV-AR request/usage adapter", target.PathMode)
	}
	delete(payload, "max_output_tokens")
	delete(payload, "max_tokens")
	payload[providerCapField] = capValue
	return json.Marshal(payload)
}

func providerOutputCapField(pathMode, originalPath string) (string, error) {
	path, _, _ := strings.Cut(first(originalPath, "/v1/chat/completions"), "?")
	lowerPath := strings.ToLower(strings.TrimSuffix(path, "/"))
	if pathMode != "openai-body" && pathMode != "azure-deployment-path" {
		return "", fmt.Errorf("route path mode %q has no authoritative GOV-AR output-cap adapter", pathMode)
	}
	switch {
	case strings.HasSuffix(lowerPath, "/responses"):
		return "max_output_tokens", nil
	case strings.HasSuffix(lowerPath, "/chat/completions"), strings.HasSuffix(lowerPath, "/completions"):
		return "max_tokens", nil
	default:
		return "", fmt.Errorf("path %q has no output-cap and authoritative-usage mapping for %s", path, pathMode)
	}
}

func routePath(original string, target RouteTarget) (string, error) {
	path, query, _ := strings.Cut(first(original, "/v1/chat/completions"), "?")
	lowerPath := strings.ToLower(strings.TrimSuffix(path, "/"))
	switch target.PathMode {
	case "openai-body":
		if !isOpenAIPath(lowerPath) {
			return "", fmt.Errorf("OpenAI body adapter does not support request path %q", path)
		}
		// Preserve the API shape; only the provider model in the body changes.
	case "azure-deployment-path":
		suffix := ""
		for _, candidate := range []string{"/chat/completions", "/responses", "/completions"} {
			if strings.HasSuffix(lowerPath, candidate) {
				suffix = candidate
				break
			}
		}
		if suffix == "" {
			return "", fmt.Errorf("azure deployment adapter does not support request path %q", path)
		}
		path = "/openai/deployments/" + url.PathEscape(target.ProviderDeployment) + suffix
	default:
		return "", fmt.Errorf("route path mode %q has no authoritative GOV-AR request/usage adapter", target.PathMode)
	}
	if query != "" {
		path += "?" + query
	}
	return path, nil
}

func isOpenAIPath(path string) bool {
	switch path {
	case "/v1/chat/completions", "/v1/responses", "/v1/completions":
		return true
	}
	return false
}

func (s *Server) dispatch(ctx context.Context, state streamState, suffix, status string) error {
	return s.post(ctx, state, "/v1/dispatch", map[string]any{"request_id": state.requestID, "event_id": state.requestID + ":extproc:" + suffix,
		"tenant_id": state.binding.TenantID, "workload_uid": state.binding.WorkloadUID, "provider_attempt_id": state.attemptID, "route_snapshot_hash": state.routeSnapshotHash, "status": status}, nil)
}
func (s *Server) settle(ctx context.Context, state streamState, usage usageSummary) error {
	return s.post(ctx, state, "/v1/settle", map[string]any{"request_id": state.requestID, "settlement_id": state.requestID + ":extproc:settle",
		"tenant_id": state.binding.TenantID, "workload_uid": state.binding.WorkloadUID, "provider_attempt_id": state.attemptID, "actual_cost_micros": 0,
		"usage": usage.Quantities, "usage_version": 1, "final": true}, nil)
}
func (s *Server) post(ctx context.Context, state streamState, path string, payload any, out any) (err error) {
	spanName := "govar.ledger_request"
	switch path {
	case "/v1/admit":
		spanName = "govar.admission_request"
	case "/v1/dispatch":
		spanName = "govar.dispatch_transition"
	case "/v1/settle":
		spanName = "govar.settlement"
	case "/v1/cancel":
		spanName = "govar.cancellation"
	}
	ctx, span := otel.Tracer("github.com/imperium/ai-sovereign-finops-operator/gov-ar-ext-proc").Start(
		ctx, spanName, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("http.route", path)))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "ledger request failed")
		}
		span.End()
	}()
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.AdmissionURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	namespace, tenant, workload := state.binding.Namespace, state.binding.TenantID, state.binding.WorkloadUID
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	digest := sha256.Sum256(body)
	message := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%x", timestamp, req.Method, req.URL.EscapedPath(), tenant, workload, namespace, digest)
	key := deriveKey(s.MasterSecret, namespace, tenant, workload)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(message))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GOVAR-Tenant-ID", tenant)
	req.Header.Set("X-GOVAR-Workload-UID", workload)
	req.Header.Set("X-GOVAR-Namespace", namespace)
	req.Header.Set("X-GOVAR-Timestamp", timestamp)
	req.Header.Set("X-GOVAR-Signature", fmt.Sprintf("%x", mac.Sum(nil)))
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", response.StatusCode))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("%s returned %d: %s", path, response.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out != nil {
		return json.NewDecoder(response.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	return nil
}

func deriveKey(master []byte, namespace, tenant, workload string) []byte {
	mac := hmac.New(sha256.New, master)
	_, _ = mac.Write([]byte("govar-identity-v2\x00" + namespace + "\x00" + tenant + "\x00" + workload))
	return []byte(fmt.Sprintf("%x", mac.Sum(nil)))
}
func (s *Server) resolvePrincipal(ctx context.Context, gatewayURI, xfcc string) (RouteBinding, error) {
	fields := map[string]string{}
	for _, part := range strings.FieldsFunc(xfcc, func(r rune) bool { return r == ';' || r == ',' }) {
		part = strings.TrimSpace(part)
		key, value, ok := strings.Cut(part, "=")
		if !ok || fields[key] != "" {
			return RouteBinding{}, errors.New("XFCC is not a single SANITIZE_SET client certificate record")
		}
		fields[key] = strings.Trim(value, `"`)
	}
	uri := fields["URI"]
	if uri == "" {
		return RouteBinding{}, errors.New("sanitized downstream mTLS URI identity is required")
	}
	if gatewayURI != "insecure-dev" && fields["By"] != gatewayURI {
		return RouteBinding{}, errors.New("XFCC By identity does not match the authenticated Envoy certificate")
	}
	hash, hashErr := hex.DecodeString(fields["Hash"])
	if hashErr != nil || len(hash) != sha256.Size {
		return RouteBinding{}, errors.New("XFCC client certificate hash is not a SHA-256 SANITIZE_SET value")
	}
	if s.ResolvePrincipal != nil {
		return s.ResolvePrincipal(ctx, uri)
	}
	binding, ok := s.PrincipalRegistry[uri]
	if !ok || binding.Namespace == "" || binding.TenantID == "" || binding.WorkloadUID == "" || binding.BudgetPolicy == "" || binding.RoutingPolicy == "" {
		return RouteBinding{}, errors.New("mTLS principal is not bound to a complete server-side workload registry entry")
	}
	return binding, nil
}
func headerMap(m *corev3.HeaderMap) map[string]string {
	out := map[string]string{}
	if m == nil {
		return out
	}
	for _, h := range m.Headers {
		out[strings.ToLower(h.Key)] = first(h.Value, string(h.RawValue))
	}
	return out
}
func headerContinue(clear bool, ctx context.Context) *extprocv3.ProcessingResponse {
	common := &extprocv3.CommonResponse{Status: extprocv3.CommonResponse_CONTINUE, ClearRouteCache: clear}
	if headers := traceHeaderOptions(ctx); len(headers) != 0 {
		common.HeaderMutation = &extprocv3.HeaderMutation{SetHeaders: headers}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: &extprocv3.HeadersResponse{Response: common}}}
}

func traceHeaderOptions(ctx context.Context) []*corev3.HeaderValueOption {
	carrier := propagation.MapCarrier{}
	// Inject only the W3C trace context. Arbitrary baggage is deliberately not
	// copied across the governance boundary because it may contain unbounded or
	// identity-bearing caller data.
	propagation.TraceContext{}.Inject(ctx, carrier)
	if traceparent := carrier.Get("traceparent"); traceparent != "" {
		set := func(key, value string) *corev3.HeaderValueOption {
			return &corev3.HeaderValueOption{Header: &corev3.HeaderValue{Key: key, RawValue: []byte(value)}, AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD}
		}
		headers := []*corev3.HeaderValueOption{set("traceparent", traceparent)}
		if tracestate := carrier.Get("tracestate"); tracestate != "" {
			headers = append(headers, set("tracestate", tracestate))
		}
		return headers
	}
	return nil
}
func bodyContinue(clear bool, route RouteTarget, body []byte, ctx context.Context) *extprocv3.ProcessingResponse {
	common := &extprocv3.CommonResponse{Status: extprocv3.CommonResponse_CONTINUE, ClearRouteCache: clear}
	if route.SelectedModel != "" {
		set := func(key, value string) *corev3.HeaderValueOption {
			return &corev3.HeaderValueOption{Header: &corev3.HeaderValue{Key: key, RawValue: []byte(value)}, AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD}
		}
		common.HeaderMutation = &extprocv3.HeaderMutation{SetHeaders: []*corev3.HeaderValueOption{
			set("x-ai-eg-model", route.SelectedModel), set("x-govar-upstream-cluster", route.Cluster), set("x-govar-upstream-authority", route.Authority), set(":path", route.Path),
			set("x-govar-route-snapshot-hash", route.SnapshotHash),
			set("content-length", strconv.Itoa(len(body))),
		}}
		common.HeaderMutation.SetHeaders = append(common.HeaderMutation.SetHeaders, traceHeaderOptions(ctx)...)
		common.BodyMutation = &extprocv3.BodyMutation{Mutation: &extprocv3.BodyMutation_Body{Body: body}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: &extprocv3.BodyResponse{Response: common}}}
}
func responseHeaderContinue() *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{ResponseHeaders: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{Status: extprocv3.CommonResponse_CONTINUE}}}}
}
func responseBodyContinue() *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: &extprocv3.BodyResponse{Response: &extprocv3.CommonResponse{Status: extprocv3.CommonResponse_CONTINUE}}}}
}
func immediate(status int, body string) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ImmediateResponse{ImmediateResponse: &extprocv3.ImmediateResponse{Status: &typev3.HttpStatus{Code: typev3.StatusCode(status)}, Body: []byte(body), Details: "govar_ext_proc_denied"}}}
}

type usageSummary struct{ Quantities []govarpricing.UsageQuantity }

func parseUsage(body []byte, required []aiopsv1alpha1.ProviderBillableBasis, route RouteTarget) (usageSummary, bool) {
	if _, err := providerOutputCapField(route.PathMode, route.Path); err != nil {
		return usageSummary{}, false
	}
	if route.PathMode != "openai-body" && route.PathMode != "azure-deployment-path" {
		return usageSummary{}, false
	}
	var raw struct {
		Usage   *map[string]json.RawMessage `json:"usage"`
		Choices []struct {
			Message struct {
				ToolCalls []json.RawMessage `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if json.Unmarshal(body, &raw) != nil {
		return usageSummary{}, false
	}
	read := func(m map[string]json.RawMessage, keys ...string) (int64, bool) {
		for _, k := range keys {
			if v, ok := m[k]; ok {
				var n int64
				if json.Unmarshal(v, &n) == nil && n >= 0 {
					return n, true
				}
				return 0, false
			}
		}
		return 0, false
	}
	usageMap := map[string]json.RawMessage{}
	if raw.Usage != nil {
		usageMap = *raw.Usage
	}
	detail := func(parentKey, detailKey string) (int64, bool) {
		value, ok := usageMap[parentKey]
		if !ok {
			return 0, false
		}
		var nested map[string]json.RawMessage
		if json.Unmarshal(value, &nested) != nil {
			return 0, false
		}
		return read(nested, detailKey)
	}
	path, _, _ := strings.Cut(route.Path, "?")
	responsesAPI := strings.HasSuffix(strings.ToLower(strings.TrimSuffix(path, "/")), "/responses")
	var input, output, cached, reasoning int64
	var inputOK, outputOK, cachedOK, reasoningOK bool
	if responsesAPI {
		input, inputOK = read(usageMap, "input_tokens")
		output, outputOK = read(usageMap, "output_tokens")
		cached, cachedOK = detail("input_tokens_details", "cached_tokens")
		reasoning, reasoningOK = detail("output_tokens_details", "reasoning_tokens")
	} else {
		input, inputOK = read(usageMap, "prompt_tokens")
		output, outputOK = read(usageMap, "completion_tokens")
		cached, cachedOK = detail("prompt_tokens_details", "cached_tokens")
		reasoning, reasoningOK = detail("completion_tokens_details", "reasoning_tokens")
	}
	requiredSet := map[aiopsv1alpha1.ProviderBillableBasis]bool{}
	for _, basis := range required {
		requiredSet[basis] = true
	}
	if requiredSet[aiopsv1alpha1.ProviderBasisCachedInputTokens] && inputOK && cachedOK {
		if cached > input {
			return usageSummary{}, false
		}
		input -= cached
	}
	if requiredSet[aiopsv1alpha1.ProviderBasisReasoningTokens] && outputOK && reasoningOK {
		if reasoning > output {
			return usageSummary{}, false
		}
		output -= reasoning
	}
	var quantities []govarpricing.UsageQuantity
	appendIf := func(b aiopsv1alpha1.ProviderBillableBasis, q int64, ok bool) {
		if requiredSet[b] && ok {
			quantities = append(quantities, govarpricing.UsageQuantity{Basis: b, Quantity: q})
		}
	}
	appendIf(aiopsv1alpha1.ProviderBasisInputTokens, input, inputOK)
	appendIf(aiopsv1alpha1.ProviderBasisCachedInputTokens, cached, cachedOK)
	appendIf(aiopsv1alpha1.ProviderBasisOutputTokens, output, outputOK)
	appendIf(aiopsv1alpha1.ProviderBasisReasoningTokens, reasoning, reasoningOK)
	// The response envelope proves one provider response for this attempt. It
	// does not prove provider-specific time, cancellation, retry, or media
	// quantities, so those bases remain absent and retain their residual holds.
	appendIf(aiopsv1alpha1.ProviderBasisRequest, 1, true)
	toolCalls := int64(0)
	for _, choice := range raw.Choices {
		toolCalls += int64(len(choice.Message.ToolCalls))
	}
	appendIf(aiopsv1alpha1.ProviderBasisToolCall, toolCalls, true)
	sort.Slice(quantities, func(i, j int) bool { return quantities[i].Basis < quantities[j].Basis })
	return usageSummary{Quantities: quantities}, raw.Usage != nil
}

func decodeJSONObject(body []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("JSON request must be an object")
	}
	result := make(map[string]json.RawMessage)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, errors.New("JSON object key is not a string")
		}
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf("duplicate JSON field %q", key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return nil, errors.New("request body contains trailing JSON")
	}
	return result, nil
}

func exactNonNegativeInt64Field(payload map[string]json.RawMessage, field string, required bool) (int64, error) {
	raw, exists := payload[field]
	if !exists {
		if required {
			return 0, fmt.Errorf("%s is required", field)
		}
		return 0, nil
	}
	text := string(raw)
	if text == "" || (len(text) > 1 && text[0] == '0') || text[0] < '0' || text[0] > '9' {
		return 0, fmt.Errorf("%s must be a non-negative exact JSON integer", field)
	}
	for i := 1; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, fmt.Errorf("%s must be a non-negative exact JSON integer", field)
		}
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is outside the int64 monetary-bound domain", field)
	}
	return value, nil
}
func first(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
