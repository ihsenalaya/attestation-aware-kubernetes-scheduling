package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarextproc"
	"github.com/imperium/ai-sovereign-finops-operator/internal/webhook/podinjector"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aiopsv1alpha1.AddToScheme(scheme))
}

type server struct {
	k8s     client.Client
	engine  admissionBackend
	auth    identityAuthenticator
	metrics *serviceMetrics
	workers workerHealth
}

type workerHealth interface {
	Healthy() error
}

type admissionBackend interface {
	Admit(req govar.AdmitRequest, budget aiopsv1alpha1.AIBudgetPolicy, routing aiopsv1alpha1.AIRoutingPolicy, candidates []govar.Candidate) (govar.AdmitResponse, error)
	Dispatch(req govar.DispatchRequest) (govar.Reservation, govar.ReasonCode, error)
	Settle(req govar.SettleRequest) (govar.Reservation, govar.ReasonCode, error)
	Cancel(req govar.CancelRequest) (govar.Reservation, govar.ReasonCode, error)
	LiabilityWithError(tenantID string) (govar.LiabilityResponse, error)
	ObservabilitySnapshot(context.Context, string) (govar.TenantObservabilitySnapshot, error)
	ComponentObservabilitySnapshot(context.Context) (govar.ComponentObservabilitySnapshot, error)
	Ready(context.Context) error
}

type authenticatedPrincipal struct {
	tenantID       string
	workloadUID    string
	namespace      string
	podName        string
	serviceAccount string
	role           principalRole
}

type principalRole string

const (
	roleAdmissionOnly         principalRole = "ADMISSION_ONLY"
	roleGateway               principalRole = "GATEWAY"
	maxAuthenticatedBodyBytes int64         = 1 << 20
)

var errAuthenticatedBodyTooLarge = errors.New("authenticated request body exceeds 1 MiB")

type identityAuthenticator struct {
	masterSecret []byte
	now          func() time.Time
	reviewToken  func(context.Context, string) (authenticatedPrincipal, error)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	tracingShutdown, err := initializeTracing(ctx)
	if err != nil {
		return fmt.Errorf("configure GOV-AR tracing: %w", err)
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracingShutdown(shutdownContext); err != nil {
			log.Printf("flush GOV-AR traces: %v", err)
		}
	}()
	metrics, err := newServiceMetricsFromEnvironment()
	if err != nil {
		return fmt.Errorf("configure GOV-AR metrics: %w", err)
	}
	addr := os.Getenv("GOV_AR_ADMISSION_ADDR")
	if addr == "" {
		addr = ":8084"
	}
	identitySecret := os.Getenv("GOV_AR_IDENTITY_MASTER_SECRET")
	if len(identitySecret) < 32 {
		return errors.New("GOV_AR_IDENTITY_MASTER_SECRET must contain at least 32 bytes")
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("get kubeconfig: %w", err)
	}
	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("create k8s client: %w", err)
	}

	var engine admissionBackend
	var workers *durableWorkerManager
	devInMemory := strings.EqualFold(strings.TrimSpace(os.Getenv("GOV_AR_DEV_IN_MEMORY")), "true")
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		softwareSHA256 := strings.TrimSpace(os.Getenv("GOV_AR_SOFTWARE_SHA256"))
		pgEngine, err := govar.OpenPostgresEngine(ctx, databaseURL, softwareSHA256)
		if err != nil {
			return fmt.Errorf("create postgres engine: %w", err)
		}
		defer pgEngine.Close()
		selectedFeedbackAuthority, err := selectedFeedbackAuthorityFromEnvironment(softwareSHA256)
		if err != nil {
			return err
		}
		if selectedFeedbackAuthority != nil {
			if err := pgEngine.ConfigureSelectedFeedbackAuthority(*selectedFeedbackAuthority); err != nil {
				return fmt.Errorf("configure selected-feedback run authority: %w", err)
			}
		}
		engine = pgEngine
		workers, err = newDurableWorkerManager(pgEngine, metrics, reconciliationWorkerConfigFromEnvironment())
		if err != nil {
			return fmt.Errorf("configure durable workers: %w", err)
		}
		if err := workers.Start(ctx); err != nil {
			return fmt.Errorf("start durable workers: %w", err)
		}
		log.Println("gov-ar-admission using PostgreSQL-backed ledger")
	} else {
		if !devInMemory {
			return errors.New("DATABASE_URL is required unless GOV_AR_DEV_IN_MEMORY=true")
		}
		engine = govar.NewEngine()
		log.Println("gov-ar-admission using explicit single-replica development in-memory ledger")
	}

	srv := &server{k8s: k8sClient, engine: engine, auth: identityAuthenticator{masterSecret: []byte(identitySecret), now: time.Now, reviewToken: tokenReviewFunc(k8sClient)}, metrics: metrics, workers: workers}
	extProcAddr := strings.TrimSpace(os.Getenv("GOV_AR_EXT_PROC_ADDR"))
	if extProcAddr == "" {
		extProcAddr = ":9002"
	}
	extListener, err := net.Listen("tcp", extProcAddr)
	if err != nil {
		return fmt.Errorf("listen ext_proc: %w", err)
	}
	defer extListener.Close()
	allowInsecureExtProc := devInMemory && strings.EqualFold(strings.TrimSpace(os.Getenv("GOV_AR_EXT_PROC_DEV_INSECURE")), "true")
	grpcOptions := make([]grpc.ServerOption, 0, 1)
	if !allowInsecureExtProc {
		transportCredentials, err := extProcServerCredentials(
			os.Getenv("GOV_AR_EXT_PROC_TLS_CERT_FILE"),
			os.Getenv("GOV_AR_EXT_PROC_TLS_KEY_FILE"),
			os.Getenv("GOV_AR_EXT_PROC_CLIENT_CA_FILE"),
		)
		if err != nil {
			return fmt.Errorf("configure ext_proc mTLS: %w", err)
		}
		grpcOptions = append(grpcOptions, grpc.Creds(transportCredentials))
	}
	extGRPC := grpc.NewServer(grpcOptions...)
	admissionURL := strings.TrimSpace(os.Getenv("GOV_AR_INTERNAL_ADMISSION_URL"))
	if admissionURL == "" {
		admissionURL = "http://127.0.0.1:8084"
	}
	gatewaySPIFFEID := strings.TrimSpace(os.Getenv("GOV_AR_EXT_PROC_GATEWAY_SPIFFE_ID"))
	if !allowInsecureExtProc && gatewaySPIFFEID == "" {
		return errors.New("GOV_AR_EXT_PROC_GATEWAY_SPIFFE_ID is required for ext_proc mTLS")
	}
	extprocv3.RegisterExternalProcessorServer(extGRPC, &govarextproc.Server{
		AdmissionURL: admissionURL, MasterSecret: []byte(identitySecret), ResolvePrincipal: extProcPrincipalResolver(srv),
		AllowedGatewayURIs: map[string]struct{}{gatewaySPIFFEID: {}}, AllowInsecureDev: allowInsecureExtProc,
	})
	grpcErrors := make(chan error, 1)
	go func() { grpcErrors <- extGRPC.Serve(extListener) }()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/readyz", srv.handleReadyz)
	mux.Handle("/metrics", metrics.handler())
	mux.HandleFunc("/v1/admit", srv.handleAdmit)
	mux.HandleFunc("/v1/dispatch", srv.handleDispatch)
	mux.HandleFunc("/v1/settle", srv.handleSettle)
	mux.HandleFunc("/v1/cancel", srv.handleCancel)
	mux.HandleFunc("/v1/liability/", srv.handleLiability)

	httpServer := &http.Server{Addr: addr, Handler: instrumentHTTP(mux, metrics), ReadHeaderTimeout: 10 * time.Second}
	httpErrors := make(chan error, 1)
	go func() { httpErrors <- httpServer.ListenAndServe() }()
	log.Printf("gov-ar-admission listening on %s; Envoy ext_proc on %s", addr, extProcAddr)
	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-httpErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("HTTP server: %w", err)
		}
	case err := <-grpcErrors:
		if err != nil {
			serveErr = fmt.Errorf("ext_proc server: %w", err)
		}
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownContext); err != nil && serveErr == nil {
		serveErr = fmt.Errorf("shutdown HTTP server: %w", err)
	}
	grpcDone := make(chan struct{})
	go func() {
		extGRPC.GracefulStop()
		close(grpcDone)
	}()
	select {
	case <-grpcDone:
	case <-shutdownContext.Done():
		extGRPC.Stop()
		if serveErr == nil {
			serveErr = fmt.Errorf("shutdown ext_proc server: %w", shutdownContext.Err())
		}
	}
	if workers != nil {
		if err := workers.Shutdown(shutdownContext); err != nil && serveErr == nil {
			serveErr = fmt.Errorf("shutdown durable workers: %w", err)
		}
	}
	return serveErr
}

func selectedFeedbackAuthorityFromEnvironment(softwareSHA256 string) (*govar.SelectedFeedbackAuthority, error) {
	values := map[string]string{
		"run_id":                 strings.TrimSpace(os.Getenv("GOV_AR_SELECTED_FEEDBACK_RUN_ID")),
		"split":                  strings.TrimSpace(os.Getenv("GOV_AR_SELECTED_FEEDBACK_SPLIT")),
		"protocol_sha256":        strings.TrimSpace(os.Getenv("GOV_AR_SELECTED_FEEDBACK_PROTOCOL_SHA256")),
		"oracle_artifact_sha256": strings.TrimSpace(os.Getenv("GOV_AR_SELECTED_FEEDBACK_ORACLE_SHA256")),
		"config_sha256":          strings.TrimSpace(os.Getenv("GOV_AR_SELECTED_FEEDBACK_CONFIG_SHA256")),
		"model_map_sha256":       strings.TrimSpace(os.Getenv("GOV_AR_SELECTED_FEEDBACK_MODEL_MAP_SHA256")),
		"model_map_json":         strings.TrimSpace(os.Getenv("GOV_AR_SELECTED_FEEDBACK_MODEL_MAP_JSON")),
	}
	configured := false
	for _, value := range values {
		configured = configured || value != ""
	}
	if !configured {
		return nil, nil
	}
	for name, value := range values {
		if value == "" {
			return nil, fmt.Errorf("GOV-AR selected-feedback configuration is incomplete: %s is empty", name)
		}
	}
	var selectedModelIDs map[string]string
	if err := json.Unmarshal([]byte(values["model_map_json"]), &selectedModelIDs); err != nil {
		return nil, fmt.Errorf("GOV-AR selected-feedback model map JSON: %w", err)
	}
	authority := &govar.SelectedFeedbackAuthority{
		RunID: values["run_id"], DatasetID: "routereval_math_outcomes", Split: values["split"],
		ProtocolSHA256: values["protocol_sha256"], OracleArtifactSHA256: values["oracle_artifact_sha256"],
		SoftwareSHA256: softwareSHA256, ConfigSHA256: values["config_sha256"],
		ModelMapSHA256: values["model_map_sha256"], SelectedModelIDs: selectedModelIDs,
	}
	if err := authority.Validate(); err != nil {
		return nil, fmt.Errorf("GOV-AR selected-feedback configuration: %w", err)
	}
	return authority, nil
}

func extProcServerCredentials(certFile, keyFile, clientCAFile string) (credentials.TransportCredentials, error) {
	if strings.TrimSpace(certFile) == "" || strings.TrimSpace(keyFile) == "" || strings.TrimSpace(clientCAFile) == "" {
		return nil, errors.New("server certificate, private key, and client CA files are required")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load ext_proc server keypair: %w", err)
	}
	caPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read ext_proc client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("ext_proc client CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: clientCAs, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}), nil
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.engine.Ready(ctx); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, govar.ReasonInvalidTransition, fmt.Errorf("ledger not ready: %w", err))
		return
	}
	if s.workers != nil {
		if err := s.workers.Healthy(); err != nil {
			writeAPIError(w, http.StatusServiceUnavailable, govar.ReasonInvalidTransition, fmt.Errorf("durable workers not ready: %w", err))
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleAdmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	principal, body, err := s.auth.authenticate(r)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	var apiRequest struct {
		govar.AdmitRequest
		ApprovalRef string `json:"approval_ref,omitempty"`
	}
	if err := decodeStrictJSON(body, &apiRequest); err != nil {
		writeAPIError(w, http.StatusBadRequest, govar.ReasonInvalidTransition, err)
		return
	}
	req := apiRequest.AdmitRequest
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	trusted, err := s.resolveTrustedWorkload(ctx, principal)
	if err != nil {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, err)
		return
	}
	if err := bindTrustedAdmitRequest(&req, trusted); err != nil {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, err)
		return
	}
	req.AuthenticatedTenantID = trusted.tenant
	req.AuthenticatedWorkloadUID = trusted.uid
	req.AuthenticatedNamespace = trusted.namespace

	var budget aiopsv1alpha1.AIBudgetPolicy
	if err := s.k8s.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.BudgetPolicyName}, &budget); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var routing aiopsv1alpha1.AIRoutingPolicy
	if err := s.k8s.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.RoutingPolicyName}, &routing); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var modelList aiopsv1alpha1.AIModelList
	if err := s.k8s.List(ctx, &modelList, client.InNamespace(req.Namespace)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var providerList aiopsv1alpha1.AIProviderList
	if err := s.k8s.List(ctx, &providerList, client.InNamespace(req.Namespace)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	providers := map[string]aiopsv1alpha1.AIProvider{}
	for i := range providerList.Items {
		providers[providerList.Items[i].Name] = providerList.Items[i]
	}

	_, feasibilitySpan := startGOVAROperation(ctx, "govar.feasibility_filter")
	candidates := govar.BuildCandidates(govar.RequestContext{
		Namespace:     req.Namespace,
		Team:          req.Team,
		Application:   req.Application,
		SensitiveData: req.SensitiveData,
		AllowedZones:  req.AllowedZones,
	}, modelList.Items, providers)
	finishGOVAROperation(feasibilitySpan, nil, attribute.Int("govar.candidate_count", len(candidates)))
	routingForAdmission := routing
	if routing.Spec.Canary.Enabled {
		approvedCandidate, approval, err := s.resolveGOVARRouteApproval(ctx, apiRequest.ApprovalRef, routing, candidates)
		if err != nil {
			if errors.Is(err, errApprovalRequired) {
				candidate, candidateErr := selectApprovalCandidate(candidates)
				if candidateErr != nil {
					writeAPIError(w, http.StatusConflict, govar.ReasonApprovalRequired, candidateErr)
					return
				}
				approvalResponse := map[string]any{"decision": govar.DecisionRequireApproval, "reason_code": govar.ReasonApprovalRequired,
					"required_approval": requiredGOVARRouteApproval(routing, candidate)}
				if traceID := traceIDFromContext(ctx); traceID != "" {
					approvalResponse["trace_id"] = traceID
				}
				writeJSON(w, approvalResponse)
				recordAdmissionDecision(s.metrics, ctx, govar.DecisionRequireApproval, govar.ReasonApprovalRequired, "none")
				return
			}
			status := http.StatusForbidden
			if errors.Is(err, errApprovalExpired) {
				status = http.StatusGone
			}
			writeAPIError(w, status, govar.ReasonApprovalRequired, err)
			return
		}
		candidates = []govar.Candidate{approvedCandidate}
		routingForAdmission = *routing.DeepCopy()
		routingForAdmission.Spec.Canary.Enabled = false
		// Bind the exact controller-approved change identity to the immutable
		// ledger policyVersion without mutating the Kubernetes policy object.
		routingForAdmission.ResourceVersion = routing.ResourceVersion + "|govar-approval:" + string(approval.UID) + ":" + strconv.FormatInt(approval.Generation, 10) + ":" + approval.Status.ApprovedScopeDigest
	}
	transactionStarted := time.Now()
	_, reservationSpan := startGOVAROperation(ctx, "govar.reservation_transaction")
	resp, err := s.engine.Admit(req, budget, routingForAdmission, candidates)
	observeCommittedTransaction(s.metrics, transactionStarted, err)
	finishGOVAROperation(reservationSpan, err,
		attribute.String("govar.decision", string(resp.Decision)),
		attribute.String("govar.reason_code", string(resp.ReasonCode)),
		attribute.String("govar.reservation_method", resp.ReservationMode))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, govar.ReasonInvalidTransition, err)
		return
	}
	if traceID := traceIDFromContext(ctx); traceID != "" {
		resp.TraceID = traceID
	}
	recordAdmissionDecision(s.metrics, ctx, resp.Decision, resp.ReasonCode, resp.ReservationMode)
	if resp.Decision == govar.DecisionAdmit {
		recordCommittedTransition(s.metrics, ctx, govar.ReservationState("NEW"), govar.StateReserved, resp.ReasonCode, resp.ReasonCode != govar.ReasonDuplicateRequest)
		recordMetricError(ctx, publishCommittedTenantMetrics(ctx, s.metrics, s.engine, req.AuthenticatedTenantID))
	}
	writeJSON(w, resp)
}

var (
	errApprovalRequired = errors.New("no current exact policy-level GOV-AR route approval exists")
	errApprovalExpired  = errors.New("policy-level GOV-AR route approval expired")
)

func (s *server) resolveGOVARRouteApproval(ctx context.Context, approvalRef string, routing aiopsv1alpha1.AIRoutingPolicy, candidates []govar.Candidate) (govar.Candidate, *aiopsv1alpha1.AIChangeRequest, error) {
	if ref := strings.TrimSpace(approvalRef); ref != "" {
		var change aiopsv1alpha1.AIChangeRequest
		if err := s.k8s.Get(ctx, client.ObjectKey{Namespace: routing.Namespace, Name: ref}, &change); err != nil {
			return govar.Candidate{}, nil, fmt.Errorf("approved AIChangeRequest lookup failed: %w", err)
		}
		candidate, err := s.validateGOVARRouteApproval(&change, routing, candidates)
		return candidate, &change, err
	}
	var changes aiopsv1alpha1.AIChangeRequestList
	if err := s.k8s.List(ctx, &changes, client.InNamespace(routing.Namespace)); err != nil {
		return govar.Candidate{}, nil, fmt.Errorf("list policy-level GOV-AR approvals: %w", err)
	}
	sort.Slice(changes.Items, func(i, j int) bool { return changes.Items[i].Name < changes.Items[j].Name })
	for i := range changes.Items {
		change := &changes.Items[i]
		candidate, err := s.validateGOVARRouteApproval(change, routing, candidates)
		if err == nil {
			return candidate, change, nil
		}
	}
	return govar.Candidate{}, nil, errApprovalRequired
}

func (s *server) validateGOVARRouteApproval(change *aiopsv1alpha1.AIChangeRequest, routing aiopsv1alpha1.AIRoutingPolicy, candidates []govar.Candidate) (govar.Candidate, error) {
	if change == nil || change.Namespace != routing.Namespace || !change.DeletionTimestamp.IsZero() || change.UID == "" || change.Generation < 1 ||
		change.Spec.Action != aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute || change.Spec.Approval != aiopsv1alpha1.AIChangeRequestApprovalApproved ||
		change.Spec.GOVARRouteApproval == nil || change.Status.Phase != aiopsv1alpha1.AIChangeRequestPhaseApproved ||
		change.Status.ObservedGeneration != change.Generation || change.Status.ApprovedAt == nil || !apimeta.IsStatusConditionTrue(change.Status.Conditions, aiopsv1alpha1.ConditionReady) {
		return govar.Candidate{}, errors.New("AIChangeRequest lacks current controller-approved GOV-AR route evidence")
	}
	now := time.Now().UTC()
	if s.auth.now != nil {
		now = s.auth.now().UTC()
	}
	scope := change.Spec.GOVARRouteApproval
	if scope.ScopeDigest == "" || scope.ScopeDigest != scope.ComputeDigest() || change.Status.ApprovedScopeDigest != scope.ScopeDigest ||
		change.Status.ExpiresAt == nil || !change.Status.ExpiresAt.Equal(&scope.ValidUntil) {
		return govar.Candidate{}, errors.New("AIChangeRequest GOV-AR route scope digest or expiry evidence is stale")
	}
	if !now.Before(scope.ValidUntil.Time) {
		return govar.Candidate{}, errApprovalExpired
	}
	decision := change.Spec.GOVARDecision
	if decision == nil || change.Status.ApprovedDecisionDigest != decision.DecisionDigest ||
		change.Status.ApprovedBy != decision.ReviewerIdentity || change.ValidateGOVARDecision(now) != nil {
		return govar.Candidate{}, errors.New("AIChangeRequest lacks a current independently authenticated GOV-AR reviewer decision")
	}
	if scope.RoutingPolicy.Name != routing.Name || scope.RoutingPolicy.UID != routing.UID || scope.RoutingPolicy.Generation != routing.Generation {
		return govar.Candidate{}, errors.New("approved routing-policy UID/generation does not match the live policy")
	}
	for _, candidate := range candidates {
		snapshot := candidate.RouteSnapshot
		if !candidate.Feasible || govar.ValidateRouteSnapshot(snapshot) != nil ||
			scope.Model.Name != snapshot.ModelName || string(scope.Model.UID) != snapshot.ModelUID || scope.Model.Generation != snapshot.ModelGeneration ||
			scope.Provider.Name != snapshot.ProviderName || string(scope.Provider.UID) != snapshot.ProviderUID || scope.Provider.Generation != snapshot.ProviderGeneration ||
			scope.RouteSnapshotDigest != snapshot.SnapshotHash || scope.RouteSnapshotDigest != govar.RouteSnapshotHash(snapshot) {
			continue
		}
		return candidate, nil
	}
	return govar.Candidate{}, errors.New("approved model/provider/route snapshot is no longer a feasible exact candidate")
}

func requiredGOVARRouteApproval(routing aiopsv1alpha1.AIRoutingPolicy, candidate govar.Candidate) map[string]any {
	snapshot := candidate.RouteSnapshot
	return map[string]any{
		"kind": "AIChangeRequest", "action": aiopsv1alpha1.AIChangeRequestActionAuthorizeGOVARRoute,
		"routing_policy":        aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: routing.Name, UID: routing.UID, Generation: routing.Generation},
		"model":                 aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: snapshot.ModelName, UID: types.UID(snapshot.ModelUID), Generation: snapshot.ModelGeneration},
		"provider":              aiopsv1alpha1.AIWorkloadBindingResolvedReference{Name: snapshot.ProviderName, UID: types.UID(snapshot.ProviderUID), Generation: snapshot.ProviderGeneration},
		"route_snapshot_digest": snapshot.SnapshotHash,
	}
}

func selectApprovalCandidate(candidates []govar.Candidate) (govar.Candidate, error) {
	var selected *govar.Candidate
	for i := range candidates {
		candidate := candidates[i]
		if !candidate.Feasible || govar.ValidateRouteSnapshot(candidate.RouteSnapshot) != nil {
			continue
		}
		if selected == nil || candidate.ModelRef < selected.ModelRef {
			copy := candidate
			selected = &copy
		}
	}
	if selected == nil {
		return govar.Candidate{}, errors.New("no feasible typed candidate exists for approval")
	}
	return *selected, nil
}

func (s *server) handleDispatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	principal, body, err := s.auth.authenticate(r)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	if principal.role != roleGateway {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, errors.New("projected workload identity is admission-only"))
		return
	}
	var req govar.DispatchRequest
	if err := decodeStrictJSON(body, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, govar.ReasonInvalidTransition, err)
		return
	}
	trusted, err := s.authorizeEvent(r.Context(), principal, req.TenantID, req.WorkloadUID)
	if err != nil {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, err)
		return
	}
	req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID = trusted.tenant, trusted.uid
	transactionStarted := time.Now()
	_, transitionSpan := startGOVAROperation(r.Context(), "govar.dispatch_transaction")
	res, code, err := s.engine.Dispatch(req)
	observeCommittedTransaction(s.metrics, transactionStarted, err)
	finishGOVAROperation(transitionSpan, err, attribute.String("govar.reason_code", string(code)))
	if err != nil {
		writeAPIError(w, http.StatusConflict, code, err)
		return
	}
	recordCommittedTransition(s.metrics, r.Context(), res.PreviousState, res.State, code, res.TransitionEffective)
	recordMetricError(r.Context(), publishCommittedTenantMetrics(r.Context(), s.metrics, s.engine, req.AuthenticatedTenantID))
	writeJSON(w, transitionResponse(res, code))
}

func (s *server) handleSettle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	principal, body, err := s.auth.authenticate(r)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	if principal.role != roleGateway {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, errors.New("projected workload identity cannot submit authoritative usage or finality"))
		return
	}
	var req govar.SettleRequest
	if err := decodeStrictJSON(body, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, govar.ReasonInvalidTransition, err)
		return
	}
	if req.ActualCostMicros != 0 {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, errors.New("gateway settlement must derive cost from authoritative usage tokens"))
		return
	}
	trusted, err := s.authorizeEvent(r.Context(), principal, req.TenantID, req.WorkloadUID)
	if err != nil {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, err)
		return
	}
	req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID = trusted.tenant, trusted.uid
	transactionStarted := time.Now()
	_, settlementSpan := startGOVAROperation(r.Context(), "govar.settlement_transaction")
	res, code, err := s.engine.Settle(req)
	observeCommittedTransaction(s.metrics, transactionStarted, err)
	finishGOVAROperation(settlementSpan, err, attribute.String("govar.reason_code", string(code)))
	if err != nil {
		writeAPIError(w, http.StatusConflict, code, err)
		return
	}
	recordCommittedTransition(s.metrics, r.Context(), res.PreviousState, res.State, code, res.TransitionEffective)
	recordMetricError(r.Context(), publishCommittedTenantMetrics(r.Context(), s.metrics, s.engine, req.AuthenticatedTenantID))
	writeJSON(w, transitionResponse(res, code))
}

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	principal, body, err := s.auth.authenticate(r)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	if principal.role != roleGateway {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, errors.New("projected workload identity cannot submit delivery or cancellation transitions"))
		return
	}
	var req govar.CancelRequest
	if err := decodeStrictJSON(body, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, govar.ReasonInvalidTransition, err)
		return
	}
	if req.AuthoritativeUnbilled {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, errors.New("gateway authority cannot release liability as authoritative unbilled"))
		return
	}
	trusted, err := s.authorizeEvent(r.Context(), principal, req.TenantID, req.WorkloadUID)
	if err != nil {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, err)
		return
	}
	req.AuthenticatedTenantID, req.AuthenticatedWorkloadUID = trusted.tenant, trusted.uid
	transactionStarted := time.Now()
	_, cancellationSpan := startGOVAROperation(r.Context(), "govar.cancellation_transaction")
	res, code, err := s.engine.Cancel(req)
	observeCommittedTransaction(s.metrics, transactionStarted, err)
	finishGOVAROperation(cancellationSpan, err, attribute.String("govar.reason_code", string(code)))
	if err != nil {
		writeAPIError(w, http.StatusConflict, code, err)
		return
	}
	recordCommittedTransition(s.metrics, r.Context(), res.PreviousState, res.State, code, res.TransitionEffective)
	recordMetricError(r.Context(), publishCommittedTenantMetrics(r.Context(), s.metrics, s.engine, req.AuthenticatedTenantID))
	writeJSON(w, transitionResponse(res, code))
}

func (s *server) handleLiability(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tenantID := r.URL.Path[len("/v1/liability/"):]
	if tenantID == "" {
		http.Error(w, "tenant id required", http.StatusBadRequest)
		return
	}
	principal, _, err := s.auth.authenticate(r)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	trusted, err := s.resolveTrustedWorkload(r.Context(), principal)
	if err != nil {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, err)
		return
	}
	if trusted.tenant != tenantID {
		writeAPIError(w, http.StatusForbidden, govar.ReasonPrincipalMismatch, errors.New("authenticated tenant cannot read another tenant ledger"))
		return
	}
	liability, err := s.engine.LiabilityWithError(tenantID)
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, govar.ReasonInvalidTransition, fmt.Errorf("ledger query failed: %w", err))
		return
	}
	writeJSON(w, liability)
}

func transitionResponse(res govar.Reservation, code govar.ReasonCode) map[string]any {
	response := map[string]any{
		"request_id": res.RequestID, "tenant_id": res.TenantID, "workload_uid": res.WorkloadUID,
		"provider_attempt_id": res.ProviderAttemptID, "reason_code": code, "state": res.State,
		"outbox_state": res.OutboxState, "reserved_cost_micros": res.ReservedCostMicros,
		"provisional_cost_micros": res.ProvisionalCostMicros,
		"residual_hold_micros":    res.ResidualHoldMicros, "usage_version": res.UsageVersion,
		"finalized": res.Finalized, "pricing_version": res.PricingVersion,
	}
	if res.SelectedFeedbackDispatchID != "" {
		response["selected_feedback_dispatch_id"] = res.SelectedFeedbackDispatchID
	}
	return response
}

func (a identityAuthenticator) authenticate(r *http.Request) (authenticatedPrincipal, []byte, error) {
	if authorization := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(authorization, "Bearer ") {
		if a.reviewToken == nil {
			return authenticatedPrincipal{}, nil, errors.New("projected token review is not configured")
		}
		principal, err := a.reviewToken(r.Context(), strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer ")))
		if err != nil {
			return authenticatedPrincipal{}, nil, err
		}
		body, err := readAuthenticatedBody(r.Body)
		return principal, body, err
	}
	if len(a.masterSecret) < 32 {
		return authenticatedPrincipal{}, nil, errors.New("identity authentication is not configured")
	}
	tenantID := strings.TrimSpace(r.Header.Get("X-GOVAR-Tenant-ID"))
	workloadUID := strings.TrimSpace(r.Header.Get("X-GOVAR-Workload-UID"))
	namespace := strings.TrimSpace(r.Header.Get("X-GOVAR-Namespace"))
	timestamp := strings.TrimSpace(r.Header.Get("X-GOVAR-Timestamp"))
	signature := strings.TrimSpace(r.Header.Get("X-GOVAR-Signature"))
	if tenantID == "" || workloadUID == "" || namespace == "" || timestamp == "" || signature == "" {
		return authenticatedPrincipal{}, nil, errors.New("signed GOV-AR identity headers are required")
	}
	unixSeconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return authenticatedPrincipal{}, nil, errors.New("invalid GOV-AR identity timestamp")
	}
	now := time.Now()
	if a.now != nil {
		now = a.now()
	}
	if delta := now.Sub(time.Unix(unixSeconds, 0)); delta < -2*time.Minute || delta > 2*time.Minute {
		return authenticatedPrincipal{}, nil, errors.New("GOV-AR identity signature is outside the replay window")
	}
	body, err := readAuthenticatedBody(r.Body)
	if err != nil {
		return authenticatedPrincipal{}, nil, errors.New("read authenticated body")
	}
	digest := sha256.Sum256(body)
	message := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%x", timestamp, r.Method, r.URL.EscapedPath(), tenantID, workloadUID, namespace, digest)
	signerKey := deriveSignerKey(a.masterSecret, namespace, tenantID, workloadUID)
	mac := hmac.New(sha256.New, signerKey)
	_, _ = mac.Write([]byte(message))
	want := fmt.Sprintf("%x", mac.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(signature)), []byte(want)) {
		return authenticatedPrincipal{}, nil, errors.New("invalid GOV-AR identity signature")
	}
	return authenticatedPrincipal{tenantID: tenantID, workloadUID: workloadUID, namespace: namespace, role: roleGateway}, body, nil
}

func readAuthenticatedBody(body io.Reader) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	value, err := io.ReadAll(io.LimitReader(body, maxAuthenticatedBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(value)) > maxAuthenticatedBodyBytes {
		return nil, errAuthenticatedBodyTooLarge
	}
	return value, nil
}

func writeAuthenticationError(w http.ResponseWriter, err error) {
	if errors.Is(err, errAuthenticatedBodyTooLarge) {
		writeAPIError(w, http.StatusRequestEntityTooLarge, govar.ReasonInvalidTransition, err)
		return
	}
	writeAPIError(w, http.StatusUnauthorized, govar.ReasonPrincipalMismatch, err)
}

func tokenReviewFunc(k8s client.Client) func(context.Context, string) (authenticatedPrincipal, error) {
	return func(ctx context.Context, token string) (authenticatedPrincipal, error) {
		if token == "" {
			return authenticatedPrincipal{}, errors.New("projected service-account token is empty")
		}
		review := &authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{Token: token, Audiences: []string{podinjector.GOVARTokenAudience}}}
		if err := k8s.Create(ctx, review); err != nil {
			return authenticatedPrincipal{}, fmt.Errorf("TokenReview failed: %w", err)
		}
		if !review.Status.Authenticated || !slices.Contains(review.Status.Audiences, podinjector.GOVARTokenAudience) {
			return authenticatedPrincipal{}, errors.New("projected token was not authenticated for GOV-AR audience")
		}
		uidValues := review.Status.User.Extra["authentication.kubernetes.io/pod-uid"]
		if len(uidValues) != 1 || strings.TrimSpace(uidValues[0]) == "" {
			return authenticatedPrincipal{}, errors.New("TokenReview lacks bound Pod UID")
		}
		parts := strings.Split(review.Status.User.Username, ":")
		if len(parts) != 4 || parts[0] != "system" || parts[1] != "serviceaccount" {
			return authenticatedPrincipal{}, errors.New("TokenReview user is not a service account")
		}
		podNames := review.Status.User.Extra["authentication.kubernetes.io/pod-name"]
		if len(podNames) != 1 || strings.TrimSpace(podNames[0]) == "" {
			return authenticatedPrincipal{}, errors.New("TokenReview lacks bound Pod name")
		}
		return authenticatedPrincipal{namespace: parts[2], workloadUID: strings.TrimSpace(uidValues[0]), podName: strings.TrimSpace(podNames[0]), serviceAccount: parts[3], role: roleAdmissionOnly}, nil
	}
}

func deriveSignerKey(master []byte, namespace, tenantID, workloadUID string) []byte {
	mac := hmac.New(sha256.New, master)
	_, _ = mac.Write([]byte("govar-identity-v2\x00" + namespace + "\x00" + tenantID + "\x00" + workloadUID))
	return []byte(fmt.Sprintf("%x", mac.Sum(nil)))
}

type trustedWorkload struct {
	namespace, tenant, team, application, budgetPolicy, routingPolicy string
	serviceAccount                                                    string
	uid, resourceVersion                                              string
	sensitive                                                         bool
	requireGateway                                                    bool
	zones                                                             []string
}

func (s *server) resolveTrustedWorkload(ctx context.Context, principal authenticatedPrincipal) (trustedWorkload, error) {
	if principal.podName != "" {
		var pod corev1.Pod
		if err := s.k8s.Get(ctx, client.ObjectKey{Namespace: principal.namespace, Name: principal.podName}, &pod); err != nil {
			return trustedWorkload{}, fmt.Errorf("bound Pod lookup failed: %w", err)
		}
		if string(pod.UID) != principal.workloadUID {
			return trustedWorkload{}, errors.New("TokenReview Pod UID no longer matches bound Pod")
		}
		return s.trustedWorkloadFromPod(ctx, &pod, principal)
	}
	var pods corev1.PodList
	if err := s.k8s.List(ctx, &pods, client.InNamespace(principal.namespace)); err != nil {
		return trustedWorkload{}, fmt.Errorf("list authenticated namespace pods: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if string(pod.UID) != principal.workloadUID {
			continue
		}
		return s.trustedWorkloadFromPod(ctx, pod, principal)
	}
	return trustedWorkload{}, errors.New("signed workload UID does not exist in signed namespace")
}

func (s *server) trustedWorkloadFromPod(ctx context.Context, pod *corev1.Pod, principal authenticatedPrincipal) (trustedWorkload, error) {
	serviceAccount := strings.TrimSpace(pod.Spec.ServiceAccountName)
	if serviceAccount == "" {
		serviceAccount = "default"
	}
	if principal.serviceAccount != "" && principal.serviceAccount != serviceAccount {
		return trustedWorkload{}, errors.New("TokenReview service account no longer matches live Pod")
	}
	var binding aiopsv1alpha1.AIWorkloadBinding
	if err := s.k8s.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: serviceAccount}, &binding); err != nil {
		return trustedWorkload{}, fmt.Errorf("operator-owned AIWorkloadBinding lookup failed: %w", err)
	}
	if binding.Name != binding.Spec.ServiceAccountName || binding.Spec.ServiceAccountName != serviceAccount ||
		binding.UID == "" || !binding.Spec.RequireGateway || binding.Status.ObservedGeneration != binding.Generation || !apimeta.IsStatusConditionTrue(binding.Status.Conditions, aiopsv1alpha1.ConditionReady) {
		return trustedWorkload{}, errors.New("AIWorkloadBinding is not observed and Ready for the live Pod service account")
	}
	var serviceAccountObject corev1.ServiceAccount
	if err := s.k8s.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: serviceAccount}, &serviceAccountObject); err != nil ||
		serviceAccountObject.UID == "" || serviceAccountObject.UID != binding.Status.ResolvedServiceAccountUID {
		return trustedWorkload{}, errors.New("AIWorkloadBinding ServiceAccount UID evidence is stale or missing")
	}
	var budget aiopsv1alpha1.AIBudgetPolicy
	if err := s.k8s.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: binding.Spec.BudgetPolicyRef}, &budget); err != nil ||
		!resolvedReferenceMatches(binding.Status.ResolvedBudgetPolicy, budget.Name, budget.UID, budget.Generation) ||
		budget.Status.ObservedGeneration != budget.Generation || !apimeta.IsStatusConditionTrue(budget.Status.Conditions, aiopsv1alpha1.ConditionReady) {
		return trustedWorkload{}, errors.New("AIWorkloadBinding budget policy identity/readiness evidence is stale or missing")
	}
	var routing aiopsv1alpha1.AIRoutingPolicy
	if err := s.k8s.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: binding.Spec.RoutingPolicyRef}, &routing); err != nil ||
		!resolvedReferenceMatches(binding.Status.ResolvedRoutingPolicy, routing.Name, routing.UID, routing.Generation) ||
		routing.Status.ObservedGeneration != routing.Generation || !apimeta.IsStatusConditionTrue(routing.Status.Conditions, aiopsv1alpha1.ConditionReady) {
		return trustedWorkload{}, errors.New("AIWorkloadBinding routing policy identity/readiness evidence is stale or missing")
	}
	trusted := trustedWorkload{namespace: pod.Namespace, uid: string(pod.UID), resourceVersion: pod.ResourceVersion + "|" + string(binding.UID) + "|" + strconv.FormatInt(binding.Generation, 10) + "|" + binding.ResourceVersion,
		serviceAccount: serviceAccount, tenant: binding.Spec.TenantID, team: binding.Spec.Team, application: binding.Spec.Application,
		budgetPolicy: binding.Spec.BudgetPolicyRef, routingPolicy: binding.Spec.RoutingPolicyRef,
		sensitive: binding.Spec.Sensitivity != aiopsv1alpha1.TierLow, zones: sortedCopy(binding.Spec.AllowedZones), requireGateway: binding.Spec.RequireGateway}
	if err := rejectConflictingPodGovernanceMetadata(pod, trusted); err != nil {
		return trustedWorkload{}, err
	}
	return trusted, nil
}

func resolvedReferenceMatches(reference *aiopsv1alpha1.AIWorkloadBindingResolvedReference, name string, uid types.UID, generation int64) bool {
	return reference != nil && reference.Name == name && reference.UID == uid && reference.Generation == generation
}

func rejectConflictingPodGovernanceMetadata(pod *corev1.Pod, trusted trustedWorkload) error {
	annotations, labels := pod.Annotations, pod.Labels
	claims := []struct{ name, actual, expected string }{
		{podinjector.GOVARTenantKey, annotations[podinjector.GOVARTenantKey], trusted.tenant},
		{podinjector.GOVARBudgetPolicyKey, annotations[podinjector.GOVARBudgetPolicyKey], trusted.budgetPolicy},
		{podinjector.GOVARRoutingKey, annotations[podinjector.GOVARRoutingKey], trusted.routingPolicy},
		{podinjector.ApplicationKey, annotations[podinjector.ApplicationKey], trusted.application},
		{"aiops.imperium.io/team", labels["aiops.imperium.io/team"], trusted.team},
	}
	for _, claim := range claims {
		if actual := strings.TrimSpace(claim.actual); actual != "" && actual != claim.expected {
			return fmt.Errorf("pod-authored %s conflicts with operator-owned AIWorkloadBinding", claim.name)
		}
	}
	if value := strings.TrimSpace(annotations[podinjector.GOVARSensitiveKey]); value != "" && enabledValue(value) != trusted.sensitive {
		return errors.New("pod-authored sensitivity conflicts with operator-owned AIWorkloadBinding")
	}
	if value := strings.TrimSpace(annotations[podinjector.GOVARZonesKey]); value != "" && !slices.Equal(splitAndNormalize(value), sortedCopy(trusted.zones)) {
		return errors.New("pod-authored allowed zones conflict with operator-owned AIWorkloadBinding")
	}
	return nil
}

func enabledValue(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "true" || value == "enabled" || value == "1" || value == "yes"
}

func bindTrustedAdmitRequest(req *govar.AdmitRequest, trusted trustedWorkload) error {
	if req.Namespace != trusted.namespace || req.TenantID != trusted.tenant || req.WorkloadUID != trusted.uid || req.Team != trusted.team || req.Application != trusted.application ||
		req.BudgetPolicyName != trusted.budgetPolicy || req.RoutingPolicyName != trusted.routingPolicy || req.SensitiveData != trusted.sensitive ||
		!slices.Equal(sortedCopy(req.AllowedZones), trusted.zones) {
		return errors.New("admission body conflicts with trusted Pod metadata")
	}
	return nil
}

func (s *server) authorizeEvent(ctx context.Context, principal authenticatedPrincipal, tenantID, workloadUID string) (trustedWorkload, error) {
	trusted, err := s.resolveTrustedWorkload(ctx, principal)
	if err != nil {
		return trustedWorkload{}, err
	}
	if tenantID != trusted.tenant || workloadUID != trusted.uid {
		return trustedWorkload{}, errors.New("event identity conflicts with trusted live Pod metadata")
	}
	return trusted, nil
}

func splitAndNormalize(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.ToLower(strings.TrimSpace(item)); item != "" {
			out = append(out, item)
		}
	}
	slices.Sort(out)
	return out
}
func sortedCopy(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.ToLower(strings.TrimSpace(value)); value != "" {
			out = append(out, value)
		}
	}
	slices.Sort(out)
	return out
}

func extProcPrincipalResolver(srv *server) func(context.Context, string) (govarextproc.RouteBinding, error) {
	return func(ctx context.Context, identity string) (govarextproc.RouteBinding, error) {
		parsed, err := url.Parse(identity)
		if err != nil || parsed.Scheme != "spiffe" || parsed.Host != "govar.local" {
			return govarextproc.RouteBinding{}, errors.New("untrusted ext_proc SPIFFE identity")
		}
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "ns" || parts[2] != "pod" || parts[1] == "" || parts[3] == "" {
			return govarextproc.RouteBinding{}, errors.New("invalid ext_proc SPIFFE workload path")
		}
		trusted, err := srv.resolveTrustedWorkload(ctx, authenticatedPrincipal{namespace: parts[1], workloadUID: parts[3]})
		if err != nil {
			return govarextproc.RouteBinding{}, err
		}
		return govarextproc.RouteBinding{Namespace: trusted.namespace, TenantID: trusted.tenant, WorkloadUID: trusted.uid, Team: trusted.team, Application: trusted.application, BudgetPolicy: trusted.budgetPolicy, RoutingPolicy: trusted.routingPolicy, Sensitive: trusted.sensitive, AllowedZones: trusted.zones}, nil
	}
}

func decodeStrictJSON(body []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}

func writeAPIError(w http.ResponseWriter, status int, code govar.ReasonCode, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"reason_code": code, "error": err.Error()})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
