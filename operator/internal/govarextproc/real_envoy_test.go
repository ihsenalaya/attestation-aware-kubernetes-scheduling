package govarextproc

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// TestRealEnvoyRoutesOnlyToAdmissionSelectedBackend is opt-in because it starts
// a real Envoy container. The release verification invokes it explicitly.
func TestRealEnvoyRoutesOnlyToAdmissionSelectedBackend(t *testing.T) {
	if os.Getenv("GOVAR_REAL_ENVOY") != "1" {
		t.Skip("set GOVAR_REAL_ENVOY=1 to run Docker Envoy integration")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker unavailable")
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl unavailable")
	}
	pullDockerImage(t, realEnvoyImage)

	dir := t.TempDir()
	gatewayURI := "spiffe://govar.local/gateway/envoy"
	workloadURI := "spiffe://govar.local/ns/finance/pod/uid-a"
	makeEnvoyTestPKI(t, dir, gatewayURI, workloadURI)

	var mu sync.Mutex
	var calls []string
	admission := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/v1/admit" {
			snapshot := extProcTestSnapshot("catalog-eu", "provider-eu", "selected_backend", "selected.backend.test")
			_ = json.NewEncoder(w).Encode(map[string]any{"decision": "ADMIT", "reason_code": "highest_utility_feasible", "selected_deployment": "catalog-eu", "provider_attempt_id": "request-real:attempt:1", "pricing_version": snapshot.PricingVersion, "route_snapshot": snapshot})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"reason_code": "ok"})
	}))
	defer admission.Close()

	type observed struct{ host, path, model string }
	observedRequest := make(chan observed, 1)
	backend := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		observedRequest <- observed{host: r.Host, path: r.URL.RequestURI(), model: fmt.Sprint(payload["model"])}
		_ = json.NewEncoder(w).Encode(map[string]any{"usage": map[string]any{"prompt_tokens": 4, "completion_tokens": 7}})
	}))
	backendListener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	backend.Listener = backendListener
	backend.Start()
	defer backend.Close()

	extListener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	serverCredentials := loadServerMTLS(t, filepath.Join(dir, "extproc-server.crt"), filepath.Join(dir, "extproc-server.key"), filepath.Join(dir, "ca.crt"))
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials))
	extprocv3.RegisterExternalProcessorServer(grpcServer, &Server{AdmissionURL: admission.URL, MasterSecret: []byte("0123456789abcdef0123456789abcdef"),
		PrincipalRegistry:  map[string]RouteBinding{workloadURI: {Namespace: "finance", TenantID: "tenant-a", WorkloadUID: "uid-a", Team: "treasury", Application: "assistant", BudgetPolicy: "budget", RoutingPolicy: "routing"}},
		AllowedGatewayURIs: map[string]struct{}{gatewayURI: {}},
	})
	go func() { _ = grpcServer.Serve(extListener) }()
	defer grpcServer.Stop()

	backendPort := mustURLPort(t, backend.URL)
	extProcPort := extListener.Addr().(*net.TCPAddr).Port
	hostPort := reservePort(t)
	hostAddress := outboundHostAddress(t)
	config := fmt.Sprintf(realEnvoyConfig, hostAddress, extProcPort, hostAddress, backendPort)
	configPath := filepath.Join(dir, "envoy.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}
	name := "govar-real-envoy-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var logs bytes.Buffer
	runContext, cancelRun := context.WithCancel(context.Background())
	command := exec.CommandContext(runContext, "docker", "run", "--rm", "--name", name, "-p", fmt.Sprintf("127.0.0.1:%d:10000", hostPort), "-v", dir+":/certs:ro", "-v", configPath+":/etc/envoy/envoy.yaml:ro", realEnvoyImage, "-c", "/etc/envoy/envoy.yaml", "--log-level", "error")
	command.Stdout, command.Stderr = &logs, &logs
	if err := command.Start(); err != nil {
		cancelRun()
		t.Fatal(err)
	}
	defer func() {
		cancelRun()
		_ = exec.Command("docker", "rm", "-f", name).Run()
		_ = command.Wait()
	}()

	client := downstreamMTLSClient(t, dir)
	var response *http.Response
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("https://127.0.0.1:%d/v1/responses?trace=1", hostPort), strings.NewReader(`{"model":"client-choice","input":"public","max_output_tokens":32}`))
		req.Host = "gateway.local"
		req.Header.Set("x-request-id", "request-real")
		response, err = client.Do(req)
		if err == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("real Envoy request failed: %v\n%s", err, logs.String())
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		mu.Lock()
		observedCalls := append([]string(nil), calls...)
		mu.Unlock()
		t.Fatalf("status=%d body=%s calls=%v\n%s", response.StatusCode, responseBody, observedCalls, logs.String())
	}
	select {
	case got := <-observedRequest:
		if got.host != "selected.backend.test" || got.path != "/v1/responses?trace=1" || got.model != "provider-eu" {
			t.Fatalf("selected backend observed %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("selected backend did not receive request")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"/v1/admit", "/v1/dispatch", "/v1/dispatch", "/v1/settle"}
	if strings.Join(calls, ",") != strings.Join(want, ",") {
		t.Fatalf("ledger calls=%v", calls)
	}
}

func pullDockerImage(t *testing.T, image string) {
	t.Helper()
	if err := exec.Command("docker", "image", "inspect", image).Run(); err == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "pull", image)
	output, err := command.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("docker pull timed out for %s after 8m: %s", image, output)
	}
	if err != nil {
		t.Fatalf("docker pull %s: %v: %s", image, err, output)
	}
}

func makeEnvoyTestPKI(t *testing.T, dir, gatewayURI, workloadURI string) {
	t.Helper()
	run := func(args ...string) {
		command := exec.Command(args[0], args[1:]...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%v: %v: %s", args, err, output)
		}
	}
	run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=govar-test-ca", "-keyout", filepath.Join(dir, "ca.key"), "-out", filepath.Join(dir, "ca.crt"))
	issue := func(name, commonName, altName, usage string) {
		run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN="+commonName, "-keyout", filepath.Join(dir, name+".key"), "-out", filepath.Join(dir, name+".csr"))
		ext := filepath.Join(dir, name+".ext")
		if err := os.WriteFile(ext, []byte("subjectAltName="+altName+"\nextendedKeyUsage="+usage+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		run("openssl", "x509", "-req", "-in", filepath.Join(dir, name+".csr"), "-CA", filepath.Join(dir, "ca.crt"), "-CAkey", filepath.Join(dir, "ca.key"), "-CAcreateserial", "-days", "1", "-extfile", ext, "-out", filepath.Join(dir, name+".crt"))
		if err := os.Chmod(filepath.Join(dir, name+".key"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	issue("gateway-server", "gateway.local", "DNS:gateway.local,URI:"+gatewayURI, "serverAuth")
	issue("workload-client", "workload", "URI:"+workloadURI, "clientAuth")
	issue("extproc-server", "host.docker.internal", "DNS:host.docker.internal", "serverAuth")
	issue("gateway-extproc-client", "gateway", "URI:"+gatewayURI, "clientAuth")
}

func loadServerMTLS(t *testing.T, certFile, keyFile, caFile string) credentials.TransportCredentials {
	t.Helper()
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid CA")
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12})
}

func downstreamMTLSClient(t *testing.T, dir string) *http.Client {
	t.Helper()
	certificate, err := tls.LoadX509KeyPair(filepath.Join(dir, "workload-client.crt"), filepath.Join(dir, "workload-client.key"))
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	return &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: pool, ServerName: "gateway.local", MinVersion: tls.VersionTLS12}}}
}

func mustURLPort(t *testing.T, raw string) int {
	t.Helper()
	_, port, err := net.SplitHostPort(strings.TrimPrefix(raw, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := strconv.Atoi(port)
	return value
}
func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func outboundHostAddress(t *testing.T) string {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagUp == 0 || networkInterface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, addressErr := networkInterface.Addrs()
		if addressErr != nil {
			continue
		}
		for _, address := range addresses {
			ip, _, parseErr := net.ParseCIDR(address.String())
			if parseErr == nil && ip.To4() != nil && !ip.IsLinkLocalUnicast() {
				return ip.String()
			}
		}
	}
	t.Fatal("no non-loopback IPv4 address reaches the local Docker bridge")
	return ""
}

const realEnvoyImage = "envoyproxy/envoy@sha256:caa5b411be1633b90023592a34a7e010c933d6e60206c758f631485e53006865" // Envoy 1.31.10

const realEnvoyConfig = `
static_resources:
  listeners:
  - name: gateway
    address: { socket_address: { address: 0.0.0.0, port_value: 10000 } }
    filter_chains:
    - transport_socket:
        name: envoy.transport_sockets.tls
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
          require_client_certificate: true
          common_tls_context:
            tls_certificates:
            - certificate_chain: { filename: /certs/gateway-server.crt }
              private_key: { filename: /certs/gateway-server.key }
            validation_context: { trusted_ca: { filename: /certs/ca.crt } }
      filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: govar_real
          internal_address_config: {}
          forward_client_cert_details: SANITIZE_SET
          set_current_client_cert_details: { uri: true }
          route_config:
            name: route
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster_header: x-govar-upstream-cluster, host_rewrite_header: x-govar-upstream-authority }
          http_filters:
          - name: envoy.filters.http.ext_proc
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
              failure_mode_allow: false
              mutation_rules:
                allow_expression: { regex: "^(:path|content-length|x-ai-eg-model|x-govar-upstream-cluster|x-govar-upstream-authority)$" }
                disallow_is_error: true
              grpc_service: { envoy_grpc: { cluster_name: extproc } }
              processing_mode: { request_header_mode: SEND, response_header_mode: SEND, request_body_mode: BUFFERED, response_body_mode: BUFFERED }
          - name: envoy.filters.http.router
            typed_config: { "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router }
  clusters:
  - name: extproc
    type: STRICT_DNS
    transport_socket:
      name: envoy.transport_sockets.tls
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
        sni: host.docker.internal
        common_tls_context:
          tls_certificates:
          - certificate_chain: { filename: /certs/gateway-extproc-client.crt }
            private_key: { filename: /certs/gateway-extproc-client.key }
          validation_context: { trusted_ca: { filename: /certs/ca.crt } }
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config: { http2_protocol_options: {} }
    load_assignment:
      cluster_name: extproc
      endpoints: [{ lb_endpoints: [{ endpoint: { address: { socket_address: { address: %s, port_value: %d } } } }] }]
  - name: selected_backend
    type: STRICT_DNS
    load_assignment:
      cluster_name: selected_backend
      endpoints: [{ lb_endpoints: [{ endpoint: { address: { socket_address: { address: %s, port_value: %d } } } }] }]
`
