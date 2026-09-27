package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aiopsv1alpha1.AddToScheme(scheme))
}

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	logger := ctrl.Log.WithName("node-attestation-agent")

	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		logger.Error(err, "create kubernetes client")
		os.Exit(1)
	}

	interval := durationFromEnv("REPORT_INTERVAL_SECONDS", 60*time.Second)
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}

	ctx := ctrl.SetupSignalHandler()
	if err := createReport(ctx, c); err != nil {
		logger.Error(err, "create initial raw attestation report")
		os.Exit(1)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	logger.Info("node attestation agent running", "interval", interval.String())
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := createReport(ctx, c); err != nil {
				logger.Error(err, "create raw attestation report")
			}
		}
	}
}

func createReport(ctx context.Context, c client.Client) error {
	nodeName := envOrDefault("NODE_NAME", "unknown-node")
	simulated := boolFromEnv("ATTESTATION_SIMULATED", true)
	provider := strings.TrimSpace(os.Getenv("ATTESTATION_PROVIDER"))
	if provider == "" {
		if simulated {
			provider = "simulator"
		} else {
			provider = "maa"
		}
	}
	if provider != "maa" && provider != "simulator" {
		return fmt.Errorf("invalid ATTESTATION_PROVIDER %q", provider)
	}

	nonce, err := newNonce()
	if err != nil {
		return err
	}
	rawToken, err := readToken(ctx, nonce)
	if err != nil {
		return err
	}
	if simulated && rawToken == "" {
		rawToken = "simulated:" + nodeName + ":" + nonce
	}

	now := metav1.NewTime(time.Now().UTC())
	report := &aiopsv1alpha1.RawAttestationReport{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    envOrDefault("POD_NAMESPACE", "default"),
			GenerateName: "raw-" + safeName(nodeName) + "-",
			Labels: map[string]string{
				"ai.sovereign.io/node":      safeName(nodeName),
				"ai.sovereign.io/collector": "node-attestation-agent",
			},
		},
		Spec: aiopsv1alpha1.RawAttestationReportSpec{
			NodeName:            nodeName,
			NodeUID:             os.Getenv("NODE_UID"),
			Provider:            provider,
			RawToken:            rawToken,
			RawTokenHash:        tokenHash(rawToken),
			Nonce:               nonce,
			CollectedAt:         now,
			AgentPodUID:         os.Getenv("POD_UID"),
			AgentServiceAccount: envOrDefault("SERVICE_ACCOUNT_NAME", "node-attestation-agent"),
			Simulated:           simulated,
		},
	}
	return c.Create(ctx, report)
}

func readToken(ctx context.Context, nonce string) (string, error) {
	if path := strings.TrimSpace(os.Getenv("ATTESTATION_TOKEN_FILE")); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read ATTESTATION_TOKEN_FILE: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	if helper := strings.TrimSpace(os.Getenv("ATTESTATION_HELPER_PATH")); helper != "" {
		token, err := runAttestationHelper(ctx, helper, nonce)
		if err != nil {
			return "", err
		}
		return token, nil
	}
	if endpoint := strings.TrimSpace(os.Getenv("ATTESTATION_TOKEN_ENDPOINT")); endpoint != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return "", err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("fetch attestation token: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return "", fmt.Errorf("fetch attestation token: HTTP %d", resp.StatusCode)
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	return strings.TrimSpace(os.Getenv("ATTESTATION_TOKEN")), nil
}

func runAttestationHelper(ctx context.Context, helperPath, nonce string) (string, error) {
	timeout := durationFromEnv("ATTESTATION_HELPER_TIMEOUT_SECONDS", 120*time.Second)
	if timeout < 5*time.Second {
		timeout = 5 * time.Second
	}
	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	endpoint := envOrDefault("AZURE_ATTESTATION_ENDPOINT", "https://sharedwus2.wus2.attest.azure.net")
	payload, err := attestationClientPayload(nonce)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(hctx, helperPath, endpoint, payload)
	cmd.Env = append(os.Environ(),
		"AZURE_ATTESTATION_ENDPOINT="+endpoint,
		"AZURE_ATTESTATION_CLIENT_PAYLOAD="+payload,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if hctx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("attestation helper timed out after %s", timeout)
	}
	if err != nil {
		return "", fmt.Errorf("attestation helper failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	token := strings.TrimSpace(stdout.String())
	if token == "" {
		return "", fmt.Errorf("attestation helper returned empty token")
	}
	if strings.Count(token, ".") != 2 {
		return "", fmt.Errorf("attestation helper returned non-JWT output")
	}
	return token, nil
}

func attestationClientPayload(nonce string) (string, error) {
	payload := map[string]string{
		"nonce": nonce,
		"Nonce": nonce,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal attestation client payload: %w", err)
	}
	return string(b), nil
}

func newNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func tokenHash(token string) string {
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func safeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "unknown-node"
	}
	if len(out) > 48 {
		out = strings.Trim(out[:48], "-")
	}
	return out
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func boolFromEnv(key string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		var seconds int64
		if _, err := fmt.Sscan(v, &seconds); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return fallback
}
