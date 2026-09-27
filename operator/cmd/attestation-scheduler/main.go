// attestation-scheduler is the AI-sovereign attestation-aware custom scheduler.
// It runs as a second scheduler alongside the default Kubernetes scheduler,
// handling only pods with schedulerName: ai-attestation-scheduler.
//
// The scheduler filters nodes by valid AttestationEvidence matching the pod's
// ConfidentialInferencePolicy, scores nodes by evidence freshness and TEE type,
// mints an Ed25519 placement token, creates an AIPlacementDecision, and binds
// the pod via the Kubernetes Binding API.
//
// SIMULATED mode (kind): evidence objects are accepted with simulated=true.
// This is explicitly visible in the AIPlacementDecision status and in logs.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/scheduler"
)

const schedulerName = "ai-attestation-scheduler"

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aiopsv1alpha1.AddToScheme(scheme))
}

func main() {
	log.SetLogger(zap.New())
	logger := log.Log.WithName("attestation-scheduler")
	logger.Info("starting attestation-scheduler",
		"schedulerName", schedulerName,
		"note", "handles only pods with schedulerName=ai-attestation-scheduler",
	)

	cfg, err := ctrl.GetConfig()
	if err != nil {
		logger.Error(err, "get kubeconfig")
		os.Exit(1)
	}

	// Build the signing key (from env or auto-generate for kind)
	privKey, pubKey, err := loadOrGenerateSigningKey()
	if err != nil {
		logger.Error(err, "load signing key")
		os.Exit(1)
	}
	logger.Info("signing key ready", "pubKey", hex.EncodeToString(pubKey))

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		logger.Error(err, "create client")
		os.Exit(1)
	}

	tokenTTL := 5 * time.Minute
	if v := os.Getenv("SCHEDULER_TOKEN_TTL_SECONDS"); v != "" {
		var secs int
		if _, err := fmt.Sscan(v, &secs); err == nil && secs > 0 {
			tokenTTL = time.Duration(secs) * time.Second
		}
	}

	sched := scheduler.New(c, privKey, pubKey, tokenTTL)
	if v := os.Getenv("AIOPS_PREBIND_TEST_DELAY_MS"); v != "" {
		var ms int
		if _, err := fmt.Sscan(v, &ms); err == nil && ms > 0 {
			delay := time.Duration(ms) * time.Millisecond
			sched.SetPreBindDelay(delay)
			logger.Info("enabled prebind test delay", "delay", delay.String())
		}
	}

	// Watch pending pods assigned to this scheduler
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Pod{}: {
					Field: fields.SelectorFromSet(fields.Set{
						"spec.schedulerName": schedulerName,
						"status.phase":       string(corev1.PodPending),
					}),
				},
			},
		},
		// No leader election needed for a scheduler — only one replica should run.
		LeaderElection:         false,
		HealthProbeBindAddress: ":8087",
		Metrics:                metricsserver.Options{BindAddress: ":8088"},
	})
	if err != nil {
		logger.Error(err, "create manager")
		os.Exit(1)
	}

	if err := (&podScheduler{sched: sched}).SetupWithManager(mgr); err != nil {
		logger.Error(err, "setup pod scheduler controller")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Error(err, "add healthz check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Error(err, "add readyz check")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("attestation-scheduler running")
	if err := mgr.Start(ctx); err != nil {
		logger.Error(err, "manager exited")
		os.Exit(1)
	}
}

// loadOrGenerateSigningKey loads the Ed25519 private key from SCHEDULER_SIGNING_KEY_HEX
// or generates a new ephemeral key for kind/dev usage.
func loadOrGenerateSigningKey() (ed25519.PrivateKey, ed25519.PublicKey, error) {
	logger := log.Log.WithName("attestation-scheduler")
	if keyHex := os.Getenv("SCHEDULER_SIGNING_KEY_HEX"); keyHex != "" {
		seed, err := hex.DecodeString(keyHex)
		if err != nil {
			return nil, nil, fmt.Errorf("SCHEDULER_SIGNING_KEY_HEX is not valid hex: %w", err)
		}
		if len(seed) != ed25519.SeedSize {
			return nil, nil, fmt.Errorf("SCHEDULER_SIGNING_KEY_HEX seed must be %d bytes", ed25519.SeedSize)
		}
		priv := ed25519.NewKeyFromSeed(seed)
		pub := priv.Public().(ed25519.PublicKey)
		logger.Info("loaded signing key from env SCHEDULER_SIGNING_KEY_HEX")
		return priv, pub, nil
	}

	// Ephemeral key for kind/dev — warn clearly
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("generate ephemeral signing key: %w", err)
	}
	logger.Info("WARNING: using ephemeral signing key — tokens will not survive restarts; set SCHEDULER_SIGNING_KEY_HEX for persistence",
		"mode", "simulated-kind",
	)
	return priv, pub, nil
}
