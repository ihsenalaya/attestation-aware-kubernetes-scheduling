package main

import (
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/controller"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aiopsv1alpha1.AddToScheme(scheme))
}

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	logger := ctrl.Log.WithName("central-verifier")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: ":8090"},
		HealthProbeBindAddress: ":8091",
		LeaderElection:         false,
	})
	if err != nil {
		logger.Error(err, "create manager")
		os.Exit(1)
	}

	reconciler := &controller.RawAttestationReportReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		VerifierIdentity: envOrDefault("VERIFIER_IDENTITY", "central-verifier"),
		VerifierPodUID:   os.Getenv("POD_UID"),
		ExpectedTEE:      envOrDefault("EXPECTED_TEE", "sevsnpvm"),
		MAAIssuerPrefix:  os.Getenv("MAA_ISSUER_PREFIX"),
		MaxTokenAge:      durationFromEnv("MAX_TOKEN_AGE_SECONDS", 5*time.Minute),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		logger.Error(err, "setup RawAttestationReport controller")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Error(err, "add healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Error(err, "add readyz")
		os.Exit(1)
	}

	logger.Info("central verifier running")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error(err, "manager exited")
		os.Exit(1)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		var seconds int
		if _, err := fmt.Sscan(v, &seconds); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return fallback
}
