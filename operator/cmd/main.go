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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/controller"
	"github.com/imperium/ai-sovereign-finops-operator/internal/qualityeval"
	"github.com/imperium/ai-sovereign-finops-operator/internal/webhook/bootstrap"
	"github.com/imperium/ai-sovereign-finops-operator/internal/webhook/podinjector"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=create;get;list;patch;update;watch
//+kubebuilder:rbac:groups=node.k8s.io,resources=runtimeclasses,verbs=create;get;list;patch;update;watch

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(aiopsv1alpha1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "quality-eval" {
		if err := runQualityEval(os.Args[2:]); err != nil {
			log.Printf("quality-eval failed: %v", err)
			os.Exit(1)
		}
		return
	}

	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", false,
		"If set the metrics endpoint is served securely")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancelation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}
	webhookCertDir := filepath.Join(os.TempDir(), "k8s-webhook-server", "serving-certs")
	cfg := ctrl.GetConfigOrDie()

	webhookServer := webhook.NewServer(webhook.Options{
		CertDir: webhookCertDir,
		TLSOpts: tlsOpts,
	})

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   metricsAddr,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOpts,
		},
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "2d92db7d.imperium.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}
	bootstrapClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "unable to create bootstrap client")
		os.Exit(1)
	}

	if err = (&controller.AIGatewayReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aigateway-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIGateway")
		os.Exit(1)
	}
	if err = (&controller.AIProviderReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aiprovider-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIProvider")
		os.Exit(1)
	}
	if err = (&controller.AIModelReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aimodel-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIModel")
		os.Exit(1)
	}
	if err = (&controller.AIBudgetPolicyReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aibudgetpolicy-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIBudgetPolicy")
		os.Exit(1)
	}
	if err = (&controller.AISovereigntyPolicyReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aisovereigntypolicy-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AISovereigntyPolicy")
		os.Exit(1)
	}
	if err = (&controller.AIBreakEvenAnalysisReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aibreakevenanalysis-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIBreakEvenAnalysis")
		os.Exit(1)
	}
	if err = (&controller.AIFinOpsReportReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aifinopsreport-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIFinOpsReport")
		os.Exit(1)
	}
	if err = (&controller.AIQualityGateReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aiqualitygate-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIQualityGate")
		os.Exit(1)
	}
	if err = (&controller.AIRouteOverrideReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("airouteoverride-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIRouteOverride")
		os.Exit(1)
	}
	if err = (&controller.AIRoutingPolicyReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("airoutingpolicy-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIRoutingPolicy")
		os.Exit(1)
	}
	if err = (&controller.AIWorkloadBindingReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIWorkloadBinding")
		os.Exit(1)
	}
	if err = (&controller.AIChangeRequestReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aichangerequest-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIChangeRequest")
		os.Exit(1)
	}
	if envBool("AIOPS_ENABLE_LEGACY_EVIDENCE_RECONCILER", false) {
		if err = (&controller.AttestationEvidenceReconciler{
			Client:   mgr.GetClient(),
			Scheme:   mgr.GetScheme(),
			Recorder: mgr.GetEventRecorderFor("attestationevidence-controller"),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "AttestationEvidence")
			os.Exit(1)
		}
	} else {
		setupLog.Info("legacy AttestationEvidence reconciler disabled; central-verifier owns evidence status")
	}
	if err = (&controller.AIRevocationPolicyReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("airevocationpolicy-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIRevocationPolicy")
		os.Exit(1)
	}
	if envBool("AIOPS_ENABLE_EMBEDDED_VERIFIER", false) {
		// Compatibility mode only. The Helm deployment uses the dedicated
		// central-verifier binary so RBAC can prove single-writer ownership.
		if err = (&controller.RawAttestationReportReconciler{
			Client:           mgr.GetClient(),
			Scheme:           mgr.GetScheme(),
			VerifierIdentity: "embedded-central-verifier",
			VerifierPodUID:   os.Getenv("POD_UID"),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "RawAttestationReport")
			os.Exit(1)
		}
	} else {
		setupLog.Info("embedded RawAttestationReport verifier disabled; dedicated central-verifier owns evidence writes")
	}
	if err = (&controller.AIEvidenceRecordReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aievidencerecord-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIEvidenceRecord")
		os.Exit(1)
	}
	if err = (&controller.AIPlacementDecisionReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aiplacementdecision-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIPlacementDecision")
		os.Exit(1)
	}
	if err = (&controller.AIKeyReleasePolicyReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("aikeyreleasepolicy-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AIKeyReleasePolicy")
		os.Exit(1)
	}
	//+kubebuilder:scaffold:builder

	mgr.GetWebhookServer().Register("/mutate-v1-pod", &admission.Webhook{
		Handler: podinjector.New(mgr.GetAPIReader(), mgr.GetScheme(), &podinjector.ManagerPodImageResolver{
			Client:       mgr.GetAPIReader(),
			PodName:      os.Getenv("POD_NAME"),
			PodNamespace: os.Getenv("POD_NAMESPACE"),
		}),
	})
	mgr.GetWebhookServer().Register("/validate-v1-pod", &admission.Webhook{
		Handler: podinjector.NewValidation(mgr.GetAPIReader(), mgr.GetScheme()),
	})

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	if err := bootstrap.EnsureSimulatedRuntimeClasses(ctx, bootstrapClient); err != nil {
		setupLog.Error(err, "unable to bootstrap simulated runtime classes")
		os.Exit(1)
	}
	if err := bootstrap.Ensure(ctx, bootstrap.Options{
		Client:           bootstrapClient,
		Name:             "aiops-sidecar-injector",
		ServiceName:      os.Getenv("WEBHOOK_SERVICE_NAME"),
		ServiceNamespace: os.Getenv("POD_NAMESPACE"),
		Path:             "/mutate-v1-pod",
		CertDir:          webhookCertDir,
	}); err != nil {
		setupLog.Error(err, "unable to bootstrap mutating webhook")
		os.Exit(1)
	}
	if err := bootstrap.EnsureValidation(ctx, bootstrap.Options{
		Client:           bootstrapClient,
		Name:             "aiops-confidential-pod-validator",
		ServiceName:      os.Getenv("WEBHOOK_SERVICE_NAME"),
		ServiceNamespace: os.Getenv("POD_NAMESPACE"),
		Path:             "/validate-v1-pod",
		CertDir:          webhookCertDir,
	}); err != nil {
		setupLog.Error(err, "unable to bootstrap validating webhook")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func runQualityEval(args []string) error {
	fs := flag.NewFlagSet("quality-eval", flag.ContinueOnError)
	var opts qualityeval.Options
	var timeoutSeconds int
	var terminationLog string
	fs.StringVar(&opts.Endpoint, "endpoint", "", "OpenAI-compatible gateway chat completions endpoint")
	fs.StringVar(&opts.PromptsDir, "prompts-dir", "", "directory containing prompts.yaml/prompts.json")
	fs.StringVar(&opts.PromptsFile, "prompts-file", "", "explicit golden dataset file")
	fs.StringVar(&opts.Namespace, "namespace", "", "application namespace attribution header")
	fs.StringVar(&opts.Application, "application", "", "application attribution header")
	fs.StringVar(&opts.SourceModel, "source-model", "", "source model name")
	fs.StringVar(&opts.CandidateModel, "candidate-model", "", "candidate model name")
	fs.IntVar(&opts.MaxTokens, "max-tokens", 96, "max completion tokens per prompt")
	fs.IntVar(&timeoutSeconds, "timeout-seconds", 60, "HTTP timeout per gateway request")
	fs.StringVar(&terminationLog, "termination-log", "/dev/termination-log", "path for Kubernetes termination message evidence")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts.Timeout = time.Duration(timeoutSeconds) * time.Second
	raw, err := qualityeval.Run(context.Background(), opts)
	if err != nil {
		if terminationLog != "" {
			_ = writeTerminationError(terminationLog, err)
		}
		return err
	}
	if terminationLog != "" {
		if err := writeTerminationMessage(terminationLog, raw); err != nil {
			return err
		}
	}
	log.Printf("quality-eval completed: wrote %d bytes of evidence", len(raw))
	return nil
}

func writeTerminationMessage(path string, raw []byte) error {
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("write termination log %s: %w", path, err)
	}
	return nil
}

func writeTerminationError(path string, cause error) error {
	const maxTerminationLogBytes = 4096
	raw := []byte("ERROR: " + cause.Error())
	if len(raw) > maxTerminationLogBytes {
		raw = raw[:maxTerminationLogBytes]
	}
	return writeTerminationMessage(path, raw)
}

func envBool(key string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}
