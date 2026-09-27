package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/controller"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
)

type options struct {
	databaseURL string
	softwareSHA string
	tenantID    string
	namespace   string
	policyName  string
	interval    time.Duration
	queryLimit  int
	once        bool
}

func main() {
	opts, err := parseOptions()
	if err != nil {
		slog.Error("invalid calibration producer configuration", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	producer, err := govar.OpenPostgresCalibrationProducer(ctx, opts.databaseURL, opts.softwareSHA)
	if err != nil {
		slog.Error("open least-privilege calibration producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()
	scheme := runtime.NewScheme()
	if err := aiopsv1alpha1.AddToScheme(scheme); err != nil {
		slog.Error("register API scheme", "error", err)
		os.Exit(1)
	}
	kubeConfig, err := config.GetConfig()
	if err != nil {
		slog.Error("load Kubernetes configuration", "error", err)
		os.Exit(1)
	}
	kubeClient, err := client.New(kubeConfig, client.Options{Scheme: scheme})
	if err != nil {
		slog.Error("create least-privilege Kubernetes client", "error", err)
		os.Exit(1)
	}
	runner := &producerRunner{producer: producer, client: kubeClient, options: opts}
	if opts.once {
		if err := runner.runOnce(ctx); err != nil {
			slog.Error("calibration producer pass failed", "error", err)
			os.Exit(1)
		}
		return
	}
	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()
	for {
		if err := runner.runOnce(ctx); err != nil {
			slog.Error("calibration producer pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type producerRunner struct {
	producer *govar.PostgresCalibrationProducer
	client   client.Client
	options  options
}

func (r *producerRunner) runOnce(ctx context.Context) error {
	var policy aiopsv1alpha1.AIRoutingPolicy
	key := types.NamespacedName{Namespace: r.options.namespace, Name: r.options.policyName}
	if err := r.client.Get(ctx, key, &policy); err != nil {
		return err
	}
	if policy.Spec.GOVAR == nil || policy.Spec.GOVAR.Calibration == nil {
		return errors.New("policy has no typed GOV-AR calibration")
	}
	cal := policy.Spec.GOVAR.Calibration
	if cal.RegistryID == "" {
		return errors.New("policy calibration registryID is required; ConfigMap evidence is forbidden")
	}
	pending, err := r.producer.PendingAuthoritativeRequests(ctx, r.options.tenantID, cal.RegistryID, r.options.queryLimit)
	if err != nil {
		return err
	}
	for _, requestID := range pending {
		if _, err := r.producer.MaterializeAuthoritativeObservation(ctx, requestID); err != nil {
			return fmt.Errorf("materialize request %s: %w", requestID, err)
		}
	}
	calibrationRegimes, support, err := r.producer.RegimesForSplit(ctx, r.options.tenantID, cal.RegistryID, govarcalibration.SplitCalibration)
	if err != nil {
		return err
	}
	if support < cal.MinimumSupport {
		return fmt.Errorf("calibration support %d below frozen minimum %d", support, cal.MinimumSupport)
	}
	artifact, err := r.producer.BuildArtifact(ctx, r.options.tenantID, cal.RegistryID, govarcalibration.AuthoritativeBuildConfig{
		ArtifactRef: cal.ArtifactRef, Version: cal.Version, FeatureSchemaVersion: cal.FeatureSchemaVersion,
		CoverageTargetPPB: cal.CoverageTargetPPB, MinimumSupport: cal.MinimumSupport, Regimes: calibrationRegimes,
	})
	if err != nil {
		return err
	}
	monitoringRegimes, monitoringSupport, err := r.producer.RegimesForSplit(ctx, r.options.tenantID, cal.RegistryID, govarcalibration.SplitMonitoring)
	if err != nil {
		return err
	}
	if monitoringSupport < policy.Spec.GOVAR.Drift.RevalidationMinimumSupport {
		return fmt.Errorf("monitoring support %d below frozen minimum %d", monitoringSupport, policy.Spec.GOVAR.Drift.RevalidationMinimumSupport)
	}
	drift, err := r.producer.BuildDriftWindow(ctx, r.options.tenantID, cal.RegistryID, artifact.ArtifactSHA256,
		policy.Spec.GOVAR.Drift.ThresholdPPB, monitoringRegimes)
	if err != nil {
		return err
	}
	if policy.Status.GOVAR != nil && policy.Status.GOVAR.Calibration != nil && policy.Status.GOVAR.Drift != nil &&
		policy.Status.GOVAR.Calibration.EvidenceSource == "postgresql-v8" &&
		policy.Status.GOVAR.Calibration.ArtifactSHA256 == artifact.ArtifactSHA256 &&
		policy.Status.GOVAR.Drift.DriftSHA256 == drift.DriftSHA256 && policy.Status.ObservedGeneration == policy.Generation {
		return nil
	}
	publisher := controller.GOVARCalibrationStatusPublisher{Client: r.client, Producer: r.producer}
	return publisher.Publish(ctx, controller.CalibrationStatusPublicationRequest{TenantID: r.options.tenantID,
		Policy: key, PolicyUID: policy.UID, PolicyGeneration: policy.Generation,
		ExpectedResourceVersion: policy.ResourceVersion, Artifact: artifact, Drift: &drift})
}

func parseOptions() (options, error) {
	var opts options
	flag.StringVar(&opts.databaseURL, "database-url", os.Getenv("GOVAR_DATABASE_URL"), "PostgreSQL URL from a Secret")
	flag.StringVar(&opts.softwareSHA, "software-sha256", os.Getenv("GOVAR_SOFTWARE_SHA256"), "immutable producer software SHA-256")
	flag.StringVar(&opts.tenantID, "tenant-id", os.Getenv("GOVAR_CALIBRATION_TENANT_ID"), "tenant identity")
	flag.StringVar(&opts.namespace, "policy-namespace", os.Getenv("GOVAR_CALIBRATION_POLICY_NAMESPACE"), "AIRoutingPolicy namespace")
	flag.StringVar(&opts.policyName, "policy-name", os.Getenv("GOVAR_CALIBRATION_POLICY_NAME"), "AIRoutingPolicy name")
	flag.DurationVar(&opts.interval, "interval", envDuration("GOVAR_CALIBRATION_INTERVAL", time.Minute), "bounded producer interval")
	flag.IntVar(&opts.queryLimit, "query-limit", envInt("GOVAR_CALIBRATION_QUERY_LIMIT", 1000), "maximum rows per pass")
	flag.BoolVar(&opts.once, "once", false, "run one deterministic producer pass")
	flag.Parse()
	if opts.databaseURL == "" || opts.softwareSHA == "" || opts.tenantID == "" || opts.namespace == "" || opts.policyName == "" {
		return opts, errors.New("database URL, software SHA, tenant, policy namespace, and policy name are required")
	}
	if opts.interval < time.Second || opts.interval > time.Hour || opts.queryLimit < 1 || opts.queryLimit > 10_000 {
		return opts, errors.New("interval or query limit is outside the bounded range")
	}
	return opts, nil
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if value := os.Getenv(name); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func envInt(name string, fallback int) int {
	if value := os.Getenv(name); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}
