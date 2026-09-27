package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
)

// GOVARCalibrationStatusPublisher is deliberately separate from the general
// routing controller. Its Kubernetes identity needs only get plus status/update
// on AIRoutingPolicy; it cannot read ConfigMaps or mutate policy spec. The
// PostgreSQL producer separately assumes govar_calibration_producer.
type GOVARCalibrationStatusPublisher struct {
	Client   client.Client
	Producer CalibrationPublicationStore
	Now      func() time.Time
}

// CalibrationPublicationStore is the narrow database capability required by
// the status publisher. PostgresCalibrationProducer implements it; keeping the
// interface narrow makes the Kubernetes CAS path independently testable.
type CalibrationPublicationStore interface {
	SoftwareSHA256() string
	PublishPolicyEvidence(context.Context, govar.PolicyEvidencePublication) error
}

type CalibrationStatusPublicationRequest struct {
	TenantID                string
	Policy                  types.NamespacedName
	PolicyUID               types.UID
	PolicyGeneration        int64
	ExpectedResourceVersion string
	Artifact                govarcalibration.Artifact
	Drift                   *govarcalibration.AuthoritativeDriftWindow
}

func (p *GOVARCalibrationStatusPublisher) Publish(ctx context.Context, request CalibrationStatusPublicationRequest) error {
	if p.Client == nil || p.Producer == nil || request.PolicyUID == "" || request.PolicyGeneration <= 0 || request.ExpectedResourceVersion == "" {
		return errors.New("client, producer, and exact policy CAS identity are required")
	}
	var policy aiopsv1alpha1.AIRoutingPolicy
	if err := p.Client.Get(ctx, request.Policy, &policy); err != nil {
		return err
	}
	if policy.UID != request.PolicyUID || policy.Generation != request.PolicyGeneration || policy.ResourceVersion != request.ExpectedResourceVersion {
		return apierrors.NewConflict(aiopsv1alpha1.GroupVersion.WithResource("airoutingpolicies").GroupResource(), request.Policy.String(), errors.New("stale policy identity or resourceVersion"))
	}
	if policy.Spec.GOVAR == nil || policy.Spec.GOVAR.Calibration == nil {
		return errors.New("policy has no typed calibration specification")
	}
	spec := policy.Spec.GOVAR.Calibration
	a := request.Artifact
	if a.SchemaVersion != govarcalibration.AuthoritativeArtifactSchema || a.ArtifactRef != spec.ArtifactRef || a.Version != spec.Version ||
		a.ArtifactSHA256 != spec.ArtifactSHA256 || a.SourceObservationsSHA256 != spec.CalibrationInputSHA256 ||
		a.FeatureSchemaVersion != spec.FeatureSchemaVersion || a.PriceRegimeSHA256 != spec.PriceRegimeSHA256 ||
		a.CapRegimeSHA256 != spec.CapRegimeSHA256 || a.ProducerSoftwareSHA256 != spec.ProducerSoftwareSHA256 ||
		a.CoverageTargetPPB != spec.CoverageTargetPPB || a.Support < spec.MinimumSupport ||
		a.CoverageIntervalLowerPPB < a.CoverageTargetPPB {
		return errors.New("artifact does not exactly match policy calibration regime")
	}
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now().UTC()
	}
	status := &aiopsv1alpha1.GOVARRoutingPolicyStatus{Calibration: &aiopsv1alpha1.GOVARCalibrationStatus{
		EvidenceSource: "postgresql-v8", SourceResourceVersion: request.ExpectedResourceVersion,
		ArtifactRef: a.ArtifactRef, Version: a.Version, ArtifactSHA256: a.ArtifactSHA256,
		CalibrationInputSHA256: a.SourceObservationsSHA256, FeatureSchemaVersion: a.FeatureSchemaVersion,
		PriceRegimeSHA256: a.PriceRegimeSHA256, CapRegimeSHA256: a.CapRegimeSHA256,
		ProducerSoftwareSHA256: a.ProducerSoftwareSHA256, CoverageTargetPPB: a.CoverageTargetPPB,
		EmpiricalCoveragePPB: a.EmpiricalCoveragePPB, CalibrationMethod: a.CalibrationMethod,
		ExchangeableMarginalCoverageLowerPPB: a.ExchangeableMarginalCoverageLowerPPB,
		ConformalRank:                        a.ConformalRank, CoverageBoundKind: a.CoverageBoundKind,
		CoverageBoundNumerator: a.CoverageBoundNumerator, CoverageBoundDenominator: a.CoverageBoundDenominator,
		CoverageIntervalLowerPPB: a.CoverageIntervalLowerPPB, CoverageIntervalUpperPPB: a.CoverageIntervalUpperPPB,
		CoverageConfidencePPB: a.CoverageConfidencePPB, FeatureRegimeSHA256: a.FeatureRegimeSHA256,
		SplitOpportunityRegimeSHA256: a.SplitOpportunityRegimeSHA256, CohortRegimeSHA256: a.CohortRegimeSHA256,
		Support: a.Support, AdaptiveOutputTokens: a.UpperOutputTokens, Valid: true, Reason: "CalibrationValid",
		CalibrationWindowStart: metav1.NewTime(a.WindowStart), CalibrationWindowEnd: metav1.NewTime(a.WindowEnd), ObservedAt: metav1.NewTime(now),
	}}
	if request.Drift == nil {
		status.Calibration.Valid = false
		status.Calibration.Reason = "MonitoringEvidenceMissing"
		status.Drift = &aiopsv1alpha1.GOVARDriftStatus{ConservativeMode: true, Detector: policy.Spec.GOVAR.Drift.Detector,
			ThresholdPPB: policy.Spec.GOVAR.Drift.ThresholdPPB, Reason: "MonitoringEvidenceMissing", ObservedAt: metav1.NewTime(now)}
	} else {
		d := request.Drift
		if d.ArtifactSHA256 != a.ArtifactSHA256 || d.Result.Detector != string(policy.Spec.GOVAR.Drift.Detector) ||
			d.Result.ThresholdPPB != policy.Spec.GOVAR.Drift.ThresholdPPB || d.Result.Support < policy.Spec.GOVAR.Drift.RevalidationMinimumSupport {
			return errors.New("drift window does not exactly match artifact and policy")
		}
		status.Drift = &aiopsv1alpha1.GOVARDriftStatus{DriftSHA256: d.DriftSHA256, Detected: d.Result.Detected,
			ConservativeMode: d.Result.Detected, Detector: policy.Spec.GOVAR.Drift.Detector,
			ThresholdPPB: d.Result.ThresholdPPB, MonitoringInputSHA256: d.Result.MonitoringInputSHA256,
			Support: d.Result.Support, EmpiricalCoveragePPB: d.Result.EmpiricalCoveragePPB,
			ConfidencePPB: d.Result.ConfidencePPB, IntervalLowerPPB: d.Result.IntervalLowerPPB,
			IntervalUpperPPB: d.Result.IntervalUpperPPB, MonitoringWindowStart: metav1.NewTime(d.Result.WindowStart),
			MonitoringWindowEnd: metav1.NewTime(d.Result.WindowEnd), ObservedAt: metav1.NewTime(now), Reason: "CalibrationValid"}
		if d.Result.Detected {
			status.Calibration.Valid, status.Calibration.Reason, status.Drift.Reason = false, "DriftDetected", "DriftDetected"
		}
	}
	// The status payload digest deliberately excludes PublicationSHA256 to avoid
	// a self-referential hash. PublicationSHA256 then commits that digest.
	payload, err := json.Marshal(status)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	statusSHA := hex.EncodeToString(sum[:])
	driftSHA := ""
	if request.Drift != nil {
		driftSHA = request.Drift.DriftSHA256
	}
	publicationSHA, err := govarcalibration.CanonicalDigest("govar-policy-evidence-publication-v1",
		request.TenantID, policy.Namespace, policy.Name, string(policy.UID), fmt.Sprint(policy.Generation),
		policy.ResourceVersion, a.ArtifactSHA256, driftSHA, statusSHA, p.Producer.SoftwareSHA256())
	if err != nil {
		return err
	}
	status.Calibration.PublicationSHA256 = publicationSHA
	publication := govar.PolicyEvidencePublication{PublicationSHA256: publicationSHA, TenantID: request.TenantID,
		PolicyNamespace: policy.Namespace, PolicyName: policy.Name, PolicyUID: string(policy.UID),
		PolicyGeneration: policy.Generation, ExpectedResourceVersion: policy.ResourceVersion,
		ArtifactSHA256: a.ArtifactSHA256, DriftSHA256: driftSHA, StatusPayloadSHA256: statusSHA,
		ProducerSoftwareSHA256: p.Producer.SoftwareSHA256()}
	if err := p.Producer.PublishPolicyEvidence(ctx, publication); err != nil {
		return err
	}
	policy.Status.GOVAR = status
	policy.Status.ObservedGeneration = policy.Generation
	if err := p.Client.Status().Update(ctx, &policy); err != nil {
		return err
	}
	return nil
}
