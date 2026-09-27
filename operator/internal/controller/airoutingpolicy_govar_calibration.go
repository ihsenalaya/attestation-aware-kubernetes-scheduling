package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
)

const (
	calibrationDataKey        = "calibration.json"
	monitoringDataKey         = "monitoring.json"
	conditionGOVARCalibration = "GOVARCalibration"
	conditionGOVARDrift       = "GOVARDrift"
)

// reconcileGOVAREvidence is the single producer of status.govar. It deliberately
// reads calibration and monitoring from distinct, typed references. Kubernetes
// status updates use the resourceVersion read by Reconcile, so a stale producer
// cannot overwrite a newer conservative state.
func (r *AIRoutingPolicyReconciler) reconcileGOVAREvidence(ctx context.Context, policy *aiopsv1alpha1.AIRoutingPolicy) *aiopsv1alpha1.GOVARRoutingPolicyStatus {
	spec := policy.Spec.GOVAR
	if spec == nil {
		return nil
	}
	method := spec.Reservation.Method
	if method != aiopsv1alpha1.GOVARReservationAdaptiveQuantile && method != aiopsv1alpha1.GOVARReservationFixedCohort {
		return nil
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	conservative := func(reason string) *aiopsv1alpha1.GOVARRoutingPolicyStatus {
		status := &aiopsv1alpha1.GOVARRoutingPolicyStatus{Drift: &aiopsv1alpha1.GOVARDriftStatus{
			Detected: false, ConservativeMode: true, Detector: spec.Drift.Detector,
			ThresholdPPB: spec.Drift.ThresholdPPB, Reason: reason, ObservedAt: metav1.NewTime(now),
		}}
		if spec.Calibration != nil {
			status.Calibration = expectedCalibrationStatus(spec.Calibration, reason, now)
		}
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{Type: conditionGOVARCalibration,
			Status: metav1.ConditionFalse, ObservedGeneration: policy.Generation, Reason: reason,
			Message: "adaptive reservation disabled: " + reason, LastTransitionTime: metav1.NewTime(now)})
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{Type: conditionGOVARDrift,
			Status: metav1.ConditionUnknown, ObservedGeneration: policy.Generation, Reason: "ConservativeMode",
			Message: "calibrated risk is not advertised", LastTransitionTime: metav1.NewTime(now)})
		return status
	}
	if errs := spec.Validate(field.NewPath("spec", "govar")); len(errs) != 0 || spec.Calibration == nil {
		return conservative("CalibrationSpecInvalid")
	}
	if !r.allowDiagnosticConfigMapEvidence {
		if policy.Status.GOVAR != nil && policy.Status.GOVAR.Calibration != nil &&
			policy.Status.GOVAR.Calibration.EvidenceSource == "postgresql-v8" {
			return policy.Status.GOVAR.DeepCopy()
		}
		return conservative("CalibrationSourceNotLedgerAuthoritative")
	}
	cal := spec.Calibration
	if spec.Drift.Detector != aiopsv1alpha1.GOVARDriftDetector("coverage-gap") {
		return conservative("DriftDetectorUnsupported")
	}

	calibrationRows, err := r.readObservations(ctx, policy.Namespace, cal.CalibrationDataRef, calibrationDataKey)
	if err != nil {
		return conservative("CalibrationInputUnavailable")
	}
	artifact, err := govarcalibration.BuildArtifact(govarcalibration.BuildConfig{
		ArtifactRef: cal.ArtifactRef, Version: cal.Version, FeatureSchemaVersion: cal.FeatureSchemaVersion,
		PriceRegimeSHA256: cal.PriceRegimeSHA256, CapRegimeSHA256: cal.CapRegimeSHA256,
		ProducerSoftwareSHA256: cal.ProducerSoftwareSHA256, CoverageTargetPPB: cal.CoverageTargetPPB,
	}, calibrationRows)
	if err != nil {
		return conservative("CalibrationInputRejected")
	}
	if artifact.SourceObservationsSHA256 != cal.CalibrationInputSHA256 || artifact.ArtifactSHA256 != cal.ArtifactSHA256 {
		return conservative("CalibrationDigestMismatch")
	}
	if artifact.Support < cal.MinimumSupport {
		return conservative("CalibrationInsufficient")
	}
	if artifact.WindowEnd.After(now) {
		return conservative("CalibrationFromFuture")
	}
	if now.Sub(artifact.WindowEnd) > time.Duration(cal.MaxAgeSeconds)*time.Second {
		return conservative("CalibrationStale")
	}

	monitoringRows, err := r.readObservations(ctx, policy.Namespace, cal.MonitoringDataRef, monitoringDataKey)
	if err != nil {
		return conservative("MonitoringInputUnavailable")
	}
	drift, err := govarcalibration.DetectCoverageGap(artifact, spec.Drift.ThresholdPPB, monitoringRows)
	if err != nil {
		return conservative("MonitoringInputRejected")
	}
	if drift.WindowStart.Before(artifact.WindowEnd) {
		return conservative("MonitoringWindowOverlap")
	}
	if drift.WindowEnd.After(now) {
		return conservative("MonitoringFromFuture")
	}
	if now.Sub(drift.WindowEnd) > time.Duration(cal.MaxAgeSeconds)*time.Second {
		return conservative("MonitoringStale")
	}
	if drift.Support < spec.Drift.RevalidationMinimumSupport {
		return conservative("RevalidationInsufficient")
	}

	status := &aiopsv1alpha1.GOVARRoutingPolicyStatus{
		Calibration: calibrationStatusFromArtifact(artifact, now),
		Drift: &aiopsv1alpha1.GOVARDriftStatus{
			Detected: drift.Detected, ConservativeMode: drift.Detected,
			Detector: spec.Drift.Detector, ThresholdPPB: spec.Drift.ThresholdPPB,
			MonitoringInputSHA256: drift.MonitoringInputSHA256, Support: drift.Support,
			EmpiricalCoveragePPB:  drift.EmpiricalCoveragePPB,
			MonitoringWindowStart: metav1.NewTime(drift.WindowStart), MonitoringWindowEnd: metav1.NewTime(drift.WindowEnd),
			ObservedAt: metav1.NewTime(drift.WindowEnd),
		},
	}
	if drift.Detected {
		status.Calibration.Valid = false
		status.Calibration.Reason = "DriftDetected"
		status.Drift.Reason = "DriftDetected"
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{Type: conditionGOVARCalibration,
			Status: metav1.ConditionFalse, ObservedGeneration: policy.Generation, Reason: "DriftDetected",
			Message: "coverage gap exceeds the frozen threshold", LastTransitionTime: metav1.NewTime(now)})
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{Type: conditionGOVARDrift,
			Status: metav1.ConditionTrue, ObservedGeneration: policy.Generation, Reason: "DriftDetected",
			Message: "conservative fallback is active", LastTransitionTime: metav1.NewTime(now)})
		return status
	}
	status.Calibration.Valid = true
	status.Calibration.Reason = "CalibrationValid"
	status.Drift.Reason = "CalibrationValid"
	apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{Type: conditionGOVARCalibration,
		Status: metav1.ConditionTrue, ObservedGeneration: policy.Generation, Reason: "CalibrationValid",
		Message: fmt.Sprintf("artifact %s validated with support %d", artifact.ArtifactSHA256, artifact.Support), LastTransitionTime: metav1.NewTime(now)})
	apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{Type: conditionGOVARDrift,
		Status: metav1.ConditionFalse, ObservedGeneration: policy.Generation, Reason: "CalibrationValid",
		Message: "monitoring coverage is within the frozen threshold", LastTransitionTime: metav1.NewTime(now)})
	return status
}

func (r *AIRoutingPolicyReconciler) readObservations(ctx context.Context, namespace, name, key string) ([]govarcalibration.Observation, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%s ConfigMap reference is empty", key)
	}
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &cm); err != nil {
		return nil, err
	}
	if cm.Immutable == nil || !*cm.Immutable {
		return nil, fmt.Errorf("ConfigMap %s must set immutable=true", name)
	}
	raw, ok := cm.Data[key]
	if !ok {
		return nil, fmt.Errorf("ConfigMap %s lacks %s", name, key)
	}
	return govarcalibration.ParseObservations([]byte(raw))
}

func expectedCalibrationStatus(cal *aiopsv1alpha1.GOVARCalibrationPolicy, reason string, now time.Time) *aiopsv1alpha1.GOVARCalibrationStatus {
	return &aiopsv1alpha1.GOVARCalibrationStatus{ArtifactRef: cal.ArtifactRef, Version: cal.Version,
		ArtifactSHA256: cal.ArtifactSHA256, CalibrationInputSHA256: cal.CalibrationInputSHA256,
		FeatureSchemaVersion: cal.FeatureSchemaVersion, PriceRegimeSHA256: cal.PriceRegimeSHA256,
		CapRegimeSHA256: cal.CapRegimeSHA256, ProducerSoftwareSHA256: cal.ProducerSoftwareSHA256,
		CoverageTargetPPB: cal.CoverageTargetPPB, Valid: false, Reason: reason, ObservedAt: metav1.NewTime(now)}
}

func calibrationStatusFromArtifact(a govarcalibration.Artifact, now time.Time) *aiopsv1alpha1.GOVARCalibrationStatus {
	return &aiopsv1alpha1.GOVARCalibrationStatus{EvidenceSource: "diagnostic-configmap-nonauthoritative", ArtifactRef: a.ArtifactRef, Version: a.Version,
		ArtifactSHA256: a.ArtifactSHA256, CalibrationInputSHA256: a.SourceObservationsSHA256,
		FeatureSchemaVersion: a.FeatureSchemaVersion, PriceRegimeSHA256: a.PriceRegimeSHA256,
		CapRegimeSHA256: a.CapRegimeSHA256, ProducerSoftwareSHA256: a.ProducerSoftwareSHA256,
		CoverageTargetPPB: a.CoverageTargetPPB, EmpiricalCoveragePPB: a.EmpiricalCoveragePPB,
		CalibrationMethod:                    a.CalibrationMethod,
		ExchangeableMarginalCoverageLowerPPB: a.ExchangeableMarginalCoverageLowerPPB,
		ConformalRank:                        a.ConformalRank,
		CoverageBoundKind:                    a.CoverageBoundKind,
		CoverageBoundNumerator:               a.CoverageBoundNumerator,
		CoverageBoundDenominator:             a.CoverageBoundDenominator,
		CoverageIntervalLowerPPB:             a.CoverageIntervalLowerPPB,
		CoverageIntervalUpperPPB:             a.CoverageIntervalUpperPPB,
		CoverageConfidencePPB:                a.CoverageConfidencePPB,
		Support:                              a.Support, AdaptiveOutputTokens: a.UpperOutputTokens, Valid: true,
		CalibrationWindowStart: metav1.NewTime(a.WindowStart), CalibrationWindowEnd: metav1.NewTime(a.WindowEnd),
		ObservedAt: metav1.NewTime(now)}
}
