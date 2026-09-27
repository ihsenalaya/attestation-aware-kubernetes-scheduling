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

package controller

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/collectors"
	"github.com/imperium/ai-sovereign-finops-operator/internal/metrics"
	"github.com/imperium/ai-sovereign-finops-operator/internal/qualityengine"
	"github.com/imperium/ai-sovereign-finops-operator/internal/sovereigntyengine"
	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
	"github.com/imperium/ai-sovereign-finops-operator/pkg/qualitystats"
)

// AIQualityGateReconciler reconciles an AIQualityGate object.
type AIQualityGateReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

const (
	qualityGateFinalizer       = "aiops.imperium.io/quality-gate-cleanup"
	qualityGateLabel           = "aiops.imperium.io/quality-gate"
	qualityEvaluatorLabel      = "aiops.imperium.io/quality-evaluator"
	qualityEvidenceSourceLabel = "aiops.imperium.io/evidence-source"
	qualityEvidenceSourceJob   = "quality-eval-job"
)

type goldenPrompt struct {
	ID       string `json:"id"`
	Prompt   string `json:"prompt"`
	Expected struct {
		RequiredKeywords []string          `json:"requiredKeywords,omitempty"`
		MaxTokens        int32             `json:"maxTokens,omitempty"`
		MustBeJSON       bool              `json:"mustBeJSON,omitempty"`
		Reference        string            `json:"reference,omitempty"`
		Fields           map[string]string `json:"fields,omitempty"`
	} `json:"expected,omitempty"`
}

type qualityEvidenceSample struct {
	ID                      string            `json:"id"`
	Model                   string            `json:"model,omitempty"`
	Reference               string            `json:"reference,omitempty"`
	Expected                string            `json:"expected,omitempty"`
	Actual                  string            `json:"actual,omitempty"`
	Output                  string            `json:"output,omitempty"`
	Response                string            `json:"response,omitempty"`
	ExpectedFields          map[string]string `json:"expectedFields,omitempty"`
	ActualFields            map[string]string `json:"actualFields,omitempty"`
	Fields                  map[string]string `json:"fields,omitempty"`
	SemanticScore           *float64          `json:"semanticScore,omitempty"`
	JudgedScore             *float64          `json:"judgedScore,omitempty"`
	SchemaValid             *bool             `json:"schemaValid,omitempty"`
	UnexpectedRefusal       *bool             `json:"unexpectedRefusal,omitempty"`
	SensitiveDataLeak       *bool             `json:"sensitiveDataLeak,omitempty"`
	RequiredKeywordsPresent *bool             `json:"requiredKeywordsPresent,omitempty"`
}

type modelObservation struct {
	model                    string
	provider                 string
	requests                 int64
	errors                   int64
	inputTokens              int64
	outputTokens             int64
	latencyWeightedMillis    float64
	latencyWeight            float64
	latencyTelemetryObserved bool
}

type qualityScoreEvidence struct {
	GoldenDatasetRef aiopsv1alpha1.ConfigMapDataReference `json:"goldenDatasetRef"`
	SourceModel      string                               `json:"sourceModel"`
	CandidateModel   string                               `json:"candidateModel"`
	Source           qualityengine.ModelResult            `json:"source"`
	Candidate        qualityengine.ModelResult            `json:"candidate"`
	Verdict          string                               `json:"verdict"`
	TolerancePoints  float64                              `json:"tolerancePoints"`
	GeneratedAt      string                               `json:"generatedAt"`
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiqualitygates,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiqualitygates/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiqualitygates/finalizers,verbs=update
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aigateways,verbs=get;list;watch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aimodels,verbs=get;list;watch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aimodels/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aiproviders;aisovereigntypolicies,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=create;delete;get;list;watch;update;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=create;delete;get;list;watch

func (r *AIQualityGateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var gate aiopsv1alpha1.AIQualityGate
	if err := r.Get(ctx, req.NamespacedName, &gate); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !gate.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&gate, qualityGateFinalizer) {
			if err := r.cleanupQualityGateResources(ctx, &gate); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&gate, qualityGateFinalizer)
			if err := r.Update(ctx, &gate); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	if controllerutil.AddFinalizer(&gate, qualityGateFinalizer) {
		if err := r.Update(ctx, &gate); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	previousEvaluationJobName := gate.Status.EvaluationJobName
	previousEvaluationJobPhase := gate.Status.EvaluationJobPhase
	previousVerdict := gate.Status.Verdict
	gate.Status.ObservedGeneration = gate.Generation
	gate.Status.Phase = aiopsv1alpha1.AIQualityGatePending
	gate.Status.Verdict = "insufficient-data"
	gate.Status.CheckedSamples = 0
	gate.Status.FailedChecks = 0
	gate.Status.QualityScore = 0
	gate.Status.CompositeScore = 0
	gate.Status.ScoreBreakdown = aiopsv1alpha1.AIQualityScoreBreakdown{}
	gate.Status.WeightsUsed = aiopsv1alpha1.AIQualityWeightsUsed{}
	gate.Status.Samples = 0
	gate.Status.RequiredSamples = 0
	gate.Status.ObservedSourceSamples = 0
	gate.Status.ObservedCandidateSamples = 0
	gate.Status.NonInferiorityLowerBound = 0
	gate.Status.DatasetVersion = ""
	gate.Status.DatasetHash = ""
	gate.Status.Dimensions = nil
	gate.Status.EvidenceRef = nil
	gate.Status.EvaluationJobName = previousEvaluationJobName
	gate.Status.EvaluationJobPhase = previousEvaluationJobPhase
	gate.Status.FailureMessages = nil
	gate.Status.SourceObservation = nil
	gate.Status.CandidateObservation = nil
	gate.Status.CanaryStatus = canaryStatus(gate.Spec.Canary)

	var failures []string
	var pending []string
	reason := aiopsv1alpha1.ReasonReconciled
	sourceObs := modelObservation{model: gate.Spec.SourceModel}
	candidateObs := modelObservation{model: gate.Spec.CandidateModel}
	sourceOK := false
	candidateOK := false

	prompts, err := r.loadGoldenDataset(ctx, gate.Namespace, gate.Spec.GoldenDatasetRef)
	if err != nil {
		reason = reasonForReadErr(err)
		pending = append(pending, err.Error())
	} else {
		gate.Status.CheckedSamples = int32(len(prompts))
		gate.Status.DatasetVersion, gate.Status.DatasetHash = r.goldenDatasetMetadata(ctx, gate.Namespace, gate.Spec.GoldenDatasetRef)
		failures = append(failures, validateGoldenDataset(prompts)...)
	}

	evidenceNeeded := qualityGateNeedsEvidence(gate.Spec.RequiredChecks) || qualityGateNeedsScoreEvidence(&gate)
	var evidence []qualityEvidenceSample
	if evidenceNeeded {
		if gate.Spec.EvidenceRef == nil {
			pending = append(pending, "quality scoring requires spec.evidenceRef so the evaluation job can persist source/candidate golden-run results")
		} else {
			ref := *gate.Spec.EvidenceRef
			gate.Status.EvidenceRef = &ref
			evidence, err = r.loadEvidence(ctx, gate.Namespace, *gate.Spec.EvidenceRef)
			if err != nil {
				if reason == aiopsv1alpha1.ReasonReconciled {
					reason = reasonForReadErr(err)
				}
				pending = append(pending, fmt.Sprintf("quality evidence not ready: %v", err))
			}
			if len(prompts) > 0 && !hasSourceAndCandidateEvidence(prompts, evidence, gate.Spec.SourceModel, gate.Spec.CandidateModel) {
				msg, evalErr := r.reconcileEvaluationJob(ctx, &gate)
				if evalErr != nil {
					if reason == aiopsv1alpha1.ReasonReconciled {
						reason = aiopsv1alpha1.ReasonReconcileError
					}
					pending = append(pending, evalErr.Error())
				} else if msg != "" {
					if reason == aiopsv1alpha1.ReasonReconciled && strings.Contains(msg, "failed") {
						reason = aiopsv1alpha1.ReasonNoTelemetry
					}
					pending = append(pending, msg)
				}
			} else {
				if gate.Status.EvaluationJobName != "" {
					gate.Status.EvaluationJobPhase = "Succeeded"
				}
				failures = append(failures, evaluateEvidence(prompts, evidence, gate.Spec.CandidateModel, gate.Spec.RequiredChecks)...)
			}
		}
	}

	samples, collectorName, err := r.collectSamples(ctx, &gate)
	if err != nil {
		if reason == aiopsv1alpha1.ReasonReconciled {
			reason = aiopsv1alpha1.ReasonNoTelemetry
		}
		pending = append(pending, err.Error())
	} else {
		sourceObs, sourceOK = observeModel(samples, gate.Spec.Target, gate.Spec.SourceModel)
		candidateObs, candidateOK = observeModel(samples, gate.Spec.Target, gate.Spec.CandidateModel)
		if sourceOK {
			apiObs := sourceObs.toAPI()
			gate.Status.SourceObservation = &apiObs
		}
		if candidateOK {
			apiObs := candidateObs.toAPI()
			gate.Status.CandidateObservation = &apiObs
		}
		failures, pending = appendOperationalChecks(failures, pending, sourceObs, sourceOK, candidateObs, candidateOK, gate.Spec.RequiredChecks)
		logger.V(1).Info("evaluated AIQualityGate telemetry", "collector", collectorName, "sourceObserved", sourceOK, "candidateObserved", candidateOK)
	}

	var comparison qualityengine.Comparison
	scoreEvaluated := false
	if len(prompts) > 0 && gate.Spec.EvidenceRef != nil {
		sourceSamples := buildQualitySamples(prompts, evidence, gate.Spec.SourceModel)
		candidateSamples := buildQualitySamples(prompts, evidence, gate.Spec.CandidateModel)
		comparison = qualityengine.Evaluate(qualityengine.EvaluateInput{
			SourceSamples:      sourceSamples,
			CandidateSamples:   candidateSamples,
			SourceTelemetry:    sourceObs.toQualityTelemetry(evidence, gate.Spec.SourceModel),
			CandidateTelemetry: candidateObs.toQualityTelemetry(evidence, gate.Spec.CandidateModel),
			Weights:            qualityEngineWeights(gate.Spec.Weights),
			MinSamples:         int(defaultedMinSamples(gate.Spec.MinSamples)),
			TolerancePoints:    float64(defaultedTolerance(gate.Spec.TolerancePoints)),
			LatencyThresholdMs: float64(gate.Spec.LatencyThresholdMs),
			JudgeEnabled:       gate.Spec.Judge.Enabled,
		})
		scoreEvaluated = true
		gate.Status.WeightsUsed = qualityWeightsToAPI(comparison.WeightsUsed)
		if comparison.Verdict != qualityengine.VerdictInsufficientData {
			gate.Status.QualityScore = roundQuality(comparison.Candidate.Overall)
			gate.Status.ScoreBreakdown = qualityBreakdownToAPI(comparison.Candidate.Breakdown)
			gate.Status.Samples = int32(comparison.Candidate.Samples)
			gate.Status.Dimensions = qualityDimensions(comparison.Candidate.Breakdown, comparison.Candidate.Overall, comparison.WeightsUsed)
		}
		if statisticalModeEnabled(gate.Spec.Statistical) {
			stats := qualitystats.EvaluateNonInferiority(statisticalConfig(gate.Spec.Statistical), evidencePassSample(sourceSamples), evidencePassSample(candidateSamples))
			gate.Status.RequiredSamples = int32(stats.RequiredSamplesPerArm)
			gate.Status.ObservedSourceSamples = int32(stats.ObservedSourceSamples)
			gate.Status.ObservedCandidateSamples = int32(stats.ObservedCandidateSlots)
			gate.Status.NonInferiorityLowerBound = roundQuality(stats.LowerConfidenceBound)
			if stats.Verdict == qualitystats.VerdictInsufficientData {
				pending = append(pending, fmt.Sprintf("statistical gate requires %d samples per arm; observed source=%d candidate=%d", stats.RequiredSamplesPerArm, stats.ObservedSourceSamples, stats.ObservedCandidateSlots))
			}
			composite := qualitystats.EvaluateComposite(statisticalWeights(gate.Spec.Statistical), qualitystats.CompositeInput{
				Quality:    comparison.Candidate.Overall / 100.0,
				ErrorRate:  qualitystats.ErrorRateScore(int(sourceObs.errors), int(sourceObs.requests), int(candidateObs.errors), int(candidateObs.requests)),
				LatencyP95: qualitystats.PercentileScore(sourceObs.averageLatencyMillis(), candidateObs.averageLatencyMillis(), 1.25),
				Cost:       r.qualityGateCostScore(ctx, gate.Namespace, sourceObs, candidateObs),
			})
			gate.Status.CompositeScore = roundQuality(composite.Score)
			hysteresisVerdict := qualitystats.ApplyHysteresis(previousVerdict, composite.Score, hysteresisConfig(gate.Spec.Statistical))
			if stats.Verdict == qualitystats.VerdictCandidateRisk || hysteresisVerdict == qualitystats.VerdictCandidateRisk {
				failures = append(failures, fmt.Sprintf("statistical gate rejected candidate: lower_bound=%.3f composite=%.3f", stats.LowerConfidenceBound, composite.Score))
			}
		}
		switch comparison.Verdict {
		case qualityengine.VerdictCandidateRisk:
			failures = append(failures, fmt.Sprintf("candidate quality score %.3f is below source %.3f minus tolerance %.3f", comparison.Candidate.Overall, comparison.Source.Overall, comparison.Tolerance))
		case qualityengine.VerdictInsufficientData:
			pending = append(pending, comparison.MissingInput...)
			if reason == aiopsv1alpha1.ReasonReconciled && hasTelemetryMissing(comparison.MissingInput) {
				reason = aiopsv1alpha1.ReasonNoTelemetry
			}
		}
	} else if len(prompts) > 0 {
		pending = append(pending, "quality scoring requires source and candidate evidence")
	}

	gate.Status.FailureMessages = append([]string{}, failures...)
	gate.Status.FailureMessages = append(gate.Status.FailureMessages, pending...)
	gate.Status.FailedChecks = int32(len(failures))

	switch {
	case len(failures) > 0:
		gate.Status.Phase = aiopsv1alpha1.AIQualityGateFailed
		gate.Status.Verdict = "candidate-risk"
		reason = aiopsv1alpha1.ReasonValidationFailed
	case len(pending) > 0:
		gate.Status.Phase = aiopsv1alpha1.AIQualityGatePending
		gate.Status.Verdict = "insufficient-data"
	default:
		gate.Status.Phase = aiopsv1alpha1.AIQualityGatePassed
		gate.Status.Verdict = "candidate-safe"
	}

	message := qualityGateMessage(&gate)
	condStatus := conditionStatusForGate(gate.Status.Phase)
	meta.SetStatusCondition(&gate.Status.Conditions, readyCondition(gate.Generation, condStatus, reason, message))

	metrics.QualityGatePassed.WithLabelValues(gate.Namespace, gate.Name, gate.Spec.Target.Namespace, gate.Spec.Target.Application, gate.Spec.SourceModel, gate.Spec.CandidateModel).Set(qualityGatePassedValue(gate.Status.Phase))
	metrics.QualityGateFailedChecks.WithLabelValues(gate.Namespace, gate.Name, gate.Spec.Target.Namespace, gate.Spec.Target.Application).Set(float64(gate.Status.FailedChecks))
	metrics.QualityGateScore.WithLabelValues(gate.Namespace, gate.Name, gate.Spec.Target.Namespace, gate.Spec.Target.Application).Set(gate.Status.CompositeScore)
	if scoreEvaluated && comparison.Verdict != qualityengine.VerdictInsufficientData {
		provider := candidateObs.provider
		if provider == "" {
			provider = r.providerForModel(ctx, gate.Namespace, gate.Spec.CandidateModel)
		}
		emitQualityScoreMetrics(&gate, provider, gate.Spec.CandidateModel)
		if err := r.writeScoreEvidence(ctx, &gate, comparison); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.updateCandidateModelQuality(ctx, &gate); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.Status().Update(ctx, &gate); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		eventType := corev1.EventTypeNormal
		if gate.Status.Phase != aiopsv1alpha1.AIQualityGatePassed {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Eventf(&gate, eventType, "QualityGateEvaluated", "%s", message)
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func (r *AIQualityGateReconciler) collectSamples(ctx context.Context, gate *aiopsv1alpha1.AIQualityGate) ([]collectors.UsageSample, string, error) {
	var gw *aiopsv1alpha1.AIGateway
	if gate.Spec.GatewayRef != "" {
		var g aiopsv1alpha1.AIGateway
		if err := r.Get(ctx, types.NamespacedName{Namespace: gate.Namespace, Name: gate.Spec.GatewayRef}, &g); err != nil {
			return nil, "", fmt.Errorf("read gateway %s/%s: %w", gate.Namespace, gate.Spec.GatewayRef, err)
		}
		gw = &g
	}
	if gw == nil {
		gw = firstGateway(ctx, r.Client, gate.Namespace)
	}
	collector, err := collectorFor(r.Client, gate.Namespace, gw)
	if err != nil {
		return nil, "", err
	}
	period := gate.Spec.Period
	if period == "" {
		period = "monthly"
	}
	samples, err := collector.Collect(ctx, periodWindow(period))
	if err != nil {
		return nil, collector.Name(), fmt.Errorf("telemetry collection failed (%s): %w", collector.Name(), err)
	}
	return samples, collector.Name(), nil
}

func (r *AIQualityGateReconciler) cleanupQualityGateResources(ctx context.Context, gate *aiopsv1alpha1.AIQualityGate) error {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(gate.Namespace), client.MatchingLabels{
		qualityGateLabel:      gate.Name,
		qualityEvaluatorLabel: "true",
	}); err != nil {
		return fmt.Errorf("list quality evaluation jobs for %s/%s: %w", gate.Namespace, gate.Name, err)
	}
	propagationPolicy := metav1.DeletePropagationBackground
	for i := range jobs.Items {
		job := jobs.Items[i]
		if err := r.Delete(ctx, &job, client.PropagationPolicy(propagationPolicy)); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete quality evaluation job %s/%s: %w", job.Namespace, job.Name, err)
		}
	}

	if gate.Spec.EvidenceRef == nil {
		return nil
	}
	ref := *gate.Spec.EvidenceRef
	namespace := ref.Namespace
	if namespace == "" {
		namespace = gate.Namespace
	}
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read evidence ConfigMap %s/%s during cleanup: %w", namespace, ref.Name, err)
	}
	if cm.Labels[qualityGateLabel] != gate.Name || cm.Labels[qualityEvidenceSourceLabel] != qualityEvidenceSourceJob {
		return nil
	}
	if err := r.Delete(ctx, &cm); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete evidence ConfigMap %s/%s: %w", namespace, ref.Name, err)
	}
	return nil
}

func (r *AIQualityGateReconciler) loadGoldenDataset(ctx context.Context, defaultNamespace string, ref aiopsv1alpha1.ConfigMapDataReference) ([]goldenPrompt, error) {
	raw, err := r.readConfigMapData(ctx, defaultNamespace, ref, []string{"prompts.yaml", "prompts.yml", "prompts.json"})
	if err != nil {
		return nil, err
	}
	var prompts []goldenPrompt
	if err := yaml.Unmarshal([]byte(raw), &prompts); err == nil && len(prompts) > 0 {
		return prompts, nil
	}
	var wrapped struct {
		Prompts []goldenPrompt `json:"prompts"`
	}
	if err := yaml.Unmarshal([]byte(raw), &wrapped); err != nil {
		return nil, fmt.Errorf("parse golden dataset %s: %w", ref.Name, err)
	}
	return wrapped.Prompts, nil
}

func (r *AIQualityGateReconciler) loadEvidence(ctx context.Context, defaultNamespace string, ref aiopsv1alpha1.ConfigMapDataReference) ([]qualityEvidenceSample, error) {
	raw, err := r.readEvidenceConfigMapData(ctx, defaultNamespace, ref, []string{"results.yaml", "results.yml", "evidence.yaml", "evidence.yml", "results.json"})
	if err != nil {
		return nil, err
	}
	var samples []qualityEvidenceSample
	if err := yaml.Unmarshal([]byte(raw), &samples); err == nil && len(samples) > 0 {
		return samples, nil
	}
	var wrapped struct {
		Samples []qualityEvidenceSample `json:"samples"`
	}
	if err := yaml.Unmarshal([]byte(raw), &wrapped); err != nil {
		return nil, fmt.Errorf("parse quality evidence %s: %w", ref.Name, err)
	}
	return wrapped.Samples, nil
}

func (r *AIQualityGateReconciler) readEvidenceConfigMapData(ctx context.Context, defaultNamespace string, ref aiopsv1alpha1.ConfigMapDataReference, defaultKeys []string) (string, error) {
	namespace := ref.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &cm); err != nil {
		return "", fmt.Errorf("read ConfigMap %s/%s: %w", namespace, ref.Name, err)
	}
	if cm.Labels[qualityEvidenceSourceLabel] != qualityEvidenceSourceJob {
		return "", fmt.Errorf("ConfigMap %s/%s is not live evidence from a quality evaluation job", namespace, ref.Name)
	}
	if cm.Labels[qualityGateLabel] == "" {
		return "", fmt.Errorf("ConfigMap %s/%s is missing quality gate ownership label", namespace, ref.Name)
	}
	return configMapData(cm, ref.Key, defaultKeys)
}

func (r *AIQualityGateReconciler) readConfigMapData(ctx context.Context, defaultNamespace string, ref aiopsv1alpha1.ConfigMapDataReference, defaultKeys []string) (string, error) {
	namespace := ref.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &cm); err != nil {
		return "", fmt.Errorf("read ConfigMap %s/%s: %w", namespace, ref.Name, err)
	}
	return configMapData(cm, ref.Key, defaultKeys)
}

func configMapData(cm corev1.ConfigMap, key string, defaultKeys []string) (string, error) {
	if key != "" {
		if v, ok := cm.Data[key]; ok {
			return v, nil
		}
		return "", fmt.Errorf("ConfigMap %s/%s missing key %q", cm.Namespace, cm.Name, key)
	}
	for _, key := range defaultKeys {
		if v, ok := cm.Data[key]; ok {
			return v, nil
		}
	}
	return "", fmt.Errorf("ConfigMap %s/%s missing one of keys %s", cm.Namespace, cm.Name, strings.Join(defaultKeys, ", "))
}

func (r *AIQualityGateReconciler) reconcileEvaluationJob(ctx context.Context, gate *aiopsv1alpha1.AIQualityGate) (string, error) {
	endpoint := strings.TrimSpace(gate.Spec.Evaluation.Endpoint)
	if endpoint == "" {
		return "quality evaluation job requires spec.evaluation.endpoint with the production gateway chat/completions URL", nil
	}
	if gate.Spec.GoldenDatasetRef.Namespace != "" && gate.Spec.GoldenDatasetRef.Namespace != gate.Namespace {
		return "", fmt.Errorf("quality evaluation job can only mount goldenDatasetRef from the gate namespace; got %s/%s", gate.Spec.GoldenDatasetRef.Namespace, gate.Spec.GoldenDatasetRef.Name)
	}
	if msg, err := r.evaluationSovereigntyBlock(ctx, gate); err != nil {
		return "", err
	} else if msg != "" {
		return msg, nil
	}
	image, err := r.qualityEvaluatorImage(ctx, gate)
	if err != nil {
		return "", err
	}
	jobName := qualityEvaluationJobName(gate)
	gate.Status.EvaluationJobName = jobName

	var job batchv1.Job
	err = r.Get(ctx, types.NamespacedName{Namespace: gate.Namespace, Name: jobName}, &job)
	if apierrors.IsNotFound(err) {
		job = *r.buildEvaluationJob(gate, jobName, image, endpoint)
		if err := ctrl.SetControllerReference(gate, &job, r.Scheme); err != nil {
			return "", fmt.Errorf("set owner on quality evaluation job: %w", err)
		}
		if err := r.Create(ctx, &job); err != nil {
			return "", fmt.Errorf("create quality evaluation job %s/%s: %w", gate.Namespace, jobName, err)
		}
		gate.Status.EvaluationJobPhase = "Pending"
		return fmt.Sprintf("quality evaluation job %q created and waiting for gateway-backed evidence", jobName), nil
	}
	if err != nil {
		return "", fmt.Errorf("read quality evaluation job %s/%s: %w", gate.Namespace, jobName, err)
	}

	switch {
	case job.Status.Succeeded > 0:
		gate.Status.EvaluationJobPhase = "Succeeded"
		raw, err := r.evaluationJobEvidence(ctx, gate.Namespace, jobName)
		if err != nil {
			return "", err
		}
		if err := validateEvaluationEvidence(raw); err != nil {
			return "", err
		}
		if err := r.writeRawEvidence(ctx, gate, raw); err != nil {
			return "", err
		}
		return fmt.Sprintf("quality evaluation job %q completed; evidenceRef has been written", jobName), nil
	case job.Status.Failed > 0:
		gate.Status.EvaluationJobPhase = "Failed"
		return fmt.Sprintf("quality evaluation job %q failed; see Job/Pod status for the real gateway error", jobName), nil
	case job.Status.Active > 0:
		gate.Status.EvaluationJobPhase = "Running"
		return fmt.Sprintf("quality evaluation job %q is still running", jobName), nil
	default:
		gate.Status.EvaluationJobPhase = "Pending"
		return fmt.Sprintf("quality evaluation job %q is pending", jobName), nil
	}
}

func (r *AIQualityGateReconciler) evaluationSovereigntyBlock(ctx context.Context, gate *aiopsv1alpha1.AIQualityGate) (string, error) {
	policy := firstSovereigntyPolicy(ctx, r.Client, gate.Namespace)
	if policy == nil {
		return "", nil
	}
	pe := policyToEngine(policy.Spec)
	for _, model := range []string{gate.Spec.SourceModel, gate.Spec.CandidateModel} {
		provider, zone, ok, err := r.providerZoneForModel(ctx, gate.Namespace, model)
		if err != nil {
			return "", err
		}
		if !ok {
			return fmt.Sprintf("quality evaluation requires catalogued AIModel/AIProvider to verify sovereignty before calling model %q", model), nil
		}
		if !sovereigntyengine.IsZoneAllowed(pe, zone) {
			return fmt.Sprintf("quality evaluation blocked: model %q on provider %q is in non-compliant zone %s for AISovereigntyPolicy %q", model, provider, sovereigntyengine.NormalizeZone(zone), policy.Name), nil
		}
	}
	return "", nil
}

func (r *AIQualityGateReconciler) providerZoneForModel(ctx context.Context, namespace, modelName string) (string, string, bool, error) {
	var models aiopsv1alpha1.AIModelList
	if err := r.List(ctx, &models, client.InNamespace(namespace)); err != nil {
		return "", "", false, err
	}
	for i := range models.Items {
		model := models.Items[i]
		if model.Spec.ModelName != modelName {
			continue
		}
		var provider aiopsv1alpha1.AIProvider
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: model.Spec.ProviderRef}, &provider); err != nil {
			if apierrors.IsNotFound(err) {
				return model.Spec.ProviderRef, "", false, nil
			}
			return "", "", false, err
		}
		return provider.Name, provider.Spec.DataResidency, true, nil
	}
	return "", "", false, nil
}

func (r *AIQualityGateReconciler) qualityEvaluatorImage(ctx context.Context, gate *aiopsv1alpha1.AIQualityGate) (string, error) {
	if image := strings.TrimSpace(gate.Spec.Evaluation.Image); image != "" {
		return image, nil
	}
	if image := strings.TrimSpace(os.Getenv("QUALITY_EVALUATOR_IMAGE")); image != "" {
		return image, nil
	}
	podName := strings.TrimSpace(os.Getenv("POD_NAME"))
	podNamespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
	if podName == "" || podNamespace == "" {
		return "", fmt.Errorf("quality evaluation image is not configured; set spec.evaluation.image or QUALITY_EVALUATOR_IMAGE")
	}
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: podNamespace, Name: podName}, &pod); err != nil {
		return "", fmt.Errorf("read operator pod image for quality evaluation: %w", err)
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == "manager" {
			return c.Image, nil
		}
	}
	return "", fmt.Errorf("operator pod %s/%s has no manager container image", podNamespace, podName)
}

func (r *AIQualityGateReconciler) buildEvaluationJob(gate *aiopsv1alpha1.AIQualityGate, jobName, image, endpoint string) *batchv1.Job {
	labels := map[string]string{
		"app.kubernetes.io/name": "ai-sovereign-finops-operator",
		qualityGateLabel:         gate.Name,
		qualityEvaluatorLabel:    "true",
	}
	backoffLimit := int32(0)
	ttlSeconds := int32(600)
	timeoutSeconds := defaultedEvalTimeoutSeconds(gate.Spec.Evaluation.TimeoutSeconds)
	maxTokens := defaultedEvalMaxTokens(gate.Spec.Evaluation.MaxTokens)
	args := []string{
		"quality-eval",
		"--endpoint=" + endpoint,
		"--prompts-dir=/quality/dataset",
		"--namespace=" + gate.Spec.Target.Namespace,
		"--application=" + gate.Spec.Target.Application,
		"--source-model=" + gate.Spec.SourceModel,
		"--candidate-model=" + gate.Spec.CandidateModel,
		fmt.Sprintf("--timeout-seconds=%d", timeoutSeconds),
		fmt.Sprintf("--max-tokens=%d", maxTokens),
	}

	cmVolume := corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{Name: gate.Spec.GoldenDatasetRef.Name},
	}
	if gate.Spec.GoldenDatasetRef.Key != "" {
		cmVolume.Items = []corev1.KeyToPath{{Key: gate.Spec.GoldenDatasetRef.Key, Path: "prompts.yaml"}}
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: gate.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPtr(true),
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:            "quality-evaluator",
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"/manager"},
						Args:            args,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "golden-dataset",
							MountPath: "/quality/dataset",
							ReadOnly:  true,
						}},
						TerminationMessagePath:   "/dev/termination-log",
						TerminationMessagePolicy: corev1.TerminationMessageReadFile,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: boolPtr(false),
							RunAsNonRoot:             boolPtr(true),
							ReadOnlyRootFilesystem:   boolPtr(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("10m"),
								corev1.ResourceMemory: resource.MustParse("32Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("250m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "golden-dataset",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &cmVolume,
						},
					}},
				},
			},
		},
	}
}

func (r *AIQualityGateReconciler) evaluationJobEvidence(ctx context.Context, namespace, jobName string) ([]byte, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{"job-name": jobName}); err != nil {
		return nil, fmt.Errorf("list pods for quality evaluation job %s/%s: %w", namespace, jobName, err)
	}
	for _, pod := range pods.Items {
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != "quality-evaluator" || status.State.Terminated == nil {
				continue
			}
			terminated := status.State.Terminated
			if terminated.ExitCode != 0 {
				return nil, fmt.Errorf("quality evaluation pod %s/%s exited %d: %s", namespace, pod.Name, terminated.ExitCode, terminated.Message)
			}
			if strings.TrimSpace(terminated.Message) == "" {
				return nil, fmt.Errorf("quality evaluation pod %s/%s completed without evidence in termination message", namespace, pod.Name)
			}
			return []byte(terminated.Message), nil
		}
	}
	return nil, fmt.Errorf("quality evaluation job %s/%s succeeded but no completed evaluator pod was found", namespace, jobName)
}

func validateEvaluationEvidence(raw []byte) error {
	var wrapped struct {
		Samples []qualityEvidenceSample `json:"samples"`
	}
	if err := yaml.Unmarshal(raw, &wrapped); err != nil {
		return fmt.Errorf("parse quality evaluation evidence: %w", err)
	}
	if len(wrapped.Samples) == 0 {
		return fmt.Errorf("quality evaluation evidence contains no samples")
	}
	return nil
}

func (r *AIQualityGateReconciler) writeRawEvidence(ctx context.Context, gate *aiopsv1alpha1.AIQualityGate, raw []byte) error {
	if gate.Spec.EvidenceRef == nil {
		return nil
	}
	ref := *gate.Spec.EvidenceRef
	namespace := ref.Namespace
	if namespace == "" {
		namespace = gate.Namespace
	}
	key := ref.Key
	if key == "" {
		key = "results.yaml"
	}
	var cm corev1.ConfigMap
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &cm)
	if apierrors.IsNotFound(err) {
		cm = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ref.Name,
				Namespace: namespace,
				Labels: map[string]string{
					"app.kubernetes.io/name":      "ai-sovereign-finops-operator",
					qualityGateLabel:              gate.Name,
					qualityEvidenceSourceLabel:    qualityEvidenceSourceJob,
					qualityEvaluatorLabel:         "true",
					"aiops.imperium.io/evidence":  "raw-gateway-run",
					"aiops.imperium.io/namespace": gate.Spec.Target.Namespace,
				},
			},
			Data: map[string]string{key: string(raw)},
		}
		if namespace == gate.Namespace {
			if err := ctrl.SetControllerReference(gate, &cm, r.Scheme); err != nil {
				return fmt.Errorf("set owner on quality evidence ConfigMap: %w", err)
			}
		}
		return r.Create(ctx, &cm)
	}
	if err != nil {
		return fmt.Errorf("read evidence ConfigMap %s/%s: %w", namespace, ref.Name, err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	if cm.Labels == nil {
		cm.Labels = map[string]string{}
	}
	cm.Labels["app.kubernetes.io/name"] = "ai-sovereign-finops-operator"
	cm.Labels[qualityGateLabel] = gate.Name
	cm.Labels[qualityEvidenceSourceLabel] = qualityEvidenceSourceJob
	cm.Labels[qualityEvaluatorLabel] = "true"
	cm.Labels["aiops.imperium.io/evidence"] = "raw-gateway-run"
	cm.Labels["aiops.imperium.io/namespace"] = gate.Spec.Target.Namespace
	if namespace == gate.Namespace {
		if err := ctrl.SetControllerReference(gate, &cm, r.Scheme); err != nil {
			return fmt.Errorf("set owner on quality evidence ConfigMap: %w", err)
		}
	}
	cm.Data[key] = string(raw)
	return r.Update(ctx, &cm)
}

func hasSourceAndCandidateEvidence(prompts []goldenPrompt, evidence []qualityEvidenceSample, sourceModel, candidateModel string) bool {
	if len(prompts) == 0 || len(evidence) == 0 {
		return false
	}
	source := evidenceByIDForModel(evidence, sourceModel)
	candidate := evidenceByIDForModel(evidence, candidateModel)
	for _, prompt := range prompts {
		if _, ok := source[prompt.ID]; !ok {
			return false
		}
		if _, ok := candidate[prompt.ID]; !ok {
			return false
		}
	}
	return true
}

func qualityEvaluationJobName(gate *aiopsv1alpha1.AIQualityGate) string {
	sum := sha1.Sum([]byte(fmt.Sprintf("%s/%s/%s/%d", gate.Namespace, gate.Name, gate.UID, gate.Generation)))
	suffix := hex.EncodeToString(sum[:])[:10]
	base := dnsLabelPart(gate.Name)
	maxBase := 63 - len("quality-eval-") - len(suffix) - 1
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	return "quality-eval-" + base + "-" + suffix
}

func dnsLabelPart(in string) string {
	in = strings.ToLower(in)
	var b strings.Builder
	lastDash := false
	for _, r := range in {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "gate"
	}
	return out
}

func defaultedEvalTimeoutSeconds(v int32) int32 {
	if v <= 0 {
		return 60
	}
	return v
}

func defaultedEvalMaxTokens(v int32) int32 {
	if v <= 0 {
		return 96
	}
	return v
}

func boolPtr(v bool) *bool { return &v }

func validateGoldenDataset(prompts []goldenPrompt) []string {
	if len(prompts) == 0 {
		return []string{"golden dataset contains no prompts"}
	}
	seen := map[string]bool{}
	var failures []string
	for i, p := range prompts {
		if strings.TrimSpace(p.ID) == "" {
			failures = append(failures, fmt.Sprintf("golden prompt at index %d is missing id", i))
		}
		if strings.TrimSpace(p.Prompt) == "" {
			failures = append(failures, fmt.Sprintf("golden prompt %q is missing prompt text", p.ID))
		}
		if p.ID != "" {
			if seen[p.ID] {
				failures = append(failures, fmt.Sprintf("golden prompt id %q is duplicated", p.ID))
			}
			seen[p.ID] = true
		}
	}
	return failures
}

func qualityGateNeedsEvidence(c aiopsv1alpha1.AIQualityRequiredChecks) bool {
	return c.SchemaValid || c.NoUnexpectedRefusal || c.NoSensitiveDataLeak || len(c.RequiredKeywords) > 0
}

func qualityGateNeedsScoreEvidence(gate *aiopsv1alpha1.AIQualityGate) bool {
	weights := qualityengine.NormalizeWeights(qualityEngineWeights(gate.Spec.Weights))
	if !gate.Spec.Judge.Enabled {
		weights.Judged = 0
		weights = qualityengine.NormalizeWeights(weights)
	}
	return weights.Correctness > 0 || weights.Semantic > 0 || weights.Judged > 0
}

func evaluateEvidence(prompts []goldenPrompt, evidence []qualityEvidenceSample, candidateModel string, checks aiopsv1alpha1.AIQualityRequiredChecks) []string {
	byID := map[string]qualityEvidenceSample{}
	for _, ev := range evidence {
		if ev.ID == "" {
			continue
		}
		if ev.Model != "" && ev.Model != candidateModel {
			continue
		}
		byID[ev.ID] = ev
	}
	var failures []string
	for _, p := range prompts {
		ev, ok := byID[p.ID]
		if !ok {
			failures = append(failures, fmt.Sprintf("missing quality evidence for prompt %q and candidate model %q", p.ID, candidateModel))
			continue
		}
		if checks.SchemaValid {
			if ev.SchemaValid == nil || !*ev.SchemaValid {
				failures = append(failures, fmt.Sprintf("schema check failed for prompt %q", p.ID))
			}
		}
		if checks.NoUnexpectedRefusal {
			if ev.UnexpectedRefusal == nil || *ev.UnexpectedRefusal {
				failures = append(failures, fmt.Sprintf("unexpected refusal check failed for prompt %q", p.ID))
			}
		}
		if checks.NoSensitiveDataLeak {
			if ev.SensitiveDataLeak == nil || *ev.SensitiveDataLeak {
				failures = append(failures, fmt.Sprintf("sensitive-data leak check failed for prompt %q", p.ID))
			}
		}
		if len(checks.RequiredKeywords) > 0 {
			if ev.RequiredKeywordsPresent == nil || !*ev.RequiredKeywordsPresent {
				failures = append(failures, fmt.Sprintf("required-keywords check failed for prompt %q", p.ID))
			}
		}
	}
	return failures
}

func buildQualitySamples(prompts []goldenPrompt, evidence []qualityEvidenceSample, model string) []qualityengine.EvidenceSample {
	byID := evidenceByIDForModel(evidence, model)
	out := make([]qualityengine.EvidenceSample, 0, len(prompts))
	for _, p := range prompts {
		ev, ok := byID[p.ID]
		if !ok {
			continue
		}
		expected := firstNonEmpty(ev.Reference, ev.Expected, p.Expected.Reference)
		actual := firstNonEmpty(ev.Actual, ev.Output, ev.Response)
		expectedFields := p.Expected.Fields
		if len(ev.ExpectedFields) > 0 {
			expectedFields = ev.ExpectedFields
		}
		actualFields := ev.ActualFields
		if len(actualFields) == 0 {
			actualFields = ev.Fields
		}
		out = append(out, qualityengine.EvidenceSample{
			ID:                      p.ID,
			Expected:                expected,
			Actual:                  actual,
			MustBeJSON:              p.Expected.MustBeJSON,
			ExpectedFields:          expectedFields,
			ActualFields:            actualFields,
			SemanticScore:           ev.SemanticScore,
			JudgedScore:             ev.JudgedScore,
			RequiredKeywordsPresent: ev.RequiredKeywordsPresent,
		})
	}
	return out
}

func evidenceByIDForModel(evidence []qualityEvidenceSample, model string) map[string]qualityEvidenceSample {
	out := map[string]qualityEvidenceSample{}
	for _, ev := range evidence {
		if ev.ID == "" {
			continue
		}
		if ev.Model != model {
			continue
		}
		out[ev.ID] = ev
	}
	return out
}

func qualityEngineWeights(spec aiopsv1alpha1.AIQualityScoreWeights) qualityengine.Weights {
	w := qualityengine.DefaultWeights()
	if spec.Correctness != nil {
		w.Correctness = *spec.Correctness
	}
	if spec.Reliability != nil {
		w.Reliability = *spec.Reliability
	}
	if spec.Latency != nil {
		w.Latency = *spec.Latency
	}
	if spec.Semantic != nil {
		w.Semantic = *spec.Semantic
	}
	if spec.Judged != nil {
		w.Judged = *spec.Judged
	}
	return w
}

func statisticalConfig(spec aiopsv1alpha1.AIQualityStatisticalSpec) qualitystats.NonInferiorityConfig {
	cfg := qualitystats.DefaultNonInferiorityConfig()
	if spec.NonInferiorityDelta > 0 {
		cfg.Delta = spec.NonInferiorityDelta
	}
	if spec.ConfidenceLevel > 0 {
		cfg.ConfidenceLevel = spec.ConfidenceLevel
	}
	if spec.Power > 0 {
		cfg.Power = spec.Power
	}
	if spec.BaselineSuccessRate > 0 {
		cfg.BaselineSuccessRate = spec.BaselineSuccessRate
	}
	return cfg
}

func statisticalWeights(spec aiopsv1alpha1.AIQualityStatisticalSpec) qualitystats.CompositeWeights {
	w := qualitystats.DefaultCompositeWeights()
	if spec.CompositeWeights.Quality != nil {
		w.Quality = *spec.CompositeWeights.Quality
	}
	if spec.CompositeWeights.ErrorRate != nil {
		w.ErrorRate = *spec.CompositeWeights.ErrorRate
	}
	if spec.CompositeWeights.LatencyP95 != nil {
		w.LatencyP95 = *spec.CompositeWeights.LatencyP95
	}
	if spec.CompositeWeights.Cost != nil {
		w.Cost = *spec.CompositeWeights.Cost
	}
	return w
}

func hysteresisConfig(spec aiopsv1alpha1.AIQualityStatisticalSpec) qualitystats.HysteresisConfig {
	cfg := qualitystats.DefaultHysteresisConfig()
	if spec.HysteresisEnterScore > 0 {
		cfg.EnterSafe = spec.HysteresisEnterScore
	}
	if spec.HysteresisExitScore > 0 {
		cfg.ExitSafe = spec.HysteresisExitScore
	}
	return cfg
}

func statisticalModeEnabled(spec aiopsv1alpha1.AIQualityStatisticalSpec) bool {
	return spec.NonInferiorityDelta > 0 ||
		spec.ConfidenceLevel > 0 ||
		spec.Power > 0 ||
		spec.BaselineSuccessRate > 0 ||
		spec.HysteresisEnterScore > 0 ||
		spec.HysteresisExitScore > 0 ||
		spec.CompositeWeights.Quality != nil ||
		spec.CompositeWeights.ErrorRate != nil ||
		spec.CompositeWeights.LatencyP95 != nil ||
		spec.CompositeWeights.Cost != nil
}

func defaultedMinSamples(v int32) int32 {
	if v <= 0 {
		return 1
	}
	return v
}

func defaultedTolerance(v int32) int32 {
	if v <= 0 {
		return 3
	}
	return v
}

func qualityWeightsToAPI(w qualityengine.Weights) aiopsv1alpha1.AIQualityWeightsUsed {
	return aiopsv1alpha1.AIQualityWeightsUsed{
		Correctness: roundQuality(w.Correctness),
		Reliability: roundQuality(w.Reliability),
		Latency:     roundQuality(w.Latency),
		Semantic:    roundQuality(w.Semantic),
		Judged:      roundQuality(w.Judged),
	}
}

func qualityBreakdownToAPI(b qualityengine.DimensionScores) aiopsv1alpha1.AIQualityScoreBreakdown {
	return aiopsv1alpha1.AIQualityScoreBreakdown{
		Correctness: roundQuality(b.Correctness),
		Reliability: roundQuality(b.Reliability),
		Latency:     roundQuality(b.Latency),
		Semantic:    roundQuality(b.Semantic),
		Judged:      roundQuality(b.Judged),
	}
}

func qualityDimensions(b qualityengine.DimensionScores, overall float64, w qualityengine.Weights) []aiopsv1alpha1.AIQualityDimensionStatus {
	return []aiopsv1alpha1.AIQualityDimensionStatus{
		{Name: "correctness", Score: roundQuality(b.Correctness), Weight: roundQuality(w.Correctness)},
		{Name: "reliability", Score: roundQuality(b.Reliability), Weight: roundQuality(w.Reliability)},
		{Name: "latency", Score: roundQuality(b.Latency), Weight: roundQuality(w.Latency)},
		{Name: "semantic", Score: roundQuality(b.Semantic), Weight: roundQuality(w.Semantic)},
		{Name: "judged", Score: roundQuality(b.Judged), Weight: roundQuality(w.Judged)},
		{Name: "overall", Score: roundQuality(overall), Weight: 1},
	}
}

func emitQualityScoreMetrics(gate *aiopsv1alpha1.AIQualityGate, provider, model string) {
	if provider == "" {
		provider = "unknown"
	}
	for _, d := range gate.Status.Dimensions {
		metrics.QualityScore.WithLabelValues(gate.Spec.Target.Namespace, gate.Spec.Target.Application, provider, model, d.Name).Set(d.Score)
	}
}

func evidencePassSample(samples []qualityengine.EvidenceSample) qualitystats.BernoulliSample {
	out := qualitystats.BernoulliSample{Total: len(samples)}
	for _, sample := range samples {
		if qualityengine.ReferenceCorrectnessScore(sample.Expected, sample.Actual) >= 80 {
			out.Successes++
		}
	}
	return out
}

func (r *AIQualityGateReconciler) writeScoreEvidence(ctx context.Context, gate *aiopsv1alpha1.AIQualityGate, comparison qualityengine.Comparison) error {
	if gate.Spec.EvidenceRef == nil {
		return nil
	}
	ref := *gate.Spec.EvidenceRef
	namespace := ref.Namespace
	if namespace == "" {
		namespace = gate.Namespace
	}
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &cm); err != nil {
		return fmt.Errorf("read evidence ConfigMap %s/%s for score audit: %w", namespace, ref.Name, err)
	}
	payload := qualityScoreEvidence{
		GoldenDatasetRef: gate.Spec.GoldenDatasetRef,
		SourceModel:      gate.Spec.SourceModel,
		CandidateModel:   gate.Spec.CandidateModel,
		Source:           comparison.Source,
		Candidate:        comparison.Candidate,
		Verdict:          comparison.Verdict,
		TolerancePoints:  comparison.Tolerance,
		GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize quality score evidence: %w", err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["quality-score.json"] = string(raw)
	if err := r.Update(ctx, &cm); err != nil {
		return fmt.Errorf("write quality score evidence ConfigMap %s/%s: %w", namespace, ref.Name, err)
	}
	return nil
}

func (r *AIQualityGateReconciler) updateCandidateModelQuality(ctx context.Context, gate *aiopsv1alpha1.AIQualityGate) error {
	var models aiopsv1alpha1.AIModelList
	if err := r.List(ctx, &models, client.InNamespace(gate.Namespace)); err != nil {
		return err
	}
	now := metav1.Now()
	for i := range models.Items {
		model := &models.Items[i]
		if model.Spec.ModelName != gate.Spec.CandidateModel {
			continue
		}
		model.Status.LastQualityScore = gate.Status.QualityScore
		model.Status.LastEvaluatedAt = &now
		if err := r.Status().Update(ctx, model); err != nil {
			return err
		}
	}
	return nil
}

func (r *AIQualityGateReconciler) providerForModel(ctx context.Context, namespace, modelName string) string {
	var models aiopsv1alpha1.AIModelList
	if err := r.List(ctx, &models, client.InNamespace(namespace)); err != nil {
		return ""
	}
	for i := range models.Items {
		if models.Items[i].Spec.ModelName == modelName {
			return models.Items[i].Spec.ProviderRef
		}
	}
	return ""
}

func hasTelemetryMissing(messages []string) bool {
	for _, msg := range messages {
		lower := strings.ToLower(msg)
		if strings.Contains(lower, "telemetry") || strings.Contains(lower, "latency") || strings.Contains(lower, "reliability") {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func roundQuality(v float64) float64 {
	return math.Round(v*1000) / 1000
}

func observeModel(samples []collectors.UsageSample, target aiopsv1alpha1.AIQualityGateTarget, model string) (modelObservation, bool) {
	obs := modelObservation{model: model}
	budgetTarget := budgetTargetForQualityGate(target)
	for _, s := range samples {
		if s.Model != model || !budgetTargetMatchesSample(s, budgetTarget) {
			continue
		}
		if obs.provider == "" {
			obs.provider = s.Provider
		}
		obs.requests += s.Requests
		obs.errors += s.Errors
		obs.inputTokens += s.InputTokens
		obs.outputTokens += s.OutputTokens
		if s.LatencyMillis > 0 && s.Requests > 0 {
			obs.latencyTelemetryObserved = true
			obs.latencyWeightedMillis += s.LatencyMillis * float64(s.Requests)
			obs.latencyWeight += float64(s.Requests)
		}
	}
	return obs, obs.requests > 0
}

func (o modelObservation) averageLatencyMillis() float64 {
	if !o.latencyTelemetryObserved || o.latencyWeight <= 0 {
		return 0
	}
	return o.latencyWeightedMillis / o.latencyWeight
}

func (r *AIQualityGateReconciler) qualityGateCostScore(ctx context.Context, namespace string, sourceObs, candidateObs modelObservation) float64 {
	cat, err := loadCatalog(ctx, r.Client, namespace)
	if err != nil {
		return 0
	}
	priceBook := cat.priceBook()
	sourcePrice, sourceOK := priceBook[sourceObs.model]
	candidatePrice, candidateOK := priceBook[candidateObs.model]
	if !sourceOK || !candidateOK {
		return 0
	}
	sourceCost := float64(sourceObs.inputTokens)/1e6*sourcePrice.InputPerMillion + float64(sourceObs.outputTokens)/1e6*sourcePrice.OutputPerMillion
	candidateCost := float64(candidateObs.inputTokens)/1e6*candidatePrice.InputPerMillion + float64(candidateObs.outputTokens)/1e6*candidatePrice.OutputPerMillion
	return qualitystats.CostScore(sourceCost, candidateCost)
}

func (r *AIQualityGateReconciler) goldenDatasetMetadata(ctx context.Context, defaultNamespace string, ref aiopsv1alpha1.ConfigMapDataReference) (string, string) {
	namespace := ref.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &cm); err != nil {
		return "", ""
	}
	raw, err := configMapData(cm, ref.Key, []string{"prompts.yaml", "prompts.yml", "prompts.json"})
	if err != nil {
		return "", ""
	}
	version := cm.Annotations["aiops.imperium.io/dataset-version"]
	if version == "" {
		version = cm.Labels["aiops.imperium.io/dataset-version"]
	}
	return version, platformcrypto.SHA256Hex([]byte(raw))
}

func budgetTargetForQualityGate(target aiopsv1alpha1.AIQualityGateTarget) aiopsv1alpha1.BudgetTarget {
	return aiopsv1alpha1.BudgetTarget{
		Namespace:   target.Namespace,
		Team:        target.Team,
		Application: target.Application,
	}
}

func (o modelObservation) errorRatePercent() float64 {
	if o.requests <= 0 {
		return 0
	}
	return float64(o.errors) * 100 / float64(o.requests)
}

func (o modelObservation) latencyMillis() float64 {
	if !o.latencyTelemetryObserved || o.latencyWeight <= 0 {
		return 0
	}
	return o.latencyWeightedMillis / o.latencyWeight
}

func (o modelObservation) toAPI() aiopsv1alpha1.AIQualityModelObservation {
	return aiopsv1alpha1.AIQualityModelObservation{
		Model:                     o.model,
		Provider:                  o.provider,
		Requests:                  o.requests,
		Errors:                    o.errors,
		ErrorRatePercent:          decimalString(o.errorRatePercent(), 3),
		ObservedLatencyMillis:     decimalString(o.latencyMillis(), 3),
		LatencyTelemetryAvailable: o.latencyTelemetryObserved,
	}
}

func (o modelObservation) toQualityTelemetry(evidence []qualityEvidenceSample, model string) qualityengine.Telemetry {
	return qualityengine.Telemetry{
		Requests:        o.requests,
		Errors:          o.errors,
		InvalidJSON:     invalidJSONCount(evidence, model),
		LatencyMillis:   o.latencyMillis(),
		LatencyObserved: o.latencyTelemetryObserved,
	}
}

func invalidJSONCount(evidence []qualityEvidenceSample, model string) int64 {
	var count int64
	for _, ev := range evidence {
		if ev.Model != model || ev.SchemaValid == nil {
			continue
		}
		if !*ev.SchemaValid {
			count++
		}
	}
	return count
}

func appendOperationalChecks(failures, pending []string, source modelObservation, sourceOK bool, candidate modelObservation, candidateOK bool, checks aiopsv1alpha1.AIQualityRequiredChecks) ([]string, []string) {
	if !candidateOK {
		pending = append(pending, fmt.Sprintf("no telemetry observed for candidate model %q in target application", candidate.model))
	}
	if checks.MaxErrorRatePercent > 0 && candidateOK {
		if candidate.errorRatePercent() > float64(checks.MaxErrorRatePercent) {
			failures = append(failures, fmt.Sprintf("candidate error rate %.3f%% exceeds threshold %d%%", candidate.errorRatePercent(), checks.MaxErrorRatePercent))
		}
	}
	if checks.MaxLatencyIncreasePercent > 0 {
		if !sourceOK || !candidateOK || !source.latencyTelemetryObserved || !candidate.latencyTelemetryObserved {
			pending = append(pending, "latency regression check requires observed latency for both source and candidate models")
		} else {
			src := source.latencyMillis()
			cand := candidate.latencyMillis()
			if src <= 0 {
				pending = append(pending, "latency regression check requires positive source latency")
			} else {
				increase := (cand - src) * 100 / src
				if increase > float64(checks.MaxLatencyIncreasePercent) {
					failures = append(failures, fmt.Sprintf("candidate latency increase %.3f%% exceeds threshold %d%%", increase, checks.MaxLatencyIncreasePercent))
				}
			}
		}
	}
	if checks.MaxRetryIncreasePercent > 0 {
		pending = append(pending, "retry regression check is configured but retry telemetry is not available yet")
	}
	return failures, pending
}

func canaryStatus(c aiopsv1alpha1.AIQualityCanarySpec) string {
	if !c.Enabled {
		return "Disabled"
	}
	return "Configured"
}

func conditionStatusForGate(phase aiopsv1alpha1.AIQualityGatePhase) metav1.ConditionStatus {
	if phase == aiopsv1alpha1.AIQualityGatePassed {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func qualityGateMessage(gate *aiopsv1alpha1.AIQualityGate) string {
	target := gate.Spec.Target.Namespace + "/" + gate.Spec.Target.Application
	switch gate.Status.Phase {
	case aiopsv1alpha1.AIQualityGatePassed:
		return fmt.Sprintf("quality gate passed for %s: candidate %q can be considered safe", target, gate.Spec.CandidateModel)
	case aiopsv1alpha1.AIQualityGateFailed:
		return fmt.Sprintf("quality gate failed for %s: %d failed check(s)", target, gate.Status.FailedChecks)
	default:
		return fmt.Sprintf("quality gate pending for %s: %d missing/insufficient signal(s)", target, len(gate.Status.FailureMessages))
	}
}

func qualityGatePassedValue(phase aiopsv1alpha1.AIQualityGatePhase) float64 {
	if phase == aiopsv1alpha1.AIQualityGatePassed {
		return 1
	}
	return 0
}

func reasonForReadErr(err error) string {
	if apierrors.IsNotFound(err) {
		return aiopsv1alpha1.ReasonReferenceNotFound
	}
	return aiopsv1alpha1.ReasonReconcileError
}

func (r *AIQualityGateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIQualityGate{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
