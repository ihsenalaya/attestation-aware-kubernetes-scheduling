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

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ConfigMapDataReference points to one key in a ConfigMap. Namespace is optional
// and defaults to the owning resource namespace.
type ConfigMapDataReference struct {
	// Name of the ConfigMap.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the ConfigMap. Defaults to the owning resource namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Key inside the ConfigMap. Defaults are resource-specific.
	// +optional
	Key string `json:"key,omitempty"`
}

// AIQualityRequiredChecks configures the deterministic and operational gates a
// candidate model must satisfy before it can be considered safe for an app.
type AIQualityRequiredChecks struct {
	// SchemaValid requires golden-run evidence that candidate responses match the expected schema.
	// +optional
	SchemaValid bool `json:"schemaValid,omitempty"`

	// NoUnexpectedRefusal requires evidence that the candidate did not refuse valid prompts.
	// +optional
	NoUnexpectedRefusal bool `json:"noUnexpectedRefusal,omitempty"`

	// NoSensitiveDataLeak requires evidence that candidate responses did not leak sensitive data.
	// +optional
	NoSensitiveDataLeak bool `json:"noSensitiveDataLeak,omitempty"`

	// RequiredKeywords requires evidence that expected domain terms are present.
	// +optional
	RequiredKeywords []string `json:"requiredKeywords,omitempty"`

	// MaxErrorRatePercent is the maximum observed candidate HTTP error rate.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MaxErrorRatePercent int32 `json:"maxErrorRatePercent,omitempty"`

	// MaxLatencyIncreasePercent is the maximum candidate latency increase versus the source model.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxLatencyIncreasePercent int32 `json:"maxLatencyIncreasePercent,omitempty"`

	// MaxRetryIncreasePercent is reserved for retry telemetry when available.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxRetryIncreasePercent int32 `json:"maxRetryIncreasePercent,omitempty"`
}

// AIQualityScoreWeights configures the composite quality score weights. Nil
// fields use the product defaults; explicit zero disables a dimension.
type AIQualityScoreWeights struct {
	// Correctness is the deterministic golden-dataset weight.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Correctness *float64 `json:"correctness,omitempty"`

	// Reliability is the real telemetry error/timeout/invalid JSON weight.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Reliability *float64 `json:"reliability,omitempty"`

	// Latency is the real telemetry latency weight.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Latency *float64 `json:"latency,omitempty"`

	// Semantic is the local embedding/reference similarity weight.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Semantic *float64 `json:"semantic,omitempty"`

	// Judged is the sovereign LLM judge weight.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Judged *float64 `json:"judged,omitempty"`
}

// AIQualityStatisticalCompositeWeights configures the higher-level audited
// composite score in [0,1] used for the safe/risk decision.
type AIQualityStatisticalCompositeWeights struct {
	// +optional
	// +kubebuilder:validation:Minimum=0
	Quality *float64 `json:"quality,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	ErrorRate *float64 `json:"errorRate,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	LatencyP95 *float64 `json:"latencyP95,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	Cost *float64 `json:"cost,omitempty"`
}

// AIQualityStatisticalSpec configures the auditable statistical gate.
type AIQualityStatisticalSpec struct {
	// NonInferiorityDelta is the tolerated candidate regression margin.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	NonInferiorityDelta float64 `json:"nonInferiorityDelta,omitempty"`

	// ConfidenceLevel is the one-sided confidence level, for example 0.95.
	// +optional
	// +kubebuilder:validation:Minimum=0.5
	// +kubebuilder:validation:Maximum=0.999
	ConfidenceLevel float64 `json:"confidenceLevel,omitempty"`

	// Power is the target statistical power.
	// +optional
	// +kubebuilder:validation:Minimum=0.5
	// +kubebuilder:validation:Maximum=0.999
	Power float64 `json:"power,omitempty"`

	// BaselineSuccessRate is the expected source pass rate used for sample sizing.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	BaselineSuccessRate float64 `json:"baselineSuccessRate,omitempty"`

	// HysteresisEnterScore is the composite score required to enter candidate-safe.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	HysteresisEnterScore float64 `json:"hysteresisEnterScore,omitempty"`

	// HysteresisExitScore is the composite score below which a safe gate becomes risky.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	HysteresisExitScore float64 `json:"hysteresisExitScore,omitempty"`

	// CompositeWeights configures the [0,1] audited composite score.
	// +optional
	CompositeWeights AIQualityStatisticalCompositeWeights `json:"compositeWeights,omitempty"`
}

// AIQualityJudgeSpec configures the optional sovereign LLM judge.
type AIQualityJudgeSpec struct {
	// Enabled includes the judged dimension when a sovereign judge model is available.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ModelRef references the AIModel used as judge. The controller must ensure it
	// is sovereignty-compliant before using it.
	// +optional
	ModelRef string `json:"modelRef,omitempty"`
}

// AIQualityEvaluationSpec configures the real gateway-backed evaluation Job.
type AIQualityEvaluationSpec struct {
	// Endpoint is the OpenAI-compatible chat completions endpoint used by the
	// evaluator Job. It must be the production data-plane path, not the metrics
	// endpoint. Example:
	// http://greenops-aigw.envoy-gateway-system.svc.cluster.local:80/v1/chat/completions
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Image overrides the evaluator image. Defaults to the running operator
	// image so Kind demos evaluate with the same build that reconciles the gate.
	// +optional
	Image string `json:"image,omitempty"`

	// MaxTokens limits each golden prompt completion.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxTokens int32 `json:"maxTokens,omitempty"`

	// TimeoutSeconds is the per-request gateway timeout.
	// +optional
	// +kubebuilder:validation:Minimum=1
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// AIQualityCanarySpec describes the intended canary before a complete reroute.
// The first implementation records the desired canary contract; traffic actuation
// is handled by routing work planned separately.
type AIQualityCanarySpec struct {
	// Enabled requires canary validation before a full reroute.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Percent of traffic intended for the candidate model during canary.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Percent int32 `json:"percent,omitempty"`

	// Duration of the canary window, for example "30m".
	// +optional
	Duration string `json:"duration,omitempty"`
}

// AIQualityRollbackSpec describes rollback thresholds after a candidate is enabled.
type AIQualityRollbackSpec struct {
	// Enabled allows automatic rollback once routing actuation is wired.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// OnErrorRatePercent is the error-rate threshold that should trigger rollback.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	OnErrorRatePercent int32 `json:"onErrorRatePercent,omitempty"`

	// OnLatencyIncreasePercent is the latency regression threshold that should trigger rollback.
	// +optional
	// +kubebuilder:validation:Minimum=0
	OnLatencyIncreasePercent int32 `json:"onLatencyIncreasePercent,omitempty"`
}

// AIQualityGateTarget identifies exactly one application protected by a quality gate.
type AIQualityGateTarget struct {
	// Namespace is the application namespace.
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`

	// Application is the application label/value in telemetry samples.
	// +kubebuilder:validation:MinLength=1
	Application string `json:"application"`

	// Team optionally narrows telemetry to one team when the same app name is shared.
	// +optional
	Team string `json:"team,omitempty"`
}

// AIQualityGateSpec defines the desired quality validation for one application.
type AIQualityGateSpec struct {
	// Target is the application this gate protects. The intended granularity is
	// one AIQualityGate per application.
	Target AIQualityGateTarget `json:"target"`

	// SourceModel is the current provider-side model identifier.
	// +kubebuilder:validation:MinLength=1
	SourceModel string `json:"sourceModel"`

	// CandidateModel is the provider-side model being evaluated as a replacement.
	// +kubebuilder:validation:MinLength=1
	CandidateModel string `json:"candidateModel"`

	// GoldenDatasetRef points to the ConfigMap holding prompts.yaml/prompts.json.
	GoldenDatasetRef ConfigMapDataReference `json:"goldenDatasetRef"`

	// EvidenceRef optionally points to deterministic golden-run results produced
	// by a CI job or future prompt runner. It is required when response-level
	// checks such as schemaValid or requiredKeywords are enabled.
	// +optional
	EvidenceRef *ConfigMapDataReference `json:"evidenceRef,omitempty"`

	// GatewayRef optionally pins telemetry to a specific AIGateway.
	// +optional
	GatewayRef string `json:"gatewayRef,omitempty"`

	// Period is the telemetry window used for operational checks.
	// +kubebuilder:validation:Enum=daily;weekly;monthly
	// +kubebuilder:default=monthly
	// +optional
	Period string `json:"period,omitempty"`

	// Weights configures the composite AI Quality Score.
	// +optional
	Weights AIQualityScoreWeights `json:"weights,omitempty"`

	// LatencyThresholdMs is the threshold used by the latency dimension: at the
	// threshold the score is 50; half threshold is 100; double threshold is 0.
	// +optional
	// +kubebuilder:validation:Minimum=1
	LatencyThresholdMs int32 `json:"latencyThresholdMs,omitempty"`

	// MinSamples is the minimum number of golden prompts required to score.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinSamples int32 `json:"minSamples,omitempty"`

	// TolerancePoints is the allowed candidate regression versus source score.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TolerancePoints int32 `json:"tolerancePoints,omitempty"`

	// Judge configures the optional sovereign LLM judge.
	// +optional
	Judge AIQualityJudgeSpec `json:"judge,omitempty"`

	// Evaluation configures the real gateway-backed evaluator Job.
	// +optional
	Evaluation AIQualityEvaluationSpec `json:"evaluation,omitempty"`

	// RequiredChecks configures the deterministic and operational guardrails.
	// +optional
	RequiredChecks AIQualityRequiredChecks `json:"requiredChecks,omitempty"`

	// Canary configures the desired canary validation.
	// +optional
	Canary AIQualityCanarySpec `json:"canary,omitempty"`

	// Rollback configures rollback thresholds.
	// +optional
	Rollback AIQualityRollbackSpec `json:"rollback,omitempty"`

	// Statistical configures non-inferiority, sample-size and hysteresis.
	// +optional
	Statistical AIQualityStatisticalSpec `json:"statistical,omitempty"`
}

// AIQualityGatePhase is the high-level status of a quality gate.
// +kubebuilder:validation:Enum=Pending;Passed;Failed
type AIQualityGatePhase string

const (
	AIQualityGatePending AIQualityGatePhase = "Pending"
	AIQualityGatePassed  AIQualityGatePhase = "Passed"
	AIQualityGateFailed  AIQualityGatePhase = "Failed"
)

// AIQualityModelObservation summarizes live telemetry for one model in the target app.
type AIQualityModelObservation struct {
	// Model is the provider-side model identifier.
	Model string `json:"model,omitempty"`
	// Provider is the observed or catalogued provider for the model.
	// +optional
	Provider string `json:"provider,omitempty"`
	// Requests observed for the target app/model.
	Requests int64 `json:"requests,omitempty"`
	// Errors observed for the target app/model.
	Errors int64 `json:"errors,omitempty"`
	// ErrorRatePercent is Errors / Requests * 100, serialized as a decimal string.
	ErrorRatePercent string `json:"errorRatePercent,omitempty"`
	// ObservedLatencyMillis is the measured mean latency when telemetry exists.
	ObservedLatencyMillis string `json:"observedLatencyMillis,omitempty"`
	// LatencyTelemetryAvailable is true when ObservedLatencyMillis is real telemetry.
	LatencyTelemetryAvailable bool `json:"latencyTelemetryAvailable,omitempty"`
}

// AIQualityScoreBreakdown contains normalized 0..100 dimension scores.
type AIQualityScoreBreakdown struct {
	// Correctness is the deterministic golden-dataset score.
	// +optional
	Correctness float64 `json:"correctness,omitempty"`

	// Reliability is the real telemetry reliability score.
	// +optional
	Reliability float64 `json:"reliability,omitempty"`

	// Latency is the real telemetry latency score.
	// +optional
	Latency float64 `json:"latency,omitempty"`

	// Semantic is the reference-based semantic score.
	// +optional
	Semantic float64 `json:"semantic,omitempty"`

	// Judged is the optional sovereign judge score.
	// +optional
	Judged float64 `json:"judged,omitempty"`
}

// AIQualityWeightsUsed records the normalized weights used for scoring.
type AIQualityWeightsUsed struct {
	// +optional
	Correctness float64 `json:"correctness,omitempty"`
	// +optional
	Reliability float64 `json:"reliability,omitempty"`
	// +optional
	Latency float64 `json:"latency,omitempty"`
	// +optional
	Semantic float64 `json:"semantic,omitempty"`
	// +optional
	Judged float64 `json:"judged,omitempty"`
}

// AIQualityDimensionStatus is one radar-ready score dimension.
type AIQualityDimensionStatus struct {
	// Name is one of correctness, reliability, latency, semantic, judged or overall.
	Name string `json:"name"`

	// Score is normalized in [0,100].
	Score float64 `json:"score"`

	// Weight is the normalized weight used for this dimension. Overall has weight 1.
	// +optional
	Weight float64 `json:"weight,omitempty"`
}

// AIQualityGateStatus defines the observed state of AIQualityGate.
type AIQualityGateStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase summarizes the gate result.
	// +optional
	Phase AIQualityGatePhase `json:"phase,omitempty"`

	// Verdict is a stable machine-readable decision: candidate-safe,
	// candidate-risk or insufficient-data.
	// +optional
	Verdict string `json:"verdict,omitempty"`

	// CheckedSamples is the number of golden dataset prompts parsed.
	// +optional
	CheckedSamples int32 `json:"checkedSamples,omitempty"`

	// FailedChecks is the number of failed deterministic or operational checks.
	// +optional
	FailedChecks int32 `json:"failedChecks,omitempty"`

	// QualityScore is the candidate composite score in [0,100].
	// +optional
	QualityScore float64 `json:"qualityScore,omitempty"`

	// CompositeScore is the higher-level audited decision score in [0,1].
	// +optional
	CompositeScore float64 `json:"compositeScore,omitempty"`

	// DatasetVersion is the declared golden dataset version, when annotated.
	// +optional
	DatasetVersion string `json:"datasetVersion,omitempty"`

	// DatasetHash is the SHA-256 of the golden dataset content.
	// +optional
	DatasetHash string `json:"datasetHash,omitempty"`

	// RequiredSamples is the per-arm sample size required by the statistical test.
	// +optional
	RequiredSamples int32 `json:"requiredSamples,omitempty"`

	// ObservedSourceSamples is the number of source samples included in the statistical test.
	// +optional
	ObservedSourceSamples int32 `json:"observedSourceSamples,omitempty"`

	// ObservedCandidateSamples is the number of candidate samples included in the statistical test.
	// +optional
	ObservedCandidateSamples int32 `json:"observedCandidateSamples,omitempty"`

	// NonInferiorityLowerBound is the lower confidence bound on candidate-source difference.
	// +optional
	NonInferiorityLowerBound float64 `json:"nonInferiorityLowerBound,omitempty"`

	// ScoreBreakdown contains the candidate score components.
	// +optional
	ScoreBreakdown AIQualityScoreBreakdown `json:"scoreBreakdown,omitempty"`

	// WeightsUsed records normalized score weights.
	// +optional
	WeightsUsed AIQualityWeightsUsed `json:"weightsUsed,omitempty"`

	// Samples is the number of candidate evidence samples used by the quality score.
	// +optional
	Samples int32 `json:"samples,omitempty"`

	// Dimensions is a radar-ready view of score components.
	// +optional
	Dimensions []AIQualityDimensionStatus `json:"dimensions,omitempty"`

	// EvidenceRef echoes the evidence ConfigMap used or written for audit.
	// +optional
	EvidenceRef *ConfigMapDataReference `json:"evidenceRef,omitempty"`

	// EvaluationJobName is the Kubernetes Job producing gateway-backed evidence.
	// +optional
	EvaluationJobName string `json:"evaluationJobName,omitempty"`

	// EvaluationJobPhase is Pending, Running, Succeeded or Failed.
	// +optional
	EvaluationJobPhase string `json:"evaluationJobPhase,omitempty"`

	// FailureMessages explains failed or missing checks for human review.
	// +optional
	FailureMessages []string `json:"failureMessages,omitempty"`

	// CanaryStatus summarizes the canary contract state.
	// +optional
	CanaryStatus string `json:"canaryStatus,omitempty"`

	// SourceObservation is the observed source model telemetry for the target app.
	// +optional
	SourceObservation *AIQualityModelObservation `json:"sourceObservation,omitempty"`

	// CandidateObservation is the observed candidate model telemetry for the target app.
	// +optional
	CandidateObservation *AIQualityModelObservation `json:"candidateObservation,omitempty"`

	// Conditions represent the latest available observations of the gate state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=aiqgate
//+kubebuilder:printcolumn:name="TargetNS",type=string,JSONPath=`.spec.target.namespace`
//+kubebuilder:printcolumn:name="Application",type=string,JSONPath=`.spec.target.application`
//+kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceModel`
//+kubebuilder:printcolumn:name="Candidate",type=string,JSONPath=`.spec.candidateModel`
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Verdict",type=string,JSONPath=`.status.verdict`
//+kubebuilder:printcolumn:name="Score",type=number,JSONPath=`.status.qualityScore`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIQualityGate validates whether a candidate model is safe enough for one app.
type AIQualityGate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIQualityGateSpec   `json:"spec,omitempty"`
	Status AIQualityGateStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIQualityGateList contains a list of AIQualityGate.
type AIQualityGateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIQualityGate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIQualityGate{}, &AIQualityGateList{})
}
