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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// GOVARReservationMethod identifies an executable reservation policy.
// +kubebuilder:validation:Enum=strict_provider_cap;mean;fixed_margin;fixed_quantile;adaptive_quantile;govar_fixed_cohort
type GOVARReservationMethod string

const (
	GOVARReservationStrictProviderCap GOVARReservationMethod = "strict_provider_cap"
	GOVARReservationMean              GOVARReservationMethod = "mean"
	GOVARReservationFixedMargin       GOVARReservationMethod = "fixed_margin"
	GOVARReservationFixedQuantile     GOVARReservationMethod = "fixed_quantile"
	GOVARReservationAdaptiveQuantile  GOVARReservationMethod = "adaptive_quantile"
	GOVARReservationFixedCohort       GOVARReservationMethod = "govar_fixed_cohort"
)

// +kubebuilder:validation:XValidation:rule="self.method != 'mean' || has(self.meanOutputTokens)",message="meanOutputTokens is required for mean reservation"
// +kubebuilder:validation:XValidation:rule="self.method != 'fixed_margin' || (has(self.meanOutputTokens) && has(self.marginOutputTokens))",message="meanOutputTokens and marginOutputTokens are required for fixed_margin reservation"
// +kubebuilder:validation:XValidation:rule="self.method != 'fixed_quantile' || has(self.fixedQuantileOutputTokens)",message="fixedQuantileOutputTokens is required for fixed_quantile reservation"
// GOVARReservationPolicy contains typed token reservations. Pointer token values
// distinguish a configured zero from a missing method parameter.
type GOVARReservationPolicy struct {
	Method GOVARReservationMethod `json:"method"`

	// MeanOutputTokens is the development-only mean estimate.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MeanOutputTokens *int64 `json:"meanOutputTokens,omitempty"`

	// MarginOutputTokens is added to the mean for fixed_margin.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MarginOutputTokens *int64 `json:"marginOutputTokens,omitempty"`

	// FixedQuantileOutputTokens is the frozen offline quantile estimate.
	// +optional
	// +kubebuilder:validation:Minimum=0
	FixedQuantileOutputTokens *int64 `json:"fixedQuantileOutputTokens,omitempty"`

	// AdaptiveQuantileOutputTokens is retained only as a migration hint. Adaptive
	// admission uses status.govar.calibration.adaptiveOutputTokens from the
	// digest-bound controller artifact and never executes this mutable spec value.
	// +optional
	// +kubebuilder:validation:Minimum=0
	AdaptiveQuantileOutputTokens *int64 `json:"adaptiveQuantileOutputTokens,omitempty"`
}

// GOVARCalibrationPolicy binds adaptive reservation to a versioned immutable
// calibration artifact and explicit freshness/support requirements.
type GOVARCalibrationPolicy struct {
	// ArtifactRef is a stable logical identifier for the frozen artifact.
	// +kubebuilder:validation:MinLength=1
	ArtifactRef string `json:"artifactRef"`

	// ArtifactSHA256 is the expected digest of the deterministically reconstructed artifact.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ArtifactSHA256 string `json:"artifactSHA256"`

	// CalibrationDataRef is diagnostic-only legacy input. Production evidence
	// never consumes ConfigMap rows and uses RegistryID instead.
	// +optional
	CalibrationDataRef string `json:"calibrationDataRef,omitempty"`

	// CalibrationInputSHA256 binds the canonical eligible calibration observations.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	CalibrationInputSHA256 string `json:"calibrationInputSHA256"`

	// MonitoringDataRef is diagnostic-only legacy input.
	// +optional
	MonitoringDataRef string `json:"monitoringDataRef,omitempty"`

	// RegistryID identifies the immutable PostgreSQL v8 split registry.
	// +optional
	// +kubebuilder:validation:MinLength=1
	RegistryID string `json:"registryID,omitempty"`

	// Version is the calibration protocol/model version.
	// +kubebuilder:validation:MinLength=1
	Version string `json:"version"`

	// FeatureSchemaVersion prevents reuse across incompatible request features.
	// +kubebuilder:validation:MinLength=1
	FeatureSchemaVersion string `json:"featureSchemaVersion"`

	// PriceRegimeSHA256 binds the provider price snapshot used by calibration.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	PriceRegimeSHA256 string `json:"priceRegimeSHA256"`

	// CapRegimeSHA256 binds the verified output-cap evidence used by calibration.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	CapRegimeSHA256 string `json:"capRegimeSHA256"`

	// ProducerSoftwareSHA256 binds the deterministic producer implementation.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ProducerSoftwareSHA256 string `json:"producerSoftwareSHA256"`

	// CoverageTargetPPB is the requested one-sided split-conformal marginal
	// coverage under the explicitly documented exchangeability assumptions.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000000
	CoverageTargetPPB int64 `json:"coverageTargetPPB"`

	// MinimumSupport is the required number of eligible observations.
	// +kubebuilder:validation:Minimum=1
	MinimumSupport int64 `json:"minimumSupport"`

	// MaxAgeSeconds is the maximum acceptable calibration age.
	// +kubebuilder:validation:Minimum=1
	MaxAgeSeconds int64 `json:"maxAgeSeconds"`
}

// GOVARDriftDetector identifies a predeclared detector.
// +kubebuilder:validation:Enum=coverage-gap;psi;ks;adwin
type GOVARDriftDetector string

// GOVARConservativeFallback is the visible behavior after invalid calibration.
// +kubebuilder:validation:Enum=strict_provider_cap;queue;reject;abstain;require_approval
type GOVARConservativeFallback string

// GOVARDriftPolicy freezes drift detection and revalidation thresholds.
type GOVARDriftPolicy struct {
	Detector GOVARDriftDetector `json:"detector"`

	// ThresholdPPB is a detector-specific threshold represented without floats.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000000000
	ThresholdPPB int64 `json:"thresholdPPB"`

	Fallback GOVARConservativeFallback `json:"fallback"`

	// RevalidationMinimumSupport is required before calibrated claims resume.
	// +kubebuilder:validation:Minimum=1
	RevalidationMinimumSupport int64 `json:"revalidationMinimumSupport"`
}

// GOVARCohortPolicy identifies the pre-outcome registry snapshot used by the
// fixed-opportunity-cohort policy. The referenced registry producer remains
// responsible for proving that membership was frozen before outcomes.
type GOVARCohortPolicy struct {
	// RegistryRef identifies the operator-owned cohort registry snapshot.
	// +kubebuilder:validation:MinLength=1
	RegistryRef string `json:"registryRef"`

	// Size is the fixed number of arrival opportunities.
	// +kubebuilder:validation:Minimum=1
	Size int64 `json:"size"`

	// OpportunitySetHash is a lower-case SHA-256 digest of fixed opportunity identities/features.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	OpportunitySetHash string `json:"opportunitySetHash"`

	// WeightsHash is a lower-case SHA-256 digest of the non-negative fixed weights.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	WeightsHash string `json:"weightsHash"`

	// FrozenAt is when membership, features, and weights became immutable.
	FrozenAt metav1.Time `json:"frozenAt"`
}

// GOVARRiskPolicy freezes the tenant risk budget and allocation mechanism.
type GOVARRiskPolicy struct {
	// TenantRiskPPB is alpha_K in parts per billion.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000000000
	TenantRiskPPB int64 `json:"tenantRiskPPB"`

	// Allocation selects uniform or registry-fixed weights.
	// +kubebuilder:validation:Enum=uniform;fixed-weights
	Allocation string `json:"allocation"`
}

// +kubebuilder:validation:XValidation:rule="!(self.reservation.method in ['adaptive_quantile', 'govar_fixed_cohort']) || has(self.calibration)",message="calibration is required for adaptive reservation"
// +kubebuilder:validation:XValidation:rule="self.reservation.method != 'govar_fixed_cohort' || (has(self.cohort) && has(self.risk))",message="cohort and risk are required for govar_fixed_cohort"
// GOVARRoutingPolicySpec is the safety-critical, typed admission policy. An
// absent block means annotations are not silently promoted to typed authority.
type GOVARRoutingPolicySpec struct {
	Reservation GOVARReservationPolicy `json:"reservation"`

	// Calibration is required for adaptive methods.
	// +optional
	Calibration *GOVARCalibrationPolicy `json:"calibration,omitempty"`

	Drift GOVARDriftPolicy `json:"drift"`

	// Cohort is required for govar_fixed_cohort.
	// +optional
	Cohort *GOVARCohortPolicy `json:"cohort,omitempty"`

	// Risk is required for govar_fixed_cohort.
	// +optional
	Risk *GOVARRiskPolicy `json:"risk,omitempty"`
}

// GOVARCalibrationStatus is controller-produced evidence for an adaptive estimate.
type GOVARCalibrationStatus struct {
	EvidenceSource         string `json:"evidenceSource"`
	PublicationSHA256      string `json:"publicationSHA256,omitempty"`
	SourceResourceVersion  string `json:"sourceResourceVersion,omitempty"`
	ArtifactRef            string `json:"artifactRef"`
	Version                string `json:"version"`
	ArtifactSHA256         string `json:"artifactSHA256"`
	CalibrationInputSHA256 string `json:"calibrationInputSHA256"`
	FeatureSchemaVersion   string `json:"featureSchemaVersion"`
	PriceRegimeSHA256      string `json:"priceRegimeSHA256"`
	CapRegimeSHA256        string `json:"capRegimeSHA256"`
	ProducerSoftwareSHA256 string `json:"producerSoftwareSHA256"`
	CoverageTargetPPB      int64  `json:"coverageTargetPPB"`
	EmpiricalCoveragePPB   int64  `json:"empiricalCoveragePPB"`
	CalibrationMethod      string `json:"calibrationMethod"`
	// ExchangeableMarginalCoverageLowerPPB is the finite-sample rank bound.
	// It is invalidated by the visible drift/conservative-mode state.
	ExchangeableMarginalCoverageLowerPPB int64  `json:"exchangeableMarginalCoverageLowerPPB"`
	ConformalRank                        int64  `json:"conformalRank"`
	CoverageBoundKind                    string `json:"coverageBoundKind,omitempty"`
	CoverageBoundNumerator               int64  `json:"coverageBoundNumerator,omitempty"`
	CoverageBoundDenominator             int64  `json:"coverageBoundDenominator,omitempty"`
	CoverageIntervalLowerPPB             int64  `json:"coverageIntervalLowerPPB,omitempty"`
	CoverageIntervalUpperPPB             int64  `json:"coverageIntervalUpperPPB,omitempty"`
	CoverageConfidencePPB                int64  `json:"coverageConfidencePPB,omitempty"`
	FeatureRegimeSHA256                  string `json:"featureRegimeSHA256,omitempty"`
	SplitOpportunityRegimeSHA256         string `json:"splitOpportunityRegimeSHA256,omitempty"`
	CohortRegimeSHA256                   string `json:"cohortRegimeSHA256,omitempty"`

	// Support is the number of eligible observations.
	// +kubebuilder:validation:Minimum=0
	Support int64 `json:"support"`

	// AdaptiveOutputTokens is the validated upper reservation estimate.
	// +kubebuilder:validation:Minimum=0
	AdaptiveOutputTokens int64 `json:"adaptiveOutputTokens"`

	Valid                  bool        `json:"valid"`
	Reason                 string      `json:"reason,omitempty"`
	CalibrationWindowStart metav1.Time `json:"calibrationWindowStart"`
	CalibrationWindowEnd   metav1.Time `json:"calibrationWindowEnd"`
	ObservedAt             metav1.Time `json:"observedAt"`
}

// GOVARDriftStatus makes calibration invalidation and fallback externally visible.
type GOVARDriftStatus struct {
	DriftSHA256           string             `json:"driftSHA256,omitempty"`
	Detected              bool               `json:"detected"`
	ConservativeMode      bool               `json:"conservativeMode"`
	Detector              GOVARDriftDetector `json:"detector"`
	ThresholdPPB          int64              `json:"thresholdPPB"`
	MonitoringInputSHA256 string             `json:"monitoringInputSHA256,omitempty"`
	Support               int64              `json:"support"`
	EmpiricalCoveragePPB  int64              `json:"empiricalCoveragePPB"`
	ConfidencePPB         int64              `json:"confidencePPB,omitempty"`
	IntervalLowerPPB      int64              `json:"intervalLowerPPB,omitempty"`
	IntervalUpperPPB      int64              `json:"intervalUpperPPB,omitempty"`
	Reason                string             `json:"reason,omitempty"`
	MonitoringWindowStart metav1.Time        `json:"monitoringWindowStart,omitempty"`
	MonitoringWindowEnd   metav1.Time        `json:"monitoringWindowEnd,omitempty"`
	ObservedAt            metav1.Time        `json:"observedAt"`
}

// GOVARRoutingPolicyStatus contains controller-owned calibration/drift evidence.
type GOVARRoutingPolicyStatus struct {
	// Calibration is absent until the configured artifact has been evaluated.
	// +optional
	Calibration *GOVARCalibrationStatus `json:"calibration,omitempty"`

	// Drift is absent until the configured detector has evaluated a window.
	// +optional
	Drift *GOVARDriftStatus `json:"drift,omitempty"`
}

// AIRoutingPolicyGuardrails configures the minimum quality, latency and
// sovereignty requirements that must hold before any automatic reroute is applied.
type AIRoutingPolicyGuardrails struct {
	// MinQualityScore is the minimum routing score (0–1) a candidate model must
	// reach before it can be automatically selected (default 0.70).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	MinQualityScore float64 `json:"minQualityScore,omitempty"`

	// MaxLatencyMillis is the maximum measured mean latency (ms) allowed for a
	// candidate model. Models above this threshold are rejected.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxLatencyMillis int32 `json:"maxLatencyMillis,omitempty"`

	// RequireSovereigntyCompliance, when true, restricts candidate selection to
	// sovereignty-compliant models only (default true).
	// +optional
	// +kubebuilder:default=true
	RequireSovereigntyCompliance bool `json:"requireSovereigntyCompliance,omitempty"`
}

// AIRoutingPolicyCanary configures a canary phase before full promotion.
type AIRoutingPolicyCanary struct {
	// Enabled requires canary validation before full reroute (default false).
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Percent of traffic to send to the candidate during canary (1–50).
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=50
	Percent int32 `json:"percent,omitempty"`
}

// AIRoutingPolicySpec defines the desired automatic routing behaviour.
type AIRoutingPolicySpec struct {
	// Objective is the primary optimisation dimension.
	// +kubebuilder:validation:Enum=cost;quality;latency
	// +kubebuilder:default=cost
	// +optional
	Objective string `json:"objective,omitempty"`

	// Guardrails defines the minimum bar that any candidate must clear.
	// +optional
	Guardrails AIRoutingPolicyGuardrails `json:"guardrails,omitempty"`

	// Canary configures the canary phase (declared intent; actuation is wired
	// through AIChangeRequest approval when human-in-the-loop is active).
	// +optional
	Canary AIRoutingPolicyCanary `json:"canary,omitempty"`

	// GOVAR is the typed reserve/calibrate/drift/risk policy. Historical
	// annotations remain readable for compatibility but are not authoritative.
	// +optional
	GOVAR *GOVARRoutingPolicySpec `json:"govar,omitempty"`
}

// AIRoutingPolicyRecommendation is one candidate model the policy considers safe.
type AIRoutingPolicyRecommendation struct {
	// Application is the workload/model pair evaluated.
	Application string `json:"application,omitempty"`
	// CurrentModel is the model currently used.
	CurrentModel string `json:"currentModel,omitempty"`
	// CandidateModel is the model recommended by the scoring engine.
	CandidateModel string `json:"candidateModel,omitempty"`
	// CandidateScore is the routing score of the candidate (higher is better).
	CandidateScore string `json:"candidateScore,omitempty"`
	// EstimatedSavingsEUR is the projected saving of switching over the observation window.
	EstimatedSavingsEUR string `json:"estimatedSavingsEUR,omitempty"`
	// Blocked is true when the candidate did not pass the guardrails.
	Blocked bool `json:"blocked,omitempty"`
	// BlockReason explains why the candidate was blocked (empty when not blocked).
	BlockReason string `json:"blockReason,omitempty"`
}

// AIRoutingPolicyStatus reflects the observed state of the routing policy.
type AIRoutingPolicyStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Recommendations lists candidates evaluated by the routing engine.
	// +optional
	Recommendations []AIRoutingPolicyRecommendation `json:"recommendations,omitempty"`

	// LastEvaluatedAt is when the routing engine last ran.
	// +optional
	LastEvaluatedAt *metav1.Time `json:"lastEvaluatedAt,omitempty"`

	// Conditions represent the latest available observations of the policy state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// GOVAR is controller-produced safety evidence for typed policy inputs.
	// +optional
	GOVAR *GOVARRoutingPolicyStatus `json:"govar,omitempty"`
}

// Validate checks conditional typed GOV-AR requirements without depending on
// the API server's CEL validation.
func (s GOVARRoutingPolicySpec) Validate(path *field.Path) field.ErrorList {
	var errs field.ErrorList
	r := s.Reservation
	requireTokens := func(name string, value *int64) {
		if value == nil {
			errs = append(errs, field.Required(path.Child("reservation", name), "required by reservation method"))
		}
	}
	switch r.Method {
	case GOVARReservationMean:
		requireTokens("meanOutputTokens", r.MeanOutputTokens)
	case GOVARReservationFixedMargin:
		requireTokens("meanOutputTokens", r.MeanOutputTokens)
		requireTokens("marginOutputTokens", r.MarginOutputTokens)
	case GOVARReservationFixedQuantile:
		requireTokens("fixedQuantileOutputTokens", r.FixedQuantileOutputTokens)
	case GOVARReservationAdaptiveQuantile, GOVARReservationFixedCohort:
		if s.Calibration == nil {
			errs = append(errs, field.Required(path.Child("calibration"), "required by adaptive reservation method"))
		}
	}
	if r.Method == GOVARReservationFixedCohort {
		if s.Cohort == nil {
			errs = append(errs, field.Required(path.Child("cohort"), "required by govar_fixed_cohort"))
		}
		if s.Risk == nil {
			errs = append(errs, field.Required(path.Child("risk"), "required by govar_fixed_cohort"))
		}
	}
	return errs
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=airpolicy
//+kubebuilder:printcolumn:name="Objective",type=string,JSONPath=`.spec.objective`
//+kubebuilder:printcolumn:name="Canary",type=boolean,JSONPath=`.spec.canary.enabled`
//+kubebuilder:printcolumn:name="Recommendations",type=integer,JSONPath=`.status.recommendations`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AIRoutingPolicy continuously evaluates all observed app/model pairs and surfaces
// the best routing candidate according to the multi-criteria routing score.
// Unlike AIRouteOverride (manual), it computes recommendations automatically and
// can create AIChangeRequest objects for human approval before actuation.
type AIRoutingPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIRoutingPolicySpec   `json:"spec,omitempty"`
	Status AIRoutingPolicyStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AIRoutingPolicyList contains a list of AIRoutingPolicy.
type AIRoutingPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIRoutingPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIRoutingPolicy{}, &AIRoutingPolicyList{})
}
