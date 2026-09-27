// Package govarobservability owns the bounded-cardinality Prometheus contract
// for GOV-AR. A Registry is explicitly constructed and injected; this package
// never registers collectors with Prometheus' process-global default registry.
package govarobservability

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	maxTenantProfiles = 64
	maxPolicyProfiles = 64
)

var profilePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// The bucket boundaries are part of the experiment protocol. Changing them is
// a source/protocol change, not an operations-only configuration change.
var (
	decisionDurationBuckets    = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}
	transactionDurationBuckets = []float64{0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}
	settlementDelayBuckets     = []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 900, 3600}
)

// Buckets returns copies of the frozen histogram boundaries.
func Buckets() (decision, transaction, settlementDelay []float64) {
	return append([]float64(nil), decisionDurationBuckets...),
		append([]float64(nil), transactionDurationBuckets...),
		append([]float64(nil), settlementDelayBuckets...)
}

// Config contains the only deployment-specific metric label values. The
// profiles are aliases from a versioned experiment/configuration manifest, not
// raw tenant IDs, Kubernetes object names, or workload UIDs.
type Config struct {
	TenantProfiles []string
	PolicyProfiles []string
}

// Registry owns all GOV-AR collectors. Methods validate all labels before a
// metric can be created, preventing request-derived values from expanding
// cardinality. Mutable ledger snapshots and effective transition counters must
// be called only after the corresponding database transaction commits.
type Registry struct {
	gatherer prometheus.Gatherer

	tenantProfiles map[string]struct{}
	policyProfiles map[string]struct{}
	updateMu       sync.Mutex

	admissionDecisions *prometheus.CounterVec
	transitions        *prometheus.CounterVec

	reservedMicros             *prometheus.GaugeVec
	settledMicros              *prometheus.GaugeVec
	outstandingLiabilityMicros *prometheus.GaugeVec
	carriedDebtMicros          *prometheus.GaugeVec
	reservationComponentMicros *prometheus.GaugeVec
	settlementComponentMicros  *prometheus.GaugeVec

	decisionDuration    prometheus.Histogram
	transactionDuration prometheus.Histogram
	settlementDelay     prometheus.Histogram

	workerClaims       *prometheus.CounterVec
	workerBacklog      *prometheus.GaugeVec
	workerOldestAge    *prometheus.GaugeVec
	workerHeartbeatAge *prometheus.GaugeVec

	calibrationSupport  *prometheus.GaugeVec
	calibrationCoverage *prometheus.GaugeVec
	driftDetected       *prometheus.GaugeVec
	conservativeMode    *prometheus.GaugeVec

	arithmeticOverflows *prometheus.CounterVec
	boundViolations     *prometheus.CounterVec
	pricingIncomplete   *prometheus.CounterVec
	auditVerifications  *prometheus.CounterVec
	unresolvedAttempts  *prometheus.CounterVec
}

// New creates an isolated native Prometheus registry. It is suitable for the
// admission service and for tests that must not share process-global state.
func New(config Config) (*Registry, error) {
	native := prometheus.NewRegistry()
	return NewWith(config, native, native)
}

// NewWith registers the GOV-AR collectors with the supplied private registry.
// Registerer and Gatherer are separate to support wrapped registerers.
func NewWith(config Config, registerer prometheus.Registerer, gatherer prometheus.Gatherer) (*Registry, error) {
	if registerer == nil || gatherer == nil {
		return nil, errors.New("prometheus registerer and gatherer are required")
	}
	tenantProfiles, err := validatedProfiles("tenant", config.TenantProfiles, maxTenantProfiles)
	if err != nil {
		return nil, err
	}
	policyProfiles, err := validatedProfiles("policy", config.PolicyProfiles, maxPolicyProfiles)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		gatherer:       gatherer,
		tenantProfiles: tenantProfiles,
		policyProfiles: policyProfiles,
		admissionDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_admission_decisions_total",
			Help: "Admission decisions by closed decision, reason, and reservation method.",
		}, []string{"decision", "reason", "method"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_transition_total",
			Help: "Effective ledger state transitions recorded only after transaction commit.",
		}, []string{"from", "to", "reason"}),
		reservedMicros: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_reserved_micros", Help: "Committed reserved monetary snapshot by bounded tenant profile.",
		}, []string{"tenant_profile"}),
		settledMicros: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_settled_micros", Help: "Committed settled monetary snapshot by bounded tenant profile.",
		}, []string{"tenant_profile"}),
		outstandingLiabilityMicros: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_outstanding_liability_micros", Help: "Committed outstanding liability snapshot by bounded tenant profile.",
		}, []string{"tenant_profile"}),
		carriedDebtMicros: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_carried_debt_micros", Help: "Committed carried debt snapshot by bounded tenant profile.",
		}, []string{"tenant_profile"}),
		reservationComponentMicros: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_reservation_component_micros", Help: "Committed aggregate reserved component snapshot by closed billable basis.",
		}, []string{"basis"}),
		settlementComponentMicros: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_settlement_component_micros", Help: "Committed aggregate settled component snapshot by closed billable basis.",
		}, []string{"basis"}),
		decisionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "govar_decision_duration_seconds", Help: "End-to-end admission decision duration.", Buckets: decisionDurationBuckets,
		}),
		transactionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "govar_transaction_duration_seconds", Help: "Duration of committed GOV-AR database transactions.", Buckets: transactionDurationBuckets,
		}),
		settlementDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "govar_settlement_delay_seconds", Help: "Delay from provider completion to committed settlement.", Buckets: settlementDelayBuckets,
		}),
		workerClaims: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_worker_claims_total", Help: "Durable worker claims by closed work kind and result.",
		}, []string{"kind", "result"}),
		workerBacklog: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_worker_backlog", Help: "Durable worker backlog by closed work kind and state.",
		}, []string{"kind", "state"}),
		workerOldestAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_worker_oldest_age_seconds", Help: "Age of the oldest durable worker item by work kind.",
		}, []string{"kind"}),
		workerHeartbeatAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_worker_heartbeat_age_seconds", Help: "Age of the last successful durable worker heartbeat by work kind.",
		}, []string{"kind"}),
		calibrationSupport: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_calibration_support", Help: "Eligible calibration support by bounded policy profile and closed detector.",
		}, []string{"policy_profile", "detector"}),
		calibrationCoverage: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_calibration_coverage_ppb", Help: "Empirical calibration coverage in parts per billion.",
		}, []string{"policy_profile", "detector"}),
		driftDetected: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_drift_detected", Help: "Whether drift is currently detected for a bounded policy profile and closed detector.",
		}, []string{"policy_profile", "detector"}),
		conservativeMode: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "govar_conservative_mode", Help: "Whether a bounded policy profile is in conservative mode.",
		}, []string{"policy_profile", "detector"}),
		arithmeticOverflows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_arithmetic_overflow_total", Help: "Rejected arithmetic overflows by closed operation.",
		}, []string{"operation"}),
		boundViolations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_bound_violation_total", Help: "Observed reservation-bound violations by closed billable basis.",
		}, []string{"basis"}),
		pricingIncomplete: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_pricing_incomplete_total", Help: "Pricing incompleteness events by closed reason.",
		}, []string{"reason"}),
		auditVerifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_audit_verification_total", Help: "Append-only audit-chain verification outcomes.",
		}, []string{"result"}),
		unresolvedAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "govar_unresolved_attempt_total", Help: "Provider attempts entering an unresolved condition by closed reason.",
		}, []string{"reason"}),
	}

	collectors := []prometheus.Collector{
		r.admissionDecisions, r.transitions,
		r.reservedMicros, r.settledMicros, r.outstandingLiabilityMicros, r.carriedDebtMicros,
		r.reservationComponentMicros, r.settlementComponentMicros,
		r.decisionDuration, r.transactionDuration, r.settlementDelay,
		r.workerClaims, r.workerBacklog, r.workerOldestAge, r.workerHeartbeatAge,
		r.calibrationSupport, r.calibrationCoverage, r.driftDetected, r.conservativeMode,
		r.arithmeticOverflows, r.boundViolations, r.pricingIncomplete, r.auditVerifications, r.unresolvedAttempts,
	}
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("register GOV-AR metric: %w", err)
		}
	}
	return r, nil
}

// Handler exposes only the Gatherer associated with this Registry.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.gatherer, promhttp.HandlerOpts{})
}

// Gatherer enables clean accounting tests without exposing collector mutation.
func (r *Registry) Gatherer() prometheus.Gatherer { return r.gatherer }

type CommittedTransition struct {
	From      string
	To        string
	Reason    string
	Effective bool
}

// RecordCommittedTransition increments only an effective state change. A
// committed duplicate/no-op is deliberately observable in the audit log, not as
// a second effective transition.
func (r *Registry) RecordCommittedTransition(transition CommittedTransition) error {
	if !validState(transition.From) || !validState(transition.To) {
		return fmt.Errorf("transition uses an unknown state %q -> %q", transition.From, transition.To)
	}
	if !validReason(transition.Reason) {
		return fmt.Errorf("transition uses unknown reason %q", transition.Reason)
	}
	if transition.Effective {
		r.transitions.WithLabelValues(transition.From, transition.To, transition.Reason).Inc()
	}
	return nil
}

func (r *Registry) RecordAdmissionDecision(decision, reason, method string) error {
	if !validDecision(decision) {
		return fmt.Errorf("unknown decision %q", decision)
	}
	if !validReason(reason) {
		return fmt.Errorf("unknown reason %q", reason)
	}
	if !validMethod(method) {
		return fmt.Errorf("unknown reservation method %q", method)
	}
	r.admissionDecisions.WithLabelValues(decision, reason, method).Inc()
	return nil
}

type TenantMonetarySnapshot struct {
	TenantProfile              string
	ReservedMicros             int64
	SettledMicros              int64
	OutstandingLiabilityMicros int64
	CarriedDebtMicros          int64
}

// ApplyCommittedTenantSnapshot publishes values computed from committed SQL
// state. Callers must invoke it after commit and before opening a scrape barrier.
func (r *Registry) ApplyCommittedTenantSnapshot(snapshot TenantMonetarySnapshot) error {
	if _, ok := r.tenantProfiles[snapshot.TenantProfile]; !ok {
		return fmt.Errorf("tenant profile %q is not configured", snapshot.TenantProfile)
	}
	if snapshot.ReservedMicros < 0 || snapshot.SettledMicros < 0 || snapshot.OutstandingLiabilityMicros < 0 || snapshot.CarriedDebtMicros < 0 {
		return errors.New("monetary snapshots cannot be negative")
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.reservedMicros.WithLabelValues(snapshot.TenantProfile).Set(float64(snapshot.ReservedMicros))
	r.settledMicros.WithLabelValues(snapshot.TenantProfile).Set(float64(snapshot.SettledMicros))
	r.outstandingLiabilityMicros.WithLabelValues(snapshot.TenantProfile).Set(float64(snapshot.OutstandingLiabilityMicros))
	r.carriedDebtMicros.WithLabelValues(snapshot.TenantProfile).Set(float64(snapshot.CarriedDebtMicros))
	return nil
}

type ComponentSnapshot struct {
	ReservationMicros map[string]int64
	SettlementMicros  map[string]int64
}

// ApplyCommittedComponentSnapshot replaces the aggregate component snapshot.
// Omitted closed bases are set to zero, preventing stale series after a reset.
func (r *Registry) ApplyCommittedComponentSnapshot(snapshot ComponentSnapshot) error {
	for basis, amount := range snapshot.ReservationMicros {
		if !validBasis(basis) || amount < 0 {
			return fmt.Errorf("invalid reservation component %q=%d", basis, amount)
		}
	}
	for basis, amount := range snapshot.SettlementMicros {
		if !validBasis(basis) || amount < 0 {
			return fmt.Errorf("invalid settlement component %q=%d", basis, amount)
		}
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	for _, basis := range allBases() {
		r.reservationComponentMicros.WithLabelValues(basis).Set(float64(snapshot.ReservationMicros[basis]))
		r.settlementComponentMicros.WithLabelValues(basis).Set(float64(snapshot.SettlementMicros[basis]))
	}
	return nil
}

func (r *Registry) ObserveDecisionDuration(duration time.Duration) error {
	if duration < 0 {
		return errors.New("decision duration cannot be negative")
	}
	r.decisionDuration.Observe(duration.Seconds())
	return nil
}

// ObserveCommittedTransactionDuration must be called only after a successful
// commit. Rollbacks and failed attempts are intentionally excluded.
func (r *Registry) ObserveCommittedTransactionDuration(duration time.Duration) error {
	if duration < 0 {
		return errors.New("transaction duration cannot be negative")
	}
	r.transactionDuration.Observe(duration.Seconds())
	return nil
}

func (r *Registry) ObserveCommittedSettlementDelay(duration time.Duration) error {
	if duration < 0 {
		return errors.New("settlement delay cannot be negative")
	}
	r.settlementDelay.Observe(duration.Seconds())
	return nil
}

func (r *Registry) RecordWorkerClaim(kind, result string) error {
	if !validWorkerKind(kind) || !validWorkerClaimResult(result) {
		return fmt.Errorf("invalid worker claim labels kind=%q result=%q", kind, result)
	}
	r.workerClaims.WithLabelValues(kind, result).Inc()
	return nil
}

func (r *Registry) SetWorkerSnapshot(kind, state string, backlog int64, oldestAge, heartbeatAge time.Duration) error {
	if !validWorkerKind(kind) || !validWorkerState(state) {
		return fmt.Errorf("invalid worker snapshot labels kind=%q state=%q", kind, state)
	}
	if backlog < 0 || oldestAge < 0 || heartbeatAge < 0 {
		return errors.New("worker backlog and ages cannot be negative")
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.workerBacklog.WithLabelValues(kind, state).Set(float64(backlog))
	r.workerOldestAge.WithLabelValues(kind).Set(oldestAge.Seconds())
	r.workerHeartbeatAge.WithLabelValues(kind).Set(heartbeatAge.Seconds())
	return nil
}

type CalibrationSnapshot struct {
	PolicyProfile    string
	Detector         string
	Support          int64
	CoveragePPB      int64
	DriftDetected    bool
	ConservativeMode bool
}

func (r *Registry) SetCalibrationSnapshot(snapshot CalibrationSnapshot) error {
	if _, ok := r.policyProfiles[snapshot.PolicyProfile]; !ok {
		return fmt.Errorf("policy profile %q is not configured", snapshot.PolicyProfile)
	}
	if !validDetector(snapshot.Detector) {
		return fmt.Errorf("unknown drift detector %q", snapshot.Detector)
	}
	if snapshot.Support < 0 || snapshot.CoveragePPB < 0 || snapshot.CoveragePPB > 1_000_000_000 {
		return errors.New("calibration support/coverage is outside its valid range")
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	labels := []string{snapshot.PolicyProfile, snapshot.Detector}
	r.calibrationSupport.WithLabelValues(labels...).Set(float64(snapshot.Support))
	r.calibrationCoverage.WithLabelValues(labels...).Set(float64(snapshot.CoveragePPB))
	r.driftDetected.WithLabelValues(labels...).Set(boolFloat(snapshot.DriftDetected))
	r.conservativeMode.WithLabelValues(labels...).Set(boolFloat(snapshot.ConservativeMode))
	return nil
}

func (r *Registry) RecordArithmeticOverflow(operation string) error {
	if !validOverflowOperation(operation) {
		return fmt.Errorf("unknown overflow operation %q", operation)
	}
	r.arithmeticOverflows.WithLabelValues(operation).Inc()
	return nil
}

func (r *Registry) RecordBoundViolation(basis string) error {
	if !validBasis(basis) {
		return fmt.Errorf("unknown billable basis %q", basis)
	}
	r.boundViolations.WithLabelValues(basis).Inc()
	return nil
}

func (r *Registry) RecordPricingIncomplete(reason string) error {
	if !validPricingReason(reason) {
		return fmt.Errorf("unknown pricing-incomplete reason %q", reason)
	}
	r.pricingIncomplete.WithLabelValues(reason).Inc()
	return nil
}

func (r *Registry) RecordAuditVerification(result string) error {
	if !validAuditResult(result) {
		return fmt.Errorf("unknown audit verification result %q", result)
	}
	r.auditVerifications.WithLabelValues(result).Inc()
	return nil
}

func (r *Registry) RecordUnresolvedAttempt(reason string) error {
	if !validUnresolvedReason(reason) {
		return fmt.Errorf("unknown unresolved-attempt reason %q", reason)
	}
	r.unresolvedAttempts.WithLabelValues(reason).Inc()
	return nil
}

func validatedProfiles(kind string, values []string, maximum int) (map[string]struct{}, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("at least one %s profile is required", kind)
	}
	if len(values) > maximum {
		return nil, fmt.Errorf("%s profile count %d exceeds %d", kind, len(values), maximum)
	}
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !profilePattern.MatchString(value) {
			return nil, fmt.Errorf("invalid bounded %s profile %q", kind, value)
		}
		if _, duplicate := result[value]; duplicate {
			return nil, fmt.Errorf("duplicate %s profile %q", kind, value)
		}
		result[value] = struct{}{}
	}
	return result, nil
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// AllowedLabelValues returns a sorted copy of every compile-time label
// vocabulary. It is intended for schema checks and release evidence.
func AllowedLabelValues() map[string][]string {
	values := map[string][]string{
		"decision": allDecisions(), "reason": allReasons(), "method": allMethods(),
		"state": allStates(), "basis": allBases(), "worker_kind": allWorkerKinds(),
		"worker_claim_result": allWorkerClaimResults(), "worker_state": allWorkerStates(),
		"detector": allDetectors(), "overflow_operation": allOverflowOperations(),
		"pricing_reason": allPricingReasons(), "audit_result": allAuditResults(),
		"unresolved_reason": allUnresolvedReasons(),
	}
	for key := range values {
		sort.Strings(values[key])
	}
	return values
}
