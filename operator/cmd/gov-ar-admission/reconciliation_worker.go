package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarworker"
)

// durableWorkerBackend is implemented only by the PostgreSQL production
// engine. Every effectful worker operation is either a ledger transaction or a
// read-only committed snapshot; handlers cannot release liability directly.
type durableWorkerBackend interface {
	govarworker.Ledger
	ReconcileExpired(context.Context, time.Time) ([]govar.ReconciliationRecord, error)
	PendingReconciliation(context.Context, int) ([]govar.ReconciliationRecord, error)
	EnqueueExpiredWork(context.Context, int) (int, error)
	EnqueueReconciliationWork(context.Context, int) (int, error)
	EnqueueOutboxRepairWork(context.Context, int) (int, error)
	EnqueuePeriodicWork(context.Context, govarworker.Kind, string, time.Time, string) (bool, error)
	WorkerQueueSnapshots(context.Context) ([]govar.WorkerQueueSnapshot, error)
	VerifyAllTenantAudits(context.Context) error
	ObservabilitySnapshot(context.Context, string) (govar.TenantObservabilitySnapshot, error)
	ComponentObservabilitySnapshot(context.Context) (govar.ComponentObservabilitySnapshot, error)
}

type reconciliationWorkerConfig struct {
	Interval         time.Duration
	QueryLimit       int
	Lease            time.Duration
	RenewEvery       time.Duration
	MaxAttempts      int
	BaseBackoff      time.Duration
	MaxBackoff       time.Duration
	HeartbeatTTL     time.Duration
	PeriodicInterval time.Duration
	Now              func() time.Time
}

func reconciliationWorkerConfigFromEnvironment() reconciliationWorkerConfig {
	config := reconciliationWorkerConfig{
		Interval: 5 * time.Second, QueryLimit: 100, Lease: 30 * time.Second,
		RenewEvery: 10 * time.Second, MaxAttempts: 8, BaseBackoff: time.Second,
		MaxBackoff: 5 * time.Minute, HeartbeatTTL: time.Minute,
		PeriodicInterval: time.Minute, Now: time.Now,
	}
	parseDuration := func(name string, target *time.Duration, minimum, maximum time.Duration) {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return
		}
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed < minimum || parsed > maximum {
			log.Printf("ignoring invalid %s %q", name, raw)
			return
		}
		*target = parsed
	}
	parseDuration("GOV_AR_RECONCILIATION_INTERVAL", &config.Interval, time.Second, time.Hour)
	parseDuration("GOV_AR_WORKER_LEASE", &config.Lease, time.Second, time.Hour)
	parseDuration("GOV_AR_WORKER_RENEW_EVERY", &config.RenewEvery, time.Second, time.Hour)
	parseDuration("GOV_AR_WORKER_BASE_BACKOFF", &config.BaseBackoff, time.Second, time.Hour)
	parseDuration("GOV_AR_WORKER_MAX_BACKOFF", &config.MaxBackoff, time.Second, 24*time.Hour)
	parseDuration("GOV_AR_WORKER_HEARTBEAT_TTL", &config.HeartbeatTTL, time.Second, time.Hour)
	parseDuration("GOV_AR_WORKER_PERIODIC_INTERVAL", &config.PeriodicInterval, time.Minute, 24*time.Hour)
	if raw := strings.TrimSpace(os.Getenv("GOV_AR_RECONCILIATION_QUERY_LIMIT")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 1 && parsed <= 10_000 {
			config.QueryLimit = parsed
		} else {
			log.Printf("ignoring invalid GOV_AR_RECONCILIATION_QUERY_LIMIT %q", raw)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("GOV_AR_WORKER_MAX_ATTEMPTS")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 1 && parsed <= 100 {
			config.MaxAttempts = parsed
		} else {
			log.Printf("ignoring invalid GOV_AR_WORKER_MAX_ATTEMPTS %q", raw)
		}
	}
	// Invalid cross-field overrides are rejected together instead of silently
	// starting a worker whose renew interval cannot precede lease expiry.
	if config.RenewEvery >= config.Lease {
		log.Printf("GOV-AR worker renew interval must be less than its lease; using 30s/10s")
		config.Lease, config.RenewEvery = 30*time.Second, 10*time.Second
	}
	if config.MaxBackoff < config.BaseBackoff {
		log.Printf("GOV-AR worker max backoff is less than base backoff; using 1s/5m")
		config.BaseBackoff, config.MaxBackoff = time.Second, 5*time.Minute
	}
	return config
}

type durableWorkerManager struct {
	backend durableWorkerBackend
	metrics *serviceMetrics
	config  reconciliationWorkerConfig

	mu                    sync.RWMutex
	workers               map[govarworker.Kind]*govarworker.Worker
	runtimes              map[govarworker.Kind]*govarworker.Runtime
	lastHeartbeat         map[govarworker.Kind]govarworker.Heartbeat
	lastEnqueueSuccess    time.Time
	lastEnqueueError      string
	criticalHandlerErrors map[govarworker.Kind]string
	queueCriticalErrors   map[govarworker.Kind]string
	cancel                context.CancelFunc
	done                  chan struct{}
}

func newDurableWorkerManager(backend durableWorkerBackend, metrics *serviceMetrics, config reconciliationWorkerConfig) (*durableWorkerManager, error) {
	if backend == nil {
		return nil, errors.New("durable worker backend is required")
	}
	if config.Interval <= 0 || config.QueryLimit < 1 || config.QueryLimit > 10_000 || config.Lease <= 0 ||
		config.RenewEvery <= 0 || config.RenewEvery >= config.Lease || config.MaxAttempts < 1 ||
		config.BaseBackoff <= 0 || config.MaxBackoff < config.BaseBackoff || config.HeartbeatTTL <= 0 ||
		config.PeriodicInterval <= 0 {
		return nil, errors.New("invalid durable worker timing, batch, retry, or heartbeat bounds")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	manager := &durableWorkerManager{backend: backend, metrics: metrics, config: config,
		workers: make(map[govarworker.Kind]*govarworker.Worker), runtimes: make(map[govarworker.Kind]*govarworker.Runtime),
		lastHeartbeat:         make(map[govarworker.Kind]govarworker.Heartbeat),
		criticalHandlerErrors: make(map[govarworker.Kind]string), queueCriticalErrors: make(map[govarworker.Kind]string), done: make(chan struct{})}
	for _, kind := range govarworker.AllKinds() {
		workerConfig := govarworker.DefaultConfig(kind)
		workerConfig.BatchSize = config.QueryLimit
		workerConfig.Lease = config.Lease
		workerConfig.RenewEvery = config.RenewEvery
		workerConfig.PollEvery = config.Interval
		workerConfig.MaxAttempts = config.MaxAttempts
		workerConfig.BaseBackoff = config.BaseBackoff
		workerConfig.MaxBackoff = config.MaxBackoff
		workerConfig.HeartbeatTTL = config.HeartbeatTTL
		worker, err := govarworker.New(backend, govarworker.HandlerFunc(manager.handle(kind)), workerConfig)
		if err != nil {
			return nil, fmt.Errorf("configure %s worker: %w", kind, err)
		}
		manager.workers[kind] = worker
	}
	return manager, nil
}

func (m *durableWorkerManager) Start(parent context.Context) error {
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return errors.New("durable worker manager already started")
	}
	ctx, cancel := context.WithCancel(parent)
	m.cancel = cancel
	m.mu.Unlock()
	if err := m.enqueue(ctx); err != nil {
		cancel()
		m.mu.Lock()
		m.cancel = nil
		m.mu.Unlock()
		return fmt.Errorf("initial durable work enqueue: %w", err)
	}
	m.mu.Lock()
	for kind, worker := range m.workers {
		m.runtimes[kind] = worker.Start(ctx)
	}
	m.mu.Unlock()
	go m.runCoordinator(ctx)
	return nil
}

func (m *durableWorkerManager) runCoordinator(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()
	m.refreshMetrics(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.enqueue(ctx); err != nil {
				log.Printf("durable worker enqueue failed: %v", err)
			}
			m.refreshMetrics(ctx)
		}
	}
}

func (m *durableWorkerManager) enqueue(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	operations := []func(context.Context, int) (int, error){
		m.backend.EnqueueExpiredWork, m.backend.EnqueueReconciliationWork, m.backend.EnqueueOutboxRepairWork,
	}
	for _, operation := range operations {
		if _, err := operation(ctx, m.config.QueryLimit); err != nil {
			m.setEnqueueResult(err)
			return err
		}
	}
	period := m.config.Now().UTC().Truncate(m.config.PeriodicInterval)
	for _, kind := range []govarworker.Kind{govarworker.KindCalibrationDrift, govarworker.KindAuditCheckpoint} {
		payload := sha256.Sum256([]byte("govar-periodic-work-v1\x00" + string(kind) + "\x00global\x00" + period.Format(time.RFC3339Nano)))
		if _, err := m.backend.EnqueuePeriodicWork(ctx, kind, "global", period, fmt.Sprintf("%x", payload[:])); err != nil {
			m.setEnqueueResult(err)
			return err
		}
	}
	m.setEnqueueResult(nil)
	return nil
}

func (m *durableWorkerManager) setEnqueueResult(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.lastEnqueueError = "enqueue_failed"
		return
	}
	m.lastEnqueueError = ""
	m.lastEnqueueSuccess = m.config.Now().UTC()
}

func (m *durableWorkerManager) handle(kind govarworker.Kind) func(context.Context, govarworker.WorkItem) (govarworker.WorkResult, error) {
	return func(ctx context.Context, item govarworker.WorkItem) (result govarworker.WorkResult, err error) {
		ctx, span := startGOVAROperation(ctx, "govar.reconciliation", attribute.String("govar.worker_kind", string(kind)))
		defer func() { finishGOVAROperation(span, err, attribute.String("govar.worker_result", string(result.State))) }()
		switch kind {
		case govarworker.KindExpiry:
			return m.handleExpiry(ctx, item), nil
		case govarworker.KindDeliveryReconciliation:
			// Provider truth is unavailable to the generic worker. Preserve the
			// hold and retry until an authoritative usage/cancellation event makes
			// the task obsolete; never manufacture a resolution.
			return govarworker.WorkResult{State: govarworker.StateRetry, ReasonCode: "delivery_confirmation_pending", LiabilityDisposition: govarworker.LiabilityPreserve}, nil
		case govarworker.KindOutboxRepair:
			return m.handleOutboxRepair(ctx, item), nil
		case govarworker.KindCalibrationDrift:
			// The v7 worker is alive, but ConfigMap evidence is explicitly not an
			// authority. The v8 evidence producer replaces this conservative no-op.
			return govarworker.WorkResult{State: govarworker.StateCompleted, ReasonCode: "calibration_conservative_mode", LiabilityDisposition: govarworker.LiabilityPreserve}, nil
		case govarworker.KindAuditCheckpoint:
			if err := m.backend.VerifyAllTenantAudits(ctx); err != nil {
				m.setCritical(kind, "audit_verification_failed")
				if m.metrics != nil {
					_ = m.metrics.registry.RecordAuditVerification("fail")
				}
				return govarworker.WorkResult{State: govarworker.StateRetry, ReasonCode: "audit_verification_failed", LiabilityDisposition: govarworker.LiabilityPreserve}, nil
			}
			m.setCritical(kind, "")
			if m.metrics != nil {
				_ = m.metrics.registry.RecordAuditVerification("pass")
			}
			return govarworker.WorkResult{State: govarworker.StateCompleted, ReasonCode: "audit_chain_verified", LiabilityDisposition: govarworker.LiabilityPreserve}, nil
		default:
			return govarworker.WorkResult{}, errors.New("unsupported durable worker kind")
		}
	}
}

func (m *durableWorkerManager) handleExpiry(ctx context.Context, item govarworker.WorkItem) govarworker.WorkResult {
	records, err := m.backend.ReconcileExpired(ctx, m.config.Now().UTC())
	if err != nil {
		m.setCritical(govarworker.KindExpiry, "expiry_reconciliation_failed")
		return govarworker.WorkResult{State: govarworker.StateRetry, ReasonCode: "expiry_reconciliation_failed", LiabilityDisposition: govarworker.LiabilityPreserve}
	}
	m.setCritical(govarworker.KindExpiry, "")
	disposition := govarworker.LiabilityPreserve
	for _, record := range records {
		if !record.TransitionEffective {
			continue
		}
		reason := govar.ReasonDispatchUnresolved
		if record.State == govar.StateExpiredUndispatched {
			reason = govar.ReasonExpiredUndispatched
		}
		recordCommittedTransition(m.metrics, ctx, record.PreviousState, record.State, reason, true)
		recordMetricError(ctx, publishCommittedTenantMetrics(ctx, m.metrics, m.backend, record.TenantID))
		if record.RequestID == item.RequestID && record.State == govar.StateExpiredUndispatched && item.AuthoritativeReleaseEligible {
			disposition = govarworker.LiabilityAuthoritativeRelease
		}
		if record.State == govar.StateUnresolved && m.metrics != nil {
			_ = m.metrics.registry.RecordUnresolvedAttempt("telemetry_missing")
		}
	}
	return govarworker.WorkResult{State: govarworker.StateCompleted, ReasonCode: "expiry_reconciled", LiabilityDisposition: disposition}
}

func (m *durableWorkerManager) handleOutboxRepair(ctx context.Context, item govarworker.WorkItem) govarworker.WorkResult {
	pending, err := m.backend.PendingReconciliation(ctx, m.config.QueryLimit)
	if err != nil {
		return govarworker.WorkResult{State: govarworker.StateRetry, ReasonCode: "outbox_observation_failed", LiabilityDisposition: govarworker.LiabilityPreserve}
	}
	for _, record := range pending {
		if record.RequestID == item.RequestID {
			return govarworker.WorkResult{State: govarworker.StateRetry, ReasonCode: "outbox_repair_pending", LiabilityDisposition: govarworker.LiabilityPreserve}
		}
	}
	return govarworker.WorkResult{State: govarworker.StateCompleted, ReasonCode: "outbox_state_already_terminal", LiabilityDisposition: govarworker.LiabilityPreserve}
}

func (m *durableWorkerManager) setCritical(kind govarworker.Kind, code string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if code == "" {
		delete(m.criticalHandlerErrors, kind)
		return
	}
	m.criticalHandlerErrors[kind] = code
}

func (m *durableWorkerManager) refreshMetrics(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	snapshots, err := m.backend.WorkerQueueSnapshots(ctx)
	if err != nil {
		m.mu.Lock()
		for _, kind := range govarworker.AllKinds() {
			if m.metrics != nil {
				_ = m.metrics.registry.RecordWorkerClaim(string(kind), "error")
			}
			m.queueCriticalErrors[kind] = "queue_snapshot_failed"
		}
		m.mu.Unlock()
		return
	}
	now := m.config.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, snapshot := range snapshots {
		if snapshot.CountsByState["DEAD_LETTER"] > 0 {
			m.queueCriticalErrors[snapshot.Kind] = "dead_letter_present"
		} else {
			delete(m.queueCriticalErrors, snapshot.Kind)
		}
		worker := m.workers[snapshot.Kind]
		heartbeat := worker.Snapshot()
		age := time.Duration(0)
		if !heartbeat.LastPassSucceeded.IsZero() && now.After(heartbeat.LastPassSucceeded) {
			age = now.Sub(heartbeat.LastPassSucceeded)
		}
		states := map[string]int64{
			"ready": snapshot.CountsByState["PENDING"], "leased": snapshot.CountsByState["LEASED"],
			"retry": snapshot.CountsByState["RETRY"], "dead_letter": snapshot.CountsByState["DEAD_LETTER"], "ambiguous": 0,
		}
		for state, count := range states {
			if m.metrics != nil {
				_ = m.metrics.registry.SetWorkerSnapshot(string(snapshot.Kind), state, count, snapshot.OldestEligibleAge, age)
			}
		}
		prior := m.lastHeartbeat[snapshot.Kind]
		if m.metrics != nil && heartbeat.Claims > prior.Claims {
			for count := prior.Claims; count < heartbeat.Claims; count++ {
				_ = m.metrics.registry.RecordWorkerClaim(string(snapshot.Kind), "claimed")
			}
		} else if m.metrics != nil && heartbeat.LastPassSucceeded.After(prior.LastPassSucceeded) {
			_ = m.metrics.registry.RecordWorkerClaim(string(snapshot.Kind), "empty")
		}
		m.lastHeartbeat[snapshot.Kind] = heartbeat
	}
}

func (m *durableWorkerManager) Healthy() error {
	m.mu.RLock()
	lastSuccess, enqueueError := m.lastEnqueueSuccess, m.lastEnqueueError
	critical := make(map[govarworker.Kind]string, len(m.criticalHandlerErrors))
	for kind, code := range m.criticalHandlerErrors {
		critical[kind] = code
	}
	queueCritical := make(map[govarworker.Kind]string, len(m.queueCriticalErrors))
	for kind, code := range m.queueCriticalErrors {
		queueCritical[kind] = code
	}
	m.mu.RUnlock()
	if lastSuccess.IsZero() || m.config.Now().UTC().Sub(lastSuccess) > m.config.HeartbeatTTL {
		return errors.New("durable enqueue heartbeat is absent or stale")
	}
	if enqueueError != "" {
		return fmt.Errorf("durable enqueue error: %s", enqueueError)
	}
	for _, kind := range govarworker.AllKinds() {
		if code := critical[kind]; code != "" {
			return fmt.Errorf("%s worker critical error: %s", kind, code)
		}
		if code := queueCritical[kind]; code != "" {
			return fmt.Errorf("%s durable queue critical error: %s", kind, code)
		}
		if err := m.workers[kind].Healthy(); err != nil {
			return fmt.Errorf("%s worker unhealthy: %w", kind, err)
		}
	}
	return nil
}

func (m *durableWorkerManager) Shutdown(ctx context.Context) error {
	m.mu.RLock()
	cancel := m.cancel
	runtimes := make(map[govarworker.Kind]*govarworker.Runtime, len(m.runtimes))
	for kind, runtime := range m.runtimes {
		runtimes[kind] = runtime
	}
	m.mu.RUnlock()
	if cancel == nil {
		return nil
	}
	cancel()
	var failures []string
	for _, kind := range govarworker.AllKinds() {
		if runtime := runtimes[kind]; runtime != nil {
			if err := runtime.Shutdown(ctx); err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", kind, err))
			}
		}
	}
	select {
	case <-m.done:
	case <-ctx.Done():
		failures = append(failures, ctx.Err().Error())
	}
	if len(failures) != 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}
