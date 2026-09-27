package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarworker"
)

type completedWork struct {
	item   govarworker.WorkItem
	result govarworker.WorkResult
}

type fakeDurableWorkerBackend struct {
	mu              sync.Mutex
	enqueueErr      error
	auditErr        error
	reconcileErr    error
	pendingErr      error
	reconcile       []govar.ReconciliationRecord
	pending         []govar.ReconciliationRecord
	claimCalls      int
	enqueueCalls    int
	periodicKinds   map[govarworker.Kind]int
	completed       []completedWork
	claimedByKind   map[govarworker.Kind][]govarworker.WorkItem
	workerSnapshots []govar.WorkerQueueSnapshot
}

func newFakeDurableWorkerBackend() *fakeDurableWorkerBackend {
	backend := &fakeDurableWorkerBackend{periodicKinds: map[govarworker.Kind]int{}, claimedByKind: map[govarworker.Kind][]govarworker.WorkItem{}}
	for _, kind := range govarworker.AllKinds() {
		backend.workerSnapshots = append(backend.workerSnapshots, govar.WorkerQueueSnapshot{Kind: kind, CountsByState: map[string]int64{
			"PENDING": 0, "LEASED": 0, "RETRY": 0, "COMPLETED": 0, "DEAD_LETTER": 0,
		}})
	}
	return backend
}

func (f *fakeDurableWorkerBackend) ClaimWork(_ context.Context, kind string, _ int, lease time.Duration) ([]govarworker.WorkItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	workerKind := govarworker.Kind(kind)
	items := append([]govarworker.WorkItem(nil), f.claimedByKind[workerKind]...)
	delete(f.claimedByKind, workerKind)
	for index := range items {
		items[index].Kind = workerKind
		items[index].LeaseOwner = "test-owner"
		items[index].LeaseUntil = time.Now().UTC().Add(lease)
		if items[index].Attempt == 0 {
			items[index].Attempt = 1
		}
	}
	return items, nil
}

func (f *fakeDurableWorkerBackend) CompleteWork(_ context.Context, item govarworker.WorkItem, result govarworker.WorkResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed = append(f.completed, completedWork{item: item, result: result})
	return nil
}

func (*fakeDurableWorkerBackend) RenewWork(context.Context, govarworker.WorkItem, time.Duration) error {
	return nil
}

func (f *fakeDurableWorkerBackend) ReconcileExpired(context.Context, time.Time) ([]govar.ReconciliationRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]govar.ReconciliationRecord(nil), f.reconcile...), f.reconcileErr
}

func (f *fakeDurableWorkerBackend) PendingReconciliation(context.Context, int) ([]govar.ReconciliationRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]govar.ReconciliationRecord(nil), f.pending...), f.pendingErr
}

func (f *fakeDurableWorkerBackend) enqueue(_ context.Context, _ int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueueCalls++
	return 0, f.enqueueErr
}

func (f *fakeDurableWorkerBackend) EnqueueExpiredWork(ctx context.Context, limit int) (int, error) {
	return f.enqueue(ctx, limit)
}

func (f *fakeDurableWorkerBackend) EnqueueReconciliationWork(ctx context.Context, limit int) (int, error) {
	return f.enqueue(ctx, limit)
}

func (f *fakeDurableWorkerBackend) EnqueueOutboxRepairWork(ctx context.Context, limit int) (int, error) {
	return f.enqueue(ctx, limit)
}

func (f *fakeDurableWorkerBackend) EnqueuePeriodicWork(_ context.Context, kind govarworker.Kind, _ string, _ time.Time, _ string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.periodicKinds[kind]++
	return true, f.enqueueErr
}

func (f *fakeDurableWorkerBackend) WorkerQueueSnapshots(context.Context) ([]govar.WorkerQueueSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]govar.WorkerQueueSnapshot(nil), f.workerSnapshots...), nil
}

func (f *fakeDurableWorkerBackend) VerifyAllTenantAudits(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.auditErr
}

func (*fakeDurableWorkerBackend) ObservabilitySnapshot(_ context.Context, tenant string) (govar.TenantObservabilitySnapshot, error) {
	return govar.TenantObservabilitySnapshot{TenantID: tenant}, nil
}

func (*fakeDurableWorkerBackend) ComponentObservabilitySnapshot(context.Context) (govar.ComponentObservabilitySnapshot, error) {
	return govar.ComponentObservabilitySnapshot{ReservationMicros: map[string]govar.MoneyMicros{}, SettlementMicros: map[string]govar.MoneyMicros{}}, nil
}

func workerTestConfig() reconciliationWorkerConfig {
	return reconciliationWorkerConfig{Interval: 5 * time.Millisecond, QueryLimit: 7, Lease: 200 * time.Millisecond,
		RenewEvery: 50 * time.Millisecond, MaxAttempts: 3, BaseBackoff: time.Millisecond,
		MaxBackoff: 10 * time.Millisecond, HeartbeatTTL: time.Second, PeriodicInterval: 20 * time.Millisecond, Now: time.Now}
}

func waitFor(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDurableWorkerManagerStartsAllKindsBecomesHealthyAndStops(t *testing.T) {
	backend := newFakeDurableWorkerBackend()
	manager, err := newDurableWorkerManager(backend, nil, workerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return manager.Healthy() == nil }, "all durable worker heartbeats did not become healthy")
	backend.mu.Lock()
	if backend.claimCalls < len(govarworker.AllKinds()) || backend.enqueueCalls < 3 || backend.periodicKinds[govarworker.KindCalibrationDrift] == 0 || backend.periodicKinds[govarworker.KindAuditCheckpoint] == 0 {
		t.Fatalf("incomplete durable startup: claims=%d enqueues=%d periodic=%v", backend.claimCalls, backend.enqueueCalls, backend.periodicKinds)
	}
	backend.mu.Unlock()
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	claimsAfterShutdown := backend.claimCalls
	backend.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.claimCalls != claimsAfterShutdown {
		t.Fatalf("workers continued claiming after shutdown: before=%d after=%d", claimsAfterShutdown, backend.claimCalls)
	}
}

func TestDeadLetterMakesReadinessFailUntilQueueIsRemediated(t *testing.T) {
	backend := newFakeDurableWorkerBackend()
	manager, err := newDurableWorkerManager(backend, nil, workerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return manager.Healthy() == nil }, "durable workers did not become healthy")
	backend.mu.Lock()
	for index := range backend.workerSnapshots {
		if backend.workerSnapshots[index].Kind == govarworker.KindDeliveryReconciliation {
			backend.workerSnapshots[index].CountsByState["DEAD_LETTER"] = 1
		}
	}
	backend.mu.Unlock()
	waitFor(t, func() bool {
		err := manager.Healthy()
		return err != nil && strings.Contains(err.Error(), "dead_letter_present")
	}, "dead-lettered liability did not fail worker readiness")
	backend.mu.Lock()
	for index := range backend.workerSnapshots {
		backend.workerSnapshots[index].CountsByState["DEAD_LETTER"] = 0
	}
	backend.mu.Unlock()
	waitFor(t, func() bool { return manager.Healthy() == nil }, "queue remediation did not restore readiness")
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
}

func TestDurableWorkerInitialEnqueueFailurePreventsStartup(t *testing.T) {
	backend := newFakeDurableWorkerBackend()
	backend.enqueueErr = errors.New("synthetic enqueue failure")
	manager, err := newDurableWorkerManager(backend, nil, workerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err == nil {
		t.Fatal("manager started without a durable initial enqueue")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.claimCalls != 0 {
		t.Fatalf("workers claimed after failed initial enqueue: %d", backend.claimCalls)
	}
}

func TestExpiryHandlerReleasesOnlyAuthoritativelyEligibleUndispatchedHold(t *testing.T) {
	backend := newFakeDurableWorkerBackend()
	backend.reconcile = []govar.ReconciliationRecord{{RequestID: "request-1", TenantID: "tenant-1", PreviousState: govar.StateReserved,
		State: govar.StateExpiredUndispatched, TransitionEffective: true}}
	manager, err := newDurableWorkerManager(backend, nil, workerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	eligible := govarworker.WorkItem{RequestID: "request-1", LiabilityHeld: true, AuthoritativeReleaseEligible: true}
	result := manager.handleExpiry(context.Background(), eligible)
	if result.State != govarworker.StateCompleted || result.LiabilityDisposition != govarworker.LiabilityAuthoritativeRelease {
		t.Fatalf("eligible expiry result=%+v", result)
	}
	ineligible := eligible
	ineligible.AuthoritativeReleaseEligible = false
	result = manager.handleExpiry(context.Background(), ineligible)
	if result.LiabilityDisposition != govarworker.LiabilityPreserve {
		t.Fatalf("ineligible expiry result=%+v", result)
	}
	backend.reconcile = []govar.ReconciliationRecord{{RequestID: "request-1", TenantID: "tenant-1", PreviousState: govar.StateDispatched,
		State: govar.StateUnresolved, TransitionEffective: true}}
	result = manager.handleExpiry(context.Background(), eligible)
	if result.LiabilityDisposition != govarworker.LiabilityPreserve {
		t.Fatalf("ambiguous expiry released liability: result=%+v", result)
	}
}

func TestOutboxAndAuditHandlersFailClosedAndRecover(t *testing.T) {
	backend := newFakeDurableWorkerBackend()
	manager, err := newDurableWorkerManager(backend, nil, workerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	backend.pending = []govar.ReconciliationRecord{{RequestID: "request-1"}}
	result := manager.handleOutboxRepair(context.Background(), govarworker.WorkItem{RequestID: "request-1", LiabilityHeld: true})
	if result.State != govarworker.StateRetry || result.LiabilityDisposition != govarworker.LiabilityPreserve {
		t.Fatalf("pending outbox result=%+v", result)
	}
	backend.auditErr = errors.New("synthetic audit corruption")
	result, err = manager.handle(govarworker.KindAuditCheckpoint)(context.Background(), govarworker.WorkItem{})
	if err != nil || result.State != govarworker.StateRetry || manager.criticalHandlerErrors[govarworker.KindAuditCheckpoint] == "" {
		t.Fatalf("audit failure was not visible and retryable: result=%+v err=%v critical=%v", result, err, manager.criticalHandlerErrors)
	}
	backend.auditErr = nil
	result, err = manager.handle(govarworker.KindAuditCheckpoint)(context.Background(), govarworker.WorkItem{})
	if err != nil || result.State != govarworker.StateCompleted || manager.criticalHandlerErrors[govarworker.KindAuditCheckpoint] != "" {
		t.Fatalf("audit recovery did not clear critical state: result=%+v err=%v critical=%v", result, err, manager.criticalHandlerErrors)
	}
}

func TestWorkerMetricsComeFromCommittedQueueSnapshotsWithoutIdentityLabels(t *testing.T) {
	backend := newFakeDurableWorkerBackend()
	for index := range backend.workerSnapshots {
		if backend.workerSnapshots[index].Kind == govarworker.KindAuditCheckpoint {
			backend.workerSnapshots[index].CountsByState["PENDING"] = 4
			backend.workerSnapshots[index].OldestEligibleAge = 2 * time.Second
		}
	}
	metrics, err := newServiceMetricsFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	manager, err := newDurableWorkerManager(backend, metrics, workerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	manager.refreshMetrics(context.Background())
	recorder := httptest.NewRecorder()
	metrics.handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, expected := range []string{
		`govar_worker_backlog{kind="audit-checkpoint",state="ready"} 4`,
		`govar_worker_oldest_age_seconds{kind="audit-checkpoint"} 2`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("worker scrape lacks %q", expected)
		}
	}
	for _, forbidden := range []string{"request-1", "test-owner", "tenant-1"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("worker scrape exposed identity %q", forbidden)
		}
	}
}

func TestDurableWorkerConfigurationBounds(t *testing.T) {
	t.Setenv("GOV_AR_RECONCILIATION_INTERVAL", "250ms")
	t.Setenv("GOV_AR_RECONCILIATION_QUERY_LIMIT", "0")
	config := reconciliationWorkerConfigFromEnvironment()
	if config.Interval != 5*time.Second || config.QueryLimit != 100 {
		t.Fatalf("invalid environment values were accepted: %+v", config)
	}
	t.Setenv("GOV_AR_RECONCILIATION_INTERVAL", "10s")
	t.Setenv("GOV_AR_RECONCILIATION_QUERY_LIMIT", "250")
	config = reconciliationWorkerConfigFromEnvironment()
	if config.Interval != 10*time.Second || config.QueryLimit != 250 {
		t.Fatalf("valid environment values were not accepted: %+v", config)
	}
	invalid := workerTestConfig()
	invalid.PeriodicInterval = 0
	if _, err := newDurableWorkerManager(newFakeDurableWorkerBackend(), nil, invalid); err == nil {
		t.Fatal("zero periodic interval was accepted")
	}
}
