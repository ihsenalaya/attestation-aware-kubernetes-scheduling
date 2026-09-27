// Package govarworker provides the lease-based execution core for GOV-AR's
// durable background work. Durable claim/renew/complete transactions belong to
// the Ledger implementation; this package owns bounded scheduling, retries,
// heartbeats, cancellation, and fail-closed liability dispositions.
package govarworker

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"
)

// Kind is closed: adding a worker requires a source change and tests.
type Kind string

const (
	KindExpiry                 Kind = "expiry"
	KindDeliveryReconciliation Kind = "delivery-reconciliation"
	KindOutboxRepair           Kind = "outbox-repair"
	KindCalibrationDrift       Kind = "calibration-drift"
	KindAuditCheckpoint        Kind = "audit-checkpoint"
)

var allKinds = []Kind{KindExpiry, KindDeliveryReconciliation, KindOutboxRepair, KindCalibrationDrift, KindAuditCheckpoint}

func AllKinds() []Kind { return append([]Kind(nil), allKinds...) }

func (k Kind) Valid() bool {
	for _, candidate := range allKinds {
		if k == candidate {
			return true
		}
	}
	return false
}

type WorkState string

const (
	StateCompleted  WorkState = "COMPLETED"
	StateRetry      WorkState = "RETRY"
	StateDeadLetter WorkState = "DEAD_LETTER"
)

type LiabilityDisposition string

const (
	LiabilityPreserve             LiabilityDisposition = "PRESERVE"
	LiabilityAuthoritativeRelease LiabilityDisposition = "AUTHORITATIVE_RELEASE"
)

// WorkItem is the durable claim returned by Ledger. Attempt is one-based.
// AuthoritativeReleaseEligible is set only by an atomic ledger query proving
// that the corresponding transition may release liability.
type WorkItem struct {
	ID                           string
	Kind                         Kind
	RequestID                    string
	ProviderAttemptID            string
	LeaseOwner                   string
	LeaseUntil                   time.Time
	Attempt                      int
	LiabilityHeld                bool
	AuthoritativeReleaseEligible bool
	PayloadDigest                string
}

// WorkResult is atomically acknowledged by Ledger. RETRY and DEAD_LETTER are
// always normalized to PRESERVE when a liability is held.
type WorkResult struct {
	State                WorkState
	ReasonCode           string
	NextAttemptAt        time.Time
	LiabilityDisposition LiabilityDisposition
}

// Ledger deliberately exposes only durable lease operations. A production
// implementation binds lease_owner to its configured worker identity.
type Ledger interface {
	ClaimWork(ctx context.Context, kind string, limit int, lease time.Duration) ([]WorkItem, error)
	CompleteWork(ctx context.Context, item WorkItem, result WorkResult) error
	RenewWork(ctx context.Context, item WorkItem, lease time.Duration) error
}

type Handler interface {
	Handle(ctx context.Context, item WorkItem) (WorkResult, error)
}

type HandlerFunc func(context.Context, WorkItem) (WorkResult, error)

func (f HandlerFunc) Handle(ctx context.Context, item WorkItem) (WorkResult, error) {
	return f(ctx, item)
}

// Clock makes every scheduling assertion independent of wall-clock sleeps.
type Clock interface {
	Now() time.Time
	Wait(ctx context.Context, delay time.Duration) error
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now().UTC() }
func (RealClock) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Jitter returns a non-negative additive delay. The worker clamps it so the
// total never exceeds MaxBackoff.
type Jitter func(upper time.Duration) time.Duration

type Config struct {
	Kind         Kind
	BatchSize    int
	Lease        time.Duration
	RenewEvery   time.Duration
	PollEvery    time.Duration
	MaxAttempts  int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	HeartbeatTTL time.Duration
	Clock        Clock
	Jitter       Jitter
}

func DefaultConfig(kind Kind) Config {
	return Config{Kind: kind, BatchSize: 32, Lease: 30 * time.Second, RenewEvery: 10 * time.Second,
		PollEvery: time.Second, MaxAttempts: 8, BaseBackoff: time.Second,
		MaxBackoff: 5 * time.Minute, HeartbeatTTL: time.Minute, Clock: RealClock{},
		Jitter: func(time.Duration) time.Duration { return 0 }}
}

type Heartbeat struct {
	Kind               Kind
	LastPassStarted    time.Time
	LastClaimSucceeded time.Time
	LastPassSucceeded  time.Time
	LastErrorAt        time.Time
	LastErrorCode      string
	Claims             uint64
	Completed          uint64
	Retried            uint64
	DeadLettered       uint64
	RenewalFailures    uint64
	Active             int
	Stopped            bool
}

type Worker struct {
	ledger  Ledger
	handler Handler
	config  Config

	mu        sync.RWMutex
	heartbeat Heartbeat
}

var reasonPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func New(ledger Ledger, handler Handler, config Config) (*Worker, error) {
	if ledger == nil || handler == nil {
		return nil, errors.New("ledger and handler are required")
	}
	if !config.Kind.Valid() {
		return nil, fmt.Errorf("unknown worker kind %q", config.Kind)
	}
	if config.BatchSize < 1 || config.BatchSize > 10_000 || config.Lease <= 0 || config.RenewEvery <= 0 || config.RenewEvery >= config.Lease ||
		config.PollEvery <= 0 || config.MaxAttempts < 1 || config.BaseBackoff <= 0 || config.MaxBackoff < config.BaseBackoff || config.HeartbeatTTL <= 0 {
		return nil, errors.New("invalid worker timing, batch, retry, or heartbeat bounds")
	}
	if config.Clock == nil {
		return nil, errors.New("clock is required")
	}
	if config.Jitter == nil {
		config.Jitter = func(time.Duration) time.Duration { return 0 }
	}
	return &Worker{ledger: ledger, handler: handler, config: config, heartbeat: Heartbeat{Kind: config.Kind}}, nil
}

// Run executes one immediate pass and then fake-clock-controlled polling until
// cancellation. Context cancellation is a normal clean shutdown.
func (w *Worker) Run(ctx context.Context) error {
	defer w.markStopped()
	for {
		if ctx.Err() != nil {
			return nil
		}
		w.runPass(ctx)
		if err := w.config.Clock.Wait(ctx, w.config.PollEvery); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.recordError("clock_wait_failed")
		}
	}
}

func (w *Worker) runPass(ctx context.Context) {
	now := w.config.Clock.Now().UTC()
	w.mu.Lock()
	w.heartbeat.LastPassStarted = now
	w.heartbeat.Stopped = false
	w.mu.Unlock()

	items, err := w.ledger.ClaimWork(ctx, string(w.config.Kind), w.config.BatchSize, w.config.Lease)
	if err != nil {
		w.recordError("claim_failed")
		return
	}
	w.mu.Lock()
	w.heartbeat.LastClaimSucceeded = now
	w.heartbeat.Claims += uint64(len(items))
	w.heartbeat.LastErrorCode = ""
	w.mu.Unlock()
	var processing sync.WaitGroup
	for _, item := range items {
		if ctx.Err() != nil {
			break
		}
		item := item
		processing.Add(1)
		go func() {
			defer processing.Done()
			if err := w.process(ctx, item); err != nil {
				w.recordError(reasonFromError(err, "work_failed"))
			}
		}()
	}
	processing.Wait()
	w.mu.Lock()
	w.heartbeat.LastPassSucceeded = w.config.Clock.Now().UTC()
	w.mu.Unlock()
}

func (w *Worker) process(parent context.Context, item WorkItem) error {
	if err := w.validateClaim(item); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	doneRenewing := make(chan struct{})
	renewalFailure := make(chan error, 1)
	go func() {
		defer close(doneRenewing)
		for {
			if err := w.config.Clock.Wait(ctx, w.config.RenewEvery); err != nil {
				return
			}
			if err := w.ledger.RenewWork(ctx, item, w.config.Lease); err != nil {
				select {
				case renewalFailure <- err:
				default:
				}
				cancel()
				return
			}
		}
	}()

	w.changeActive(1)
	result, handleErr := w.handler.Handle(ctx, item)
	w.changeActive(-1)
	cancel()
	<-doneRenewing
	select {
	case err := <-renewalFailure:
		w.mu.Lock()
		w.heartbeat.RenewalFailures++
		w.mu.Unlock()
		// Ownership is uncertain. Do not acknowledge; lease takeover is the only
		// safe next action, even if the handler happened to return concurrently.
		return fmt.Errorf("lease_renewal_failed: %w", err)
	default:
	}
	if parent.Err() != nil {
		// Crash/shutdown after claim but before acknowledgement leaves the item
		// leased for safe takeover; no completion is fabricated.
		return parent.Err()
	}
	result = w.normalizeResult(item, result, handleErr)
	if err := w.ledger.CompleteWork(parent, item, result); err != nil {
		return fmt.Errorf("completion_failed: %w", err)
	}
	w.mu.Lock()
	switch result.State {
	case StateCompleted:
		w.heartbeat.Completed++
	case StateRetry:
		w.heartbeat.Retried++
	case StateDeadLetter:
		w.heartbeat.DeadLettered++
	}
	w.mu.Unlock()
	return nil
}

func (w *Worker) validateClaim(item WorkItem) error {
	now := w.config.Clock.Now().UTC()
	if item.ID == "" || item.Kind != w.config.Kind || item.LeaseOwner == "" || item.Attempt < 1 || item.LeaseUntil.IsZero() || !item.LeaseUntil.After(now) {
		return errors.New("invalid_claim")
	}
	return nil
}

func (w *Worker) normalizeResult(item WorkItem, result WorkResult, handleErr error) WorkResult {
	if handleErr != nil {
		result.ReasonCode = reasonFromError(handleErr, result.ReasonCode)
		if item.Attempt >= w.config.MaxAttempts {
			result.State = StateDeadLetter
		} else {
			result.State = StateRetry
		}
	}
	if result.State == "" {
		result.State = StateCompleted
	}
	if !reasonPattern.MatchString(result.ReasonCode) {
		result.ReasonCode = "worker_internal_error"
	}
	switch result.State {
	case StateRetry:
		if item.Attempt >= w.config.MaxAttempts {
			result.State = StateDeadLetter
			result.NextAttemptAt = time.Time{}
		} else {
			result.NextAttemptAt = w.config.Clock.Now().UTC().Add(w.backoff(item.Attempt))
		}
		if item.LiabilityHeld {
			result.LiabilityDisposition = LiabilityPreserve
		}
	case StateDeadLetter:
		result.NextAttemptAt = time.Time{}
		if item.LiabilityHeld {
			result.LiabilityDisposition = LiabilityPreserve
		}
	case StateCompleted:
		result.NextAttemptAt = time.Time{}
		if item.LiabilityHeld {
			if result.LiabilityDisposition == LiabilityAuthoritativeRelease && !item.AuthoritativeReleaseEligible {
				// A handler cannot manufacture release authority.
				result.State = StateDeadLetter
				result.ReasonCode = "unauthorized_liability_release"
				result.LiabilityDisposition = LiabilityPreserve
			} else if result.LiabilityDisposition == "" {
				result.LiabilityDisposition = LiabilityPreserve
			}
		}
	default:
		result.State = StateDeadLetter
		result.ReasonCode = "invalid_worker_result"
		if item.LiabilityHeld {
			result.LiabilityDisposition = LiabilityPreserve
		}
	}
	return result
}

func (w *Worker) backoff(attempt int) time.Duration {
	delay := w.config.BaseBackoff
	for i := 1; i < attempt && delay < w.config.MaxBackoff; i++ {
		if delay > w.config.MaxBackoff/2 {
			delay = w.config.MaxBackoff
			break
		}
		delay *= 2
	}
	remaining := w.config.MaxBackoff - delay
	if remaining <= 0 {
		return w.config.MaxBackoff
	}
	jitter := w.config.Jitter(remaining)
	if jitter < 0 {
		jitter = 0
	}
	if jitter > remaining {
		jitter = remaining
	}
	return delay + jitter
}

func (w *Worker) Snapshot() Heartbeat {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.heartbeat
}

func (w *Worker) Healthy() error {
	h := w.Snapshot()
	if h.LastPassStarted.IsZero() {
		return errors.New("worker has no heartbeat")
	}
	if w.config.Clock.Now().UTC().Sub(h.LastPassStarted) > w.config.HeartbeatTTL {
		return errors.New("worker heartbeat is stale")
	}
	if h.LastErrorCode != "" {
		return fmt.Errorf("worker error: %s", h.LastErrorCode)
	}
	return nil
}

func (w *Worker) recordError(code string) {
	if !reasonPattern.MatchString(code) {
		code = "worker_internal_error"
	}
	w.mu.Lock()
	w.heartbeat.LastErrorAt = w.config.Clock.Now().UTC()
	w.heartbeat.LastErrorCode = code
	w.mu.Unlock()
}

func (w *Worker) changeActive(delta int) {
	w.mu.Lock()
	w.heartbeat.Active += delta
	w.mu.Unlock()
}

func (w *Worker) markStopped() {
	w.mu.Lock()
	w.heartbeat.Stopped = true
	w.heartbeat.Active = 0
	w.mu.Unlock()
}

func reasonFromError(err error, fallback string) string {
	type reasonCoder interface{ ReasonCode() string }
	var coded reasonCoder
	if errors.As(err, &coded) && reasonPattern.MatchString(coded.ReasonCode()) {
		return coded.ReasonCode()
	}
	if reasonPattern.MatchString(fallback) {
		return fallback
	}
	return "handler_failed"
}

// Runtime owns one worker goroutine. Shutdown is bounded by the caller's
// context; Go cannot forcibly terminate a handler that violates cancellation.
type Runtime struct {
	cancel context.CancelFunc
	done   chan error
}

func (w *Worker) Start(parent context.Context) *Runtime {
	ctx, cancel := context.WithCancel(parent)
	runtime := &Runtime{cancel: cancel, done: make(chan error, 1)}
	go func() { runtime.done <- w.Run(ctx); close(runtime.done) }()
	return runtime
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	r.cancel()
	select {
	case err := <-r.done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("worker shutdown deadline: %w", ctx.Err())
	}
}
