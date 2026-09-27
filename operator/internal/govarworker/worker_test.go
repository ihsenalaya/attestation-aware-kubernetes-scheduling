package govarworker

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type manualClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []manualWaiter
}

type manualWaiter struct {
	deadline time.Time
	ready    chan struct{}
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Wait(ctx context.Context, delay time.Duration) error {
	c.mu.Lock()
	w := manualWaiter{deadline: c.now.Add(delay), ready: make(chan struct{})}
	c.waiters = append(c.waiters, w)
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ready:
		return nil
	}
}

func (c *manualClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	remaining := c.waiters[:0]
	for _, waiter := range c.waiters {
		if !waiter.deadline.After(c.now) {
			close(waiter.ready)
		} else {
			remaining = append(remaining, waiter)
		}
	}
	c.waiters = remaining
	c.mu.Unlock()
}

func (c *manualClock) waiterReady() <-chan struct{} {
	ready := make(chan struct{})
	go func() {
		for {
			c.mu.Lock()
			n := len(c.waiters)
			c.mu.Unlock()
			if n > 0 {
				close(ready)
				return
			}
			runtime.Gosched()
		}
	}()
	return ready
}

type recordingLedger struct {
	mu          sync.Mutex
	clock       *manualClock
	owner       string
	items       []WorkItem
	claimErr    error
	renewErr    error
	completeErr error
	claims      int
	renews      int
	completes   int
	results     []WorkResult
	claimed     chan struct{}
	renewed     chan struct{}
	completed   chan struct{}
}

func newRecordingLedger(clock *manualClock, owner string, items ...WorkItem) *recordingLedger {
	return &recordingLedger{clock: clock, owner: owner, items: items,
		claimed: make(chan struct{}, 16), renewed: make(chan struct{}, 16), completed: make(chan struct{}, 16)}
}

func (l *recordingLedger) ClaimWork(_ context.Context, kind string, limit int, lease time.Duration) ([]WorkItem, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.claims++
	l.claimed <- struct{}{}
	if l.claimErr != nil {
		return nil, l.claimErr
	}
	if len(l.items) == 0 {
		return nil, nil
	}
	n := len(l.items)
	if n > limit {
		n = limit
	}
	result := append([]WorkItem(nil), l.items[:n]...)
	l.items = l.items[n:]
	for i := range result {
		result[i].Kind = Kind(kind)
		result[i].LeaseOwner = l.owner
		result[i].LeaseUntil = l.clock.Now().Add(lease)
		if result[i].Attempt == 0 {
			result[i].Attempt = 1
		}
	}
	return result, nil
}

func (l *recordingLedger) CompleteWork(_ context.Context, _ WorkItem, result WorkResult) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.completes++
	l.results = append(l.results, result)
	l.completed <- struct{}{}
	return l.completeErr
}

func (l *recordingLedger) RenewWork(_ context.Context, _ WorkItem, _ time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.renews++
	l.renewed <- struct{}{}
	return l.renewErr
}

func testConfig(clock *manualClock, kind Kind) Config {
	cfg := DefaultConfig(kind)
	cfg.Clock = clock
	cfg.Lease = 30 * time.Second
	cfg.RenewEvery = 10 * time.Second
	cfg.PollEvery = 5 * time.Second
	cfg.BaseBackoff = time.Second
	cfg.MaxBackoff = 8 * time.Second
	cfg.MaxAttempts = 3
	cfg.HeartbeatTTL = time.Minute
	return cfg
}

func validItem(id string) WorkItem {
	return WorkItem{ID: id, Kind: KindExpiry, RequestID: "request-" + id, ProviderAttemptID: "attempt-" + id,
		Attempt: 1, LiabilityHeld: true, PayloadDigest: "digest"}
}

func waitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestKindsAreClosedAndConfigurationValidated(t *testing.T) {
	clock := newManualClock()
	if len(AllKinds()) != 5 || Kind("sixth-kind").Valid() {
		t.Fatalf("closed kinds=%v", AllKinds())
	}
	for _, kind := range AllKinds() {
		ledger := newRecordingLedger(clock, "worker")
		if _, err := New(ledger, HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
			return WorkResult{ReasonCode: "ok"}, nil
		}), testConfig(clock, kind)); err != nil {
			t.Fatalf("kind %s: %v", kind, err)
		}
	}
	bad := testConfig(clock, Kind("unknown"))
	if _, err := New(newRecordingLedger(clock, "worker"), HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) { return WorkResult{}, nil }), bad); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func TestFakeClockLoopRunsImmediatelyPollsAndShutsDownCleanly(t *testing.T) {
	clock := newManualClock()
	ledger := newRecordingLedger(clock, "worker")
	w, _ := New(ledger, HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		return WorkResult{ReasonCode: "ok"}, nil
	}), testConfig(clock, KindExpiry))
	runtime := w.Start(context.Background())
	waitSignal(t, ledger.claimed, "immediate claim")
	waitSignal(t, clock.waiterReady(), "poll waiter")
	clock.Advance(5 * time.Second)
	waitSignal(t, ledger.claimed, "fake-clock poll")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if !w.Snapshot().Stopped {
		t.Fatal("worker did not record clean stop")
	}
}

type codedFailure string

func (e codedFailure) Error() string      { return string(e) }
func (e codedFailure) ReasonCode() string { return string(e) }

func TestRetryBackoffJitterAndDeadLetterAlwaysPreserveLiability(t *testing.T) {
	clock := newManualClock()
	first, poison := validItem("retry"), validItem("poison")
	poison.Attempt = 3
	ledger := newRecordingLedger(clock, "worker", first, poison)
	cfg := testConfig(clock, KindExpiry)
	cfg.Jitter = func(upper time.Duration) time.Duration {
		if upper != 7*time.Second {
			t.Fatalf("jitter upper=%s want 7s", upper)
		}
		return 500 * time.Millisecond
	}
	w, _ := New(ledger, HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		return WorkResult{}, codedFailure("authoritative_evidence_missing")
	}), cfg)
	w.runPass(context.Background())
	if len(ledger.results) != 2 {
		t.Fatalf("results=%+v", ledger.results)
	}
	var retry, dead WorkResult
	for _, result := range ledger.results {
		switch result.State {
		case StateRetry:
			retry = result
		case StateDeadLetter:
			dead = result
		}
	}
	if retry.State != StateRetry || retry.LiabilityDisposition != LiabilityPreserve || retry.ReasonCode != "authoritative_evidence_missing" || retry.NextAttemptAt.Sub(clock.Now()) != 1500*time.Millisecond {
		t.Fatalf("retry=%+v", retry)
	}
	if dead.State != StateDeadLetter || dead.LiabilityDisposition != LiabilityPreserve || !dead.NextAttemptAt.IsZero() {
		t.Fatalf("dead=%+v", dead)
	}
	h := w.Snapshot()
	if h.Retried != 1 || h.DeadLettered != 1 {
		t.Fatalf("heartbeat=%+v", h)
	}
	if got := w.backoff(99); got != cfg.MaxBackoff {
		t.Fatalf("bounded backoff=%s want=%s", got, cfg.MaxBackoff)
	}
}

func TestHandlerRequestedRetryCannotBypassMaximumAttempts(t *testing.T) {
	clock := newManualClock()
	item := validItem("explicit-retry-at-limit")
	item.Attempt = 3
	ledger := newRecordingLedger(clock, "worker", item)
	cfg := testConfig(clock, KindExpiry)
	w, _ := New(ledger, HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		return WorkResult{State: StateRetry, ReasonCode: "evidence_still_missing", NextAttemptAt: clock.Now().Add(24 * time.Hour)}, nil
	}), cfg)
	w.runPass(context.Background())
	if len(ledger.results) != 1 {
		t.Fatalf("results=%+v", ledger.results)
	}
	result := ledger.results[0]
	if result.State != StateDeadLetter || !result.NextAttemptAt.IsZero() || result.LiabilityDisposition != LiabilityPreserve || result.ReasonCode != "evidence_still_missing" {
		t.Fatalf("max-attempt explicit retry=%+v", result)
	}
	if h := w.Snapshot(); h.Retried != 0 || h.DeadLettered != 1 {
		t.Fatalf("heartbeat=%+v", h)
	}
}

func TestLeaseRenewalAndRenewalFailurePreventStaleAcknowledgement(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "renew", true: "failure"}[fail], func(t *testing.T) {
			clock := newManualClock()
			ledger := newRecordingLedger(clock, "worker", validItem("lease"))
			if fail {
				ledger.renewErr = errors.New("lost ownership")
			}
			started := make(chan struct{})
			release := make(chan struct{})
			w, _ := New(ledger, HandlerFunc(func(ctx context.Context, _ WorkItem) (WorkResult, error) {
				close(started)
				select {
				case <-release:
					return WorkResult{ReasonCode: "reconciled"}, nil
				case <-ctx.Done():
					return WorkResult{}, ctx.Err()
				}
			}), testConfig(clock, KindExpiry))
			done := make(chan struct{})
			go func() { w.runPass(context.Background()); close(done) }()
			waitSignal(t, started, "handler start")
			waitSignal(t, clock.waiterReady(), "renewal waiter")
			clock.Advance(10 * time.Second)
			waitSignal(t, ledger.renewed, "renewal")
			if !fail {
				close(release)
			}
			waitSignal(t, done, "pass completion")
			if fail && ledger.completes != 0 {
				t.Fatal("stale claimant acknowledged after renewal failure")
			}
			if !fail && ledger.completes != 1 {
				t.Fatal("successfully renewed work was not acknowledged")
			}
		})
	}
}

func TestReleaseRequiresLedgerClaimedAuthoritativeEvidence(t *testing.T) {
	clock := newManualClock()
	unauthorized := validItem("unauthorized")
	authorized := validItem("authorized")
	authorized.AuthoritativeReleaseEligible = true
	ledger := newRecordingLedger(clock, "worker", unauthorized, authorized)
	w, _ := New(ledger, HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		return WorkResult{ReasonCode: "authoritative_no_delivery", LiabilityDisposition: LiabilityAuthoritativeRelease}, nil
	}), testConfig(clock, KindExpiry))
	w.runPass(context.Background())
	var blocked, released bool
	for _, result := range ledger.results {
		blocked = blocked || (result.State == StateDeadLetter && result.LiabilityDisposition == LiabilityPreserve && result.ReasonCode == "unauthorized_liability_release")
		released = released || (result.State == StateCompleted && result.LiabilityDisposition == LiabilityAuthoritativeRelease)
	}
	if !blocked || !released {
		t.Fatalf("release results=%+v", ledger.results)
	}
}

func TestCrashBoundariesLeaveLeaseForSafeIdempotentTakeover(t *testing.T) {
	clock := newManualClock()
	// Claim then crash before transition/acknowledgement.
	ledger := newRecordingLedger(clock, "worker-a", validItem("claim-crash"))
	started := make(chan struct{})
	w, _ := New(ledger, HandlerFunc(func(ctx context.Context, _ WorkItem) (WorkResult, error) {
		close(started)
		<-ctx.Done()
		return WorkResult{}, ctx.Err()
	}), testConfig(clock, KindExpiry))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.runPass(ctx); close(done) }()
	waitSignal(t, started, "claimed handler")
	cancel()
	waitSignal(t, done, "crashed pass")
	if ledger.completes != 0 {
		t.Fatal("crash before transition fabricated acknowledgement")
	}

	// Effective transition then crash before acknowledgement. The takeover sees
	// the same durable identity and the idempotent handler has one total effect.
	var effective atomic.Int32
	item := validItem("after-transition")
	first := newRecordingLedger(clock, "worker-a", item)
	first.completeErr = errors.New("crash before ack")
	handler := HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		effective.CompareAndSwap(0, 1)
		return WorkResult{ReasonCode: "transition_committed"}, nil
	})
	w1, _ := New(first, handler, testConfig(clock, KindExpiry))
	w1.runPass(context.Background())
	second := newRecordingLedger(clock, "worker-b", item)
	w2, _ := New(second, handler, testConfig(clock, KindExpiry))
	w2.runPass(context.Background())
	if effective.Load() != 1 || second.completes != 1 {
		t.Fatalf("effective=%d takeover completes=%d", effective.Load(), second.completes)
	}
}

func TestKilledClaimantLeaseExpiresForSafeTakeover(t *testing.T) {
	clock := newManualClock()
	store := &sharedLeaseStore{clock: clock, item: validItem("takeover")}
	started := make(chan struct{})
	firstHandler := HandlerFunc(func(ctx context.Context, _ WorkItem) (WorkResult, error) {
		close(started)
		<-ctx.Done()
		return WorkResult{}, ctx.Err()
	})
	w1, _ := New(&sharedLedger{store: store, owner: "killed"}, firstHandler, testConfig(clock, KindExpiry))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w1.runPass(ctx); close(done) }()
	waitSignal(t, started, "first claimant")
	cancel()
	waitSignal(t, done, "killed claimant exit")
	if store.completions != 0 {
		t.Fatal("killed claimant completed work")
	}
	clock.Advance(31 * time.Second)
	var effects atomic.Int32
	w2, _ := New(&sharedLedger{store: store, owner: "takeover"}, HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		effects.Add(1)
		return WorkResult{ReasonCode: "takeover_completed"}, nil
	}), testConfig(clock, KindExpiry))
	w2.runPass(context.Background())
	if effects.Load() != 1 || store.completions != 1 {
		t.Fatalf("takeover effects=%d completions=%d", effects.Load(), store.completions)
	}
}

func TestConcurrentClaimantsHaveOneEffectiveCompletion(t *testing.T) {
	clock := newManualClock()
	store := &sharedLeaseStore{clock: clock, item: validItem("contended")}
	var effects atomic.Int32
	handler := HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		effects.Add(1)
		return WorkResult{ReasonCode: "done"}, nil
	})
	w1, _ := New(&sharedLedger{store: store, owner: "one"}, handler, testConfig(clock, KindExpiry))
	w2, _ := New(&sharedLedger{store: store, owner: "two"}, handler, testConfig(clock, KindExpiry))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); w1.runPass(context.Background()) }()
	go func() { defer wg.Done(); w2.runPass(context.Background()) }()
	wg.Wait()
	if effects.Load() != 1 || store.completions != 1 {
		t.Fatalf("effects=%d completions=%d", effects.Load(), store.completions)
	}
}

type sharedLeaseStore struct {
	mu          sync.Mutex
	clock       *manualClock
	item        WorkItem
	owner       string
	leaseUntil  time.Time
	completed   bool
	completions int
}

type sharedLedger struct {
	store *sharedLeaseStore
	owner string
}

func (l *sharedLedger) ClaimWork(_ context.Context, kind string, _ int, lease time.Duration) ([]WorkItem, error) {
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.store.completed || (l.store.owner != "" && l.store.leaseUntil.After(l.store.clock.Now())) {
		return nil, nil
	}
	l.store.owner = l.owner
	l.store.leaseUntil = l.store.clock.Now().Add(lease)
	l.store.item.Kind, l.store.item.LeaseOwner, l.store.item.LeaseUntil = Kind(kind), l.owner, l.store.leaseUntil
	return []WorkItem{l.store.item}, nil
}

func (l *sharedLedger) RenewWork(_ context.Context, item WorkItem, lease time.Duration) error {
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.store.owner != item.LeaseOwner || l.store.completed {
		return errors.New("lease lost")
	}
	l.store.leaseUntil = l.store.clock.Now().Add(lease)
	return nil
}

func (l *sharedLedger) CompleteWork(_ context.Context, item WorkItem, _ WorkResult) error {
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.store.owner != item.LeaseOwner || l.store.completed {
		return errors.New("lease lost")
	}
	l.store.completed = true
	l.store.completions++
	return nil
}

func TestHeartbeatFailureRecoveryStalenessAndBoundedShutdown(t *testing.T) {
	clock := newManualClock()
	ledger := newRecordingLedger(clock, "worker")
	ledger.claimErr = errors.New("db unavailable")
	w, _ := New(ledger, HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		return WorkResult{ReasonCode: "ok"}, nil
	}), testConfig(clock, KindExpiry))
	w.runPass(context.Background())
	if err := w.Healthy(); err == nil {
		t.Fatal("database/claim failure reported healthy")
	}
	ledger.claimErr = nil
	w.runPass(context.Background())
	if err := w.Healthy(); err != nil {
		t.Fatalf("recovered worker unhealthy: %v", err)
	}
	clock.Advance(2 * time.Minute)
	if err := w.Healthy(); err == nil {
		t.Fatal("stale heartbeat reported healthy")
	}

	// A cancellation-violating handler cannot make Shutdown block beyond the
	// caller's deadline. Release it afterwards to avoid leaking the test goroutine.
	blockingLedger := newRecordingLedger(clock, "worker", validItem("blocked"))
	started, release := make(chan struct{}), make(chan struct{})
	blocked, _ := New(blockingLedger, HandlerFunc(func(context.Context, WorkItem) (WorkResult, error) {
		close(started)
		<-release
		return WorkResult{ReasonCode: "done"}, nil
	}), testConfig(clock, KindExpiry))
	runtime := blocked.Start(context.Background())
	waitSignal(t, started, "blocking handler")
	deadline, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Shutdown(deadline); err == nil {
		t.Fatal("bounded shutdown accepted a cancellation-violating handler")
	}
	close(release)
	cleanup, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
	defer cleanupCancel()
	if err := runtime.Shutdown(cleanup); err != nil {
		t.Fatalf("cleanup shutdown: %v", err)
	}
}
