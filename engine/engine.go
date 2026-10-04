// Package engine runs declarative load scenarios against a target with
// bounded concurrency, backpressure, deterministic fault injection, circuit
// breaking and resumable checkpoints.
package engine

import (
	"context"
	"errors"
	"io/fs"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/curve"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
)

// Outcome classifies a completed action.
type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeFailure
	OutcomeTimeout
	OutcomeCanceled
)

// StopReason explains why a run ended.
type StopReason string

const (
	StopCompleted StopReason = "completed"
	StopStopped   StopReason = "stopped"
	StopBreaker   StopReason = "breaker"
	StopCanceled  StopReason = "canceled"
)

// Action executes one scenario step. Implementations must honour context
// cancellation so in-flight work converges promptly.
type Action interface {
	Execute(ctx context.Context, rng *rand.Rand) error
}

// Config controls engine behaviour.
type Config struct {
	Seed            int64
	MaxConcurrency  int           // hard cap on in-flight actions; 0 = unlimited
	TickInterval    time.Duration // curve re-evaluation interval
	ActionTimeout   time.Duration // per-action timeout; 0 = none
	ShutdownTimeout time.Duration // grace period for in-flight actions on stop

	// Fault / latency injection (deterministic per seed).
	FaultProbability float64
	LatencyMin       time.Duration
	LatencyMax       time.Duration

	// Circuit breaker.
	BreakerEnabled    bool
	BreakerThreshold  float64 // error rate (0..1) that trips the breaker
	BreakerMinSamples int     // minimum samples before the breaker may trip
	BreakerWindow     time.Duration

	// Checkpointing.
	CheckpointPath string
	Resume         bool
}

type workerHandle struct {
	cancel context.CancelFunc
}

type breaker struct {
	enabled    bool
	threshold  float64
	minSamples int
	window     time.Duration

	mu       sync.Mutex
	events   []breakerEvent
	tripped  bool
	tripCh   chan struct{}
}

type breakerEvent struct {
	at      time.Time
	outcome Outcome
}

func newBreaker(cfg Config) *breaker {
	return &breaker{
		enabled:    cfg.BreakerEnabled,
		threshold:  cfg.BreakerThreshold,
		minSamples: cfg.BreakerMinSamples,
		window:     cfg.BreakerWindow,
		tripCh:     make(chan struct{}),
	}
}

func (b *breaker) record(outcome Outcome) {
	if !b.enabled {
		return
	}
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tripped {
		return
	}
	b.events = append(b.events, breakerEvent{at: now, outcome: outcome})
	cutoff := now.Add(-b.window)
	kept := b.events[:0]
	for _, ev := range b.events {
		if ev.at.After(cutoff) {
			kept = append(kept, ev)
		}
	}
	b.events = kept
	if len(b.events) < b.minSamples {
		return
	}
	var errs int
	for _, ev := range b.events {
		if ev.outcome == OutcomeFailure || ev.outcome == OutcomeTimeout {
			errs++
		}
	}
	if float64(errs)/float64(len(b.events)) >= b.threshold {
		b.tripped = true
		close(b.tripCh)
	}
}

func (b *breaker) trippedCh() <-chan struct{} { return b.tripCh }

// Engine executes a scenario driven by a load curve.
type Engine struct {
	cfg      Config
	scen     *scenario.Scenario
	curve    curve.Curve
	stats    *Stats
	injector *Injector

	mu           sync.Mutex
	rootCancel   context.CancelFunc
	stopOnce     sync.Once
	stopCh       chan struct{}
	draining     chan struct{}
	drainOnce    sync.Once
	capOverride  int
	workers      map[int]*workerHandle
	wg           sync.WaitGroup
	brk          *breaker
	elapsed      time.Duration
	vuStates     map[int]*VUState
}

// New creates an engine for the given scenario and curve.
func New(cfg Config, scen *scenario.Scenario, c curve.Curve) *Engine {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 200 * time.Millisecond
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.BreakerWindow <= 0 {
		cfg.BreakerWindow = 30 * time.Second
	}
	if cfg.BreakerMinSamples <= 0 {
		cfg.BreakerMinSamples = 20
	}
	if cfg.BreakerThreshold <= 0 {
		cfg.BreakerThreshold = 0.5
	}
	if cfg.Seed == 0 {
		cfg.Seed = scen.Seed
	}
	return &Engine{
		cfg:      cfg,
		scen:     scen,
		curve:    c,
		stats:    NewStats(),
		injector: &Injector{
			FaultProbability: cfg.FaultProbability,
			LatencyMin:       cfg.LatencyMin,
			LatencyMax:       cfg.LatencyMax,
		},
		stopCh:   make(chan struct{}),
		draining: make(chan struct{}),
		workers:  make(map[int]*workerHandle),
		brk:      newBreaker(cfg),
		vuStates: make(map[int]*VUState),
	}
}

// SetConcurrency dynamically overrides the concurrency cap. A value <= 0
// removes the override and lets the curve drive concurrency again.
func (e *Engine) SetConcurrency(n int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.capOverride = n
}

// Stop requests an early stop. In-flight actions are cancelled and the run
// finalizes with a checkpoint.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
}

// beginDrain marks the run as completed: workers finish their current action
// but do not start new ones, so in-flight work is counted as success.
func (e *Engine) beginDrain() {
	e.drainOnce.Do(func() { close(e.draining) })
}

func (e *Engine) effectiveCap() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	cap := math.MaxInt
	if e.cfg.MaxConcurrency > 0 {
		cap = e.cfg.MaxConcurrency
	}
	if e.capOverride > 0 && e.capOverride < cap {
		cap = e.capOverride
	}
	return cap
}

// Run executes the scenario until the curve ends, the context is cancelled,
// the breaker trips or Stop is called.
func (e *Engine) Run(ctx context.Context) (*Report, error) {
	if err := e.scen.Validate(); err != nil {
		return nil, err
	}
	duration := e.runDuration()

	rootCtx, cancel := context.WithCancel(ctx)
	e.rootCancel = cancel
	defer cancel()

	// Resume from checkpoint if requested.
	if e.cfg.Resume && e.cfg.CheckpointPath != "" {
		if cp, err := LoadCheckpoint(e.cfg.CheckpointPath); err == nil {
			e.elapsed = cp.Elapsed
			for id, st := range cp.VUStates {
				e.vuStates[id] = st
			}
			e.stats.Merge(cp.Stats)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}

	e.stats.SetStartTime(time.Now())
	stopReason := StopCompleted

	controllerDone := make(chan StopReason, 1)
	go e.controller(rootCtx, duration, controllerDone)

	select {
	case reason := <-controllerDone:
		stopReason = reason
	case <-ctx.Done():
		stopReason = StopCanceled
	}
	// On completion, let in-flight actions finish (drain). On any other stop
	// reason the root context was already cancelled by the controller.
	if stopReason != StopCompleted {
		cancel()
	}

	// Wait for in-flight actions to converge, bounded by shutdown timeout.
	waitCh := make(chan struct{})
	go func() { e.wg.Wait(); close(waitCh) }()
	select {
	case <-waitCh:
	case <-time.After(e.cfg.ShutdownTimeout):
	}
	cancel() // clean up any stragglers
	e.stats.MarkEnd(time.Now())

	if e.cfg.CheckpointPath != "" {
		cp := e.buildCheckpoint()
		if err := SaveCheckpoint(e.cfg.CheckpointPath, cp); err != nil {
			return nil, err
		}
	}

	snap := e.stats.Snapshot()
	if e.elapsed > 0 {
		snap.Elapsed = e.elapsed
	}
	report := snap.Report(e.scen.Name, e.cfg.Seed)
	report.StopReason = string(stopReason)
	return &report, nil
}

func (e *Engine) runDuration() time.Duration {
	if dp, ok := e.curve.(curve.DurationProvider); ok {
		return dp.Duration()
	}
	return 60 * time.Second
}

func (e *Engine) controller(ctx context.Context, duration time.Duration, done chan<- StopReason) {
	ticker := time.NewTicker(e.cfg.TickInterval)
	defer ticker.Stop()
	start := time.Now()
	baseline := e.elapsed // resume baseline (0 for fresh runs)
	for {
		select {
		case <-ctx.Done():
			done <- StopCanceled
			return
		case <-e.stopCh:
			e.rootCancel()
			done <- StopStopped
			return
		case <-e.brk.trippedCh():
			e.rootCancel()
			done <- StopBreaker
			return
		case <-ticker.C:
		}
		elapsed := baseline + time.Since(start)
		e.mu.Lock()
		e.elapsed = elapsed
		e.mu.Unlock()
		if elapsed >= duration {
			e.beginDrain()
			done <- StopCompleted
			return
		}
		e.adjust(ctx, elapsed)
	}
}

func (e *Engine) adjust(ctx context.Context, elapsed time.Duration) {
	target := e.curve.At(elapsed)
	if target < 0 {
		target = 0
	}
	cap := e.effectiveCap()
	if cap != math.MaxInt && target > float64(cap) {
		target = float64(cap)
	}
	n := int(math.Round(target))

	e.mu.Lock()
	defer e.mu.Unlock()

	// Scale up: spawn missing workers.
	for id := 0; id < n; id++ {
		if _, ok := e.workers[id]; ok {
			continue
		}
		vuID := id
		startStep := 0
		iteration := int64(0)
		if st, ok := e.vuStates[vuID]; ok {
			startStep = st.StepIndex
			iteration = st.Iteration
		}
		wctx, wcancel := context.WithCancel(ctx)
		wctx = context.WithValue(wctx, vuContextKey{}, vuID)
		e.workers[vuID] = &workerHandle{cancel: wcancel}
		e.wg.Add(1)
		go e.runWorker(wctx, vuID, startStep, iteration)
	}

	// Scale down: cancel excess workers (highest ids first).
	ids := make([]int, 0, len(e.workers))
	for id := range e.workers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for _, id := range ids {
		if id < n {
			break
		}
		if w, ok := e.workers[id]; ok {
			w.cancel()
			delete(e.workers, id)
		}
	}
}

type vuContextKey struct{}

// VUFromContext returns the virtual user id attached to a worker context.
func VUFromContext(ctx context.Context) int {
	if id, ok := ctx.Value(vuContextKey{}).(int); ok {
		return id
	}
	return -1
}

func (e *Engine) runWorker(ctx context.Context, vuID int, startStep int, iteration int64) {
	defer e.wg.Done()
	seed := scenario.DeriveSeed(e.cfg.Seed, vuID, iteration)
	rng := rand.New(rand.NewSource(seed))
	stepIdx := startStep

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.draining:
			return
		default:
		}
		if stepIdx >= len(e.scen.Steps) {
			iteration++
			stepIdx = 0
			seed = scenario.DeriveSeed(e.cfg.Seed, vuID, iteration)
			rng = rand.New(rand.NewSource(seed))
			e.checkpointVU(vuID, iteration, stepIdx)
			continue
		}
		step := e.scen.Steps[stepIdx]

		if step.Wait.Value() > 0 {
			if err := sleepCtx(ctx, step.Wait.Value()); err != nil {
				return
			}
		}

		act, err := buildAction(step.Action)
		if err != nil {
			// Structural error: count as failure and stop this VU.
			e.stats.Record(0, OutcomeFailure, "action_build")
			return
		}

		actCtx := ctx
		var cancel context.CancelFunc
		if e.cfg.ActionTimeout > 0 {
			actCtx, cancel = context.WithTimeout(ctx, e.cfg.ActionTimeout)
		}
		start := time.Now()
		actErr := act.Execute(actCtx, rng)
		latency := time.Since(start)
		if cancel != nil {
			cancel()
		}

		// Deterministic perturbation injection. The rng is always advanced
		// so the sequence stays reproducible; perturbations only apply when
		// the action itself succeeded.
		injLat, injErr := e.injector.MaybeInject(rng)
		if actErr == nil {
			if injErr != nil {
				actErr = injErr
			} else if injLat > 0 {
				if err := sleepCtx(ctx, injLat); err != nil {
					return
				}
				latency += injLat
			}
		}

		outcome, errClass := classify(actErr, actCtx)
		e.stats.Record(latency, outcome, errClass)
		e.brk.record(outcome)

		if actErr != nil && step.OnError != nil {
			switch step.OnError.Action {
			case "stop":
				e.checkpointVU(vuID, iteration, stepIdx)
				return
			case "goto":
				stepIdx = e.scen.StepIndex(step.OnError.Goto)
				if stepIdx < 0 {
					stepIdx = 0
				}
				e.checkpointVU(vuID, iteration, stepIdx)
				continue
			}
		}

		stepIdx++
		e.checkpointVU(vuID, iteration, stepIdx)
	}
}

func (e *Engine) checkpointVU(vuID int, iteration int64, stepIdx int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.vuStates[vuID] = &VUState{Iteration: iteration, StepIndex: stepIdx}
}

func classify(err error, ctx context.Context) (Outcome, string) {
	if err == nil {
		return OutcomeSuccess, ""
	}
	if errors.Is(err, context.Canceled) {
		return OutcomeCanceled, "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return OutcomeTimeout, "timeout"
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		if httpErr.Status >= 500 {
			return OutcomeFailure, "http_5xx"
		}
		return OutcomeFailure, "http_4xx"
	}
	if errors.Is(err, FaultError{}) {
		return OutcomeFailure, "fault"
	}
	return OutcomeFailure, "error"
}

func (e *Engine) buildCheckpoint() *Checkpoint {
	e.mu.Lock()
	defer e.mu.Unlock()
	states := make(map[int]*VUState, len(e.vuStates))
	for id, st := range e.vuStates {
		cp := *st
		states[id] = &cp
	}
	return &Checkpoint{
		ScenarioName: e.scen.Name,
		Seed:         e.cfg.Seed,
		Elapsed:      e.elapsed,
		VUStates:     states,
		Stats:        e.stats.Snapshot(),
	}
}
