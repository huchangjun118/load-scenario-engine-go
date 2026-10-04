// Package engine executes scenarios against a target with bounded
// concurrency, backpressure, cancellation and dynamic adjustment.
//
// Architecture: a single generator goroutine turns the scenario iterator
// into actions and pushes them into a bounded jobs channel (backpressure:
// when workers are saturated the generator blocks, nothing piles up). A
// control loop adjusts the worker pool toward the load-curve target once
// per tick with a bounded step, so curve transitions never cause
// instantaneous over/under-shoot of concurrency.
//
// Consistency: results are recorded into stats by the generator in
// sequence order, and every checkpoint pairs the contiguous completed
// prefix with the stats snapshot taken at that exact point, so resuming
// never double-counts completed actions.
package engine

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/checkpoint"
	"github.com/huchangjun118/load-scenario-engine-go/loadcurve"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
	"github.com/huchangjun118/load-scenario-engine-go/stats"
)

// Handler executes one action against the system under test.
type Handler func(ctx context.Context, a scenario.Action) error

// Config controls an Engine run.
type Config struct {
	Scenario *scenario.Scenario
	// Curve may be nil: then the concurrency ceiling is used constantly.
	Curve          *loadcurve.Curve
	MaxConcurrency int
	// QueueSize bounds the number of generated-but-not-started actions.
	QueueSize int
	// Tick is the control-loop interval (default 50ms).
	Tick time.Duration
	// CheckpointStore, when set, receives periodic and final checkpoints.
	CheckpointStore checkpoint.Store
	// CheckpointInterval is how often checkpoints are saved.
	CheckpointInterval time.Duration
	// Resume continues from the last checkpoint in CheckpointStore.
	Resume bool
	// OnTrip, when set, is evaluated once per tick; returning true stops
	// the run (used by circuit breakers).
	OnTrip func(s stats.Snapshot) bool
}

// progress pairs the contiguous completed prefix with the stats snapshot
// taken at that point; updated atomically by the generator.
type progress struct {
	prefix int64
	snap   stats.Snapshot
}

// Engine drives a scenario according to a load curve.
type Engine struct {
	cfg       Config
	handler   Handler
	collector *stats.Collector

	limit   atomic.Int64 // dynamic concurrency ceiling
	desired atomic.Int64 // desired worker count
	cur     atomic.Int64 // live worker count

	cancel   atomic.Pointer[context.CancelFunc]
	stopOnce sync.Once

	prog atomic.Pointer[progress]

	base          stats.Snapshot // stats restored from checkpoint
	elapsedOffset time.Duration  // curve offset restored from checkpoint
}

// result carries a finished action back to the generator.
type result struct {
	seq     int64
	action  scenario.Action
	latency time.Duration
	err     error
}

// New builds an Engine.
func New(cfg Config, h Handler) (*Engine, error) {
	if cfg.Scenario == nil {
		return nil, errors.New("engine: scenario is required")
	}
	if h == nil {
		return nil, errors.New("engine: handler is required")
	}
	if err := cfg.Scenario.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = cfg.MaxConcurrency
	}
	if cfg.Tick <= 0 {
		cfg.Tick = 50 * time.Millisecond
	}
	if cfg.Curve != nil {
		if err := cfg.Curve.Validate(); err != nil {
			return nil, err
		}
	}
	e := &Engine{cfg: cfg, handler: h, collector: stats.NewCollector()}
	e.limit.Store(int64(cfg.MaxConcurrency))
	return e, nil
}

// SetConcurrency dynamically adjusts the concurrency ceiling. Statistics
// already collected are unaffected; snapshots remain mergeable across the
// adjustment.
func (e *Engine) SetConcurrency(n int) {
	if n < 0 {
		n = 0
	}
	e.limit.Store(int64(n))
}

// Stop requests an early stop; in-flight actions are drained.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		if c := e.cancel.Load(); c != nil {
			(*c)()
		}
	})
}

// Snapshot returns the current merged statistics (checkpoint base + live).
func (e *Engine) Snapshot() stats.Snapshot {
	return stats.Merge(e.base, e.collector.Snapshot())
}

// targetAt returns the effective concurrency target at curve offset t.
func (e *Engine) targetAt(t time.Duration) float64 {
	target := float64(e.limit.Load())
	if e.cfg.Curve != nil {
		target = math.Min(target, e.cfg.Curve.TargetAt(t))
	}
	if target < 0 {
		target = 0
	}
	return target
}

// approach moves current toward target by at most 25% of target (min 1)
// per tick, so transitions are smooth in both directions.
func approach(current, target float64) float64 {
	step := math.Max(1, math.Abs(target)*0.25)
	switch {
	case target > current+step:
		return current + step
	case target < current-step:
		return current - step
	default:
		return target
	}
}

// Run executes until the scenario is exhausted, the curve ends, ctx is
// cancelled, Stop is called or the OnTrip hook trips. It returns the
// merged, reproducible statistics snapshot.
func (e *Engine) Run(ctx context.Context) (stats.Snapshot, error) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel.Store(&cancel)
	defer cancel()

	// Resume from checkpoint if requested.
	var skip int64
	if e.cfg.Resume && e.cfg.CheckpointStore != nil {
		st, ok, err := e.cfg.CheckpointStore.Load()
		if err != nil {
			return stats.Snapshot{}, err
		}
		if ok {
			e.base = st.Stats
			skip = st.CompletedActions
			e.elapsedOffset = st.Elapsed
		}
	}

	it := scenario.NewIterator(e.cfg.Scenario)
	for i := int64(0); i < skip; i++ {
		if _, ok := it.Next(); !ok {
			break
		}
	}

	jobs := make(chan scenario.Action, e.cfg.QueueSize)
	results := make(chan result, 2*e.cfg.QueueSize)
	genDone := make(chan struct{})

	var genWg sync.WaitGroup
	genWg.Add(1)
	go func() {
		defer genWg.Done()
		defer close(genDone)
		e.generate(ctx, it, skip, jobs, results)
	}()

	var workWg sync.WaitGroup
	spawn := func() {
		e.cur.Add(1)
		workWg.Add(1)
		go func() {
			defer workWg.Done()
			e.worker(ctx, jobs, results, it)
		}()
	}

	start := time.Now()
	current := e.targetAt(e.elapsedOffset)
	ticker := time.NewTicker(e.cfg.Tick)
	defer ticker.Stop()
	var lastCheckpoint time.Time

loop:
	for {
		select {
		case <-ticker.C:
			elapsed := e.elapsedOffset + time.Since(start)
			if e.cfg.Curve != nil && elapsed >= e.cfg.Curve.Duration() {
				cancel() // curve finished: converge
				continue
			}
			current = approach(current, e.targetAt(elapsed))
			d := int64(int(math.Round(current)))
			e.desired.Store(d)
			for e.cur.Load() < d {
				spawn()
			}
			if e.cfg.OnTrip != nil && e.cfg.OnTrip(e.Snapshot()) {
				cancel() // breaker tripped: stop applying load
				continue
			}
			if e.cfg.CheckpointStore != nil && e.cfg.CheckpointInterval > 0 &&
				time.Since(lastCheckpoint) >= e.cfg.CheckpointInterval {
				e.saveCheckpoint(elapsed)
				lastCheckpoint = time.Now()
			}
		case <-genDone:
			cancel() // scenario exhausted
		case <-ctx.Done():
			break loop
		}
	}

	// Converge: generator first (it may be blocked on the jobs channel),
	// then workers. Both observe ctx and exit promptly. The generator
	// closes the jobs channel when it returns.
	genWg.Wait()
	workWg.Wait()

	// Actions that started but whose results the generator no longer
	// consumed are counted as cancelled, never as success. Queued actions
	// that never started are simply discarded: they were never recorded
	// as started, so they keep no entry in the stats at all.
	for len(results) > 0 {
		<-results
		e.collector.RecordCancelled()
	}

	// Preserve the scene for a later resume.
	if e.cfg.CheckpointStore != nil {
		e.saveCheckpoint(e.elapsedOffset + time.Since(start))
	}

	return e.Snapshot(), nil
}

// saveCheckpoint persists the resumable state. The completed prefix and
// the stats snapshot come from the same atomic progress pointer, so a
// resumed run never recounts completed actions.
func (e *Engine) saveCheckpoint(elapsed time.Duration) {
	st := checkpoint.State{Elapsed: elapsed}
	if p := e.prog.Load(); p != nil {
		st.CompletedActions = p.prefix
		st.Stats = stats.Merge(e.base, p.snap)
	} else {
		st.Stats = e.Snapshot()
	}
	_ = e.cfg.CheckpointStore.Save(st)
}

// generate turns the iterator into actions, records results in sequence
// order and applies failure branches, keeping the action stream
// deterministic.
func (e *Engine) generate(ctx context.Context, it *scenario.Iterator, skip int64,
	jobs chan<- scenario.Action, results <-chan result) {
	defer close(jobs)
	pending := map[int64]result{}
	next := skip + 1
	var generated int64 = skip

	// apply records all contiguous finished results in seq order.
	apply := func() {
		changed := false
		for {
			r, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			e.collector.Record(r.latency, r.err)
			if r.err != nil {
				it.Resolve(r.action)
			}
			next++
			changed = true
		}
		if changed {
			e.prog.Store(&progress{prefix: next - 1, snap: e.collector.Snapshot()})
		}
	}

	for {
		apply()
		a, ok := it.Next()
		if !ok {
			break
		}
		// Wait-before: keep draining results while waiting; the action
		// itself must not be dropped.
		if wb := it.WaitBefore(a); wb > 0 {
			t := time.NewTimer(wb)
			waiting := true
			for waiting {
				select {
				case <-t.C:
					waiting = false
				case r := <-results:
					pending[r.seq] = r
				case <-ctx.Done():
					t.Stop()
					return
				}
			}
		}
		// Send: keep draining results until the action is handed off.
		sent := false
		for !sent {
			select {
			case jobs <- a:
				generated = a.Seq
				sent = true
			case r := <-results:
				pending[r.seq] = r
			case <-ctx.Done():
				return
			}
		}
	}

	// Scenario exhausted: keep consuming results until every generated
	// action has been accounted for, so no completion is lost.
	for next <= generated {
		select {
		case r := <-results:
			pending[r.seq] = r
			apply()
		case <-ctx.Done():
			return
		}
	}
}

// shouldExit reports whether this worker is above the desired count and
// won the CAS to retire.
func (e *Engine) shouldExit() bool {
	for {
		c := e.cur.Load()
		if c <= e.desired.Load() {
			return false
		}
		if e.cur.CompareAndSwap(c, c-1) {
			return true
		}
	}
}

// worker pulls actions and executes them until the jobs channel closes,
// the context is cancelled, or the pool is being shrunk.
func (e *Engine) worker(ctx context.Context, jobs <-chan scenario.Action,
	results chan<- result, it *scenario.Iterator) {
	for {
		if e.shouldExit() {
			return
		}
		select {
		case <-ctx.Done():
			e.cur.Add(-1)
			return
		case a, ok := <-jobs:
			if !ok {
				e.cur.Add(-1)
				return
			}
			e.collector.RecordStarted()
			start := time.Now()
			err := e.handler(ctx, a)
			lat := time.Since(start)
			if err != nil && ctx.Err() != nil {
				// Cancelled in flight: not a success, not a failure.
				e.collector.RecordCancelled()
				e.cur.Add(-1)
				return
			}
			if wa := it.WaitAfter(a); wa > 0 {
				t := time.NewTimer(wa)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					e.collector.RecordCancelled()
					e.cur.Add(-1)
					return
				}
			}
			select {
			case results <- result{seq: a.Seq, action: a, latency: lat, err: err}:
			case <-ctx.Done():
				e.collector.RecordCancelled()
				e.cur.Add(-1)
				return
			}
		}
	}
}
