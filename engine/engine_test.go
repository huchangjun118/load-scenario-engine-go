package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/checkpoint"
	"github.com/huchangjun118/load-scenario-engine-go/loadcurve"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
	"github.com/huchangjun118/load-scenario-engine-go/stats"
)

func testScenario(n int) *scenario.Scenario {
	return &scenario.Scenario{
		Name:  "test",
		Seed:  1,
		Steps: []scenario.Step{{Name: "op", Op: "read", Weight: n}},
	}
}

// inflightTracker records the max observed concurrency of the handler.
type inflightTracker struct {
	cur atomic.Int64
	max atomic.Int64
}

func (t *inflightTracker) wrap(h Handler) Handler {
	return func(ctx context.Context, a scenario.Action) error {
		c := t.cur.Add(1)
		for {
			m := t.max.Load()
			if c <= m || t.max.CompareAndSwap(m, c) {
				break
			}
		}
		defer t.cur.Add(-1)
		return h(ctx, a)
	}
}

func fastConfig(s *scenario.Scenario, maxConc int) Config {
	return Config{
		Scenario:       s,
		MaxConcurrency: maxConc,
		QueueSize:      maxConc,
		Tick:           5 * time.Millisecond,
	}
}

func TestBackpressureBoundedInflight(t *testing.T) {
	tr := &inflightTracker{}
	slow := tr.wrap(func(ctx context.Context, a scenario.Action) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	e, err := New(fastConfig(testScenario(60), 4), slow)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := tr.max.Load(); got > 4 {
		t.Fatalf("inflight exceeded limit: %d > 4", got)
	}
	if snap.Completed != 60 || snap.Succeeded != 60 {
		t.Fatalf("unexpected stats: %+v", snap)
	}
	if snap.Cancelled != 0 {
		t.Fatalf("nothing should be cancelled: %+v", snap)
	}
}

func TestSlowTargetDoesNotPileUp(t *testing.T) {
	// Target is much slower than the generator could produce; the queue
	// must stay bounded and the run must still finish.
	var started atomic.Int64
	slow := func(ctx context.Context, a scenario.Action) error {
		started.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil
	}
	cfg := fastConfig(testScenario(30), 2)
	e, err := New(cfg, slow)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan stats.Snapshot, 1)
	go func() {
		s, _ := e.Run(context.Background())
		done <- s
	}()
	select {
	case snap := <-done:
		if snap.Completed != 30 {
			t.Fatalf("completed = %d, want 30", snap.Completed)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not converge under backpressure")
	}
}

func TestCancellationConverges(t *testing.T) {
	block := func(ctx context.Context, a scenario.Action) error {
		<-ctx.Done()
		return ctx.Err()
	}
	e, err := New(fastConfig(testScenario(1000), 8), block)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	snap, err := e.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("cancellation did not converge promptly: %v", d)
	}
	// Cancelled actions must never be counted as success.
	if snap.Succeeded != 0 {
		t.Fatalf("cancelled run reported successes: %+v", snap)
	}
	if snap.Cancelled == 0 {
		t.Fatalf("expected cancelled actions to be recorded: %+v", snap)
	}
	if snap.Completed+snap.Cancelled == 0 {
		t.Fatalf("no actions accounted: %+v", snap)
	}
}

func TestCurveSwitchNoOvershoot(t *testing.T) {
	tr := &inflightTracker{}
	h := tr.wrap(func(ctx context.Context, a scenario.Action) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	cfg := fastConfig(testScenario(100000), 16)
	cfg.Curve = &loadcurve.Curve{Segments: []loadcurve.Segment{
		{Kind: loadcurve.Constant, Level: 4, Duration: 300 * time.Millisecond},
		{Kind: loadcurve.Spike, Level: 16, Duration: 300 * time.Millisecond},
		{Kind: loadcurve.Constant, Level: 4, Duration: 300 * time.Millisecond},
		{Kind: loadcurve.Idle, Level: 0, Duration: 200 * time.Millisecond},
		{Kind: loadcurve.Constant, Level: 4, Duration: 200 * time.Millisecond},
	}}
	e, err := New(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Never exceed the configured spike level.
	if got := tr.max.Load(); got > 16 {
		t.Fatalf("concurrency overshot curve maximum: %d > 16", got)
	}
	// The spike must actually be reached (not smoothed away).
	if got := tr.max.Load(); got < 12 {
		t.Fatalf("spike level not reached: max inflight %d", got)
	}
}

func TestDynamicSetConcurrency(t *testing.T) {
	tr := &inflightTracker{}
	h := tr.wrap(func(ctx context.Context, a scenario.Action) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	cfg := fastConfig(testScenario(100000), 2)
	cfg.Curve = &loadcurve.Curve{Segments: []loadcurve.Segment{
		{Kind: loadcurve.Constant, Level: 100, Duration: 900 * time.Millisecond},
	}}
	e, err := New(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		e.Run(context.Background())
		close(done)
	}()
	// Phase 1: ceiling 2.
	time.Sleep(200 * time.Millisecond)
	if got := tr.max.Load(); got > 2 {
		t.Fatalf("phase 1 exceeded ceiling: %d", got)
	}
	// Raise the ceiling mid-run.
	tr.max.Store(0)
	e.SetConcurrency(8)
	time.Sleep(400 * time.Millisecond)
	if got := tr.max.Load(); got > 8 {
		t.Fatalf("phase 2 exceeded new ceiling: %d", got)
	}
	if got := tr.max.Load(); got < 4 {
		t.Fatalf("phase 2 did not scale up: %d", got)
	}
	// Lower it again; stats must stay consistent.
	e.SetConcurrency(1)
	<-done
	snap := e.Snapshot()
	if snap.Completed != snap.Succeeded+snap.Failed {
		t.Fatalf("stats inconsistent after adjustments: %+v", snap)
	}
	if snap.Succeeded+snap.Failed+snap.Cancelled != snap.Started {
		t.Fatalf("started != terminal states: %+v", snap)
	}
}

func TestStopDrainsInflight(t *testing.T) {
	var inFlight atomic.Int64
	h := func(ctx context.Context, a scenario.Action) error {
		inFlight.Add(1)
		defer inFlight.Add(-1)
		select {
		case <-time.After(50 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e, err := New(fastConfig(testScenario(100000), 4), h)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		e.Run(context.Background())
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	e.Stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not converge")
	}
	if got := inFlight.Load(); got != 0 {
		t.Fatalf("resources not released after stop: %d in flight", got)
	}
}

func TestBreakerTripStopsRun(t *testing.T) {
	fail := func(ctx context.Context, a scenario.Action) error {
		return errors.New("target down")
	}
	cfg := fastConfig(testScenario(100000), 4)
	cfg.Curve = &loadcurve.Curve{Segments: []loadcurve.Segment{
		{Kind: loadcurve.Constant, Level: 4, Duration: 10 * time.Second},
	}}
	var tripped atomic.Bool
	cfg.OnTrip = func(s stats.Snapshot) bool {
		if s.Completed >= 20 && s.ErrorRate() > 0.5 {
			tripped.Store(true)
			return true
		}
		return false
	}
	e, err := New(cfg, fail)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !tripped.Load() {
		t.Fatal("breaker never evaluated to trip")
	}
	// Run stopped early: far fewer than the full scenario.
	if snap.Completed >= 100000 {
		t.Fatalf("breaker did not stop the run: %+v", snap)
	}
	if snap.Failed == 0 || snap.ErrorRate() < 0.5 {
		t.Fatalf("expected error classification: %+v", snap)
	}
	if msg := snap.Errors["target down"]; msg == 0 {
		t.Fatalf("error class not recorded: %+v", snap.Errors)
	}
}

func TestCheckpointResumeNoDoubleCount(t *testing.T) {
	store := &checkpoint.MemStore{}
	mk := func(resume bool) *Engine {
		cfg := fastConfig(testScenario(200), 4)
		cfg.CheckpointStore = store
		cfg.CheckpointInterval = 10 * time.Millisecond
		cfg.Resume = resume
		e, err := New(cfg, func(ctx context.Context, a scenario.Action) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}

	// First run: interrupt partway.
	e1 := mk(false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan stats.Snapshot, 1)
	go func() {
		s, _ := e1.Run(ctx)
		done <- s
	}()
	time.Sleep(120 * time.Millisecond)
	cancel()
	snap1 := <-done
	if snap1.Completed == 0 || snap1.Completed >= 200 {
		t.Fatalf("first run should be partial, got %+v", snap1)
	}
	st, ok, err := store.Load()
	if err != nil || !ok {
		t.Fatalf("checkpoint not saved: ok=%v err=%v", ok, err)
	}
	if st.CompletedActions <= 0 || st.CompletedActions >= 200 {
		t.Fatalf("bad checkpoint prefix: %+v", st)
	}
	// Checkpoint stats must cover exactly the completed prefix.
	if st.Stats.Completed != st.CompletedActions {
		t.Fatalf("checkpoint stats/prefix mismatch: %+v", st)
	}

	// Second run: resume and finish.
	e2 := mk(true)
	snap2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Merged totals must equal exactly one full pass over the scenario:
	// no double counting of completed actions.
	if snap2.Completed != 200 {
		t.Fatalf("resumed run double-counted or lost actions: completed=%d (snap1=%d)", snap2.Completed, snap1.Completed)
	}
	if snap2.Succeeded != 200 {
		t.Fatalf("resumed run successes wrong: %+v", snap2)
	}
}

func TestDeterministicActionSequence(t *testing.T) {
	collect := func() []string {
		var mu sync.Mutex
		var keys []string
		e, err := New(fastConfig(testScenario(100), 8), func(ctx context.Context, a scenario.Action) error {
			mu.Lock()
			keys = append(keys, a.Key)
			mu.Unlock()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		return keys
	}
	k1, k2 := collect(), collect()
	if len(k1) != len(k2) {
		t.Fatalf("action count differs: %d vs %d", len(k1), len(k2))
	}
	set := map[string]int{}
	for _, k := range k1 {
		set[k]++
	}
	for _, k := range k2 {
		set[k]--
	}
	for k, v := range set {
		if v != 0 {
			t.Fatalf("key %q count mismatch between identical runs", k)
		}
	}
}
