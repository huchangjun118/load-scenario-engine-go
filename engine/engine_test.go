package engine

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/curve"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
)

// gatedAction blocks on gate until closed or ctx cancelled, and tracks
// in-flight concurrency.
type gatedAction struct {
	mu       sync.Mutex
	inflight int
	max      int
	gate     chan struct{}
	failAll  bool
}

func (a *gatedAction) Execute(ctx context.Context, rng *rand.Rand) error {
	a.mu.Lock()
	a.inflight++
	if a.inflight > a.max {
		a.max = a.inflight
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.inflight--
		a.mu.Unlock()
	}()
	if a.gate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.gate:
		}
	}
	if a.failAll {
		return errTest
	}
	return nil
}

var errTest = &testError{}

type testError struct{}

func (e *testError) Error() string { return "test failure" }

func waitFor(pred func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return pred()
}

func testScenario() *scenario.Scenario {
	return &scenario.Scenario{
		Name: "test",
		Seed: 42,
		Steps: []scenario.Step{
			{ID: "a", Action: scenario.ActionSpec{Type: "gated"}},
		},
	}
}

func registerGated(t *testing.T, a *gatedAction) {
	t.Helper()
	RegisterAction("gated", func(scenario.ActionSpec) (Action, error) { return a, nil })
}

// 1. Backpressure: in-flight actions never exceed the cap even when the
//    target slows to a halt.
func TestBackpressure(t *testing.T) {
	gate := make(chan struct{})
	a := &gatedAction{gate: gate}
	registerGated(t, a)

	scen := testScenario()
	c := curve.Limited{Curve: curve.Constant{Level: 10}, Limit: 2 * time.Second}
	cfg := Config{MaxConcurrency: 5, TickInterval: 50 * time.Millisecond}
	eng := New(cfg, scen, c)

	done := make(chan *Report, 1)
	go func() {
		r, _ := eng.Run(context.Background())
		done <- r
	}()

	if !waitFor(func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.inflight == 5
	}, 2*time.Second) {
		t.Fatalf("inflight did not reach cap, got %d", a.max)
	}
	// Hold the gate; inflight must stay exactly at the cap.
	time.Sleep(200 * time.Millisecond)
	a.mu.Lock()
	max := a.max
	inflight := a.inflight
	a.mu.Unlock()
	if max > 5 {
		t.Fatalf("inflight exceeded cap: max %d", max)
	}
	if inflight != 5 {
		t.Fatalf("inflight = %d, want 5", inflight)
	}
	close(gate)

	select {
	case r := <-done:
		if r.Success == 0 {
			t.Fatal("expected successful actions")
		}
		if r.Canceled != 0 {
			t.Fatalf("unexpected canceled: %d", r.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not finish after gate release")
	}
}

// 2. Cancel: cancelling the context converges in-flight actions promptly and
//    cancelled actions are not counted as success.
func TestCancel(t *testing.T) {
	gate := make(chan struct{})
	a := &gatedAction{gate: gate}
	registerGated(t, a)

	scen := testScenario()
	c := curve.Limited{Curve: curve.Constant{Level: 10}, Limit: 10 * time.Second}
	cfg := Config{MaxConcurrency: 3, TickInterval: 50 * time.Millisecond}
	eng := New(cfg, scen, c)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *Report, 1)
	go func() {
		r, _ := eng.Run(ctx)
		done <- r
	}()

	if !waitFor(func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.inflight == 3
	}, 2*time.Second) {
		t.Fatalf("inflight did not reach cap, got %d", a.max)
	}
	cancel()

	select {
	case r := <-done:
		if r.Canceled == 0 {
			t.Fatalf("expected canceled actions, got %+v", r)
		}
		if r.Success != 0 {
			t.Fatalf("canceled actions counted as success: %d", r.Success)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not converge after cancel")
	}
}

// 3. Circuit breaker: sustained failures trip the breaker and stop the run.
func TestBreaker(t *testing.T) {
	a := &gatedAction{failAll: true}
	registerGated(t, a)

	scen := testScenario()
	c := curve.Limited{Curve: curve.Constant{Level: 5}, Limit: 10 * time.Second}
	cfg := Config{
		MaxConcurrency:   5,
		TickInterval:     50 * time.Millisecond,
		BreakerEnabled:   true,
		BreakerThreshold: 0.5,
		BreakerMinSamples: 10,
		BreakerWindow:    30 * time.Second,
	}
	eng := New(cfg, scen, c)

	r, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.StopReason != string(StopBreaker) {
		t.Fatalf("stop reason = %q, want breaker", r.StopReason)
	}
	if r.Failed == 0 {
		t.Fatal("expected failed actions")
	}
}

// 4. Checkpoint resume: stopping early and resuming continues statistics
//    without double-counting and resumes VU state.
func TestCheckpointResume(t *testing.T) {
	a := &gatedAction{}
	registerGated(t, a)

	scen := testScenario()
	curveFor := func() curve.Curve {
		return curve.Limited{Curve: curve.Constant{Level: 4}, Limit: 1 * time.Second}
	}
	ckpt := t.TempDir() + "/checkpoint.json"

	cfg := Config{MaxConcurrency: 4, TickInterval: 50 * time.Millisecond, CheckpointPath: ckpt}
	eng := New(cfg, scen, curveFor())
	done := make(chan *Report, 1)
	go func() {
		r, _ := eng.Run(context.Background())
		done <- r
	}()
	time.Sleep(350 * time.Millisecond)
	eng.Stop()
	r1 := <-done
	if r1.Success == 0 {
		t.Fatal("first run produced no successes")
	}

	// Resume from checkpoint.
	cfg2 := cfg
	cfg2.Resume = true
	eng2 := New(cfg2, scen, curveFor())
	r2, err := eng2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r2.StopReason != string(StopCompleted) {
		t.Fatalf("resume stop reason = %q", r2.StopReason)
	}
	// Resumed stats must include the first run's work plus new work.
	if r2.Success < r1.Success {
		t.Fatalf("resumed successes %d < first run %d", r2.Success, r1.Success)
	}
	if r2.Started < r1.Started {
		t.Fatalf("resumed started %d < first run %d", r2.Started, r1.Started)
	}
	if r2.Success == r1.Success {
		t.Fatal("resume did not perform new work")
	}

	// Checkpoint must record VU progress.
	cp, err := LoadCheckpoint(ckpt)
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.VUStates) == 0 {
		t.Fatal("checkpoint has no VU states")
	}
}

// 5. Determinism: same seed produces the same per-VU action sequence.
func TestDeterminism(t *testing.T) {
	type event struct {
		vu  int
		val int64
	}
	var mu sync.Mutex
	var log []event
	RegisterAction("rng", func(scenario.ActionSpec) (Action, error) {
		return ActionFunc(func(ctx context.Context, rng *rand.Rand) error {
			vu := VUFromContext(ctx)
			mu.Lock()
			log = append(log, event{vu, rng.Int63()})
			mu.Unlock()
			return nil
		}), nil
	})

	scen := &scenario.Scenario{
		Name: "det",
		Seed: 7,
		Steps: []scenario.Step{
			{ID: "a", Action: scenario.ActionSpec{Type: "rng"}},
			{ID: "b", Action: scenario.ActionSpec{Type: "rng"}},
		},
	}
	run := func() []event {
		mu.Lock()
		log = nil
		mu.Unlock()
		c := curve.Limited{Curve: curve.Constant{Level: 2}, Limit: 400 * time.Second}
		// 400ms wall duration, not 400s:
		c = curve.Limited{Curve: curve.Constant{Level: 2}, Limit: 400 * time.Millisecond}
		cfg := Config{MaxConcurrency: 2, TickInterval: 50 * time.Millisecond}
		eng := New(cfg, scen, c)
		if _, err := eng.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		out := append([]event(nil), log...)
		mu.Unlock()
		return out
	}

	first := run()
	second := run()
	if len(first) == 0 || len(second) == 0 {
		t.Fatal("no events recorded")
	}
	// Compare per-VU subsequences up to the common length.
	for _, vu := range []int{0, 1} {
		var a, b []event
		for _, e := range first {
			if e.vu == vu {
				a = append(a, e)
			}
		}
		for _, e := range second {
			if e.vu == vu {
				b = append(b, e)
			}
		}
		n := len(a)
		if len(b) < n {
			n = len(b)
		}
		if n < 10 {
			t.Fatalf("vu %d: too few events: %d", vu, n)
		}
		for i := 0; i < n; i++ {
			if a[i].val != b[i].val {
				t.Fatalf("vu %d event %d: %d != %d", vu, i, a[i].val, b[i].val)
			}
		}
	}
}

// 6. Dynamic concurrency adjustment takes effect without breaking the run.
func TestDynamicConcurrency(t *testing.T) {
	gate := make(chan struct{})
	a := &gatedAction{gate: gate}
	registerGated(t, a)

	scen := testScenario()
	c := curve.Limited{Curve: curve.Constant{Level: 10}, Limit: 2 * time.Second}
	cfg := Config{MaxConcurrency: 10, TickInterval: 50 * time.Millisecond}
	eng := New(cfg, scen, c)

	done := make(chan *Report, 1)
	go func() {
		r, _ := eng.Run(context.Background())
		done <- r
	}()

	if !waitFor(func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.inflight == 10
	}, 2*time.Second) {
		t.Fatalf("inflight did not reach 10, got %d", a.max)
	}

	// Lower the cap at runtime.
	eng.SetConcurrency(2)
	if !waitFor(func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.inflight <= 2 && a.inflight > 0
	}, 2*time.Second) {
		a.mu.Lock()
		got := a.inflight
		a.mu.Unlock()
		t.Fatalf("inflight did not drop to ~2, got %d", got)
	}
	a.mu.Lock()
	if a.max > 10 {
		t.Fatalf("exceeded original cap: %d", a.max)
	}
	a.mu.Unlock()

	// Remove the override; concurrency returns to the curve level.
	eng.SetConcurrency(0)
	if !waitFor(func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.inflight >= 9
	}, 2*time.Second) {
		a.mu.Lock()
		got := a.inflight
		a.mu.Unlock()
		t.Fatalf("inflight did not recover, got %d", got)
	}
	close(gate)
	<-done
}

// 7. Per-action timeouts converge and are classified as timeouts.
func TestActionTimeout(t *testing.T) {
	gate := make(chan struct{})
	a := &gatedAction{gate: gate}
	registerGated(t, a)

	scen := testScenario()
	c := curve.Limited{Curve: curve.Constant{Level: 3}, Limit: 2 * time.Second}
	cfg := Config{MaxConcurrency: 3, TickInterval: 50 * time.Millisecond, ActionTimeout: 100 * time.Millisecond}
	eng := New(cfg, scen, c)

	r, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.TimedOut == 0 {
		t.Fatalf("expected timeouts, got %+v", r)
	}
	if r.Success != 0 {
		t.Fatalf("timed-out actions counted as success: %d", r.Success)
	}
}

// 8. Histogram quantiles and merge.
func TestHistogram(t *testing.T) {
	h := NewHistogram()
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	if h.Count() != 1000 {
		t.Fatalf("count = %d", h.Count())
	}
	p50 := h.Quantile(0.5)
	p99 := h.Quantile(0.99)
	if p50 <= 0 || p99 <= p50 {
		t.Fatalf("bad quantiles: p50=%v p99=%v", p50, p99)
	}
	// p99 should be near 990ms.
	if p99 < 900*time.Millisecond || p99 > 1100*time.Millisecond {
		t.Fatalf("p99 out of range: %v", p99)
	}

	h2 := NewHistogram()
	h2.Record(2 * time.Second)
	h.Merge(h2)
	if h.Count() != 1001 {
		t.Fatalf("merged count = %d", h.Count())
	}
	if h.Quantile(0.999) < 1500*time.Millisecond {
		t.Fatalf("merged p999 too small: %v", h.Quantile(0.999))
	}
}
