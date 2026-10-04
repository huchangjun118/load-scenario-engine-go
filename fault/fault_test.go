package fault

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/engine"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
	"github.com/huchangjun118/load-scenario-engine-go/stats"
)

var errInjected = errors.New("injected fault")

func runActions(h engine.Handler, n int) (errs int) {
	for i := 1; i <= n; i++ {
		a := scenario.Action{Seq: int64(i), Op: "read"}
		if err := h(context.Background(), a); err != nil {
			errs++
		}
	}
	return errs
}

func TestInjectorDeterministic(t *testing.T) {
	rule := Rule{Probability: 0.5, Err: errInjected}
	i1 := NewInjector(99, rule)
	i2 := NewInjector(99, rule)
	ok := func(context.Context, scenario.Action) error { return nil }
	e1 := runActions(i1.Wrap(ok), 200)
	e2 := runActions(i2.Wrap(ok), 200)
	if e1 != e2 {
		t.Fatalf("same seed, different injection counts: %d vs %d", e1, e2)
	}
	if e1 == 0 || e1 == 200 {
		t.Fatalf("injection count not stochastic: %d", e1)
	}
}

func TestInjectorOpFilter(t *testing.T) {
	rule := Rule{Op: "write", Probability: 1, Err: errInjected}
	inj := NewInjector(1, rule)
	ok := func(context.Context, scenario.Action) error { return nil }
	h := inj.Wrap(ok)
	if err := h(context.Background(), scenario.Action{Seq: 1, Op: "read"}); err != nil {
		t.Fatalf("non-matching op should not be injected: %v", err)
	}
	if err := h(context.Background(), scenario.Action{Seq: 1, Op: "write"}); !errors.Is(err, errInjected) {
		t.Fatalf("matching op should be injected: %v", err)
	}
}

func TestInjectorLatency(t *testing.T) {
	rule := Rule{Probability: 1, Latency: 30 * time.Millisecond}
	inj := NewInjector(1, rule)
	ok := func(context.Context, scenario.Action) error { return nil }
	start := time.Now()
	if err := inj.Wrap(ok)(context.Background(), scenario.Action{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 30*time.Millisecond {
		t.Fatalf("latency not injected: %v", d)
	}
}

func TestInjectorLatencyCancelled(t *testing.T) {
	rule := Rule{Probability: 1, Latency: time.Hour}
	inj := NewInjector(1, rule)
	ok := func(context.Context, scenario.Action) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := inj.Wrap(ok)(ctx, scenario.Action{Seq: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected ctx error, got %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("cancellation not prompt: %v", d)
	}
}

func TestBreakerErrorRate(t *testing.T) {
	b := NewBreaker(Thresholds{ErrorRate: 0.2, MinSamples: 10}, Policy{Kind: "stop"})
	// Below min samples: never trips.
	if b.Check(stats.Snapshot{Completed: 5, Failed: 5}) {
		t.Fatal("tripped below min samples")
	}
	// Below threshold.
	if b.Check(stats.Snapshot{Completed: 100, Failed: 10}) {
		t.Fatal("tripped below threshold")
	}
	// Above threshold.
	if !b.Check(stats.Snapshot{Completed: 100, Failed: 30}) {
		t.Fatal("did not trip above threshold")
	}
	if !b.Tripped() {
		t.Fatal("Tripped() not set")
	}
	// Stays tripped.
	if !b.Check(stats.Snapshot{Completed: 100, Failed: 0}) {
		t.Fatal("breaker should stay tripped")
	}
}

func TestBreakerLatency(t *testing.T) {
	b := NewBreaker(Thresholds{P99Latency: 50 * time.Millisecond, MinSamples: 1}, Policy{Kind: "stop"})
	c := stats.NewCollector()
	for i := 0; i < 100; i++ {
		c.Record(200*time.Millisecond, nil)
	}
	if !b.Check(c.Snapshot()) {
		t.Fatal("did not trip on p99 latency")
	}
}
