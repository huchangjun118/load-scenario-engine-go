// Package fault provides deterministic fault/latency injection and
// threshold-based circuit breaking.
package fault

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/engine"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
	"github.com/huchangjun118/load-scenario-engine-go/stats"
)

// Rule describes one injection: with Probability, matching actions either
// fail with Err or are delayed by Latency (or both).
type Rule struct {
	// Op, when non-empty, limits the rule to actions with this op.
	Op string `json:"op,omitempty"`
	// Probability in [0,1].
	Probability float64 `json:"probability"`
	// Latency is injected before delegating to the handler.
	Latency time.Duration `json:"latency,omitempty"`
	// Err, when non-nil, is returned instead of calling the handler.
	Err error `json:"-"`
}

// Injector wraps a handler with seeded fault and latency injection. The
// injection decision for an action depends only on the seed and the
// action's sequence number, so runs are reproducible.
type Injector struct {
	seed  int64
	rules []Rule
}

// NewInjector creates an injector with a deterministic seed.
func NewInjector(seed int64, rules ...Rule) *Injector {
	return &Injector{seed: seed, rules: rules}
}

// rngFor derives the deterministic RNG for one action.
func (i *Injector) rngFor(seq int64) *rand.Rand {
	s1 := uint64(i.seed)*0x9e3779b97f4a7c15 + uint64(seq)*0xbf58476d1ce4e5b9
	s2 := uint64(i.seed) ^ uint64(seq)*0x2545f4914f6cdd1d
	return rand.New(rand.NewPCG(s1, s2))
}

// Wrap returns a handler that injects faults before delegating.
func (i *Injector) Wrap(h engine.Handler) engine.Handler {
	return func(ctx context.Context, a scenario.Action) error {
		r := i.rngFor(a.Seq)
		for _, rule := range i.rules {
			if rule.Op != "" && rule.Op != a.Op {
				continue
			}
			if r.Float64() >= rule.Probability {
				continue
			}
			if rule.Latency > 0 {
				t := time.NewTimer(rule.Latency)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return ctx.Err()
				}
			}
			if rule.Err != nil {
				return rule.Err
			}
		}
		return h(ctx, a)
	}
}

// Policy describes what to do when the breaker trips.
type Policy struct {
	// Kind is "stop" (stop applying load, preserve state).
	Kind string `json:"kind"`
}

// Thresholds define breaker trip conditions.
type Thresholds struct {
	// ErrorRate trips when failed/completed exceeds this value.
	ErrorRate float64 `json:"error_rate,omitempty"`
	// P99Latency trips when the estimated p99 exceeds this value.
	P99Latency time.Duration `json:"p99_latency,omitempty"`
	// MinSamples is the minimum number of completed actions before the
	// breaker is allowed to trip.
	MinSamples int64 `json:"min_samples,omitempty"`
}

// Breaker watches stats snapshots and trips on threshold violations. Once
// tripped it stays tripped (the run stops and the scene is preserved).
type Breaker struct {
	t Thresholds
	p Policy

	mu      sync.Mutex
	tripped bool
}

// NewBreaker creates a breaker.
func NewBreaker(t Thresholds, p Policy) *Breaker {
	if t.MinSamples <= 0 {
		t.MinSamples = 1
	}
	return &Breaker{t: t, p: p}
}

// Check evaluates a snapshot; true means the breaker has tripped.
func (b *Breaker) Check(s stats.Snapshot) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tripped {
		return true
	}
	if s.Completed < b.t.MinSamples {
		return false
	}
	if b.t.ErrorRate > 0 && s.ErrorRate() > b.t.ErrorRate {
		b.tripped = true
	}
	if b.t.P99Latency > 0 && s.Quantile(0.99) > b.t.P99Latency {
		b.tripped = true
	}
	return b.tripped
}

// Tripped reports whether the breaker has tripped.
func (b *Breaker) Tripped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tripped
}

// Policy returns the configured trip policy.
func (b *Breaker) Policy() Policy { return b.p }
