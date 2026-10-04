package engine

import (
	"math/rand"
	"testing"
	"time"
)

func TestInjectorDeterministic(t *testing.T) {
	inj := &Injector{FaultProbability: 0.5, LatencyMin: 10 * time.Millisecond, LatencyMax: 100 * time.Millisecond}
	rng1 := rand.New(rand.NewSource(123))
	rng2 := rand.New(rand.NewSource(123))
	var lat1, lat2 time.Duration
	for i := 0; i < 20; i++ {
		l1, e1 := inj.MaybeInject(rng1)
		l2, e2 := inj.MaybeInject(rng2)
		lat1 += l1
		lat2 += l2
		if (e1 == nil) != (e2 == nil) {
			t.Fatalf("fault mismatch at %d", i)
		}
	}
	if lat1 != lat2 {
		t.Fatalf("latency mismatch: %v != %v", lat1, lat2)
	}
}

func TestInjectorFaultsClassified(t *testing.T) {
	inj := &Injector{FaultProbability: 1.0}
	rng := rand.New(rand.NewSource(1))
	_, err := inj.MaybeInject(rng)
	if err == nil {
		t.Fatal("expected injected fault")
	}
	outcome, class := classify(err, nil)
	if outcome != OutcomeFailure || class != "fault" {
		t.Fatalf("got %v/%s", outcome, class)
	}
}
