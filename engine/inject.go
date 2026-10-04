package engine

import (
	"math/rand"
	"time"
)

// Injector applies deterministic fault and latency perturbations to actions.
// All randomness is driven by a caller-provided rng so that runs with the
// same seed produce the same perturbation sequence.
type Injector struct {
	FaultProbability float64
	LatencyMin       time.Duration
	LatencyMax       time.Duration
}

// FaultError is the error returned when a fault is injected.
type FaultError struct{}

func (FaultError) Error() string { return "injected fault" }

// MaybeInject returns an injected latency and/or error for an action.
// The rng is advanced deterministically on every call.
func (i *Injector) MaybeInject(rng *rand.Rand) (latency time.Duration, err error) {
	if i == nil {
		return 0, nil
	}
	if i.LatencyMax > 0 && i.LatencyMax >= i.LatencyMin {
		span := i.LatencyMax - i.LatencyMin
		latency = i.LatencyMin + time.Duration(rng.Int63n(int64(span)+1))
	}
	if i.FaultProbability > 0 && rng.Float64() < i.FaultProbability {
		err = FaultError{}
	}
	return latency, err
}
