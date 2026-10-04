// Package scenario defines declarative load scenarios and a deterministic
// iterator that turns a scenario plus a seed into a reproducible action
// sequence.
//
// Determinism: for a given Scenario (same steps, same seed) the iterator
// always produces the same sequence of actions, waits and keys, regardless
// of wall-clock time or goroutine scheduling.
package scenario

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"time"
)

// Failure policy kinds.
const (
	FailureAbort    = "abort"
	FailureContinue = "continue"
	FailureGoto     = "goto"
)

// FailurePolicy describes what to do when a step's action fails.
type FailurePolicy struct {
	// Kind is "abort", "continue" or "goto".
	Kind string `json:"kind"`
	// Step is the target step index when Kind == "goto".
	Step int `json:"step,omitempty"`
}

// Step is a single declarative step in a scenario.
type Step struct {
	Name       string         `json:"name"`
	Op         string         `json:"op"`
	Weight     int            `json:"weight,omitempty"`
	WaitBefore time.Duration  `json:"wait_before,omitempty"`
	WaitAfter  time.Duration  `json:"wait_after,omitempty"`
	OnFailure  *FailurePolicy `json:"on_failure,omitempty"`
}

// Scenario is a declarative description of a load run.
type Scenario struct {
	Name  string `json:"name"`
	Seed  int64  `json:"seed"`
	Steps []Step `json:"steps"`
}

// Validate checks the scenario for structural errors.
func (s *Scenario) Validate() error {
	if len(s.Steps) == 0 {
		return errors.New("scenario: at least one step is required")
	}
	for i, st := range s.Steps {
		if st.Op == "" {
			return fmt.Errorf("scenario: step %d (%q) missing op", i, st.Name)
		}
		if st.Weight < 0 {
			return fmt.Errorf("scenario: step %d (%q) has negative weight", i, st.Name)
		}
		if st.WaitBefore < 0 || st.WaitAfter < 0 {
			return fmt.Errorf("scenario: step %d (%q) has negative wait", i, st.Name)
		}
		if p := st.OnFailure; p != nil {
			switch p.Kind {
			case FailureAbort, FailureContinue:
			case FailureGoto:
				if p.Step < 0 || p.Step >= len(s.Steps) {
					return fmt.Errorf("scenario: step %d (%q) on_failure goto out of range: %d", i, st.Name, p.Step)
				}
			default:
				return fmt.Errorf("scenario: step %d (%q) unknown on_failure kind %q", i, st.Name, p.Kind)
			}
		}
	}
	return nil
}

// Action is a single unit of work produced by an Iterator.
type Action struct {
	Seq       int64
	StepIndex int
	StepName  string
	Op        string
	Key       string
}

// Iterator deterministically yields actions for a scenario. The step
// selection and per-action keys derive only from the scenario seed and the
// action sequence number, so the sequence is fully reproducible.
type Iterator struct {
	scenario *Scenario
	seq      int64
	// stepIdx is the current step; it advances in declaration order and
	// may jump on failure branches via Advance.
	stepIdx int
	// stepCounts tracks how many actions each step has emitted, used to
	// make keys unique per step.
	stepCounts []int64
}

// NewIterator builds an iterator bound to a scenario.
func NewIterator(s *Scenario) *Iterator {
	return &Iterator{scenario: s, stepCounts: make([]int64, len(s.Steps))}
}

// rngFor derives a deterministic RNG for (seed, seq) pairs.
func rngFor(seed, seq int64) *rand.Rand {
	var pcgSeed [4]uint64
	h := fnv.New64a()
	var b [8]byte
	put := func(v int64) {
		for i := 0; i < 8; i++ {
			b[i] = byte(v >> (8 * i))
		}
		h.Write(b[:])
	}
	put(seed)
	put(seq)
	sum := h.Sum64()
	pcgSeed[0] = sum
	pcgSeed[1] = sum ^ 0x9e3779b97f4a7c15
	return rand.New(rand.NewPCG(pcgSeed[0], pcgSeed[1]))
}

// Next returns the next action, or false when the scenario is exhausted.
// Steps are visited in declaration order; each step emits Weight actions
// (default 1) before advancing. Failure branches are applied by Resolve.
func (it *Iterator) Next() (Action, bool) {
	s := it.scenario
	for it.stepIdx < len(s.Steps) {
		st := s.Steps[it.stepIdx]
		w := st.Weight
		if w <= 0 {
			w = 1
		}
		if it.stepCounts[it.stepIdx] < int64(w) {
			n := it.stepCounts[it.stepIdx]
			it.stepCounts[it.stepIdx]++
			it.seq++
			r := rngFor(s.Seed, it.seq)
			return Action{
				Seq:       it.seq,
				StepIndex: it.stepIdx,
				StepName:  st.Name,
				Op:        st.Op,
				Key:       fmt.Sprintf("%s-%d-%d-%016x", st.Op, it.stepIdx, n, r.Uint64()),
			}, true
		}
		it.stepIdx++
	}
	return Action{}, false
}

// Resolve applies the failure policy of the step that produced a. It
// returns the policy kind and, for "goto", repositions the iterator.
func (it *Iterator) Resolve(a Action) string {
	st := it.scenario.Steps[a.StepIndex]
	p := st.OnFailure
	if p == nil {
		return FailureContinue
	}
	if p.Kind == FailureGoto {
		it.stepIdx = p.Step
	}
	return p.Kind
}

// WaitBefore returns the deterministic wait before the given action.
func (it *Iterator) WaitBefore(a Action) time.Duration {
	return it.scenario.Steps[a.StepIndex].WaitBefore
}

// WaitAfter returns the deterministic wait after the given action.
func (it *Iterator) WaitAfter(a Action) time.Duration {
	return it.scenario.Steps[a.StepIndex].WaitAfter
}
