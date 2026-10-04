// Package scenario defines the declarative load scenario format.
package scenario

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a time.Duration that accepts Go-style duration strings
// ("100ms", "2s") or numeric seconds when unmarshalling JSON.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = Duration(v)
		return nil
	}
	var n float64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*d = Duration(n * float64(time.Second))
	return nil
}

func (d Duration) Value() time.Duration { return time.Duration(d) }

// Scenario is a declarative description of a load test.
type Scenario struct {
	Name  string   `json:"name"`
	Seed  int64    `json:"seed"`
	Curve CurveSpec `json:"curve"`
	Steps []Step   `json:"steps"`
}

// Step is one action in a scenario.
type Step struct {
	ID      string     `json:"id"`
	Action  ActionSpec `json:"action"`
	Wait    Duration   `json:"wait,omitempty"`
	OnError *OnError   `json:"on_error,omitempty"`
}

// ActionSpec describes the action to execute.
type ActionSpec struct {
	Type     string   `json:"type"`
	Method   string   `json:"method,omitempty"`
	URL      string   `json:"url,omitempty"`
	Body     string   `json:"body,omitempty"`
	Duration Duration `json:"duration,omitempty"`
}

// OnError describes the failure branch taken when an action errors.
type OnError struct {
	Action string `json:"action"` // "continue" | "goto" | "stop"
	Goto   string `json:"goto,omitempty"`
}

// CurveSpec is the declarative load curve description.
type CurveSpec struct {
	Type     string        `json:"type"`
	Level    float64       `json:"level,omitempty"`
	Base     float64       `json:"base,omitempty"`
	Steps    []CurveStep   `json:"steps,omitempty"`
	Start    Duration      `json:"start,omitempty"`
	End      Duration      `json:"end,omitempty"`
	Segments []SegmentSpec `json:"segments,omitempty"`
	Curves   []CurveSpec   `json:"curves,omitempty"`
}

// CurveStep is a level change at a point in time.
type CurveStep struct {
	At    Duration `json:"at"`
	Level float64  `json:"level"`
}

// SegmentSpec is one segment of a composite curve.
type SegmentSpec struct {
	Curve      CurveSpec `json:"curve"`
	Duration   Duration  `json:"duration"`
	Transition Duration  `json:"transition,omitempty"`
}

// Parse decodes a scenario from JSON bytes.
func Parse(data []byte) (*Scenario, error) {
	var s Scenario
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Validate checks the scenario for structural correctness.
func (s *Scenario) Validate() error {
	if len(s.Steps) == 0 {
		return fmt.Errorf("scenario %q: at least one step is required", s.Name)
	}
	ids := make(map[string]int, len(s.Steps))
	for i, st := range s.Steps {
		if st.ID == "" {
			return fmt.Errorf("scenario %q: step %d has no id", s.Name, i)
		}
		if _, dup := ids[st.ID]; dup {
			return fmt.Errorf("scenario %q: duplicate step id %q", s.Name, st.ID)
		}
		ids[st.ID] = i
		if st.Action.Type == "" {
			return fmt.Errorf("scenario %q: step %q has no action type", s.Name, st.ID)
		}
		if st.OnError != nil {
			switch st.OnError.Action {
			case "continue", "stop":
			case "goto":
				if st.OnError.Goto == "" {
					return fmt.Errorf("scenario %q: step %q: on_error goto requires a target id", s.Name, st.ID)
				}
			default:
				return fmt.Errorf("scenario %q: step %q: unknown on_error action %q", s.Name, st.ID, st.OnError.Action)
			}
		}
	}
	for _, st := range s.Steps {
		if st.OnError != nil && st.OnError.Action == "goto" {
			if _, ok := ids[st.OnError.Goto]; !ok {
				return fmt.Errorf("scenario %q: step %q: on_error goto target %q does not exist", s.Name, st.ID, st.OnError.Goto)
			}
		}
	}
	return nil
}

// StepIndex returns the index of the step with the given id, or -1.
func (s *Scenario) StepIndex(id string) int {
	for i, st := range s.Steps {
		if st.ID == id {
			return i
		}
	}
	return -1
}

// DeriveSeed produces a deterministic seed for a given virtual user and
// iteration, so the same scenario with the same seed always produces the
// same action sequence.
func DeriveSeed(seed int64, vuID int, iteration int64) int64 {
	x := uint64(seed)
	x = splitMix64(x + uint64(vuID)*0x9e3779b97f4a7c15)
	x = splitMix64(x + uint64(iteration)*0xbf58476d1ce4e5b9)
	return int64(x)
}

func splitMix64(z uint64) uint64 {
	z += 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
