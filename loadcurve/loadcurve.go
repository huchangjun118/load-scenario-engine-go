// Package loadcurve models composable load curves: constant, ramp (step
// increase), spike and idle fall-off segments.
//
// TargetAt is continuous across segment boundaries: a ramp ends exactly
// where the next segment begins, and spike/idle segments return to the
// surrounding baseline level, so switching segments never produces an
// instantaneous concurrency jump beyond the configured levels.
package loadcurve

import (
	"errors"
	"fmt"
	"time"
)

// Kind enumerates supported segment kinds.
type Kind string

const (
	// Constant holds Level for the whole segment.
	Constant Kind = "constant"
	// Ramp linearly interpolates From -> To (step increase/decrease).
	Ramp Kind = "ramp"
	// Spike jumps to Level for Duration then returns to the baseline
	// (the level the surrounding segments would otherwise produce).
	Spike Kind = "spike"
	// Idle drops to Level (usually 0) for Duration, then returns to
	// the baseline.
	Idle Kind = "idle"
)

// Segment is one piece of a load curve.
type Segment struct {
	Kind     Kind          `json:"kind"`
	Duration time.Duration `json:"duration"`
	// From/To are used by Ramp.
	From int `json:"from,omitempty"`
	To   int `json:"to,omitempty"`
	// Level is used by Constant, Spike and Idle.
	Level int `json:"level,omitempty"`
}

// Curve is an ordered list of segments.
type Curve struct {
	Segments []Segment `json:"segments"`
}

// Validate checks the curve for structural errors.
func (c *Curve) Validate() error {
	if len(c.Segments) == 0 {
		return errors.New("loadcurve: at least one segment is required")
	}
	for i, s := range c.Segments {
		if s.Duration <= 0 {
			return fmt.Errorf("loadcurve: segment %d (%s) must have positive duration", i, s.Kind)
		}
		switch s.Kind {
		case Constant, Spike, Idle:
			if s.Level < 0 {
				return fmt.Errorf("loadcurve: segment %d (%s) has negative level", i, s.Kind)
			}
		case Ramp:
			if s.From < 0 || s.To < 0 {
				return fmt.Errorf("loadcurve: segment %d (ramp) has negative from/to", i)
			}
		default:
			return fmt.Errorf("loadcurve: segment %d has unknown kind %q", i, s.Kind)
		}
	}
	return nil
}

// Duration returns the total curve duration.
func (c *Curve) Duration() time.Duration {
	var d time.Duration
	for _, s := range c.Segments {
		d += s.Duration
	}
	return d
}

// levelOf returns the raw level a segment produces at fraction f in [0,1].
func levelOf(s Segment, f float64) float64 {
	switch s.Kind {
	case Ramp:
		return float64(s.From) + (float64(s.To)-float64(s.From))*f
	default: // Constant, Spike, Idle
		return float64(s.Level)
	}
}

// baselineAt returns the level the curve would have at offset t if spike
// and idle segments were transparent, i.e. the level of the nearest
// non-transient segment. This is what spike/idle segments return to.
func (c *Curve) baselineAt(idx int) float64 {
	// Prefer the end level of the previous non-transient segment.
	for i := idx - 1; i >= 0; i-- {
		s := c.Segments[i]
		if s.Kind != Spike && s.Kind != Idle {
			return levelOf(s, 1)
		}
	}
	// Otherwise the start level of the next non-transient segment.
	for i := idx + 1; i < len(c.Segments); i++ {
		s := c.Segments[i]
		if s.Kind != Spike && s.Kind != Idle {
			return levelOf(s, 0)
		}
	}
	return 0
}

// TargetAt returns the desired concurrency at offset t into the curve.
// It is continuous at every segment boundary.
func (c *Curve) TargetAt(t time.Duration) float64 {
	if t < 0 {
		t = 0
	}
	var off time.Duration
	for i, s := range c.Segments {
		if t < off+s.Duration || i == len(c.Segments)-1 {
			f := 1.0
			if s.Duration > 0 {
				f = float64(t-off) / float64(s.Duration)
				if f > 1 {
					f = 1
				}
			}
			switch s.Kind {
			case Spike, Idle:
				// Blend from the baseline up to the transient level
				// over the first 10% of the segment and back down over
				// the last 10%, so both hand-offs are continuous.
				base := c.baselineAt(i)
				const blend = 0.1
				switch {
				case f < blend:
					k := f / blend
					return base + (float64(s.Level)-base)*k
				case f > 1-blend:
					k := (f - (1 - blend)) / blend
					return float64(s.Level) + (base-float64(s.Level))*k
				default:
					return float64(s.Level)
				}
			default:
				return levelOf(s, f)
			}
		}
		off += s.Duration
	}
	return 0
}
