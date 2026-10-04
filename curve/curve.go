// Package curve provides load curves that map elapsed time to a target
// concurrency level. Curves can be combined into composite segments with
// smooth transitions so that curve switches never produce instantaneous
// concurrency jumps.
package curve

import (
	"fmt"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/scenario"
)

// Curve maps elapsed time to a target concurrency level.
type Curve interface {
	At(t time.Duration) float64
}

// DurationProvider is an optional interface for curves with a defined length.
type DurationProvider interface {
	Duration() time.Duration
}

// Constant holds a fixed concurrency level.
type Constant struct{ Level float64 }

func (c Constant) At(time.Duration) float64 { return c.Level }

// StepRamp changes level at discrete points in time.
type StepRamp struct {
	Base  float64
	Steps []StepPoint
}

// StepPoint is a level change at time At.
type StepPoint struct {
	At    time.Duration
	Level float64
}

func (s StepRamp) At(t time.Duration) float64 {
	level := s.Base
	for _, p := range s.Steps {
		if p.At <= t {
			level = p.Level
		}
	}
	return level
}

// Spike adds an increased level over a window on top of a base curve.
type Spike struct {
	Base       Curve
	SpikeLevel float64
	Start      time.Duration
	End        time.Duration
}

func (s Spike) At(t time.Duration) float64 {
	v := 0.0
	if s.Base != nil {
		v = s.Base.At(t)
	}
	if t >= s.Start && t < s.End {
		v = s.SpikeLevel
	}
	return v
}

// Idle forces the level to zero over a window on top of a base curve.
type Idle struct {
	Base  Curve
	Start time.Duration
	End   time.Duration
}

func (s Idle) At(t time.Duration) float64 {
	if t >= s.Start && t < s.End {
		return 0
	}
	if s.Base != nil {
		return s.Base.At(t)
	}
	return 0
}

// Sum adds the levels of several curves.
type Sum struct{ Curves []Curve }

func (s Sum) At(t time.Duration) float64 {
	v := 0.0
	for _, c := range s.Curves {
		v += c.At(t)
	}
	return v
}

// Segment is one piece of a Composite curve.
type Segment struct {
	Curve      Curve
	Duration   time.Duration
	Transition time.Duration
}

// Composite plays segments in order, crossfading between them during
// transition windows so that the target level never jumps instantaneously.
type Composite struct{ Segments []Segment }

// Limited wraps a curve so it returns zero after Duration, and reports the
// duration to the engine.
type Limited struct {
	Curve Curve
	Limit time.Duration
}

func (l Limited) At(t time.Duration) float64 {
	if t >= l.Limit {
		return 0
	}
	return l.Curve.At(t)
}

func (l Limited) Duration() time.Duration { return l.Limit }

// Duration returns the total length of all segments.
func (c Composite) Duration() time.Duration {
	var d time.Duration
	for _, s := range c.Segments {
		d += s.Duration
	}
	return d
}

// At returns the target concurrency at elapsed time t.
func (c Composite) At(t time.Duration) float64 {
	if len(c.Segments) == 0 {
		return 0
	}
	cum := time.Duration(0)
	for i, seg := range c.Segments {
		if t < cum+seg.Duration {
			local := t - cum
			if seg.Transition > 0 && local < seg.Transition {
				frac := smoothstep(float64(local) / float64(seg.Transition))
				var prev float64
				if i > 0 {
					prevSeg := c.Segments[i-1]
					prev = prevSeg.Curve.At(prevSeg.Duration)
				}
				cur := seg.Curve.At(local)
				return prev*(1-frac) + cur*frac
			}
			return seg.Curve.At(local)
		}
		cum += seg.Duration
	}
	// Past the end: hold the last segment's final level.
	last := c.Segments[len(c.Segments)-1]
	return last.Curve.At(last.Duration)
}

// smoothstep eases from 0 to 1 with zero derivative at both ends, avoiding
// abrupt level changes at curve switches.
func smoothstep(x float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	return x * x * (3 - 2*x)
}

// Build constructs a Curve from a declarative spec.
func Build(spec scenario.CurveSpec) (Curve, error) {
	switch spec.Type {
	case "constant":
		return Constant{Level: spec.Level}, nil
	case "step":
		steps := make([]StepPoint, len(spec.Steps))
		for i, st := range spec.Steps {
			steps[i] = StepPoint{At: st.At.Value(), Level: st.Level}
		}
		return StepRamp{Base: spec.Base, Steps: steps}, nil
	case "spike":
		base, err := buildBase(spec)
		if err != nil {
			return nil, err
		}
		return Spike{Base: base, SpikeLevel: spec.Level, Start: spec.Start.Value(), End: spec.End.Value()}, nil
	case "idle":
		base, err := buildBase(spec)
		if err != nil {
			return nil, err
		}
		return Idle{Base: base, Start: spec.Start.Value(), End: spec.End.Value()}, nil
	case "sum":
		curves := make([]Curve, 0, len(spec.Curves))
		for _, cs := range spec.Curves {
			c, err := Build(cs)
			if err != nil {
				return nil, err
			}
			curves = append(curves, c)
		}
		return Sum{Curves: curves}, nil
	case "composite":
		segs := make([]Segment, 0, len(spec.Segments))
		for _, ss := range spec.Segments {
			c, err := Build(ss.Curve)
			if err != nil {
				return nil, err
			}
			segs = append(segs, Segment{
				Curve:      c,
				Duration:   ss.Duration.Value(),
				Transition: ss.Transition.Value(),
			})
		}
		return Composite{Segments: segs}, nil
	case "":
		return nil, fmt.Errorf("curve type is required")
	default:
		return nil, fmt.Errorf("unknown curve type %q", spec.Type)
	}
}

func buildBase(spec scenario.CurveSpec) (Curve, error) {
	if spec.Base != 0 {
		return Constant{Level: spec.Base}, nil
	}
	if len(spec.Curves) > 0 {
		return Build(scenario.CurveSpec{Type: "sum", Curves: spec.Curves})
	}
	return nil, nil
}
