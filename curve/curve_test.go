package curve

import (
	"testing"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/scenario"
)

func TestConstant(t *testing.T) {
	c := Constant{Level: 5}
	if c.At(0) != 5 || c.At(time.Hour) != 5 {
		t.Fatalf("constant level mismatch: %v", c.At(0))
	}
}

func TestStepRamp(t *testing.T) {
	c := StepRamp{Base: 1, Steps: []StepPoint{
		{At: 10 * time.Second, Level: 5},
		{At: 20 * time.Second, Level: 10},
	}}
	cases := map[time.Duration]float64{
		0: 1, 5 * time.Second: 1, 10 * time.Second: 5, 15 * time.Second: 5, 20 * time.Second: 10, 30 * time.Second: 10,
	}
	for at, want := range cases {
		if got := c.At(at); got != want {
			t.Errorf("At(%v) = %v, want %v", at, got, want)
		}
	}
}

func TestSpike(t *testing.T) {
	c := Spike{Base: Constant{Level: 2}, SpikeLevel: 20, Start: 10 * time.Second, End: 20 * time.Second}
	if got := c.At(5 * time.Second); got != 2 {
		t.Errorf("before spike: %v", got)
	}
	if got := c.At(15 * time.Second); got != 20 {
		t.Errorf("during spike: %v", got)
	}
	if got := c.At(25 * time.Second); got != 2 {
		t.Errorf("after spike: %v", got)
	}
}

func TestIdle(t *testing.T) {
	c := Idle{Base: Constant{Level: 5}, Start: 10 * time.Second, End: 20 * time.Second}
	if got := c.At(5 * time.Second); got != 5 {
		t.Errorf("before idle: %v", got)
	}
	if got := c.At(15 * time.Second); got != 0 {
		t.Errorf("during idle: %v", got)
	}
	if got := c.At(25 * time.Second); got != 5 {
		t.Errorf("after idle: %v", got)
	}
}

func TestCompositeSmoothTransition(t *testing.T) {
	// 0 -> 10 over 1s transition; sample every 10ms and ensure the level
	// never jumps by more than a small epsilon per sample.
	c := Composite{Segments: []Segment{
		{Curve: Constant{Level: 0}, Duration: 2 * time.Second, Transition: 1 * time.Second},
		{Curve: Constant{Level: 10}, Duration: 2 * time.Second, Transition: 1 * time.Second},
	}}
	dt := 10 * time.Millisecond
	prev := c.At(0)
	maxDelta := 0.0
	for i := 1; i <= 400; i++ {
		at := time.Duration(i) * dt
		got := c.At(at)
		delta := got - prev
		if delta < 0 {
			delta = -delta
		}
		if delta > maxDelta {
			maxDelta = delta
		}
		prev = got
	}
	// 1s transition from 0..10 sampled at 10ms => max per-sample delta ~0.22
	// near the steepest point of smoothstep; allow generous headroom.
	if maxDelta > 0.5 {
		t.Fatalf("curve jump too large: max delta %v", maxDelta)
	}
	// Transition must actually complete: level reaches 10 after transition.
	if got := c.At(3500 * time.Millisecond); got != 10 {
		t.Fatalf("transition did not complete: %v", got)
	}
}

func TestBuild(t *testing.T) {
	spec := scenario.CurveSpec{
		Type: "composite",
		Segments: []scenario.SegmentSpec{
			{Curve: scenario.CurveSpec{Type: "constant", Level: 5}, Duration: scenario.Duration(2 * time.Second), Transition: scenario.Duration(500 * time.Millisecond)},
			{Curve: scenario.CurveSpec{Type: "spike", Base: 5, Level: 50, Start: scenario.Duration(0), End: scenario.Duration(1 * time.Second)}, Duration: scenario.Duration(2 * time.Second)},
		},
	}
	c, err := Build(spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.At(0); got != 0 {
		t.Errorf("start level = %v, want 0 (fade-in)", got)
	}
	if got := c.At(2500 * time.Millisecond); got != 50 {
		t.Errorf("during spike = %v, want 50", got)
	}
	if got := c.At(3500 * time.Millisecond); got != 5 {
		t.Errorf("after spike = %v, want 5", got)
	}
}

func TestLimited(t *testing.T) {
	c := Limited{Curve: Constant{Level: 5}, Limit: 10 * time.Second}
	if c.Duration() != 10*time.Second {
		t.Fatal("duration mismatch")
	}
	if got := c.At(5 * time.Second); got != 5 {
		t.Errorf("before limit: %v", got)
	}
	if got := c.At(10 * time.Second); got != 0 {
		t.Errorf("at limit: %v", got)
	}
}
