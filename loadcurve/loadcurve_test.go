package loadcurve

import (
	"math"
	"testing"
	"time"
)

func TestConstant(t *testing.T) {
	c := &Curve{Segments: []Segment{{Kind: Constant, Level: 10, Duration: time.Second}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, ms := range []int{0, 100, 500, 999} {
		if got := c.TargetAt(time.Duration(ms) * time.Millisecond); got != 10 {
			t.Fatalf("at %dms expected 10, got %v", ms, got)
		}
	}
	if c.Duration() != time.Second {
		t.Fatalf("duration mismatch")
	}
}

func TestRamp(t *testing.T) {
	c := &Curve{Segments: []Segment{{Kind: Ramp, From: 0, To: 100, Duration: time.Second}}}
	if got := c.TargetAt(0); got != 0 {
		t.Fatalf("ramp start: got %v", got)
	}
	if got := c.TargetAt(500 * time.Millisecond); math.Abs(got-50) > 1e-9 {
		t.Fatalf("ramp mid: got %v", got)
	}
	if got := c.TargetAt(time.Second); math.Abs(got-100) > 1e-9 {
		t.Fatalf("ramp end: got %v", got)
	}
}

func TestSpikeReturnsToBaseline(t *testing.T) {
	c := &Curve{Segments: []Segment{
		{Kind: Constant, Level: 10, Duration: time.Second},
		{Kind: Spike, Level: 100, Duration: 200 * time.Millisecond},
		{Kind: Constant, Level: 10, Duration: time.Second},
	}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	// Middle of spike.
	if got := c.TargetAt(1100 * time.Millisecond); got != 100 {
		t.Fatalf("spike level: got %v", got)
	}
	// Just before spike ends, target blends back toward baseline 10.
	if got := c.TargetAt(1199 * time.Millisecond); got > 100 || got < 9 {
		t.Fatalf("spike blend out of range: %v", got)
	}
	// After spike: baseline.
	if got := c.TargetAt(1300 * time.Millisecond); got != 10 {
		t.Fatalf("post-spike baseline: got %v", got)
	}
}

func TestIdleFallOff(t *testing.T) {
	c := &Curve{Segments: []Segment{
		{Kind: Constant, Level: 20, Duration: time.Second},
		{Kind: Idle, Level: 0, Duration: 500 * time.Millisecond},
		{Kind: Constant, Level: 20, Duration: time.Second},
	}}
	if got := c.TargetAt(1200 * time.Millisecond); got != 0 {
		t.Fatalf("idle level: got %v", got)
	}
	if got := c.TargetAt(1600 * time.Millisecond); got != 20 {
		t.Fatalf("post-idle baseline: got %v", got)
	}
}

// TestBoundaryContinuity ensures no instantaneous jump beyond configured
// levels at any segment switch.
func TestBoundaryContinuity(t *testing.T) {
	c := &Curve{Segments: []Segment{
		{Kind: Constant, Level: 10, Duration: time.Second},
		{Kind: Ramp, From: 10, To: 50, Duration: time.Second},
		{Kind: Spike, Level: 200, Duration: 300 * time.Millisecond},
		{Kind: Constant, Level: 50, Duration: time.Second},
		{Kind: Idle, Level: 0, Duration: 300 * time.Millisecond},
		{Kind: Constant, Level: 50, Duration: time.Second},
	}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	const step = time.Millisecond
	// Steepest configured transition: spike blends 50 -> 200 over 10% of
	// its 300ms segment, i.e. 150/30ms = 5 per ms. Anything beyond that
	// would be a discontinuity.
	const maxSlope = 5.5
	prev := c.TargetAt(0)
	for at := step; at <= c.Duration(); at += step {
		cur := c.TargetAt(at)
		if math.Abs(cur-prev) > maxSlope {
			t.Fatalf("discontinuity at %v: %v -> %v", at, prev, cur)
		}
		if cur < 0 || cur > 200 {
			t.Fatalf("target out of configured bounds at %v: %v", at, cur)
		}
		prev = cur
	}
}

func TestValidateErrors(t *testing.T) {
	if err := (&Curve{}).Validate(); err == nil {
		t.Fatal("empty curve should be invalid")
	}
	bad := &Curve{Segments: []Segment{{Kind: "weird", Duration: time.Second}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("unknown kind should be invalid")
	}
	neg := &Curve{Segments: []Segment{{Kind: Constant, Level: -1, Duration: time.Second}}}
	if err := neg.Validate(); err == nil {
		t.Fatal("negative level should be invalid")
	}
}
