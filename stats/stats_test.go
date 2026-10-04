package stats

import (
	"errors"
	"testing"
	"time"
)

func TestRecordAndSnapshot(t *testing.T) {
	c := NewCollector()
	c.RecordStarted()
	c.Record(10*time.Millisecond, nil)
	c.Record(20*time.Millisecond, nil)
	c.RecordStarted()
	c.Record(30*time.Millisecond, errors.New("boom"))
	c.RecordStarted()
	c.RecordCancelled()

	s := c.Snapshot()
	if s.Started != 3 || s.Completed != 3 || s.Succeeded != 2 || s.Failed != 1 {
		t.Fatalf("bad counters: %+v", s)
	}
	if s.Cancelled != 1 {
		t.Fatalf("cancelled not counted: %+v", s)
	}
	// Cancelled must not be counted as success.
	if s.Succeeded+s.Failed != s.Completed {
		t.Fatalf("completed mismatch: %+v", s)
	}
	if got := s.SuccessRate(); got != 2.0/3.0 {
		t.Fatalf("success rate: %v", got)
	}
	if s.Errors["boom"] != 1 {
		t.Fatalf("error classification: %+v", s.Errors)
	}
	if s.MeanLatency() != 20*time.Millisecond {
		t.Fatalf("mean latency: %v", s.MeanLatency())
	}
}

func TestQuantiles(t *testing.T) {
	c := NewCollector()
	for i := 1; i <= 100; i++ {
		c.Record(time.Duration(i)*time.Millisecond, nil)
	}
	s := c.Snapshot()
	p50 := s.Quantile(0.50)
	if p50 < 45*time.Millisecond || p50 > 60*time.Millisecond {
		t.Fatalf("p50 out of expected band: %v", p50)
	}
	p99 := s.Quantile(0.99)
	if p99 < 95*time.Millisecond {
		t.Fatalf("p99 too low: %v", p99)
	}
	if s.Quantile(1.0) != s.LatencyMax && s.Quantile(1.0) < 99*time.Millisecond {
		t.Fatalf("p100 inconsistent: %v max=%v", s.Quantile(1.0), s.LatencyMax)
	}
}

func TestMerge(t *testing.T) {
	a := NewCollector()
	b := NewCollector()
	a.Record(10*time.Millisecond, nil)
	a.Record(20*time.Millisecond, errors.New("x"))
	b.Record(30*time.Millisecond, nil)
	b.RecordCancelled()

	m := Merge(a.Snapshot(), b.Snapshot())
	if m.Completed != 3 || m.Succeeded != 2 || m.Failed != 1 || m.Cancelled != 1 {
		t.Fatalf("merged counters: %+v", m)
	}
	if m.Errors["x"] != 1 {
		t.Fatalf("merged errors: %+v", m.Errors)
	}
	if m.LatencySum != 60*time.Millisecond {
		t.Fatalf("merged latency sum: %v", m.LatencySum)
	}
	if m.LatencyMax != 30*time.Millisecond {
		t.Fatalf("merged max: %v", m.LatencyMax)
	}
	// Merge is associative/commutative on counters.
	m2 := Merge(b.Snapshot(), a.Snapshot())
	if m2.Completed != m.Completed || m2.Succeeded != m.Succeeded {
		t.Fatalf("merge not commutative")
	}
}

func TestCancelledNeverSuccess(t *testing.T) {
	c := NewCollector()
	for i := 0; i < 10; i++ {
		c.RecordCancelled()
	}
	s := c.Snapshot()
	if s.Succeeded != 0 || s.SuccessRate() != 0 {
		t.Fatalf("cancelled counted as success: %+v", s)
	}
	if s.Completed != 0 {
		t.Fatalf("cancelled counted as completed: %+v", s)
	}
}
