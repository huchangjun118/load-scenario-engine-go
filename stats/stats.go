// Package stats collects reproducible latency distributions, throughput and
// error classification with mergeable snapshots.
//
// Latencies are recorded into a fixed-bucket exponential histogram, so
// snapshots are deterministic, cheap to merge and safe to checkpoint.
package stats

import (
	"sort"
	"sync"
	"time"
)

// bucketBounds defines histogram bucket upper bounds (exponential growth
// from 100µs to ~1h). Buckets are fixed so Merge is a pure element-wise sum.
var bucketBounds []time.Duration

func init() {
	b := time.Duration(100) * time.Microsecond
	for b < time.Hour {
		bucketBounds = append(bucketBounds, b)
		b += b / 4 // grow by 25%
	}
}

func bucketIndex(d time.Duration) int {
	i := sort.Search(len(bucketBounds), func(i int) bool { return d <= bucketBounds[i] })
	return i // may be len(bucketBounds) -> overflow bucket
}

// Snapshot is an immutable, mergeable view of collected statistics.
type Snapshot struct {
	Started   int64            `json:"started"`
	Completed int64            `json:"completed"`
	Succeeded int64            `json:"succeeded"`
	Failed    int64            `json:"failed"`
	Cancelled int64            `json:"cancelled"`
	Errors    map[string]int64 `json:"errors,omitempty"`
	// Buckets holds per-bucket counts; index len(bucketBounds) is the
	// overflow bucket.
	Buckets []int64 `json:"buckets,omitempty"`
	// LatencySum is the total latency of completed actions (for means).
	LatencySum time.Duration `json:"latency_sum"`
	// LatencyMax is the largest observed latency.
	LatencyMax time.Duration `json:"latency_max"`
	Elapsed    time.Duration `json:"elapsed"`
}

// Quantile returns the estimated latency quantile q in [0,1] over
// completed actions, using the histogram bucket upper bounds.
func (s Snapshot) Quantile(q float64) time.Duration {
	if s.Completed == 0 || len(s.Buckets) == 0 {
		return 0
	}
	if q <= 0 {
		q = 0.0001
	}
	if q > 1 {
		q = 1
	}
	want := int64(float64(s.Completed)*q + 0.5)
	if want < 1 {
		want = 1
	}
	var cum int64
	for i, n := range s.Buckets {
		cum += n
		if cum >= want {
			if i < len(bucketBounds) {
				return bucketBounds[i]
			}
			return s.LatencyMax
		}
	}
	return s.LatencyMax
}

// MeanLatency returns the average latency of completed actions.
func (s Snapshot) MeanLatency() time.Duration {
	if s.Completed == 0 {
		return 0
	}
	return s.LatencySum / time.Duration(s.Completed)
}

// Throughput returns completed actions per second.
func (s Snapshot) Throughput() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.Completed) / s.Elapsed.Seconds()
}

// SuccessRate returns succeeded / completed in [0,1].
func (s Snapshot) SuccessRate() float64 {
	if s.Completed == 0 {
		return 0
	}
	return float64(s.Succeeded) / float64(s.Completed)
}

// ErrorRate returns failed / completed in [0,1].
func (s Snapshot) ErrorRate() float64 {
	if s.Completed == 0 {
		return 0
	}
	return float64(s.Failed) / float64(s.Completed)
}

// Merge combines two snapshots into one. Buckets and counters are summed;
// elapsed takes the max (snapshots may cover overlapping wall time).
func Merge(a, b Snapshot) Snapshot {
	out := Snapshot{
		Started:    a.Started + b.Started,
		Completed:  a.Completed + b.Completed,
		Succeeded:  a.Succeeded + b.Succeeded,
		Failed:     a.Failed + b.Failed,
		Cancelled:  a.Cancelled + b.Cancelled,
		LatencySum: a.LatencySum + b.LatencySum,
		LatencyMax: max(a.LatencyMax, b.LatencyMax),
		Elapsed:    max(a.Elapsed, b.Elapsed),
	}
	if len(a.Errors) > 0 || len(b.Errors) > 0 {
		out.Errors = map[string]int64{}
		for k, v := range a.Errors {
			out.Errors[k] += v
		}
		for k, v := range b.Errors {
			out.Errors[k] += v
		}
	}
	n := max(len(a.Buckets), len(b.Buckets))
	if n > 0 {
		out.Buckets = make([]int64, n)
		for i := range out.Buckets {
			if i < len(a.Buckets) {
				out.Buckets[i] += a.Buckets[i]
			}
			if i < len(b.Buckets) {
				out.Buckets[i] += b.Buckets[i]
			}
		}
	}
	return out
}

// Collector records outcomes and produces snapshots. Safe for concurrent
// use; Record calls never block for longer than a short mutex section.
type Collector struct {
	mu       sync.Mutex
	snap     Snapshot
	errCount map[string]int64
	start    time.Time
}

// NewCollector creates an empty collector.
func NewCollector() *Collector {
	return &Collector{
		snap:     Snapshot{Buckets: make([]int64, len(bucketBounds)+1)},
		errCount: map[string]int64{},
		start:    time.Now(),
	}
}

// RecordStarted counts an action that has begun execution.
func (c *Collector) RecordStarted() {
	c.mu.Lock()
	c.snap.Started++
	c.mu.Unlock()
}

// Record stores one completed action outcome. err == nil means success;
// otherwise the error is classified by its message.
func (c *Collector) Record(latency time.Duration, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snap.Completed++
	c.snap.LatencySum += latency
	if latency > c.snap.LatencyMax {
		c.snap.LatencyMax = latency
	}
	c.snap.Buckets[bucketIndex(latency)]++
	if err == nil {
		c.snap.Succeeded++
	} else {
		c.snap.Failed++
		c.errCount[classify(err)]++
	}
}

// RecordCancelled counts an action that never completed. Cancelled actions
// are excluded from Completed and never counted as success.
func (c *Collector) RecordCancelled() {
	c.mu.Lock()
	c.snap.Cancelled++
	c.mu.Unlock()
}

// classify maps an error to a stable class string.
func classify(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Snapshot returns a consistent point-in-time view.
func (c *Collector) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.snap
	out.Buckets = append([]int64(nil), c.snap.Buckets...)
	if len(c.errCount) > 0 {
		out.Errors = make(map[string]int64, len(c.errCount))
		for k, v := range c.errCount {
			out.Errors[k] = v
		}
	}
	out.Elapsed = time.Since(c.start)
	return out
}
