package engine

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// Histogram is a log-bucketed latency histogram with microsecond resolution,
// used for quantile estimation. Buckets cover 1µs..90s in a 1/2/..9 per
// decade scheme, plus an overflow bucket.
type Histogram struct {
	mu      sync.Mutex
	buckets []int64 // bucket i covers [bounds[i-1], bounds[i]) (bounds[-1]=0)
	bounds  []int64 // upper bounds in microseconds
	count   int64
	sumUs   int64
	minUs   int64
	maxUs   int64
}

// NewHistogram creates an empty histogram.
func NewHistogram() *Histogram {
	h := &Histogram{}
	for decade := int64(1); decade <= 10_000_000; decade *= 10 {
		for m := int64(1); m <= 9; m++ {
			h.bounds = append(h.bounds, m*decade)
		}
	}
	h.buckets = make([]int64, len(h.bounds)+1)
	return h
}

// Record records one latency sample.
func (h *Histogram) Record(latency time.Duration) {
	us := int64(latency / time.Microsecond)
	if us < 0 {
		us = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	idx := sort.Search(len(h.bounds), func(i int) bool { return h.bounds[i] > us })
	h.buckets[idx]++
	h.count++
	h.sumUs += us
	if h.count == 1 || us < h.minUs {
		h.minUs = us
	}
	if us > h.maxUs {
		h.maxUs = us
	}
}

// Merge adds another histogram into this one.
func (h *Histogram) Merge(o *Histogram) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.buckets) != len(o.buckets) {
		return
	}
	for i, c := range o.buckets {
		h.buckets[i] += c
	}
	h.count += o.count
	h.sumUs += o.sumUs
	if h.count == 0 || (o.count > 0 && o.minUs < h.minUs) {
		h.minUs = o.minUs
	}
	if o.maxUs > h.maxUs {
		h.maxUs = o.maxUs
	}
}

// Quantile estimates the q-th quantile (0..1) using linear interpolation
// within the containing bucket.
func (h *Histogram) Quantile(q float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0
	}
	rank := q * float64(h.count)
	var cum int64
	for i, c := range h.buckets {
		prev := cum
		cum += c
		if float64(cum) >= rank {
			var lo int64
			if i > 0 {
				lo = h.bounds[i-1]
			}
			var hi int64
			if i < len(h.bounds) {
				hi = h.bounds[i]
			} else {
				hi = lo * 10
				if hi == 0 {
					hi = 1
				}
			}
			if c == 0 {
				return time.Duration(lo) * time.Microsecond
			}
			frac := (rank - float64(prev)) / float64(c)
			return time.Duration(float64(lo)+frac*float64(hi-lo)) * time.Microsecond
		}
	}
	return time.Duration(h.maxUs) * time.Microsecond
}

// Count returns the number of recorded samples.
func (h *Histogram) Count() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

type histogramJSON struct {
	Buckets []int64 `json:"buckets"`
	Bounds  []int64 `json:"bounds"`
	Count   int64   `json:"count"`
	SumUs   int64   `json:"sum_us"`
	MinUs   int64   `json:"min_us"`
	MaxUs   int64   `json:"max_us"`
}

// MarshalJSON implements json.Marshaler.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return json.Marshal(histogramJSON{
		Buckets: append([]int64(nil), h.buckets...),
		Bounds:  append([]int64(nil), h.bounds...),
		Count:   h.count,
		SumUs:   h.sumUs,
		MinUs:   h.minUs,
		MaxUs:   h.maxUs,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (h *Histogram) UnmarshalJSON(b []byte) error {
	var j histogramJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.buckets = j.Buckets
	h.bounds = j.Bounds
	h.count = j.Count
	h.sumUs = j.SumUs
	h.minUs = j.MinUs
	h.maxUs = j.MaxUs
	return nil
}

// Snapshot is a point-in-time, mergeable view of statistics.
type Snapshot struct {
	Started  int64         `json:"started"`
	Success  int64         `json:"success"`
	Failed   int64         `json:"failed"`
	Canceled int64         `json:"canceled"`
	TimedOut int64         `json:"timed_out"`
	Errors   map[string]int64 `json:"errors"`
	Hist     *Histogram    `json:"histogram"`
	Elapsed  time.Duration `json:"elapsed"`
}

// Stats collects all run statistics and is safe for concurrent use.
type Stats struct {
	mu        sync.Mutex
	started   int64
	success   int64
	failed    int64
	canceled  int64
	timedOut  int64
	errors    map[string]int64
	hist      *Histogram
	startTime time.Time
	endTime   time.Time
}

// NewStats creates an empty stats collector.
func NewStats() *Stats {
	return &Stats{
		errors: make(map[string]int64),
		hist:   NewHistogram(),
	}
}

// SetStartTime records the run start (for elapsed-time throughput).
func (s *Stats) SetStartTime(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startTime = t
}

// Record records one completed action.
func (s *Stats) Record(latency time.Duration, outcome Outcome, errClass string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started++
	switch outcome {
	case OutcomeSuccess:
		s.success++
	case OutcomeFailure:
		s.failed++
	case OutcomeTimeout:
		s.timedOut++
	case OutcomeCanceled:
		s.canceled++
	}
	if outcome != OutcomeSuccess && errClass != "" {
		s.errors[errClass]++
	}
	s.hist.Record(latency)
}

// Snapshot returns a consistent copy of the statistics.
func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	errs := make(map[string]int64, len(s.errors))
	for k, v := range s.errors {
		errs[k] = v
	}
	var elapsed time.Duration
	if !s.startTime.IsZero() {
		end := s.endTime
		if end.IsZero() {
			end = time.Now()
		}
		elapsed = end.Sub(s.startTime)
	}
	return Snapshot{
		Started:  s.started,
		Success:  s.success,
		Failed:   s.failed,
		Canceled: s.canceled,
		TimedOut: s.timedOut,
		Errors:   errs,
		Hist:     s.hist,
		Elapsed:  elapsed,
	}
}

// MarkEnd records the run end time.
func (s *Stats) MarkEnd(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endTime = t
}

// Merge adds another snapshot into this one (used for checkpoint resume).
func (s *Stats) Merge(o Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started += o.Started
	s.success += o.Success
	s.failed += o.Failed
	s.canceled += o.Canceled
	s.timedOut += o.TimedOut
	for k, v := range o.Errors {
		s.errors[k] += v
	}
	s.hist.Merge(o.Hist)
}

// Report is the final human/machine-readable result of a run.
type Report struct {
	Scenario   string           `json:"scenario"`
	Seed       int64            `json:"seed"`
	Started    int64            `json:"started"`
	Success    int64            `json:"success"`
	Failed     int64            `json:"failed"`
	Canceled   int64            `json:"canceled"`
	TimedOut   int64            `json:"timed_out"`
	Errors     map[string]int64 `json:"errors"`
	Throughput float64          `json:"throughput_per_sec"`
	Elapsed    time.Duration    `json:"elapsed"`
	Quantiles  map[string]time.Duration `json:"quantiles"`
	StopReason string                  `json:"stop_reason"`
}

// Finalize computes the report from a snapshot.
func (s Snapshot) Report(name string, seed int64) Report {
	r := Report{
		Scenario: name,
		Seed:     seed,
		Started:  s.Started,
		Success:  s.Success,
		Failed:   s.Failed,
		Canceled: s.Canceled,
		TimedOut: s.TimedOut,
		Errors:   make(map[string]int64, len(s.Errors)),
		Elapsed:  s.Elapsed,
		Quantiles: make(map[string]time.Duration),
	}
	for k, v := range s.Errors {
		r.Errors[k] = v
	}
	secs := s.Elapsed.Seconds()
	if secs > 0 {
		r.Throughput = float64(s.Success) / secs
	}
	if s.Hist != nil {
		r.Quantiles["p50"] = s.Hist.Quantile(0.50)
		r.Quantiles["p90"] = s.Hist.Quantile(0.90)
		r.Quantiles["p99"] = s.Hist.Quantile(0.99)
		r.Quantiles["p999"] = s.Hist.Quantile(0.999)
	}
	return r
}
