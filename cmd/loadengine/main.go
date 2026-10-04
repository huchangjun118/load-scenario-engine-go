// Command loadengine runs a declarative load scenario from a JSON config
// file and prints a reproducible report.
//
// Usage:
//
//	loadengine -config run.json [-resume] [-checkpoint path]
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/checkpoint"
	"github.com/huchangjun118/load-scenario-engine-go/engine"
	"github.com/huchangjun118/load-scenario-engine-go/fault"
	"github.com/huchangjun118/load-scenario-engine-go/loadcurve"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
	"github.com/huchangjun118/load-scenario-engine-go/stats"
)

// Duration is a time.Duration that unmarshals from JSON strings like
// "500ms" or "2s".
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

type stepJSON struct {
	Name       string   `json:"name"`
	Op         string   `json:"op"`
	Weight     int      `json:"weight,omitempty"`
	WaitBefore Duration `json:"wait_before,omitempty"`
	WaitAfter  Duration `json:"wait_after,omitempty"`
	OnFailure  *struct {
		Kind string `json:"kind"`
		Step int    `json:"step,omitempty"`
	} `json:"on_failure,omitempty"`
}

type segmentJSON struct {
	Kind     string   `json:"kind"`
	Duration Duration `json:"duration"`
	From     int      `json:"from,omitempty"`
	To       int      `json:"to,omitempty"`
	Level    int      `json:"level,omitempty"`
}

type ruleJSON struct {
	Op          string   `json:"op,omitempty"`
	Probability float64  `json:"probability"`
	Latency     Duration `json:"latency,omitempty"`
	Error       string   `json:"error,omitempty"`
}

type configJSON struct {
	Scenario struct {
		Name  string     `json:"name"`
		Seed  int64      `json:"seed"`
		Steps []stepJSON `json:"steps"`
	} `json:"scenario"`
	Curve struct {
		Segments []segmentJSON `json:"segments"`
	} `json:"curve"`
	MaxConcurrency     int      `json:"max_concurrency"`
	QueueSize          int      `json:"queue_size,omitempty"`
	Tick               Duration `json:"tick,omitempty"`
	CheckpointPath     string   `json:"checkpoint_path,omitempty"`
	CheckpointInterval Duration `json:"checkpoint_interval,omitempty"`
	Target             struct {
		Kind      string   `json:"kind"` // "sim" or "http"
		URL       string   `json:"url,omitempty"`
		Method    string   `json:"method,omitempty"`
		Latency   Duration `json:"latency,omitempty"`
		ErrorRate float64  `json:"error_rate,omitempty"`
	} `json:"target"`
	Faults  []ruleJSON `json:"faults,omitempty"`
	Breaker *struct {
		ErrorRate  float64  `json:"error_rate,omitempty"`
		P99Latency Duration `json:"p99_latency,omitempty"`
		MinSamples int64    `json:"min_samples,omitempty"`
	} `json:"breaker,omitempty"`
}

func toScenario(c *configJSON) *scenario.Scenario {
	s := &scenario.Scenario{Name: c.Scenario.Name, Seed: c.Scenario.Seed}
	for _, st := range c.Scenario.Steps {
		step := scenario.Step{
			Name:       st.Name,
			Op:         st.Op,
			Weight:     st.Weight,
			WaitBefore: time.Duration(st.WaitBefore),
			WaitAfter:  time.Duration(st.WaitAfter),
		}
		if st.OnFailure != nil {
			step.OnFailure = &scenario.FailurePolicy{Kind: st.OnFailure.Kind, Step: st.OnFailure.Step}
		}
		s.Steps = append(s.Steps, step)
	}
	return s
}

func toCurve(c *configJSON) *loadcurve.Curve {
	cv := &loadcurve.Curve{}
	for _, sg := range c.Curve.Segments {
		cv.Segments = append(cv.Segments, loadcurve.Segment{
			Kind:     loadcurve.Kind(sg.Kind),
			Duration: time.Duration(sg.Duration),
			From:     sg.From,
			To:       sg.To,
			Level:    sg.Level,
		})
	}
	return cv
}

// simHandler is a deterministic in-process target: latency and failures
// derive only from the action key, so identical runs produce identical
// results.
func simHandler(latency time.Duration, errRate float64) engine.Handler {
	return func(ctx context.Context, a scenario.Action) error {
		h := fnv.New64a()
		h.Write([]byte(a.Key))
		sum := h.Sum64()
		jitter := time.Duration(sum % uint64(latency/4+1))
		d := latency + jitter
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		if errRate > 0 && float64(sum%10000)/10000 < errRate {
			return errors.New("simulated target error")
		}
		return nil
	}
}

func httpHandler(client *http.Client, method, url string) engine.Handler {
	return func(ctx context.Context, a scenario.Action) error {
		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode >= 500 {
			return fmt.Errorf("http status %d", resp.StatusCode)
		}
		return nil
	}
}

func main() {
	var (
		configPath = flag.String("config", "", "path to JSON run config")
		resume     = flag.Bool("resume", false, "resume from the last checkpoint")
		ckptPath   = flag.String("checkpoint", "", "override checkpoint file path")
	)
	flag.Parse()
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing -config")
		os.Exit(2)
	}
	data, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read config:", err)
		os.Exit(1)
	}
	var cfg configJSON
	if err := json.Unmarshal(data, &cfg); err != nil {
		fmt.Fprintln(os.Stderr, "parse config:", err)
		os.Exit(1)
	}

	scn := toScenario(&cfg)
	curve := toCurve(&cfg)

	var handler engine.Handler
	switch cfg.Target.Kind {
	case "", "sim":
		lat := time.Duration(cfg.Target.Latency)
		if lat <= 0 {
			lat = 5 * time.Millisecond
		}
		handler = simHandler(lat, cfg.Target.ErrorRate)
	case "http":
		m := cfg.Target.Method
		if m == "" {
			m = http.MethodGet
		}
		handler = httpHandler(&http.Client{Timeout: 30 * time.Second}, m, cfg.Target.URL)
	default:
		fmt.Fprintln(os.Stderr, "unknown target kind:", cfg.Target.Kind)
		os.Exit(2)
	}

	// Fault injection wraps the target.
	if len(cfg.Faults) > 0 {
		rules := make([]fault.Rule, 0, len(cfg.Faults))
		for _, r := range cfg.Faults {
			rule := fault.Rule{Op: r.Op, Probability: r.Probability, Latency: time.Duration(r.Latency)}
			if r.Error != "" {
				rule.Err = errors.New(r.Error)
			}
			rules = append(rules, rule)
		}
		handler = fault.NewInjector(scn.Seed, rules...).Wrap(handler)
	}

	engCfg := engine.Config{
		Scenario:       scn,
		Curve:          curve,
		MaxConcurrency: cfg.MaxConcurrency,
		QueueSize:      cfg.QueueSize,
		Tick:           time.Duration(cfg.Tick),
		Resume:         *resume,
	}
	ckpt := *ckptPath
	if ckpt == "" {
		ckpt = cfg.CheckpointPath
	}
	if ckpt != "" {
		engCfg.CheckpointStore = &checkpoint.FileStore{Path: ckpt}
		engCfg.CheckpointInterval = time.Duration(cfg.CheckpointInterval)
		if engCfg.CheckpointInterval <= 0 {
			engCfg.CheckpointInterval = time.Second
		}
	}

	// Circuit breaker: on trip, stop applying load but keep the scene
	// (final checkpoint is saved by the engine).
	if cfg.Breaker != nil {
		breaker := fault.NewBreaker(fault.Thresholds{
			ErrorRate:  cfg.Breaker.ErrorRate,
			P99Latency: time.Duration(cfg.Breaker.P99Latency),
			MinSamples: cfg.Breaker.MinSamples,
		}, fault.Policy{Kind: "stop"})
		engCfg.OnTrip = breaker.Check
	}

	eng, err := engine.New(engCfg, handler)
	if err != nil {
		fmt.Fprintln(os.Stderr, "engine:", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	snap, err := eng.Run(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
		os.Exit(1)
	}
	printReport(snap)
}

func printReport(s stats.Snapshot) {
	fmt.Println("=== load run report ===")
	fmt.Printf("completed: %d  succeeded: %d  failed: %d  cancelled: %d\n",
		s.Completed, s.Succeeded, s.Failed, s.Cancelled)
	fmt.Printf("success rate: %.4f  error rate: %.4f\n", s.SuccessRate(), s.ErrorRate())
	fmt.Printf("throughput: %.2f ops/s over %s\n", s.Throughput(), s.Elapsed.Round(time.Millisecond))
	fmt.Printf("latency mean: %s  p50: %s  p95: %s  p99: %s  max: %s\n",
		s.MeanLatency().Round(time.Microsecond),
		s.Quantile(0.50), s.Quantile(0.95), s.Quantile(0.99), s.LatencyMax)
	if len(s.Errors) > 0 {
		fmt.Println("error classes:")
		for k, v := range s.Errors {
			fmt.Printf("  %q: %d\n", k, v)
		}
	}
}
