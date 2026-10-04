package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/checkpoint"
	"github.com/huchangjun118/load-scenario-engine-go/engine"
	"github.com/huchangjun118/load-scenario-engine-go/fault"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
)

const testConfig = `{
  "scenario": {
    "name": "it",
    "seed": 9,
    "steps": [
      {"name": "read", "op": "read", "weight": 120},
      {"name": "write", "op": "write", "weight": 30,
       "on_failure": {"kind": "continue"}}
    ]
  },
  "curve": {"segments": [
    {"kind": "constant", "level": 4, "duration": "150ms"},
    {"kind": "spike", "level": 8, "duration": "150ms"},
    {"kind": "idle", "level": 0, "duration": "100ms"},
    {"kind": "constant", "level": 4, "duration": "150ms"}
  ]},
  "max_concurrency": 8,
  "tick": "10ms",
  "checkpoint_interval": "50ms",
  "target": {"kind": "sim", "latency": "2ms", "error_rate": 0.01},
  "faults": [{"op": "write", "probability": 0.2, "latency": "1ms"}],
  "breaker": {"error_rate": 0.9, "min_samples": 10}
}`

func loadTestConfig(t *testing.T) *configJSON {
	t.Helper()
	var cfg configJSON
	if err := json.Unmarshal([]byte(testConfig), &cfg); err != nil {
		t.Fatalf("config parse: %v", err)
	}
	return &cfg
}

func TestConfigParsing(t *testing.T) {
	cfg := loadTestConfig(t)
	scn := toScenario(cfg)
	if err := scn.Validate(); err != nil {
		t.Fatalf("scenario invalid: %v", err)
	}
	if len(scn.Steps) != 2 || scn.Steps[1].OnFailure.Kind != scenario.FailureContinue {
		t.Fatalf("scenario conversion wrong: %+v", scn)
	}
	cv := toCurve(cfg)
	if err := cv.Validate(); err != nil {
		t.Fatalf("curve invalid: %v", err)
	}
	if cv.Duration() != 550*time.Millisecond {
		t.Fatalf("curve duration: %v", cv.Duration())
	}
}

// TestEndToEndReproducible runs the full pipeline twice with the same
// seed and verifies identical action streams and stable statistics.
func TestEndToEndReproducible(t *testing.T) {
	run := func(t *testing.T) (keys []string, completed int64) {
		cfg := loadTestConfig(t)
		scn := toScenario(cfg)
		cv := toCurve(cfg)

		var mu chan scenario.Action
		mu = make(chan scenario.Action, 4096)
		base := simHandler(time.Duration(cfg.Target.Latency), cfg.Target.ErrorRate)
		rec := func(ctx context.Context, a scenario.Action) error {
			mu <- a
			return base(ctx, a)
		}
		var rules []fault.Rule
		for _, r := range cfg.Faults {
			rule := fault.Rule{Op: r.Op, Probability: r.Probability, Latency: time.Duration(r.Latency)}
			if r.Error != "" {
				rule.Err = errString(r.Error)
			}
			rules = append(rules, rule)
		}
		inj := fault.NewInjector(scn.Seed, rules...)
		h := inj.Wrap(rec)

		eng, err := engine.New(engine.Config{
			Scenario:       scn,
			Curve:          cv,
			MaxConcurrency: cfg.MaxConcurrency,
			Tick:           time.Duration(cfg.Tick),
		}, h)
		if err != nil {
			t.Fatal(err)
		}
		snap, err := eng.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		close(mu)
		for a := range mu {
			keys = append(keys, a.Key)
		}
		return keys, snap.Completed
	}

	k1, c1 := run(t)
	k2, c2 := run(t)
	if c1 != c2 {
		t.Fatalf("completed differs between identical runs: %d vs %d", c1, c2)
	}
	if len(k1) != len(k2) {
		t.Fatalf("action stream length differs: %d vs %d", len(k1), len(k2))
	}
	set := map[string]int{}
	for _, k := range k1 {
		set[k]++
	}
	for _, k := range k2 {
		set[k]--
	}
	for k, v := range set {
		if v != 0 {
			t.Fatalf("action %q differs between runs", k)
		}
	}
}

// TestEndToEndCheckpointResume kills a run partway and resumes it via a
// file checkpoint; merged stats must not double count.
func TestEndToEndCheckpointResume(t *testing.T) {
	cfg := loadTestConfig(t)
	ckptPath := filepath.Join(t.TempDir(), "ckpt.json")

	runOnce := func(resume bool) (int64, error) {
		scn := toScenario(cfg)
		h := simHandler(3*time.Millisecond, 0)
		eng, err := engine.New(engine.Config{
			Scenario:           scn,
			MaxConcurrency:     4,
			Tick:               10 * time.Millisecond,
			CheckpointStore:    &checkpoint.FileStore{Path: ckptPath},
			CheckpointInterval: 20 * time.Millisecond,
			Resume:             resume,
		}, h)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		if !resume {
			go func() {
				time.Sleep(60 * time.Millisecond)
				cancel() // simulate crash mid-run
			}()
		}
		defer cancel()
		snap, err := eng.Run(ctx)
		if err != nil {
			return 0, err
		}
		return snap.Completed, nil
	}

	first, err := runOnce(false)
	if err != nil {
		t.Fatal(err)
	}
	if first == 0 || first >= 150 {
		t.Fatalf("first run should be partial, got %d", first)
	}
	final, err := runOnce(true)
	if err != nil {
		t.Fatal(err)
	}
	// Scenario has 120 + 30 = 150 actions; merged total must be exact.
	if final != 150 {
		t.Fatalf("resume double-counted or lost actions: got %d want 150", final)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestMainConfigLoadsFromDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "run.json")
	if err := os.WriteFile(path, []byte(testConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg configJSON
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Breaker == nil || cfg.Breaker.ErrorRate != 0.9 {
		t.Fatalf("breaker config not parsed: %+v", cfg.Breaker)
	}
}
