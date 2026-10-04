// Command loadgen runs a declarative load scenario.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/curve"
	"github.com/huchangjun118/load-scenario-engine-go/engine"
	"github.com/huchangjun118/load-scenario-engine-go/scenario"
)

func main() {
	scenarioPath := flag.String("scenario", "", "path to scenario JSON file")
	checkpointPath := flag.String("checkpoint", "", "checkpoint file path")
	resume := flag.Bool("resume", false, "resume from checkpoint")
	maxConcurrency := flag.Int("max-concurrency", 0, "hard concurrency cap (0 = curve-driven)")
	seed := flag.Int64("seed", 0, "seed override (0 = use scenario seed)")
	duration := flag.Duration("duration", 0, "override run duration (0 = curve-defined)")
	actionTimeout := flag.Duration("action-timeout", 0, "per-action timeout (0 = none)")
	faultProb := flag.Float64("fault-prob", 0, "injected fault probability per action (0..1)")
	latencyMin := flag.Duration("latency-min", 0, "minimum injected latency")
	latencyMax := flag.Duration("latency-max", 0, "maximum injected latency")
	breaker := flag.Bool("breaker", false, "enable circuit breaker")
	breakerThreshold := flag.Float64("breaker-threshold", 0.5, "error rate that trips the breaker")
	breakerMinSamples := flag.Int("breaker-min-samples", 20, "minimum samples before breaker may trip")
	reportPath := flag.String("report", "", "report output path (default stdout)")
	flag.Parse()

	if *scenarioPath == "" {
		fmt.Fprintln(os.Stderr, "usage: loadgen -scenario <file.json> [flags]")
		os.Exit(2)
	}

	data, err := os.ReadFile(*scenarioPath)
	if err != nil {
		fatal("read scenario: %v", err)
	}
	scen, err := scenario.Parse(data)
	if err != nil {
		fatal("parse scenario: %v", err)
	}
	if err := scen.Validate(); err != nil {
		fatal("invalid scenario: %v", err)
	}

	c, err := curve.Build(scen.Curve)
	if err != nil {
		fatal("build curve: %v", err)
	}
	if *duration > 0 {
		c = curve.Limited{Curve: c, Limit: *duration}
	}

	cfg := engine.Config{
		MaxConcurrency:   *maxConcurrency,
		ActionTimeout:    *actionTimeout,
		FaultProbability: *faultProb,
		LatencyMin:       *latencyMin,
		LatencyMax:       *latencyMax,
		BreakerEnabled:   *breaker,
		BreakerThreshold: *breakerThreshold,
		BreakerMinSamples: *breakerMinSamples,
		CheckpointPath:   *checkpointPath,
		Resume:           *resume,
	}
	if *seed != 0 {
		cfg.Seed = *seed
	}

	eng := engine.New(cfg, scen, c)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := eng.Run(ctx)
	if err != nil {
		fatal("run: %v", err)
	}

	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal("marshal report: %v", err)
	}
	if *reportPath != "" {
		if err := os.WriteFile(*reportPath, out, 0o644); err != nil {
			fatal("write report: %v", err)
		}
	} else {
		fmt.Println(string(out))
	}
	if report.StopReason == string(engine.StopBreaker) {
		os.Exit(3)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

var _ = time.Second
