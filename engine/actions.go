package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/scenario"
)

// ActionFunc adapts a function to the Action interface.
type ActionFunc func(ctx context.Context, rng *rand.Rand) error

func (f ActionFunc) Execute(ctx context.Context, rng *rand.Rand) error {
	return f(ctx, rng)
}

// HTTPError wraps a non-2xx HTTP response.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http status %d: %s", e.Status, e.Body)
}

// HTTPAction performs an HTTP request.
type HTTPAction struct {
	Method string
	URL    string
	Body   string
	client *http.Client
}

func (a *HTTPAction) Execute(ctx context.Context, rng *rand.Rand) error {
	var body io.Reader
	if a.Body != "" {
		body = bytes.NewReader([]byte(a.Body))
	}
	req, err := http.NewRequestWithContext(ctx, a.Method, a.URL, body)
	if err != nil {
		return err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 400 {
		return &HTTPError{Status: resp.StatusCode, Body: string(respBody)}
	}
	return nil
}

// SleepAction pauses for a duration.
type SleepAction struct{ Duration time.Duration }

func (a *SleepAction) Execute(ctx context.Context, rng *rand.Rand) error {
	return sleepCtx(ctx, a.Duration)
}

// LogAction records a log line (the step id) without touching the target.
type LogAction struct{ Message string }

func (a *LogAction) Execute(ctx context.Context, rng *rand.Rand) error {
	fmt.Printf("[log] %s\n", a.Message)
	return nil
}

// sleepCtx sleeps for d but returns early with context.Canceled if ctx is
// cancelled, so waits never outlive a stop.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ActionBuilder constructs an Action from a scenario spec.
type ActionBuilder func(spec scenario.ActionSpec) (Action, error)

var actionBuilders = map[string]ActionBuilder{}

// RegisterAction registers a custom action type (used by tests and extensions).
func RegisterAction(name string, b ActionBuilder) {
	actionBuilders[name] = b
}

func buildAction(spec scenario.ActionSpec) (Action, error) {
	if b, ok := actionBuilders[spec.Type]; ok {
		return b(spec)
	}
	switch spec.Type {
	case "http":
		method := spec.Method
		if method == "" {
			method = "GET"
		}
		if spec.URL == "" {
			return nil, fmt.Errorf("http action requires a url")
		}
		return &HTTPAction{
			Method: method,
			URL:    spec.URL,
			Body:   spec.Body,
			client: http.DefaultClient,
		}, nil
	case "sleep":
		return &SleepAction{Duration: spec.Duration.Value()}, nil
	case "log":
		return &LogAction{Message: spec.Body}, nil
	default:
		return nil, fmt.Errorf("unknown action type %q", spec.Type)
	}
}
