package reconciler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lame13/pgvector-index-manager/internal/config"
)

func TestMultiApplyResultAggregatesTargetOutcomes(t *testing.T) {
	multi := &MultiApplyResult{Targets: []TargetResult{
		{Name: "changed", Result: &ApplyResult{Actions: []ActionResult{{Kind: ActionBuild}}}},
		{Name: "quiet", Result: &ApplyResult{}},
		{Name: "failed", Result: &ApplyResult{Failed: true}, Err: errors.New("boom")},
	}}
	if !multi.Failed() {
		t.Fatal("Failed() = false, want true when a target fails")
	}
	if !multi.Changed() {
		t.Fatal("Changed() = false, want true when a target applied actions")
	}
	errs := multi.Errors()
	if len(errs) != 1 || errs[0].Error() != "boom" {
		t.Fatalf("Errors() = %v, want the single target error", errs)
	}
}

func TestMultiApplyResultReportsQuietRun(t *testing.T) {
	multi := &MultiApplyResult{Targets: []TargetResult{
		{Name: "a", Result: &ApplyResult{}},
		{Name: "b", Result: &ApplyResult{}},
	}}
	if multi.Failed() || multi.Changed() {
		t.Fatalf("quiet run = %#v, want no failures and no changes", multi)
	}
	if len(multi.Errors()) != 0 {
		t.Fatalf("Errors() = %v, want none", multi.Errors())
	}
}

func TestMultiApplyResultFlagsFailureWithoutError(t *testing.T) {
	multi := &MultiApplyResult{Targets: []TargetResult{
		{Name: "failed", Result: &ApplyResult{Failed: true, Actions: []ActionResult{{Kind: ActionBlocked, Error: "conflict"}}}},
	}}
	if !multi.Failed() {
		t.Fatal("Failed() = false, want true for a failed result without a returned error")
	}
	if len(multi.Errors()) != 0 {
		t.Fatalf("Errors() = %v, want none for an in-result failure", multi.Errors())
	}
}

func TestApplyAllSkipsWorkWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Nil pools prove ApplyAll never dials once the context is done.
	multi := ApplyAll(ctx, []Target{{Name: "a"}, {Name: "b"}}, false)
	if len(multi.Targets) != 2 {
		t.Fatalf("ApplyAll() targets = %#v, want one entry per target", multi.Targets)
	}
	errs := multi.Errors()
	if len(errs) != 2 {
		t.Fatalf("Errors() = %v, want a cancellation error per target", errs)
	}
	for _, err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Errors() = %v, want context.Canceled", errs)
		}
	}
}

func TestApplyAllContinuousRejectsEmptyTargetList(t *testing.T) {
	err := ApplyAllContinuous(context.Background(), nil, time.Second, nil)
	if err == nil || !strings.Contains(err.Error(), "no targets") {
		t.Fatalf("ApplyAllContinuous() error = %v, want a no-targets error", err)
	}
}

func TestApplyAllContinuousNeverReportsNilResults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// A canceled context makes every target fail before Apply runs, which is
	// exactly the case where reporting a nil result would panic a printer.
	err := ApplyAllContinuous(ctx, []Target{{Name: "a"}}, time.Second, func(_ Target, result *ApplyResult, _ error) error {
		t.Fatalf("callback invoked with result %#v, want no callback for a target that never reconciled", result)
		return nil
	})
	if err != nil {
		t.Fatalf("ApplyAllContinuous() error = %v, want a clean shutdown", err)
	}
}

func TestContinuousInterval(t *testing.T) {
	if _, err := ContinuousInterval(&config.Config{
		Reconcile: config.ReconcileConfig{Interval: "soon"},
	}); err == nil || !strings.Contains(err.Error(), "parsing interval") {
		t.Fatalf("ContinuousInterval() error = %v, want a parsing error", err)
	}

	interval, err := ContinuousInterval(&config.Config{
		Reconcile: config.ReconcileConfig{Interval: "90s"},
	})
	if err != nil {
		t.Fatalf("ContinuousInterval() error = %v", err)
	}
	if interval != 90*time.Second {
		t.Fatalf("ContinuousInterval() = %s, want 1m30s", interval)
	}
}

func TestContinuousRejectsNonPositiveIntervals(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		if err := ApplyAllContinuous(context.Background(), []Target{{Name: "a"}}, interval, nil); err == nil {
			t.Fatalf("ApplyAllContinuous(%s) must reject a non-positive interval", interval)
		}
		if _, err := ContinuousInterval(&config.Config{
			Reconcile: config.ReconcileConfig{Interval: interval.String()},
		}); err == nil {
			t.Fatalf("ContinuousInterval(%s) must reject a non-positive interval", interval)
		}
	}
}
