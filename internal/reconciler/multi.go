package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lame13/pgvector-index-manager/internal/config"
)

// Target pairs one resolved configuration with the pool that reaches it.
// Targets that share a DSN usually share a single pool.
type Target struct {
	Name   string
	Config *config.Config
	Pool   *pgxpool.Pool
}

// label names the target in logs, preferring the explicit target name.
func (t Target) label() string {
	if t.Name != "" {
		return t.Name
	}
	if t.Config != nil {
		return t.Config.Name
	}
	return ""
}

// TargetResult is the outcome of reconciling a single target.
type TargetResult struct {
	Name   string
	Result *ApplyResult
	Err    error
}

// MultiApplyResult aggregates one pass over every target of a configuration.
type MultiApplyResult struct {
	Targets []TargetResult
}

// Failed reports whether any target failed.
func (m *MultiApplyResult) Failed() bool {
	for _, target := range m.Targets {
		if target.Err != nil || (target.Result != nil && target.Result.Failed) {
			return true
		}
	}
	return false
}

// Changed reports whether any target executed at least one action.
func (m *MultiApplyResult) Changed() bool {
	for _, target := range m.Targets {
		if target.Result != nil && len(target.Result.Actions) > 0 {
			return true
		}
	}
	return false
}

// Errors returns the per-target failures in target order.
func (m *MultiApplyResult) Errors() []error {
	var errs []error
	for _, target := range m.Targets {
		if target.Err != nil {
			errs = append(errs, target.Err)
		}
	}
	return errs
}

// ApplyAll reconciles every target in order. A failure is recorded against its
// target and the remaining targets still run: one broken index family must not
// discard unrelated work in the same pass.
func ApplyAll(ctx context.Context, targets []Target, dryRun bool) *MultiApplyResult {
	multi := &MultiApplyResult{Targets: make([]TargetResult, 0, len(targets))}
	for _, target := range targets {
		item := TargetResult{Name: target.label()}
		if canceled := ctx.Err(); canceled != nil {
			item.Err = canceled
			multi.Targets = append(multi.Targets, item)
			continue
		}
		item.Result, item.Err = Apply(ctx, target.Pool, target.Config, dryRun)
		multi.Targets = append(multi.Targets, item)
	}
	return multi
}

// ApplyAllContinuous reconciles every target immediately and then at each
// interval. Cancellation is a clean shutdown rather than a runtime failure.
//
// onResult is called only for targets that produced a result with at least one
// action or a failure, so it never receives a nil result. Failures that
// prevented a reconciliation from running are logged and passed to no callback.
func ApplyAllContinuous(ctx context.Context, targets []Target, interval time.Duration, onResult func(Target, *ApplyResult, error) error) error {
	if len(targets) == 0 {
		return fmt.Errorf("no targets to reconcile")
	}
	if interval <= 0 {
		return fmt.Errorf("reconciliation interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("Starting continuous reconciliation of %d target(s) (interval: %s)", len(targets), interval)

	for {
		multi := ApplyAll(ctx, targets, false)
		for i, item := range multi.Targets {
			target := targets[i]
			if item.Err != nil && !errors.Is(item.Err, context.Canceled) {
				logTargetError(target, item.Err)
			}
			if onResult == nil || item.Result == nil {
				continue
			}
			if len(item.Result.Actions) == 0 && !item.Result.Failed {
				continue
			}
			if err := onResult(target, item.Result, item.Err); err != nil {
				log.Printf("Target %s result handling error: %v", target.label(), err)
			}
		}

		select {
		case <-ctx.Done():
			log.Printf("Stopping continuous reconciliation")
			return nil
		case <-ticker.C:
		}
	}
}

// ContinuousInterval returns the configured polling interval. Continuous mode
// is a run-level setting, so every target shares the value.
func ContinuousInterval(cfg *config.Config) (time.Duration, error) {
	interval, err := time.ParseDuration(cfg.Reconcile.Interval)
	if err != nil {
		return 0, fmt.Errorf("parsing interval: %w", err)
	}
	if interval <= 0 {
		return 0, fmt.Errorf("reconciliation interval must be positive")
	}
	return interval, nil
}

func logTargetError(target Target, err error) {
	if target.label() == "" {
		log.Printf("Reconciliation error: %v", err)
		return
	}
	log.Printf("Target %s reconciliation error: %v", target.label(), err)
}
