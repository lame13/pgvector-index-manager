package cmd

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"

	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/reconciler"
	"github.com/lame13/pgvector-index-manager/internal/report"
)

var dryRun bool

var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Execute index reconciliation",
	Long: `Inspect the current state, plan changes, and execute the reconciliation.
Builds replacements concurrently, verifies them, and retires old owned
indexes only after verification.

A configuration file may declare several targets. Every target is reconciled
in order; a failure on one target is reported and does not stop the others.

Use --dry-run to see what would be done without making changes.

When reconcile.continuous is enabled in the config, apply runs in a loop
at the configured interval until interrupted.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if CfgFile == "" {
			return fmt.Errorf("--config (-c) flag is required")
		}

		configs, err := config.LoadTargets(CfgFile)
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		ctx := cmd.Context()
		targets, closePools, err := connectTargets(ctx, configs)
		if err != nil {
			return err
		}
		defer closePools()

		// Continuous mode
		if configs[0].Reconcile.Continuous && !dryRun {
			interval, err := reconciler.ContinuousInterval(configs[0])
			if err != nil {
				return err
			}
			return reconciler.ApplyAllContinuous(ctx, targets, interval, func(target reconciler.Target, result *reconciler.ApplyResult, _ error) error {
				printTargetBanner(target.Name)
				printApplyResult(result)
				return writeApplyReport(target, result)
			})
		}

		// One-shot mode
		multi := reconciler.ApplyAll(ctx, targets, dryRun)
		var errs []error
		for i, item := range multi.Targets {
			target := targets[i]
			printTargetBanner(target.Name)
			if item.Err != nil {
				errs = append(errs, targetError("applying", target.Name, item.Err))
			}
			if item.Result != nil {
				printApplyResult(item.Result)
				if err := writeApplyReport(target, item.Result); err != nil {
					errs = append(errs, targetError("writing report for", target.Name, err))
				}
			}
		}
		if err := errors.Join(errs...); err != nil {
			return err
		}
		if len(targets) == 1 && multi.Targets[0].Result == nil {
			return fmt.Errorf("applying: no result returned")
		}

		if !multi.Changed() {
			return ErrNothingToApply
		}

		return nil
	},
}

func init() {
	applyCmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be done without making changes")
	rootCmd.AddCommand(applyCmd)
}

func printApplyResult(result *reconciler.ApplyResult) {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  Reconciliation Result")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Table: %s\n", result.Table)
	fmt.Printf("  Dry run: %t\n", result.DryRun)
	if !result.StartTime.IsZero() && !result.EndTime.IsZero() {
		fmt.Printf("  Duration: %s\n", result.EndTime.Sub(result.StartTime).Round(time.Second))
	}
	fmt.Println()

	if len(result.Actions) == 0 && result.Failed {
		fmt.Println("  Reconciliation failed before any actions completed.")
	} else if len(result.Actions) == 0 {
		fmt.Println("  No changes were needed.")
	} else {
		for _, action := range result.Actions {
			status := "✓"
			if action.Error != "" {
				status = "✗"
			}
			durationStr := ""
			if action.Duration > 0 {
				durationStr = fmt.Sprintf(" (%s)", action.Duration.Round(time.Second))
			}
			fmt.Printf("  %s %s%s\n", status, action.Description, durationStr)
			if action.Error != "" {
				fmt.Printf("    Error: %s\n", action.Error)
			}
		}
	}

	fmt.Println()
	if result.Failed {
		fmt.Println("  Result: FAILED")
	} else if result.DryRun {
		fmt.Println("  Result: DRY RUN COMPLETE")
	} else {
		fmt.Println("  Result: SUCCESS")
	}
	fmt.Println("═══════════════════════════════════════════")

	fmt.Println()
}

func writeApplyReport(target reconciler.Target, result *reconciler.ApplyResult) error {
	cfg := target.Config
	if cfg.Report.OutputDir == "" {
		return nil
	}
	log.Printf("Writing report to %s", cfg.Report.OutputDir)
	return report.WriteJSONForTarget(Version, result, cfg, cfg.Name)
}
