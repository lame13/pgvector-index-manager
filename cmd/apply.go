package cmd

import (
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"

	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/pg"
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

Use --dry-run to see what would be done without making changes.

When reconcile.continuous is enabled in the config, apply runs in a loop
at the configured interval until interrupted.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if CfgFile == "" {
			return fmt.Errorf("--config (-c) flag is required")
		}

		cfg, err := config.Load(CfgFile)
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		ctx := cmd.Context()
		pool, err := pg.Connect(ctx, cfg.Connection.DSN)
		if err != nil {
			return fmt.Errorf("connecting to PostgreSQL: %w", err)
		}
		defer pool.Close()

		// Continuous mode
		if cfg.Reconcile.Continuous && !dryRun {
			return reconciler.ApplyContinuous(ctx, pool, cfg, func(result *reconciler.ApplyResult) error {
				printApplyResult(result)
				return writeApplyReport(result, cfg)
			})
		}

		// One-shot mode
		result, applyErr := reconciler.Apply(ctx, pool, cfg, dryRun)
		if result != nil {
			printApplyResult(result)
			if err := writeApplyReport(result, cfg); err != nil {
				if applyErr != nil {
					return fmt.Errorf("applying: %v; writing report: %w", applyErr, err)
				}
				return fmt.Errorf("writing report: %w", err)
			}
		}
		if applyErr != nil {
			return fmt.Errorf("applying: %w", applyErr)
		}
		if result == nil {
			return fmt.Errorf("applying: no result returned")
		}

		if len(result.Actions) == 0 {
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

	if len(result.Actions) == 0 {
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

func writeApplyReport(result *reconciler.ApplyResult, cfg *config.Config) error {
	if cfg.Report.OutputDir == "" {
		return nil
	}
	log.Printf("Writing report to %s", cfg.Report.OutputDir)
	return report.WriteJSON(Version, result, cfg)
}
