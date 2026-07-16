package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/pg"
	"github.com/lame13/pgvector-index-manager/internal/reconciler"
)

var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "Show what index changes would be made",
	Long: `Inspect the current state and produce a plan of index changes that would
be made by the apply command. No changes are executed.`,
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

		plan, err := reconciler.Plan(ctx, pool, cfg)
		if err != nil {
			return fmt.Errorf("planning: %w", err)
		}

		printPlan(plan, cfg)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(planCmd)
}

func printPlan(plan *reconciler.PlanResult, cfg *config.Config) {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  Reconciliation Plan")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Table: %s\n", plan.Table)
	fmt.Println()

	// Show desired configuration
	fmt.Println("  Desired Configuration:")
	fmt.Printf("    Index name: %s\n", cfg.Index.Name)
	fmt.Printf("    Type: %s\n", cfg.Index.Type)
	fmt.Printf("    Metric: %s\n", cfg.Index.Metric)
	fmt.Printf("    OpClass: %s\n", cfg.OpClass())
	if cfg.Index.M > 0 {
		fmt.Printf("    m: %d\n", cfg.Index.M)
	}
	if cfg.Index.EFConstruction > 0 {
		fmt.Printf("    ef_construction: %d\n", cfg.Index.EFConstruction)
	}
	fmt.Printf("    Dimensions: %d\n", cfg.Index.Dimensions)
	if predicate := cfg.PopulationPredicateSQL(); predicate != "" {
		fmt.Printf("    Population filters: %d\n", len(cfg.Population.Filters))
	} else {
		fmt.Println("    Population filters: none (whole table)")
	}
	fmt.Printf("    Ownership tag: %s\n", cfg.Reconcile.OwnershipTag)
	fmt.Printf("    Allow replacement of same-name unowned index: %t\n", cfg.Reconcile.DropUnowned)
	fmt.Println()
	if len(plan.DriftReasons) > 0 {
		fmt.Println("  Drift:")
		for _, reason := range plan.DriftReasons {
			fmt.Printf("    • %s\n", reason)
		}
		fmt.Println()
	}

	if len(plan.Actions) == 0 {
		fmt.Println("  Actions: No changes needed.")
	} else {
		fmt.Println("  Actions:")
		for i, action := range plan.Actions {
			fmt.Printf("    %d. %s\n", i+1, action.Description)
			if action.Reason != "" {
				fmt.Printf("       Reason: %s\n", action.Reason)
			}
		}
	}
	if plan.Blocked {
		fmt.Println()
		fmt.Println("  Result: BLOCKED — apply will not mutate an unowned same-name index.")
	}

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println()
}
